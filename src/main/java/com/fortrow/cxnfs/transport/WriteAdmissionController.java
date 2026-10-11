package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public final class WriteAdmissionController {
    private static final long DEFAULT_FLOW_CONTROL_WINDOW_BYTES = 1024L * 1024 * 1024;

    private final WriteAdmissionPolicy policy;
    private final CordPoolConfig cordPool;
    private final WriteAdmissionLimits limits;
    private final Clock clock;
    private final Map<UUID, AdmissionRecord> admittedTransfers = new LinkedHashMap<>();
    private final Map<UUID, Map<Integer, TransferEntryCommittedResponse>> committedEntriesByTransfer = new LinkedHashMap<>();
    private final Map<UUID, Long> reservedPayloadBytesByTransfer = new LinkedHashMap<>();
    private Optional<UUID> sessionId = Optional.empty();
    private Optional<String> principal = Optional.empty();

    public WriteAdmissionController() {
        this(WriteAdmissionPolicy.defaults(), CordPoolConfig.xetLikeDefaults());
    }

    public WriteAdmissionController(WriteAdmissionPolicy policy) {
        this(policy, CordPoolConfig.xetLikeDefaults());
    }

    public WriteAdmissionController(WriteAdmissionPolicy policy, CordPoolConfig cordPool) {
        this(policy, cordPool, WriteAdmissionLimits.defaults(), Clock.systemUTC());
    }

    public WriteAdmissionController(
            WriteAdmissionPolicy policy,
            CordPoolConfig cordPool,
            WriteAdmissionLimits limits,
            Clock clock
    ) {
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        if (limits == null) {
            throw new IllegalArgumentException("limits is required");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock is required");
        }
        this.policy = policy;
        this.cordPool = cordPool;
        this.limits = limits;
        this.clock = clock;
    }

    public synchronized void authenticateControlSession(UUID sessionId, String principal) {
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
        this.sessionId = Optional.of(sessionId);
        this.principal = Optional.of(principal);
    }

    public synchronized TransferAcceptedResponse admit(TransferBootstrapRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (policy.authenticatedControlSessionRequired() && sessionId.isEmpty()) {
            throw new IllegalStateException("control session must be authenticated before write admission");
        }
        return admitAuthenticated(sessionId.orElseThrow(), principal.orElseThrow(), request);
    }

    public synchronized TransferAcceptedResponse admit(
            UUID sessionId,
            String principal,
            TransferBootstrapRequest request
    ) {
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        return admitAuthenticated(sessionId, principal, request);
    }

    private TransferAcceptedResponse admitAuthenticated(
            UUID sessionId,
            String principal,
            TransferBootstrapRequest request
    ) {
        if (request.direction() != TransferDirection.WRITE) {
            throw new IllegalArgumentException("write admission only accepts write transfers");
        }
        pruneExpiredTransfers();
        if (admittedTransfers.containsKey(request.transferId())) {
            throw new IllegalStateException("write transfer is already admitted: " + request.transferId());
        }
        if (admittedTransfers.size() >= limits.maxAdmittedWriteTransfers()) {
            throw new IllegalStateException("too many admitted write transfers");
        }

        String rootPath = normalizePath(request.path());
        int acceptedMaxCords = Math.min(request.maxCords(), cordPool.maxCords());
        int acceptedInitialCords = cordPool.adaptive()
                ? Math.min(request.initialCords(), acceptedMaxCords)
                : acceptedMaxCords;
        AdmittedWriteTransfer admitted = new AdmittedWriteTransfer(
                request.transferId(),
                sessionId,
                principal,
                rootPath,
                acceptedMaxCords,
                request.expectedSizeLimitBytes()
        );
        admittedTransfers.put(
                request.transferId(),
                new AdmissionRecord(admitted, clock.instant().plus(limits.writeTransferTtl()))
        );
        return new TransferAcceptedResponse(
                request.transferId(),
                TransferDirection.WRITE,
                acceptedInitialCords,
                acceptedMaxCords,
                DEFAULT_FLOW_CONTROL_WINDOW_BYTES,
                true
        );
    }

    public synchronized boolean canBufferPayload(UUID transferId) {
        pruneExpiredTransfers();
        return transferId != null && admittedTransfers.containsKey(transferId);
    }

    public synchronized void requirePayloadAdmission(UUID transferId) {
        if (!canBufferPayload(transferId)) {
            throw new IllegalStateException("write payload is not admitted for transfer: " + transferId);
        }
    }

    public synchronized void reservePayload(BulkDataFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        AdmittedWriteTransfer admitted = admitted(frame.transferId());
        if (frame.cordIndex() >= admitted.maxCords()) {
            throw new IllegalStateException("write payload cord is outside accepted cord range: " + frame.cordIndex());
        }
        if (admitted.expectedSizeLimitBytes() > 0) {
            long reserved = reservedPayloadBytesByTransfer.getOrDefault(frame.transferId(), 0L);
            long projected = reserved + frame.payloadLength();
            if (projected < reserved || projected > admitted.expectedSizeLimitBytes()) {
                throw new IllegalStateException("write payload exceeds admitted transfer size limit");
            }
        }
        reservedPayloadBytesByTransfer.merge(frame.transferId(), (long) frame.payloadLength(), Long::sum);
    }

    public synchronized void acceptEntryMetadata(TransferEntryUpdate update) {
        if (update == null) {
            throw new IllegalArgumentException("update is required");
        }
        AdmittedWriteTransfer admitted = admitted(update.transferId());
        requireUnderRoot(admitted.rootPath(), update.path());
        if (!update.targetPath().isBlank()) {
            requireUnderRoot(admitted.rootPath(), update.targetPath());
        }
    }

    public synchronized TransferEntryCommittedResponse commitEntry(TransferEntryCommitRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        AdmittedWriteTransfer admitted = admitted(request.transferId());
        requireUnderRoot(admitted.rootPath(), request.path());
        TransferEntryCommittedResponse response = new TransferEntryCommittedResponse(
                request.transferId(),
                request.entrySequence(),
                request.path(),
                request.contentHash(),
                request.bytesWritten(),
                request.checksum()
        );
        recordCommittedEntry(response);
        return response;
    }

    public synchronized TransferCommittedResponse commit(TransferCommitRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        AdmittedWriteTransfer admitted = admitted(request.transferId());
        requireUnderRoot(admitted.rootPath(), request.path());
        requireTransferSummaryMatchesCommittedEntries(request);
        if (admitted.expectedSizeLimitBytes() > 0 && request.bytesWritten() > admitted.expectedSizeLimitBytes()) {
            throw new IllegalStateException("transfer exceeds admitted size limit");
        }
        admittedTransfers.remove(request.transferId());
        committedEntriesByTransfer.remove(request.transferId());
        reservedPayloadBytesByTransfer.remove(request.transferId());
        return new TransferCommittedResponse(request.transferId(), request.contentHash(), request.bytesWritten());
    }

    public synchronized Optional<AdmittedWriteTransfer> admittedTransfer(UUID transferId) {
        pruneExpiredTransfers();
        return Optional.ofNullable(admittedTransfers.get(transferId)).map(AdmissionRecord::transfer);
    }

    public synchronized int admittedTransferCount() {
        pruneExpiredTransfers();
        return admittedTransfers.size();
    }

    public synchronized List<UUID> pruneExpiredTransfers() {
        Instant now = clock.instant();
        List<UUID> expired = admittedTransfers.entrySet().stream()
                .filter(entry -> !entry.getValue().expiresAt().isAfter(now))
                .map(Map.Entry::getKey)
                .toList();
        for (UUID transferId : expired) {
            admittedTransfers.remove(transferId);
            committedEntriesByTransfer.remove(transferId);
            reservedPayloadBytesByTransfer.remove(transferId);
        }
        return expired;
    }

    synchronized void rollbackEntryCommit(UUID transferId, int entrySequence) {
        Map<Integer, TransferEntryCommittedResponse> entries = committedEntriesByTransfer.get(transferId);
        if (entries == null) {
            return;
        }
        entries.remove(entrySequence);
        if (entries.isEmpty()) {
            committedEntriesByTransfer.remove(transferId);
        }
    }

    public synchronized void rollbackPayloadReservation(BulkDataFrame frame) {
        if (frame == null) {
            return;
        }
        reservedPayloadBytesByTransfer.computeIfPresent(frame.transferId(), (ignored, bytes) -> {
            long remaining = bytes - frame.payloadLength();
            return remaining > 0 ? remaining : null;
        });
    }

    private AdmittedWriteTransfer admitted(UUID transferId) {
        pruneExpiredTransfers();
        AdmissionRecord admitted = admittedTransfers.get(transferId);
        if (admitted == null) {
            throw new IllegalStateException("transfer is not admitted: " + transferId);
        }
        return admitted.transfer();
    }

    private void recordCommittedEntry(TransferEntryCommittedResponse response) {
        Map<Integer, TransferEntryCommittedResponse> entries = committedEntriesByTransfer.computeIfAbsent(
                response.transferId(),
                ignored -> new LinkedHashMap<>()
        );
        TransferEntryCommittedResponse prior = entries.putIfAbsent(response.entrySequence(), response);
        if (prior != null) {
            throw new IllegalStateException("entry sequence already committed: " + response.entrySequence());
        }
    }

    private void requireTransferSummaryMatchesCommittedEntries(TransferCommitRequest request) {
        List<TransferEntryCommittedResponse> entries = new ArrayList<>(
                committedEntriesByTransfer.getOrDefault(request.transferId(), Map.of()).values()
        );
        if (entries.isEmpty() && request.bytesWritten() > 0) {
            throw new IllegalStateException("transfer has no committed entries: " + request.transferId());
        }
        entries.sort(Comparator.comparingInt(TransferEntryCommittedResponse::entrySequence));
        List<ContentHash> entryHashes = entries.stream()
                .map(TransferEntryCommittedResponse::contentHash)
                .toList();
        List<Long> entrySizes = entries.stream()
                .map(TransferEntryCommittedResponse::bytesWritten)
                .toList();
        long actualBytes = entrySizes.stream().mapToLong(Long::longValue).sum();
        ContentHash actualHash = Hashing.transfer(entryHashes, entrySizes);
        if (actualBytes != request.bytesWritten()) {
            throw new IllegalStateException("transfer byte total does not match committed entries");
        }
        if (!actualHash.equals(request.contentHash())) {
            throw new IllegalStateException("transfer hash does not match committed entries");
        }
    }

    private static void requireUnderRoot(String rootPath, String path) {
        NamespacePath.requireUnderRoot(rootPath, path);
    }

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }

    private record AdmissionRecord(AdmittedWriteTransfer transfer, Instant expiresAt) {
        private AdmissionRecord {
            if (transfer == null) {
                throw new IllegalArgumentException("transfer is required");
            }
            if (expiresAt == null) {
                throw new IllegalArgumentException("expiresAt is required");
            }
        }
    }
}
