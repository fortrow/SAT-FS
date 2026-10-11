package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public final class ReadAdmissionController {
    private static final long DEFAULT_FLOW_CONTROL_WINDOW_BYTES = 128L * 1024 * 1024;

    private final CordPoolConfig cordPool;
    private final ReadAdmissionLimits limits;
    private final Clock clock;
    private final Map<UUID, AdmissionRecord> admittedTransfers = new LinkedHashMap<>();
    private Optional<UUID> sessionId = Optional.empty();
    private Optional<String> principal = Optional.empty();

    public ReadAdmissionController() {
        this(CordPoolConfig.xetLikeDefaults());
    }

    public ReadAdmissionController(CordPoolConfig cordPool) {
        this(cordPool, ReadAdmissionLimits.defaults(), Clock.systemUTC());
    }

    public ReadAdmissionController(CordPoolConfig cordPool, ReadAdmissionLimits limits, Clock clock) {
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        if (limits == null) {
            throw new IllegalArgumentException("limits is required");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock is required");
        }
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
        if (sessionId.isEmpty()) {
            throw new IllegalStateException("control session must be authenticated before read admission");
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
        if (request.direction() != TransferDirection.READ) {
            throw new IllegalArgumentException("read admission only accepts read transfers");
        }
        pruneExpiredTransfers();
        if (admittedTransfers.containsKey(request.transferId())) {
            throw new IllegalStateException("read transfer is already admitted: " + request.transferId());
        }
        if (admittedTransfers.size() >= limits.maxAdmittedReadTransfers()) {
            throw new IllegalStateException("too many admitted read transfers");
        }

        int acceptedMaxCords = Math.min(request.maxCords(), cordPool.maxCords());
        int acceptedInitialCords = cordPool.adaptive()
                ? Math.min(request.initialCords(), acceptedMaxCords)
                : acceptedMaxCords;
        AdmittedReadTransfer admitted = new AdmittedReadTransfer(
                request.transferId(),
                sessionId,
                principal,
                request.path(),
                request.payloadFormat(),
                acceptedInitialCords,
                acceptedMaxCords,
                request.expectedSizeLimitBytes()
        );
        admittedTransfers.put(
                request.transferId(),
                new AdmissionRecord(admitted, clock.instant().plus(limits.readTransferTtl()))
        );
        return new TransferAcceptedResponse(
                request.transferId(),
                TransferDirection.READ,
                acceptedInitialCords,
                acceptedMaxCords,
                DEFAULT_FLOW_CONTROL_WINDOW_BYTES,
                false
        );
    }

    public synchronized Optional<AdmittedReadTransfer> admittedTransfer(UUID transferId) {
        pruneExpiredTransfers();
        return Optional.ofNullable(admittedTransfers.get(transferId)).map(AdmissionRecord::transfer);
    }

    public synchronized AdmittedReadTransfer requireAdmitted(UUID transferId) {
        return admittedTransfer(transferId)
                .orElseThrow(() -> new IllegalStateException("read transfer is not admitted: " + transferId));
    }

    public synchronized int admittedTransferCount() {
        pruneExpiredTransfers();
        return admittedTransfers.size();
    }

    private void pruneExpiredTransfers() {
        Instant now = clock.instant();
        admittedTransfers.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
    }

    private record AdmissionRecord(AdmittedReadTransfer transfer, Instant expiresAt) {
        private AdmissionRecord {
            if (expiresAt == null) {
                throw new IllegalArgumentException("expiresAt is required");
            }
        }
    }
}
