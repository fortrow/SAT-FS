package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.auth.PasswordAuthProof;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.AuthProof;
import com.fortrow.cxnfs.protocol.AuthStart;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.DeletePathRequest;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.DirectoryListRequest;
import com.fortrow.cxnfs.protocol.DirectoryListResponse;
import com.fortrow.cxnfs.protocol.ErrorResponse;
import com.fortrow.cxnfs.protocol.ManifestRequest;
import com.fortrow.cxnfs.protocol.ManifestResponse;
import com.fortrow.cxnfs.protocol.MountNegotiateRequest;
import com.fortrow.cxnfs.protocol.MountNegotiatedResponse;
import com.fortrow.cxnfs.protocol.ProtocolVersion;
import com.fortrow.cxnfs.protocol.PutFileRequest;
import com.fortrow.cxnfs.protocol.ReadContainerRangeRequest;
import com.fortrow.cxnfs.protocol.ReadFileRangeRequest;
import com.fortrow.cxnfs.protocol.ReadFileRangeResponse;
import com.fortrow.cxnfs.protocol.ReconstructionPlanRequest;
import com.fortrow.cxnfs.protocol.ReconstructionPlanResponse;
import com.fortrow.cxnfs.protocol.RenamePathRequest;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.ResponseStatus;
import com.fortrow.cxnfs.protocol.StatPathRequest;
import com.fortrow.cxnfs.protocol.StatPathResponse;
import com.fortrow.cxnfs.store.PathDeleteResult;
import com.fortrow.cxnfs.store.PathRenameResult;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.transport.AdmittedReadTransfer;
import com.fortrow.cxnfs.transport.AdmittedWriteTransfer;
import com.fortrow.cxnfs.transport.BoundedPayloadStagingArea;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.CordPoolConfig;
import com.fortrow.cxnfs.transport.EntryPublishCoordinator;
import com.fortrow.cxnfs.transport.PublishedEntry;
import com.fortrow.cxnfs.transport.ReadAdmissionController;
import com.fortrow.cxnfs.transport.ReadAdmissionLimits;
import com.fortrow.cxnfs.transport.StreamingVerifiedEntryPublisher;
import com.fortrow.cxnfs.transport.StagedPayload;
import com.fortrow.cxnfs.transport.VerifiedEntryAssembler;
import com.fortrow.cxnfs.transport.VerifiedEntryPublisher;
import com.fortrow.cxnfs.transport.WriteAdmissionController;
import com.fortrow.cxnfs.transport.WriteAdmissionLimits;
import com.fortrow.cxnfs.transport.WriteAdmissionPolicy;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class ControlSessionDispatcher {
    private final CxnFileServer server;
    private final ReadAdmissionController readAdmissionController;
    private final WriteAdmissionController writeAdmissionController;
    private final BoundedPayloadStagingArea writePayloadStagingArea;
    private final EntryPublishCoordinator entryPublishCoordinator;
    private final ControlSessionLimits limits;
    private final Clock clock;
    private final Map<UUID, String> principalsBySession = new ConcurrentHashMap<>();
    private final Map<UUID, PendingAuthChallenge> pendingChallenges = new ConcurrentHashMap<>();
    private final Map<TransferEntryKey, VerifiedEntryPublisher> earlyLivePublishers = new ConcurrentHashMap<>();

    public ControlSessionDispatcher(CxnFileServer server) {
        this(server, ControlSessionLimits.defaults(), Clock.systemUTC());
    }

    ControlSessionDispatcher(CxnFileServer server, ControlSessionLimits limits, Clock clock) {
        if (server == null) {
            throw new IllegalArgumentException("server is required");
        }
        if (limits == null) {
            throw new IllegalArgumentException("limits is required");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock is required");
        }
        this.server = server;
        this.limits = limits;
        this.clock = clock;
        CordPoolConfig cordPool = server.config().containerTransferBinding()
                .cordPool()
                .orElse(CordPoolConfig.xetLikeDefaults());
        this.readAdmissionController = new ReadAdmissionController(cordPool, ReadAdmissionLimits.defaults(), clock);
        this.writeAdmissionController = new WriteAdmissionController(
                server.config().containerTransferBinding()
                        .writeAdmissionPolicy()
                        .orElse(WriteAdmissionPolicy.defaults()),
                cordPool,
                WriteAdmissionLimits.defaults(),
                clock
        );
        this.writePayloadStagingArea = new BoundedPayloadStagingArea(
                writeAdmissionController,
                server.config().payloadStagingConfig()
        );
        this.entryPublishCoordinator = new EntryPublishCoordinator(
                writeAdmissionController,
                new VerifiedEntryAssembler(writePayloadStagingArea),
                new ServerVerifiedEntryPublisher(server)
        );
    }

    public ControlEnvelope<ControlPayload> handle(ControlEnvelope<? extends ControlPayload> request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (!ProtocolVersion.CURRENT.equals(request.version())) {
            return response(request, Optional.empty(), invalid("unsupported protocol version"));
        }
        if (request.payload() instanceof AuthStart authStart) {
            return startAuthentication(request, authStart);
        }
        if (request.payload() instanceof AuthProof authProof) {
            return completeAuthentication(request, authProof);
        }

        Optional<UUID> sessionId = authenticatedSession(request);
        if (sessionId.isEmpty()) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.UNAUTHENTICATED,
                    "authenticated control session required"
            ));
        }

        try {
            releaseExpiredWriteTransfers();
            return response(request, sessionId, dispatchAuthenticated(sessionId.orElseThrow(), request.payload()));
        } catch (IOException e) {
            return response(request, sessionId, new ErrorResponse(ResponseStatus.SERVER_ERROR, e.getMessage()));
        } catch (SecurityException e) {
            return response(request, sessionId, new ErrorResponse(ResponseStatus.UNAUTHORIZED, e.getMessage()));
        } catch (IllegalArgumentException e) {
            return response(request, sessionId, new ErrorResponse(ResponseStatus.NOT_FOUND, e.getMessage()));
        } catch (IllegalStateException e) {
            return response(request, sessionId, new ErrorResponse(ResponseStatus.CONFLICT, e.getMessage()));
        } catch (UnsupportedOperationException e) {
            return response(request, sessionId, new ErrorResponse(ResponseStatus.UNSUPPORTED, e.getMessage()));
        } catch (RuntimeException e) {
            return response(request, sessionId, new ErrorResponse(ResponseStatus.SERVER_ERROR, e.getMessage()));
        }
    }

    public Optional<String> principal(UUID sessionId) {
        if (sessionId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(principalsBySession.get(sessionId));
    }

    public int authenticatedSessionCount() {
        return principalsBySession.size();
    }

    public int pendingAuthChallengeCount() {
        pruneExpiredAuthChallenges();
        return pendingChallenges.size();
    }

    public Optional<AdmittedReadTransfer> admittedReadTransfer(UUID transferId) {
        if (transferId == null) {
            return Optional.empty();
        }
        return readAdmissionController.admittedTransfer(transferId);
    }

    public Optional<AdmittedWriteTransfer> admittedWriteTransfer(UUID transferId) {
        if (transferId == null) {
            return Optional.empty();
        }
        releaseExpiredWriteTransfersUnchecked();
        return writeAdmissionController.admittedTransfer(transferId);
    }

    public StagedPayload stageWritePayload(BulkDataFrame frame) throws IOException {
        releaseExpiredWriteTransfers();
        TransferEntryKey key = new TransferEntryKey(frame.transferId(), frame.entrySequence());
        VerifiedEntryPublisher publisher = earlyLivePublishers.get(key);
        if (publisher instanceof StreamingVerifiedEntryPublisher streamingPublisher) {
            writeAdmissionController.reservePayload(frame);
            try {
                return streamingPublisher.acceptFrame(frame);
            } catch (IOException | RuntimeException e) {
                writeAdmissionController.rollbackPayloadReservation(frame);
                earlyLivePublishers.remove(key);
                abortPublisher(publisher, e);
                throw e;
            }
        }

        StagedPayload staged = writePayloadStagingArea.stage(frame);
        if (publisher != null) {
            try {
                publisher.acceptChunk(Hashing.chunk(frame.payload()), frame.entryOffset(), frame.payload());
            } catch (IOException | RuntimeException e) {
                earlyLivePublishers.remove(key);
                abortPublisher(publisher, e);
                throw e;
            }
        }
        return staged;
    }

    public int releaseExpiredWriteTransfers() throws IOException {
        int released = 0;
        for (UUID transferId : writeAdmissionController.pruneExpiredTransfers()) {
            released += writePayloadStagingArea.releaseTransfer(transferId);
            abortEarlyPublishersForTransfer(transferId);
        }
        return released;
    }

    private ControlEnvelope<ControlPayload> startAuthentication(
            ControlEnvelope<? extends ControlPayload> request,
            AuthStart authStart
    ) {
        pruneExpiredAuthChallenges();
        if (!server.config().authenticationPolicy().accepts(authStart.mechanism())) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.UNAUTHORIZED,
                    "authentication mechanism is not accepted"
            ));
        }
        if (authStart.mechanism() != AuthMechanism.USERNAME_PASSWORD) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.UNSUPPORTED,
                    "only username/password proof authentication is wired for control sessions"
            ));
        }
        if (pendingChallenges.size() >= limits.maxPendingAuthChallenges()) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.CONFLICT,
                    "too many pending authentication challenges"
            ));
        }

        UUID challengeId = UUID.randomUUID();
        byte[] nonce = PasswordAuthProof.newNonce();
        Instant expiresAt = clock.instant().plus(limits.authChallengeTtl());
        AuthChallenge challenge = new AuthChallenge(
                challengeId,
                authStart.mechanism(),
                authStart.principal(),
                nonce,
                PasswordAuthProof.PROOF_ALGORITHM,
                PasswordAuthProof.DEFAULT_ITERATIONS
        );
        pendingChallenges.put(challengeId, new PendingAuthChallenge(
                authStart.mechanism(),
                authStart.principal(),
                challenge.nonce(),
                challenge.iterations(),
                expiresAt
        ));
        return response(request, Optional.empty(), challenge);
    }

    private ControlEnvelope<ControlPayload> completeAuthentication(
            ControlEnvelope<? extends ControlPayload> request,
            AuthProof authProof
    ) {
        pruneExpiredAuthChallenges();
        PendingAuthChallenge challenge = pendingChallenges.remove(authProof.challengeId());
        if (challenge == null || !challenge.principal().equals(authProof.principal())) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.UNAUTHENTICATED,
                    "authentication challenge is missing or expired"
            ));
        }
        if (principalsBySession.size() >= limits.maxAuthenticatedSessions()) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.CONFLICT,
                    "too many authenticated control sessions"
            ));
        }

        boolean accepted = switch (challenge.mechanism()) {
            case USERNAME_PASSWORD -> server.config().authenticationPolicy()
                    .authenticateUsernamePasswordProof(
                            authProof.principal(),
                            challenge.nonce(),
                            challenge.iterations(),
                            authProof.proof()
                    );
            case BEARER_TOKEN -> false;
        };
        if (!accepted) {
            return response(request, Optional.empty(), new ErrorResponse(
                    ResponseStatus.UNAUTHENTICATED,
                    "authentication failed"
            ));
        }

        UUID sessionId = UUID.randomUUID();
        principalsBySession.put(sessionId, authProof.principal());
        return response(request, Optional.of(sessionId), new AuthAccepted(
                sessionId,
                server.config().authenticationPolicy().acceptedMechanisms()
        ));
    }

    private ControlPayload dispatchAuthenticated(UUID sessionId, ControlPayload payload) throws IOException {
        String principal = principalsBySession.get(sessionId);
        if (payload instanceof DirectoryListRequest value) {
            requireTraverse(principal, value.path());
            return new DirectoryListResponse(filterListing(principal, server.list(value.path())));
        }
        if (payload instanceof StatPathRequest value) {
            requireTraverse(principal, value.path());
            return new StatPathResponse(server.stat(value.path()));
        }
        if (payload instanceof ManifestRequest value) {
            Optional<FileManifest> manifest = server.manifest(value.fileHash());
            manifest.ifPresent(valueManifest -> requireAccess(principal, valueManifest.path()));
            return new ManifestResponse(manifest);
        }
        if (payload instanceof ReconstructionPlanRequest value) {
            server.manifest(value.fileHash()).ifPresent(valueManifest -> requireAccess(principal, valueManifest.path()));
            return new ReconstructionPlanResponse(server.reconstructionPlan(value.fileHash()));
        }
        if (payload instanceof ReadFileRangeRequest value) {
            requireAccess(principal, value.path());
            ReadPlan plan = server.readPlan(value.path(), value.offset(), value.length());
            return new ReadFileRangeResponse(value.path(), plan);
        }
        if (payload instanceof MountNegotiateRequest value) {
            return negotiateMount(value);
        }
        if (payload instanceof PutFileRequest value) {
            requireAccess(principal, value.path());
            return server.preparePutFile(value);
        }
        if (payload instanceof RenamePathRequest value) {
            return renamePath(principal, value);
        }
        if (payload instanceof DeletePathRequest value) {
            return deletePath(principal, value);
        }
        if (payload instanceof TransferBootstrapRequest value) {
            return bootstrapTransfer(sessionId, value);
        }
        if (payload instanceof TransferEntryUpdate value) {
            requireAccess(principal, value.path());
            if (!value.targetPath().isBlank()) {
                requireAccess(principal, value.targetPath());
            }
            writeAdmissionController.acceptEntryMetadata(value);
            beginEarlyLivePublish(value);
            return new TransferEntryAcceptedResponse(value.transferId(), value.entrySequence(), value.path());
        }
        if (payload instanceof TransferMetadataUpdate value) {
            return acceptTransferMetadata(value);
        }
        if (payload instanceof TransferEntryCommitRequest value) {
            requireAccess(principal, value.path());
            return commitEntry(value).response();
        }
        if (payload instanceof TransferCommitRequest value) {
            requireAccess(principal, value.path());
            return commitTransfer(value);
        }
        if (payload instanceof ReadContainerRangeRequest) {
            throw new UnsupportedOperationException("operation is reserved until the network transfer loop is wired");
        }
        throw new UnsupportedOperationException("operation is not handled by the control dispatcher: "
                + payload.operation());
    }

    private DeletePathResponse deletePath(String principal, DeletePathRequest request) {
        requireAccess(principal, request.path());
        try {
            PathDeleteResult result = server.deletePath(request.path(), request.recursive());
            return new DeletePathResponse(result.path(), result.deletedEntries());
        } catch (IOException e) {
            throw new RuntimeException("delete path failed: " + e.getMessage(), e);
        }
    }

    private RenamePathResponse renamePath(String principal, RenamePathRequest request) {
        requireAccess(principal, request.sourcePath());
        requireAccess(principal, request.targetPath());
        try {
            PathRenameResult result = server.renamePath(request.sourcePath(), request.targetPath());
            return new RenamePathResponse(result.sourcePath(), result.targetPath(), result.metadata());
        } catch (IOException e) {
            throw new RuntimeException("rename path failed: " + e.getMessage(), e);
        }
    }

    private TransferAcceptedResponse bootstrapTransfer(UUID sessionId, TransferBootstrapRequest request) {
        String principal = principalsBySession.get(sessionId);
        requireAccess(principal, request.path());
        if (request.direction() == TransferDirection.WRITE) {
            if (readAdmissionController.admittedTransfer(request.transferId()).isPresent()) {
                throw new IllegalStateException("transfer id is already admitted for read: " + request.transferId());
            }
            return writeAdmissionController.admit(sessionId, principal, request);
        }
        if (request.direction() != TransferDirection.READ) {
            throw new UnsupportedOperationException("unsupported transfer direction: " + request.direction());
        }
        if (writeAdmissionController.admittedTransfer(request.transferId()).isPresent()) {
            throw new IllegalStateException("transfer id is already admitted for write: " + request.transferId());
        }
        FileMetadata metadata = server.stat(request.path())
                .orElseThrow(() -> new IllegalArgumentException("unknown file path: " + request.path()));
        if (metadata.type() != FileType.REGULAR_FILE) {
            throw new IllegalArgumentException("read transfers require a regular file path: " + request.path());
        }
        if (request.expectedSizeLimitBytes() > 0 && metadata.size() > request.expectedSizeLimitBytes()) {
            throw new IllegalArgumentException("file exceeds expected transfer size limit: " + request.path());
        }

        return readAdmissionController.admit(sessionId, principal, request);
    }

    private TransferMetadataAcceptedResponse acceptTransferMetadata(TransferMetadataUpdate update) {
        try {
            TransferEntryKey key = new TransferEntryKey(update.transferId(), update.entrySequence());
            VerifiedEntryPublisher publisher = earlyLivePublishers.get(key);
            if (publisher instanceof StreamingVerifiedEntryPublisher streamingPublisher) {
                int verified;
                try {
                    verified = streamingPublisher.acceptMetadata(update);
                } catch (IOException | RuntimeException e) {
                    earlyLivePublishers.remove(key);
                    abortPublisher(publisher, e);
                    throw e;
                }
                return new TransferMetadataAcceptedResponse(
                        update.transferId(),
                        update.entrySequence(),
                        update.metadataSequence(),
                        update.chunkHashes().size(),
                        verified,
                        update.finalMetadata()
                );
            }
            int verified = writePayloadStagingArea.acceptMetadata(update);
            return new TransferMetadataAcceptedResponse(
                    update.transferId(),
                    update.entrySequence(),
                    update.metadataSequence(),
                    update.chunkHashes().size(),
                    verified,
                    update.finalMetadata()
            );
        } catch (IOException e) {
            throw new IllegalStateException("transfer metadata verification failed: " + e.getMessage(), e);
        }
    }

    private PublishedEntry commitEntry(TransferEntryCommitRequest request) {
        try {
            TransferEntryKey key = new TransferEntryKey(request.transferId(), request.entrySequence());
            VerifiedEntryPublisher publisher = earlyLivePublishers.get(key);
            if (publisher instanceof StreamingVerifiedEntryPublisher streamingPublisher) {
                return entryPublishCoordinator.publishStreaming(request, streamingPublisher);
            }
            return entryPublishCoordinator.publish(request);
        } catch (IOException e) {
            throw new IllegalStateException("entry verification failed: " + e.getMessage(), e);
        } finally {
            earlyLivePublishers.remove(new TransferEntryKey(request.transferId(), request.entrySequence()));
        }
    }

    private ControlPayload commitTransfer(TransferCommitRequest request) {
        try {
            ControlPayload response = writeAdmissionController.commit(request);
            writePayloadStagingArea.releaseTransfer(request.transferId());
            abortEarlyPublishersForTransfer(request.transferId());
            return response;
        } catch (IOException e) {
            throw new IllegalStateException("failed to release staged write transfer: " + e.getMessage(), e);
        }
    }

    private void beginEarlyLivePublish(TransferEntryUpdate update) throws IOException {
        if (!server.isServedPath(update.path())) {
            return;
        }
        TransferEntryKey key = new TransferEntryKey(update.transferId(), update.entrySequence());
        if (earlyLivePublishers.containsKey(key)) {
            return;
        }
        earlyLivePublishers.put(key, server.verifiedEntryPublisher(update.path(), update.sizeHintBytes()));
    }

    private void abortEarlyPublishersForTransfer(UUID transferId) throws IOException {
        IOException failure = null;
        for (Map.Entry<TransferEntryKey, VerifiedEntryPublisher> entry : earlyLivePublishers.entrySet()) {
            if (!entry.getKey().transferId().equals(transferId)) {
                continue;
            }
            VerifiedEntryPublisher publisher = earlyLivePublishers.remove(entry.getKey());
            if (publisher == null) {
                continue;
            }
            try {
                publisher.abort();
            } catch (IOException e) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void abortPublisher(VerifiedEntryPublisher publisher, Exception e) {
        try {
            publisher.abort();
        } catch (IOException abortFailure) {
            e.addSuppressed(abortFailure);
        }
    }

    private MountNegotiatedResponse negotiateMount(MountNegotiateRequest request) {
        MountStrategy strategy = strategyFor(request.backend());
        if (!strategy.capabilities().containsAll(request.requestedCapabilities())) {
            throw new UnsupportedOperationException("requested mount capabilities are not supported by "
                    + request.backend());
        }
        return new MountNegotiatedResponse(strategy);
    }

    private DirectoryListing filterListing(String principal, DirectoryListing listing) {
        if (server.config().authenticationPolicy().directoriesByUsername().get(principal) == null) {
            return listing;
        }
        return new DirectoryListing(
                listing.path(),
                listing.entries()
                        .stream()
                        .filter(entry -> server.config().authenticationPolicy().canTraversePath(principal, entry.path()))
                        .toList()
        );
    }

    private void requireAccess(String principal, String path) {
        if (!server.config().authenticationPolicy().canAccessPath(principal, path)) {
            throw new SecurityException("principal is not authorized for path: " + path);
        }
    }

    private void requireTraverse(String principal, String path) {
        if (!server.config().authenticationPolicy().canTraversePath(principal, path)) {
            throw new SecurityException("principal is not authorized for path: " + path);
        }
    }

    private Optional<UUID> authenticatedSession(ControlEnvelope<? extends ControlPayload> request) {
        Optional<UUID> sessionId = request.sessionId();
        return sessionId.filter(principalsBySession::containsKey);
    }

    private void releaseExpiredWriteTransfersUnchecked() {
        try {
            releaseExpiredWriteTransfers();
        } catch (IOException e) {
            throw new IllegalStateException("failed to release expired write transfers: " + e.getMessage(), e);
        }
    }

    private void pruneExpiredAuthChallenges() {
        Instant now = clock.instant();
        pendingChallenges.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
    }

    private static MountStrategy strategyFor(MountBackend backend) {
        return switch (backend) {
            case LINUX_FUSE -> MountStrategy.linuxFuse();
            case MACOS_FUSE_T -> MountStrategy.macFuseT();
            case MACOS_MACFUSE -> MountStrategy.macFuse();
            case WINDOWS_WINFSP -> MountStrategy.windowsWinFsp();
            case WINDOWS_PROJFS -> MountStrategy.windowsProjectionListRead();
            case CLI_LIST_ONLY -> MountStrategy.listOnlyFallback();
        };
    }

    private static ErrorResponse invalid(String message) {
        return new ErrorResponse(ResponseStatus.INVALID_REQUEST, message);
    }

    private static ControlEnvelope<ControlPayload> response(
            ControlEnvelope<? extends ControlPayload> request,
            Optional<UUID> sessionId,
            ControlPayload payload
    ) {
        return new ControlEnvelope<>(request.version(), UUID.randomUUID(), sessionId, payload);
    }

    private record PendingAuthChallenge(
            AuthMechanism mechanism,
            String principal,
            byte[] nonce,
            int iterations,
            Instant expiresAt
    ) {
        private PendingAuthChallenge {
            nonce = nonce.clone();
            if (expiresAt == null) {
                throw new IllegalArgumentException("expiresAt is required");
            }
        }

        @Override
        public byte[] nonce() {
            return nonce.clone();
        }
    }

    private record TransferEntryKey(UUID transferId, int entrySequence) {
        private TransferEntryKey {
            if (transferId == null) {
                throw new IllegalArgumentException("transferId is required");
            }
            if (entrySequence < 0) {
                throw new IllegalArgumentException("entrySequence must be non-negative");
            }
        }
    }

    private final class ServerVerifiedEntryPublisher
            implements com.fortrow.cxnfs.transport.VerifiedEntryPublisherFactory {
        private final CxnFileServer server;

        private ServerVerifiedEntryPublisher(CxnFileServer server) {
            if (server == null) {
                throw new IllegalArgumentException("server is required");
            }
            this.server = server;
        }

        @Override
        public VerifiedEntryPublisher forEntry(TransferEntryCommitRequest request) throws IOException {
            VerifiedEntryPublisher earlyPublisher = earlyLivePublishers.get(new TransferEntryKey(
                    request.transferId(),
                    request.entrySequence()
            ));
            if (earlyPublisher != null) {
                return earlyPublisher;
            }
            return server.verifiedEntryPublisher(request.path());
        }
    }
}
