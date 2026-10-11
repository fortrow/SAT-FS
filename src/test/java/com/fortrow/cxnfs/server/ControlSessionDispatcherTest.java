package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.client.ClientAuthenticator;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlFrame;
import com.fortrow.cxnfs.protocol.ControlFrameCodec;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.ControlPayloadCodec;
import com.fortrow.cxnfs.protocol.DirectoryListRequest;
import com.fortrow.cxnfs.protocol.DirectoryListResponse;
import com.fortrow.cxnfs.protocol.DeletePathRequest;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.ErrorResponse;
import com.fortrow.cxnfs.protocol.ManifestRequest;
import com.fortrow.cxnfs.protocol.ManifestResponse;
import com.fortrow.cxnfs.protocol.MountNegotiateRequest;
import com.fortrow.cxnfs.protocol.MountNegotiatedResponse;
import com.fortrow.cxnfs.protocol.PutFileRequest;
import com.fortrow.cxnfs.protocol.PutFileResponse;
import com.fortrow.cxnfs.protocol.ReadFileRangeRequest;
import com.fortrow.cxnfs.protocol.ReadFileRangeResponse;
import com.fortrow.cxnfs.protocol.ReconstructionPlanRequest;
import com.fortrow.cxnfs.protocol.ReconstructionPlanResponse;
import com.fortrow.cxnfs.protocol.RenamePathRequest;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.ResponseStatus;
import com.fortrow.cxnfs.protocol.StatPathRequest;
import com.fortrow.cxnfs.protocol.StatPathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryAction;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.PayloadStagingConfig;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ControlSessionDispatcherTest {
    @Test
    void authenticatesBeforeDispatchingCoreReadOperations() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());

        ErrorResponse unauthenticated = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.unauthenticated(new DirectoryListRequest("/datasets")))
                .payload();
        AuthAccepted accepted = authenticate(dispatcher);
        UUID sessionId = accepted.sessionId();

        DirectoryListResponse listing = (DirectoryListResponse) dispatcher
                .handle(ControlEnvelope.authenticated(sessionId, new DirectoryListRequest("/datasets/train")))
                .payload();
        StatPathResponse stat = (StatPathResponse) dispatcher
                .handle(ControlEnvelope.authenticated(sessionId, new StatPathRequest("/datasets/train/shard.mds")))
                .payload();
        ManifestResponse manifest = (ManifestResponse) dispatcher
                .handle(ControlEnvelope.authenticated(sessionId, new ManifestRequest(fixture.manifest().fileHash())))
                .payload();
        ReconstructionPlanResponse reconstruction = (ReconstructionPlanResponse) dispatcher
                .handle(ControlEnvelope.authenticated(sessionId, new ReconstructionPlanRequest(fixture.manifest().fileHash())))
                .payload();
        ReadFileRangeResponse range = (ReadFileRangeResponse) dispatcher
                .handle(ControlEnvelope.authenticated(sessionId, new ReadFileRangeRequest("/datasets/train/shard.mds", 7, 96)))
                .payload();
        MountNegotiatedResponse mount = (MountNegotiatedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        sessionId,
                        new MountNegotiateRequest(MountBackend.MACOS_FUSE_T, Set.of(MountCapability.LIST, MountCapability.READ))
                ))
                .payload();

        assertEquals(ResponseStatus.UNAUTHENTICATED, unauthenticated.status());
        assertEquals(Optional.of("trainer"), dispatcher.principal(sessionId));
        assertEquals(1, dispatcher.authenticatedSessionCount());
        assertEquals("shard.mds", listing.listing().entries().getFirst().name());
        assertEquals(fixture.manifest().fileHash(), stat.metadata().orElseThrow().contentHash().orElseThrow());
        assertEquals(fixture.manifest(), manifest.manifest().orElseThrow());
        assertEquals(fixture.manifest().fileHash(), reconstruction.plan().fileHash());
        assertEquals(fixture.manifest().fileHash(), range.plan().fileHash());
        assertEquals(7, range.plan().requestedRange().offset());
        assertEquals(96, range.plan().requestedRange().length());
        assertEquals(MountBackend.MACOS_FUSE_T, mount.strategy().backend());
    }

    @Test
    void rejectsBadCredentialsAndMissingReadPaths() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());

        ErrorResponse rejected = (ErrorResponse) authenticateWithPassword(dispatcher, "bad");
        AuthAccepted accepted = authenticate(dispatcher);
        ErrorResponse missing = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new ReadFileRangeRequest("/datasets/train/missing.mds", 0, 64)
                ))
                .payload();

        assertEquals(ResponseStatus.UNAUTHENTICATED, rejected.status());
        assertEquals(ResponseStatus.NOT_FOUND, missing.status());
    }

    @Test
    void enforcesAuthenticatedPrincipalDirectoryJails() throws Exception {
        Fixture fixture = fixture(AuthenticationPolicy.requireUsernamePassword(
                Map.of("trainer", "secret"),
                Map.of("trainer", List.of("/datasets"))
        ));
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        FileManifest outputManifest = fixture.server()
                .manifest("/outputs/run-1/model.bin")
                .orElseThrow();

        DirectoryListResponse rootListing = (DirectoryListResponse) dispatcher
                .handle(ControlEnvelope.authenticated(accepted.sessionId(), new DirectoryListRequest("/")))
                .payload();
        StatPathResponse allowedStat = (StatPathResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new StatPathRequest("/datasets/train/shard.mds")
                ))
                .payload();
        ErrorResponse deniedStat = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new StatPathRequest("/outputs/run-1/model.bin")
                ))
                .payload();
        ErrorResponse deniedManifest = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new ManifestRequest(outputManifest.fileHash())
                ))
                .payload();
        ErrorResponse deniedRead = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new ReadFileRangeRequest("/outputs/run-1/model.bin", 0, 8)
                ))
                .payload();
        ErrorResponse deniedWrite = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferBootstrapRequest(
                                UUID.randomUUID(),
                                TransferDirection.WRITE,
                                "/outputs/run-2",
                                TransferPayloadFormat.SAFETENSORS,
                                8,
                                64,
                                64 * 1024 * 1024,
                                1024
                        )
                ))
                .payload();

        assertEquals(List.of("/datasets"), rootListing.listing().entries().stream().map(FileMetadata::path).toList());
        assertEquals(fixture.manifest().fileHash(), allowedStat.metadata().orElseThrow().contentHash().orElseThrow());
        assertEquals(ResponseStatus.UNAUTHORIZED, deniedStat.status());
        assertEquals(ResponseStatus.UNAUTHORIZED, deniedManifest.status());
        assertEquals(ResponseStatus.UNAUTHORIZED, deniedRead.status());
        assertEquals(ResponseStatus.UNAUTHORIZED, deniedWrite.status());
    }

    @Test
    void mutatesNamespaceAfterAuthentication() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);

        RenamePathResponse rename = (RenamePathResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new RenamePathRequest("/datasets/train/shard.mds", "/datasets/train/renamed.mds")
                ))
                .payload();
        DeletePathResponse delete = (DeletePathResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new DeletePathRequest("/datasets/train/renamed.mds", false)
                ))
                .payload();

        assertEquals("/datasets/train/renamed.mds", rename.targetPath());
        assertEquals(fixture.manifest().fileHash(), rename.metadata().contentHash().orElseThrow());
        assertEquals(1, delete.deletedEntries());
        assertEquals(Optional.empty(), ((StatPathResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new StatPathRequest("/datasets/train/renamed.mds")
                ))
                .payload()).metadata());
        assertEquals(Optional.empty(), ((StatPathResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new StatPathRequest("/datasets/train/shard.mds")
                ))
                .payload()).metadata());
    }

    @Test
    void putFilePreflightReturnsMissingChunksWithoutPublishingManifest() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        ChunkRef existing = fixture.manifest().chunks().getFirst();
        byte[] missingBytes = "new checkpoint chunk".getBytes(StandardCharsets.UTF_8);
        ChunkRef missing = new ChunkRef(Hashing.chunk(missingBytes), missingBytes.length);
        List<ChunkRef> chunks = List.of(existing, missing, missing);
        ContentHash fileHash = Hashing.file(
                chunks.stream().map(ChunkRef::hash).toList(),
                chunks.stream().map(ChunkRef::size).toList()
        );
        long size = chunks.stream().mapToLong(ChunkRef::size).sum();

        PutFileResponse response = (PutFileResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new PutFileRequest(
                                "outputs/checkpoint/model.safetensors",
                                size,
                                fileHash,
                                chunks
                        )
                ))
                .payload();

        assertEquals("/outputs/checkpoint/model.safetensors", response.manifest().path());
        assertEquals(fileHash, response.manifest().fileHash());
        assertEquals(size, response.manifest().size());
        assertEquals(List.of(missing.hash()), response.missingChunks());
        assertTrue(fixture.server().manifest("/outputs/checkpoint/model.safetensors").isEmpty());
    }

    @Test
    void putFilePreflightRejectsInvalidManifestHash() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        ChunkRef chunk = fixture.manifest().chunks().getFirst();

        ErrorResponse response = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new PutFileRequest(
                                "/outputs/bad.safetensors",
                                chunk.size(),
                                Hashing.chunk("wrong".getBytes(StandardCharsets.UTF_8)),
                                List.of(chunk)
                        )
                ))
                .payload();

        assertEquals(ResponseStatus.NOT_FOUND, response.status());
        assertTrue(response.message().contains("hash does not match"));
    }

    @Test
    void admitsReadTransferBootstrapForAuthenticatedSession() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();

        TransferAcceptedResponse response = (TransferAcceptedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferBootstrapRequest(
                                transferId,
                                TransferDirection.READ,
                                "/datasets/train/shard.mds",
                                TransferPayloadFormat.MDS_SHARDS,
                                8,
                                64,
                                64 * 1024 * 1024,
                                fixture.manifest().size()
                        )
                ))
                .payload();

        assertEquals(transferId, response.transferId());
        assertEquals(TransferDirection.READ, response.direction());
        assertEquals(8, response.acceptedInitialCords());
        assertEquals(64, response.acceptedMaxCords());
        assertFalse(response.writePayloadAdmissionGranted());
        assertEquals(Optional.of(accepted.sessionId()), dispatcher.admittedReadTransfer(transferId).map(t -> t.sessionId()));
        assertEquals(Optional.of("trainer"), dispatcher.admittedReadTransfer(transferId).map(t -> t.principal()));
    }

    @Test
    void expiresReadTransfersWithDispatcherClock() throws Exception {
        Fixture fixture = fixture();
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(
                fixture.server(),
                ControlSessionLimits.defaults(),
                clock
        );
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.READ,
                        "/datasets/train/shard.mds",
                        TransferPayloadFormat.MDS_SHARDS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        fixture.manifest().size()
                )
        ));
        assertTrue(dispatcher.admittedReadTransfer(transferId).isPresent());

        clock.advance(Duration.ofMinutes(11));

        assertEquals(Optional.empty(), dispatcher.admittedReadTransfer(transferId));
    }

    @Test
    void admitsWriteTransferBootstrapForAuthenticatedTreeBeforePayloadBuffering() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();

        TransferAcceptedResponse response = (TransferAcceptedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferBootstrapRequest(
                                transferId,
                                TransferDirection.WRITE,
                                "outputs/checkpoint",
                                TransferPayloadFormat.SAFETENSORS,
                                16,
                                64,
                                64 * 1024 * 1024,
                                5L * 1024 * 1024 * 1024
                        )
                ))
                .payload();

        assertEquals(transferId, response.transferId());
        assertEquals(TransferDirection.WRITE, response.direction());
        assertEquals(16, response.acceptedInitialCords());
        assertEquals(64, response.acceptedMaxCords());
        assertTrue(response.writePayloadAdmissionGranted());
        assertEquals(Optional.of(accepted.sessionId()), dispatcher.admittedWriteTransfer(transferId).map(t -> t.sessionId()));
        assertEquals(Optional.of("trainer"), dispatcher.admittedWriteTransfer(transferId).map(t -> t.principal()));
        assertEquals(Optional.of("/outputs/checkpoint"), dispatcher.admittedWriteTransfer(transferId).map(t -> t.rootPath()));
        assertEquals(Optional.empty(), dispatcher.admittedReadTransfer(transferId));
    }

    @Test
    void rejectsDuplicateTransferIdsAcrossDirections() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.READ,
                        "/datasets/train/shard.mds",
                        TransferPayloadFormat.MDS_SHARDS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        fixture.manifest().size()
                )
        ));
        ErrorResponse writeRejected = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferBootstrapRequest(
                                transferId,
                                TransferDirection.WRITE,
                                "/outputs/checkpoint",
                                TransferPayloadFormat.SAFETENSORS,
                                8,
                                64,
                                64 * 1024 * 1024,
                                1024 * 1024
                        )
                ))
                .payload();

        assertEquals(ResponseStatus.CONFLICT, writeRejected.status());
        assertTrue(writeRejected.message().contains("already admitted for read"));
        assertTrue(dispatcher.admittedReadTransfer(transferId).isPresent());
        assertEquals(Optional.empty(), dispatcher.admittedWriteTransfer(transferId));
    }

    @Test
    void rejectsDuplicateTransferIdsWithinWriteDirection() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/checkpoint",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        ErrorResponse duplicate = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferBootstrapRequest(
                                transferId,
                                TransferDirection.WRITE,
                                "/outputs/other",
                                TransferPayloadFormat.SAFETENSORS,
                                8,
                                64,
                                64 * 1024 * 1024,
                                1024 * 1024
                        )
                ))
                .payload();

        assertEquals(ResponseStatus.CONFLICT, duplicate.status());
        assertTrue(duplicate.message().contains("already admitted"));
        assertEquals(Optional.of("/outputs/checkpoint"), dispatcher.admittedWriteTransfer(transferId).map(t -> t.rootPath()));
    }

    @Test
    void acceptsLateWriteEntryAndChunkMetadataAfterWriteAdmission() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        byte[] payload = "checkpoint payload".getBytes(StandardCharsets.UTF_8);

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/run-1",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        TransferEntryAcceptedResponse entryAccepted = (TransferEntryAcceptedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferEntryUpdate(
                                transferId,
                                0,
                                TransferEntryAction.CREATE_OR_REPLACE,
                                "/outputs/run-1/model.safetensors",
                                "",
                                payload.length,
                                TransferPayloadFormat.SAFETENSORS
                        )
                ))
                .payload();
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));
        TransferMetadataAcceptedResponse metadataAccepted = (TransferMetadataAcceptedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferMetadataUpdate(
                                transferId,
                                0,
                                0,
                                true,
                                payload.length,
                                List.of(Hashing.chunk(payload))
                        )
                ))
                .payload();

        assertEquals(transferId, entryAccepted.transferId());
        assertEquals("/outputs/run-1/model.safetensors", entryAccepted.path());
        assertEquals(1, metadataAccepted.chunkHashesAccepted());
        assertEquals(1, metadataAccepted.stagedPayloadsVerified());
        assertTrue(metadataAccepted.finalMetadata());
    }

    @Test
    void commitsVerifiedWriteEntriesAndThenClosesTransferAdmission() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        byte[] payload = "checkpoint payload".getBytes(StandardCharsets.UTF_8);
        ContentHash chunkHash = Hashing.chunk(payload);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(payload.length));
        ContentHash transferHash = Hashing.transfer(List.of(fileHash), List.of((long) payload.length));

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/run-1",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferMetadataUpdate(transferId, 0, 0, true, payload.length, List.of(chunkHash))
        ));

        TransferEntryCommittedResponse entryCommitted = (TransferEntryCommittedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferEntryCommitRequest(
                                transferId,
                                0,
                                "/outputs/run-1/model.safetensors",
                                fileHash,
                                payload.length
                        )
                ))
                .payload();
        TransferCommittedResponse committed = (TransferCommittedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferCommitRequest(transferId, "/outputs/run-1", transferHash, payload.length)
                ))
                .payload();

        assertEquals(fileHash, entryCommitted.contentHash());
        assertEquals(transferHash, committed.contentHash());
        assertEquals(fileHash, fixture.server().stat("/outputs/run-1/model.safetensors")
                .orElseThrow()
                .contentHash()
                .orElseThrow());
        assertEquals(fileHash, fixture.server().manifest("/outputs/run-1/model.safetensors").orElseThrow().fileHash());
        assertEquals(
                "checkpoint payload",
                new String(
                        fixture.server().readRange("/outputs/run-1/model.safetensors", 0, payload.length),
                        StandardCharsets.UTF_8
                )
        );
        assertEquals(Optional.empty(), dispatcher.admittedWriteTransfer(transferId));
    }

    @Test
    void spillsLargeAdmittedWritePayloadsAndPublishesAfterMetadata(@TempDir Path tempDir) throws Exception {
        Path spillDirectory = tempDir.resolve("payload-spill");
        Fixture fixture = fixture(PayloadStagingConfig.withSpill(0, spillDirectory));
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        byte[] first = "checkpoint chunk one".getBytes(StandardCharsets.UTF_8);
        byte[] second = "checkpoint chunk two".getBytes(StandardCharsets.UTF_8);
        ContentHash firstHash = Hashing.chunk(first);
        ContentHash secondHash = Hashing.chunk(second);
        ContentHash fileHash = Hashing.file(List.of(firstHash, secondHash), List.of(first.length, second.length));
        ContentHash transferHash = Hashing.transfer(List.of(fileHash), List.of((long) first.length + second.length));

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/run-2",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 0, 0, 0, 0, true, first));
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 1, 0, 1, first.length, true, second));
        assertEquals(2, countRegularFiles(spillDirectory));

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferMetadataUpdate(
                        transferId,
                        0,
                        0,
                        true,
                        first.length + second.length,
                        List.of(firstHash, secondHash)
                )
        ));
        TransferEntryCommittedResponse entryCommitted = (TransferEntryCommittedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferEntryCommitRequest(
                                transferId,
                                0,
                                "/outputs/run-2/model.safetensors",
                                fileHash,
                                first.length + second.length
                        )
                ))
                .payload();
        TransferCommittedResponse transferCommitted = (TransferCommittedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferCommitRequest(
                                transferId,
                                "/outputs/run-2",
                                transferHash,
                                first.length + second.length
                        )
                ))
                .payload();

        assertEquals(fileHash, entryCommitted.contentHash());
        assertEquals(transferHash, transferCommitted.contentHash());
        assertEquals(0, countRegularFiles(spillDirectory));
        assertEquals(
                "checkpoint chunk onecheckpoint chunk two",
                new String(
                        fixture.server().readRange("/outputs/run-2/model.safetensors", 0, first.length + second.length),
                        StandardCharsets.UTF_8
                )
        );
    }

    @Test
    void liveServedRootWritesStartTempFileBeforeEntryCommit(@TempDir Path tempDir) throws Exception {
        Path source = tempDir.resolve("share");
        Path spillDirectory = tempDir.resolve("payload-spill");
        Files.createDirectories(source);
        Files.createDirectories(spillDirectory);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, repository, Duration.ZERO);
        refresher.refreshNow();
        CxnFileServer server = new CxnFileServer(
                config(roots, PayloadStagingConfig.withSpill(0, spillDirectory)),
                repository,
                refresher
        );
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(server);
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        byte[] payload = "early live write\n".getBytes(StandardCharsets.UTF_8);
        ContentHash chunkHash = Hashing.chunk(payload);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(payload.length));

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/share/hello.txt",
                        TransferPayloadFormat.CONTAINER,
                        1,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferEntryUpdate(
                        transferId,
                        0,
                        TransferEntryAction.CREATE_OR_REPLACE,
                        "/share/hello.txt",
                        "",
                        payload.length,
                        TransferPayloadFormat.CONTAINER
                )
        ));
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));

        waitForTempWrite(source, "hello.txt.tmp", payload);

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferMetadataUpdate(transferId, 0, 0, true, payload.length, List.of(chunkHash))
        ));
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferEntryCommitRequest(transferId, 0, "/share/hello.txt", fileHash, payload.length)
        ));
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferCommitRequest(transferId, "/share/hello.txt", Hashing.transfer(List.of(fileHash), List.of((long) payload.length)), payload.length)
        ));

        assertEquals("early live write\n", Files.readString(source.resolve("hello.txt"), StandardCharsets.UTF_8));
        assertEquals(0, countRegularFiles(spillDirectory));
        assertEquals(0, repository.containerCount());
        assertEquals(0, repository.chunkCount());
    }

    @Test
    void transferCommitMustMatchVerifiedCommittedEntries() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        byte[] payload = "checkpoint payload".getBytes(StandardCharsets.UTF_8);
        ContentHash chunkHash = Hashing.chunk(payload);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(payload.length));

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/run-1",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferMetadataUpdate(transferId, 0, 0, true, payload.length, List.of(chunkHash))
        ));
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferEntryCommitRequest(
                        transferId,
                        0,
                        "/outputs/run-1/model.safetensors",
                        fileHash,
                        payload.length
                )
        ));

        ErrorResponse rejected = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferCommitRequest(
                                transferId,
                                "/outputs/run-1",
                                Hashing.transfer(
                                        List.of(Hashing.chunk("wrong".getBytes(StandardCharsets.UTF_8))),
                                        List.of((long) payload.length)
                                ),
                                payload.length
                        )
                ))
                .payload();

        assertEquals(ResponseStatus.CONFLICT, rejected.status());
        assertTrue(dispatcher.admittedWriteTransfer(transferId).isPresent());
    }

    @Test
    void expiresWriteTransfersAndReleasesStagedPayloads(@TempDir Path tempDir) throws Exception {
        Path spillDirectory = tempDir.resolve("payload-spill");
        Fixture fixture = fixture(PayloadStagingConfig.withSpill(0, spillDirectory));
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(
                fixture.server(),
                ControlSessionLimits.defaults(),
                clock
        );
        AuthAccepted accepted = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        byte[] payload = "abandoned checkpoint payload".getBytes(StandardCharsets.UTF_8);

        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/run-expired",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        1024 * 1024
                )
        ));
        dispatcher.stageWritePayload(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));
        assertEquals(1, countRegularFiles(spillDirectory));

        clock.advance(Duration.ofMinutes(11));
        ErrorResponse expired = (ErrorResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
                        accepted.sessionId(),
                        new TransferMetadataUpdate(
                                transferId,
                                0,
                                0,
                                true,
                                payload.length,
                                List.of(Hashing.chunk(payload))
                        )
                ))
                .payload();

        assertEquals(ResponseStatus.CONFLICT, expired.status());
        assertTrue(expired.message().contains("not admitted"));
        assertEquals(Optional.empty(), dispatcher.admittedWriteTransfer(transferId));
        assertEquals(0, countRegularFiles(spillDirectory));
    }

    @Test
    void expiresPendingAuthenticationChallenges() throws Exception {
        Fixture fixture = fixture();
        MutableClock clock = new MutableClock(Instant.parse("2026-05-31T12:00:00Z"));
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(
                fixture.server(),
                new ControlSessionLimits(Duration.ofSeconds(5), 16, 16),
                clock
        );
        ClientAuthenticator authenticator = new ClientAuthenticator();

        try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
            AuthChallenge challenge = (AuthChallenge) dispatcher
                    .handle(ControlEnvelope.unauthenticated(authenticator.start(credentials)))
                    .payload();
            assertEquals(1, dispatcher.pendingAuthChallengeCount());

            clock.advance(Duration.ofSeconds(6));
            ErrorResponse expired = (ErrorResponse) dispatcher
                    .handle(ControlEnvelope.unauthenticated(authenticator.answer(challenge, credentials)))
                    .payload();

            assertEquals(ResponseStatus.UNAUTHENTICATED, expired.status());
            assertEquals(0, dispatcher.pendingAuthChallengeCount());
            assertEquals(0, dispatcher.authenticatedSessionCount());
        }
    }

    @Test
    void capsPendingAuthenticationChallengesAndAuthenticatedSessions() throws Exception {
        Fixture fixture = fixture();
        MutableClock clock = new MutableClock(Instant.parse("2026-05-31T12:00:00Z"));
        ControlSessionDispatcher challengeLimited = new ControlSessionDispatcher(
                fixture.server(),
                new ControlSessionLimits(Duration.ofMinutes(2), 1, 16),
                clock
        );
        ClientAuthenticator authenticator = new ClientAuthenticator();

        try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
            ControlPayload first = challengeLimited
                    .handle(ControlEnvelope.unauthenticated(authenticator.start(credentials)))
                    .payload();
            ErrorResponse second = (ErrorResponse) challengeLimited
                    .handle(ControlEnvelope.unauthenticated(authenticator.start(credentials)))
                    .payload();

            assertTrue(first instanceof AuthChallenge);
            assertEquals(ResponseStatus.CONFLICT, second.status());
            assertEquals(1, challengeLimited.pendingAuthChallengeCount());
        }

        ControlSessionDispatcher sessionLimited = new ControlSessionDispatcher(
                fixture.server(),
                new ControlSessionLimits(Duration.ofMinutes(2), 16, 1),
                clock
        );
        authenticate(sessionLimited);
        ErrorResponse rejected = (ErrorResponse) authenticateWithPassword(sessionLimited, "secret");

        assertEquals(ResponseStatus.CONFLICT, rejected.status());
        assertEquals(1, sessionLimited.authenticatedSessionCount());
    }

    @Test
    void frameHandlerRoundTripsControlFramesThroughTheDispatcher() throws Exception {
        Fixture fixture = fixture();
        ControlFrameHandler handler = new ControlFrameHandler(new ControlSessionDispatcher(fixture.server()));

        ClientAuthenticator authenticator = new ClientAuthenticator();
        try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
            DecodedFrame challengeFrame = handle(
                    handler,
                    ControlEnvelope.unauthenticated(authenticator.start(credentials))
            );
            DecodedFrame auth = handle(
                    handler,
                    ControlEnvelope.unauthenticated(authenticator.answer((AuthChallenge) challengeFrame.payload(), credentials))
            );
            AuthAccepted accepted = (AuthAccepted) auth.payload();
            DecodedFrame range = handle(
                    handler,
                    ControlEnvelope.authenticated(
                            accepted.sessionId(),
                            new ReadFileRangeRequest("/datasets/train/shard.mds", 11, 128)
                    )
            );

            assertTrue(challengeFrame.frame().sessionId().isEmpty());
            assertEquals(Optional.of(accepted.sessionId()), auth.frame().sessionId());
            assertEquals(Optional.of(accepted.sessionId()), range.frame().sessionId());
            ReadFileRangeResponse response = (ReadFileRangeResponse) range.payload();
            assertEquals(fixture.manifest().fileHash(), response.plan().fileHash());
            assertEquals(11, response.plan().requestedRange().offset());
            assertEquals(128, response.plan().requestedRange().length());
        }
    }

    private static AuthAccepted authenticate(ControlSessionDispatcher dispatcher) {
        ControlPayload payload = authenticateWithPassword(dispatcher, "secret");
        return (AuthAccepted) payload;
    }

    private static ControlPayload authenticateWithPassword(ControlSessionDispatcher dispatcher, String password) {
        ClientAuthenticator authenticator = new ClientAuthenticator();
        try (ClientCredentials credentials = new ClientCredentials("trainer", password.toCharArray())) {
            ControlPayload challenge = dispatcher
                    .handle(ControlEnvelope.unauthenticated(authenticator.start(credentials)))
                    .payload();
            return dispatcher
                    .handle(ControlEnvelope.unauthenticated(authenticator.answer((AuthChallenge) challenge, credentials)))
                    .payload();
        }
    }

    private static DecodedFrame handle(ControlFrameHandler handler, ControlEnvelope<? extends ControlPayload> envelope)
            throws Exception {
        byte[] payload = ControlPayloadCodec.encode(envelope.payload());
        byte[] request = ControlFrameCodec.encode(ControlFrame.fromEnvelope(envelope, payload));
        ControlFrame responseFrame = ControlFrameCodec.decode(handler.handle(request));
        ControlPayload responsePayload = ControlPayloadCodec.decode(responseFrame.operation(), responseFrame.payload());
        return new DecodedFrame(responseFrame, responsePayload);
    }

    private static Fixture fixture() throws Exception {
        return fixture(PayloadStagingConfig.serverDefault(), defaultAuthenticationPolicy(), false);
    }

    private static Fixture fixture(PayloadStagingConfig payloadStagingConfig) throws Exception {
        return fixture(payloadStagingConfig, defaultAuthenticationPolicy(), false);
    }

    private static Fixture fixture(AuthenticationPolicy authenticationPolicy) throws Exception {
        return fixture(PayloadStagingConfig.serverDefault(), authenticationPolicy, true);
    }

    private static Fixture fixture(
            PayloadStagingConfig payloadStagingConfig,
            AuthenticationPolicy authenticationPolicy,
            boolean includeOutputs
    ) throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        byte[] data = "control dispatcher shard data\n".repeat(4096).getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.mds", data);
        if (includeOutputs) {
            repository.put("/outputs/run-1/model.bin", "output model".getBytes(StandardCharsets.UTF_8));
        }
        return new Fixture(new CxnFileServer(config(payloadStagingConfig, authenticationPolicy), repository), manifest);
    }

    private static CxnFileServerConfig config() {
        return config(PayloadStagingConfig.serverDefault());
    }

    private static CxnFileServerConfig config(PayloadStagingConfig payloadStagingConfig) {
        return config(payloadStagingConfig, defaultAuthenticationPolicy());
    }

    private static CxnFileServerConfig config(
            PayloadStagingConfig payloadStagingConfig,
            AuthenticationPolicy authenticationPolicy
    ) {
        Path keyStore = Path.of("cxnfs-test-server.p12");
        return new CxnFileServerConfig(
                Path.of("."),
                authenticationPolicy,
                new TransportBinding(
                        TransportRole.FILE_SERVER_CONTROL,
                        new InetSocketAddress("127.0.0.1", 4509),
                        TransportSecurityConfig.tls13ForTcp(keyStore)
                ),
                new TransportBinding(
                        TransportRole.CONTAINER_TRANSFER,
                        new InetSocketAddress("127.0.0.1", 4510),
                        TransportSecurityConfig.quicTls13ForUdp(keyStore)
                ),
                payloadStagingConfig
        );
    }

    private static CxnFileServerConfig config(List<ServedRoot> roots) {
        return config(roots, PayloadStagingConfig.serverDefault());
    }

    private static CxnFileServerConfig config(List<ServedRoot> roots, PayloadStagingConfig payloadStagingConfig) {
        Path keyStore = Path.of("cxnfs-test-server.p12");
        return new CxnFileServerConfig(
                roots,
                defaultAuthenticationPolicy(),
                new TransportBinding(
                        TransportRole.FILE_SERVER_CONTROL,
                        new InetSocketAddress("127.0.0.1", 4509),
                        TransportSecurityConfig.tls13ForTcp(keyStore)
                ),
                new TransportBinding(
                        TransportRole.CONTAINER_TRANSFER,
                        new InetSocketAddress("127.0.0.1", 4510),
                        TransportSecurityConfig.quicTls13ForUdp(keyStore)
                ),
                payloadStagingConfig
        );
    }

    private static AuthenticationPolicy defaultAuthenticationPolicy() {
        return AuthenticationPolicy.requireUsernamePassword(Map.of("trainer", "secret"));
    }

    private static void waitForTempWrite(Path source, String tempFileName, byte[] expectedData) throws Exception {
        Path tempFile = source.resolve(tempFileName);
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(tempFile) && Files.size(tempFile) == expectedData.length) {
                byte[] actual = Files.readAllBytes(tempFile);
                if (java.util.Arrays.equals(actual, expectedData)) {
                    return;
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for live temp write");
    }

    private static long countRegularFiles(Path directory) throws Exception {
        try (var paths = java.nio.file.Files.list(directory)) {
            return paths.filter(java.nio.file.Files::isRegularFile).count();
        }
    }

    private record Fixture(CxnFileServer server, FileManifest manifest) {
    }

    private record DecodedFrame(ControlFrame frame, ControlPayload payload) {
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
