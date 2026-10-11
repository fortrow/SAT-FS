package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.ByteRange;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ControlProtocolVocabularyTest {
    @Test
    void envelopeCarriesCurrentVersionSessionAndPayloadOperation() {
        UUID sessionId = UUID.randomUUID();
        DirectoryListRequest request = new DirectoryListRequest("/docs");

        ControlEnvelope<DirectoryListRequest> envelope = ControlEnvelope.authenticated(sessionId, request);

        assertEquals(ProtocolVersion.CURRENT, envelope.version());
        assertEquals(Optional.of(sessionId), envelope.sessionId());
        assertEquals(ControlOperation.LIST_DIRECTORY, envelope.operation());
    }

    @Test
    void unauthenticatedEnvelopeIsOnlyMissingSessionIdentity() {
        ControlEnvelope<AuthStart> envelope = ControlEnvelope.unauthenticated(
                new AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer")
        );

        assertEquals(ControlOperation.AUTH_START, envelope.operation());
        assertTrue(envelope.sessionId().isEmpty());
        assertEquals(ProtocolVersion.CURRENT, envelope.version());
    }

    @Test
    void lockedPayloadOperationsCoverCoreFileserverVocabulary() {
        ContentHash hash = hash(1);
        ContentHash containerHash = hash(2);
        FileManifest manifest = new FileManifest("/docs/a.txt", hash, 5, List.of(new ChunkRef(hash, 5)));
        FileMetadata metadata = new FileMetadata("/docs/b.txt", "b.txt", FileType.REGULAR_FILE, 5, Optional.of(hash), Instant.EPOCH);
        ReconstructionPlan plan = new ReconstructionPlan(hash, 5, List.of(new ReconstructionTerm(containerHash, 0, 1)));
        ReadPlan readPlan = new ReadPlan(hash, 5, new ByteRange(0, 5), List.of(new PlannedChunkRead(0, hash, 0, 5, 0, 5)));
        UUID transferId = UUID.randomUUID();

        UUID challengeId = UUID.randomUUID();
        List<ControlPayload> payloads = List.of(
                new AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer"),
                new AuthChallenge(challengeId, AuthMechanism.USERNAME_PASSWORD, "trainer", new byte[]{1, 2, 3}, "test-proof", 1),
                new AuthProof(challengeId, "trainer", new byte[]{4, 5, 6}),
                new AuthAccepted(UUID.randomUUID(), List.of(AuthMechanism.USERNAME_PASSWORD)),
                new DirectoryListRequest("/docs"),
                new DirectoryListResponse(new DirectoryListing("/docs", List.of())),
                new StatPathRequest("/docs/a.txt"),
                new StatPathResponse(Optional.empty()),
                new PutFileRequest("/docs/a.txt", 5, hash, manifest.chunks()),
                new PutFileResponse(manifest, List.of(hash)),
                new ManifestRequest(hash),
                new ManifestResponse(Optional.of(manifest)),
                new ReconstructionPlanRequest(hash),
                new ReconstructionPlanResponse(plan),
                new ReadFileRangeRequest("/docs/a.txt", 0, 5),
                new ReadFileRangeResponse("/docs/a.txt", readPlan),
                new ReadContainerRangeRequest(containerHash, 0, 1),
                new RenamePathRequest("/docs/a.txt", "/docs/b.txt"),
                new DeletePathRequest("/docs/b.txt", false),
                new RenamePathResponse("/docs/a.txt", "/docs/b.txt", metadata),
                new DeletePathResponse("/docs/b.txt", 1),
                new MountNegotiateRequest(MountBackend.MACOS_FUSE_T, Set.of(MountCapability.LIST, MountCapability.READ)),
                new MountNegotiatedResponse(MountStrategy.macFuseT()),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "/outputs/checkpoint",
                        TransferPayloadFormat.SAFETENSORS,
                        1,
                        64,
                        64 * 1024 * 1024,
                        10L * 1024 * 1024 * 1024
                ),
                new TransferAcceptedResponse(transferId, TransferDirection.WRITE, 1, 64, 128L * 1024 * 1024, true),
                new TransferMetadataUpdate(transferId, 0, 0, true, 5, List.of(hash)),
                new TransferMetadataAcceptedResponse(transferId, 0, 0, 1, 1, true),
                new TransferCommitRequest(transferId, "/outputs/checkpoint", hash, 5),
                new TransferCommittedResponse(transferId, hash, 5),
                new TransferEntryUpdate(
                        transferId,
                        0,
                        TransferEntryAction.CREATE_OR_REPLACE,
                        "/outputs/checkpoint/model.safetensors",
                        "",
                        5,
                        TransferPayloadFormat.SAFETENSORS
                ),
                new TransferEntryAcceptedResponse(transferId, 0, "/outputs/checkpoint/model.safetensors"),
                new TransferEntryCommitRequest(transferId, 0, "/outputs/checkpoint/model.safetensors", hash, 5),
                new TransferEntryCommittedResponse(transferId, 0, "/outputs/checkpoint/model.safetensors", hash, 5),
                new ErrorResponse(ResponseStatus.NOT_FOUND, "missing")
        );

        assertEquals(List.of(
                ControlOperation.AUTH_START,
                ControlOperation.AUTH_CHALLENGE,
                ControlOperation.AUTH_PROOF,
                ControlOperation.AUTH_ACCEPTED,
                ControlOperation.LIST_DIRECTORY,
                ControlOperation.LIST_DIRECTORY_RESULT,
                ControlOperation.STAT_PATH,
                ControlOperation.STAT_PATH_RESULT,
                ControlOperation.PUT_FILE,
                ControlOperation.PUT_FILE_RESULT,
                ControlOperation.GET_MANIFEST,
                ControlOperation.MANIFEST_RESULT,
                ControlOperation.GET_RECONSTRUCTION_PLAN,
                ControlOperation.RECONSTRUCTION_PLAN_RESULT,
                ControlOperation.READ_FILE_RANGE,
                ControlOperation.READ_FILE_RANGE_RESULT,
                ControlOperation.READ_CONTAINER_RANGE,
                ControlOperation.RENAME_PATH,
                ControlOperation.DELETE_PATH,
                ControlOperation.RENAME_PATH_RESULT,
                ControlOperation.DELETE_PATH_RESULT,
                ControlOperation.MOUNT_NEGOTIATE,
                ControlOperation.MOUNT_NEGOTIATED,
                ControlOperation.TRANSFER_BOOTSTRAP,
                ControlOperation.TRANSFER_ACCEPTED,
                ControlOperation.TRANSFER_METADATA,
                ControlOperation.TRANSFER_METADATA_ACCEPTED,
                ControlOperation.TRANSFER_COMMIT,
                ControlOperation.TRANSFER_COMMITTED,
                ControlOperation.TRANSFER_ENTRY_UPDATE,
                ControlOperation.TRANSFER_ENTRY_ACCEPTED,
                ControlOperation.TRANSFER_ENTRY_COMMIT,
                ControlOperation.TRANSFER_ENTRY_COMMITTED,
                ControlOperation.ERROR
        ), payloads.stream().map(ControlPayload::operation).toList());
    }

    @Test
    void invalidProtocolMessagesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new DirectoryListRequest(""));
        assertThrows(IllegalArgumentException.class, () -> new ReadFileRangeRequest("/a", -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new PutFileRequest("/a", -1, hash(1), List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ReadContainerRangeRequest(hash(1), 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ErrorResponse(ResponseStatus.OK, "not an error"));
        assertThrows(IllegalArgumentException.class, () -> new TransferBootstrapRequest(
                UUID.randomUUID(),
                TransferDirection.WRITE,
                "/datasets/train",
                TransferPayloadFormat.MDS_SHARDS,
                1,
                64,
                64 * 1024 * 1024,
                1024
        ));
        assertThrows(IllegalArgumentException.class, () -> new TransferEntryUpdate(
                UUID.randomUUID(),
                0,
                TransferEntryAction.RENAME,
                "/a",
                "",
                0,
                TransferPayloadFormat.CONTAINER
        ));
        assertThrows(IllegalArgumentException.class, () -> new TransferEntryCommitRequest(UUID.randomUUID(), -1, "/a", hash(1), 1));

        ControlEnvelope<DirectoryListRequest> envelope = ControlEnvelope.unauthenticated(new DirectoryListRequest("/"));
        assertFalse(envelope.messageId().toString().isBlank());
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
