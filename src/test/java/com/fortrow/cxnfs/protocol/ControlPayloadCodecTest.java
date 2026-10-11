package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.format.BinaryFormatException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ControlPayloadCodecTest {
    @Test
    void encodesAndDecodesAllCorePayloads() throws Exception {
        ContentHash fileHash = hash(1);
        ContentHash chunkHash = hash(2);
        ContentHash containerHash = hash(3);
        FileManifest manifest = new FileManifest(
                "/docs/a.txt",
                fileHash,
                64,
                List.of(new ChunkRef(chunkHash, 64))
        );
        FileMetadata metadata = new FileMetadata(
                "/docs/a.txt",
                "a.txt",
                FileType.REGULAR_FILE,
                64,
                Optional.of(fileHash),
                Instant.ofEpochSecond(1_700_000_000L, 123)
        );
        ReconstructionPlan plan = new ReconstructionPlan(
                fileHash,
                64,
                List.of(new ReconstructionTerm(containerHash, 2, 4))
        );
        ReadPlan readPlan = new ReadPlan(
                fileHash,
                64,
                new ByteRange(8, 24),
                List.of(new PlannedChunkRead(0, chunkHash, 0, 64, 8, 24))
        );
        UUID transferId = UUID.randomUUID();
        UUID challengeId = UUID.randomUUID();

        List<ControlPayload> payloads = List.of(
                new AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer"),
                new AuthChallenge(challengeId, AuthMechanism.USERNAME_PASSWORD, "trainer", new byte[]{1, 2, 3}, "test-proof", 1),
                new AuthProof(challengeId, "trainer", new byte[]{4, 5, 6}),
                new AuthAccepted(UUID.randomUUID(), List.of(AuthMechanism.USERNAME_PASSWORD)),
                new DirectoryListRequest("/docs"),
                new DirectoryListResponse(new DirectoryListing("/docs", List.of(metadata))),
                new StatPathRequest("/docs/a.txt"),
                new StatPathResponse(Optional.of(metadata)),
                new PutFileRequest("/docs/a.txt", 64, fileHash, manifest.chunks()),
                new PutFileResponse(manifest, List.of(chunkHash)),
                new ManifestRequest(fileHash),
                new ManifestResponse(Optional.of(manifest)),
                new ReconstructionPlanRequest(fileHash),
                new ReconstructionPlanResponse(plan),
                new ReadFileRangeRequest("/docs/a.txt", 8, 24),
                new ReadFileRangeResponse("/docs/a.txt", readPlan),
                new ReadContainerRangeRequest(containerHash, 2, 4),
                new RenamePathRequest("/docs/a.txt", "/docs/b.txt"),
                new DeletePathRequest("/docs/b.txt", false),
                new RenamePathResponse("/docs/a.txt", "/docs/b.txt", metadata),
                new DeletePathResponse("/docs/b.txt", 1),
                new MountNegotiateRequest(MountBackend.MACOS_FUSE_T, Set.of(MountCapability.LIST, MountCapability.READ)),
                new MountNegotiatedResponse(MountStrategy.macFuseT()),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.READ,
                        "/datasets/train/shard.00000.mds",
                        TransferPayloadFormat.MDS_SHARDS,
                        1,
                        64,
                        64 * 1024 * 1024,
                        512L * 1024 * 1024
                ),
                new TransferAcceptedResponse(transferId, TransferDirection.READ, 1, 64, 128L * 1024 * 1024, false),
                new TransferMetadataUpdate(transferId, 3, 7, true, 64, List.of(chunkHash)),
                new TransferMetadataAcceptedResponse(transferId, 3, 7, 1, 1, true),
                new TransferCommitRequest(transferId, "/outputs/checkpoint", fileHash, 64),
                new TransferCommittedResponse(transferId, fileHash, 64),
                new TransferEntryUpdate(
                        transferId,
                        3,
                        TransferEntryAction.CREATE_OR_REPLACE,
                        "/outputs/checkpoint/model.safetensors",
                        "",
                        64,
                        TransferPayloadFormat.SAFETENSORS
                ),
                new TransferEntryAcceptedResponse(transferId, 3, "/outputs/checkpoint/model.safetensors"),
                new TransferEntryCommitRequest(transferId, 3, "/outputs/checkpoint/model.safetensors", fileHash, 64),
                new TransferEntryCommittedResponse(transferId, 3, "/outputs/checkpoint/model.safetensors", fileHash, 64),
                new ErrorResponse(ResponseStatus.NOT_FOUND, "missing")
        );

        for (ControlPayload payload : payloads) {
            ControlPayload decoded = ControlPayloadCodec.decode(payload.operation(), ControlPayloadCodec.encode(payload));

            assertEquals(payload, decoded, "round trip for " + payload.operation());
        }
    }

    @Test
    void rejectsPayloadsDecodedAgainstTheWrongOperation() throws Exception {
        byte[] payload = ControlPayloadCodec.encode(new DirectoryListRequest("/docs"));

        assertThrows(
                BinaryFormatException.class,
                () -> ControlPayloadCodec.decode(ControlOperation.STAT_PATH, payload)
        );
    }

    @Test
    void decodesOptionalEmptyPayloadFields() throws Exception {
        ControlPayload stat = ControlPayloadCodec.decode(
                ControlOperation.STAT_PATH_RESULT,
                ControlPayloadCodec.encode(new StatPathResponse(Optional.empty()))
        );
        ControlPayload manifest = ControlPayloadCodec.decode(
                ControlOperation.MANIFEST_RESULT,
                ControlPayloadCodec.encode(new ManifestResponse(Optional.empty()))
        );

        assertEquals(new StatPathResponse(Optional.empty()), stat);
        assertEquals(new ManifestResponse(Optional.empty()), manifest);
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
