package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.ByteRange;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.performance.ReadAheadPolicy;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;
import com.fortrow.cxnfs.transport.CordPoolConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EarlyReadStoragePlannerTest {
    @Test
    void plansPayloadFirstWithTrailingStructuresFromOsDisk() {
        FileManifest manifest = manifest("/datasets/train/shard.mds", 1024 * 1024, 64 * 1024);
        ReconstructionPlan reconstructionPlan = reconstructionPlan(manifest.fileHash(), 80);
        EarlyReadStoragePolicy policy = new EarlyReadStoragePolicy(
                Path.of("/var/lib/cxnfs/disk-a"),
                new ReadAheadPolicy(256 * 1024, 512 * 1024),
                CordPoolConfig.fixed(64),
                true,
                true,
                true
        );

        EarlyReadStoragePlan plan = new EarlyReadStoragePlanner()
                .plan(manifest, reconstructionPlan, 128 * 1024, 64 * 1024, policy);

        assertEquals(new ByteRange(128 * 1024, 64 * 1024), plan.readAheadPlan().requested().requestedRange());
        assertEquals(new ByteRange(128 * 1024, 320 * 1024), plan.readAheadPlan().prefetch().requestedRange());
        assertEquals(5, plan.payloadExtents().size());
        assertEquals(320L * 1024, plan.earlyPayloadBytes());
        assertTrue(plan.payloadMayStartBeforeTrailingStructures());
        assertTrue(plan.finalPublishRequiresTrailingStructures());
        assertEquals(64, plan.cordTransferPlan().activeCords());
        assertEquals(3, plan.bootstrapStructures().size());
        assertTrue(plan.bootstrapStructures().stream().allMatch(FilesystemStructureRef::requiredBeforePayload));
        assertTrue(plan.trailingStructures().stream().allMatch(FilesystemStructureRef::trailing));
        assertTrue(plan.allStructures().stream()
                .map(FilesystemStructureRef::path)
                .allMatch(path -> path.startsWith(Path.of("/var/lib/cxnfs/disk-a"))));
        assertEquals(
                Set.of(
                        FilesystemStructureRole.MANIFEST_INDEX,
                        FilesystemStructureRole.CHUNK_HASH_INDEX,
                        FilesystemStructureRole.CACHE_INDEX,
                        FilesystemStructureRole.JOURNAL,
                        FilesystemStructureRole.FREE_SPACE_MAP
                ),
                plan.trailingStructures().stream()
                        .map(FilesystemStructureRef::role)
                        .collect(java.util.stream.Collectors.toSet())
        );
    }

    @Test
    void rejectsEarlyPayloadWhenStructuresDoNotLiveOnOsDisk() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new EarlyReadStoragePolicy(
                        Path.of("/var/lib/cxnfs/disk-a"),
                        ReadAheadPolicy.linuxAiTrainingDataset(),
                        CordPoolConfig.xetLikeDefaults(),
                        false,
                        true,
                        true
                )
        );
    }

    @Test
    void rejectsEarlyPayloadThatWouldPublishWithoutTrailingVerification() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new EarlyReadStoragePolicy(
                        Path.of("/var/lib/cxnfs/disk-a"),
                        ReadAheadPolicy.linuxAiTrainingDataset(),
                        CordPoolConfig.xetLikeDefaults(),
                        true,
                        true,
                        false
                )
        );
    }

    @Test
    void rejectsPathTraversalForSidecarMetadata() {
        FileManifest manifest = manifest("/datasets/../shard.mds", 64 * 1024, 64 * 1024);
        ReconstructionPlan reconstructionPlan = reconstructionPlan(manifest.fileHash(), 1);

        assertThrows(
                IllegalArgumentException.class,
                () -> new EarlyReadStoragePlanner().plan(
                        manifest,
                        reconstructionPlan,
                        0,
                        64 * 1024,
                        EarlyReadStoragePolicy.serverHostDefaults(Path.of("/var/lib/cxnfs/disk-a"))
                )
        );
    }

    private static FileManifest manifest(String path, int size, int chunkSize) {
        int chunks = size / chunkSize;
        return new FileManifest(
                path,
                hash(1),
                size,
                java.util.stream.IntStream.range(0, chunks)
                        .mapToObj(index -> new ChunkRef(hash(index + 2), chunkSize))
                        .toList()
        );
    }

    private static ReconstructionPlan reconstructionPlan(ContentHash fileHash, int terms) {
        return new ReconstructionPlan(
                fileHash,
                1024L * 1024,
                java.util.stream.IntStream.range(0, terms)
                        .mapToObj(index -> new ReconstructionTerm(hash(index + 100), 0, 1))
                        .toList()
        );
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
