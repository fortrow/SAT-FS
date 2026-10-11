package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MountedBlockfsStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void readsListsStatsAndPlansFromMountedBlockfsPath() throws Exception {
        MountedFixture fixture = mountedFixture("training-data-block\n".repeat(3000).getBytes(StandardCharsets.UTF_8));
        MountedBlockfsStore store = MountedBlockfsStore.load(fixture.mountRoot(), fixture.metadataRoot());

        DirectoryListing root = store.list("/");
        DirectoryListing train = store.list("/datasets/train");
        byte[] read = store.reconstruct(fixture.manifest().fileHash());
        byte[] range = store.readRange(fixture.manifest().fileHash(), 11, 257);
        ReconstructionPlan reconstructionPlan = store.plan(fixture.manifest().fileHash());

        assertEquals(1, store.fileCount());
        assertEquals(FileType.DIRECTORY, root.entries().getFirst().type());
        assertEquals("sample.bin", train.entries().getFirst().name());
        assertEquals(fixture.manifest().fileHash(), train.entries().getFirst().contentHash().orElseThrow());
        assertEquals(FileType.REGULAR_FILE, store.stat("/datasets/train/sample.bin").orElseThrow().type());
        assertEquals(fixture.manifest(), store.manifest("/datasets/train/sample.bin").orElseThrow());
        assertEquals(fixture.manifest(), store.manifest(fixture.manifest().fileHash()).orElseThrow());
        assertArrayEquals(fixture.data(), read);
        assertArrayEquals(Arrays.copyOfRange(fixture.data(), 11, 268), range);
        assertEquals(100, store.readPlan(fixture.manifest().fileHash(), 5, 100).requestedRange().length());
        assertEquals(fixture.manifest().fileHash(), reconstructionPlan.fileHash());
        assertEquals(fixture.manifest().size(), reconstructionPlan.fileSize());
        assertEquals(1, reconstructionPlan.terms().size());
        assertEquals(0, reconstructionPlan.terms().getFirst().firstChunkIndex());
        assertEquals(fixture.manifest().chunks().size(), reconstructionPlan.terms().getFirst().chunkCount());
        assertEquals(
                fixture.manifest(),
                store.manifestForSyntheticContainer(reconstructionPlan.terms().getFirst().containerHash()).orElseThrow()
        );
    }

    @Test
    void rejectsMountedFileThatDoesNotMatchManifestChunks() throws Exception {
        MountedFixture fixture = mountedFixture("original bytes".getBytes(StandardCharsets.UTF_8));
        Files.writeString(fixture.mountRoot().resolve("datasets/train/sample.bin"), "tampered bytes");
        MountedBlockfsStore store = MountedBlockfsStore.load(fixture.mountRoot(), fixture.metadataRoot());

        assertThrows(IOException.class, () -> store.reconstruct(fixture.manifest().fileHash()));
        assertThrows(IOException.class, () -> store.readRange(fixture.manifest().fileHash(), 0, 4));
        assertThrows(IOException.class, () -> store.readVerifiedPlanChunks(
                store.readPlan(fixture.manifest().fileHash(), 0, 4),
                (chunk, payload) -> {
                }
        ));
    }

    @Test
    void rejectsPlanChunkListsThatDoNotMatchTheMountedManifest() throws Exception {
        MountedFixture fixture = mountedFixture("abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8));
        MountedBlockfsStore store = MountedBlockfsStore.load(fixture.mountRoot(), fixture.metadataRoot());
        ReadPlan plan = store.readPlan(fixture.manifest().fileHash(), 3, 18);
        ReadPlan forged = new ReadPlan(plan.fileHash(), plan.fileSize(), plan.requestedRange(), List.of());

        assertThrows(IllegalArgumentException.class, () -> store.readVerifiedPlanChunks(
                forged,
                (chunk, payload) -> {
                }
        ));
    }

    @Test
    void rejectsTraversalSegmentsInMountedNamespaceLookups() throws Exception {
        MountedFixture fixture = mountedFixture("mounted path".getBytes(StandardCharsets.UTF_8));
        MountedBlockfsStore store = MountedBlockfsStore.load(fixture.mountRoot(), fixture.metadataRoot());

        assertThrows(IllegalArgumentException.class, () -> store.list("/datasets/../train"));
        assertThrows(IllegalArgumentException.class, () -> store.stat("/datasets/./train"));
        assertThrows(IllegalArgumentException.class, () -> store.manifest("/datasets/train/../sample.bin"));
    }

    @Test
    void remainsReadOnlyUntilRecoverySemanticsExist() throws Exception {
        MountedFixture fixture = mountedFixture("readonly".getBytes(StandardCharsets.UTF_8));
        MountedBlockfsStore store = MountedBlockfsStore.load(fixture.mountRoot(), fixture.metadataRoot());

        assertThrows(UnsupportedOperationException.class, () -> store.put("/new.bin", new byte[]{1}));
        assertThrows(UnsupportedOperationException.class, () -> store.publishManifest(fixture.manifest()));
    }

    private MountedFixture mountedFixture(byte[] data) throws Exception {
        Path mountRoot = tempDir.resolve("mount");
        Path metadataRoot = tempDir.resolve("metadata");
        Path file = mountRoot.resolve("datasets/train/sample.bin");
        Files.createDirectories(file.getParent());
        Files.write(file, data);

        ContentHash chunkHash = Hashing.chunk(data);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(data.length));
        FileManifest manifest = new FileManifest(
                "/datasets/train/sample.bin",
                fileHash,
                data.length,
                List.of(new com.fortrow.cxnfs.manifest.ChunkRef(chunkHash, data.length))
        );
        new LinuxBlockfsNamespaceStore().write(
                metadataRoot,
                new LinuxBlockfsNamespaceSnapshot(
                        4,
                        List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", data.length))
                )
        );
        new LinuxBlockfsManifestIndexStore().write(
                metadataRoot,
                new LinuxBlockfsManifestIndexSnapshot(
                        4,
                        List.of(LinuxBlockfsManifestEntry.fromManifest(2, manifest))
                )
        );
        return new MountedFixture(mountRoot, metadataRoot, manifest, data);
    }

    private record MountedFixture(
            Path mountRoot,
            Path metadataRoot,
            FileManifest manifest,
            byte[] data
    ) {
    }
}
