package com.fortrow.cxnfs.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LinuxBlockfsNamespaceStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void writesAndLoadsNamespaceSnapshotFromMetadataRoot() throws Exception {
        LinuxBlockfsNamespaceStore store = new LinuxBlockfsNamespaceStore();
        LinuxBlockfsNamespaceSnapshot snapshot = new LinuxBlockfsNamespaceSnapshot(
                7,
                List.of(
                        LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096),
                        new LinuxBlockfsFile(3, "datasets/train/labels.mds", 8192, 0400)
                )
        );

        Path path = store.write(tempDir, snapshot);
        LinuxBlockfsNamespaceSnapshot loaded = store.load(tempDir);

        assertEquals(tempDir.resolve("namespace/current.cxn"), path);
        assertEquals(7, loaded.generation());
        assertEquals(snapshot.files(), loaded.files());
    }

    @Test
    void rejectsUnsupportedVersion() throws Exception {
        Path path = tempDir.resolve("namespace/current.cxn");
        Files.createDirectories(path.getParent());
        Files.writeString(path, """
                version=2
                generation=1
                file.count=0
                """);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsNamespaceStore().load(tempDir)
        );

        assertEquals("unsupported namespace snapshot version: 2", error.getMessage());
    }

    @Test
    void rejectsUnsafeFilePathsFromSnapshot() throws Exception {
        Path path = tempDir.resolve("namespace/current.cxn");
        Files.createDirectories(path.getParent());
        Files.writeString(path, """
                version=1
                generation=1
                file.count=1
                file.0.inode=2
                file.0.path=../sample.bin
                file.0.size=4096
                file.0.mode=0444
                """);

        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsNamespaceStore().load(tempDir)
        );
    }

    @Test
    void rejectsControlCharactersInFilePaths() {
        assertThrows(
                IllegalArgumentException.class,
                () -> LinuxBlockfsFile.readOnly(2, "datasets/train/\u0007sample.bin", 4096)
        );
    }

    @Test
    void rejectsFilePathThatWouldAlsoNeedToBeADirectory() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsNamespaceSnapshot(
                        1,
                        List.of(
                                LinuxBlockfsFile.readOnly(2, "datasets", 4096),
                                LinuxBlockfsFile.readOnly(3, "datasets/train/shard.mds", 4096)
                        )
                )
        );
    }
}
