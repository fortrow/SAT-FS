package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LinuxBlockfsMetadataAuditorTest {
    @Test
    void auditsNamespaceManifestAndExtentHintsAsOneGeneration() {
        LinuxBlockfsMetadataAuditResult result = new LinuxBlockfsMetadataAuditor().audit(
                namespace(7),
                Optional.of(manifest(7)),
                Optional.of(extents(7, hashBytes(2)))
        );

        assertEquals(1, result.namespaceFiles());
        assertEquals(1, result.manifestFiles());
        assertEquals(1, result.manifestChunks());
        assertEquals(1, result.extentHints());
    }

    @Test
    void rejectsManifestGenerationMismatch() {
        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsMetadataAuditor().audit(
                        namespace(7),
                        Optional.of(manifest(8)),
                        Optional.empty()
                )
        );
    }

    @Test
    void rejectsManifestMissingNamespaceFile() {
        LinuxBlockfsNamespaceSnapshot namespace = new LinuxBlockfsNamespaceSnapshot(
                7,
                List.of(
                        LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096),
                        LinuxBlockfsFile.readOnly(3, "datasets/train/labels.mds", 2048)
                )
        );

        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsMetadataAuditor().audit(
                        namespace,
                        Optional.of(manifest(7)),
                        Optional.empty()
                )
        );
    }

    @Test
    void rejectsExtentHintsWithoutManifestIndex() {
        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsMetadataAuditor().audit(
                        namespace(7),
                        Optional.empty(),
                        Optional.of(extents(7, hashBytes(2)))
                )
        );
    }

    @Test
    void rejectsExtentHintHashMismatch() {
        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsMetadataAuditor().audit(
                        namespace(7),
                        Optional.of(manifest(7)),
                        Optional.of(extents(7, hashBytes(99)))
                )
        );
    }

    private static LinuxBlockfsNamespaceSnapshot namespace(long generation) {
        return new LinuxBlockfsNamespaceSnapshot(
                generation,
                List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096))
        );
    }

    private static LinuxBlockfsManifestIndexSnapshot manifest(long generation) {
        return new LinuxBlockfsManifestIndexSnapshot(
                generation,
                List.of(new LinuxBlockfsManifestEntry(
                        2,
                        "datasets/train/sample.bin",
                        4096,
                        hash(1),
                        List.of(new LinuxBlockfsManifestChunk(0, 4096, hash(2)))
                ))
        );
    }

    private static LinuxBlockfsExtentHintsSnapshot extents(long generation, byte[] hash) {
        return new LinuxBlockfsExtentHintsSnapshot(
                generation,
                List.of(new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash))
        );
    }

    private static ContentHash hash(int seed) {
        return ContentHash.of(hashBytes(seed));
    }

    private static byte[] hashBytes(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return bytes;
    }
}
