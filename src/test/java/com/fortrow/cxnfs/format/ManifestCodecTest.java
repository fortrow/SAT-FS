package com.fortrow.cxnfs.format;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ManifestCodecTest {
    @Test
    void encodesAndDecodesFileManifest() throws Exception {
        FileManifest manifest = new FileManifest(
                "/docs/readme.txt",
                hash(1),
                128,
                List.of(new ChunkRef(hash(2), 64), new ChunkRef(hash(3), 64))
        );

        FileManifest decoded = ManifestCodec.decode(ManifestCodec.encode(manifest));

        assertEquals(manifest.path(), decoded.path());
        assertEquals(manifest.fileHash(), decoded.fileHash());
        assertEquals(manifest.size(), decoded.size());
        assertEquals(manifest.chunks(), decoded.chunks());
    }

    @Test
    void rejectsTrailingManifestBytes() throws Exception {
        FileManifest manifest = new FileManifest("/a.txt", hash(1), 0, List.of());
        byte[] encoded = ManifestCodec.encode(manifest);
        byte[] withTrailingBytes = java.util.Arrays.copyOf(encoded, encoded.length + 1);

        assertThrows(BinaryFormatException.class, () -> ManifestCodec.decode(withTrailingBytes));
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
