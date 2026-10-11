package com.fortrow.cxnfs.format;

import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.codec.IdentityCompressionCodec;
import com.fortrow.cxnfs.container.Container;
import com.fortrow.cxnfs.container.ContainerBuilder;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ContainerCodecTest {
    @Test
    void encodesAndDecodesCanonicalContainerBytes() throws Exception {
        byte[] data = "container bytes need a stable shape".repeat(200).getBytes(StandardCharsets.UTF_8);
        Container container = new ContainerBuilder(new IdentityCompressionCodec())
                .build(GearHashChunker.defaults().split(data));

        byte[] encoded = ContainerCodec.encode(container);
        Container decoded = ContainerCodec.decode(encoded);

        assertEquals(container.hash(), decoded.hash());
        assertEquals(container.chunks().size(), decoded.chunks().size());
        for (int i = 0; i < container.chunks().size(); i++) {
            assertEquals(container.chunkAt(i).chunkHash(), decoded.chunkAt(i).chunkHash());
            assertEquals(container.chunkAt(i).originalSize(), decoded.chunkAt(i).originalSize());
            assertEquals(container.chunkAt(i).compressionType(), decoded.chunkAt(i).compressionType());
            assertArrayEquals(container.chunkAt(i).payload(), decoded.chunkAt(i).payload());
        }
    }

    @Test
    void rejectsBadContainerMagic() throws Exception {
        byte[] data = "bad magic".getBytes(StandardCharsets.UTF_8);
        Container container = new ContainerBuilder(new IdentityCompressionCodec())
                .build(GearHashChunker.defaults().split(data));
        byte[] encoded = ContainerCodec.encode(container);
        encoded[0] = 'X';

        assertThrows(BinaryFormatException.class, () -> ContainerCodec.decode(encoded));
    }
}
