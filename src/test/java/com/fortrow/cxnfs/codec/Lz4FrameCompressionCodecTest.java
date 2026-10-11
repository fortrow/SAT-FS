package com.fortrow.cxnfs.codec;

import com.fortrow.cxnfs.format.BinaryFormatException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class Lz4FrameCompressionCodecTest {
    @Test
    void roundTripsCompressiblePayloads() throws Exception {
        Lz4FrameCompressionCodec codec = new Lz4FrameCompressionCodec();
        byte[] input = "dataset shard block\n".repeat(10_000).getBytes(StandardCharsets.UTF_8);

        byte[] compressed = codec.compress(input);
        byte[] decompressed = codec.decompress(compressed, input.length);

        assertEquals(Lz4FrameCompressionCodec.ID, codec.id());
        assertEquals("lz4-frame", codec.name());
        assertArrayEquals(input, decompressed);
        assertTrue(compressed.length < input.length);
    }

    @Test
    void roundTripsEmptyPayloads() throws Exception {
        Lz4FrameCompressionCodec codec = new Lz4FrameCompressionCodec();

        byte[] compressed = codec.compress(new byte[0]);

        assertArrayEquals(new byte[0], codec.decompress(compressed, 0));
    }

    @Test
    void rejectsIncorrectExpectedSize() throws Exception {
        Lz4FrameCompressionCodec codec = new Lz4FrameCompressionCodec();
        byte[] compressed = codec.compress("abc".getBytes(StandardCharsets.UTF_8));

        assertThrows(BinaryFormatException.class, () -> codec.decompress(compressed, 4));
    }

    @Test
    void rejectsCorruptFrameMagic() throws Exception {
        Lz4FrameCompressionCodec codec = new Lz4FrameCompressionCodec();
        byte[] compressed = codec.compress("abc".getBytes(StandardCharsets.UTF_8));
        byte[] corrupt = Arrays.copyOf(compressed, compressed.length);
        corrupt[0] = 'X';

        assertThrows(BinaryFormatException.class, () -> codec.decompress(corrupt, 3));
    }
}
