package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkCompressionCodecTest {

    @Test
    void testLz4CompressionRoundtrip() {
        byte[] original = "SAT-FS High Throughput Saturated Async Transfer Network File System "
                .repeat(100).getBytes(StandardCharsets.UTF_8);

        byte[] compressed = BulkCompressionCodec.compressLz4(original);
        assertTrue(compressed.length < original.length, "compressed size should be smaller");

        byte[] decompressed = BulkCompressionCodec.decompressLz4(compressed, original.length);
        assertArrayEquals(original, decompressed);
    }

    @Test
    void testZstdCompressionRoundtrip() {
        byte[] original = "SAT-FS High Throughput Saturated Async Transfer Network File System "
                .repeat(100).getBytes(StandardCharsets.UTF_8);

        byte[] compressed = BulkCompressionCodec.compressZstd(original);
        assertTrue(compressed.length < original.length, "compressed size should be smaller");

        byte[] decompressed = BulkCompressionCodec.decompressZstd(compressed, original.length);
        assertArrayEquals(original, decompressed);
    }

    @Test
    void testFrameLz4Roundtrip() throws IOException {
        byte[] original = "SAT-FS tensor chunk range streaming with CPU decompression expansion "
                .repeat(50).getBytes(StandardCharsets.UTF_8);
        UUID transferId = UUID.randomUUID();

        BulkDataFrame frame = new BulkDataFrame(
                transferId,
                0,
                1,
                100L,
                0L,
                true,
                original
        );

        byte[] encoded = BulkDataFrameCodec.encodeCompressed(
                frame,
                BulkCompressionCodec.CompressionAlgorithm.LZ4
        );
        assertTrue(encoded.length < original.length, "encoded frame should be compressed");

        BulkDataFrame decoded = BulkDataFrameCodec.decode(encoded);
        assertEquals(transferId, decoded.transferId());
        assertEquals(1, decoded.entrySequence());
        assertEquals(100L, decoded.chunkSequence());
        assertTrue(decoded.finalFrameForChunk());
        assertArrayEquals(original, decoded.payload());
    }

    @Test
    void testFrameZstdRoundtrip() throws IOException {
        byte[] original = "SAT-FS tensor chunk range streaming with CPU decompression expansion "
                .repeat(50).getBytes(StandardCharsets.UTF_8);
        UUID transferId = UUID.randomUUID();

        BulkDataFrame frame = new BulkDataFrame(
                transferId,
                0,
                1,
                200L,
                0L,
                true,
                original
        );

        byte[] encoded = BulkDataFrameCodec.encodeCompressed(
                frame,
                BulkCompressionCodec.CompressionAlgorithm.ZSTD
        );
        assertTrue(encoded.length < original.length, "encoded frame should be compressed");

        BulkDataFrame decoded = BulkDataFrameCodec.decode(encoded);
        assertEquals(transferId, decoded.transferId());
        assertEquals(200L, decoded.chunkSequence());
        assertArrayEquals(original, decoded.payload());
    }
}
