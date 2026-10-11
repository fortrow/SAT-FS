package com.fortrow.cxnfs.transport;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * High-performance frame-level compression codec supporting LZ4 and ZSTD.
 * Designed to compress wire frames for network link saturation and expand
 * data on the receiving side across all available CPU cores into memory.
 */
public final class BulkCompressionCodec {
    public static final int LZ4_MARKER = -2;
    public static final int ZSTD_MARKER = -3;
    public static final int MIN_COMPRESSIBLE_BYTES = 512;

    public enum CompressionAlgorithm {
        NONE(0),
        PATTERN(-1),
        LZ4(LZ4_MARKER),
        ZSTD(ZSTD_MARKER);

        private final int marker;

        CompressionAlgorithm(int marker) {
            this.marker = marker;
        }

        public int marker() {
            return marker;
        }

        public static CompressionAlgorithm fromMarker(int marker) {
            return switch (marker) {
                case -1 -> PATTERN;
                case -2 -> LZ4;
                case -3 -> ZSTD;
                default -> NONE;
            };
        }
    }

    private static final ThreadLocal<Lz4Compressor> LZ4_COMPRESSOR = ThreadLocal.withInitial(Lz4Compressor::new);
    private static final ThreadLocal<Lz4Decompressor> LZ4_DECOMPRESSOR = ThreadLocal.withInitial(Lz4Decompressor::new);
    private static final ThreadLocal<ZstdCompressor> ZSTD_COMPRESSOR = ThreadLocal.withInitial(ZstdCompressor::new);
    private static final ThreadLocal<ZstdDecompressor> ZSTD_DECOMPRESSOR = ThreadLocal.withInitial(ZstdDecompressor::new);

    private BulkCompressionCodec() {
    }

    public static byte[] compressLz4(byte[] uncompressed) {
        if (uncompressed == null || uncompressed.length == 0) {
            return new byte[0];
        }
        Lz4Compressor compressor = LZ4_COMPRESSOR.get();
        int maxLen = compressor.maxCompressedLength(uncompressed.length);
        byte[] output = new byte[maxLen];
        int compressedLen = compressor.compress(uncompressed, 0, uncompressed.length, output, 0, maxLen);
        return Arrays.copyOf(output, compressedLen);
    }

    public static byte[] decompressLz4(byte[] compressed, int uncompressedLength) {
        if (compressed == null || uncompressedLength <= 0) {
            return new byte[0];
        }
        Lz4Decompressor decompressor = LZ4_DECOMPRESSOR.get();
        byte[] uncompressed = new byte[uncompressedLength];
        decompressor.decompress(compressed, 0, compressed.length, uncompressed, 0, uncompressedLength);
        return uncompressed;
    }

    public static byte[] compressZstd(byte[] uncompressed) {
        if (uncompressed == null || uncompressed.length == 0) {
            return new byte[0];
        }
        ZstdCompressor compressor = ZSTD_COMPRESSOR.get();
        int maxLen = compressor.maxCompressedLength(uncompressed.length);
        byte[] output = new byte[maxLen];
        int compressedLen = compressor.compress(uncompressed, 0, uncompressed.length, output, 0, maxLen);
        return Arrays.copyOf(output, compressedLen);
    }

    public static byte[] decompressZstd(byte[] compressed, int uncompressedLength) {
        if (compressed == null || uncompressedLength <= 0) {
            return new byte[0];
        }
        ZstdDecompressor decompressor = ZSTD_DECOMPRESSOR.get();
        byte[] uncompressed = new byte[uncompressedLength];
        decompressor.decompress(compressed, 0, compressed.length, uncompressed, 0, uncompressedLength);
        return uncompressed;
    }

    public record CompressedPayload(int marker, int uncompressedLength, byte[] compressedBytes) {
    }

    /**
     * Compresses the payload if compressible and saves at least 10% space.
     * Returns null if uncompressed representation is more efficient.
     */
    public static CompressedPayload compressIfBeneficial(byte[] payload, CompressionAlgorithm algorithm) {
        if (payload == null || payload.length < MIN_COMPRESSIBLE_BYTES || algorithm == CompressionAlgorithm.NONE) {
            return null;
        }

        byte[] compressed = switch (algorithm) {
            case LZ4 -> compressLz4(payload);
            case ZSTD -> compressZstd(payload);
            default -> null;
        };

        if (compressed != null && compressed.length < (int) (payload.length * 0.90)) {
            return new CompressedPayload(algorithm.marker(), payload.length, compressed);
        }
        return null;
    }

    public static void writeCompressedPayload(DataOutputStream out, CompressedPayload payload) throws IOException {
        out.writeInt(payload.marker());
        out.writeInt(payload.uncompressedLength());
        out.write(payload.compressedBytes());
    }

    public static byte[] readCompressedPayload(DataInputStream in, int marker, int remainingBytes) throws IOException {
        int uncompressedLength = in.readInt();
        int compressedLength = remainingBytes - Integer.BYTES;
        byte[] compressed = new byte[compressedLength];
        in.readFully(compressed);

        return switch (marker) {
            case LZ4_MARKER -> decompressLz4(compressed, uncompressedLength);
            case ZSTD_MARKER -> decompressZstd(compressed, uncompressedLength);
            default -> throw new IOException("unknown compression marker: " + marker);
        };
    }
}
