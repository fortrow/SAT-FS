package com.fortrow.cxnfs.codec;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class Lz4FrameCompressionCodec implements CompressionCodec {
    public static final byte ID = 1;
    public static final int VERSION = 1;

    private static final byte[] MAGIC = "CXNFSLZ4".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private final Lz4Compressor compressor = new Lz4Compressor();
    private final Lz4Decompressor decompressor = new Lz4Decompressor();

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public String name() {
        return "lz4-frame";
    }

    @Override
    public byte[] compress(byte[] input) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        byte[] compressed = new byte[compressor.maxCompressedLength(input.length)];
        int compressedLength = compressor.compress(input, 0, input.length, compressed, 0, compressed.length);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream(MAGIC.length + 12 + compressedLength);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            out.writeInt(input.length);
            out.writeInt(compressedLength);
            out.write(compressed, 0, compressedLength);
        }
        return bytes.toByteArray();
    }

    @Override
    public byte[] decompress(byte[] input, int expectedSize) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expectedSize must be non-negative");
        }

        ByteArrayInputStream bytes = new ByteArrayInputStream(input);
        try (DataInputStream in = new DataInputStream(bytes)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            int originalSize = in.readInt();
            if (originalSize != expectedSize) {
                throw new BinaryFormatException("LZ4 frame original size does not match expected size");
            }
            int compressedLength = in.readInt();
            if (compressedLength < 0 || compressedLength > input.length) {
                throw new BinaryFormatException("invalid LZ4 compressed length");
            }
            byte[] compressed = BinaryIO.readFixedBytes(in, compressedLength);
            if (bytes.available() != 0) {
                throw new BinaryFormatException("trailing bytes after LZ4 frame");
            }

            byte[] output = new byte[expectedSize];
            try {
                int decompressed = decompressor.decompress(compressed, 0, compressed.length, output, 0, output.length);
                if (decompressed != expectedSize) {
                    throw new BinaryFormatException("LZ4 decompressed size does not match expected size");
                }
                return output;
            } catch (MalformedInputException e) {
                throw new BinaryFormatException("malformed LZ4 frame payload: " + e.getMessage());
            }
        }
    }
}
