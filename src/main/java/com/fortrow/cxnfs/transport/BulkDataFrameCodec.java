package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public final class BulkDataFrameCodec {
    public static final int VERSION = 1;
    public static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;

    private static final byte[] MAGIC = "CXNFSDAT".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private BulkDataFrameCodec() {
    }

    public static byte[] encode(BulkDataFrame frame) throws IOException {
        return encode(frame, null);
    }

    public static byte[] encodeCompressed(
            BulkDataFrame frame,
            BulkCompressionCodec.CompressionAlgorithm algorithm
    ) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        BulkCompressionCodec.CompressedPayload compressed = BulkCompressionCodec.compressIfBeneficial(
                frame.payloadView(), algorithm
        );
        if (compressed == null) {
            return encode(frame);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeFrameHeader(out, frame);
            BulkCompressionCodec.writeCompressedPayload(out, compressed);
        }
        return bytes.toByteArray();
    }

    public static int encodedLength(BulkDataFrame frame) throws IOException {
        requireRawEncodable(frame);
        return rawHeaderLength() + frame.payloadLength();
    }

    public static void write(OutputStream output, BulkDataFrame frame) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        requireRawEncodable(frame);
        DataOutputStream out = new DataOutputStream(output);
        writeRawHeader(out, frame);
        out.write(frame.payloadForEncoding());
    }

    public static byte[] encodeWithPayload(
            BulkDataFrame frame,
            BulkPayloadPatternCodec.EncodedPayload encodedPayload
    ) throws IOException {
        if (encodedPayload == null) {
            throw new IllegalArgumentException("encodedPayload is required");
        }
        return encode(frame, encodedPayload);
    }

    private static byte[] encode(
            BulkDataFrame frame,
            BulkPayloadPatternCodec.EncodedPayload encodedPayload
    ) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        if (frame.payloadLength() > MAX_PAYLOAD_BYTES) {
            throw new BinaryFormatException("bulk data frame payload exceeds maximum size");
        }
        if (encodedPayload != null && encodedPayload.originalLength() != frame.payloadLength()) {
            throw new BinaryFormatException("encoded bulk payload length does not match frame payload");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeFrameHeader(out, frame);
            if (encodedPayload == null) {
                BinaryIO.writeBytes(out, frame.payloadForEncoding());
            } else {
                BulkPayloadPatternCodec.writeEncodedPayload(out, encodedPayload);
            }
        }
        return bytes.toByteArray();
    }

    public static BulkDataFrame decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }
        if (bytes.length < MAGIC.length) {
            throw new BinaryFormatException("byte array too short for bulk data frame");
        }
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        BinaryIO.requireMagic(new DataInputStream(input), MAGIC);
        BulkDataFrame frame = decodeAfterMagic(input, bytes.length - MAGIC.length);
        if (input.available() != 0) {
            throw new BinaryFormatException("trailing bytes after bulk data frame");
        }
        return frame;
    }

    public static BulkDataFrame decodeAfterMagic(InputStream input, int remainingBytes) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (remainingBytes <= 0 || remainingBytes > BulkFrameStream.MAX_ENCODED_FRAME_BYTES - MAGIC.length) {
            throw new BinaryFormatException("invalid encoded bulk data frame length");
        }
        DataInputStream in = new DataInputStream(input);
        int consumed = 0;
        int version = in.readInt();
        consumed += Integer.BYTES;
        BinaryIO.requireVersion(version, VERSION);
        UUID transferId = readUuid(in);
        consumed += 2 * Long.BYTES;
        int cordIndex = in.readInt();
        consumed += Integer.BYTES;
        int entrySequence = in.readInt();
        consumed += Integer.BYTES;
        long chunkSequence = in.readLong();
        consumed += Long.BYTES;
        long entryOffset = in.readLong();
        consumed += Long.BYTES;
        boolean finalFrameForChunk = in.readBoolean();
        consumed += 1;
        int payloadLengthMarker = in.readInt();
        consumed += Integer.BYTES;
        int payloadBytesRemaining = remainingBytes - consumed;
        if (payloadBytesRemaining < 0) {
            throw new BinaryFormatException("invalid encoded bulk data frame length");
        }
        if (payloadLengthMarker >= 0) {
            if (payloadLengthMarker > MAX_PAYLOAD_BYTES) {
                throw new BinaryFormatException("bulk data frame payload exceeds maximum size");
            }
            if (payloadLengthMarker != payloadBytesRemaining) {
                throw new BinaryFormatException("bulk data frame raw payload length mismatch");
            }
            return BulkDataFrame.wrapPayloadForTransport(
                    transferId,
                    cordIndex,
                    entrySequence,
                    chunkSequence,
                    entryOffset,
                    finalFrameForChunk,
                    BinaryIO.readFixedBytes(in, payloadLengthMarker)
            );
        }
        if (payloadLengthMarker == BulkCompressionCodec.LZ4_MARKER || payloadLengthMarker == BulkCompressionCodec.ZSTD_MARKER) {
            byte[] uncompressed = BulkCompressionCodec.readCompressedPayload(in, payloadLengthMarker, payloadBytesRemaining);
            return BulkDataFrame.wrapPayloadForTransport(
                    transferId,
                    cordIndex,
                    entrySequence,
                    chunkSequence,
                    entryOffset,
                    finalFrameForChunk,
                    uncompressed
            );
        }
        ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(Integer.BYTES + payloadBytesRemaining);
        try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
            out.writeInt(payloadLengthMarker);
            out.write(BinaryIO.readFixedBytes(in, payloadBytesRemaining));
        }
        BulkPayloadPatternCodec.DecodedPayload payload = BulkPayloadPatternCodec.readDecodedPayload(
                new DataInputStream(new ByteArrayInputStream(payloadBytes.toByteArray())),
                MAX_PAYLOAD_BYTES
        );
        return new BulkDataFrame(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                payload
        );
    }

    private static void writeUuid(DataOutputStream out, UUID uuid) throws IOException {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    private static void writeRawHeader(DataOutputStream out, BulkDataFrame frame) throws IOException {
        writeFrameHeader(out, frame);
        out.writeInt(frame.payloadLength());
    }

    private static void writeFrameHeader(DataOutputStream out, BulkDataFrame frame) throws IOException {
        BinaryIO.writeMagic(out, MAGIC);
        out.writeInt(VERSION);
        writeUuid(out, frame.transferId());
        out.writeInt(frame.cordIndex());
        out.writeInt(frame.entrySequence());
        out.writeLong(frame.chunkSequence());
        out.writeLong(frame.entryOffset());
        out.writeBoolean(frame.finalFrameForChunk());
    }

    private static int rawHeaderLength() {
        return MAGIC.length
                + Integer.BYTES
                + (2 * Long.BYTES)
                + Integer.BYTES
                + Integer.BYTES
                + Long.BYTES
                + Long.BYTES
                + 1
                + Integer.BYTES;
    }

    private static void requireRawEncodable(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        if (frame.payloadLength() > MAX_PAYLOAD_BYTES) {
            throw new BinaryFormatException("bulk data frame payload exceeds maximum size");
        }
        if (frame.payloadPattern().isPresent()) {
            throw new BinaryFormatException("direct bulk frame streaming requires a raw payload frame");
        }
    }

    private static UUID readUuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }
}
