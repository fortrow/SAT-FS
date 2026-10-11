package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

public final class BulkFrameStream {
    public static final int MAX_ENCODED_FRAME_BYTES = BulkDataFrameCodec.MAX_PAYLOAD_BYTES + 1024;

    private BulkFrameStream() {
    }

    public static Optional<byte[]> read(InputStream input) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        DataInputStream in = new DataInputStream(input);
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            return Optional.empty();
        }
        if (length <= 0 || length > MAX_ENCODED_FRAME_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk frame length: " + length);
        }
        return Optional.of(BinaryIO.readFixedBytes(in, length));
    }

    public static void write(OutputStream output, byte[] frameBytes) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        if (frameBytes == null) {
            throw new IllegalArgumentException("frameBytes is required");
        }
        if (frameBytes.length <= 0 || frameBytes.length > MAX_ENCODED_FRAME_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk frame length: " + frameBytes.length);
        }
        DataOutputStream out = new DataOutputStream(output);
        out.writeInt(frameBytes.length);
        out.write(frameBytes);
        out.flush();
    }

    public static void write(OutputStream output, BulkDataFrame frame) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        int length = BulkDataFrameCodec.encodedLength(frame);
        if (length <= 0 || length > MAX_ENCODED_FRAME_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk frame length: " + length);
        }
        DataOutputStream out = new DataOutputStream(output);
        out.writeInt(length);
        BulkDataFrameCodec.write(out, frame);
        out.flush();
    }
}
