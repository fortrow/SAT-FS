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

public final class BulkTransferMessageStream {
    public static final int MAX_ENCODED_MESSAGE_BYTES = Math.max(
            Math.max(BulkReadRequestStream.MAX_ENCODED_REQUEST_BYTES, BulkFrameStream.MAX_ENCODED_FRAME_BYTES),
            BulkWriteAckSubscriptionCodec.MAX_ENCODED_MESSAGE_BYTES
    );

    private BulkTransferMessageStream() {
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
        if (length <= 0 || length > MAX_ENCODED_MESSAGE_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk transfer message length: " + length);
        }
        return Optional.of(BinaryIO.readFixedBytes(in, length));
    }

    public static void write(OutputStream output, byte[] messageBytes) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        if (messageBytes == null) {
            throw new IllegalArgumentException("messageBytes is required");
        }
        if (messageBytes.length <= 0 || messageBytes.length > MAX_ENCODED_MESSAGE_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk transfer message length: " + messageBytes.length);
        }
        DataOutputStream out = new DataOutputStream(output);
        out.writeInt(messageBytes.length);
        out.write(messageBytes);
        out.flush();
    }
}
