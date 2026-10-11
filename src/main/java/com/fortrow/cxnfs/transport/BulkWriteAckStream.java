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

public final class BulkWriteAckStream {
    private BulkWriteAckStream() {
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
        if (length <= 0 || length > BulkWriteAckCodec.MAX_ENCODED_ACK_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk write ack length: " + length);
        }
        return Optional.of(BinaryIO.readFixedBytes(in, length));
    }

    public static void write(OutputStream output, byte[] ackBytes) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        if (ackBytes == null) {
            throw new IllegalArgumentException("ackBytes is required");
        }
        if (ackBytes.length <= 0 || ackBytes.length > BulkWriteAckCodec.MAX_ENCODED_ACK_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk write ack length: " + ackBytes.length);
        }
        DataOutputStream out = new DataOutputStream(output);
        out.writeInt(ackBytes.length);
        out.write(ackBytes);
        out.flush();
    }
}
