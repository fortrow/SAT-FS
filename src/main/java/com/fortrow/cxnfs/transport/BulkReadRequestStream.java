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

public final class BulkReadRequestStream {
    public static final int MAX_ENCODED_REQUEST_BYTES = BulkReadRequestCodec.MAX_PATH_BYTES + 128;

    private BulkReadRequestStream() {
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
        if (length <= 0 || length > MAX_ENCODED_REQUEST_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk read request length: " + length);
        }
        return Optional.of(BinaryIO.readFixedBytes(in, length));
    }

    public static void write(OutputStream output, byte[] requestBytes) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        if (requestBytes == null) {
            throw new IllegalArgumentException("requestBytes is required");
        }
        if (requestBytes.length <= 0 || requestBytes.length > MAX_ENCODED_REQUEST_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk read request length: " + requestBytes.length);
        }
        DataOutputStream out = new DataOutputStream(output);
        out.writeInt(requestBytes.length);
        out.write(requestBytes);
        out.flush();
    }
}
