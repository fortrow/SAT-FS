package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

public final class BulkReadRequestCodec {
    public static final int VERSION = 1;
    public static final int MAX_PATH_BYTES = 64 * 1024;

    private static final byte[] MAGIC = "CXNFSRDQ".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private BulkReadRequestCodec() {
    }

    public static byte[] encode(BulkReadRequest request) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            writeUuid(out, request.sessionId());
            writeUuid(out, request.transferId());
            BinaryIO.writeUtf8(out, request.path());
            out.writeLong(request.offset());
            out.writeLong(request.length());
            out.writeInt(request.activeCords());
        }
        return bytes.toByteArray();
    }

    public static BulkReadRequest decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            BulkReadRequest request = new BulkReadRequest(
                    readUuid(in),
                    readUuid(in),
                    BinaryIO.readUtf8(in, MAX_PATH_BYTES),
                    in.readLong(),
                    in.readLong(),
                    in.readInt()
            );
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after bulk read request");
            }
            return request;
        }
    }

    private static void writeUuid(DataOutputStream out, UUID uuid) throws IOException {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }
}
