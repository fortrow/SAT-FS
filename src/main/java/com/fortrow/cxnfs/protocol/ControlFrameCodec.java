package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public final class ControlFrameCodec {
    public static final int VERSION = 1;
    public static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;

    private static final byte[] MAGIC = "CXNFSPRO".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private ControlFrameCodec() {
    }

    public static byte[] encode(ControlFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        if (frame.payload().length > MAX_PAYLOAD_BYTES) {
            throw new BinaryFormatException("control frame payload exceeds maximum size");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            out.writeInt(frame.version().major());
            out.writeInt(frame.version().minor());
            writeUuid(out, frame.messageId());
            out.writeBoolean(frame.sessionId().isPresent());
            if (frame.sessionId().isPresent()) {
                writeUuid(out, frame.sessionId().orElseThrow());
            }
            out.writeInt(frame.operation().code());
            BinaryIO.writeBytes(out, frame.payload());
        }
        return bytes.toByteArray();
    }

    public static ControlFrame decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            ProtocolVersion version = new ProtocolVersion(in.readInt(), in.readInt());
            UUID messageId = readUuid(in);
            Optional<UUID> sessionId = in.readBoolean() ? Optional.of(readUuid(in)) : Optional.empty();
            ControlOperation operation = ControlOperation.fromCode(in.readInt());
            byte[] payload = BinaryIO.readBytes(in, MAX_PAYLOAD_BYTES);
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after control frame");
            }
            return new ControlFrame(version, messageId, sessionId, operation, payload);
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
