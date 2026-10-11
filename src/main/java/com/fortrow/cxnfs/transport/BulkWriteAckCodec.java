package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class BulkWriteAckCodec {
    public static final int VERSION = 1;
    public static final int MAX_ENCODED_ACK_BYTES = 256;

    private static final byte[] MAGIC = "CXNFSWAK".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_STORAGE_NAME_BYTES = 16;

    private BulkWriteAckCodec() {
    }

    public static byte[] encode(BulkWriteAck ack) throws IOException {
        if (ack == null) {
            throw new IllegalArgumentException("ack is required");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            writeUuid(out, ack.key().transferId());
            out.writeInt(ack.key().entrySequence());
            out.writeLong(ack.key().chunkSequence());
            out.writeInt(ack.payloadLength());
            out.writeBoolean(ack.metadataVerified());
            BinaryIO.writeUtf8(out, ack.storage().name());
        }
        return bytes.toByteArray();
    }

    public static BulkWriteAck decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }
        if (bytes.length <= 0 || bytes.length > MAX_ENCODED_ACK_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk write ack length: " + bytes.length);
        }
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            BulkWriteAck ack = new BulkWriteAck(
                    new BulkDataFrameKey(readUuid(in), in.readInt(), in.readLong()),
                    in.readInt(),
                    in.readBoolean(),
                    StagedPayloadStorage.valueOf(BinaryIO.readUtf8(in, MAX_STORAGE_NAME_BYTES))
            );
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after bulk write ack");
            }
            return ack;
        } catch (IllegalArgumentException e) {
            throw new BinaryFormatException("invalid bulk write ack field");
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
