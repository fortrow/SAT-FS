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

public final class BulkWriteAckSubscriptionCodec {
    public static final int VERSION = 1;
    public static final int MAX_ENCODED_MESSAGE_BYTES = 64;

    static final byte[] SUBSCRIBE_MAGIC = "CXNFSWAS".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] READY_MAGIC = "CXNFSWAR".getBytes(StandardCharsets.US_ASCII);

    private BulkWriteAckSubscriptionCodec() {
    }

    public static boolean isSubscribe(byte[] bytes) {
        return startsWith(bytes, SUBSCRIBE_MAGIC);
    }

    public static byte[] encodeSubscribe(BulkWriteAckSubscription subscription) throws IOException {
        if (subscription == null) {
            throw new IllegalArgumentException("subscription is required");
        }
        return encode(SUBSCRIBE_MAGIC, subscription.transferId());
    }

    public static BulkWriteAckSubscription decodeSubscribe(byte[] bytes) throws IOException {
        return new BulkWriteAckSubscription(decode(bytes, SUBSCRIBE_MAGIC));
    }

    public static byte[] encodeReady(UUID transferId) throws IOException {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        return encode(READY_MAGIC, transferId);
    }

    public static UUID decodeReady(byte[] bytes) throws IOException {
        return decode(bytes, READY_MAGIC);
    }

    private static byte[] encode(byte[] magic, UUID transferId) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, magic);
            out.writeInt(VERSION);
            writeUuid(out, transferId);
        }
        return bytes.toByteArray();
    }

    private static UUID decode(byte[] bytes, byte[] expectedMagic) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }
        if (bytes.length <= 0 || bytes.length > MAX_ENCODED_MESSAGE_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk write ack subscription length: " + bytes.length);
        }
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, expectedMagic);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            UUID transferId = readUuid(in);
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after bulk write ack subscription");
            }
            return transferId;
        }
    }

    private static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes == null || bytes.length < prefix.length) {
            return false;
        }
        for (int index = 0; index < prefix.length; index++) {
            if (bytes[index] != prefix[index]) {
                return false;
            }
        }
        return true;
    }

    private static void writeUuid(DataOutputStream out, UUID uuid) throws IOException {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }
}
