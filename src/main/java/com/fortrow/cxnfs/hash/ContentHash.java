package com.fortrow.cxnfs.hash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.zip.CRC32C;

public final class ContentHash {
    public static final int BYTE_LENGTH = 32;
    private static final byte[] CRC32C_MARKER = Arrays.copyOf(
            "cxnfs.crc32c.v1".getBytes(StandardCharsets.US_ASCII),
            16
    );

    private static final HexFormat HEX = HexFormat.of();

    private final byte[] bytes;

    private ContentHash(byte[] bytes) {
        if (bytes.length != BYTE_LENGTH) {
            throw new IllegalArgumentException("content hashes must be 32 bytes");
        }
        this.bytes = Arrays.copyOf(bytes, BYTE_LENGTH);
    }

    public static ContentHash of(byte[] bytes) {
        return new ContentHash(bytes);
    }

    public static ContentHash crc32c(String domain, long value, long length) {
        Objects.requireNonNull(domain, "domain");
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        byte[] bytes = new byte[BYTE_LENGTH];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put(CRC32C_MARKER);
        buffer.putInt((int) (value & 0xffff_ffffL));
        buffer.putLong(length);
        buffer.putInt(domainChecksum(domain));
        return new ContentHash(bytes);
    }

    public static ContentHash fromHex(String hex) {
        Objects.requireNonNull(hex, "hex");
        return new ContentHash(HEX.parseHex(hex));
    }

    public byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public String hex() {
        return HEX.formatHex(bytes);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ContentHash that && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return hex();
    }

    private static int domainChecksum(String domain) {
        CRC32C checksum = new CRC32C();
        byte[] bytes = domain.getBytes(StandardCharsets.US_ASCII);
        checksum.update(bytes, 0, bytes.length);
        return (int) checksum.getValue();
    }
}
