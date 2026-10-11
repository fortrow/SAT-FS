package com.fortrow.cxnfs.storage;

import java.util.HexFormat;

public record LinuxBlockfsExtent(
        long inodeNumber,
        long fileOffset,
        long payloadBlock,
        int payloadOffset,
        int payloadLength,
        byte[] expectedHash
) {
    public static final int PAYLOAD_BLOCK_SIZE = LinuxBlockfsPayloadAllocation.PAYLOAD_BLOCK_SIZE;

    public LinuxBlockfsExtent {
        if (inodeNumber <= 0) {
            throw new IllegalArgumentException("inodeNumber must be positive");
        }
        if (fileOffset < 0) {
            throw new IllegalArgumentException("fileOffset must be non-negative");
        }
        if (payloadBlock < 0) {
            throw new IllegalArgumentException("payloadBlock must be non-negative");
        }
        if (payloadOffset < 0) {
            throw new IllegalArgumentException("payloadOffset must be non-negative");
        }
        if (payloadOffset >= PAYLOAD_BLOCK_SIZE) {
            throw new IllegalArgumentException("payloadOffset must be within the payload block");
        }
        if (payloadLength <= 0) {
            throw new IllegalArgumentException("payloadLength must be positive");
        }
        if (expectedHash == null || expectedHash.length != 32) {
            throw new IllegalArgumentException("expectedHash must be 32 bytes");
        }
        expectedHash = expectedHash.clone();
    }

    @Override
    public byte[] expectedHash() {
        return expectedHash.clone();
    }

    public String expectedHashHex() {
        return HexFormat.of().formatHex(expectedHash);
    }
}
