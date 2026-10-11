package com.fortrow.cxnfs.storage;

public record LinuxBlockfsPayloadAllocation(
        long inodeNumber,
        long fileOffset,
        long payloadBlock,
        int payloadOffset,
        int payloadLength
) {
    public static final int PAYLOAD_BLOCK_SIZE = 4096;

    public LinuxBlockfsPayloadAllocation {
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
    }
}
