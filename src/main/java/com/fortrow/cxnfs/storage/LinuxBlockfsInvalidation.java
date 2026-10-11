package com.fortrow.cxnfs.storage;

public record LinuxBlockfsInvalidation(
        long inodeNumber,
        long offset,
        long length
) {
    public LinuxBlockfsInvalidation {
        if (inodeNumber <= 0) {
            throw new IllegalArgumentException("inodeNumber must be positive");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
    }
}
