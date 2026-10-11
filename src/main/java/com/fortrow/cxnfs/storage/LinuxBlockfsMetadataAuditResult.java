package com.fortrow.cxnfs.storage;

public record LinuxBlockfsMetadataAuditResult(
        int namespaceFiles,
        int manifestFiles,
        int manifestChunks,
        int extentHints
) {
    public LinuxBlockfsMetadataAuditResult {
        if (namespaceFiles < 0) {
            throw new IllegalArgumentException("namespaceFiles must be non-negative");
        }
        if (manifestFiles < 0) {
            throw new IllegalArgumentException("manifestFiles must be non-negative");
        }
        if (manifestChunks < 0) {
            throw new IllegalArgumentException("manifestChunks must be non-negative");
        }
        if (extentHints < 0) {
            throw new IllegalArgumentException("extentHints must be non-negative");
        }
    }
}
