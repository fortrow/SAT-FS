package com.fortrow.cxnfs.storage;

import java.nio.file.Path;

public record LinuxBlockfsNamespaceSyncResult(
        Path metadataRoot,
        long generation,
        int publishedFiles,
        int manifestFiles,
        int manifestChunks,
        int auditedExtentHints,
        int attemptedExtents,
        int acceptedExtents,
        int unsupportedExtents,
        long acceptedExtentBytes
) {
    public LinuxBlockfsNamespaceSyncResult(Path metadataRoot, long generation, int publishedFiles) {
        this(metadataRoot, generation, publishedFiles, 0, 0, 0, 0, 0, 0, 0);
    }

    public LinuxBlockfsNamespaceSyncResult {
        if (metadataRoot == null) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        if (publishedFiles < 0) {
            throw new IllegalArgumentException("publishedFiles must be non-negative");
        }
        if (manifestFiles < 0) {
            throw new IllegalArgumentException("manifestFiles must be non-negative");
        }
        if (manifestChunks < 0) {
            throw new IllegalArgumentException("manifestChunks must be non-negative");
        }
        if (auditedExtentHints < 0) {
            throw new IllegalArgumentException("auditedExtentHints must be non-negative");
        }
        if (attemptedExtents < 0) {
            throw new IllegalArgumentException("attemptedExtents must be non-negative");
        }
        if (acceptedExtents < 0) {
            throw new IllegalArgumentException("acceptedExtents must be non-negative");
        }
        if (unsupportedExtents < 0) {
            throw new IllegalArgumentException("unsupportedExtents must be non-negative");
        }
        if (acceptedExtentBytes < 0) {
            throw new IllegalArgumentException("acceptedExtentBytes must be non-negative");
        }
        if (acceptedExtents + unsupportedExtents > attemptedExtents) {
            throw new IllegalArgumentException("accepted plus unsupported cannot exceed attempts");
        }
    }
}
