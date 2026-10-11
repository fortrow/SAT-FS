package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.FileManifest;

import java.util.ArrayList;
import java.util.List;

public record LinuxBlockfsManifestEntry(
        long inodeNumber,
        String path,
        long size,
        ContentHash fileHash,
        List<LinuxBlockfsManifestChunk> chunks
) {
    public LinuxBlockfsManifestEntry {
        if (fileHash == null) {
            throw new IllegalArgumentException("fileHash is required");
        }
        new LinuxBlockfsFile(inodeNumber, path, size, 0444);
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
        validateChunks(size, chunks);
    }

    public static LinuxBlockfsManifestEntry fromManifest(long inodeNumber, FileManifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        List<LinuxBlockfsManifestChunk> chunks = new ArrayList<>(manifest.chunks().size());
        long offset = 0;
        for (var chunk : manifest.chunks()) {
            chunks.add(new LinuxBlockfsManifestChunk(offset, chunk.size(), chunk.hash()));
            offset = Math.addExact(offset, chunk.size());
        }
        return new LinuxBlockfsManifestEntry(
                inodeNumber,
                trimLeadingSlash(manifest.path()),
                manifest.size(),
                manifest.fileHash(),
                chunks
        );
    }

    private static void validateChunks(long size, List<LinuxBlockfsManifestChunk> chunks) {
        long expectedOffset = 0;
        for (LinuxBlockfsManifestChunk chunk : chunks) {
            if (chunk.fileOffset() != expectedOffset) {
                throw new IllegalArgumentException("manifest chunks must be contiguous");
            }
            expectedOffset = Math.addExact(expectedOffset, chunk.length());
        }
        if (expectedOffset != size) {
            throw new IllegalArgumentException("manifest chunk lengths must equal file size");
        }
    }

    private static String trimLeadingSlash(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
