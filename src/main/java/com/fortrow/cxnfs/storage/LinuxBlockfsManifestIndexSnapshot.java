package com.fortrow.cxnfs.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record LinuxBlockfsManifestIndexSnapshot(
        long generation,
        List<LinuxBlockfsManifestEntry> entries
) {
    public LinuxBlockfsManifestIndexSnapshot {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        entries = entries == null ? List.of() : List.copyOf(entries);
        validateEntries(entries);
    }

    public int chunkCount() {
        return entries.stream().mapToInt(entry -> entry.chunks().size()).sum();
    }

    private static void validateEntries(List<LinuxBlockfsManifestEntry> entries) {
        Set<Long> inodes = new HashSet<>();
        Set<String> paths = new HashSet<>();
        for (LinuxBlockfsManifestEntry entry : entries) {
            if (!inodes.add(entry.inodeNumber())) {
                throw new IllegalArgumentException("duplicate manifest inode: " + entry.inodeNumber());
            }
            if (!paths.add(entry.path())) {
                throw new IllegalArgumentException("duplicate manifest path: " + entry.path());
            }
        }
    }
}
