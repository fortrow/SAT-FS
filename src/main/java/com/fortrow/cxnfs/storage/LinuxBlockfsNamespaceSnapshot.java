package com.fortrow.cxnfs.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record LinuxBlockfsNamespaceSnapshot(
        long generation,
        List<LinuxBlockfsFile> files
) {
    public LinuxBlockfsNamespaceSnapshot {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        files = files == null ? List.of() : List.copyOf(files);
        validateFiles(files);
    }

    private static void validateFiles(List<LinuxBlockfsFile> files) {
        Set<Long> inodes = new HashSet<>();
        Set<String> paths = new HashSet<>();
        for (LinuxBlockfsFile file : files) {
            if (!inodes.add(file.inodeNumber())) {
                throw new IllegalArgumentException("duplicate namespace inode: " + file.inodeNumber());
            }
            if (!paths.add(file.path())) {
                throw new IllegalArgumentException("duplicate namespace path: " + file.path());
            }
        }
        for (LinuxBlockfsFile file : files) {
            String prefix = file.path() + "/";
            for (String path : paths) {
                if (path.startsWith(prefix)) {
                    throw new IllegalArgumentException("file path conflicts with implicit directory: " + file.path());
                }
            }
        }
    }
}
