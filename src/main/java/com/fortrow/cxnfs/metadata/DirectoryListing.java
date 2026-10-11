package com.fortrow.cxnfs.metadata;

import java.util.Comparator;
import java.util.List;

public record DirectoryListing(
        String path,
        List<FileMetadata> entries
) {
    public DirectoryListing {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        entries = entries == null ? List.of() : entries.stream()
                .sorted(Comparator.comparing(FileMetadata::type).thenComparing(FileMetadata::name))
                .toList();
    }
}
