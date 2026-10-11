package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.manifest.FileManifest;

import java.util.List;

record DiskRepositoryJournalEntry(
        List<String> deletePaths,
        List<FileManifest> putManifests
) {
    DiskRepositoryJournalEntry {
        if (deletePaths == null) {
            throw new IllegalArgumentException("deletePaths is required");
        }
        if (putManifests == null) {
            throw new IllegalArgumentException("putManifests is required");
        }
        deletePaths = List.copyOf(deletePaths);
        putManifests = List.copyOf(putManifests);
        for (String path : deletePaths) {
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("deletePaths must not contain blank paths");
            }
        }
        for (FileManifest manifest : putManifests) {
            if (manifest == null) {
                throw new IllegalArgumentException("putManifests must not contain null");
            }
        }
    }

    boolean empty() {
        return deletePaths.isEmpty() && putManifests.isEmpty();
    }
}
