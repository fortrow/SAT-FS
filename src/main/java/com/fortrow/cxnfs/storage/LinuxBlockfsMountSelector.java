package com.fortrow.cxnfs.storage;

import java.nio.file.Path;

public record LinuxBlockfsMountSelector(long mountId, String metadataRoot) {
    public static LinuxBlockfsMountSelector unselected() {
        return new LinuxBlockfsMountSelector(0, "");
    }

    public static LinuxBlockfsMountSelector mountId(long mountId) {
        return new LinuxBlockfsMountSelector(mountId, "");
    }

    public static LinuxBlockfsMountSelector metadataRoot(Path metadataRoot) {
        if (metadataRoot == null) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        return metadataRoot(metadataRoot.normalize().toString());
    }

    public static LinuxBlockfsMountSelector metadataRoot(String metadataRoot) {
        if (metadataRoot == null || metadataRoot.isBlank()) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        return new LinuxBlockfsMountSelector(0, Path.of(metadataRoot.trim()).normalize().toString());
    }

    public LinuxBlockfsMountSelector {
        if (mountId < 0) {
            throw new IllegalArgumentException("mountId must be non-negative");
        }
        metadataRoot = metadataRoot == null ? "" : metadataRoot;
        if (mountId == 0 && metadataRoot.isBlank()) {
            metadataRoot = "";
        } else if (!metadataRoot.isBlank()) {
            metadataRoot = metadataRoot.trim();
        }
        if (mountId > 0 && !metadataRoot.isBlank()) {
            throw new IllegalArgumentException("select by mountId or metadataRoot, not both");
        }
    }

    public boolean selected() {
        return mountId > 0 || !metadataRoot.isBlank();
    }
}
