package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.namespace.NamespacePath;

import java.nio.file.Path;

public record ServedRoot(String remotePrefix, Path localDirectory) {
    public ServedRoot {
        if (remotePrefix == null || remotePrefix.isBlank()) {
            throw new IllegalArgumentException("remotePrefix is required");
        }
        if (localDirectory == null) {
            throw new IllegalArgumentException("localDirectory is required");
        }
        remotePrefix = NamespacePath.normalizeDirectory(remotePrefix);
        localDirectory = localDirectory.toAbsolutePath().normalize();
    }

    public static ServedRoot root(Path directory) {
        return new ServedRoot("/", directory);
    }
}
