package com.fortrow.cxnfs.server;

import java.util.Locale;

public enum LiveWriteMode {
    POSITIONAL,
    CHUNK_FILES;

    public static final LiveWriteMode DEFAULT = POSITIONAL;

    public static LiveWriteMode parse(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT).replace('_', '-')) {
            case "positional" -> POSITIONAL;
            case "chunk-files", "chunkfiles", "chunks" -> CHUNK_FILES;
            default -> throw new IllegalArgumentException("unsupported live write mode: " + value);
        };
    }

    public String configValue() {
        return switch (this) {
            case POSITIONAL -> "positional";
            case CHUNK_FILES -> "chunk-files";
        };
    }
}
