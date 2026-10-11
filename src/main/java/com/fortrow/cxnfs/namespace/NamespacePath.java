package com.fortrow.cxnfs.namespace;

public final class NamespacePath {
    private NamespacePath() {
    }

    public static String normalize(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        String normalized = path.startsWith("/") ? path : "/" + path;
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        requireCleanSegments(path, normalized);
        return normalized;
    }

    public static String normalizeDirectory(String path) {
        return normalize(path == null || path.isBlank() ? "/" : path);
    }

    public static void requireUnderRoot(String rootPath, String path) {
        String normalizedRoot = normalize(rootPath);
        String normalizedPath = normalize(path);
        if (normalizedRoot.equals("/")) {
            return;
        }
        if (!normalizedPath.equals(normalizedRoot) && !normalizedPath.startsWith(normalizedRoot + "/")) {
            throw new IllegalArgumentException("path is outside admitted transfer root: " + path);
        }
    }

    private static void requireCleanSegments(String original, String normalized) {
        if (normalized.equals("/")) {
            return;
        }
        for (String part : normalized.substring(1).split("/")) {
            if (part.equals(".") || part.equals("..")) {
                throw new IllegalArgumentException("path must not contain traversal segments: " + original);
            }
            for (int index = 0; index < part.length(); index++) {
                if (Character.isISOControl(part.charAt(index))) {
                    throw new IllegalArgumentException("path must not contain control characters: " + original);
                }
            }
        }
    }
}
