package com.fortrow.cxnfs.storage;

import java.nio.charset.StandardCharsets;

public record LinuxBlockfsFile(
        long inodeNumber,
        String path,
        long size,
        int mode
) {
    private static final int PATH_MAX = 256;

    public LinuxBlockfsFile {
        if (inodeNumber <= 0) {
            throw new IllegalArgumentException("inodeNumber must be positive");
        }
        path = validatePath(path);
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        if (mode < 0 || mode > 0777) {
            throw new IllegalArgumentException("mode must fit in permission bits");
        }
    }

    public static LinuxBlockfsFile readOnly(long inodeNumber, String path, long size) {
        return new LinuxBlockfsFile(inodeNumber, path, size, 0444);
    }

    private static String validatePath(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path must be a relative file path");
        }
        if (path.startsWith("/") || path.endsWith("/") || path.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("path must be a relative file path");
        }
        if (path.getBytes(StandardCharsets.UTF_8).length >= PATH_MAX) {
            throw new IllegalArgumentException("path is too long for blockfs control ABI");
        }
        for (String component : path.split("/", -1)) {
            if (component.isEmpty() || component.equals(".") || component.equals("..")) {
                throw new IllegalArgumentException("path must not contain empty, dot, or parent components");
            }
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);

            if (c < 0x20 || c == 0x7f) {
                throw new IllegalArgumentException("path must not contain control characters");
            }
        }
        return path;
    }
}
