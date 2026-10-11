package com.fortrow.cxnfs.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class LinuxBlockfsNamespaceStore {
    public static final String RELATIVE_PATH = "namespace/current.cxn";
    private static final int FORMAT_VERSION = 1;

    public Path snapshotPath(Path metadataRoot) {
        if (metadataRoot == null) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        return metadataRoot.resolve(RELATIVE_PATH);
    }

    public LinuxBlockfsNamespaceSnapshot load(Path metadataRoot) throws IOException {
        return loadFile(snapshotPath(metadataRoot));
    }

    public LinuxBlockfsNamespaceSnapshot loadFile(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        }
        int version = intValue(properties, "version");
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported namespace snapshot version: " + version);
        }
        long generation = longValue(properties, "generation");
        int count = intValue(properties, "file.count");
        List<LinuxBlockfsFile> files = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            String prefix = "file." + index + ".";
            files.add(new LinuxBlockfsFile(
                    longValue(properties, prefix + "inode"),
                    required(properties, prefix + "path"),
                    longValue(properties, prefix + "size"),
                    modeValue(properties, prefix + "mode")
            ));
        }
        return new LinuxBlockfsNamespaceSnapshot(generation, files);
    }

    public Path write(Path metadataRoot, LinuxBlockfsNamespaceSnapshot snapshot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        Files.createDirectories(path.getParent());
        writeFile(path, snapshot);
        return path;
    }

    public void writeFile(Path path, LinuxBlockfsNamespaceSnapshot snapshot) throws IOException {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot is required");
        }
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Properties properties = new Properties();
        properties.setProperty("version", Integer.toString(FORMAT_VERSION));
        properties.setProperty("generation", Long.toString(snapshot.generation()));
        properties.setProperty("file.count", Integer.toString(snapshot.files().size()));
        for (int index = 0; index < snapshot.files().size(); index++) {
            LinuxBlockfsFile file = snapshot.files().get(index);
            String prefix = "file." + index + ".";
            properties.setProperty(prefix + "inode", Long.toString(file.inodeNumber()));
            properties.setProperty(prefix + "path", file.path());
            properties.setProperty(prefix + "size", Long.toString(file.size()));
            properties.setProperty(prefix + "mode", "0" + Integer.toOctalString(file.mode()));
        }

        try (OutputStream out = Files.newOutputStream(path)) {
            properties.store(out, "CXNFS blockfs namespace snapshot");
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing namespace field: " + key);
        }
        return value;
    }

    private static int intValue(Properties properties, String key) {
        return Math.toIntExact(longValue(properties, key));
    }

    private static long longValue(Properties properties, String key) {
        return Long.parseLong(required(properties, key));
    }

    private static int modeValue(Properties properties, String key) {
        String value = required(properties, key);
        if (!value.startsWith("0")) {
            throw new IllegalArgumentException("mode must be octal: " + key);
        }
        return Integer.parseInt(value, 8);
    }
}
