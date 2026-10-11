package com.fortrow.cxnfs.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public final class LinuxBlockfsExtentHintStore {
    public static final String RELATIVE_PATH = "extent-hints/current.cxn";
    private static final int FORMAT_VERSION = 1;
    private static final HexFormat HEX = HexFormat.of();

    public Path snapshotPath(Path metadataRoot) {
        if (metadataRoot == null) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        return metadataRoot.resolve(RELATIVE_PATH);
    }

    public Optional<LinuxBlockfsExtentHintsSnapshot> loadIfExists(Path metadataRoot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        return Optional.of(loadFile(path));
    }

    public LinuxBlockfsExtentHintsSnapshot load(Path metadataRoot) throws IOException {
        return loadFile(snapshotPath(metadataRoot));
    }

    public LinuxBlockfsExtentHintsSnapshot loadFile(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        }
        int version = intValue(properties, "version");
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported extent hint snapshot version: " + version);
        }
        long generation = longValue(properties, "generation");
        int count = intValue(properties, "extent.count");
        List<LinuxBlockfsExtent> extents = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            String prefix = "extent." + index + ".";
            extents.add(new LinuxBlockfsExtent(
                    longValue(properties, prefix + "inode"),
                    longValue(properties, prefix + "file-offset"),
                    longValue(properties, prefix + "payload-block"),
                    intValue(properties, prefix + "payload-offset"),
                    intValue(properties, prefix + "payload-length"),
                    hashValue(properties, prefix + "hash")
            ));
        }
        return new LinuxBlockfsExtentHintsSnapshot(generation, extents);
    }

    public Path write(Path metadataRoot, LinuxBlockfsExtentHintsSnapshot snapshot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        Files.createDirectories(path.getParent());
        writeFile(path, snapshot);
        return path;
    }

    public void writeFile(Path path, LinuxBlockfsExtentHintsSnapshot snapshot) throws IOException {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot is required");
        }
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Properties properties = new Properties();
        properties.setProperty("version", Integer.toString(FORMAT_VERSION));
        properties.setProperty("generation", Long.toString(snapshot.generation()));
        properties.setProperty("extent.count", Integer.toString(snapshot.extents().size()));
        for (int index = 0; index < snapshot.extents().size(); index++) {
            LinuxBlockfsExtent extent = snapshot.extents().get(index);
            String prefix = "extent." + index + ".";
            properties.setProperty(prefix + "inode", Long.toString(extent.inodeNumber()));
            properties.setProperty(prefix + "file-offset", Long.toString(extent.fileOffset()));
            properties.setProperty(prefix + "payload-block", Long.toString(extent.payloadBlock()));
            properties.setProperty(prefix + "payload-offset", Integer.toString(extent.payloadOffset()));
            properties.setProperty(prefix + "payload-length", Integer.toString(extent.payloadLength()));
            properties.setProperty(prefix + "hash", extent.expectedHashHex());
        }

        try (OutputStream out = Files.newOutputStream(path)) {
            properties.store(out, "CXNFS blockfs extent hints");
        }
    }

    private static byte[] hashValue(Properties properties, String key) {
        String value = required(properties, key);
        try {
            byte[] hash = HEX.parseHex(value);
            if (hash.length != 32) {
                throw new IllegalArgumentException("extent hash must be 32 bytes of hex: " + key);
            }
            return hash;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("extent hash must be 32 bytes of hex: " + key, e);
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing extent hint field: " + key);
        }
        return value;
    }

    private static int intValue(Properties properties, String key) {
        return Math.toIntExact(longValue(properties, key));
    }

    private static long longValue(Properties properties, String key) {
        return Long.parseLong(required(properties, key));
    }
}
