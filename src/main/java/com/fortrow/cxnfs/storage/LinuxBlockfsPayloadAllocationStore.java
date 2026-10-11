package com.fortrow.cxnfs.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public final class LinuxBlockfsPayloadAllocationStore {
    public static final String RELATIVE_PATH = "payload-map/current.cxn";
    private static final int FORMAT_VERSION = 1;

    public Path snapshotPath(Path metadataRoot) {
        if (metadataRoot == null) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        return metadataRoot.resolve(RELATIVE_PATH);
    }

    public Optional<LinuxBlockfsPayloadAllocationSnapshot> loadIfExists(Path metadataRoot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        return Optional.of(loadFile(path));
    }

    public LinuxBlockfsPayloadAllocationSnapshot load(Path metadataRoot) throws IOException {
        return loadFile(snapshotPath(metadataRoot));
    }

    public LinuxBlockfsPayloadAllocationSnapshot loadFile(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        }
        int version = intValue(properties, "version");
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported payload allocation version: " + version);
        }
        long generation = longValue(properties, "generation");
        int count = intValue(properties, "allocation.count");
        List<LinuxBlockfsPayloadAllocation> allocations = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            String prefix = "allocation." + index + ".";
            allocations.add(new LinuxBlockfsPayloadAllocation(
                    longValue(properties, prefix + "inode"),
                    longValue(properties, prefix + "file-offset"),
                    longValue(properties, prefix + "payload-block"),
                    intValue(properties, prefix + "payload-offset"),
                    intValue(properties, prefix + "payload-length")
            ));
        }
        return new LinuxBlockfsPayloadAllocationSnapshot(generation, allocations);
    }

    public Path write(Path metadataRoot, LinuxBlockfsPayloadAllocationSnapshot snapshot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        Files.createDirectories(path.getParent());
        writeFile(path, snapshot);
        return path;
    }

    public void writeFile(Path path, LinuxBlockfsPayloadAllocationSnapshot snapshot) throws IOException {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot is required");
        }
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Properties properties = new Properties();
        properties.setProperty("version", Integer.toString(FORMAT_VERSION));
        properties.setProperty("generation", Long.toString(snapshot.generation()));
        properties.setProperty("allocation.count", Integer.toString(snapshot.allocations().size()));
        for (int index = 0; index < snapshot.allocations().size(); index++) {
            LinuxBlockfsPayloadAllocation allocation = snapshot.allocations().get(index);
            String prefix = "allocation." + index + ".";
            properties.setProperty(prefix + "inode", Long.toString(allocation.inodeNumber()));
            properties.setProperty(prefix + "file-offset", Long.toString(allocation.fileOffset()));
            properties.setProperty(prefix + "payload-block", Long.toString(allocation.payloadBlock()));
            properties.setProperty(prefix + "payload-offset", Integer.toString(allocation.payloadOffset()));
            properties.setProperty(prefix + "payload-length", Integer.toString(allocation.payloadLength()));
        }

        try (OutputStream out = Files.newOutputStream(path)) {
            properties.store(out, "CXNFS blockfs payload allocation map");
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing payload allocation field: " + key);
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
