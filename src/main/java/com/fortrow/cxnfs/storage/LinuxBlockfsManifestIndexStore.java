package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public final class LinuxBlockfsManifestIndexStore {
    public static final String RELATIVE_PATH = "manifest-index/current.cxn";
    private static final int FORMAT_VERSION = 1;

    public Path snapshotPath(Path metadataRoot) {
        if (metadataRoot == null) {
            throw new IllegalArgumentException("metadataRoot is required");
        }
        return metadataRoot.resolve(RELATIVE_PATH);
    }

    public Optional<LinuxBlockfsManifestIndexSnapshot> loadIfExists(Path metadataRoot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        return Optional.of(loadFile(path));
    }

    public LinuxBlockfsManifestIndexSnapshot load(Path metadataRoot) throws IOException {
        return loadFile(snapshotPath(metadataRoot));
    }

    public LinuxBlockfsManifestIndexSnapshot loadFile(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        }
        int version = intValue(properties, "version");
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported manifest index version: " + version);
        }
        long generation = longValue(properties, "generation");
        int fileCount = intValue(properties, "file.count");
        List<LinuxBlockfsManifestEntry> entries = new ArrayList<>(fileCount);
        for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
            String prefix = "file." + fileIndex + ".";
            int chunkCount = intValue(properties, prefix + "chunk.count");
            List<LinuxBlockfsManifestChunk> chunks = new ArrayList<>(chunkCount);
            for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
                String chunkPrefix = prefix + "chunk." + chunkIndex + ".";
                chunks.add(new LinuxBlockfsManifestChunk(
                        longValue(properties, chunkPrefix + "file-offset"),
                        intValue(properties, chunkPrefix + "length"),
                        ContentHash.fromHex(required(properties, chunkPrefix + "hash"))
                ));
            }
            entries.add(new LinuxBlockfsManifestEntry(
                    longValue(properties, prefix + "inode"),
                    required(properties, prefix + "path"),
                    longValue(properties, prefix + "size"),
                    ContentHash.fromHex(required(properties, prefix + "file-hash")),
                    chunks
            ));
        }
        return new LinuxBlockfsManifestIndexSnapshot(generation, entries);
    }

    public Path write(Path metadataRoot, LinuxBlockfsManifestIndexSnapshot snapshot) throws IOException {
        Path path = snapshotPath(metadataRoot);
        Files.createDirectories(path.getParent());
        writeFile(path, snapshot);
        return path;
    }

    public void writeFile(Path path, LinuxBlockfsManifestIndexSnapshot snapshot) throws IOException {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot is required");
        }
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Properties properties = new Properties();
        properties.setProperty("version", Integer.toString(FORMAT_VERSION));
        properties.setProperty("generation", Long.toString(snapshot.generation()));
        properties.setProperty("file.count", Integer.toString(snapshot.entries().size()));
        for (int fileIndex = 0; fileIndex < snapshot.entries().size(); fileIndex++) {
            LinuxBlockfsManifestEntry entry = snapshot.entries().get(fileIndex);
            String prefix = "file." + fileIndex + ".";
            properties.setProperty(prefix + "inode", Long.toString(entry.inodeNumber()));
            properties.setProperty(prefix + "path", entry.path());
            properties.setProperty(prefix + "size", Long.toString(entry.size()));
            properties.setProperty(prefix + "file-hash", entry.fileHash().hex());
            properties.setProperty(prefix + "chunk.count", Integer.toString(entry.chunks().size()));
            for (int chunkIndex = 0; chunkIndex < entry.chunks().size(); chunkIndex++) {
                LinuxBlockfsManifestChunk chunk = entry.chunks().get(chunkIndex);
                String chunkPrefix = prefix + "chunk." + chunkIndex + ".";
                properties.setProperty(chunkPrefix + "file-offset", Long.toString(chunk.fileOffset()));
                properties.setProperty(chunkPrefix + "length", Integer.toString(chunk.length()));
                properties.setProperty(chunkPrefix + "hash", chunk.chunkHash().hex());
            }
        }

        try (OutputStream out = Files.newOutputStream(path)) {
            properties.store(out, "CXNFS blockfs manifest index");
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing manifest index field: " + key);
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
