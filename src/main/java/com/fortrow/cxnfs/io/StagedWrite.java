package com.fortrow.cxnfs.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public record StagedWrite(
        String path,
        WriteMode mode,
        long declaredSize,
        List<WriteExtent> extents,
        byte[] materializedContent,
        Optional<Path> materializedPath
) implements AutoCloseable {
    public StagedWrite(
            String path,
            WriteMode mode,
            long declaredSize,
            List<WriteExtent> extents,
            byte[] materializedContent
    ) {
        this(path, mode, declaredSize, extents, materializedContent, Optional.empty());
    }

    public static StagedWrite fileBacked(
            String path,
            WriteMode mode,
            long declaredSize,
            List<WriteExtent> extents,
            Path materializedPath
    ) {
        return new StagedWrite(path, mode, declaredSize, extents, new byte[0], Optional.of(materializedPath));
    }

    public StagedWrite {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (mode == null) {
            throw new IllegalArgumentException("mode is required");
        }
        if (declaredSize < 0) {
            throw new IllegalArgumentException("declaredSize must be non-negative");
        }
        extents = extents == null ? List.of() : List.copyOf(extents);
        materializedPath = materializedPath == null ? Optional.empty() : materializedPath;
        if (materializedContent == null) {
            throw new IllegalArgumentException("materializedContent is required");
        }
        materializedPath = materializedPath.map(pathValue -> {
            if (pathValue == null) {
                throw new IllegalArgumentException("materializedPath must not contain null");
            }
            return pathValue.toAbsolutePath().normalize();
        });
        materializedContent = Arrays.copyOf(materializedContent, materializedContent.length);
        if (materializedPath.isEmpty() && materializedContent.length != declaredSize) {
            throw new IllegalArgumentException("materializedContent length must match declaredSize");
        }
    }

    @Override
    public byte[] materializedContent() {
        if (materializedPath.isPresent()) {
            try {
                return Files.readAllBytes(materializedPath.orElseThrow());
            } catch (IOException e) {
                throw new UncheckedIOException("failed to read staged write file", e);
            }
        }
        return Arrays.copyOf(materializedContent, materializedContent.length);
    }

    public InputStream openStream() throws IOException {
        if (materializedPath.isPresent()) {
            return Files.newInputStream(materializedPath.orElseThrow());
        }
        return new ByteArrayInputStream(materializedContent);
    }

    public byte[] readRange(long offset, int length) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        long end = offset + length;
        if (end < offset || end > declaredSize) {
            throw new IllegalArgumentException("range exceeds staged write size");
        }
        if (length == 0) {
            return new byte[0];
        }
        if (materializedPath.isPresent()) {
            byte[] data = new byte[length];
            try (SeekableByteChannel channel = Files.newByteChannel(
                    materializedPath.orElseThrow(),
                    StandardOpenOption.READ
            )) {
                channel.position(offset);
                ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) {
                    int read = channel.read(buffer);
                    if (read < 0) {
                        throw new IOException("staged write file ended before requested range");
                    }
                }
            }
            return data;
        }
        int start = Math.toIntExact(offset);
        return Arrays.copyOfRange(materializedContent, start, start + length);
    }

    @Override
    public void close() throws IOException {
        if (materializedPath.isPresent()) {
            Files.deleteIfExists(materializedPath.orElseThrow());
        }
    }
}
