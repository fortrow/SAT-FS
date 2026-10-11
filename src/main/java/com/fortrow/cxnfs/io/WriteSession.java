package com.fortrow.cxnfs.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public final class WriteSession {
    private final String path;
    private final WriteMode mode;
    private final byte[] baseContent;
    private final List<WriteExtent> extents = new ArrayList<>();
    private long declaredSize;

    public WriteSession(String path, WriteMode mode, byte[] baseContent) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (mode == null) {
            throw new IllegalArgumentException("mode is required");
        }
        this.path = path;
        this.mode = mode;
        this.baseContent = baseContent == null ? new byte[0] : java.util.Arrays.copyOf(baseContent, baseContent.length);
        this.declaredSize = this.baseContent.length;
    }

    public static WriteSession createOrReplace(String path) {
        return new WriteSession(path, WriteMode.CREATE_OR_REPLACE, new byte[0]);
    }

    public static WriteSession copyOnWrite(String path, byte[] baseContent) {
        return new WriteSession(path, WriteMode.COPY_ON_WRITE, baseContent);
    }

    public void write(long offset, byte[] data) {
        WriteExtent extent = new WriteExtent(offset, data);
        extents.add(extent);
        declaredSize = Math.max(declaredSize, extent.endExclusive());
    }

    public void truncate(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        declaredSize = size;
    }

    public StagedWrite stage() {
        byte[] materialized = materialize();
        return new StagedWrite(path, mode, declaredSize, List.copyOf(extents), materialized);
    }

    public StagedWrite stageToFile(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("directory is required");
        }
        Files.createDirectories(directory);
        Path target = directory.toAbsolutePath()
                .normalize()
                .resolve("cxnfs-write-session-" + UUID.randomUUID() + ".tmp");
        boolean complete = false;
        try (SeekableByteChannel channel = Files.newByteChannel(
                target,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        )) {
            if (declaredSize > 0) {
                channel.position(declaredSize - 1);
                writeAll(channel, ByteBuffer.wrap(new byte[]{0}));
            }
            writeBaseContent(channel);
            for (WriteExtent extent : extents.stream()
                    .sorted(Comparator.comparingLong(WriteExtent::offset))
                    .toList()) {
                writeExtent(channel, extent);
            }
            complete = true;
        } finally {
            if (!complete) {
                Files.deleteIfExists(target);
            }
        }
        return StagedWrite.fileBacked(path, mode, declaredSize, List.copyOf(extents), target);
    }

    private byte[] materialize() {
        if (declaredSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("in-memory write staging cannot materialize files larger than 2 GiB yet");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(declaredSize));
        out.writeBytes(slice(baseContent, 0, Math.min(baseContent.length, Math.toIntExact(declaredSize))));
        while (out.size() < declaredSize) {
            out.write(0);
        }

        byte[] bytes = out.toByteArray();
        extents.stream()
                .sorted(Comparator.comparingLong(WriteExtent::offset))
                .forEach(extent -> applyExtent(bytes, extent));
        return bytes;
    }

    private void applyExtent(byte[] target, WriteExtent extent) {
        if (extent.offset() >= declaredSize) {
            return;
        }
        int targetOffset = Math.toIntExact(extent.offset());
        int length = Math.toIntExact(Math.min(extent.data().length, declaredSize - extent.offset()));
        System.arraycopy(extent.data(), 0, target, targetOffset, length);
    }

    private void writeBaseContent(SeekableByteChannel channel) throws IOException {
        int length = Math.toIntExact(Math.min(baseContent.length, declaredSize));
        if (length == 0) {
            return;
        }
        channel.position(0);
        writeAll(channel, ByteBuffer.wrap(baseContent, 0, length));
    }

    private void writeExtent(SeekableByteChannel channel, WriteExtent extent) throws IOException {
        if (extent.offset() >= declaredSize) {
            return;
        }
        byte[] extentData = extent.data();
        int length = Math.toIntExact(Math.min(extentData.length, declaredSize - extent.offset()));
        channel.position(extent.offset());
        writeAll(channel, ByteBuffer.wrap(extentData, 0, length));
    }

    private static void writeAll(SeekableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static byte[] slice(byte[] source, int offset, int length) {
        byte[] result = new byte[length];
        System.arraycopy(source, offset, result, 0, length);
        return result;
    }
}
