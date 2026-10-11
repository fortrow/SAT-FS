package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class LinuxBlockfsPayloadCacheWriter {
    private final Path payloadDevice;

    public LinuxBlockfsPayloadCacheWriter(Path payloadDevice) {
        if (payloadDevice == null) {
            throw new IllegalArgumentException("payloadDevice is required");
        }
        this.payloadDevice = payloadDevice.toAbsolutePath().normalize();
    }

    public long writeVerified(LinuxBlockfsReadMissChunk chunk, byte[] data) throws IOException {
        if (chunk == null) {
            throw new IllegalArgumentException("chunk is required");
        }
        if (data == null) {
            throw new IllegalArgumentException("data is required");
        }
        if (data.length != chunk.length()) {
            throw new IOException("fetched chunk length does not match manifest");
        }
        ContentHash actualHash = Hashing.chunk(data);
        if (!actualHash.equals(chunk.chunkHash())) {
            throw new IOException("fetched chunk hash does not match manifest");
        }

        try (SeekableByteChannel channel = openPayloadDevice()) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            channel.position(chunk.physicalPayloadOffset());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
        return data.length;
    }

    public Path payloadDevice() {
        return payloadDevice;
    }

    private SeekableByteChannel openPayloadDevice() throws IOException {
        if (Files.exists(payloadDevice)) {
            return Files.newByteChannel(payloadDevice, StandardOpenOption.WRITE);
        }
        return Files.newByteChannel(payloadDevice, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
}
