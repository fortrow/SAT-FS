package com.fortrow.cxnfs.format;

import com.fortrow.cxnfs.container.Container;
import com.fortrow.cxnfs.container.StoredChunk;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ContainerCodec {
    public static final int VERSION = 1;
    public static final int MAX_CHUNKS = 8_192;
    public static final int MAX_SERIALIZED_BYTES = 64 * 1024 * 1024;

    private static final byte[] MAGIC = "CXNFSCNT".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private ContainerCodec() {
    }

    public static byte[] encode(Container container) throws IOException {
        if (container == null) {
            throw new IllegalArgumentException("container is required");
        }
        byte[] bytes = canonicalBytes(container.chunks());
        ContentHash hash = Hashing.container(bytes);
        if (!hash.equals(container.hash())) {
            throw new BinaryFormatException("container hash does not match canonical bytes");
        }
        return bytes;
    }

    public static Container decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }
        if (bytes.length > MAX_SERIALIZED_BYTES) {
            throw new BinaryFormatException("container exceeds maximum serialized size");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);

            int chunkCount = in.readInt();
            if (chunkCount < 0 || chunkCount > MAX_CHUNKS) {
                throw new BinaryFormatException("invalid container chunk count: " + chunkCount);
            }

            List<StoredChunk> chunks = new ArrayList<>(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                ContentHash chunkHash = ContentHash.of(BinaryIO.readFixedBytes(in, ContentHash.BYTE_LENGTH));
                int originalSize = in.readInt();
                byte compressionType = in.readByte();
                byte[] payload = BinaryIO.readBytes(in, MAX_SERIALIZED_BYTES);
                chunks.add(new StoredChunk(chunkHash, originalSize, compressionType, payload));
            }

            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after container");
            }
            return new Container(Hashing.container(bytes), chunks);
        }
    }

    public static ContentHash hash(List<StoredChunk> chunks) throws IOException {
        return Hashing.container(canonicalBytes(chunks));
    }

    public static byte[] canonicalBytes(List<StoredChunk> chunks) throws IOException {
        if (chunks == null) {
            throw new IllegalArgumentException("chunks is required");
        }
        if (chunks.size() > MAX_CHUNKS) {
            throw new BinaryFormatException("container exceeds maximum chunk count");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            out.writeInt(chunks.size());
            for (StoredChunk chunk : chunks) {
                out.write(chunk.chunkHash().bytes());
                out.writeInt(chunk.originalSize());
                out.writeByte(chunk.compressionType());
                BinaryIO.writeBytes(out, chunk.payload());
            }
        }

        byte[] serialized = bytes.toByteArray();
        if (serialized.length > MAX_SERIALIZED_BYTES) {
            throw new BinaryFormatException("container exceeds maximum serialized size");
        }
        return serialized;
    }
}
