package com.fortrow.cxnfs.format;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ManifestCodec {
    public static final int VERSION = 1;
    public static final int MAX_PATH_BYTES = 16 * 1024;
    public static final int MAX_CHUNKS = 1_000_000;

    private static final byte[] MAGIC = "CXNFSMAN".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private ManifestCodec() {
    }

    public static byte[] encode(FileManifest manifest) throws IOException {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            BinaryIO.writeUtf8(out, manifest.path());
            out.write(manifest.fileHash().bytes());
            out.writeLong(manifest.size());
            out.writeInt(manifest.chunks().size());
            for (ChunkRef chunk : manifest.chunks()) {
                out.write(chunk.hash().bytes());
                out.writeInt(chunk.size());
            }
        }
        return bytes.toByteArray();
    }

    public static FileManifest decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            String path = BinaryIO.readUtf8(in, MAX_PATH_BYTES);
            ContentHash fileHash = ContentHash.of(BinaryIO.readFixedBytes(in, ContentHash.BYTE_LENGTH));
            long size = in.readLong();
            int chunkCount = in.readInt();
            if (chunkCount < 0 || chunkCount > MAX_CHUNKS) {
                throw new BinaryFormatException("invalid manifest chunk count: " + chunkCount);
            }

            List<ChunkRef> chunks = new ArrayList<>(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                chunks.add(new ChunkRef(
                        ContentHash.of(BinaryIO.readFixedBytes(in, ContentHash.BYTE_LENGTH)),
                        in.readInt()
                ));
            }
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after manifest");
            }
            return new FileManifest(path, fileHash, size, chunks);
        }
    }
}
