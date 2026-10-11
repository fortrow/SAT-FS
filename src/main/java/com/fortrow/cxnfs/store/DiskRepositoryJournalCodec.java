package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;
import com.fortrow.cxnfs.format.ManifestCodec;
import com.fortrow.cxnfs.manifest.FileManifest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class DiskRepositoryJournalCodec {
    static final int VERSION = 1;
    static final int MAX_PATHS = 1_000_000;
    static final int MAX_MANIFESTS = 1_000_000;
    static final int MAX_MANIFEST_BYTES = 256 * 1024 * 1024;

    private static final byte[] MAGIC = "CXNFSJRN".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private DiskRepositoryJournalCodec() {
    }

    static byte[] encode(DiskRepositoryJournalEntry entry) throws IOException {
        if (entry == null) {
            throw new IllegalArgumentException("entry is required");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            out.writeInt(entry.deletePaths().size());
            for (String path : entry.deletePaths()) {
                BinaryIO.writeUtf8(out, path);
            }
            out.writeInt(entry.putManifests().size());
            for (FileManifest manifest : entry.putManifests()) {
                BinaryIO.writeBytes(out, ManifestCodec.encode(manifest));
            }
        }
        return bytes.toByteArray();
    }

    static DiskRepositoryJournalEntry decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            int deleteCount = in.readInt();
            if (deleteCount < 0 || deleteCount > MAX_PATHS) {
                throw new BinaryFormatException("invalid journal delete path count: " + deleteCount);
            }
            List<String> deletePaths = new ArrayList<>(deleteCount);
            for (int i = 0; i < deleteCount; i++) {
                deletePaths.add(BinaryIO.readUtf8(in, ManifestCodec.MAX_PATH_BYTES));
            }

            int manifestCount = in.readInt();
            if (manifestCount < 0 || manifestCount > MAX_MANIFESTS) {
                throw new BinaryFormatException("invalid journal manifest count: " + manifestCount);
            }
            List<FileManifest> putManifests = new ArrayList<>(manifestCount);
            for (int i = 0; i < manifestCount; i++) {
                putManifests.add(ManifestCodec.decode(BinaryIO.readBytes(in, MAX_MANIFEST_BYTES)));
            }
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after repository journal");
            }
            return new DiskRepositoryJournalEntry(deletePaths, putManifests);
        }
    }
}
