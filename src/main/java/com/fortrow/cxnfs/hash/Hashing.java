package com.fortrow.cxnfs.hash;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.CRC32C;

public final class Hashing {
    private static final String CHUNK_DOMAIN = "cxnfs.chunk.v1";
    private static final String FILE_DOMAIN = "cxnfs.file.v1";
    private static final String TRANSFER_DOMAIN = "cxnfs.transfer.v1";
    private static final String CONTAINER_DOMAIN = "cxnfs.container.v1";
    private static final ConcurrentMap<RunKey, ContentHash> RUN_CHUNK_HASHES = new ConcurrentHashMap<>();

    private Hashing() {
    }

    public static ContentHash chunk(byte[] data) {
        CRC32C checksum = new CRC32C();
        checksum.update(data, 0, data.length);
        return ContentHash.crc32c(CHUNK_DOMAIN, checksum.getValue(), data.length);
    }

    public static ContentHash chunkRun(byte value, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        RunKey key = new RunKey(value, length);
        ContentHash cached = RUN_CHUNK_HASHES.get(key);
        if (cached != null) {
            return cached;
        }
        CRC32C checksum = new CRC32C();
        byte[] buffer = new byte[Math.min(Math.max(length, 1), 8192)];
        java.util.Arrays.fill(buffer, value);
        int remaining = length;
        while (remaining > 0) {
            int count = Math.min(remaining, buffer.length);
            checksum.update(buffer, 0, count);
            remaining -= count;
        }
        ContentHash hash = ContentHash.crc32c(CHUNK_DOMAIN, checksum.getValue(), length);
        ContentHash existing = RUN_CHUNK_HASHES.putIfAbsent(key, hash);
        return existing == null ? hash : existing;
    }

    public static ContentHash chunkRepeatedSymbol(byte[] symbol, int length) {
        if (symbol == null || symbol.length == 0) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        CRC32C checksum = new CRC32C();
        int remaining = length;
        while (remaining > 0) {
            int count = Math.min(remaining, symbol.length);
            checksum.update(symbol, 0, count);
            remaining -= count;
        }
        return ContentHash.crc32c(CHUNK_DOMAIN, checksum.getValue(), length);
    }

    public static ContentHash file(List<ContentHash> chunkHashes, List<Integer> chunkSizes) {
        if (chunkHashes.size() != chunkSizes.size()) {
            throw new IllegalArgumentException("chunk hash and size counts must match");
        }
        CRC32C checksum = new CRC32C();
        long logicalLength = 0L;
        for (int i = 0; i < chunkHashes.size(); i++) {
            int chunkSize = chunkSizes.get(i);
            if (chunkSize < 0) {
                throw new IllegalArgumentException("chunk size must be non-negative");
            }
            byte[] hashBytes = chunkHashes.get(i).bytes();
            checksum.update(hashBytes, 0, hashBytes.length);
            byte[] sizeBytes = intBytes(chunkSize);
            checksum.update(sizeBytes, 0, sizeBytes.length);
            logicalLength += chunkSize;
        }
        return ContentHash.crc32c(FILE_DOMAIN, checksum.getValue(), logicalLength);
    }

    public static ContentHash transfer(List<ContentHash> entryHashes, List<Long> entrySizes) {
        if (entryHashes.size() != entrySizes.size()) {
            throw new IllegalArgumentException("entry hash and size counts must match");
        }
        CRC32C checksum = new CRC32C();
        long logicalLength = 0L;
        for (int i = 0; i < entryHashes.size(); i++) {
            long entrySize = entrySizes.get(i);
            if (entrySize < 0) {
                throw new IllegalArgumentException("entry size must be non-negative");
            }
            byte[] hashBytes = entryHashes.get(i).bytes();
            checksum.update(hashBytes, 0, hashBytes.length);
            byte[] sizeBytes = longBytes(entrySize);
            checksum.update(sizeBytes, 0, sizeBytes.length);
            logicalLength += entrySize;
        }
        return ContentHash.crc32c(TRANSFER_DOMAIN, checksum.getValue(), logicalLength);
    }

    public static ContentHash container(byte[] serialized) {
        CRC32C checksum = new CRC32C();
        checksum.update(serialized, 0, serialized.length);
        return ContentHash.crc32c(CONTAINER_DOMAIN, checksum.getValue(), serialized.length);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private record RunKey(byte value, int length) {
    }
}
