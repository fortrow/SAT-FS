package com.fortrow.cxnfs.container;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.Arrays;

public record StoredChunk(
        ContentHash chunkHash,
        int originalSize,
        byte compressionType,
        byte[] payload
) {
    public StoredChunk {
        if (chunkHash == null) {
            throw new IllegalArgumentException("chunkHash is required");
        }
        if (originalSize < 0) {
            throw new IllegalArgumentException("originalSize must be non-negative");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        payload = Arrays.copyOf(payload, payload.length);
    }

    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }
}
