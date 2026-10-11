package com.fortrow.cxnfs.chunk;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.Arrays;

public record Chunk(
        ContentHash hash,
        long sourceOffset,
        byte[] data
) {
    public Chunk {
        if (sourceOffset < 0) {
            throw new IllegalArgumentException("sourceOffset must be non-negative");
        }
        data = Arrays.copyOf(data, data.length);
    }

    public int size() {
        return data.length;
    }

    @Override
    public byte[] data() {
        return Arrays.copyOf(data, data.length);
    }
}
