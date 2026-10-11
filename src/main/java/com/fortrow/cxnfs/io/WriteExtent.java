package com.fortrow.cxnfs.io;

import java.util.Arrays;

public record WriteExtent(
        long offset,
        byte[] data
) {
    public WriteExtent {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (data == null) {
            throw new IllegalArgumentException("data is required");
        }
        data = Arrays.copyOf(data, data.length);
    }

    public long endExclusive() {
        return offset + data.length;
    }

    public int size() {
        return data.length;
    }

    @Override
    public byte[] data() {
        return Arrays.copyOf(data, data.length);
    }
}
