package com.fortrow.cxnfs.io;

public record ByteRange(
        long offset,
        long length
) {
    public ByteRange {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
    }

    public long endExclusive() {
        return Math.addExact(offset, length);
    }

    public boolean isEmpty() {
        return length == 0;
    }
}
