package com.fortrow.cxnfs.codec;

import java.util.Arrays;

public final class IdentityCompressionCodec implements CompressionCodec {
    public static final byte ID = 0;

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public String name() {
        return "none";
    }

    @Override
    public byte[] compress(byte[] input) {
        return Arrays.copyOf(input, input.length);
    }

    @Override
    public byte[] decompress(byte[] input, int expectedSize) {
        if (input.length != expectedSize) {
            throw new IllegalArgumentException("identity payload size does not match expected size");
        }
        return Arrays.copyOf(input, input.length);
    }
}
