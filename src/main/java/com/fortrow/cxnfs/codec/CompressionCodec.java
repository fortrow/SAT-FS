package com.fortrow.cxnfs.codec;

import java.io.IOException;

public interface CompressionCodec {
    byte id();

    String name();

    byte[] compress(byte[] input) throws IOException;

    byte[] decompress(byte[] input, int expectedSize) throws IOException;
}
