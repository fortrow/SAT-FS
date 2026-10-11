package com.fortrow.cxnfs.chunk;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface Chunker {
    List<Chunk> split(byte[] data);

    default void split(InputStream input, ChunkHandler handler) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler is required");
        }
        for (Chunk chunk : split(input.readAllBytes())) {
            handler.accept(chunk);
        }
    }
}
