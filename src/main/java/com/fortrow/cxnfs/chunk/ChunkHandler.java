package com.fortrow.cxnfs.chunk;

import java.io.IOException;

@FunctionalInterface
public interface ChunkHandler {
    void accept(Chunk chunk) throws IOException;
}
