package com.fortrow.cxnfs.io;

import java.io.IOException;

@FunctionalInterface
public interface PlannedChunkConsumer {
    void accept(PlannedChunkRead chunk, byte[] payload) throws IOException;
}
