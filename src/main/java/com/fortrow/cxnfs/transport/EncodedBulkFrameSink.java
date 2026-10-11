package com.fortrow.cxnfs.transport;

import java.io.IOException;

@FunctionalInterface
public interface EncodedBulkFrameSink {
    void send(int cordIndex, byte[] frameBytes) throws IOException;
}
