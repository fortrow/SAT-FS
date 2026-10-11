package com.fortrow.cxnfs.transport;

import java.io.IOException;

@FunctionalInterface
public interface BulkFrameSink {
    void send(BulkDataFrame frame) throws IOException;
}
