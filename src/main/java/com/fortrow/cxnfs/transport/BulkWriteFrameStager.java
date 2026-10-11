package com.fortrow.cxnfs.transport;

import java.io.IOException;

@FunctionalInterface
public interface BulkWriteFrameStager {
    StagedPayload stage(BulkDataFrame frame) throws IOException;
}
