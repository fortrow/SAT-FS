package com.fortrow.cxnfs.transport;

import java.io.IOException;

public final class BulkFrameEncodingSink implements BulkFrameSink {
    private final EncodedBulkFrameSink sink;

    public BulkFrameEncodingSink(EncodedBulkFrameSink sink) {
        if (sink == null) {
            throw new IllegalArgumentException("sink is required");
        }
        this.sink = sink;
    }

    @Override
    public void send(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        sink.send(frame.cordIndex(), BulkDataFrameCodec.encode(frame));
    }
}
