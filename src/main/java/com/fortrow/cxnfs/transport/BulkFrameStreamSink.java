package com.fortrow.cxnfs.transport;

import java.io.IOException;
import java.io.OutputStream;

public final class BulkFrameStreamSink implements EncodedBulkFrameSink {
    private final OutputStream output;

    public BulkFrameStreamSink(OutputStream output) {
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        this.output = output;
    }

    @Override
    public void send(int cordIndex, byte[] frameBytes) throws IOException {
        BulkFrameStream.write(output, frameBytes);
    }
}
