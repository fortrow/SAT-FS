package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.transport.BulkDataFrame;

import java.util.Arrays;

public record EncodedWriteFrame(BulkDataFrame frame, byte[] encodedBytes) {
    public EncodedWriteFrame {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        if (encodedBytes == null || encodedBytes.length == 0) {
            throw new IllegalArgumentException("encodedBytes are required");
        }
        encodedBytes = Arrays.copyOf(encodedBytes, encodedBytes.length);
    }

    @Override
    public byte[] encodedBytes() {
        return Arrays.copyOf(encodedBytes, encodedBytes.length);
    }
}
