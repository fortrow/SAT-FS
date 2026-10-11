package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameKey;
import com.fortrow.cxnfs.transport.BulkWriteAck;

import java.io.IOException;
import java.util.List;

public record BulkWriteFrameInfo(
        BulkDataFrameKey key,
        long chunkSequence,
        long entryOffset,
        int payloadLength
) {
    public BulkWriteFrameInfo {
        if (key == null) {
            throw new IllegalArgumentException("key is required");
        }
        if (chunkSequence < 0) {
            throw new IllegalArgumentException("chunkSequence must be non-negative");
        }
        if (entryOffset < 0) {
            throw new IllegalArgumentException("entryOffset must be non-negative");
        }
        if (payloadLength <= 0) {
            throw new IllegalArgumentException("payloadLength must be positive");
        }
    }

    public static BulkWriteFrameInfo from(BulkDataFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        return new BulkWriteFrameInfo(
                frame.key(),
                frame.chunkSequence(),
                frame.entryOffset(),
                frame.payloadLength()
        );
    }

    public void verify(BulkWriteAck ack) throws IOException {
        if (!ack.key().equals(key)) {
            throw new IOException("QUIC bulk write ack key does not match sent frame");
        }
        if (ack.payloadLength() != payloadLength) {
            throw new IOException("QUIC bulk write ack length does not match sent frame");
        }
    }

    public static String describeRange(List<BulkWriteFrameInfo> frames) {
        if (frames == null || frames.isEmpty()) {
            return "";
        }
        BulkWriteFrameInfo first = frames.get(0);
        BulkWriteFrameInfo last = frames.get(frames.size() - 1);
        long lastEnd = last.entryOffset() + last.payloadLength();
        return " chunks=" + first.chunkSequence() + ".." + last.chunkSequence()
                + " offsets=" + first.entryOffset() + ".." + lastEnd
                + " frames=" + frames.size();
    }
}
