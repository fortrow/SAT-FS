package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkWriteAck;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public interface BulkTransferClientChannel extends AutoCloseable {
    long read(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output
    ) throws IOException;

    BulkWriteAck sendWriteFrame(byte[] frameBytes) throws IOException;

    default BulkWriteAck sendWriteFrame(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        return sendWriteDataFrames(List.of(frame)).getFirst();
    }

    default List<BulkWriteAck> sendWriteDataFrames(List<BulkDataFrame> frames) throws IOException {
        if (frames == null) {
            throw new IllegalArgumentException("frames are required");
        }
        List<byte[]> encodedFrames = new ArrayList<>(frames.size());
        List<BulkWriteFrameInfo> expectedFrames = new ArrayList<>(frames.size());
        for (BulkDataFrame frame : frames) {
            encodedFrames.add(BulkDataFrameCodec.encode(frame));
            expectedFrames.add(BulkWriteFrameInfo.from(frame));
        }
        return sendWriteFrames(encodedFrames, expectedFrames);
    }

    default List<BulkWriteAck> sendWriteFrames(List<byte[]> frameBytes) throws IOException {
        return sendWriteFrames(frameBytes, null);
    }

    default List<BulkWriteAck> sendWriteFrames(
            List<byte[]> frameBytes,
            List<BulkWriteFrameInfo> expectedFrames
    ) throws IOException {
        if (frameBytes == null) {
            throw new IllegalArgumentException("frameBytes are required");
        }
        if (expectedFrames != null && expectedFrames.size() != frameBytes.size()) {
            throw new IllegalArgumentException("expectedFrames must match frameBytes size");
        }
        List<BulkWriteAck> acks = new ArrayList<>(frameBytes.size());
        for (int index = 0; index < frameBytes.size(); index++) {
            BulkWriteAck ack = sendWriteFrame(frameBytes.get(index));
            if (expectedFrames != null) {
                expectedFrames.get(index).verify(ack);
            }
            acks.add(ack);
        }
        return List.copyOf(acks);
    }

    @Override
    void close() throws IOException;
}
