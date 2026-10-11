package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class BulkReadStreamingReceiver {
    private final TransferAcceptedResponse transfer;
    private final List<PlannedChunkRead> orderedChunks;
    private final Map<Long, PlannedChunkRead> expectedChunks = new HashMap<>();
    private final Map<Long, BulkDataFrame> pendingFrames = new HashMap<>();
    private final Set<Long> receivedSequences = new HashSet<>();
    private final OutputStream output;
    private final int entrySequence;
    private final int activeCords;
    private int nextOutputIndex;
    private long bytesWritten;

    public BulkReadStreamingReceiver(
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output
    ) {
        this(transfer, plan, output, 0);
    }

    public BulkReadStreamingReceiver(
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output,
            int entrySequence
    ) {
        this(transfer, plan, output, entrySequence, defaultActiveCords(transfer));
    }

    public BulkReadStreamingReceiver(
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output,
            int entrySequence,
            int activeCords
    ) {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (transfer.direction() != TransferDirection.READ) {
            throw new IllegalArgumentException("bulk read receiver requires a read transfer");
        }
        if (activeCords <= 0 || activeCords > transfer.acceptedMaxCords()) {
            throw new IllegalArgumentException("activeCords must be between 1 and acceptedMaxCords");
        }
        this.transfer = transfer;
        this.orderedChunks = List.copyOf(plan.chunks());
        this.output = output;
        this.entrySequence = entrySequence;
        this.activeCords = activeCords;
        for (PlannedChunkRead chunk : orderedChunks) {
            expectedChunks.put((long) chunk.chunkIndex(), chunk);
        }
    }

    public int accept(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        if (!transfer.transferId().equals(frame.transferId())) {
            throw new IllegalArgumentException("bulk frame belongs to a different transfer: " + frame.transferId());
        }
        if (frame.entrySequence() != entrySequence) {
            throw new IllegalArgumentException("bulk frame belongs to a different entry sequence: "
                    + frame.entrySequence());
        }
        if (frame.cordIndex() >= activeCords) {
            throw new IllegalArgumentException("bulk frame cord is outside active cord set: " + frame.cordIndex());
        }
        if (frame.cordIndex() >= transfer.acceptedMaxCords()) {
            throw new IllegalArgumentException("bulk frame cord is outside accepted cord range: " + frame.cordIndex());
        }
        PlannedChunkRead chunk = expectedChunks.get(frame.chunkSequence());
        if (chunk == null) {
            throw new IllegalArgumentException("unexpected bulk frame chunk sequence: " + frame.chunkSequence());
        }
        if (receivedSequences.contains(frame.chunkSequence())) {
            throw new IllegalArgumentException("duplicate bulk frame chunk sequence: " + frame.chunkSequence());
        }
        BulkReadFrameReceiver.validateFullChunkFrame(chunk, frame);
        receivedSequences.add(frame.chunkSequence());
        pendingFrames.put(frame.chunkSequence(), frame);
        flushContiguousFrames();
        return receivedSequences.size();
    }

    private static int defaultActiveCords(TransferAcceptedResponse transfer) {
        return transfer == null ? 1 : transfer.acceptedMaxCords();
    }

    public long complete() {
        if (nextOutputIndex != orderedChunks.size()) {
            PlannedChunkRead missing = orderedChunks.get(nextOutputIndex);
            throw new IllegalStateException("missing bulk frame chunk sequence: " + missing.chunkIndex());
        }
        return bytesWritten;
    }

    public int receivedCount() {
        return receivedSequences.size();
    }

    public long bytesWritten() {
        return bytesWritten;
    }

    public int pendingFrameCount() {
        return pendingFrames.size();
    }

    private void flushContiguousFrames() throws IOException {
        while (nextOutputIndex < orderedChunks.size()) {
            PlannedChunkRead chunk = orderedChunks.get(nextOutputIndex);
            BulkDataFrame frame = pendingFrames.remove((long) chunk.chunkIndex());
            if (frame == null) {
                return;
            }
            byte[] payload = frame.payload();
            output.write(payload, chunk.offsetInChunk(), chunk.bytesToRead());
            bytesWritten += chunk.bytesToRead();
            nextOutputIndex++;
        }
    }
}
