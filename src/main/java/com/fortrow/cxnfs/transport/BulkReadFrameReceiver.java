package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class BulkReadFrameReceiver {
    private final TransferAcceptedResponse transfer;
    private final ReadPlan plan;
    private final int entrySequence;
    private final int activeCords;
    private final Map<Long, PlannedChunkRead> expectedChunks = new LinkedHashMap<>();
    private final Map<Long, BulkDataFrame> frames = new LinkedHashMap<>();

    public BulkReadFrameReceiver(TransferAcceptedResponse transfer, ReadPlan plan) {
        this(transfer, plan, 0);
    }

    public BulkReadFrameReceiver(TransferAcceptedResponse transfer, ReadPlan plan, int entrySequence) {
        this(transfer, plan, entrySequence, defaultActiveCords(transfer));
    }

    public BulkReadFrameReceiver(
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            int entrySequence,
            int activeCords
    ) {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
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
        this.plan = plan;
        this.entrySequence = entrySequence;
        this.activeCords = activeCords;
        for (PlannedChunkRead chunk : plan.chunks()) {
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
        if (frames.containsKey(frame.chunkSequence())) {
            throw new IllegalArgumentException("duplicate bulk frame chunk sequence: " + frame.chunkSequence());
        }
        validateFullChunkFrame(chunk, frame);
        frames.put(frame.chunkSequence(), frame);
        return frames.size();
    }

    private static int defaultActiveCords(TransferAcceptedResponse transfer) {
        return transfer == null ? 1 : transfer.acceptedMaxCords();
    }

    public byte[] complete() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (PlannedChunkRead chunk : plan.chunks()) {
            BulkDataFrame frame = frames.get((long) chunk.chunkIndex());
            if (frame == null) {
                throw new IllegalStateException("missing bulk frame chunk sequence: " + chunk.chunkIndex());
            }
            byte[] payload = frame.payload();
            output.write(payload, chunk.offsetInChunk(), chunk.bytesToRead());
        }
        return output.toByteArray();
    }

    public int receivedCount() {
        return frames.size();
    }

    static void validateFullChunkFrame(PlannedChunkRead chunk, BulkDataFrame frame) throws IOException {
        if (!frame.finalFrameForChunk()) {
            throw new IllegalArgumentException("bulk frame is not final for chunk: " + frame.chunkSequence());
        }
        if (frame.entryOffset() != chunk.fileOffset()) {
            throw new IllegalArgumentException("bulk frame entry offset does not match read plan: "
                    + frame.chunkSequence());
        }
        if (frame.payloadLength() != chunk.chunkSize()) {
            throw new IllegalArgumentException("bulk frame must carry the full touched chunk: "
                    + frame.chunkSequence());
        }
        ContentHash actualHash = Hashing.chunk(frame.payload());
        if (!actualHash.equals(chunk.chunkHash())) {
            throw new IOException("bulk frame chunk hash verification failed: " + frame.chunkSequence());
        }
    }
}
