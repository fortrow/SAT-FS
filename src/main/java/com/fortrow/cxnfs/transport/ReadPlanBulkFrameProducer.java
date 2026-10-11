package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.server.CxnFileServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ReadPlanBulkFrameProducer {
    private final CxnFileServer server;

    public ReadPlanBulkFrameProducer(CxnFileServer server) {
        if (server == null) {
            throw new IllegalArgumentException("server is required");
        }
        this.server = server;
    }

    public List<BulkDataFrame> produce(
            UUID transferId,
            int entrySequence,
            int activeCords,
            ReadPlan plan
    ) throws IOException {
        List<BulkDataFrame> frames = new ArrayList<>(plan == null ? 0 : plan.chunks().size());
        produceTo(transferId, entrySequence, activeCords, plan, frames::add);
        return List.copyOf(frames);
    }

    public BulkReadFrameProductionResult produceTo(
            UUID transferId,
            int entrySequence,
            int activeCords,
            ReadPlan plan,
            BulkFrameSink sink
    ) throws IOException {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (activeCords <= 0 || activeCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("activeCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (sink == null) {
            throw new IllegalArgumentException("sink is required");
        }

        int[] frameCount = new int[1];
        long[] payloadBytes = new long[1];
        server.readVerifiedPlanChunks(plan, (chunk, payload) -> {
            BulkDataFrame frame = new BulkDataFrame(
                    transferId,
                    frameCount[0] % activeCords,
                    entrySequence,
                    chunk.chunkIndex(),
                    chunk.fileOffset(),
                    true,
                    payload
            );
            sink.send(frame);
            frameCount[0]++;
            payloadBytes[0] += frame.payloadLength();
        });
        return new BulkReadFrameProductionResult(frameCount[0], payloadBytes[0]);
    }

    public List<BulkDataFrame> produce(
            AdmittedReadTransfer transfer,
            int entrySequence,
            ReadPlan plan
    ) throws IOException {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        return produce(transfer, entrySequence, transfer.acceptedInitialCords(), plan);
    }

    public List<BulkDataFrame> produce(
            AdmittedReadTransfer transfer,
            int entrySequence,
            int activeCords,
            ReadPlan plan
    ) throws IOException {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (activeCords <= 0 || activeCords > transfer.acceptedMaxCords()) {
            throw new IllegalArgumentException("activeCords must be between 1 and the admitted maximum");
        }
        if (transfer.expectedSizeLimitBytes() > 0 && plan.fileSize() > transfer.expectedSizeLimitBytes()) {
            throw new IOException("read plan exceeds admitted transfer size limit: " + transfer.transferId());
        }
        return produce(transfer.transferId(), entrySequence, activeCords, plan);
    }

    public BulkReadFrameProductionResult produceTo(
            AdmittedReadTransfer transfer,
            int entrySequence,
            int activeCords,
            ReadPlan plan,
            BulkFrameSink sink
    ) throws IOException {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (activeCords <= 0 || activeCords > transfer.acceptedMaxCords()) {
            throw new IllegalArgumentException("activeCords must be between 1 and the admitted maximum");
        }
        if (transfer.expectedSizeLimitBytes() > 0 && plan.fileSize() > transfer.expectedSizeLimitBytes()) {
            throw new IOException("read plan exceeds admitted transfer size limit: " + transfer.transferId());
        }
        return produceTo(transfer.transferId(), entrySequence, activeCords, plan, sink);
    }
}
