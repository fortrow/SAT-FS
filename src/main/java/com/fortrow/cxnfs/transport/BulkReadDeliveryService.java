package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.server.CxnFileServer;

import java.io.IOException;

public final class BulkReadDeliveryService {
    private final ReadPlanBulkFrameProducer producer;

    public BulkReadDeliveryService(CxnFileServer server) {
        this(new ReadPlanBulkFrameProducer(server));
    }

    public BulkReadDeliveryService(ReadPlanBulkFrameProducer producer) {
        if (producer == null) {
            throw new IllegalArgumentException("producer is required");
        }
        this.producer = producer;
    }

    public BulkReadDeliveryResult deliver(
            AdmittedReadTransfer transfer,
            int entrySequence,
            ReadPlan plan,
            BulkFrameSink sink
    ) throws IOException {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        return deliver(transfer, entrySequence, transfer.acceptedInitialCords(), plan, sink);
    }

    public BulkReadDeliveryResult deliver(
            AdmittedReadTransfer transfer,
            int entrySequence,
            int activeCords,
            ReadPlan plan,
            BulkFrameSink sink
    ) throws IOException {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (sink == null) {
            throw new IllegalArgumentException("sink is required");
        }
        if (activeCords <= 0 || activeCords > transfer.acceptedMaxCords()) {
            throw new IllegalArgumentException("activeCords must be between 1 and the admitted maximum");
        }

        BulkReadFrameProductionResult production = producer.produceTo(transfer, entrySequence, activeCords, plan, frame -> {
            requireDeliveryFrameMatchesAdmission(transfer, entrySequence, activeCords, frame);
            sink.send(frame);
        });
        return new BulkReadDeliveryResult(
                transfer.transferId(),
                entrySequence,
                production.frameCount() == 0 ? 0 : activeCords,
                production.frameCount(),
                production.payloadBytes()
        );
    }

    private static void requireDeliveryFrameMatchesAdmission(
            AdmittedReadTransfer transfer,
            int entrySequence,
            int activeCords,
            BulkDataFrame frame
    ) {
        if (!transfer.transferId().equals(frame.transferId())) {
            throw new IllegalStateException("producer emitted a frame for the wrong transfer");
        }
        if (frame.entrySequence() != entrySequence) {
            throw new IllegalStateException("producer emitted a frame for the wrong entry sequence");
        }
        if (frame.cordIndex() >= activeCords || frame.cordIndex() >= transfer.acceptedMaxCords()) {
            throw new IllegalStateException("producer emitted a frame outside the active cord set");
        }
    }
}
