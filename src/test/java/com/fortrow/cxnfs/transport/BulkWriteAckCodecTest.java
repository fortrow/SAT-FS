package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class BulkWriteAckCodecTest {
    @Test
    void roundTripsWriteAck() throws Exception {
        BulkWriteAck ack = new BulkWriteAck(
                new BulkDataFrameKey(
                        UUID.fromString("00000000-0000-0000-0000-000000000123"),
                        4,
                        9
                ),
                8192,
                true,
                StagedPayloadStorage.SPILL
        );

        BulkWriteAck decoded = BulkWriteAckCodec.decode(BulkWriteAckCodec.encode(ack));

        assertEquals(ack, decoded);
    }
}
