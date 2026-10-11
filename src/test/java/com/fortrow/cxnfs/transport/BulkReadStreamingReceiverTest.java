package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ByteRange;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class BulkReadStreamingReceiverTest {
    @Test
    void writesVerifiedContiguousFramesWithoutWaitingForWholeRead() throws Exception {
        byte[] first = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        byte[] second = "ijklmnop".getBytes(StandardCharsets.UTF_8);
        byte[] third = "qrstuvwx".getBytes(StandardCharsets.UTF_8);
        ReadPlan plan = readPlan(first, second, third);
        TransferAcceptedResponse transfer = transfer(UUID.randomUUID(), 64);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(transfer, plan, output);

        assertEquals(1, receiver.accept(new BulkDataFrame(transfer.transferId(), 1, 0, 1, 8, true, second)));
        assertEquals(0, receiver.bytesWritten());
        assertEquals(1, receiver.pendingFrameCount());

        assertEquals(2, receiver.accept(new BulkDataFrame(transfer.transferId(), 0, 0, 0, 0, true, first)));
        assertEquals(11, receiver.bytesWritten());
        assertEquals(0, receiver.pendingFrameCount());
        assertArrayEquals("fghijklmnop".getBytes(StandardCharsets.UTF_8), output.toByteArray());

        assertEquals(3, receiver.accept(new BulkDataFrame(transfer.transferId(), 2, 0, 2, 16, true, third)));

        assertEquals(15, receiver.complete());
        assertEquals(3, receiver.receivedCount());
        assertArrayEquals("fghijklmnopqrst".getBytes(StandardCharsets.UTF_8), output.toByteArray());
    }

    @Test
    void refusesToCompleteUntilMissingEarlierFrameArrives() throws Exception {
        byte[] first = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        byte[] second = "ijklmnop".getBytes(StandardCharsets.UTF_8);
        byte[] third = "qrstuvwx".getBytes(StandardCharsets.UTF_8);
        ReadPlan plan = readPlan(first, second, third);
        TransferAcceptedResponse transfer = transfer(UUID.randomUUID(), 64);
        BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(
                transfer,
                plan,
                new ByteArrayOutputStream()
        );

        receiver.accept(new BulkDataFrame(transfer.transferId(), 1, 0, 1, 8, true, second));

        assertThrows(IllegalStateException.class, receiver::complete);
    }

    @Test
    void rejectsDuplicateAndCorruptFrames() throws Exception {
        byte[] first = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        byte[] second = "ijklmnop".getBytes(StandardCharsets.UTF_8);
        byte[] third = "qrstuvwx".getBytes(StandardCharsets.UTF_8);
        ReadPlan plan = readPlan(first, second, third);
        TransferAcceptedResponse transfer = transfer(UUID.randomUUID(), 64);
        BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(
                transfer,
                plan,
                new ByteArrayOutputStream()
        );

        receiver.accept(new BulkDataFrame(transfer.transferId(), 0, 0, 0, 0, true, first));

        assertThrows(
                IllegalArgumentException.class,
                () -> receiver.accept(new BulkDataFrame(transfer.transferId(), 0, 0, 0, 0, true, first))
        );
        assertThrows(
                java.io.IOException.class,
                () -> receiver.accept(new BulkDataFrame(
                        transfer.transferId(),
                        1,
                        0,
                        1,
                        8,
                        true,
                        "ijkLmnop".getBytes(StandardCharsets.UTF_8)
                ))
        );
    }

    @Test
    void rejectsFrameOutsideActiveCordSet() throws Exception {
        byte[] first = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        byte[] second = "ijklmnop".getBytes(StandardCharsets.UTF_8);
        byte[] third = "qrstuvwx".getBytes(StandardCharsets.UTF_8);
        ReadPlan plan = readPlan(first, second, third);
        TransferAcceptedResponse transfer = transfer(UUID.randomUUID(), 64);
        BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(
                transfer,
                plan,
                new ByteArrayOutputStream(),
                0,
                1
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> receiver.accept(new BulkDataFrame(transfer.transferId(), 7, 0, 0, 0, true, first))
        );
    }

    private static ReadPlan readPlan(byte[] first, byte[] second, byte[] third) {
        return new ReadPlan(
                Hashing.chunk("file-hash-placeholder".getBytes(StandardCharsets.UTF_8)),
                26,
                new ByteRange(5, 15),
                List.of(
                        new PlannedChunkRead(0, Hashing.chunk(first), 0, 8, 5, 3),
                        new PlannedChunkRead(1, Hashing.chunk(second), 8, 8, 0, 8),
                        new PlannedChunkRead(2, Hashing.chunk(third), 16, 8, 0, 4)
                )
        );
    }

    private static TransferAcceptedResponse transfer(UUID transferId, int acceptedMaxCords) {
        return new TransferAcceptedResponse(
                transferId,
                TransferDirection.READ,
                1,
                acceptedMaxCords,
                128L * 1024 * 1024,
                false
        );
    }
}
