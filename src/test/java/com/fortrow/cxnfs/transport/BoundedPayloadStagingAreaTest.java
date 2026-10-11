package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BoundedPayloadStagingAreaTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsPayloadBeforeWriteAdmission() {
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                new WriteAdmissionController(),
                PayloadStagingConfig.memoryOnly(1024)
        );

        assertThrows(
                IllegalStateException.class,
                () -> stagingArea.stage(new BulkDataFrame(UUID.randomUUID(), 0, 0, 0, 0, true, new byte[]{1}))
        );
    }

    @Test
    void rejectsPayloadOutsideTheAcceptedCordSetBeforeStaging() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = new WriteAdmissionController(
                WriteAdmissionPolicy.defaults(),
                new CordPoolConfig(1, 2, true)
        );
        controller.authenticateControlSession(UUID.randomUUID(), "trainer.example");
        controller.admit(new TransferBootstrapRequest(
                transferId,
                TransferDirection.WRITE,
                "/outputs/run-1",
                TransferPayloadFormat.SAFETENSORS,
                1,
                2,
                64 * 1024 * 1024,
                1024
        ));
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );

        assertThrows(IllegalStateException.class, () -> stagingArea.stage(new BulkDataFrame(
                transferId,
                2,
                0,
                0,
                0,
                true,
                "chunk bytes".getBytes(StandardCharsets.UTF_8)
        )));
        assertEquals(0, stagingArea.stagedPayloadCount());
        assertEquals(0, stagingArea.memoryBytes());
    }

    @Test
    void rejectsPayloadThatWouldExceedAdmittedTransferSizeBeforeStaging() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId, 12);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );

        stagingArea.stage(new BulkDataFrame(
                transferId,
                0,
                0,
                0,
                0,
                true,
                "12345678".getBytes(StandardCharsets.UTF_8)
        ));

        assertThrows(IllegalStateException.class, () -> stagingArea.stage(new BulkDataFrame(
                transferId,
                0,
                0,
                1,
                8,
                true,
                "90123".getBytes(StandardCharsets.UTF_8)
        )));
        assertEquals(1, stagingArea.stagedPayloadCount());
        assertEquals(8, stagingArea.memoryBytes());
    }

    @Test
    void stagesAdmittedPayloadInMemoryThenVerifiesLateChunkMetadata() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        BulkDataFrame frame = new BulkDataFrame(transferId, 0, 2, 5, 0, true, payload);

        StagedPayload staged = stagingArea.stage(frame);
        int verified = stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                2,
                5,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        ));

        assertEquals(StagedPayloadStorage.MEMORY, staged.storage());
        assertEquals(1, verified);
        assertEquals(payload.length, stagingArea.memoryBytes());
        assertArrayEquals(payload, stagingArea.readPayload(frame.key()));
        assertEquals(1, stagingArea.verifiedPayloads(transferId, 2).size());
    }

    @Test
    void acceptsDuplicatePayloadFrameAsIdempotentResend() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        BulkDataFrame frame = new BulkDataFrame(transferId, 0, 2, 5, 0, true, payload);

        StagedPayload first = stagingArea.stage(frame);
        StagedPayload second = stagingArea.stage(frame);

        assertEquals(first, second);
        assertEquals(1, stagingArea.stagedPayloadCount());
        assertEquals(payload.length, stagingArea.memoryBytes());
        assertArrayEquals(payload, stagingArea.readPayload(frame.key()));
    }

    @Test
    void rejectsConflictingDuplicatePayloadFrame() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        byte[] conflicting = "other bytes".getBytes(StandardCharsets.UTF_8);
        BulkDataFrame frame = new BulkDataFrame(transferId, 0, 2, 5, 0, true, payload);

        stagingArea.stage(frame);

        assertThrows(IOException.class, () -> stagingArea.stage(new BulkDataFrame(
                transferId,
                0,
                2,
                5,
                0,
                true,
                conflicting
        )));
        assertEquals(1, stagingArea.stagedPayloadCount());
        assertArrayEquals(payload, stagingArea.readPayload(frame.key()));
    }

    @Test
    void spillsAdmittedPayloadWhenMemoryLimitIsExceeded() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.withSpill(4, tempDir)
        );
        byte[] payload = "larger chunk".getBytes(StandardCharsets.UTF_8);
        BulkDataFrame frame = new BulkDataFrame(transferId, 1, 0, 0, 0, true, payload);

        StagedPayload staged = stagingArea.stage(frame);

        assertEquals(StagedPayloadStorage.SPILL, staged.storage());
        assertTrue(staged.spillPath().isPresent());
        assertTrue(Files.exists(staged.spillPath().orElseThrow()));
        assertEquals(payload.length, stagingArea.spilledBytes());
        assertArrayEquals(payload, stagingArea.readPayload(frame.key()));
    }

    @Test
    void rejectsHashMismatchWhenLateMetadataArrives() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));

        assertThrows(IOException.class, () -> stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                payload.length,
                List.of(Hashing.chunk("different".getBytes(StandardCharsets.UTF_8)))
        )));
    }

    @Test
    void metadataCanArriveBeforePayloadAndVerifyWhenFrameArrives() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);

        int verifiedBeforePayload = stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                1,
                2,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        ));
        StagedPayload staged = stagingArea.stage(new BulkDataFrame(transferId, 0, 1, 2, 0, true, payload));

        assertEquals(0, verifiedBeforePayload);
        assertTrue(staged.metadataVerified());
        assertEquals(0, stagingArea.pendingMetadataCount());
        assertEquals(1, stagingArea.verifiedPayloads(transferId, 1).size());
    }

    @Test
    void payloadAndMetadataCanRaceAcrossBulkAndControlThreads() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        BulkDataFrame frame = new BulkDataFrame(transferId, 0, 1, 2, 0, true, payload);
        TransferMetadataUpdate metadata = new TransferMetadataUpdate(
                transferId,
                1,
                2,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        );
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<StagedPayload> staged = executor.submit(() -> {
                start.await();
                return stagingArea.stage(frame);
            });
            Future<Integer> accepted = executor.submit(() -> {
                start.await();
                return stagingArea.acceptMetadata(metadata);
            });

            start.countDown();
            staged.get(5, TimeUnit.SECONDS);
            accepted.get(5, TimeUnit.SECONDS);

            assertEquals(1, stagingArea.verifiedPayloads(transferId, 1).size());
            assertArrayEquals(payload, stagingArea.readPayload(frame.key()));
            assertEquals(0, stagingArea.pendingMetadataCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void metadataBeforePayloadRejectsMismatchingFrame() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] expected = "expected".getBytes(StandardCharsets.UTF_8);
        byte[] actual = "actual".getBytes(StandardCharsets.UTF_8);

        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                expected.length,
                List.of(Hashing.chunk(expected))
        ));

        assertThrows(IOException.class, () -> stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 0, 0, true, actual)));
        assertEquals(0, stagingArea.stagedPayloadCount());
        assertEquals(1, stagingArea.pendingMetadataCount());
    }

    @Test
    void releaseEntryFreesMemoryAndPendingMetadata() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(1024)
        );
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 0, 3, 0, 0, true, payload));
        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                3,
                0,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        ));

        int released = stagingArea.releaseEntry(transferId, 3);

        assertEquals(1, released);
        assertEquals(0, stagingArea.stagedPayloadCount());
        assertEquals(0, stagingArea.memoryBytes());
        assertEquals(0, stagingArea.pendingMetadataCount());
    }

    @Test
    void releaseEntryDeletesSpillFile() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.withSpill(0, tempDir)
        );
        byte[] payload = "larger chunk".getBytes(StandardCharsets.UTF_8);
        StagedPayload staged = stagingArea.stage(new BulkDataFrame(transferId, 0, 4, 0, 0, true, payload));
        Path spillPath = staged.spillPath().orElseThrow();

        int released = stagingArea.releaseEntry(transferId, 4);

        assertEquals(1, released);
        assertEquals(0, stagingArea.spilledBytes());
        assertEquals(0, stagingArea.stagedPayloadCount());
        assertTrue(Files.notExists(spillPath));
    }

    private static WriteAdmissionController admittedController(UUID transferId) {
        return admittedController(transferId, 1024 * 1024);
    }

    private static WriteAdmissionController admittedController(UUID transferId, long expectedSizeLimitBytes) {
        WriteAdmissionController controller = new WriteAdmissionController();
        controller.authenticateControlSession(UUID.randomUUID(), "trainer.example");
        controller.admit(new TransferBootstrapRequest(
                transferId,
                TransferDirection.WRITE,
                "/outputs/run-1",
                TransferPayloadFormat.SAFETENSORS,
                1,
                64,
                64 * 1024 * 1024,
                expectedSizeLimitBytes
        ));
        return controller;
    }
}
