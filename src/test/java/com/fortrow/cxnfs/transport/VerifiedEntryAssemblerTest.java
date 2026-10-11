package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class VerifiedEntryAssemblerTest {
    @Test
    void assemblesVerifiedEntryInChunkSequenceOrder() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(4096)
        );
        byte[] first = "first ".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 1, 4, 1, first.length, true, second));
        stagingArea.stage(new BulkDataFrame(transferId, 0, 4, 0, 0, true, first));
        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                4,
                0,
                true,
                first.length + second.length,
                List.of(Hashing.chunk(first), Hashing.chunk(second))
        ));

        ContentHash fileHash = Hashing.file(
                List.of(Hashing.chunk(first), Hashing.chunk(second)),
                List.of(first.length, second.length)
        );
        VerifiedEntry entry = new VerifiedEntryAssembler(stagingArea).assemble(new TransferEntryCommitRequest(
                transferId,
                4,
                "/outputs/run-1/model.safetensors",
                fileHash,
                first.length + second.length
        ));

        assertEquals("/outputs/run-1/model.safetensors", entry.manifest().path());
        assertEquals(fileHash, entry.manifest().fileHash());
        assertEquals(2, entry.manifest().chunks().size());
        assertEquals(first.length + second.length, entry.verifiedBytes());
    }

    @Test
    void rejectsEntryAssemblyBeforeMetadataVerification() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(4096)
        );
        byte[] payload = "unverified".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));

        ContentHash fileHash = Hashing.file(List.of(Hashing.chunk(payload)), List.of(payload.length));

        assertThrows(IllegalStateException.class, () -> new VerifiedEntryAssembler(stagingArea).assemble(
                new TransferEntryCommitRequest(transferId, 0, "/outputs/run-1/model.safetensors", fileHash, payload.length)
        ));
    }

    @Test
    void publishCoordinatorCommitsEntryButKeepsTransferOpen() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(4096)
        );
        byte[] payload = "checkpoint".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));
        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        ));
        ContentHash fileHash = Hashing.file(List.of(Hashing.chunk(payload)), List.of(payload.length));
        EntryPublishCoordinator coordinator = new EntryPublishCoordinator(
                controller,
                new VerifiedEntryAssembler(stagingArea)
        );

        PublishedEntry published = coordinator.publish(new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                fileHash,
                payload.length
        ));

        assertEquals(fileHash, published.response().contentHash());
        assertEquals(payload.length, published.entry().verifiedBytes());
        assertTrue(controller.canBufferPayload(transferId));
        assertEquals(0, stagingArea.stagedPayloadCount());
        assertEquals(0, stagingArea.memoryBytes());
    }

    @Test
    void publishCoordinatorRejectsDuplicateEntrySequenceBeforePublishingAgain() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(4096)
        );
        CountingPublisher publisher = new CountingPublisher();
        EntryPublishCoordinator coordinator = new EntryPublishCoordinator(
                controller,
                new VerifiedEntryAssembler(stagingArea),
                publisher
        );
        byte[] first = "checkpoint".getBytes(StandardCharsets.UTF_8);
        stageVerifiedPayload(stagingArea, transferId, 0, first);
        ContentHash firstHash = Hashing.file(List.of(Hashing.chunk(first)), List.of(first.length));
        coordinator.publish(new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                firstHash,
                first.length
        ));

        byte[] second = "replacement".getBytes(StandardCharsets.UTF_8);
        stageVerifiedPayload(stagingArea, transferId, 0, second);
        ContentHash secondHash = Hashing.file(List.of(Hashing.chunk(second)), List.of(second.length));

        assertThrows(IllegalStateException.class, () -> coordinator.publish(new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model-copy.safetensors",
                secondHash,
                second.length
        )));
        assertEquals(1, publisher.manifestsPublished);
    }

    @Test
    void publishCoordinatorRollsBackEntrySummaryWhenVerificationFails() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(4096)
        );
        EntryPublishCoordinator coordinator = new EntryPublishCoordinator(
                controller,
                new VerifiedEntryAssembler(stagingArea)
        );
        byte[] payload = "checkpoint".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 0, 0, true, payload));
        ContentHash fileHash = Hashing.file(List.of(Hashing.chunk(payload)), List.of(payload.length));
        TransferEntryCommitRequest request = new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                fileHash,
                payload.length
        );

        assertThrows(IllegalStateException.class, () -> coordinator.publish(request));
        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        ));

        PublishedEntry published = coordinator.publish(request);

        assertEquals(fileHash, published.response().contentHash());
    }

    @Test
    void assemblerReleasesStagedPayloadsWhenPublishingFailsAfterVerificationStarts() throws Exception {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = admittedController(transferId);
        BoundedPayloadStagingArea stagingArea = new BoundedPayloadStagingArea(
                controller,
                PayloadStagingConfig.memoryOnly(4096)
        );
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 0, 0, true, first));
        stagingArea.stage(new BulkDataFrame(transferId, 0, 0, 1, first.length, true, second));
        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                first.length + second.length,
                List.of(Hashing.chunk(first), Hashing.chunk(second))
        ));
        ContentHash fileHash = Hashing.file(
                List.of(Hashing.chunk(first), Hashing.chunk(second)),
                List.of(first.length, second.length)
        );

        IOException failure = assertThrows(IOException.class, () -> new VerifiedEntryAssembler(stagingArea).assemble(
                new TransferEntryCommitRequest(
                        transferId,
                        0,
                        "/outputs/run-1/model.safetensors",
                        fileHash,
                        first.length + second.length
                ),
                new FailingChunkPublisher()
        ));

        assertTrue(failure.getMessage().contains("simulated chunk publish failure"));
        assertEquals(0, stagingArea.stagedPayloadCount());
        assertEquals(0, stagingArea.memoryBytes());
    }

    private static WriteAdmissionController admittedController(UUID transferId) {
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
                1024 * 1024
        ));
        return controller;
    }

    private static void stageVerifiedPayload(
            BoundedPayloadStagingArea stagingArea,
            UUID transferId,
            int entrySequence,
            byte[] payload
    ) throws Exception {
        stagingArea.stage(new BulkDataFrame(transferId, 0, entrySequence, 0, 0, true, payload));
        stagingArea.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                entrySequence,
                0,
                true,
                payload.length,
                List.of(Hashing.chunk(payload))
        ));
    }

    private static final class CountingPublisher implements VerifiedEntryPublisher {
        private int manifestsPublished;

        @Override
        public void acceptChunk(ContentHash chunkHash, long sourceOffset, byte[] data) {
        }

        @Override
        public FileManifest publishManifest(FileManifest manifest) {
            manifestsPublished++;
            return manifest;
        }
    }

    private static final class FailingChunkPublisher implements VerifiedEntryPublisher {
        @Override
        public void acceptChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
            throw new IOException("simulated chunk publish failure");
        }

        @Override
        public FileManifest publishManifest(FileManifest manifest) {
            return manifest;
        }
    }
}
