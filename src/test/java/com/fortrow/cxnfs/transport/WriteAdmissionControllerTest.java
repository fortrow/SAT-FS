package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAction;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WriteAdmissionControllerTest {
    @Test
    void rejectsPayloadBufferingBeforeWriteAdmission() {
        WriteAdmissionController controller = new WriteAdmissionController();

        assertFalse(controller.canBufferPayload(UUID.randomUUID()));
        assertThrows(IllegalStateException.class, () -> controller.requirePayloadAdmission(UUID.randomUUID()));
    }

    @Test
    void admitsTreeScopedWriteAfterAuthenticatedControlSession() {
        UUID sessionId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = new WriteAdmissionController();
        controller.authenticateControlSession(sessionId, "trainer.example");

        TransferAcceptedResponse response = controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        assertTrue(controller.canBufferPayload(transferId));
        assertTrue(response.writePayloadAdmissionGranted());
        assertEquals(TransferDirection.WRITE, response.direction());
        AdmittedWriteTransfer admitted = controller.admittedTransfer(transferId).orElseThrow();
        assertEquals(sessionId, admitted.sessionId());
        assertEquals("trainer.example", admitted.principal());
        assertEquals("/outputs/run-1", admitted.rootPath());
    }

    @Test
    void advertisesInitialAndMaximumCordsForClientSideRamp() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();

        TransferAcceptedResponse response = controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        assertEquals(1, response.acceptedInitialCords());
        assertEquals(64, response.acceptedMaxCords());
        assertEquals(512L * 1024 * 1024, response.flowControlWindowBytes());
    }

    @Test
    void rejectsDuplicateLiveWriteTransferAdmission() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();

        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        assertThrows(
                IllegalStateException.class,
                () -> controller.admit(writeBootstrap(transferId, "/outputs/run-2"))
        );
        assertEquals("/outputs/run-1", controller.admittedTransfer(transferId).orElseThrow().rootPath());
    }

    @Test
    void expiresAdmittedWriteTransfers() {
        UUID transferId = UUID.randomUUID();
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        WriteAdmissionController controller = new WriteAdmissionController(
                WriteAdmissionPolicy.defaults(),
                CordPoolConfig.xetLikeDefaults(),
                new WriteAdmissionLimits(Duration.ofSeconds(5), 16),
                clock
        );
        controller.authenticateControlSession(UUID.randomUUID(), "trainer.example");

        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));
        assertEquals(1, controller.admittedTransferCount());

        clock.advance(Duration.ofSeconds(6));

        assertEquals(0, controller.admittedTransferCount());
        assertThrows(IllegalStateException.class, () -> controller.requirePayloadAdmission(transferId));
        assertEquals(List.of(), controller.pruneExpiredTransfers());
    }

    @Test
    void capsAdmittedWriteTransfers() {
        WriteAdmissionController controller = new WriteAdmissionController(
                WriteAdmissionPolicy.defaults(),
                CordPoolConfig.xetLikeDefaults(),
                new WriteAdmissionLimits(Duration.ofMinutes(10), 1),
                Clock.systemUTC()
        );
        controller.authenticateControlSession(UUID.randomUUID(), "trainer.example");
        controller.admit(writeBootstrap(UUID.randomUUID(), "/outputs/run-1"));

        assertThrows(
                IllegalStateException.class,
                () -> controller.admit(writeBootstrap(UUID.randomUUID(), "/outputs/run-2"))
        );
        assertEquals(1, controller.admittedTransferCount());
    }

    @Test
    void rejectsAdmissionBeforeAuthentication() {
        WriteAdmissionController controller = new WriteAdmissionController();

        assertThrows(
                IllegalStateException.class,
                () -> controller.admit(writeBootstrap(UUID.randomUUID(), "/outputs/run-1"))
        );
    }

    @Test
    void admitsSpecificSessionAndClampsCordCountsToServerPool() {
        UUID sessionId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = new WriteAdmissionController(
                WriteAdmissionPolicy.defaults(),
                new CordPoolConfig(4, 16, false)
        );

        TransferAcceptedResponse response = controller.admit(
                sessionId,
                "trainer.example",
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.WRITE,
                        "outputs/run-1",
                        TransferPayloadFormat.SAFETENSORS,
                        8,
                        64,
                        64 * 1024 * 1024,
                        10L * 1024 * 1024 * 1024
                )
        );

        assertEquals(16, response.acceptedInitialCords());
        assertEquals(16, response.acceptedMaxCords());
        assertEquals(sessionId, controller.admittedTransfer(transferId).orElseThrow().sessionId());
        assertEquals(16, controller.admittedTransfer(transferId).orElseThrow().maxCords());
    }

    @Test
    void reservesWritePayloadOnlyOnAcceptedCords() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = new WriteAdmissionController(
                WriteAdmissionPolicy.defaults(),
                new CordPoolConfig(1, 2, true)
        );
        controller.authenticateControlSession(UUID.randomUUID(), "trainer.example");
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        controller.reservePayload(payloadFrame(transferId, 1, 0, "accepted"));

        assertThrows(IllegalStateException.class, () ->
                controller.reservePayload(payloadFrame(transferId, 2, 1, "outside")));
    }

    @Test
    void reservesWritePayloadAgainstExpectedTransferSizeLimit() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1", 8));

        controller.reservePayload(payloadFrame(transferId, 0, 0, "12345"));

        assertThrows(IllegalStateException.class, () ->
                controller.reservePayload(payloadFrame(transferId, 0, 1, "6789")));
    }

    @Test
    void rejectsTraversalSegmentsInAdmittedRootsAndLateEntryPaths() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();

        assertThrows(IllegalArgumentException.class, () -> controller.admit(
                writeBootstrap(transferId, "/outputs/../run-1")
        ));

        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        assertThrows(IllegalArgumentException.class, () -> controller.acceptEntryMetadata(new TransferEntryUpdate(
                transferId,
                1,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/run-1/../model.safetensors",
                "",
                128,
                TransferPayloadFormat.SAFETENSORS
        )));
    }

    @Test
    void acceptsLateEntryMetadataOnlyInsideAdmittedTree() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        controller.acceptEntryMetadata(new TransferEntryUpdate(
                transferId,
                1,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/run-1/model.safetensors",
                "",
                128,
                TransferPayloadFormat.SAFETENSORS
        ));

        assertThrows(IllegalArgumentException.class, () -> controller.acceptEntryMetadata(new TransferEntryUpdate(
                transferId,
                2,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/other/model.safetensors",
                "",
                128,
                TransferPayloadFormat.SAFETENSORS
        )));
    }

    @Test
    void finalCommitPublishesAndClosesPayloadAdmission() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));
        ContentHash entryHash = hash(7);
        controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                entryHash,
                1024
        ));

        TransferCommittedResponse response = controller.commit(new TransferCommitRequest(
                transferId,
                "/outputs/run-1",
                Hashing.transfer(List.of(entryHash), List.of(1024L)),
                1024
        ));

        assertEquals(transferId, response.transferId());
        assertFalse(controller.canBufferPayload(transferId));
        assertTrue(controller.admittedTransfer(transferId).isEmpty());
    }

    @Test
    void finalCommitRejectsTransferHashThatDoesNotMatchCommittedEntries() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));
        controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                hash(7),
                1024
        ));

        assertThrows(IllegalStateException.class, () -> controller.commit(new TransferCommitRequest(
                transferId,
                "/outputs/run-1",
                Hashing.transfer(List.of(hash(8)), List.of(1024L)),
                1024
        )));
        assertTrue(controller.canBufferPayload(transferId));
    }

    @Test
    void finalCommitRejectsByteTotalThatDoesNotMatchCommittedEntries() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));
        ContentHash entryHash = hash(7);
        controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                entryHash,
                1024
        ));

        assertThrows(IllegalStateException.class, () -> controller.commit(new TransferCommitRequest(
                transferId,
                "/outputs/run-1",
                Hashing.transfer(List.of(entryHash), List.of(1024L)),
                512
        )));
        assertTrue(controller.canBufferPayload(transferId));
    }

    @Test
    void finalCommitRejectsNonEmptyTransferWithoutCommittedEntries() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        assertThrows(IllegalStateException.class, () -> controller.commit(new TransferCommitRequest(
                transferId,
                "/outputs/run-1",
                Hashing.transfer(List.of(hash(7)), List.of(1024L)),
                1024
        )));
        assertTrue(controller.canBufferPayload(transferId));
    }

    @Test
    void entrySequenceCanOnlyCommitOnce() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));
        controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                2,
                "/outputs/run-1/model.safetensors",
                hash(9),
                256
        ));

        assertThrows(IllegalStateException.class, () -> controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                2,
                "/outputs/run-1/model-copy.safetensors",
                hash(10),
                128
        )));
    }

    @Test
    void entryCommitDoesNotCloseTreeTransferAdmission() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        TransferEntryCommittedResponse response = controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                2,
                "/outputs/run-1/model.safetensors",
                hash(9),
                256
        ));

        assertEquals(2, response.entrySequence());
        assertTrue(controller.canBufferPayload(transferId));
        assertTrue(controller.admittedTransfer(transferId).isPresent());
    }

    @Test
    void entryCommitMustStayInsideAdmittedTree() {
        UUID transferId = UUID.randomUUID();
        WriteAdmissionController controller = authenticatedController();
        controller.admit(writeBootstrap(transferId, "/outputs/run-1"));

        assertThrows(IllegalArgumentException.class, () -> controller.commitEntry(new TransferEntryCommitRequest(
                transferId,
                2,
                "/outputs/other/model.safetensors",
                hash(9),
                256
        )));
    }

    private static WriteAdmissionController authenticatedController() {
        WriteAdmissionController controller = new WriteAdmissionController();
        controller.authenticateControlSession(UUID.randomUUID(), "trainer.example");
        return controller;
    }

    private static TransferBootstrapRequest writeBootstrap(UUID transferId, String rootPath) {
        return writeBootstrap(transferId, rootPath, 10L * 1024 * 1024 * 1024);
    }

    private static TransferBootstrapRequest writeBootstrap(UUID transferId, String rootPath, long expectedSizeLimitBytes) {
        return new TransferBootstrapRequest(
                transferId,
                TransferDirection.WRITE,
                rootPath,
                TransferPayloadFormat.SAFETENSORS,
                1,
                64,
                64 * 1024 * 1024,
                expectedSizeLimitBytes
        );
    }

    private static BulkDataFrame payloadFrame(UUID transferId, int cordIndex, long chunkSequence, String payload) {
        byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new BulkDataFrame(
                transferId,
                cordIndex,
                0,
                chunkSequence,
                chunkSequence * 4096,
                true,
                bytes
        );
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
