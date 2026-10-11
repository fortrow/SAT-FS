package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ReadAdmissionControllerTest {
    @Test
    void rejectsReadPayloadBeforeAuthenticatedControlSession() {
        ReadAdmissionController controller = new ReadAdmissionController();
        TransferBootstrapRequest request = readBootstrap(UUID.randomUUID(), "/datasets/train/shard.mds", 8, 64);

        assertThrows(IllegalStateException.class, () -> controller.admit(request));
    }

    @Test
    void admitsReadTransferWithServerCordLimitsAndNoWritePayloadAdmission() {
        UUID transferId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();
        ReadAdmissionController controller = new ReadAdmissionController(new CordPoolConfig(1, 16, true));
        controller.authenticateControlSession(sessionId, "trainer");

        TransferAcceptedResponse response = controller.admit(
                readBootstrap(transferId, "/datasets/train/shard.mds", 8, 64)
        );
        AdmittedReadTransfer admitted = controller.admittedTransfer(transferId).orElseThrow();

        assertEquals(transferId, response.transferId());
        assertEquals(TransferDirection.READ, response.direction());
        assertEquals(8, response.acceptedInitialCords());
        assertEquals(16, response.acceptedMaxCords());
        assertFalse(response.writePayloadAdmissionGranted());
        assertEquals(sessionId, admitted.sessionId());
        assertEquals("trainer", admitted.principal());
        assertEquals("/datasets/train/shard.mds", admitted.path());
        assertEquals(TransferPayloadFormat.MDS_SHARDS, admitted.payloadFormat());
    }

    @Test
    void rejectsTraversalSegmentsInAdmittedReadPaths() {
        ReadAdmissionController controller = new ReadAdmissionController();
        controller.authenticateControlSession(UUID.randomUUID(), "trainer");

        assertThrows(
                IllegalArgumentException.class,
                () -> controller.admit(readBootstrap(UUID.randomUUID(), "/datasets/../train/shard.mds", 1, 64))
        );
    }

    @Test
    void rejectsDuplicateLiveReadTransferAdmission() {
        UUID transferId = UUID.randomUUID();
        ReadAdmissionController controller = new ReadAdmissionController();
        controller.authenticateControlSession(UUID.randomUUID(), "trainer");

        controller.admit(readBootstrap(transferId, "/datasets/train/shard.mds", 8, 64));

        assertThrows(
                IllegalStateException.class,
                () -> controller.admit(readBootstrap(transferId, "/datasets/train/other.mds", 1, 8))
        );
        assertEquals("/datasets/train/shard.mds", controller.admittedTransfer(transferId).orElseThrow().path());
        assertEquals(1, controller.admittedTransferCount());
    }

    @Test
    void fixedCordPoolsAdvertiseTheFixedCordCount() {
        UUID transferId = UUID.randomUUID();
        ReadAdmissionController controller = new ReadAdmissionController(CordPoolConfig.fixed(4));
        controller.authenticateControlSession(UUID.randomUUID(), "trainer");

        TransferAcceptedResponse response = controller.admit(
                readBootstrap(transferId, "/datasets/train/shard.mds", 1, 64)
        );

        assertEquals(4, response.acceptedInitialCords());
        assertEquals(4, response.acceptedMaxCords());
    }

    @Test
    void expiresAdmittedReadTransfers() {
        UUID transferId = UUID.randomUUID();
        MutableClock clock = new MutableClock(Instant.parse("2026-05-31T12:00:00Z"));
        ReadAdmissionController controller = new ReadAdmissionController(
                CordPoolConfig.xetLikeDefaults(),
                new ReadAdmissionLimits(Duration.ofSeconds(5), 16),
                clock
        );
        controller.authenticateControlSession(UUID.randomUUID(), "trainer");

        controller.admit(readBootstrap(transferId, "/datasets/train/shard.mds", 1, 64));
        assertEquals(1, controller.admittedTransferCount());

        clock.advance(Duration.ofSeconds(6));

        assertEquals(0, controller.admittedTransferCount());
        assertThrows(IllegalStateException.class, () -> controller.requireAdmitted(transferId));
    }

    @Test
    void capsAdmittedReadTransfers() {
        ReadAdmissionController controller = new ReadAdmissionController(
                CordPoolConfig.xetLikeDefaults(),
                new ReadAdmissionLimits(Duration.ofMinutes(10), 1),
                Clock.systemUTC()
        );
        controller.authenticateControlSession(UUID.randomUUID(), "trainer");
        UUID first = UUID.randomUUID();

        controller.admit(readBootstrap(first, "/datasets/train/shard.mds", 1, 64));

        assertThrows(
                IllegalStateException.class,
                () -> controller.admit(readBootstrap(UUID.randomUUID(), "/datasets/train/other.mds", 1, 64))
        );
        assertEquals(1, controller.admittedTransferCount());
    }

    private static TransferBootstrapRequest readBootstrap(
            UUID transferId,
            String path,
            int initialCords,
            int maxCords
    ) {
        return new TransferBootstrapRequest(
                transferId,
                TransferDirection.READ,
                path,
                TransferPayloadFormat.MDS_SHARDS,
                initialCords,
                maxCords,
                64 * 1024 * 1024,
                0
        );
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
