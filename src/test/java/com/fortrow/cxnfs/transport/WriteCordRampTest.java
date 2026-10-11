package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class WriteCordRampTest {
    @Test
    void keepsSmallWritesOnOneCord() {
        AtomicLong now = new AtomicLong();
        WriteCordRamp ramp = new WriteCordRamp(transfer(1, 64), 1024, now::get);

        now.set(5_000_000_000L);

        assertEquals(1, ramp.activeCords());
        assertEquals(0, ramp.cordIndex(7));
    }

    @Test
    void holdsInitialCordThenDoublesToMaximumOverAboutThreeSeconds() {
        AtomicLong now = new AtomicLong();
        WriteCordRamp ramp = new WriteCordRamp(
                transfer(1, CordPoolConfig.ABSOLUTE_MAX_CORDS),
                2L * 1024 * 1024 * 1024,
                now::get
        );

        assertEquals(1, ramp.activeCords());
        now.set(500_000_000L);
        assertEquals(1, ramp.activeCords());
        now.set(1_000_000_000L);
        assertEquals(2, ramp.activeCords());
        now.set(1_400_000_000L);
        assertEquals(4, ramp.activeCords());
        now.set(1_800_000_000L);
        assertEquals(8, ramp.activeCords());
        now.set(3_000_000_000L);
        assertEquals(64, ramp.activeCords());
        now.set(3_400_000_000L);
        assertEquals(128, ramp.activeCords());
        assertEquals(1, ramp.cordIndex(129));
    }

    private static TransferAcceptedResponse transfer(int initialCords, int maxCords) {
        return new TransferAcceptedResponse(
                UUID.randomUUID(),
                TransferDirection.WRITE,
                initialCords,
                maxCords,
                128L * 1024 * 1024,
                true
        );
    }
}
