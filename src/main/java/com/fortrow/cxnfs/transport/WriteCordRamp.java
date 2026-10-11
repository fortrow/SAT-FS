package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;

import java.util.function.LongSupplier;

public final class WriteCordRamp {
    private static final long SMALL_TRANSFER_BYTES = 1024L * 1024;
    private static final long INITIAL_RAMP_HOLD_NANOS = 1_000_000_000L;
    private static final long RAMP_STEP_NANOS = 400_000_000L;

    private final int initialCords;
    private final int maxCords;
    private final long totalBytes;
    private final long startedAtNanos;
    private final LongSupplier nanoTime;

    public WriteCordRamp(TransferAcceptedResponse transfer, long totalBytes) {
        this(transfer, totalBytes, System::nanoTime);
    }

    WriteCordRamp(TransferAcceptedResponse transfer, long totalBytes, LongSupplier nanoTime) {
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (totalBytes < 0) {
            throw new IllegalArgumentException("totalBytes must be non-negative");
        }
        if (nanoTime == null) {
            throw new IllegalArgumentException("nanoTime is required");
        }
        this.initialCords = transfer.acceptedInitialCords();
        this.maxCords = transfer.acceptedMaxCords();
        this.totalBytes = totalBytes;
        this.nanoTime = nanoTime;
        this.startedAtNanos = nanoTime.getAsLong();
    }

    public int activeCords() {
        if (totalBytes <= SMALL_TRANSFER_BYTES) {
            return 1;
        }
        long elapsedNanos = Math.max(0L, nanoTime.getAsLong() - startedAtNanos);
        long elapsedSteps = elapsedNanos < INITIAL_RAMP_HOLD_NANOS
                ? 0L
                : ((elapsedNanos - INITIAL_RAMP_HOLD_NANOS) / RAMP_STEP_NANOS) + 1L;
        int active = initialCords;
        for (long step = 0; step < elapsedSteps && active < maxCords; step++) {
            active = Math.min(maxCords, active * 2);
        }
        return Math.max(1, Math.min(active, maxCords));
    }

    public int cordIndex(long chunkSequence) {
        return Math.floorMod(chunkSequence, activeCords());
    }
}
