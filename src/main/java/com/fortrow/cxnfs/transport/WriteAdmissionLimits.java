package com.fortrow.cxnfs.transport;

import java.time.Duration;

public record WriteAdmissionLimits(
        Duration writeTransferTtl,
        int maxAdmittedWriteTransfers
) {
    public static WriteAdmissionLimits defaults() {
        return new WriteAdmissionLimits(Duration.ofMinutes(10), 4096);
    }

    public WriteAdmissionLimits {
        if (writeTransferTtl == null || writeTransferTtl.isNegative() || writeTransferTtl.isZero()) {
            throw new IllegalArgumentException("writeTransferTtl must be positive");
        }
        if (maxAdmittedWriteTransfers <= 0) {
            throw new IllegalArgumentException("maxAdmittedWriteTransfers must be positive");
        }
    }
}
