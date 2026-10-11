package com.fortrow.cxnfs.transport;

import java.time.Duration;

public record ReadAdmissionLimits(
        Duration readTransferTtl,
        int maxAdmittedReadTransfers
) {
    public static ReadAdmissionLimits defaults() {
        return new ReadAdmissionLimits(Duration.ofMinutes(10), 4096);
    }

    public ReadAdmissionLimits {
        if (readTransferTtl == null || readTransferTtl.isNegative() || readTransferTtl.isZero()) {
            throw new IllegalArgumentException("readTransferTtl must be positive");
        }
        if (maxAdmittedReadTransfers <= 0) {
            throw new IllegalArgumentException("maxAdmittedReadTransfers must be positive");
        }
    }
}
