package com.fortrow.cxnfs.store;

import java.time.Duration;

public record DiskRepositoryMaintenancePolicy(
        Duration orphanContainerMinAge,
        Duration tempFileMinAge
) {
    public static final DiskRepositoryMaintenancePolicy IMMEDIATE = new DiskRepositoryMaintenancePolicy(
            Duration.ZERO,
            Duration.ZERO
    );

    public DiskRepositoryMaintenancePolicy {
        if (orphanContainerMinAge == null) {
            throw new IllegalArgumentException("orphanContainerMinAge is required");
        }
        if (tempFileMinAge == null) {
            throw new IllegalArgumentException("tempFileMinAge is required");
        }
        if (orphanContainerMinAge.isNegative()) {
            throw new IllegalArgumentException("orphanContainerMinAge must be non-negative");
        }
        if (tempFileMinAge.isNegative()) {
            throw new IllegalArgumentException("tempFileMinAge must be non-negative");
        }
    }
}
