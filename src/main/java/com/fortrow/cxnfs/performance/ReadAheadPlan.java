package com.fortrow.cxnfs.performance;

import com.fortrow.cxnfs.io.ReadPlan;

public record ReadAheadPlan(
        ReadPlan requested,
        ReadPlan prefetch
) {
    public ReadAheadPlan {
        if (requested == null) {
            throw new IllegalArgumentException("requested plan is required");
        }
        if (prefetch == null) {
            throw new IllegalArgumentException("prefetch plan is required");
        }
        if (prefetch.requestedRange().offset() != requested.requestedRange().offset()) {
            throw new IllegalArgumentException("prefetch must start at the requested range offset");
        }
        if (prefetch.requestedRange().length() < requested.requestedRange().length()) {
            throw new IllegalArgumentException("prefetch must cover at least the requested range");
        }
    }
}
