package com.fortrow.cxnfs.performance;

import com.fortrow.cxnfs.io.ReadPlanner;
import com.fortrow.cxnfs.manifest.FileManifest;

public final class ReadAheadPlanner {
    private final ReadPlanner readPlanner = new ReadPlanner();

    public ReadAheadPlan plan(FileManifest manifest, long offset, long length, ReadAheadPolicy policy) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        long requestedEnd = Math.min(manifest.size(), Math.addExact(offset, length));
        long prefetchEnd = Math.min(
                manifest.size(),
                Math.min(Math.addExact(offset, policy.maxPrefetchBytes()), Math.addExact(requestedEnd, policy.forwardWindowBytes()))
        );
        long prefetchLength = Math.max(0, prefetchEnd - Math.min(offset, manifest.size()));
        return new ReadAheadPlan(
                readPlanner.plan(manifest, offset, length),
                readPlanner.plan(manifest, offset, prefetchLength)
        );
    }
}
