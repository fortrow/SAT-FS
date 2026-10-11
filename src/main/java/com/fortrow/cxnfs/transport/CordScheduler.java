package com.fortrow.cxnfs.transport;

public final class CordScheduler {
    private final CordPoolConfig cordPool;
    private final TransferBackpressurePolicy policy;

    public CordScheduler(CordPoolConfig cordPool, TransferBackpressurePolicy policy) {
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        this.cordPool = cordPool;
        this.policy = policy;
    }

    public static CordScheduler defaults() {
        return new CordScheduler(CordPoolConfig.xetLikeDefaults(), TransferBackpressurePolicy.defaults());
    }

    public CordSchedulerDecision evaluate(TransferBackpressureSignals signals) {
        if (signals == null) {
            throw new IllegalArgumentException("signals are required");
        }
        int activeCords = clamp(signals.activeCords(), 1, cordPool.maxCords());
        if (!cordPool.adaptive()) {
            return new CordSchedulerDecision(BackpressureDecision.HOLD, cordPool.initialCords(), "fixed cord pool");
        }

        if (signals.diskQueueDepth() > policy.maxDiskQueueDepth()) {
            return decrease(activeCords, "disk queue depth is above policy");
        }
        if (signals.bytesInFlight() > policy.maxBytesInFlight()) {
            return decrease(activeCords, "bytes in flight are above policy");
        }
        if (signals.p95DiskLatencyMillis() > policy.maxP95DiskLatencyMillis()) {
            return decrease(activeCords, "p95 disk latency is above policy");
        }
        if (signals.observedThroughputBytesPerSecond() < policy.minThroughputBytesPerSecond()
                && activeCords > cordPool.initialCords()) {
            return decrease(activeCords, "observed throughput is below policy");
        }
        if (isHealthy(signals) && activeCords < cordPool.maxCords()) {
            return new CordSchedulerDecision(BackpressureDecision.INCREASE, activeCords + 1, "disk and network signals are healthy");
        }
        return new CordSchedulerDecision(BackpressureDecision.HOLD, activeCords, "cord count is already appropriate");
    }

    private CordSchedulerDecision decrease(int activeCords, String reason) {
        int recommended = Math.max(cordPool.initialCords(), Math.max(1, activeCords / 2));
        return new CordSchedulerDecision(BackpressureDecision.DECREASE, recommended, reason);
    }

    private boolean isHealthy(TransferBackpressureSignals signals) {
        return signals.diskQueueDepth() <= policy.maxDiskQueueDepth() / 2
                && signals.bytesInFlight() <= policy.maxBytesInFlight() / 2
                && signals.p95DiskLatencyMillis() <= policy.maxP95DiskLatencyMillis() / 2
                && signals.observedThroughputBytesPerSecond() >= policy.minThroughputBytesPerSecond();
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
