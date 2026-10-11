package com.fortrow.cxnfs.transport;

public record CordSchedulerDecision(
        BackpressureDecision decision,
        int recommendedCords,
        String reason
) {
    public CordSchedulerDecision {
        if (decision == null) {
            throw new IllegalArgumentException("decision is required");
        }
        if (recommendedCords <= 0) {
            throw new IllegalArgumentException("recommendedCords must be positive");
        }
        reason = reason == null ? "" : reason;
    }
}
