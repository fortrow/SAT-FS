package com.fortrow.cxnfs.server;

import java.time.Duration;

public record ControlSessionLimits(
        Duration authChallengeTtl,
        int maxPendingAuthChallenges,
        int maxAuthenticatedSessions
) {
    public static ControlSessionLimits defaults() {
        return new ControlSessionLimits(Duration.ofMinutes(2), 4096, 16384);
    }

    public ControlSessionLimits {
        if (authChallengeTtl == null || authChallengeTtl.isNegative() || authChallengeTtl.isZero()) {
            throw new IllegalArgumentException("authChallengeTtl must be positive");
        }
        if (maxPendingAuthChallenges <= 0) {
            throw new IllegalArgumentException("maxPendingAuthChallenges must be positive");
        }
        if (maxAuthenticatedSessions <= 0) {
            throw new IllegalArgumentException("maxAuthenticatedSessions must be positive");
        }
    }
}
