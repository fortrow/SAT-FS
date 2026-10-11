package com.fortrow.cxnfs.transport;

public record CordPoolConfig(
        int initialCords,
        int maxCords,
        boolean adaptive
) {
    public static final int ABSOLUTE_MAX_CORDS = 128;
    public static final int DEFAULT_MAX_CORDS = 64;

    public static CordPoolConfig xetLikeDefaults() {
        return new CordPoolConfig(1, DEFAULT_MAX_CORDS, true);
    }

    public static CordPoolConfig fixed(int cords) {
        return new CordPoolConfig(cords, cords, false);
    }

    public CordPoolConfig {
        if (initialCords <= 0) {
            throw new IllegalArgumentException("initialCords must be positive");
        }
        if (maxCords <= 0) {
            throw new IllegalArgumentException("maxCords must be positive");
        }
        if (initialCords > maxCords) {
            throw new IllegalArgumentException("initialCords must be <= maxCords");
        }
        if (maxCords > ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("maxCords cannot exceed " + ABSOLUTE_MAX_CORDS);
        }
    }
}
