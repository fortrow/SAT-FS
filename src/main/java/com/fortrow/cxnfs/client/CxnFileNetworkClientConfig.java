package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.transport.CordPoolConfig;

public record CxnFileNetworkClientConfig(
        int initialCords,
        int maxCords,
        int provisionalContainerSizeBytes,
        long expectedSizeLimitBytes,
        int connectTimeoutMillis,
        int readTimeoutMillis
) {
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 15_000;
    public static final int DEFAULT_READ_TIMEOUT_MILLIS = 120_000;

    public static CxnFileNetworkClientConfig defaults() {
        return new CxnFileNetworkClientConfig(
                1,
                CordPoolConfig.DEFAULT_MAX_CORDS,
                64 * 1024 * 1024,
                0,
                DEFAULT_CONNECT_TIMEOUT_MILLIS,
                DEFAULT_READ_TIMEOUT_MILLIS
        );
    }

    public CxnFileNetworkClientConfig(
            int initialCords,
            int maxCords,
            int provisionalContainerSizeBytes,
            long expectedSizeLimitBytes
    ) {
        this(
                initialCords,
                maxCords,
                provisionalContainerSizeBytes,
                expectedSizeLimitBytes,
                DEFAULT_CONNECT_TIMEOUT_MILLIS,
                DEFAULT_READ_TIMEOUT_MILLIS
        );
    }

    public CxnFileNetworkClientConfig {
        if (initialCords <= 0) {
            throw new IllegalArgumentException("initialCords must be positive");
        }
        if (maxCords <= 0 || maxCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("maxCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (initialCords > maxCords) {
            throw new IllegalArgumentException("initialCords must be <= maxCords");
        }
        if (provisionalContainerSizeBytes <= 0) {
            throw new IllegalArgumentException("provisionalContainerSizeBytes must be positive");
        }
        if (expectedSizeLimitBytes < 0) {
            throw new IllegalArgumentException("expectedSizeLimitBytes must be non-negative");
        }
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectTimeoutMillis must be positive");
        }
        if (readTimeoutMillis <= 0) {
            throw new IllegalArgumentException("readTimeoutMillis must be positive");
        }
    }
}
