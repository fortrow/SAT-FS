package com.fortrow.cxnfs.protocol;

public record ProtocolVersion(
        int major,
        int minor
) {
    public static final ProtocolVersion CURRENT = new ProtocolVersion(0, 1);

    public ProtocolVersion {
        if (major < 0 || minor < 0) {
            throw new IllegalArgumentException("protocol version numbers must be non-negative");
        }
    }
}
