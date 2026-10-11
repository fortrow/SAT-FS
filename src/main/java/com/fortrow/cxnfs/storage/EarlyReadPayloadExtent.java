package com.fortrow.cxnfs.storage;

public record EarlyReadPayloadExtent(
        int chunkIndex,
        long fileOffset,
        int length,
        boolean mayReadBeforeTrailingMetadata,
        boolean publishRequiresTrailingMetadata
) {
    public EarlyReadPayloadExtent {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("chunkIndex must be non-negative");
        }
        if (fileOffset < 0) {
            throw new IllegalArgumentException("fileOffset must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (mayReadBeforeTrailingMetadata && !publishRequiresTrailingMetadata) {
            throw new IllegalArgumentException("early payload reads must be gated before publish");
        }
    }
}
