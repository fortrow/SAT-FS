package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.metadata.DirectoryListing;

public record DirectoryListResponse(
        DirectoryListing listing
) implements ControlPayload {
    public DirectoryListResponse {
        if (listing == null) {
            throw new IllegalArgumentException("listing is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.LIST_DIRECTORY_RESULT;
    }
}
