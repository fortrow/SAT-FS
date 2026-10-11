package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;

public record PublishedEntry(
        VerifiedEntry entry,
        TransferEntryCommittedResponse response
) {
    public PublishedEntry {
        if (entry == null) {
            throw new IllegalArgumentException("entry is required");
        }
        if (response == null) {
            throw new IllegalArgumentException("response is required");
        }
    }
}
