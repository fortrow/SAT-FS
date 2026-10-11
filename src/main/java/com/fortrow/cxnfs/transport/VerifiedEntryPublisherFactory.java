package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;

import java.io.IOException;

@FunctionalInterface
public interface VerifiedEntryPublisherFactory {
    VerifiedEntryPublisher forEntry(TransferEntryCommitRequest request) throws IOException;
}
