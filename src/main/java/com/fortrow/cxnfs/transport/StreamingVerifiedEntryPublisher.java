package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;

import java.io.IOException;

public interface StreamingVerifiedEntryPublisher extends VerifiedEntryPublisher {
    StagedPayload acceptFrame(BulkDataFrame frame) throws IOException;

    int acceptMetadata(TransferMetadataUpdate update) throws IOException;

    VerifiedEntry publishEntry(TransferEntryCommitRequest request) throws IOException;
}
