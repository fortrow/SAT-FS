package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;

import java.io.IOException;

public final class EntryPublishCoordinator {
    private final WriteAdmissionController admissionController;
    private final VerifiedEntryAssembler assembler;
    private final VerifiedEntryPublisherFactory publisherFactory;

    public EntryPublishCoordinator(WriteAdmissionController admissionController, VerifiedEntryAssembler assembler) {
        this(admissionController, assembler, VerifiedEntryPublisher.NOOP);
    }

    public EntryPublishCoordinator(
            WriteAdmissionController admissionController,
            VerifiedEntryAssembler assembler,
            VerifiedEntryPublisher publisher
    ) {
        this(admissionController, assembler, request -> publisher);
    }

    public EntryPublishCoordinator(
            WriteAdmissionController admissionController,
            VerifiedEntryAssembler assembler,
            VerifiedEntryPublisherFactory publisherFactory
    ) {
        if (admissionController == null) {
            throw new IllegalArgumentException("admissionController is required");
        }
        if (assembler == null) {
            throw new IllegalArgumentException("assembler is required");
        }
        if (publisherFactory == null) {
            throw new IllegalArgumentException("publisherFactory is required");
        }
        this.admissionController = admissionController;
        this.assembler = assembler;
        this.publisherFactory = publisherFactory;
    }

    public PublishedEntry publish(TransferEntryCommitRequest request) throws IOException {
        TransferEntryCommittedResponse response = admissionController.commitEntry(request);
        VerifiedEntryPublisher publisher = publisherFactory.forEntry(request);
        VerifiedEntry verifiedEntry;
        try {
            verifiedEntry = assembler.assemble(request, publisher);
        } catch (IOException | RuntimeException e) {
            admissionController.rollbackEntryCommit(request.transferId(), request.entrySequence());
            throw e;
        }
        assembler.release(request.transferId(), request.entrySequence());
        return new PublishedEntry(verifiedEntry, response);
    }

    public PublishedEntry publishStreaming(
            TransferEntryCommitRequest request,
            StreamingVerifiedEntryPublisher publisher
    ) throws IOException {
        if (publisher == null) {
            throw new IllegalArgumentException("publisher is required");
        }
        TransferEntryCommittedResponse response = admissionController.commitEntry(request);
        VerifiedEntry verifiedEntry;
        try {
            verifiedEntry = publisher.publishEntry(request);
        } catch (IOException | RuntimeException e) {
            admissionController.rollbackEntryCommit(request.transferId(), request.entrySequence());
            throw e;
        }
        return new PublishedEntry(verifiedEntry, response);
    }
}
