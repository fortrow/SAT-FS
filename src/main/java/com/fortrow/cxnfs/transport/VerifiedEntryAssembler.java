package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.checksum.TransferChecksums;
import com.fortrow.cxnfs.protocol.TransferChecksum;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;

public final class VerifiedEntryAssembler {
    private final BoundedPayloadStagingArea stagingArea;

    public VerifiedEntryAssembler(BoundedPayloadStagingArea stagingArea) {
        if (stagingArea == null) {
            throw new IllegalArgumentException("stagingArea is required");
        }
        this.stagingArea = stagingArea;
    }

    public VerifiedEntry assemble(TransferEntryCommitRequest request) throws IOException {
        return assemble(request, VerifiedEntryPublisher.NOOP);
    }

    public VerifiedEntry assemble(TransferEntryCommitRequest request, VerifiedEntryPublisher publisher) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (publisher == null) {
            throw new IllegalArgumentException("publisher is required");
        }

        List<StagedPayload> payloads = stagingArea.verifiedPayloads(request.transferId(), request.entrySequence()).stream()
                .sorted(Comparator.comparingLong(payload -> payload.key().chunkSequence()))
                .toList();
        if (payloads.isEmpty() && request.bytesWritten() > 0) {
            throw new IllegalStateException("entry has no verified payloads: " + request.path());
        }

        List<ChunkRef> chunks = new java.util.ArrayList<>(payloads.size());
        List<ContentHash> chunkHashes = new java.util.ArrayList<>(payloads.size());
        List<Integer> chunkSizes = new java.util.ArrayList<>(payloads.size());
        CRC32C checksum = new CRC32C();

        long expectedOffset = 0;
        long expectedChunkSequence = 0;
        boolean publishingStarted = false;
        try {
            for (StagedPayload payload : payloads) {
                if (payload.key().chunkSequence() != expectedChunkSequence) {
                    throw new IllegalStateException("missing verified chunk sequence: " + expectedChunkSequence);
                }
                if (payload.entryOffset() != expectedOffset) {
                    throw new IllegalStateException("non-contiguous verified payload at offset: " + payload.entryOffset());
                }
                if (!payload.finalFrameForChunk()) {
                    throw new IllegalStateException("verified payload is not final for chunk: " + payload.key());
                }
                byte[] bytes = stagingArea.readPayload(payload.key());
                if (bytes.length != payload.payloadLength()) {
                    throw new IOException("staged payload length changed for " + payload.key());
                }
                ContentHash chunkHash = Hashing.chunk(bytes);
                checksum.update(bytes, 0, bytes.length);
                chunks.add(new ChunkRef(chunkHash, bytes.length));
                chunkHashes.add(chunkHash);
                chunkSizes.add(bytes.length);
                publishingStarted = true;
                publisher.acceptChunk(chunkHash, payload.entryOffset(), bytes);
                stagingArea.releasePayload(payload.key());
                expectedOffset += bytes.length;
                expectedChunkSequence++;
            }
        } catch (IOException | RuntimeException e) {
            if (publishingStarted) {
                try {
                    publisher.abort();
                } catch (IOException abortFailure) {
                    e.addSuppressed(abortFailure);
                }
                try {
                    stagingArea.releaseEntry(request.transferId(), request.entrySequence());
                } catch (IOException releaseFailure) {
                    e.addSuppressed(releaseFailure);
                }
            }
            throw e;
        }

        if (expectedOffset != request.bytesWritten()) {
            throw new IllegalStateException("verified entry size does not match commit request");
        }
        ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
        if (!fileHash.equals(request.contentHash())) {
            throw new IOException("verified entry hash does not match commit request");
        }
        verifyChecksum(request.checksum(), TransferChecksums.crc32c(checksum));

        FileManifest manifest = new FileManifest(request.path(), fileHash, request.bytesWritten(), chunks);
        FileManifest publishedManifest;
        try {
            publishedManifest = publisher.publishManifest(manifest);
        } catch (IOException | RuntimeException e) {
            try {
                publisher.abort();
            } catch (IOException abortFailure) {
                e.addSuppressed(abortFailure);
            }
            throw e;
        }
        return new VerifiedEntry(request.transferId(), request.entrySequence(), publishedManifest, expectedOffset);
    }

    private static void verifyChecksum(TransferChecksum expected, TransferChecksum actual) throws IOException {
        expected = expected == null ? TransferChecksum.none() : expected;
        if (expected.present() && !expected.equals(actual)) {
            throw new IOException("verified entry checksum mismatch: expected "
                    + expected.display() + ", got " + actual.display());
        }
    }

    public int release(UUID transferId, int entrySequence) throws IOException {
        return stagingArea.releaseEntry(transferId, entrySequence);
    }
}
