package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.FileManifest;

import java.io.IOException;

public interface VerifiedEntryPublisher {
    VerifiedEntryPublisher NOOP = new VerifiedEntryPublisher() {
        @Override
        public void acceptChunk(ContentHash chunkHash, long sourceOffset, byte[] data) {
            // Test and policy-only paths can verify without mutating a repository.
        }

        @Override
        public FileManifest publishManifest(FileManifest manifest) {
            return manifest;
        }
    };

    void acceptChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException;

    FileManifest publishManifest(FileManifest manifest) throws IOException;

    default void abort() throws IOException {
    }
}
