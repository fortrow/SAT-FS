package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.store.CxnfsStore;

import java.io.IOException;

public final class LinuxBlockfsStoreChunkFetcher implements LinuxBlockfsChunkFetcher {
    private final CxnfsStore store;

    public LinuxBlockfsStoreChunkFetcher(CxnfsStore store) {
        if (store == null) {
            throw new IllegalArgumentException("store is required");
        }
        this.store = store;
    }

    @Override
    public byte[] fetchChunk(LinuxBlockfsManifestEntry entry, LinuxBlockfsManifestChunk chunk)
            throws IOException {
        if (entry == null) {
            throw new IllegalArgumentException("entry is required");
        }
        if (chunk == null) {
            throw new IllegalArgumentException("chunk is required");
        }
        return store.readRange(entry.fileHash(), chunk.fileOffset(), chunk.length());
    }
}
