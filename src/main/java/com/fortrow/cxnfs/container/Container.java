package com.fortrow.cxnfs.container;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.List;

public record Container(
        ContentHash hash,
        List<StoredChunk> chunks
) {
    public Container {
        chunks = List.copyOf(chunks);
    }

    public StoredChunk chunkAt(int index) {
        return chunks.get(index);
    }
}
