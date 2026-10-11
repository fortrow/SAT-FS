package com.fortrow.cxnfs.container;

import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.codec.CompressionCodec;
import com.fortrow.cxnfs.format.ContainerCodec;
import com.fortrow.cxnfs.hash.ContentHash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ContainerBuilder {
    private final CompressionCodec codec;

    public ContainerBuilder(CompressionCodec codec) {
        this.codec = codec;
    }

    public Container build(List<Chunk> chunks) throws IOException {
        List<StoredChunk> storedChunks = new ArrayList<>(chunks.size());
        for (Chunk chunk : chunks) {
            storedChunks.add(new StoredChunk(
                    chunk.hash(),
                    chunk.size(),
                    codec.id(),
                    codec.compress(chunk.data())
            ));
        }
        ContentHash hash = ContainerCodec.hash(storedChunks);
        return new Container(hash, storedChunks);
    }
}
