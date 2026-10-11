package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.ChunkRef;

import java.util.List;

public record PutFileRequest(
        String path,
        long size,
        ContentHash fileHash,
        List<ChunkRef> chunks
) implements ControlPayload {
    public PutFileRequest {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        if (fileHash == null) {
            throw new IllegalArgumentException("fileHash is required");
        }
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.PUT_FILE;
    }
}
