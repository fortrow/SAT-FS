package com.fortrow.cxnfs.storage;

import java.util.List;

public record LinuxBlockfsReadMissPlan(
        LinuxBlockfsMissEvent event,
        List<LinuxBlockfsReadMissChunk> chunks
) {
    public LinuxBlockfsReadMissPlan {
        if (event == null) {
            throw new IllegalArgumentException("event is required");
        }
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
    }

    public boolean hasChunks() {
        return !chunks.isEmpty();
    }

    public long plannedBytes() {
        long total = 0;
        for (LinuxBlockfsReadMissChunk chunk : chunks) {
            total = Math.addExact(total, chunk.length());
        }
        return total;
    }
}
