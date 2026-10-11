package com.fortrow.cxnfs.io;

import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;

import java.util.ArrayList;
import java.util.List;

public final class ReadPlanner {
    public ReadPlan plan(FileManifest manifest, long offset, long length) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        ByteRange requested = clampToFile(manifest.size(), new ByteRange(offset, length));
        if (requested.isEmpty()) {
            return new ReadPlan(manifest.fileHash(), manifest.size(), requested, List.of());
        }

        List<PlannedChunkRead> chunks = new ArrayList<>();
        long chunkFileOffset = 0;
        long readEnd = requested.endExclusive();
        for (int i = 0; i < manifest.chunks().size(); i++) {
            ChunkRef chunk = manifest.chunks().get(i);
            long chunkEnd = chunkFileOffset + chunk.size();
            if (chunkEnd > requested.offset() && chunkFileOffset < readEnd) {
                int offsetInChunk = Math.toIntExact(Math.max(0, requested.offset() - chunkFileOffset));
                int bytesToRead = Math.toIntExact(Math.min(chunkEnd, readEnd) - (chunkFileOffset + offsetInChunk));
                chunks.add(new PlannedChunkRead(
                        i,
                        chunk.hash(),
                        chunkFileOffset,
                        chunk.size(),
                        offsetInChunk,
                        bytesToRead
                ));
            }
            if (chunkEnd >= readEnd) {
                break;
            }
            chunkFileOffset = chunkEnd;
        }
        return new ReadPlan(manifest.fileHash(), manifest.size(), requested, chunks);
    }

    private static ByteRange clampToFile(long fileSize, ByteRange range) {
        if (range.offset() >= fileSize || range.isEmpty()) {
            return new ByteRange(Math.min(range.offset(), fileSize), 0);
        }
        long end = Math.min(fileSize, range.endExclusive());
        return new ByteRange(range.offset(), end - range.offset());
    }
}
