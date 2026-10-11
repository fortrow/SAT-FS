package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;

public record LinuxBlockfsReadMissChunk(
        LinuxBlockfsManifestEntry entry,
        LinuxBlockfsManifestChunk manifestChunk,
        LinuxBlockfsPayloadAllocation allocation
) {
    public LinuxBlockfsReadMissChunk {
        if (entry == null) {
            throw new IllegalArgumentException("entry is required");
        }
        if (manifestChunk == null) {
            throw new IllegalArgumentException("manifestChunk is required");
        }
        if (allocation == null) {
            throw new IllegalArgumentException("allocation is required");
        }
        if (entry.inodeNumber() != allocation.inodeNumber()) {
            throw new IllegalArgumentException("allocation inode does not match manifest entry");
        }
        if (manifestChunk.fileOffset() != allocation.fileOffset()) {
            throw new IllegalArgumentException("allocation offset does not match manifest chunk");
        }
        if (manifestChunk.length() != allocation.payloadLength()) {
            throw new IllegalArgumentException("allocation length does not match manifest chunk");
        }
    }

    public long inodeNumber() {
        return entry.inodeNumber();
    }

    public long fileOffset() {
        return manifestChunk.fileOffset();
    }

    public int length() {
        return manifestChunk.length();
    }

    public ContentHash chunkHash() {
        return manifestChunk.chunkHash();
    }

    public long physicalPayloadOffset() {
        return Math.addExact(
                Math.multiplyExact(allocation.payloadBlock(), LinuxBlockfsPayloadAllocation.PAYLOAD_BLOCK_SIZE),
                allocation.payloadOffset()
        );
    }

    public LinuxBlockfsExtent toExtent() {
        return new LinuxBlockfsExtent(
                inodeNumber(),
                fileOffset(),
                allocation.payloadBlock(),
                allocation.payloadOffset(),
                allocation.payloadLength(),
                chunkHash().bytes()
        );
    }
}
