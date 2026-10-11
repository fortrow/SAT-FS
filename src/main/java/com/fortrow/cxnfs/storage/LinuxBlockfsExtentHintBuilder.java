package com.fortrow.cxnfs.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class LinuxBlockfsExtentHintBuilder {
    public LinuxBlockfsExtentHintsSnapshot build(
            LinuxBlockfsManifestIndexSnapshot manifestIndex,
            LinuxBlockfsPayloadAllocationSnapshot allocations,
            boolean requireComplete
    ) {
        if (manifestIndex == null) {
            throw new IllegalArgumentException("manifestIndex is required");
        }
        if (allocations == null) {
            throw new IllegalArgumentException("allocations is required");
        }
        if (manifestIndex.generation() != allocations.generation()) {
            throw new IllegalStateException("payload allocation generation "
                    + allocations.generation()
                    + " does not match manifest index generation "
                    + manifestIndex.generation());
        }

        Map<ChunkKey, LinuxBlockfsManifestChunk> chunks = chunksByKey(manifestIndex);
        List<LinuxBlockfsExtent> extents = new ArrayList<>(allocations.allocations().size());
        for (LinuxBlockfsPayloadAllocation allocation : allocations.allocations()) {
            ChunkKey key = new ChunkKey(
                    allocation.inodeNumber(),
                    allocation.fileOffset(),
                    allocation.payloadLength()
            );
            LinuxBlockfsManifestChunk chunk = chunks.get(key);
            if (chunk == null) {
                throw new IllegalStateException("payload allocation does not match a manifest chunk");
            }
            extents.add(new LinuxBlockfsExtent(
                    allocation.inodeNumber(),
                    allocation.fileOffset(),
                    allocation.payloadBlock(),
                    allocation.payloadOffset(),
                    allocation.payloadLength(),
                    chunk.chunkHash().bytes()
            ));
        }

        if (requireComplete && extents.size() != chunks.size()) {
            throw new IllegalStateException("payload allocations do not cover every manifest chunk");
        }
        extents.sort(Comparator
                .comparingLong(LinuxBlockfsExtent::inodeNumber)
                .thenComparingLong(LinuxBlockfsExtent::fileOffset));
        return new LinuxBlockfsExtentHintsSnapshot(manifestIndex.generation(), extents);
    }

    public LinuxBlockfsExtentHintBuildResult describe(
            LinuxBlockfsManifestIndexSnapshot manifestIndex,
            LinuxBlockfsPayloadAllocationSnapshot allocations,
            LinuxBlockfsExtentHintsSnapshot extentHints
    ) {
        if (manifestIndex == null) {
            throw new IllegalArgumentException("manifestIndex is required");
        }
        if (allocations == null) {
            throw new IllegalArgumentException("allocations is required");
        }
        if (extentHints == null) {
            throw new IllegalArgumentException("extentHints is required");
        }
        int manifestChunks = manifestIndex.chunkCount();
        return new LinuxBlockfsExtentHintBuildResult(
                manifestIndex.generation(),
                manifestIndex.entries().size(),
                manifestChunks,
                allocations.allocations().size(),
                extentHints.extents().size(),
                extentHints.extents().size() == manifestChunks
        );
    }

    private static Map<ChunkKey, LinuxBlockfsManifestChunk> chunksByKey(
            LinuxBlockfsManifestIndexSnapshot manifestIndex
    ) {
        Map<ChunkKey, LinuxBlockfsManifestChunk> chunks = new HashMap<>();
        for (LinuxBlockfsManifestEntry entry : manifestIndex.entries()) {
            for (LinuxBlockfsManifestChunk chunk : entry.chunks()) {
                chunks.put(new ChunkKey(entry.inodeNumber(), chunk.fileOffset(), chunk.length()), chunk);
            }
        }
        return chunks;
    }

    private record ChunkKey(long inodeNumber, long fileOffset, int length) {
    }
}
