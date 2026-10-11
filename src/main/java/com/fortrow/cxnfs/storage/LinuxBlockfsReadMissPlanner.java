package com.fortrow.cxnfs.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class LinuxBlockfsReadMissPlanner {
    private final List<LinuxBlockfsManifestEntry> entries;
    private final Map<ChunkKey, LinuxBlockfsPayloadAllocation> allocations;

    public LinuxBlockfsReadMissPlanner(
            LinuxBlockfsManifestIndexSnapshot manifestIndex,
            LinuxBlockfsPayloadAllocationSnapshot allocationSnapshot
    ) {
        if (manifestIndex == null) {
            throw new IllegalArgumentException("manifestIndex is required");
        }
        if (allocationSnapshot == null) {
            throw new IllegalArgumentException("allocationSnapshot is required");
        }
        if (manifestIndex.generation() != allocationSnapshot.generation()) {
            throw new IllegalStateException("payload allocation generation "
                    + allocationSnapshot.generation()
                    + " does not match manifest index generation "
                    + manifestIndex.generation());
        }
        this.entries = manifestIndex.entries().stream()
                .sorted(Comparator.comparingLong(LinuxBlockfsManifestEntry::inodeNumber))
                .toList();
        this.allocations = allocationsByChunk(allocationSnapshot);
    }

    public LinuxBlockfsReadMissPlan plan(LinuxBlockfsMissEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event is required");
        }
        long missEnd = Math.addExact(event.offset(), event.length());
        List<LinuxBlockfsReadMissChunk> chunks = new ArrayList<>();
        for (LinuxBlockfsManifestEntry entry : entries) {
            if (entry.inodeNumber() != event.inodeNumber()) {
                continue;
            }
            for (LinuxBlockfsManifestChunk chunk : entry.chunks()) {
                long chunkEnd = Math.addExact(chunk.fileOffset(), chunk.length());
                if (!overlaps(event.offset(), missEnd, chunk.fileOffset(), chunkEnd)) {
                    continue;
                }
                LinuxBlockfsPayloadAllocation allocation = allocations.get(
                        new ChunkKey(entry.inodeNumber(), chunk.fileOffset(), chunk.length())
                );
                if (allocation == null) {
                    throw new IllegalStateException("missing payload allocation for inode "
                            + entry.inodeNumber()
                            + " offset "
                            + chunk.fileOffset());
                }
                chunks.add(new LinuxBlockfsReadMissChunk(entry, chunk, allocation));
            }
        }
        chunks.sort(Comparator
                .comparingLong(LinuxBlockfsReadMissChunk::inodeNumber)
                .thenComparingLong(LinuxBlockfsReadMissChunk::fileOffset));
        return new LinuxBlockfsReadMissPlan(event, chunks);
    }

    private static boolean overlaps(long leftStart, long leftEnd, long rightStart, long rightEnd) {
        return leftStart < rightEnd && rightStart < leftEnd;
    }

    private static Map<ChunkKey, LinuxBlockfsPayloadAllocation> allocationsByChunk(
            LinuxBlockfsPayloadAllocationSnapshot snapshot
    ) {
        Map<ChunkKey, LinuxBlockfsPayloadAllocation> result = new HashMap<>();
        for (LinuxBlockfsPayloadAllocation allocation : snapshot.allocations()) {
            result.put(
                    new ChunkKey(allocation.inodeNumber(), allocation.fileOffset(), allocation.payloadLength()),
                    allocation
            );
        }
        return result;
    }

    private record ChunkKey(long inodeNumber, long fileOffset, int length) {
    }
}
