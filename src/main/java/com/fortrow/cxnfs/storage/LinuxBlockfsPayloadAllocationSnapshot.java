package com.fortrow.cxnfs.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public record LinuxBlockfsPayloadAllocationSnapshot(
        long generation,
        List<LinuxBlockfsPayloadAllocation> allocations
) {
    public LinuxBlockfsPayloadAllocationSnapshot {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        allocations = allocations == null ? List.of() : List.copyOf(allocations);
        validateNonOverlapping(allocations);
        validatePhysicalNonOverlapping(allocations);
    }

    private static void validateNonOverlapping(List<LinuxBlockfsPayloadAllocation> allocations) {
        List<LinuxBlockfsPayloadAllocation> sorted = new ArrayList<>(allocations);
        sorted.sort(Comparator
                .comparingLong(LinuxBlockfsPayloadAllocation::inodeNumber)
                .thenComparingLong(LinuxBlockfsPayloadAllocation::fileOffset));
        LinuxBlockfsPayloadAllocation previous = null;
        for (LinuxBlockfsPayloadAllocation current : sorted) {
            if (previous != null && previous.inodeNumber() == current.inodeNumber()) {
                long previousEnd = Math.addExact(previous.fileOffset(), previous.payloadLength());
                if (current.fileOffset() < previousEnd) {
                    throw new IllegalArgumentException(
                            "overlapping payload allocations for inode: " + current.inodeNumber()
                    );
                }
            }
            previous = current;
        }
    }

    private static void validatePhysicalNonOverlapping(List<LinuxBlockfsPayloadAllocation> allocations) {
        List<LinuxBlockfsPayloadAllocation> sorted = new ArrayList<>(allocations);
        sorted.sort(Comparator
                .comparingLong(LinuxBlockfsPayloadAllocationSnapshot::physicalStart)
                .thenComparingLong(LinuxBlockfsPayloadAllocationSnapshot::physicalEnd));
        LinuxBlockfsPayloadAllocation previous = null;
        for (LinuxBlockfsPayloadAllocation current : sorted) {
            if (previous != null && physicalStart(current) < physicalEnd(previous)) {
                throw new IllegalArgumentException("overlapping physical payload allocations");
            }
            previous = current;
        }
    }

    private static long physicalStart(LinuxBlockfsPayloadAllocation allocation) {
        return Math.addExact(
                Math.multiplyExact(allocation.payloadBlock(), LinuxBlockfsPayloadAllocation.PAYLOAD_BLOCK_SIZE),
                allocation.payloadOffset()
        );
    }

    private static long physicalEnd(LinuxBlockfsPayloadAllocation allocation) {
        return Math.addExact(physicalStart(allocation), allocation.payloadLength());
    }
}
