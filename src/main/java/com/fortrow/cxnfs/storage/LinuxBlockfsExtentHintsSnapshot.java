package com.fortrow.cxnfs.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public record LinuxBlockfsExtentHintsSnapshot(
        long generation,
        List<LinuxBlockfsExtent> extents
) {
    public LinuxBlockfsExtentHintsSnapshot {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        extents = extents == null ? List.of() : List.copyOf(extents);
        validateNonOverlapping(extents);
        validatePhysicalConflicts(extents);
    }

    private static void validateNonOverlapping(List<LinuxBlockfsExtent> extents) {
        List<LinuxBlockfsExtent> sorted = new ArrayList<>(extents);
        sorted.sort(Comparator
                .comparingLong(LinuxBlockfsExtent::inodeNumber)
                .thenComparingLong(LinuxBlockfsExtent::fileOffset));
        LinuxBlockfsExtent previous = null;
        for (LinuxBlockfsExtent current : sorted) {
            if (previous != null && previous.inodeNumber() == current.inodeNumber()) {
                long previousEnd = Math.addExact(previous.fileOffset(), previous.payloadLength());
                if (current.fileOffset() < previousEnd) {
                    throw new IllegalArgumentException(
                            "overlapping extent hints for inode: " + current.inodeNumber()
                    );
                }
            }
            previous = current;
        }
    }

    private static void validatePhysicalConflicts(List<LinuxBlockfsExtent> extents) {
        List<LinuxBlockfsExtent> sorted = new ArrayList<>(extents);
        sorted.sort(Comparator
                .comparingLong(LinuxBlockfsExtentHintsSnapshot::physicalStart)
                .thenComparingLong(LinuxBlockfsExtentHintsSnapshot::physicalEnd));
        LinuxBlockfsExtent previous = null;
        for (LinuxBlockfsExtent current : sorted) {
            if (previous != null && physicalStart(current) < physicalEnd(previous)
                    && !sameVerifiedPhysicalPayload(previous, current)) {
                throw new IllegalArgumentException("conflicting physical extent hints");
            }
            previous = current;
        }
    }

    private static boolean sameVerifiedPhysicalPayload(LinuxBlockfsExtent left, LinuxBlockfsExtent right) {
        return physicalStart(left) == physicalStart(right)
                && physicalEnd(left) == physicalEnd(right)
                && Arrays.equals(left.expectedHash(), right.expectedHash());
    }

    private static long physicalStart(LinuxBlockfsExtent extent) {
        return Math.addExact(
                Math.multiplyExact(extent.payloadBlock(), LinuxBlockfsExtent.PAYLOAD_BLOCK_SIZE),
                extent.payloadOffset()
        );
    }

    private static long physicalEnd(LinuxBlockfsExtent extent) {
        return Math.addExact(physicalStart(extent), extent.payloadLength());
    }
}
