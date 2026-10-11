package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.performance.ReadAheadPlan;
import com.fortrow.cxnfs.transport.CordTransferPlan;

import java.util.ArrayList;
import java.util.List;

public record EarlyReadStoragePlan(
        String path,
        ReadAheadPlan readAheadPlan,
        CordTransferPlan cordTransferPlan,
        List<EarlyReadPayloadExtent> payloadExtents,
        List<FilesystemStructureRef> bootstrapStructures,
        List<FilesystemStructureRef> trailingStructures
) {
    public EarlyReadStoragePlan {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (readAheadPlan == null) {
            throw new IllegalArgumentException("readAheadPlan is required");
        }
        if (cordTransferPlan == null) {
            throw new IllegalArgumentException("cordTransferPlan is required");
        }
        payloadExtents = payloadExtents == null ? List.of() : List.copyOf(payloadExtents);
        bootstrapStructures = bootstrapStructures == null ? List.of() : List.copyOf(bootstrapStructures);
        trailingStructures = trailingStructures == null ? List.of() : List.copyOf(trailingStructures);
        if (bootstrapStructures.stream().anyMatch(ref -> !ref.requiredBeforePayload())) {
            throw new IllegalArgumentException("bootstrap structures must be required before payload");
        }
        if (trailingStructures.stream().anyMatch(ref -> !ref.trailing())) {
            throw new IllegalArgumentException("trailing structures must trail payload start");
        }
        boolean payloadMayStart = !payloadExtents.isEmpty()
                && !trailingStructures.isEmpty()
                && payloadExtents.stream().allMatch(EarlyReadPayloadExtent::mayReadBeforeTrailingMetadata);
        if (payloadMayStart && bootstrapStructures.isEmpty()) {
            throw new IllegalArgumentException("early payload requires bootstrap structures");
        }
    }

    public boolean payloadMayStartBeforeTrailingStructures() {
        return !payloadExtents.isEmpty()
                && !trailingStructures.isEmpty()
                && payloadExtents.stream().allMatch(EarlyReadPayloadExtent::mayReadBeforeTrailingMetadata);
    }

    public boolean finalPublishRequiresTrailingStructures() {
        return !payloadExtents.isEmpty()
                && payloadExtents.stream().allMatch(EarlyReadPayloadExtent::publishRequiresTrailingMetadata);
    }

    public long earlyPayloadBytes() {
        long total = 0;
        for (EarlyReadPayloadExtent extent : payloadExtents) {
            total = Math.addExact(total, extent.length());
        }
        return total;
    }

    public List<FilesystemStructureRef> allStructures() {
        List<FilesystemStructureRef> refs = new ArrayList<>(bootstrapStructures.size() + trailingStructures.size());
        refs.addAll(bootstrapStructures);
        refs.addAll(trailingStructures);
        return List.copyOf(refs);
    }
}
