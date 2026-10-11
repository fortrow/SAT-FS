package com.fortrow.cxnfs.reconstruction;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.List;

public record ReconstructionPlan(
        ContentHash fileHash,
        long fileSize,
        List<ReconstructionTerm> terms
) {
    public ReconstructionPlan {
        if (fileSize < 0) {
            throw new IllegalArgumentException("fileSize must be non-negative");
        }
        terms = List.copyOf(terms);
    }
}
