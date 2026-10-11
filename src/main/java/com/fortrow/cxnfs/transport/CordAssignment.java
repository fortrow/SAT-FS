package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;

public record CordAssignment(
        int cordIndex,
        ReconstructionTerm term
) {
    public CordAssignment {
        if (cordIndex < 0) {
            throw new IllegalArgumentException("cordIndex must be non-negative");
        }
        if (term == null) {
            throw new IllegalArgumentException("term is required");
        }
    }
}
