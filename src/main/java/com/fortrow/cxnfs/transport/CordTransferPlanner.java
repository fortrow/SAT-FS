package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;

import java.util.ArrayList;
import java.util.List;

public final class CordTransferPlanner {
    private final CordPoolConfig cordPool;

    public CordTransferPlanner(CordPoolConfig cordPool) {
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        this.cordPool = cordPool;
    }

    public CordTransferPlan plan(ReconstructionPlan reconstructionPlan) {
        if (reconstructionPlan == null) {
            throw new IllegalArgumentException("reconstructionPlan is required");
        }
        int activeCords = Math.min(cordPool.maxCords(), reconstructionPlan.terms().size());
        List<CordAssignment> assignments = new ArrayList<>(reconstructionPlan.terms().size());
        for (int i = 0; i < reconstructionPlan.terms().size(); i++) {
            ReconstructionTerm term = reconstructionPlan.terms().get(i);
            assignments.add(new CordAssignment(i % activeCords, term));
        }
        return new CordTransferPlan(cordPool, activeCords, assignments);
    }
}
