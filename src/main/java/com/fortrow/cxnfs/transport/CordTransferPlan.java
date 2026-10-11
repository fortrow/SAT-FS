package com.fortrow.cxnfs.transport;

import java.util.List;

public record CordTransferPlan(
        CordPoolConfig cordPool,
        int activeCords,
        List<CordAssignment> assignments
) {
    public CordTransferPlan {
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        if (activeCords < 0 || activeCords > cordPool.maxCords()) {
            throw new IllegalArgumentException("activeCords must be between 0 and maxCords");
        }
        assignments = assignments == null ? List.of() : List.copyOf(assignments);
    }
}
