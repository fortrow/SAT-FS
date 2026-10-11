package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FastStartTransferPolicyTest {
    @Test
    void defaultsDeferFullMetadataButStageEarlyData() {
        FastStartTransferPolicy policy = FastStartTransferPolicy.defaults();

        assertTrue(policy.enabled());
        assertTrue(policy.sendBootstrapMetadataFirst());
        assertTrue(policy.deferFullMetadataOnControlLane());
        assertTrue(policy.stageEarlyDataUntilVerified());
        assertTrue(policy.finalCommitRequiredForWrites());
        assertTrue(policy.overlapEveryFileChange());
        assertTrue(policy.allowChunkMetadataAfterPayload());
    }

    @Test
    void rejectsFastStartWithoutStaging() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new FastStartTransferPolicy(true, true, true, false, true, true, true)
        );
    }

    @Test
    void rejectsFastStartThatOnlyOptimizesTransferStart() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new FastStartTransferPolicy(true, true, true, true, true, false, true)
        );
    }

    @Test
    void rejectsFastStartThatRequiresChunkMetadataBeforePayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new FastStartTransferPolicy(true, true, true, true, true, true, false)
        );
    }
}
