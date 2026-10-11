package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class CordSchedulerTest {
    private static final TransferBackpressurePolicy POLICY = new TransferBackpressurePolicy(
            100,
            1000,
            100,
            100
    );

    @Test
    void increasesOneCordAtATimeWhenSignalsAreHealthy() {
        CordScheduler scheduler = new CordScheduler(CordPoolConfig.xetLikeDefaults(), POLICY);

        CordSchedulerDecision decision = scheduler.evaluate(new TransferBackpressureSignals(
                4,
                10,
                100,
                500,
                10
        ));

        assertEquals(BackpressureDecision.INCREASE, decision.decision());
        assertEquals(5, decision.recommendedCords());
    }

    @Test
    void decreasesQuicklyWhenDiskQueueIsTooDeep() {
        CordScheduler scheduler = new CordScheduler(CordPoolConfig.xetLikeDefaults(), POLICY);

        CordSchedulerDecision decision = scheduler.evaluate(new TransferBackpressureSignals(
                32,
                120,
                100,
                500,
                10
        ));

        assertEquals(BackpressureDecision.DECREASE, decision.decision());
        assertEquals(16, decision.recommendedCords());
    }

    @Test
    void decreasesWhenBytesInFlightAreTooHigh() {
        CordScheduler scheduler = new CordScheduler(CordPoolConfig.xetLikeDefaults(), POLICY);

        CordSchedulerDecision decision = scheduler.evaluate(new TransferBackpressureSignals(
                16,
                10,
                1200,
                500,
                10
        ));

        assertEquals(BackpressureDecision.DECREASE, decision.decision());
        assertEquals(8, decision.recommendedCords());
    }

    @Test
    void decreasesWhenDiskLatencyIsTooHigh() {
        CordScheduler scheduler = new CordScheduler(CordPoolConfig.xetLikeDefaults(), POLICY);

        CordSchedulerDecision decision = scheduler.evaluate(new TransferBackpressureSignals(
                8,
                10,
                100,
                500,
                120
        ));

        assertEquals(BackpressureDecision.DECREASE, decision.decision());
        assertEquals(4, decision.recommendedCords());
    }

    @Test
    void fixedPoolsHoldTheirConfiguredCordCount() {
        CordScheduler scheduler = new CordScheduler(CordPoolConfig.fixed(7), POLICY);

        CordSchedulerDecision decision = scheduler.evaluate(new TransferBackpressureSignals(
                7,
                1,
                1,
                500,
                1
        ));

        assertEquals(BackpressureDecision.HOLD, decision.decision());
        assertEquals(7, decision.recommendedCords());
    }

    @Test
    void neverRecommendsBelowInitialCordCount() {
        CordScheduler scheduler = new CordScheduler(new CordPoolConfig(4, 64, true), POLICY);

        CordSchedulerDecision decision = scheduler.evaluate(new TransferBackpressureSignals(
                4,
                120,
                100,
                500,
                10
        ));

        assertEquals(BackpressureDecision.DECREASE, decision.decision());
        assertEquals(4, decision.recommendedCords());
    }
}
