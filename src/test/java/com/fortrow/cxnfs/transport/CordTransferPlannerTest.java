package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CordTransferPlannerTest {
    @Test
    void xetLikeDefaultsStartAdaptiveAndCapAt64Cords() {
        CordPoolConfig config = CordPoolConfig.xetLikeDefaults();

        assertEquals(1, config.initialCords());
        assertEquals(64, config.maxCords());
        assertTrue(config.adaptive());
    }

    @Test
    void rejectsMoreThanAbsoluteMaxCords() {
        assertThrows(IllegalArgumentException.class, () -> new CordPoolConfig(1, CordPoolConfig.ABSOLUTE_MAX_CORDS + 1, true));
    }

    @Test
    void assignsTermsAcrossNoMoreThan64Cords() {
        ReconstructionPlan reconstructionPlan = new ReconstructionPlan(
                hash(1),
                100,
                terms(100)
        );

        CordTransferPlan transferPlan = new CordTransferPlanner(CordPoolConfig.xetLikeDefaults()).plan(reconstructionPlan);

        assertEquals(64, transferPlan.activeCords());
        assertEquals(100, transferPlan.assignments().size());
        assertEquals(0, transferPlan.assignments().getFirst().cordIndex());
        assertEquals(63, transferPlan.assignments().get(63).cordIndex());
        assertEquals(0, transferPlan.assignments().get(64).cordIndex());
    }

    @Test
    void containerTransferBindingsDefaultToTheCordPool() {
        TransportBinding binding = new TransportBinding(
                TransportRole.CONTAINER_TRANSFER,
                java.net.InetSocketAddress.createUnresolved("example.test", 4433),
                TransportSecurityConfig.quicTls13ForUdp(java.nio.file.Path.of("keystore.p12"))
        );

        assertEquals(CordPoolConfig.xetLikeDefaults(), binding.cordPool().orElseThrow());
        assertEquals(QuicHandshakePolicy.fastStartDefaults(), binding.quicHandshakePolicy().orElseThrow());
        assertEquals(FastStartTransferPolicy.defaults(), binding.fastStartTransferPolicy().orElseThrow());
        assertEquals(WanTransferSecurityPolicy.defaults(), binding.wanTransferSecurityPolicy().orElseThrow());
        assertEquals(WriteAdmissionPolicy.defaults(), binding.writeAdmissionPolicy().orElseThrow());
        assertEquals(BulkReliabilityPolicy.defaults(), binding.bulkReliabilityPolicy().orElseThrow());
    }

    private static List<ReconstructionTerm> terms(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> new ReconstructionTerm(hash(index + 2), 0, 1))
                .toList();
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
