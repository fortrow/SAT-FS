package com.fortrow.cxnfs.mount;

import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MountStrategyTest {
    @Test
    void macFuseTIsThePreferredMacosMountBackend() {
        MountStrategy strategy = MountStrategy.macFuseT();

        assertEquals(MountBackend.MACOS_FUSE_T, strategy.backend());
        assertTrue(strategy.capabilities().containsAll(Set.of(
                MountCapability.LIST,
                MountCapability.READ,
                MountCapability.WRITE
        )));
    }

    @Test
    void listOnlyFallbackOnlyAdvertisesListing() {
        MountStrategy strategy = MountStrategy.listOnlyFallback();

        assertEquals(MountBackend.CLI_LIST_ONLY, strategy.backend());
        assertEquals(Set.of(MountCapability.LIST), strategy.capabilities());
    }

    @Test
    void portableUserSpaceDefaultsCoverTheSupportedMountTargets() {
        assertEquals(
                Set.of(
                        MountBackend.LINUX_FUSE,
                        MountBackend.MACOS_FUSE_T,
                        MountBackend.WINDOWS_WINFSP,
                        MountBackend.CLI_LIST_ONLY
                ),
                MountStrategy.portableUserSpaceDefaults().stream()
                        .map(MountStrategy::backend)
                        .collect(java.util.stream.Collectors.toSet())
        );
    }

    @Test
    void mountAdapterHandoffSerializesStableAdapterInputsWithoutSecrets() {
        MountAdapterHandoff handoff = new MountAdapterHandoff(
                "cxnfs.example.internal",
                4509,
                4510,
                MountBackend.LINUX_FUSE,
                Set.of(MountCapability.LIST, MountCapability.READ),
                MountStrategy.linuxFuse(),
                "/datasets/train",
                "/mnt/cxnfs",
                new CxnFileNetworkClientConfig(8, 64, 64 * 1024 * 1024, 0, 1234, 5678),
                Optional.of(Path.of("/etc/cxnfs/client-trust.p12"))
        );

        String report = handoff.toReportText();

        assertTrue(report.contains("cxnfs.mount.handoff.v1"));
        assertTrue(report.contains("host=cxnfs.example.internal"));
        assertTrue(report.contains("requested_backend=linux_fuse"));
        assertTrue(report.contains("requested_capabilities=list,read"));
        assertTrue(report.contains("backend=linux_fuse"));
        assertTrue(report.contains("capabilities=list,read,write"));
        assertTrue(report.contains("adapter_kind=fuse"));
        assertTrue(report.contains("adapter_binary=cxnfs-fuse"));
        assertTrue(report.contains("remote_root=/datasets/train"));
        assertTrue(report.contains("mount_point=/mnt/cxnfs"));
        assertTrue(report.contains("initial_cords=8"));
        assertTrue(report.contains("max_cords=64"));
        assertTrue(report.contains("truststore_path=/etc/cxnfs/client-trust.p12"));
        assertTrue(report.contains("truststore_password_inline=false"));
        assertTrue(report.contains("auth_password_inline=false"));
    }

    @Test
    void mountAdapterHandoffParsesStableAdapterInputsAndRejectsInlineSecrets() {
        MountAdapterHandoff original = new MountAdapterHandoff(
                "cxnfs.example.internal",
                4509,
                4510,
                MountBackend.MACOS_FUSE_T,
                Set.of(MountCapability.LIST, MountCapability.READ),
                MountStrategy.macFuseT(),
                "/datasets/train",
                "/Volumes/CXNFS",
                new CxnFileNetworkClientConfig(8, 64, 64 * 1024 * 1024, 0, 1234, 5678),
                Optional.empty()
        );

        MountAdapterHandoff parsed = MountAdapterHandoff.parse(original.toReportText());

        assertEquals(original.host(), parsed.host());
        assertEquals(original.controlPort(), parsed.controlPort());
        assertEquals(original.transportPort(), parsed.transportPort());
        assertEquals(original.requestedBackend(), parsed.requestedBackend());
        assertEquals(original.requestedCapabilities(), parsed.requestedCapabilities());
        assertEquals(original.strategy(), parsed.strategy());
        assertEquals(original.remoteRoot(), parsed.remoteRoot());
        assertEquals(original.mountPoint(), parsed.mountPoint());
        assertEquals(original.clientConfig(), parsed.clientConfig());

        assertThrows(
                IllegalArgumentException.class,
                () -> MountAdapterHandoff.parse(original.toReportText().replace(
                        "auth_password_inline=false",
                        "auth_password_inline=true"
                ))
        );
    }
}
