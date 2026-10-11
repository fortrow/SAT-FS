package com.fortrow.cxnfs.mount;

import com.fortrow.cxnfs.sys.PlatformUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MountCliTest {
    @TempDir
    Path tempDir;

    @Test
    void mountsNamespacePrefixThroughMacosAdapterWithDefaultPort() {
        AtomicReference<MountAdapterHandoff> handoffRef = new AtomicReference<>();
        AtomicReference<MountAdapterLaunchPlan> launchPlanRef = new AtomicReference<>();
        Path handoffPath = tempDir.resolve("cxnfs-mount.handoff");
        MountCli cli = new MountCli(
                PlatformUtils.OpSys.MAC_OSX,
                handoff -> {
                    handoffRef.set(handoff);
                    Files.writeString(handoffPath, handoff.toReportText());
                    return handoffPath;
                },
                (plan, out, err) -> {
                    launchPlanRef.set(plan);
                    return 0;
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"192.168.101.13", "/datasets", "/Volumes/CXNFS"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        MountAdapterHandoff handoff = handoffRef.get();
        MountAdapterLaunchPlan plan = launchPlanRef.get();
        assertNotNull(handoff);
        assertNotNull(plan);
        assertEquals("192.168.101.13", handoff.host());
        assertEquals(4509, handoff.controlPort());
        assertEquals(4510, handoff.transportPort());
        assertEquals("/datasets", handoff.remoteRoot());
        assertEquals("/Volumes/CXNFS", handoff.mountPoint());
        assertEquals(MountBackend.MACOS_FUSE_T, handoff.strategy().backend());
        assertEquals(Set.of(MountCapability.LIST, MountCapability.READ, MountCapability.WRITE), handoff.strategy().capabilities());
        assertEquals(Set.of(MountCapability.LIST, MountCapability.READ, MountCapability.WRITE), handoff.requestedCapabilities());
        assertEquals("cxnfs-fuse-t", plan.adapterBinary());
        assertEquals("cxnfs-fuse-t", plan.argv().get(0));
        assertEquals("--handoff", plan.argv().get(1));
        assertEquals(handoffPath.toAbsolutePath().normalize().toString(), plan.argv().get(2));
        String output = out.toString();
        assertTrue(output.contains("cxnfs mount"));
        assertTrue(output.contains("namespace_prefix=/datasets"));
        assertTrue(output.contains("remote_root=/datasets"));
        assertTrue(output.contains("mount_point=/Volumes/CXNFS"));
        assertTrue(output.contains("adapter_exit_code=0"));
    }

    @Test
    void acceptsCustomControlPortAndNormalizesNamespacePrefix() {
        AtomicReference<MountAdapterHandoff> handoffRef = new AtomicReference<>();
        MountCli cli = new MountCli(
                PlatformUtils.OpSys.LINUX,
                handoff -> {
                    handoffRef.set(handoff);
                    return tempDir.resolve("cxnfs-linux.handoff");
                },
                (plan, out, err) -> 0
        );

        int exit = cli.run(
                new String[]{"10.0.0.5", "outputs", "/mnt/outputs", "--port", "4512"},
                new PrintStream(new ByteArrayOutputStream()),
                System.err
        );

        assertEquals(0, exit);
        MountAdapterHandoff handoff = handoffRef.get();
        assertNotNull(handoff);
        assertEquals(4512, handoff.controlPort());
        assertEquals(4513, handoff.transportPort());
        assertEquals("/outputs", handoff.remoteRoot());
        assertEquals(MountBackend.LINUX_FUSE, handoff.strategy().backend());
        assertEquals(64, handoff.clientConfig().maxCords());
    }

    @Test
    void linuxAdapterLaunchesInsideSystemdScopeByDefault() {
        AtomicReference<MountAdapterLaunchPlan> launchPlanRef = new AtomicReference<>();
        MountCli cli = new MountCli(
                PlatformUtils.OpSys.LINUX,
                handoff -> tempDir.resolve("cxnfs-linux.handoff"),
                (plan, out, err) -> {
                    launchPlanRef.set(plan);
                    return 0;
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"10.0.0.5", "/test", "/mnt/test"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        MountAdapterLaunchPlan plan = launchPlanRef.get();
        assertNotNull(plan);
        var argv = MountCli.adapterProcessArgv(plan);
        int systemdRunIndex = argv.indexOf("systemd-run");
        assertTrue(systemdRunIndex >= 0);
        if (!"root".equals(System.getProperty("user.name"))) {
            assertEquals("sudo", argv.get(0));
        }
        assertEquals("--scope", argv.get(systemdRunIndex + 1));
        assertTrue(argv.contains("--quiet"));
        assertTrue(argv.contains("--collect"));
        assertTrue(argv.stream().anyMatch(arg -> arg.startsWith("--unit=cxnfs-mount-")));
        assertTrue(argv.stream().anyMatch(arg -> arg.startsWith("--setenv=CXNFS_FUSE_STAGING_DIR=")));
        assertTrue(argv.stream().anyMatch(arg -> arg.endsWith("cxnfs-fuse")));
        assertTrue(argv.contains("--handoff"));
        assertTrue(argv.contains(tempDir.resolve("cxnfs-linux.handoff").toAbsolutePath().normalize().toString()));
        assertTrue(out.toString().contains("staging_directory="));
    }

    @Test
    void rejectsMissingNamespacePrefixOrMountpoint() {
        MountCli cli = new MountCli(
                PlatformUtils.OpSys.LINUX,
                handoff -> tempDir.resolve("unused.handoff"),
                (plan, out, err) -> 0
        );
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"10.0.0.5", "/mnt/cxnfs"},
                System.out,
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("cxnfs mount requires a host, namespace prefix, and mountpoint"));
    }

    @Test
    void mapsJfluxPlatformsToPortableMountBackends() {
        for (PlatformUtils.OpSys os : PlatformUtils.OpSys.values()) {
            if (os == PlatformUtils.OpSys.WINDOWS) {
                assertEquals(MountBackend.WINDOWS_WINFSP, MountCli.backendFor(os));
            } else if (os == PlatformUtils.OpSys.MAC_OSX) {
                assertEquals(MountBackend.MACOS_FUSE_T, MountCli.backendFor(os));
            } else if (os == PlatformUtils.OpSys.UNKNOWN) {
                assertThrows(IllegalArgumentException.class, () -> MountCli.backendFor(os));
            } else {
                assertEquals(MountBackend.LINUX_FUSE, MountCli.backendFor(os));
            }
        }
    }
}
