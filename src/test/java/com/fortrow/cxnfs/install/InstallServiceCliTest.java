package com.fortrow.cxnfs.install;

import com.fortrow.cxnfs.Cxnfs;
import com.fortrow.cxnfs.sys.PlatformUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.fortrow.cxnfs.install.InstallServiceCli.ServicePlatform.LINUX;
import static com.fortrow.cxnfs.install.InstallServiceCli.ServicePlatform.MACOS;
import static com.fortrow.cxnfs.install.InstallServiceCli.ServicePlatform.WINDOWS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class InstallServiceCliTest {
    @TempDir
    Path tempDir;

    @Test
    void linuxInstallWritesConfigAndSystemdService() throws Exception {
        InstallServiceCli.ServicePaths paths = paths("linux", LINUX);
        Path datasets = tempDir.resolve("srv/datasets");
        Path outputs = tempDir.resolve("srv/outputs");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new InstallServiceCli(LINUX, paths).run(
                new String[]{
                        "--keystore", tempDir.resolve("server.p12").toString(),
                        "--keystore-pass-file", tempDir.resolve("server.p12.pass").toString(),
                        "--root", "/datasets=" + datasets,
                        "--root", "/outputs=" + outputs
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String config = Files.readString(paths.configPath());
        assertTrue(config.contains("serve.root.0=/datasets=" + datasets.toAbsolutePath().normalize()));
        assertTrue(config.contains("serve.root.1=/outputs=" + outputs.toAbsolutePath().normalize()));
        assertTrue(config.contains("serve.keystore=" + tempDir.resolve("server.p12").toAbsolutePath().normalize()));
        assertTrue(config.contains("serve.keystore-password-file="
                + tempDir.resolve("server.p12.pass").toAbsolutePath().normalize()));
        assertTrue(config.contains("serve.repository-root=" + paths.repositoryRoot()));
        assertTrue(config.contains("auth.user.trainer.password=REPLACE_WITH_LONG_RANDOM_PASSWORD"));
        assertTrue(!config.contains("serve.listen"));

        String service = Files.readString(paths.servicePath());
        assertTrue(service.contains("[Service]"));
        assertTrue(service.contains("ExecStart="));
        assertTrue(service.contains(paths.executablePath() + "\" serve --config"));
        assertTrue(service.contains(datasets.toAbsolutePath().normalize().toString()));
        assertTrue(service.contains(outputs.toAbsolutePath().normalize().toString()));
        assertTrue(out.toString().contains("executable_file=" + paths.executablePath()));
        assertTrue(out.toString().contains("next_command=systemctl daemon-reload && systemctl enable --now cxnfs.service"));
    }

    @Test
    void macInstallWritesLaunchDaemonPlist() throws Exception {
        InstallServiceCli.ServicePaths paths = paths("mac", MACOS);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new InstallServiceCli(MACOS, paths).run(
                new String[]{
                        "--keystore", tempDir.resolve("server.p12").toString(),
                        "--keystore-pass-file", tempDir.resolve("server.p12.pass").toString(),
                        "--root", "/datasets=" + tempDir.resolve("datasets")
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String plist = Files.readString(paths.servicePath());
        assertTrue(plist.contains("<key>Label</key>"));
        assertTrue(plist.contains("<string>com.fortrow.cxnfs</string>"));
        assertTrue(plist.contains("<key>ProgramArguments</key>"));
        assertTrue(plist.contains("<string>" + paths.executablePath() + "</string>"));
        assertTrue(plist.contains("<string>serve</string>"));
        assertTrue(plist.contains("<string>--config</string>"));
        assertTrue(plist.contains(paths.configPath().toString()));
        assertTrue(out.toString().contains("platform=macos"));
        assertTrue(out.toString().contains("launchctl bootstrap system"));
    }

    @Test
    void windowsInstallWritesCommandAndInstallerScripts() throws Exception {
        InstallServiceCli.ServicePaths paths = paths("windows", WINDOWS);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new InstallServiceCli(WINDOWS, paths).run(
                new String[]{
                        "--keystore", tempDir.resolve("server.p12").toString(),
                        "--keystore-pass-file", tempDir.resolve("server.p12.pass").toString(),
                        "--root", "/datasets=" + tempDir.resolve("datasets")
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String command = Files.readString(paths.commandScriptPath());
        String service = Files.readString(paths.servicePath());
        String installer = Files.readString(paths.serviceInstallScriptPath());
        assertTrue(command.contains("call " + quoteWindows(paths.executablePath().toString()) + " serve --config"));
        assertTrue(command.contains(paths.configPath().toString()));
        assertTrue(service.contains("<id>cxnfs</id>"));
        assertTrue(service.contains("<name>CXNFS File Server</name>"));
        assertTrue(service.contains("<executable>cmd.exe</executable>"));
        assertTrue(service.contains(paths.commandScriptPath().toString()));
        assertTrue(installer.contains("Join-Path $env:ProgramFiles \"CXNFS\\cxnfs-service.exe\""));
        assertTrue(installer.contains("WinSW is required for Windows service installation"));
        assertTrue(installer.contains("CXNFS File Server"));
        assertTrue(out.toString().contains("platform=windows"));
        assertTrue(out.toString().contains("service_install_script=" + paths.serviceInstallScriptPath()));
    }

    @Test
    void installPreservesExistingAuthWhenUpdatingConfig() throws Exception {
        InstallServiceCli.ServicePaths paths = paths("preserve", LINUX);
        Files.createDirectories(paths.configPath().getParent());
        Files.writeString(
                paths.configPath(),
                """
                auth.user.alice.password=existing-secret
                auth.user.alice.directories=/datasets
                serve.host=old-host
                custom.value=keep-me
                """
        );

        int exit = new InstallServiceCli(LINUX, paths).run(
                new String[]{
                        "--keystore", tempDir.resolve("server.p12").toString(),
                        "--keystore-pass-file", tempDir.resolve("server.p12.pass").toString(),
                        "--root", "/datasets=" + tempDir.resolve("datasets")
                },
                new PrintStream(new ByteArrayOutputStream()),
                System.err
        );

        assertEquals(0, exit);
        String config = Files.readString(paths.configPath());
        assertTrue(config.contains("serve.host=0.0.0.0"));
        assertTrue(config.contains("auth.user.alice.password=existing-secret"));
        assertTrue(config.contains("auth.user.alice.directories=/datasets"));
        assertTrue(config.contains("custom.value=keep-me"));
        assertTrue(!config.contains("serve.host=old-host"));
        assertTrue(!config.contains("auth.user.trainer.password=REPLACE_WITH_LONG_RANDOM_PASSWORD"));
    }

    @Test
    void installServiceRequiresKeystorePasswordFileAndRoot() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "install-service",
                        "--keystore", tempDir.resolve("server.p12").toString()
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("install-service requires --keystore-pass-file"));
    }

    @Test
    void mapsEveryJfluxLinuxDetectionToLinuxServiceModel() {
        for (PlatformUtils.OpSys os : PlatformUtils.OpSys.values()) {
            if (os == PlatformUtils.OpSys.WINDOWS) {
                assertEquals(WINDOWS, InstallServiceCli.servicePlatformFor(os));
            } else if (os == PlatformUtils.OpSys.MAC_OSX) {
                assertEquals(MACOS, InstallServiceCli.servicePlatformFor(os));
            } else if (os == PlatformUtils.OpSys.UNKNOWN) {
                assertThrows(IllegalArgumentException.class, () -> InstallServiceCli.servicePlatformFor(os));
            } else {
                assertEquals(LINUX, InstallServiceCli.servicePlatformFor(os));
            }
        }
    }

    @Test
    void topLevelHelpListsInstallService() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(new String[]{"help"}, new PrintStream(out), System.err);

        assertEquals(0, exit);
        assertTrue(out.toString().contains("install-service"));
    }

    private InstallServiceCli.ServicePaths paths(String name, InstallServiceCli.ServicePlatform platform) {
        Path base = tempDir.resolve(name);
        Path service = platform == WINDOWS
                ? base.resolve("cxnfs-service.xml")
                : platform == MACOS
                ? base.resolve("com.fortrow.cxnfs.plist")
                : base.resolve("cxnfs.service");
        Path installScript = platform == WINDOWS ? base.resolve("install-cxnfs-service.ps1") : null;
        Path commandScript = platform == WINDOWS ? base.resolve("cxnfs-service.cmd") : null;
        Path executable = platform == WINDOWS ? base.resolve("cxnfs.cmd") : base.resolve("bin/cxnfs");
        return new InstallServiceCli.ServicePaths(
                base.resolve("server.properties"),
                service,
                installScript,
                commandScript,
                executable,
                base.resolve("repository"),
                base.resolve("payload-staging"),
                base.resolve("run/server.ready"),
                base.resolve("run/server.health"),
                base.resolve("run/server.stop"),
                base.resolve("run/server.lock"),
                base.resolve("logs/server.out.log"),
                base.resolve("logs/server.err.log")
        );
    }

    private static String quoteWindows(String value) {
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }
}
