package com.fortrow.cxnfs.install;

import com.fortrow.cxnfs.server.LiveWriteMode;
import com.fortrow.cxnfs.server.ServedRoot;
import com.fortrow.cxnfs.sys.PlatformUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public final class InstallServiceCli {
    private static final String SERVICE_NAME = "cxnfs";
    private static final long DEFAULT_PAYLOAD_STAGING_MEMORY_BYTES = 536_870_912L;

    private final ServicePlatform platform;
    private final ServicePaths paths;

    public InstallServiceCli() {
        this(null, null);
    }

    InstallServiceCli(ServicePlatform platform, ServicePaths paths) {
        this.platform = platform;
        this.paths = paths;
    }

    public int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }
        try {
            InstallRequest request = parse(args);
            ServicePlatform installPlatform = platform == null ? detectPlatform() : platform;
            ServicePaths installPaths = paths == null
                    ? ServicePaths.defaults(installPlatform, request.configPath())
                    : paths.withConfigOverride(request.configPath());
            InstallResult result = install(request, installPlatform, installPaths);
            writeResult(result, out);
            return 0;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println("failed to install service: " + e.getMessage());
            return 1;
        }
    }

    private static InstallResult install(
            InstallRequest request,
            ServicePlatform platform,
            ServicePaths paths
    ) throws IOException {
        createDirectories(paths);
        writeConfig(request, paths);
        switch (platform) {
            case LINUX -> Files.writeString(
                    paths.servicePath(),
                    linuxService(paths, serviceCommand(paths), request.roots()),
                    StandardCharsets.UTF_8
            );
            case MACOS -> Files.writeString(paths.servicePath(), macLaunchDaemon(paths), StandardCharsets.UTF_8);
            case WINDOWS -> {
                Files.writeString(paths.commandScriptPath(), windowsCommandScript(paths), StandardCharsets.UTF_8);
                Files.writeString(paths.servicePath(), windowsServiceXml(paths), StandardCharsets.UTF_8);
                Files.writeString(paths.serviceInstallScriptPath(), windowsInstallScript(paths), StandardCharsets.UTF_8);
            }
        }
        return new InstallResult(platform, paths, request.roots(), nextCommand(platform, paths));
    }

    private static void createDirectories(ServicePaths paths) throws IOException {
        createParent(paths.configPath());
        createParent(paths.servicePath());
        createParent(paths.serviceInstallScriptPath());
        createParent(paths.commandScriptPath());
        createParent(paths.repositoryRoot());
        createParent(paths.payloadSpillDir());
        createParent(paths.readyFile());
        createParent(paths.healthFile());
        createParent(paths.shutdownFile());
        createParent(paths.daemonLockFile());
        createParent(paths.stdoutLog());
        createParent(paths.stderrLog());
    }

    private static void createParent(Path path) throws IOException {
        if (path == null) {
            return;
        }
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static void writeConfig(InstallRequest request, ServicePaths paths) throws IOException {
        Map<String, String> preserved = readPreservedProperties(paths.configPath());
        if (preserved.keySet().stream().noneMatch(name -> name.startsWith("auth."))) {
            preserved.put("auth.mechanisms", "username_password");
            preserved.put("auth.user.trainer.password", "REPLACE_WITH_LONG_RANDOM_PASSWORD");
        }

        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < request.roots().size(); index++) {
            ServedRoot root = request.roots().get(index);
            builder.append("serve.root.").append(index).append('=')
                    .append(root.remotePrefix()).append('=')
                    .append(propertyValue(root.localDirectory().toString())).append(lineSeparator);
        }
        serveProperties(request, paths)
                .forEach((name, value) -> builder.append(name).append('=')
                        .append(propertyValue(value)).append(lineSeparator));
        builder.append(lineSeparator);
        preserved.forEach((name, value) -> builder.append(name).append('=')
                .append(propertyValue(value)).append(lineSeparator));
        Files.writeString(paths.configPath(), builder.toString(), StandardCharsets.UTF_8);
    }

    private static Map<String, String> readPreservedProperties(Path configPath) throws IOException {
        Map<String, String> preserved = new TreeMap<>();
        if (!Files.exists(configPath)) {
            return preserved;
        }
        Properties properties = new Properties();
        try (var input = Files.newInputStream(configPath)) {
            properties.load(input);
        }
        for (String name : properties.stringPropertyNames()) {
            if (!name.startsWith("serve.")) {
                preserved.put(name, properties.getProperty(name));
            }
        }
        return preserved;
    }

    private static Map<String, String> serveProperties(InstallRequest request, ServicePaths paths) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("serve.host", "0.0.0.0");
        properties.put("serve.control-port", "4509");
        properties.put("serve.transport-port", "4510");
        properties.put("serve.bulk-transport", "quic");
        properties.put("serve.initial-cords", "1");
        properties.put("serve.max-cords", "64");
        properties.put("serve.adaptive-cords", "true");
        properties.put("serve.live-write-mode", LiveWriteMode.DEFAULT.configValue());
        properties.put("serve.payload-staging-memory-bytes", Long.toString(DEFAULT_PAYLOAD_STAGING_MEMORY_BYTES));
        properties.put("serve.payload-spill-dir", paths.payloadSpillDir().toString());
        properties.put("serve.repository-root", paths.repositoryRoot().toString());
        properties.put("serve.keystore", request.keystore().toString());
        properties.put("serve.keystore-password-file", request.keystorePasswordFile().toString());
        properties.put("serve.ready-file", paths.readyFile().toString());
        properties.put("serve.health-file", paths.healthFile().toString());
        properties.put("serve.health-interval-ms", "1000");
        properties.put("serve.shutdown-file", paths.shutdownFile().toString());
        properties.put("serve.shutdown-poll-ms", "250");
        properties.put("serve.daemon-lock-file", paths.daemonLockFile().toString());
        return properties;
    }

    private static String linuxService(ServicePaths paths, String command, List<ServedRoot> roots) {
        List<String> readWritePaths = new ArrayList<>();
        readWritePaths.add(quoteSystemd(paths.repositoryRoot()));
        readWritePaths.add(quoteSystemd(paths.payloadSpillDir()));
        readWritePaths.add(quoteSystemd(paths.readyFile().getParent()));
        readWritePaths.add(quoteSystemd(paths.stdoutLog().getParent()));
        roots.stream()
                .map(ServedRoot::localDirectory)
                .map(InstallServiceCli::quoteSystemd)
                .forEach(readWritePaths::add);
        return String.join(System.lineSeparator(),
                "[Unit]",
                "Description=CXNFS file server",
                "Wants=network-online.target",
                "After=network-online.target",
                "",
                "[Service]",
                "Type=simple",
                "ExecStart=" + command,
                "Restart=always",
                "RestartSec=2",
                "NoNewPrivileges=true",
                "PrivateTmp=true",
                "ProtectSystem=strict",
                "ProtectHome=read-only",
                "ReadWritePaths=" + String.join(" ", readWritePaths),
                "",
                "[Install]",
                "WantedBy=multi-user.target",
                ""
        );
    }

    private static String macLaunchDaemon(ServicePaths paths) {
        String[] parts = serviceCommandParts(paths);
        String lineSeparator = System.lineSeparator();
        StringBuilder arguments = new StringBuilder();
        for (String part : parts) {
            arguments.append("        <string>").append(xmlEscape(part)).append("</string>").append(lineSeparator);
        }
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
                <plist version="1.0">
                <dict>
                    <key>Label</key>
                    <string>com.fortrow.cxnfs</string>
                    <key>ProgramArguments</key>
                    <array>
                %s    </array>
                    <key>RunAtLoad</key>
                    <true/>
                    <key>KeepAlive</key>
                    <true/>
                    <key>StandardOutPath</key>
                    <string>%s</string>
                    <key>StandardErrorPath</key>
                    <string>%s</string>
                </dict>
                </plist>
                """.formatted(arguments, xmlEscape(paths.stdoutLog().toString()), xmlEscape(paths.stderrLog().toString()));
    }

    private static String windowsCommandScript(ServicePaths paths) {
        return "@echo off" + System.lineSeparator()
                + "call " + quoteWindows(paths.executablePath().toString())
                + " serve --config " + quoteWindows(paths.configPath().toString())
                + System.lineSeparator();
    }

    private static String windowsServiceXml(ServicePaths paths) {
        String arguments = "/c " + quoteWindows(paths.commandScriptPath().toString());
        return """
                <service>
                  <id>cxnfs</id>
                  <name>CXNFS File Server</name>
                  <description>CXNFS authenticated file server daemon.</description>
                  <executable>%s</executable>
                  <arguments>%s</arguments>
                  <logpath>%s</logpath>
                  <log mode="roll-by-size">
                    <sizeThreshold>10485760</sizeThreshold>
                    <keepFiles>8</keepFiles>
                  </log>
                  <onfailure action="restart" delay="2 sec"/>
                  <resetfailure>1 hour</resetfailure>
                </service>
                """.formatted(
                "cmd.exe",
                xmlEscape(arguments),
                xmlEscape(paths.stdoutLog().getParent().toString())
        );
    }

    private static String windowsInstallScript(ServicePaths paths) {
        String wrapperPath = paths.servicePath().resolveSibling("cxnfs-service.exe").toString();
        return """
                $ErrorActionPreference = "Stop"
                $wrapper = %s
                if (!(Test-Path $wrapper)) {
                    $installedWrapper = Join-Path $env:ProgramFiles "CXNFS\\cxnfs-service.exe"
                    if (Test-Path $installedWrapper) {
                        Copy-Item $installedWrapper $wrapper
                    }
                }
                if (!(Test-Path $wrapper)) {
                    $candidate = Get-Command winsw.exe -ErrorAction SilentlyContinue
                    if ($candidate) {
                        Copy-Item $candidate.Source $wrapper
                    }
                }
                if (!(Test-Path $wrapper)) {
                    throw "WinSW is required for Windows service installation. Place WinSW at $wrapper or put winsw.exe on PATH."
                }
                & $wrapper stop 2>$null | Out-Null
                & $wrapper uninstall 2>$null | Out-Null
                & $wrapper install
                if ($LASTEXITCODE -ne 0) {
                    throw "CXNFS WinSW install failed."
                }
                Write-Host "Installed CXNFS File Server service. Start with: Start-Service cxnfs"
                """.formatted(powerShellSingleQuoted(wrapperPath));
    }

    private static String serviceCommand(ServicePaths paths) {
        return quoteSystemd(paths.executablePath()) + " serve --config " + quoteSystemd(paths.configPath());
    }

    private static String[] serviceCommandParts(ServicePaths paths) {
        return new String[]{
                paths.executablePath().toString(),
                "serve",
                "--config",
                paths.configPath().toString()
        };
    }

    private static String nextCommand(ServicePlatform platform, ServicePaths paths) {
        return switch (platform) {
            case LINUX -> "systemctl daemon-reload && systemctl enable --now " + SERVICE_NAME + ".service";
            case MACOS -> "launchctl bootstrap system " + shellQuote(paths.servicePath().toString())
                    + " && launchctl enable system/com.fortrow.cxnfs"
                    + " && launchctl kickstart -k system/com.fortrow.cxnfs";
            case WINDOWS -> "powershell -ExecutionPolicy Bypass -File " + quoteWindows(paths.serviceInstallScriptPath().toString());
        };
    }

    private static InstallRequest parse(String[] args) {
        Path keystore = null;
        Path keystorePasswordFile = null;
        Path configPath = null;
        List<ServedRoot> roots = new ArrayList<>();
        for (int index = 0; index < args.length; index++) {
            String option = args[index];
            if (!option.startsWith("--")) {
                throw new IllegalArgumentException("expected option but found: " + option);
            }
            if (index + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for option: " + option);
            }
            String value = args[++index];
            switch (option) {
                case "--keystore" -> keystore = Path.of(value).toAbsolutePath().normalize();
                case "--keystore-pass-file" -> keystorePasswordFile = Path.of(value).toAbsolutePath().normalize();
                case "--config" -> configPath = Path.of(value).toAbsolutePath().normalize();
                case "--root" -> roots.add(parseServedRoot(value));
                default -> throw new IllegalArgumentException("unknown install-service option: " + option);
            }
        }
        if (keystore == null) {
            throw new IllegalArgumentException("install-service requires --keystore <path>");
        }
        if (keystorePasswordFile == null) {
            throw new IllegalArgumentException("install-service requires --keystore-pass-file <path>");
        }
        if (roots.isEmpty()) {
            throw new IllegalArgumentException("install-service requires at least one --root");
        }
        return new InstallRequest(keystore, keystorePasswordFile, List.copyOf(roots), configPath);
    }

    private static ServedRoot parseServedRoot(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--root requires a value");
        }
        int separator = value.indexOf('=');
        if (separator < 0) {
            return ServedRoot.root(Path.of(value));
        }
        String remotePrefix = value.substring(0, separator);
        String localDirectory = value.substring(separator + 1);
        if (remotePrefix.isBlank() || localDirectory.isBlank()) {
            throw new IllegalArgumentException("--root mapping must be <remote-prefix>=<local-directory>");
        }
        return new ServedRoot(remotePrefix, Path.of(localDirectory));
    }

    private static ServicePlatform detectPlatform() {
        return servicePlatformFor(PlatformUtils.getOsType(true));
    }

    static ServicePlatform servicePlatformFor(PlatformUtils.OpSys os) {
        if (os == PlatformUtils.OpSys.WINDOWS) {
            return ServicePlatform.WINDOWS;
        }
        if (os == PlatformUtils.OpSys.MAC_OSX) {
            return ServicePlatform.MACOS;
        }
        if (os == PlatformUtils.OpSys.LINUX || os.name().endsWith("_LINUX")) {
            return ServicePlatform.LINUX;
        }
        throw new IllegalArgumentException("unsupported service platform: " + os);
    }

    private static void writeResult(InstallResult result, PrintStream out) {
        out.println("cxnfs service install");
        out.println("platform=" + result.platform().label());
        out.println("executable_file=" + result.paths().executablePath());
        out.println("config_file=" + result.paths().configPath());
        out.println("service_file=" + result.paths().servicePath());
        if (result.paths().commandScriptPath() != null) {
            out.println("service_command_script=" + result.paths().commandScriptPath());
        }
        if (result.paths().serviceInstallScriptPath() != null) {
            out.println("service_install_script=" + result.paths().serviceInstallScriptPath());
        }
        out.println("served_roots=" + result.roots().size());
        for (int index = 0; index < result.roots().size(); index++) {
            ServedRoot root = result.roots().get(index);
            out.println("served_root." + index + ".remote_prefix=" + root.remotePrefix());
            out.println("served_root." + index + ".local_directory=" + root.localDirectory());
        }
        out.println("next_command=" + result.nextCommand());
        out.println("status=installed");
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs install-service --keystore <path> --keystore-pass-file <path> --root <local-dir>|<remote-prefix>=<local-dir> [--root ...] [--config path]");
    }

    private static String quoteSystemd(Path path) {
        return quoteSystemd(path.toString());
    }

    private static String quoteSystemd(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String quoteWindows(String value) {
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private static String powerShellSingleQuoted(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String xmlEscape(String value) {
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    private static String propertyValue(String value) {
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }

    enum ServicePlatform {
        LINUX("linux"),
        MACOS("macos"),
        WINDOWS("windows");

        private final String label;

        ServicePlatform(String label) {
            this.label = label;
        }

        String label() {
            return label;
        }
    }

    record ServicePaths(
            Path configPath,
            Path servicePath,
            Path serviceInstallScriptPath,
            Path commandScriptPath,
            Path executablePath,
            Path repositoryRoot,
            Path payloadSpillDir,
            Path readyFile,
            Path healthFile,
            Path shutdownFile,
            Path daemonLockFile,
            Path stdoutLog,
            Path stderrLog
    ) {
        static ServicePaths defaults(ServicePlatform platform, Path configOverride) {
            return switch (platform) {
                case LINUX -> linux(configOverride);
                case MACOS -> macos(configOverride);
                case WINDOWS -> windows(configOverride);
            };
        }

        ServicePaths withConfigOverride(Path configOverride) {
            return configOverride == null ? this : new ServicePaths(
                    configOverride.toAbsolutePath().normalize(),
                    servicePath,
                    serviceInstallScriptPath,
                    commandScriptPath,
                    executablePath,
                    repositoryRoot,
                    payloadSpillDir,
                    readyFile,
                    healthFile,
                    shutdownFile,
                    daemonLockFile,
                    stdoutLog,
                    stderrLog
            );
        }

        private static ServicePaths linux(Path configOverride) {
            Path config = configOverride == null ? Path.of("/etc/cxnfs/server.properties") : configOverride;
            return new ServicePaths(
                    config.toAbsolutePath().normalize(),
                    Path.of("/etc/systemd/system/cxnfs.service"),
                    null,
                    null,
                    Path.of("/usr/local/bin/cxnfs"),
                    Path.of("/var/lib/cxnfs/repository"),
                    Path.of("/var/cache/cxnfs/payload-staging"),
                    Path.of("/run/cxnfs/server.ready"),
                    Path.of("/run/cxnfs/server.health"),
                    Path.of("/run/cxnfs/server.stop"),
                    Path.of("/run/cxnfs/server.lock"),
                    Path.of("/var/log/cxnfs/server.out.log"),
                    Path.of("/var/log/cxnfs/server.err.log")
            );
        }

        private static ServicePaths macos(Path configOverride) {
            Path config = configOverride == null ? Path.of("/etc/cxnfs/server.properties") : configOverride;
            return new ServicePaths(
                    config.toAbsolutePath().normalize(),
                    Path.of("/Library/LaunchDaemons/com.fortrow.cxnfs.plist"),
                    null,
                    null,
                    Path.of("/usr/local/bin/cxnfs"),
                    Path.of("/var/db/cxnfs/repository"),
                    Path.of("/var/cache/cxnfs/payload-staging"),
                    Path.of("/var/run/cxnfs/server.ready"),
                    Path.of("/var/run/cxnfs/server.health"),
                    Path.of("/var/run/cxnfs/server.stop"),
                    Path.of("/var/run/cxnfs/server.lock"),
                    Path.of("/var/log/cxnfs/server.out.log"),
                    Path.of("/var/log/cxnfs/server.err.log")
            );
        }

        private static ServicePaths windows(Path configOverride) {
            Path base = Path.of(System.getenv().getOrDefault("ProgramData", "C:\\ProgramData"), "CXNFS");
            Path executable = Path.of(System.getenv().getOrDefault("ProgramFiles", "C:\\Program Files"), "CXNFS", "cxnfs.cmd");
            Path config = configOverride == null ? base.resolve("server.properties") : configOverride;
            return new ServicePaths(
                    config.toAbsolutePath().normalize(),
                    base.resolve("cxnfs-service.xml").toAbsolutePath().normalize(),
                    base.resolve("install-cxnfs-service.ps1").toAbsolutePath().normalize(),
                    base.resolve("cxnfs-service.cmd").toAbsolutePath().normalize(),
                    executable.toAbsolutePath().normalize(),
                    base.resolve("repository").toAbsolutePath().normalize(),
                    base.resolve("payload-staging").toAbsolutePath().normalize(),
                    base.resolve("run/server.ready").toAbsolutePath().normalize(),
                    base.resolve("run/server.health").toAbsolutePath().normalize(),
                    base.resolve("run/server.stop").toAbsolutePath().normalize(),
                    base.resolve("run/server.lock").toAbsolutePath().normalize(),
                    base.resolve("logs/server.out.log").toAbsolutePath().normalize(),
                    base.resolve("logs/server.err.log").toAbsolutePath().normalize()
            );
        }
    }

    record InstallRequest(Path keystore, Path keystorePasswordFile, List<ServedRoot> roots, Path configPath) {
    }

    record InstallResult(ServicePlatform platform, ServicePaths paths, List<ServedRoot> roots, String nextCommand) {
    }
}
