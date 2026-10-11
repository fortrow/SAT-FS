package com.fortrow.cxnfs.mount;

import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import com.fortrow.cxnfs.sys.PlatformUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public final class MountCli {
    static final int DEFAULT_CONTROL_PORT = 4509;
    static final int DEFAULT_TRANSPORT_PORT = 4510;
    private static final String LINUX_SYSTEMD_SCOPE_ENV = "CXNFS_LINUX_MOUNT_SYSTEMD_SCOPE";
    private static final String FUSE_STAGING_DIR_ENV = "CXNFS_FUSE_STAGING_DIR";
    private static final String FUSE_FALLBACK_STAGING_DIR_ENV = "CXNFS_FUSE_FALLBACK_STAGING_DIR";
    private static final String FUSE_COMMIT_BUFFER_BYTES_ENV = "CXNFS_FUSE_COMMIT_BUFFER_BYTES";
    private static final long MIN_AUTOMATIC_STAGING_BYTES = 1024L * 1024L * 1024L;
    private static final Set<String> SKIPPED_LINUX_STAGING_FILE_SYSTEMS = Set.of(
            "autofs",
            "binfmt_misc",
            "bpf",
            "cgroup",
            "cgroup2",
            "configfs",
            "debugfs",
            "devpts",
            "devtmpfs",
            "efivarfs",
            "fusectl",
            "hugetlbfs",
            "mqueue",
            "nsfs",
            "proc",
            "pstore",
            "securityfs",
            "squashfs",
            "sysfs",
            "tmpfs",
            "tracefs"
    );

    private final PlatformUtils.OpSys platform;
    private final HandoffWriter handoffWriter;
    private final AdapterLauncher adapterLauncher;

    public MountCli() {
        this(null, MountCli::writeTempHandoff, MountCli::launchAdapterProcess);
    }

    MountCli(
            PlatformUtils.OpSys platform,
            HandoffWriter handoffWriter,
            AdapterLauncher adapterLauncher
    ) {
        this.platform = platform;
        this.handoffWriter = handoffWriter;
        this.adapterLauncher = adapterLauncher;
    }

    public int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }
        try {
            MountRequest request = parse(args);
            MountBackend backend = backendFor(platform == null ? PlatformUtils.getOsType(true) : platform);
            MountAdapterHandoff handoff = handoff(request, backend);
            Path handoffPath = handoffWriter.write(handoff);
            MountAdapterLaunchPlan plan = new MountAdapterLaunchPlan(
                    handoffPath,
                    handoff,
                    handoff.adapterBinaryName()
            );
            writePlan(request, handoff, handoffPath, plan, out);
            out.flush();
            int adapterExitCode = adapterLauncher.launch(plan, out, err);
            out.println("adapter_exit_code=" + adapterExitCode);
            return adapterExitCode;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println("mount interrupted");
            return 130;
        } catch (IOException e) {
            err.println("failed to mount: " + e.getMessage());
            return 1;
        }
    }

    private static MountRequest parse(String[] args) {
        List<String> positional = new ArrayList<>();
        Integer controlPort = null;
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if (!arg.startsWith("--")) {
                positional.add(arg);
                continue;
            }
            if (!arg.equals("--port")) {
                throw new IllegalArgumentException("unknown option: " + arg);
            }
            if (index + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for option: --port");
            }
            controlPort = parsePort(args[++index]);
        }
        if (positional.size() != 3) {
            throw new IllegalArgumentException("cxnfs mount requires a host, namespace prefix, and mountpoint");
        }
        return new MountRequest(
                positional.get(0),
                positional.get(1),
                positional.get(2),
                controlPort == null ? DEFAULT_CONTROL_PORT : controlPort
        );
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port <= 0 || port >= 65535) {
                throw new IllegalArgumentException("--port must be between 1 and 65534");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--port must be a number", e);
        }
    }

    private static MountAdapterHandoff handoff(MountRequest request, MountBackend backend) {
        Set<MountCapability> capabilities = EnumSet.of(MountCapability.LIST, MountCapability.READ, MountCapability.WRITE);
        MountStrategy strategy = new MountStrategy(backend, capabilities);
        return new MountAdapterHandoff(
                request.host(),
                request.controlPort(),
                transportPortFor(request.controlPort()),
                backend,
                capabilities,
                strategy,
                request.namespacePrefix(),
                request.mountPoint(),
                CxnFileNetworkClientConfig.defaults(),
                Optional.empty()
        );
    }

    private static int transportPortFor(int controlPort) {
        if (controlPort == DEFAULT_CONTROL_PORT) {
            return DEFAULT_TRANSPORT_PORT;
        }
        return controlPort + 1;
    }

    static MountBackend backendFor(PlatformUtils.OpSys os) {
        if (os == PlatformUtils.OpSys.WINDOWS) {
            return MountBackend.WINDOWS_WINFSP;
        }
        if (os == PlatformUtils.OpSys.MAC_OSX) {
            return MountBackend.MACOS_FUSE_T;
        }
        if (os == PlatformUtils.OpSys.UNKNOWN) {
            throw new IllegalArgumentException("cxnfs mount does not support platform: " + os.name().toLowerCase(Locale.ROOT));
        }
        return MountBackend.LINUX_FUSE;
    }

    private static Path writeTempHandoff(MountAdapterHandoff handoff) throws IOException {
        Path handoffPath = Files.createTempFile("cxnfs-mount-", ".handoff");
        Files.writeString(handoffPath, handoff.toReportText(), StandardCharsets.UTF_8);
        return handoffPath.toAbsolutePath().normalize();
    }

    private static int launchAdapterProcess(
            MountAdapterLaunchPlan plan,
            PrintStream out,
            PrintStream err
    ) throws IOException, InterruptedException {
        if (plan.handoff().strategy().backend() == MountBackend.LINUX_FUSE && !linuxSystemdScopeDisabled()) {
            prepareLinuxStagingDirectory(plan);
        }
        List<String> argv = adapterProcessArgv(plan);
        Process process = new ProcessBuilder(argv)
                .inheritIO()
                .start();
        return process.waitFor();
    }

    static List<String> adapterProcessArgv(MountAdapterLaunchPlan plan) {
        if (plan.handoff().strategy().backend() != MountBackend.LINUX_FUSE || linuxSystemdScopeDisabled()) {
            return plan.argv();
        }
        return linuxSystemdScopeArgv(plan, linuxStagingRoot(plan));
    }

    private static List<String> linuxSystemdScopeArgv(MountAdapterLaunchPlan plan, Path stagingRoot) {
        List<String> argv = new ArrayList<>();
        if (!runningAsRoot()) {
            argv.add("sudo");
        }
        argv.add("systemd-run");
        argv.add("--scope");
        argv.add("--quiet");
        argv.add("--collect");
        argv.add("--unit=" + linuxScopeUnitName(plan));
        argv.add("--description=CXNFS mount "
                + plan.handoff().remoteRoot()
                + " at "
                + plan.handoff().mountPoint());
        linuxScopeRunUser().ifPresent(user -> argv.add("--uid=" + user));
        linuxScopeRunGroup().ifPresent(group -> argv.add("--gid=" + group));
        linuxScopeEnvironment(stagingRoot).forEach((name, value) -> argv.add("--setenv=" + name + "=" + value));
        List<String> adapterArgv = new ArrayList<>(plan.argv());
        adapterArgv.set(0, resolveExecutable(adapterArgv.get(0)));
        argv.addAll(adapterArgv);
        return List.copyOf(argv);
    }

    static Path linuxStagingRoot(MountAdapterLaunchPlan plan) {
        String configured = nonBlankEnv(FUSE_STAGING_DIR_ENV);
        if (configured != null) {
            return Path.of(configured).toAbsolutePath().normalize();
        }
        return bestLinuxNvmeStagingRoot()
                .orElseGet(() -> mountPointParent(plan.handoff().mountPoint()))
                .toAbsolutePath()
                .normalize();
    }

    private static Optional<Path> bestLinuxNvmeStagingRoot() {
        Comparator<LinuxMountCandidate> preference = Comparator.comparingLong(LinuxMountCandidate::usableBytes);
        return linuxMountCandidates().stream()
                .filter(LinuxMountCandidate::nonRotational)
                .filter(candidate -> candidate.usableBytes() >= MIN_AUTOMATIC_STAGING_BYTES)
                .max(preference)
                .map(LinuxMountCandidate::mountPoint);
    }

    private static List<LinuxMountCandidate> linuxMountCandidates() {
        Path mountInfo = Path.of("/proc/self/mountinfo");
        if (!Files.isRegularFile(mountInfo)) {
            return List.of();
        }
        List<LinuxMountCandidate> candidates = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(mountInfo, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                parseLinuxMountCandidate(line).ifPresent(candidates::add);
            }
        } catch (IOException ignored) {
            return List.of();
        }
        return List.copyOf(candidates);
    }

    private static Optional<LinuxMountCandidate> parseLinuxMountCandidate(String line) {
        String[] parts = line.split(" - ", 2);
        if (parts.length != 2) {
            return Optional.empty();
        }
        String[] mountFields = parts[0].split(" ");
        String[] filesystemFields = parts[1].split(" ");
        if (mountFields.length < 5 || filesystemFields.length < 1) {
            return Optional.empty();
        }
        String filesystemType = filesystemFields[0];
        if (SKIPPED_LINUX_STAGING_FILE_SYSTEMS.contains(filesystemType)
                || filesystemType.startsWith("fuse.")) {
            return Optional.empty();
        }
        Path mountPoint = Path.of(decodeMountInfoPath(mountFields[4]));
        if (!Files.isDirectory(mountPoint)) {
            return Optional.empty();
        }
        try {
            FileStore fileStore = Files.getFileStore(mountPoint);
            long usableBytes = fileStore.getUsableSpace();
            return Optional.of(new LinuxMountCandidate(
                    mountPoint,
                    usableBytes,
                    linuxDeviceIsNonRotational(mountFields[2])
            ));
        } catch (IOException ignored) {
            return Optional.empty();
        }
    }

    private static String decodeMountInfoPath(String value) {
        return value
                .replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\012", "\n")
                .replace("\\134", "\\");
    }

    private static boolean linuxDeviceIsNonRotational(String majorMinor) {
        Path sysBlock = Path.of("/sys/dev/block", majorMinor);
        if (!Files.exists(sysBlock)) {
            return false;
        }
        try {
            Path realBlock = sysBlock.toRealPath();
            Optional<Boolean> direct = readLinuxRotationalFlag(realBlock.resolve("queue/rotational"));
            if (direct.isPresent()) {
                return !direct.get();
            }
            Optional<Boolean> slave = readLinuxSlaveRotationalFlag(realBlock.resolve("slaves"));
            if (slave.isPresent()) {
                return !slave.get();
            }
            Path current = realBlock;
            while (current != null) {
                Optional<Boolean> parent = readLinuxRotationalFlag(current.resolve("queue/rotational"));
                if (parent.isPresent()) {
                    return !parent.get();
                }
                current = current.getParent();
            }
            return realBlock.toString().contains("/nvme");
        } catch (IOException ignored) {
            return false;
        }
    }

    private static Optional<Boolean> readLinuxSlaveRotationalFlag(Path slavesDirectory) {
        if (!Files.isDirectory(slavesDirectory)) {
            return Optional.empty();
        }
        boolean sawRotational = false;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(slavesDirectory)) {
            for (Path slave : stream) {
                Optional<Boolean> rotational = readLinuxRotationalFlag(slave.resolve("queue/rotational"));
                if (rotational.isPresent()) {
                    if (!rotational.get()) {
                        return Optional.of(false);
                    }
                    sawRotational = true;
                }
            }
        } catch (IOException ignored) {
            return Optional.empty();
        }
        return sawRotational ? Optional.of(true) : Optional.empty();
    }

    private static Optional<Boolean> readLinuxRotationalFlag(Path rotationalPath) {
        if (!Files.isRegularFile(rotationalPath)) {
            return Optional.empty();
        }
        try {
            String value = Files.readString(rotationalPath, StandardCharsets.UTF_8).trim();
            if (value.equals("0")) {
                return Optional.of(false);
            }
            if (value.equals("1")) {
                return Optional.of(true);
            }
        } catch (IOException ignored) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private static Path mountPointParent(String mountPoint) {
        Path path = Path.of(mountPoint).toAbsolutePath().normalize();
        Path parent = path.getParent();
        return parent == null ? path : parent;
    }

    private static void prepareLinuxStagingDirectory(MountAdapterLaunchPlan plan) throws IOException, InterruptedException {
        Path stagingRoot = linuxStagingRoot(plan);
        Path stagingDirectory = stagingRoot.resolve(".tmp").toAbsolutePath().normalize();
        Optional<String> ownerSpec = linuxScopeOwnerSpec();
        try {
            Files.createDirectories(stagingDirectory);
        } catch (IOException ignored) {
            // Retry below through sudo so system-wide NVMe staging roots can be prepared on demand.
        }
        if (Files.isDirectory(stagingDirectory) && Files.isWritable(stagingDirectory)) {
            if (ownerSpec.isPresent()) {
                runChecked(sudoPrefixed(List.of("chown", ownerSpec.orElseThrow(), stagingDirectory.toString())));
            }
            runChecked(sudoPrefixed(List.of("chmod", "u+rwx", stagingDirectory.toString())));
            return;
        }
        runChecked(sudoPrefixed(List.of("mkdir", "-p", stagingDirectory.toString())));
        if (ownerSpec.isPresent()) {
            runChecked(sudoPrefixed(List.of("chown", ownerSpec.orElseThrow(), stagingDirectory.toString())));
        }
        runChecked(sudoPrefixed(List.of("chmod", "u+rwx", stagingDirectory.toString())));
        if (!Files.isDirectory(stagingDirectory) || !Files.isWritable(stagingDirectory)) {
            throw new IOException("staging directory is not writable: " + stagingDirectory);
        }
    }

    private static Optional<String> linuxScopeOwnerSpec() {
        String uid = nonBlankEnv("SUDO_UID");
        String gid = nonBlankEnv("SUDO_GID");
        if (uid != null && gid != null) {
            return Optional.of(uid + ":" + gid);
        }
        if (uid != null) {
            return Optional.of(uid);
        }
        if (!runningAsRoot()) {
            return Optional.of(System.getProperty("user.name"));
        }
        return Optional.empty();
    }

    private static List<String> sudoPrefixed(List<String> command) {
        if (runningAsRoot()) {
            return command;
        }
        List<String> argv = new ArrayList<>();
        argv.add("sudo");
        argv.addAll(command);
        return List.copyOf(argv);
    }

    private static void runChecked(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .inheritIO()
                .start();
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("command failed with exit " + exit + ": " + String.join(" ", command));
        }
    }

    private static String linuxScopeUnitName(MountAdapterLaunchPlan plan) {
        String seed = plan.handoff().host()
                + "|"
                + plan.handoff().remoteRoot()
                + "|"
                + plan.handoff().mountPoint()
                + "|"
                + ProcessHandle.current().pid()
                + "|"
                + System.nanoTime();
        return "cxnfs-mount-" + Integer.toUnsignedString(seed.hashCode(), 16);
    }

    private static boolean linuxSystemdScopeDisabled() {
        String value = System.getenv(LINUX_SYSTEMD_SCOPE_ENV);
        return value != null
                && (value.equalsIgnoreCase("false")
                || value.equalsIgnoreCase("off")
                || value.equalsIgnoreCase("no")
                || value.equals("0"));
    }

    private static boolean runningAsRoot() {
        return "root".equals(System.getProperty("user.name"));
    }

    private static Optional<String> linuxScopeRunUser() {
        String sudoUid = nonBlankEnv("SUDO_UID");
        if (sudoUid != null) {
            return Optional.of(sudoUid);
        }
        if (!runningAsRoot()) {
            return Optional.of(System.getProperty("user.name"));
        }
        return Optional.empty();
    }

    private static Optional<String> linuxScopeRunGroup() {
        return Optional.ofNullable(nonBlankEnv("SUDO_GID"));
    }

    private static LinkedHashMap<String, String> linuxScopeEnvironment(Path stagingRoot) {
        LinkedHashMap<String, String> environment = new LinkedHashMap<>();
        putNonBlankEnvironment(environment, "PATH");
        environment.put(FUSE_STAGING_DIR_ENV, stagingRoot.toString());
        putNonBlankEnvironment(environment, FUSE_FALLBACK_STAGING_DIR_ENV);
        putNonBlankEnvironment(environment, FUSE_COMMIT_BUFFER_BYTES_ENV);
        return environment;
    }

    private static void putNonBlankEnvironment(LinkedHashMap<String, String> environment, String name) {
        String value = nonBlankEnv(name);
        if (value != null) {
            environment.put(name, value);
        }
    }

    private static String nonBlankEnv(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value;
    }

    private static String resolveExecutable(String executable) {
        if (executable.contains("/") || executable.isBlank()) {
            return executable;
        }
        String path = System.getenv("PATH");
        if (path == null || path.isBlank()) {
            return executable;
        }
        for (String directory : path.split(File.pathSeparator)) {
            if (directory.isBlank()) {
                continue;
            }
            Path candidate = Path.of(directory, executable);
            if (Files.isExecutable(candidate)) {
                return candidate.toAbsolutePath().normalize().toString();
            }
        }
        return executable;
    }

    private static void writePlan(
            MountRequest request,
            MountAdapterHandoff handoff,
            Path handoffPath,
            MountAdapterLaunchPlan plan,
            PrintStream out
    ) {
        out.println("cxnfs mount");
        out.println("host=" + request.host());
        out.println("control_port=" + handoff.controlPort());
        out.println("transport_port=" + handoff.transportPort());
        out.println("namespace_prefix=" + handoff.remoteRoot());
        out.println("remote_root=" + handoff.remoteRoot());
        out.println("mount_point=" + handoff.mountPoint());
        out.println("backend=" + handoff.strategy().backend().name().toLowerCase(Locale.ROOT));
        out.println("capabilities=" + capabilities(handoff.strategy().capabilities()));
        out.println("handoff_report=" + handoffPath.toAbsolutePath().normalize());
        out.println("adapter_binary=" + plan.adapterBinary());
        if (handoff.strategy().backend() == MountBackend.LINUX_FUSE && !linuxSystemdScopeDisabled()) {
            out.println("launch_scope=systemd");
            out.println("staging_root=" + linuxStagingRoot(plan));
            out.println("staging_directory=" + linuxStagingRoot(plan).resolve(".tmp").toAbsolutePath().normalize());
        }
    }

    private static String capabilities(Set<MountCapability> capabilities) {
        return capabilities.stream()
                .sorted(java.util.Comparator.comparing(Enum::ordinal))
                .map(capability -> capability.name().toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.joining(","));
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs mount <host-ip-address> <namespace-prefix> <mountpoint> [--port port]");
    }

    @FunctionalInterface
    interface HandoffWriter {
        Path write(MountAdapterHandoff handoff) throws IOException;
    }

    @FunctionalInterface
    interface AdapterLauncher {
        int launch(MountAdapterLaunchPlan plan, PrintStream out, PrintStream err) throws IOException, InterruptedException;
    }

    private record MountRequest(String host, String namespacePrefix, String mountPoint, int controlPort) {
        private MountRequest {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host is required");
            }
            if (namespacePrefix == null || namespacePrefix.isBlank()) {
                throw new IllegalArgumentException("namespace prefix is required");
            }
            if (mountPoint == null || mountPoint.isBlank()) {
                throw new IllegalArgumentException("mountpoint is required");
            }
        }
    }

    private record LinuxMountCandidate(Path mountPoint, long usableBytes, boolean nonRotational) {
    }
}
