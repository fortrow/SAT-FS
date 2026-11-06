package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.auth.AuthenticationConfigReader;
import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.storage.LinuxBlockfsControlCommand;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtentHintBuildResult;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtentHintBuilder;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtentHintStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsMetadataAuditResult;
import com.fortrow.cxnfs.storage.LinuxBlockfsMetadataAuditor;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtentPublisher;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsMountSelector;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceSyncResult;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceSynchronizer;
import com.fortrow.cxnfs.storage.LinuxBlockfsPayloadCacheWriter;
import com.fortrow.cxnfs.storage.LinuxBlockfsPayloadAllocationStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsReadMissPlanner;
import com.fortrow.cxnfs.storage.LinuxBlockfsReadMissResolution;
import com.fortrow.cxnfs.storage.LinuxBlockfsReadMissService;
import com.fortrow.cxnfs.storage.LinuxBlockfsReadMissWorker;
import com.fortrow.cxnfs.storage.LinuxBlockfsReadMissWorkerSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsStoreChunkFetcher;
import com.fortrow.cxnfs.storage.MountedBlockfsStore;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.store.CxnfsStore;
import com.fortrow.cxnfs.store.DiskCxnfsRepository;
import com.fortrow.cxnfs.store.DiskRepositoryAuditResult;
import com.fortrow.cxnfs.store.DiskRepositoryMaintenanceResult;
import com.fortrow.cxnfs.store.DiskRepositoryMaintenancePolicy;
import com.fortrow.cxnfs.transport.PayloadStagingCleaner;
import com.fortrow.cxnfs.transport.PayloadStagingCleanupResult;
import com.fortrow.cxnfs.transport.PayloadStagingConfig;
import com.fortrow.cxnfs.transport.CordPoolConfig;
import com.fortrow.cxnfs.transport.SecureTransportMode;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;

import javax.net.ssl.SSLContext;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class ServerCli {
    private static final int DEFAULT_CONTROL_PORT = 4509;
    private static final int DEFAULT_CONTAINER_PORT = 4510;
    private static final Set<String> SERVE_CLI_OPTIONS = Set.of(
            "--config",
            "--root",
            "--host",
            "--control-port",
            "--transport-port",
            "--listen",
            "--direct-write-access",
            "--direct-write"
    );

    public int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }
        try {
            return switch (args[0]) {
                case "serve" -> serve(args, out);
                case "serve-blockfs", "serve-cxbfs", "serve-satfs" -> serveBlockfs(args, out);
                case "read-blockfs-range", "read-cxbfs-range" -> readBlockfsRange(args, out);
                case "audit-blockfs-metadata", "audit-cxbfs-metadata" -> auditBlockfsMetadata(args, out);
                case "build-blockfs-extent-hints", "build-cxbfs-extent-hints" -> buildBlockfsExtentHints(args, out);
                case "resolve-blockfs-misses", "resolve-cxbfs-misses" -> resolveBlockfsMisses(args, out);
                case "audit-repository" -> auditRepository(args, out);
                case "compact-repository" -> compactRepository(args, out);
                case "clean-payload-staging" -> cleanPayloadStaging(args, out);
                case "check-health" -> checkHealth(args, out);
                case "health-metrics" -> healthMetrics(args, out);
                case "sync-blockfs-namespace", "sync-cxbfs-namespace" -> syncBlockfsNamespace(args, out);
                default -> {
                    err.println("unknown server command: " + args[0]);
                    usage(err);
                    yield 2;
                }
            };
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println("failed to prepare server: " + e.getMessage());
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println("server command interrupted");
            return 1;
        }
    }

    private int serve(String[] args, PrintStream out) throws IOException, InterruptedException {
        ServeArguments serveArguments = parseServeArguments(args);
        Map<String, String> cliOptions = serveArguments.options();
        String configValue = cliOptions.get("--config");
        if (configValue == null) {
            throw new IllegalArgumentException("serve requires --config <path>");
        }
        Properties properties = loadProperties(Path.of(configValue));
        rejectServeListenProperty(properties);
        List<ServedRoot> servedRoots = serveArguments.roots().isEmpty()
                ? servedRootsFromConfig(properties)
                : serveArguments.roots();
        if (servedRoots.isEmpty()) {
            throw new IllegalArgumentException("serve requires at least one --root or serve.root entry");
        }
        Map<String, String> options = new LinkedHashMap<>();
        options.put("--listen", "true");
        options.putAll(serveOptionsFromConfig(properties));
        for (Map.Entry<String, String> entry : cliOptions.entrySet()) {
            if (!entry.getKey().equals("--config")) {
                options.put(entry.getKey(), entry.getValue());
            }
        }
        rejectCommandLinePassword(options);
        rejectListenerFilesWithoutListener(options);
        String host = options.getOrDefault("--host", "0.0.0.0");
        int controlPort = intOption(options, "--control-port", DEFAULT_CONTROL_PORT);
        int containerPort = intOption(options, "--transport-port", DEFAULT_CONTAINER_PORT);
        Path keyStore = Path.of(options.getOrDefault("--keystore", "cxnfs-server.p12"));
        LiveWriteMode liveWriteMode = LiveWriteMode.parse(options.get("--live-write-mode"));
        CordPoolConfig cordPool = cordPoolConfig(options);

        AuthenticationPolicy authenticationPolicy = new AuthenticationConfigReader().read(Path.of(configValue));
        TransportBinding controlBinding = new TransportBinding(
                TransportRole.FILE_SERVER_CONTROL,
                new InetSocketAddress(host, controlPort),
                TransportSecurityConfig.tls13ForTcp(keyStore)
        );
        TransportBinding containerBinding = new TransportBinding(
                TransportRole.CONTAINER_TRANSFER,
                new InetSocketAddress(host, containerPort),
                TransportSecurityConfig.quicTls13ForUdp(keyStore),
                java.util.Optional.of(cordPool)
        );
        CxnFileServerConfig config = new CxnFileServerConfig(
                servedRoots,
                authenticationPolicy,
                controlBinding,
                containerBinding,
                payloadStagingConfig(options),
                liveWriteMode
        );
        try (DaemonLock daemonLock = acquireDaemonLockIfListening(options, config)) {
            CxnfsStore repository = repositoryForServe(options);
            LiveDirectoryRefresher liveDirectoryRefresher = new LiveDirectoryRefresher(
                    servedRoots,
                    GearHashChunker.defaults(),
                    LiveDirectoryRefresher.DEFAULT_REFRESH_INTERVAL,
                    liveWriteMode
            );
            DirectoryLoadResult loadResult = liveDirectoryRefresher.refreshNow();
            CxnFileServer server = new CxnFileServer(config, repository, liveDirectoryRefresher);

            out.println("cxnfs serve plan");
            out.println("directory=" + server.config().servedDirectory());
            out.println("served_roots=" + server.config().servedRoots().size());
            for (int index = 0; index < server.config().servedRoots().size(); index++) {
                ServedRoot root = server.config().servedRoots().get(index);
                out.println("served_root." + index + ".remote_prefix=" + root.remotePrefix());
                out.println("served_root." + index + ".local_directory=" + root.localDirectory());
            }
            String repositoryRoot = options.get("--repository-root");
            out.println("repository_root=" + (repositoryRoot == null
                    ? "memory"
                    : Path.of(repositoryRoot).toAbsolutePath().normalize()));
            out.println("files_loaded=" + loadResult.filesLoaded());
            out.println("bytes_loaded=" + loadResult.bytesLoaded());
            out.println("control_binding=" + binding("tls13-tcp", server.config().controlBinding()));
            out.println("container_transfer_binding=" + binding("quic-tls13-udp", server.config().containerTransferBinding()));
            out.println("cipher_suite=" + server.config().controlBinding().security().cipherSuite().protocolName());
            out.println("bulk_transport=" + bulkTransportOption(options));
            out.println("cord_pool_initial=" + cordPool.initialCords());
            out.println("cord_pool_max=" + cordPool.maxCords());
            out.println("cord_pool_adaptive=" + cordPool.adaptive());
            out.println("live_write_mode=" + server.config().liveWriteMode().configValue());
            boolean directWriteAccess = booleanOption(options, "--direct-write-access", false)
                    || booleanOption(options, "--direct-write", false);
            out.println("direct_write_access=" + (directWriteAccess ? "enabled" : "disabled"));
            writePayloadStagingConfig(out, server.config().payloadStagingConfig());
            out.println("auth=" + mechanisms(authenticationPolicy));
            return maybeStartControlListener(server, options, out, "server facade loaded", null, daemonLock);
        }
    }

    private static CxnfsStore repositoryForServe(Map<String, String> options) throws IOException {
        String repositoryRoot = options.get("--repository-root");
        if (repositoryRoot == null || repositoryRoot.isBlank()) {
            return new CxnfsRepository(GearHashChunker.defaults());
        }
        return new DiskCxnfsRepository(Path.of(repositoryRoot), GearHashChunker.defaults());
    }

    private int serveBlockfs(String[] args, PrintStream out) throws IOException, InterruptedException {
        if (args.length < 2) {
            throw new IllegalArgumentException(args[0] + " requires a mounted CXBFS directory");
        }
        Path mountRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        rejectCommandLinePassword(options);
        rejectListenerFilesWithoutListener(options);
        String metadataRootValue = options.get("--metadata-root");
        if (metadataRootValue == null) {
            throw new IllegalArgumentException(args[0] + " requires --metadata-root <path>");
        }
        String configValue = options.get("--config");
        if (configValue == null) {
            throw new IllegalArgumentException(args[0] + " requires --config <path>");
        }
        String host = options.getOrDefault("--host", "0.0.0.0");
        int controlPort = intOption(options, "--control-port", DEFAULT_CONTROL_PORT);
        int containerPort = intOption(options, "--transport-port", DEFAULT_CONTAINER_PORT);
        Path keyStore = Path.of(options.getOrDefault("--keystore", "cxnfs-server.p12"));

        AuthenticationPolicy authenticationPolicy = new AuthenticationConfigReader().read(Path.of(configValue));
        TransportBinding controlBinding = new TransportBinding(
                TransportRole.FILE_SERVER_CONTROL,
                new InetSocketAddress(host, controlPort),
                TransportSecurityConfig.tls13ForTcp(keyStore)
        );
        TransportBinding containerBinding = new TransportBinding(
                TransportRole.CONTAINER_TRANSFER,
                new InetSocketAddress(host, containerPort),
                TransportSecurityConfig.quicTls13ForUdp(keyStore),
                java.util.Optional.of(cordPoolConfig(options))
        );
        CxnFileServerConfig config = new CxnFileServerConfig(
                mountRoot,
                authenticationPolicy,
                controlBinding,
                containerBinding,
                payloadStagingConfig(options)
        );
        try (DaemonLock daemonLock = acquireDaemonLockIfListening(options, config)) {
            MountedBlockfsStore store = MountedBlockfsStore.load(mountRoot, Path.of(metadataRootValue));
            CxnFileServer server = new CxnFileServer(config, store);
            LinuxBlockfsReadMissWorker missWorker = blockfsReadMissWorker(options, Path.of(metadataRootValue));

            out.println("cxnfs " + args[0] + " plan");
            out.println("project=cx");
            out.println("filesystem=cxbfs");
            out.println("compatibility_command=serve-blockfs");
            out.println("mount_root=" + server.config().servedDirectory());
            out.println("metadata_root=" + metadataRootValue);
            out.println("files_indexed=" + store.fileCount());
            out.println("control_binding=" + binding("tls13-tcp", server.config().controlBinding()));
            out.println("container_transfer_binding=" + binding("quic-tls13-udp", server.config().containerTransferBinding()));
            out.println("cipher_suite=" + server.config().controlBinding().security().cipherSuite().protocolName());
            out.println("bulk_transport=" + bulkTransportOption(options));
            boolean directWriteAccess = booleanOption(options, "--direct-write-access", false)
                    || booleanOption(options, "--direct-write", false);
            out.println("direct_write_access=" + (directWriteAccess ? "enabled" : "disabled"));
            String payloadDevice = options.get("--payload-device");
            if (payloadDevice != null) {
                out.println("payload_device=" + payloadDevice);
            }
            writePayloadStagingConfig(out, server.config().payloadStagingConfig());
            writeBlockfsMissWorkerConfig(out, missWorker);
            out.println("auth=" + mechanisms(authenticationPolicy));
            return maybeStartControlListener(
                    server,
                    options,
                    out,
                    "mounted CXBFS server facade loaded",
                    missWorker,
                    daemonLock
            );
        }
    }

    private int readBlockfsRange(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException(args[0] + " requires a mounted CXBFS directory");
        }
        Path mountRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        String metadataRootValue = requiredOption(options, "--metadata-root");
        String path = requiredOption(options, "--path");
        long offset = longOption(options, "--offset");
        long length = longOption(options, "--length");

        MountedBlockfsStore store = MountedBlockfsStore.load(mountRoot, Path.of(metadataRootValue));
        FileManifest manifest = store.manifest(path)
                .orElseThrow(() -> new IllegalArgumentException("unknown mounted CXBFS path: " + path));
        ReadPlan plan = store.readPlan(manifest.fileHash(), offset, length);
        byte[] data = store.readRange(manifest.fileHash(), offset, length);

        String outputValue = options.get("--output");
        if (outputValue != null) {
            Path output = Path.of(outputValue);
            Path parent = output.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(output, data);
        }

        out.println(labelForCommand(args[0]) + " range read");
        out.println("project=cx");
        out.println("filesystem=cxbfs");
        out.println("compatibility_command=read-blockfs-range");
        out.println("mount_root=" + mountRoot);
        out.println("metadata_root=" + metadataRootValue);
        out.println("path=" + manifest.path());
        out.println("file_hash=" + manifest.fileHash());
        out.println("file_size=" + manifest.size());
        out.println("offset=" + offset);
        out.println("requested_length=" + length);
        out.println("returned_offset=" + plan.requestedRange().offset());
        out.println("returned_bytes=" + data.length);
        out.println("chunks_verified=" + plan.chunks().size());
        if (outputValue != null) {
            out.println("output=" + outputValue);
        } else {
            out.println("preview_hex=" + hexPreview(data, 32));
        }
        return 0;
    }

    private int compactRepository(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("compact-repository requires a repository root");
        }
        Path repositoryRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        boolean dryRun = booleanOption(options, "--dry-run", false);
        DiskRepositoryMaintenancePolicy policy = new DiskRepositoryMaintenancePolicy(
                Duration.ofMillis(longOption(options, "--orphan-container-min-age-ms", 0L)),
                Duration.ofMillis(longOption(options, "--temp-file-min-age-ms", 0L))
        );
        DiskRepositoryMaintenanceResult result = dryRun
                ? DiskCxnfsRepository.inspect(repositoryRoot, policy)
                : DiskCxnfsRepository.compact(repositoryRoot, policy);

        out.println(dryRun ? "repository compaction dry run" : "repository compacted");
        writeMaintenanceResult(out, result);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            Path parent = report.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(report, result.toReportText(), StandardCharsets.UTF_8);
            out.println("report=" + report);
        }
        return 0;
    }

    private int auditRepository(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("audit-repository requires a repository root");
        }
        Path repositoryRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        boolean failOnIssues = booleanOption(options, "--fail-on-issues", false);
        DiskRepositoryAuditResult result = DiskCxnfsRepository.audit(repositoryRoot);

        out.println("repository audit");
        writeAuditResult(out, result);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            Path parent = report.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(report, result.toReportText(), StandardCharsets.UTF_8);
            out.println("report=" + report);
        }
        return failOnIssues && !result.clean() ? 1 : 0;
    }

    private int cleanPayloadStaging(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("clean-payload-staging requires a payload spill directory");
        }
        Path spillDirectory = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        boolean dryRun = booleanOption(options, "--dry-run", true);
        Duration minAge = Duration.ofMillis(longOption(
                options,
                "--min-age-ms",
                PayloadStagingCleaner.DEFAULT_MIN_AGE.toMillis()
        ));
        PayloadStagingCleanupResult result = new PayloadStagingCleaner()
                .clean(spillDirectory, minAge, dryRun);

        out.println(dryRun ? "payload staging cleanup dry run" : "payload staging cleaned");
        writePayloadStagingCleanupResult(out, result);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            Path parent = report.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(report, result.toReportText(), StandardCharsets.UTF_8);
            out.println("report=" + report);
        }
        return 0;
    }

    private int checkHealth(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("check-health requires a health file");
        }
        Path healthFile = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        Duration maxAge = Duration.ofMillis(longOption(options, "--max-age-ms", 5_000L));
        boolean failOnUnhealthy = booleanOption(options, "--fail-on-unhealthy", true);

        ServerHealthCheckResult result = new ServerHealthFile().check(healthFile, maxAge);
        String reportText = result.toReportText();
        out.print(reportText);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            writeReport(report, reportText);
            out.println("report=" + report);
        }
        return failOnUnhealthy && !result.checkPassed() ? 1 : 0;
    }

    private int healthMetrics(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("health-metrics requires a health file");
        }
        Path healthFile = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        Duration maxAge = Duration.ofMillis(longOption(options, "--max-age-ms", 5_000L));
        boolean failOnUnhealthy = booleanOption(options, "--fail-on-unhealthy", false);
        ServerHealthCheckResult result = new ServerHealthFile().check(healthFile, maxAge);
        String metrics = result.toPrometheusText();
        out.print(metrics);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            Path parent = report.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(report, metrics, StandardCharsets.UTF_8);
        }
        return failOnUnhealthy && !result.checkPassed() ? 1 : 0;
    }

    private static void writePayloadStagingCleanupResult(PrintStream out, PayloadStagingCleanupResult result) {
        out.println("spill_directory=" + result.spillDirectory());
        out.println("dry_run=" + result.dryRun());
        out.println("min_age_ms=" + result.minAge().toMillis());
        out.println("scanned_stage_files=" + result.scannedStageFiles());
        out.println("eligible_stage_files=" + result.eligibleStageFiles());
        out.println("eligible_stage_bytes=" + result.eligibleStageBytes());
        out.println("removed_stage_files=" + result.removedStageFiles());
        out.println("removed_stage_bytes=" + result.removedStageBytes());
        out.println("reclaimed_bytes=" + result.reclaimedBytes());
    }

    private static void writeAuditResult(PrintStream out, DiskRepositoryAuditResult result) {
        out.println("repository_root=" + result.root());
        out.println("clean=" + result.clean());
        out.println("issue_count=" + result.issueCount());
        out.println("manifest_files=" + result.manifestFiles());
        out.println("container_files=" + result.containerFiles());
        out.println("journal_files=" + result.journalFiles());
        out.println("temp_files=" + result.tempFiles());
        out.println("referenced_chunks=" + result.referencedChunks());
        out.println("stored_chunks=" + result.storedChunks());
        out.println("corrupt_manifest_files=" + result.corruptManifestFiles());
        out.println("corrupt_container_files=" + result.corruptContainerFiles());
        out.println("corrupt_journal_files=" + result.corruptJournalFiles());
        out.println("invalid_manifest_files=" + result.invalidManifestFiles());
        out.println("replayable_journal_files=" + result.replayableJournalFiles());
        out.println("incomplete_journal_files=" + result.incompleteJournalFiles());
        out.println("container_name_mismatches=" + result.containerNameMismatches());
        out.println("missing_chunks=" + result.missingChunks());
        out.println("orphan_containers=" + result.orphanContainers());
        out.println("namespace_conflicts=" + result.namespaceConflicts());
    }

    private static void writeMaintenanceResult(PrintStream out, DiskRepositoryMaintenanceResult result) {
        out.println("repository_root=" + result.root());
        out.println("dry_run=" + result.dryRun());
        out.println("manifest_files=" + result.manifestFiles());
        out.println("referenced_chunks=" + result.referencedChunks());
        out.println("scanned_containers=" + result.scannedContainers());
        out.println("removed_containers=" + result.removedContainers());
        out.println("removed_container_bytes=" + result.removedContainerBytes());
        out.println("retained_young_containers=" + result.retainedYoungContainers());
        out.println("orphan_container_min_age_ms=" + result.orphanContainerMinAgeMillis());
        out.println("removed_temp_files=" + result.removedTempFiles());
        out.println("removed_temp_bytes=" + result.removedTempBytes());
        out.println("retained_young_temp_files=" + result.retainedYoungTempFiles());
        out.println("temp_file_min_age_ms=" + result.tempFileMinAgeMillis());
        out.println("removed_bytes=" + result.removedBytes());
    }

    private int syncBlockfsNamespace(String[] args, PrintStream out)
            throws IOException, InterruptedException {
        if (args.length < 2) {
            throw new IllegalArgumentException(args[0] + " requires a metadata root");
        }
        Path metadataRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        Path executable = Path.of(options.getOrDefault("--ctl", "cxnfs-blockfs-ctl"));
        Path device = Path.of(options.getOrDefault("--device", "/dev/cxnfs_blockfs_ctl"));
        boolean allowUnsupportedExtents = booleanOption(options, "--allow-unsupported-extents", false);
        Duration ctlTimeout = Duration.ofMillis(longOption(
                options,
                "--ctl-timeout-ms",
                LinuxBlockfsControlCommand.DEFAULT_PROCESS_TIMEOUT.toMillis()
        ));

        LinuxBlockfsControlCommand control = new LinuxBlockfsControlCommand(executable, device, ctlTimeout);
        LinuxBlockfsNamespaceSynchronizer synchronizer = new LinuxBlockfsNamespaceSynchronizer(
                new LinuxBlockfsNamespaceStore(),
                new LinuxBlockfsManifestIndexStore(),
                new LinuxBlockfsExtentHintStore(),
                new LinuxBlockfsExtentPublisher(
                        control,
                        (plan, extent) -> {
                            throw new IllegalStateException("namespace sync does not resolve extents");
                        }
                )
        );
        LinuxBlockfsNamespaceSyncResult result = synchronizer.sync(metadataRoot, allowUnsupportedExtents);

        out.println(labelForCommand(args[0]) + " namespace sync");
        out.println("project=cx");
        out.println("filesystem=cxbfs");
        out.println("compatibility_command=sync-blockfs-namespace");
        out.println("metadata_root=" + result.metadataRoot());
        out.println("generation=" + result.generation());
        out.println("files_published=" + result.publishedFiles());
        out.println("manifest_files=" + result.manifestFiles());
        out.println("manifest_chunks=" + result.manifestChunks());
        out.println("extent_hints_audited=" + result.auditedExtentHints());
        out.println("extents_attempted=" + result.attemptedExtents());
        out.println("extents_accepted=" + result.acceptedExtents());
        out.println("extents_unsupported=" + result.unsupportedExtents());
        out.println("extent_bytes_accepted=" + result.acceptedExtentBytes());
        out.println("control_device=" + device);
        out.println("control_timeout_ms=" + ctlTimeout.toMillis());
        out.println("status=synced");
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            writeReport(report, blockfsNamespaceSyncReport(result, device, ctlTimeout));
            out.println("report=" + report);
        }
        return 0;
    }

    private int resolveBlockfsMisses(String[] args, PrintStream out)
            throws IOException, InterruptedException {
        if (args.length < 2) {
            throw new IllegalArgumentException(args[0] + " requires a metadata root");
        }
        Path metadataRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        Path repositoryRoot = Path.of(requiredOption(options, "--repository-root"));
        Path payloadDevice = Path.of(requiredOption(options, "--payload-device"));
        Path executable = Path.of(options.getOrDefault("--ctl", "cxnfs-blockfs-ctl"));
        Path device = Path.of(options.getOrDefault("--device", "/dev/cxnfs_blockfs_ctl"));
        int maxEvents = intOption(options, "--max-events", 64);
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("--max-events must be positive");
        }
        int cordCount = intOption(options, "--cords", LinuxBlockfsReadMissService.DEFAULT_CORD_COUNT);
        boolean allowUnsupportedExtents = booleanOption(options, "--allow-unsupported-extents", false);
        Duration ctlTimeout = Duration.ofMillis(longOption(
                options,
                "--ctl-timeout-ms",
                LinuxBlockfsControlCommand.DEFAULT_PROCESS_TIMEOUT.toMillis()
        ));

        LinuxBlockfsControlCommand control = new LinuxBlockfsControlCommand(executable, device, ctlTimeout);
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        LinuxBlockfsReadMissService service = new LinuxBlockfsReadMissService(
                control,
                new LinuxBlockfsReadMissPlanner(
                        new LinuxBlockfsManifestIndexStore().load(metadataRoot),
                        new LinuxBlockfsPayloadAllocationStore().load(metadataRoot)
                ),
                new LinuxBlockfsPayloadCacheWriter(payloadDevice),
                new LinuxBlockfsStoreChunkFetcher(repository),
                cordCount,
                LinuxBlockfsMountSelector.metadataRoot(metadataRoot)
        );
        List<LinuxBlockfsReadMissResolution> resolutions =
                service.drainAvailable(maxEvents, allowUnsupportedExtents);

        String reportText = blockfsMissDrainReport(
                args[0],
                metadataRoot,
                repositoryRoot,
                payloadDevice,
                device,
                ctlTimeout,
                maxEvents,
                service.cordCount(),
                allowUnsupportedExtents,
                resolutions
        );
        out.print(reportText);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            writeReport(report, reportText);
            out.println("report=" + report);
        }
        return 0;
    }

    private int auditBlockfsMetadata(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException(args[0] + " requires a metadata root");
        }
        Path metadataRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        LinuxBlockfsNamespaceStore namespaceStore = new LinuxBlockfsNamespaceStore();
        LinuxBlockfsManifestIndexStore manifestIndexStore = new LinuxBlockfsManifestIndexStore();
        LinuxBlockfsExtentHintStore extentHintStore = new LinuxBlockfsExtentHintStore();
        var namespace = namespaceStore.load(metadataRoot);
        var manifestIndex = manifestIndexStore.loadIfExists(metadataRoot);
        var extentHints = extentHintStore.loadIfExists(metadataRoot);
        LinuxBlockfsMetadataAuditResult result = new LinuxBlockfsMetadataAuditor()
                .audit(namespace, manifestIndex, extentHints);

        out.println(labelForCommand(args[0]) + " metadata audit");
        out.println("project=cx");
        out.println("filesystem=cxbfs");
        out.println("compatibility_command=audit-blockfs-metadata");
        out.println("metadata_root=" + metadataRoot);
        out.println("generation=" + namespace.generation());
        out.println("namespace_files=" + result.namespaceFiles());
        out.println("manifest_files=" + result.manifestFiles());
        out.println("manifest_chunks=" + result.manifestChunks());
        out.println("extent_hints=" + result.extentHints());
        out.println("manifest_index_present=" + manifestIndex.isPresent());
        out.println("extent_hints_present=" + extentHints.isPresent());
        out.println("status=clean");
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            writeReport(report, blockfsMetadataAuditReport(
                    metadataRoot,
                    namespace.generation(),
                    manifestIndex.isPresent(),
                    extentHints.isPresent(),
                    result
            ));
            out.println("report=" + report);
        }
        return 0;
    }

    private int buildBlockfsExtentHints(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException(args[0] + " requires a metadata root");
        }
        Path metadataRoot = Path.of(args[1]);
        Map<String, String> options = parseOptions(args, 2);
        boolean requireComplete = booleanOption(options, "--require-complete", false);

        LinuxBlockfsNamespaceStore namespaceStore = new LinuxBlockfsNamespaceStore();
        LinuxBlockfsManifestIndexStore manifestIndexStore = new LinuxBlockfsManifestIndexStore();
        LinuxBlockfsPayloadAllocationStore allocationStore = new LinuxBlockfsPayloadAllocationStore();
        LinuxBlockfsExtentHintStore extentHintStore = new LinuxBlockfsExtentHintStore();

        var namespace = namespaceStore.load(metadataRoot);
        var manifestIndex = manifestIndexStore.load(metadataRoot);
        var allocations = allocationStore.load(metadataRoot);
        LinuxBlockfsExtentHintBuilder builder = new LinuxBlockfsExtentHintBuilder();
        var extentHints = builder.build(manifestIndex, allocations, requireComplete);
        new LinuxBlockfsMetadataAuditor()
                .audit(namespace, java.util.Optional.of(manifestIndex), java.util.Optional.of(extentHints));
        Path output = extentHintStore.write(metadataRoot, extentHints);
        LinuxBlockfsExtentHintBuildResult result = builder.describe(manifestIndex, allocations, extentHints);

        out.println(labelForCommand(args[0]) + " extent hints built");
        out.println("project=cx");
        out.println("filesystem=cxbfs");
        out.println("compatibility_command=build-blockfs-extent-hints");
        out.println("metadata_root=" + metadataRoot);
        out.println("output=" + output);
        out.println("generation=" + result.generation());
        out.println("manifest_files=" + result.manifestFiles());
        out.println("manifest_chunks=" + result.manifestChunks());
        out.println("payload_allocations=" + result.allocations());
        out.println("extent_hints=" + result.extentHints());
        out.println("complete=" + result.complete());
        out.println("status=built");
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path report = Path.of(reportValue).toAbsolutePath().normalize();
            writeReport(report, blockfsExtentHintBuildReport(metadataRoot, output, result));
            out.println("report=" + report);
        }
        return 0;
    }

    private static void writeReport(Path report, String reportText) throws IOException {
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(report, reportText, StandardCharsets.UTF_8);
    }

    private static String blockfsMetadataAuditReport(
            Path metadataRoot,
            long generation,
            boolean manifestIndexPresent,
            boolean extentHintsPresent,
            LinuxBlockfsMetadataAuditResult result
    ) {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.blockfs.metadata-audit.v1" + lineSeparator
                + "project=cx" + lineSeparator
                + "filesystem=cxbfs" + lineSeparator
                + "compatibility_report_format=cxnfs.blockfs.metadata-audit.v1" + lineSeparator
                + "metadata_root=" + metadataRoot.toAbsolutePath().normalize() + lineSeparator
                + "generation=" + generation + lineSeparator
                + "namespace_files=" + result.namespaceFiles() + lineSeparator
                + "manifest_files=" + result.manifestFiles() + lineSeparator
                + "manifest_chunks=" + result.manifestChunks() + lineSeparator
                + "extent_hints=" + result.extentHints() + lineSeparator
                + "manifest_index_present=" + manifestIndexPresent + lineSeparator
                + "extent_hints_present=" + extentHintsPresent + lineSeparator
                + "status=clean" + lineSeparator;
    }

    private static String blockfsExtentHintBuildReport(
            Path metadataRoot,
            Path output,
            LinuxBlockfsExtentHintBuildResult result
    ) {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.blockfs.extent-hints.v1" + lineSeparator
                + "project=cx" + lineSeparator
                + "filesystem=cxbfs" + lineSeparator
                + "compatibility_report_format=cxnfs.blockfs.extent-hints.v1" + lineSeparator
                + "metadata_root=" + metadataRoot.toAbsolutePath().normalize() + lineSeparator
                + "output=" + output.toAbsolutePath().normalize() + lineSeparator
                + "generation=" + result.generation() + lineSeparator
                + "manifest_files=" + result.manifestFiles() + lineSeparator
                + "manifest_chunks=" + result.manifestChunks() + lineSeparator
                + "payload_allocations=" + result.allocations() + lineSeparator
                + "extent_hints=" + result.extentHints() + lineSeparator
                + "complete=" + result.complete() + lineSeparator
                + "status=built" + lineSeparator;
    }

    private static String blockfsNamespaceSyncReport(
            LinuxBlockfsNamespaceSyncResult result,
            Path device,
            Duration ctlTimeout
    ) {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.blockfs.namespace-sync.v1" + lineSeparator
                + "project=cx" + lineSeparator
                + "filesystem=cxbfs" + lineSeparator
                + "compatibility_report_format=cxnfs.blockfs.namespace-sync.v1" + lineSeparator
                + "metadata_root=" + result.metadataRoot().toAbsolutePath().normalize() + lineSeparator
                + "generation=" + result.generation() + lineSeparator
                + "files_published=" + result.publishedFiles() + lineSeparator
                + "manifest_files=" + result.manifestFiles() + lineSeparator
                + "manifest_chunks=" + result.manifestChunks() + lineSeparator
                + "extent_hints_audited=" + result.auditedExtentHints() + lineSeparator
                + "extents_attempted=" + result.attemptedExtents() + lineSeparator
                + "extents_accepted=" + result.acceptedExtents() + lineSeparator
                + "extents_unsupported=" + result.unsupportedExtents() + lineSeparator
                + "extent_bytes_accepted=" + result.acceptedExtentBytes() + lineSeparator
                + "control_device=" + device + lineSeparator
                + "control_timeout_ms=" + ctlTimeout.toMillis() + lineSeparator
                + "status=synced" + lineSeparator;
    }

    private static String blockfsMissDrainReport(
            String command,
            Path metadataRoot,
            Path repositoryRoot,
            Path payloadDevice,
            Path device,
            Duration ctlTimeout,
            int maxEvents,
            int cordCount,
            boolean allowUnsupportedExtents,
            List<LinuxBlockfsReadMissResolution> resolutions
    ) {
        long plannedChunks = 0;
        long fetchedChunks = 0;
        long publishedExtents = 0;
        long unsupportedExtents = 0;
        long bytesWritten = 0;
        for (LinuxBlockfsReadMissResolution resolution : resolutions) {
            plannedChunks = Math.addExact(plannedChunks, resolution.plannedChunks());
            fetchedChunks = Math.addExact(fetchedChunks, resolution.fetchedChunks());
            publishedExtents = Math.addExact(publishedExtents, resolution.publishedExtents());
            unsupportedExtents = Math.addExact(unsupportedExtents, resolution.unsupportedExtents());
            bytesWritten = Math.addExact(bytesWritten, resolution.bytesWritten());
        }

        String lineSeparator = System.lineSeparator();
        return "cxnfs.blockfs.miss-drain.v1" + lineSeparator
                + "project=cx" + lineSeparator
                + "filesystem=cxbfs" + lineSeparator
                + "compatibility_command=resolve-blockfs-misses" + lineSeparator
                + "command_label=" + labelForCommand(command) + lineSeparator
                + "metadata_root=" + metadataRoot.toAbsolutePath().normalize() + lineSeparator
                + "repository_root=" + repositoryRoot.toAbsolutePath().normalize() + lineSeparator
                + "payload_device=" + payloadDevice.toAbsolutePath().normalize() + lineSeparator
                + "control_device=" + device + lineSeparator
                + "control_timeout_ms=" + ctlTimeout.toMillis() + lineSeparator
                + "max_events=" + maxEvents + lineSeparator
                + "cords=" + cordCount + lineSeparator
                + "allow_unsupported_extents=" + allowUnsupportedExtents + lineSeparator
                + "events_resolved=" + resolutions.size() + lineSeparator
                + "planned_chunks=" + plannedChunks + lineSeparator
                + "fetched_chunks=" + fetchedChunks + lineSeparator
                + "published_extents=" + publishedExtents + lineSeparator
                + "unsupported_extents=" + unsupportedExtents + lineSeparator
                + "payload_bytes_written=" + bytesWritten + lineSeparator
                + "status=drained" + lineSeparator;
    }

    private static String labelForCommand(String command) {
        return command.contains("cxbfs") ? "cxbfs" : "blockfs";
    }

    private static Map<String, String> parseOptions(String[] args, int startIndex) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = startIndex; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("expected option but found: " + key);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for option: " + key);
            }
            options.put(key, args[++i]);
        }
        return options;
    }

    private static ServeArguments parseServeArguments(String[] args) {
        List<ServedRoot> roots = new ArrayList<>();
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            String token = args[i];
            if (token.startsWith("--")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for option: " + token);
                }
                if (!SERVE_CLI_OPTIONS.contains(token)) {
                    throw new IllegalArgumentException("unsupported serve CLI option: " + token
                            + " (configure it in --config instead)");
                }
                String value = args[++i];
                if (token.equals("--root")) {
                    roots.add(parseServedRoot(value));
                } else {
                    if (token.equals("--listen") && !value.trim().equalsIgnoreCase("true")) {
                        throw new IllegalArgumentException("--listen true is required");
                    }
                    if (options.put(token, value) != null) {
                        throw new IllegalArgumentException("duplicate option: " + token);
                    }
                }
            } else {
                throw new IllegalArgumentException("serve no longer accepts a positional directory; "
                        + "use --root or serve.root entries in --config");
            }
        }
        return new ServeArguments(List.copyOf(roots), options);
    }

    private static Properties loadProperties(Path configPath) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(configPath)) {
            properties.load(input);
        } catch (IOException e) {
            throw new IOException("failed to load config " + configPath + ": " + e.getMessage(), e);
        }
        return properties;
    }

    private static Map<String, String> serveOptionsFromConfig(Properties properties) {
        Map<String, String> options = new LinkedHashMap<>();
        putServeOption(properties, options, "host", "--host");
        putServeOption(properties, options, "control-port", "--control-port");
        putServeOption(properties, options, "transport-port", "--transport-port");
        putServeOption(properties, options, "repository-root", "--repository-root");
        putServeOption(properties, options, "bulk-transport", "--bulk-transport");
        putServeOption(properties, options, "initial-cords", "--initial-cords");
        putServeOption(properties, options, "max-cords", "--max-cords");
        putServeOption(properties, options, "adaptive-cords", "--adaptive-cords");
        putServeOption(properties, options, "live-write-mode", "--live-write-mode");
        putServeOption(properties, options, "payload-staging-memory-bytes", "--payload-staging-memory-bytes");
        putServeOption(properties, options, "payload-spill-dir", "--payload-spill-dir");
        putServeOption(properties, options, "keystore", "--keystore");
        putServeOption(properties, options, "keystore-password-file", "--keystore-password-file");
        putServeOption(properties, options, "keystore-password-stdin", "--keystore-password-stdin");
        putServeOption(properties, options, "ready-file", "--ready-file");
        putServeOption(properties, options, "health-file", "--health-file");
        putServeOption(properties, options, "health-interval-ms", "--health-interval-ms");
        putServeOption(properties, options, "shutdown-file", "--shutdown-file");
        putServeOption(properties, options, "shutdown-poll-ms", "--shutdown-poll-ms");
        putServeOption(properties, options, "daemon-lock-file", "--daemon-lock-file");
        return options;
    }

    private static void rejectServeListenProperty(Properties properties) {
        if (properties.containsKey("serve.listen")) {
            throw new IllegalArgumentException("serve.listen is not supported; cxnfs serve always listens");
        }
    }

    private static void putServeOption(
            Properties properties,
            Map<String, String> options,
            String propertyName,
            String optionName
    ) {
        String value = trimToNull(properties.getProperty("serve." + propertyName));
        if (value != null) {
            options.put(optionName, value);
        }
    }

    private static List<ServedRoot> servedRootsFromConfig(Properties properties) {
        List<ServedRoot> roots = new ArrayList<>();
        String singleRoot = trimToNull(properties.getProperty("serve.root"));
        if (singleRoot != null) {
            roots.add(parseServedRoot(singleRoot));
        }
        properties.stringPropertyNames()
                .stream()
                .filter(name -> name.startsWith("serve.root."))
                .sorted(SERVE_ROOT_PROPERTY_ORDER)
                .map(name -> trimToNull(properties.getProperty(name)))
                .filter(value -> value != null)
                .map(ServerCli::parseServedRoot)
                .forEach(roots::add);
        return List.copyOf(roots);
    }

    private static final Comparator<String> SERVE_ROOT_PROPERTY_ORDER = (left, right) -> {
        String leftSuffix = left.substring("serve.root.".length());
        String rightSuffix = right.substring("serve.root.".length());
        Integer leftIndex = parseIntegerOrNull(leftSuffix);
        Integer rightIndex = parseIntegerOrNull(rightSuffix);
        if (leftIndex != null && rightIndex != null) {
            return leftIndex.compareTo(rightIndex);
        }
        if (leftIndex != null) {
            return -1;
        }
        if (rightIndex != null) {
            return 1;
        }
        return left.compareTo(right);
    };

    private static Integer parseIntegerOrNull(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ServedRoot parseServedRoot(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--root requires a value");
        }
        int separator = value.indexOf('=');
        if (separator >= 0) {
            String remotePrefix = value.substring(0, separator);
            String localDirectory = value.substring(separator + 1);
            if (remotePrefix.isBlank() || localDirectory.isBlank()) {
                throw new IllegalArgumentException("--root mapping must be <remote-prefix>=<local-directory>");
            }
            return new ServedRoot(remotePrefix, Path.of(localDirectory));
        }
        Path localDirectory = Path.of(value);
        return new ServedRoot(actualPathRemotePrefix(localDirectory), localDirectory);
    }

    private static String actualPathRemotePrefix(Path localDirectory) {
        String prefix = localDirectory.toAbsolutePath().normalize().toString().replace('\\', '/');
        return prefix.startsWith("/") ? prefix : "/" + prefix;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String requiredOption(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required option: " + name);
        }
        return value;
    }

    private static int intOption(Map<String, String> options, String name, int defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static long longOption(Map<String, String> options, String name) {
        return Long.parseLong(requiredOption(options, name));
    }

    private static long longOption(Map<String, String> options, String name, long defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    private static boolean booleanOption(Map<String, String> options, String name, boolean defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static CordPoolConfig cordPoolConfig(Map<String, String> options) {
        CordPoolConfig defaults = CordPoolConfig.xetLikeDefaults();
        return new CordPoolConfig(
                intOption(options, "--initial-cords", defaults.initialCords()),
                intOption(options, "--max-cords", defaults.maxCords()),
                booleanOption(options, "--adaptive-cords", defaults.adaptive())
        );
    }

    private static PayloadStagingConfig payloadStagingConfig(Map<String, String> options) {
        long memoryBytes = longOption(
                options,
                "--payload-staging-memory-bytes",
                PayloadStagingConfig.DEFAULT_MAX_MEMORY_BYTES
        );
        Path spillDirectory = Path.of(options.getOrDefault(
                "--payload-spill-dir",
                PayloadStagingConfig.defaultSpillDirectory().toString()
        ));
        return PayloadStagingConfig.withSpill(memoryBytes, spillDirectory);
    }

    private static void writePayloadStagingConfig(PrintStream out, PayloadStagingConfig config) {
        out.println("payload_staging_memory_bytes=" + config.maxMemoryBytes());
        out.println("payload_spill_dir=" + config.spillDirectory().orElseThrow());
    }

    private static LinuxBlockfsReadMissWorker blockfsReadMissWorker(
            Map<String, String> options,
            Path metadataRoot
    ) throws IOException {
        boolean resolveMisses = booleanOption(options, "--resolve-misses", false);
        if (!resolveMisses) {
            return null;
        }
        Path repositoryRoot = Path.of(requiredOption(options, "--repository-root"));
        Path payloadDevice = Path.of(requiredOption(options, "--payload-device"));
        int maxEvents = intOption(
                options,
                "--miss-resolver-max-events",
                LinuxBlockfsReadMissWorker.DEFAULT_MAX_EVENTS_PER_DRAIN
        );
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("--miss-resolver-max-events must be positive");
        }
        Duration pollInterval = Duration.ofMillis(longOption(
                options,
                "--miss-resolver-poll-ms",
                LinuxBlockfsReadMissWorker.DEFAULT_IDLE_POLL_INTERVAL.toMillis()
        ));
        if (pollInterval.isZero() || pollInterval.isNegative()) {
            throw new IllegalArgumentException("--miss-resolver-poll-ms must be positive");
        }
        int cordCount = intOption(
                options,
                "--miss-resolver-cords",
                LinuxBlockfsReadMissService.DEFAULT_CORD_COUNT
        );
        boolean allowUnsupportedExtents = booleanOption(options, "--allow-unsupported-extents", false);
        Path executable = Path.of(options.getOrDefault("--ctl", "cxnfs-blockfs-ctl"));
        Path device = Path.of(options.getOrDefault("--device", "/dev/cxnfs_blockfs_ctl"));
        Duration ctlTimeout = Duration.ofMillis(longOption(
                options,
                "--ctl-timeout-ms",
                LinuxBlockfsControlCommand.DEFAULT_PROCESS_TIMEOUT.toMillis()
        ));
        LinuxBlockfsReadMissService service = new LinuxBlockfsReadMissService(
                new LinuxBlockfsControlCommand(executable, device, ctlTimeout),
                new LinuxBlockfsReadMissPlanner(
                        new LinuxBlockfsManifestIndexStore().load(metadataRoot),
                        new LinuxBlockfsPayloadAllocationStore().load(metadataRoot)
                ),
                new LinuxBlockfsPayloadCacheWriter(payloadDevice),
                new LinuxBlockfsStoreChunkFetcher(new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults())),
                cordCount,
                LinuxBlockfsMountSelector.metadataRoot(metadataRoot)
        );
        return new LinuxBlockfsReadMissWorker(service, maxEvents, pollInterval, allowUnsupportedExtents);
    }

    private static void writeBlockfsMissWorkerConfig(PrintStream out, LinuxBlockfsReadMissWorker worker) {
        if (worker == null) {
            out.println("blockfs_miss_resolver=disabled");
            return;
        }
        out.println("blockfs_miss_resolver=configured");
        out.println("blockfs_miss_resolver_cords=" + worker.cordCount());
        out.println("blockfs_miss_resolver_max_events=" + worker.maxEventsPerDrain());
        out.println("blockfs_miss_resolver_poll_ms=" + worker.idlePollInterval().toMillis());
        out.println("blockfs_miss_resolver_allow_unsupported_extents=" + worker.allowUnsupportedExtents());
    }

    private static int maybeStartControlListener(
            CxnFileServer server,
            Map<String, String> options,
            PrintStream out,
            String loadedStatus,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker,
            DaemonLock daemonLock
    ) throws IOException, InterruptedException {
        if (!booleanOption(options, "--listen", false)) {
            rejectListenerFilesWithoutListener(options);
            out.println("control_listener=disabled");
            out.println("bulk_read_listener=disabled");
            out.println("blockfs_miss_resolver_status="
                    + (blockfsReadMissWorker == null ? "disabled" : "configured; use --listen true to start"));
            out.println("status=" + loadedStatus + "; use --listen true to start TLS control and bulk read listeners");
            return 0;
        }

        if (daemonLock == null) {
            throw new IOException("cxnfs daemon lock was not acquired before listener startup");
        }
        out.println("daemon_lock_file=" + daemonLock.path());
        char[] password = keyStorePassword(options);
        try {
            SSLContext context = new ControlTlsContextFactory()
                    .create(server.config().controlBinding().security(), password);
            ControlTlsServerSocketFactory socketFactory = new ControlTlsServerSocketFactory(
                    context,
                    server.config().controlBinding().security().cipherSuite()
            );
            CxnFileServerRuntime runtime = new CxnFileServerRuntime(server);
            try (ControlTcpServer controlListener = ControlTcpServer.bind(runtime, socketFactory);
                 BulkServerListener bulkReadListener = bindBulkListener(runtime, socketFactory, password, options)) {
                controlListener.start();
                bulkReadListener.start();
                String bulkScheme = bulkListenerScheme(bulkReadListener);
                out.println("control_listener=running");
                out.println("control_listener_bound=" + socketBinding("tls13-tcp", controlListener.boundAddress()));
                out.println("bulk_read_listener=running");
                out.println("bulk_read_listener_bound=" + socketBinding(bulkScheme, bulkReadListener.boundAddress()));
                Thread shutdownWatcher = startShutdownFileWatcher(options, controlListener, bulkReadListener, out);
                HealthFileWriter healthWriter = null;
                Path readyFile = null;
                try {
                    startBlockfsReadMissWorker(blockfsReadMissWorker, out);
                    healthWriter = startHealthFileWriter(
                            options,
                            server,
                            controlListener,
                            bulkReadListener,
                            blockfsReadMissWorker,
                            out
                    );
                    readyFile = writeReadyFile(
                            options,
                            server,
                            controlListener,
                            bulkReadListener,
                            blockfsReadMissWorker,
                            out
                    );
                    out.println("status=" + loadedStatus + "; serving TLS control and bulk read frames");
                    out.flush();
                    controlListener.awaitShutdown();
                    bulkReadListener.awaitShutdown();
                    if (controlListener.lastAcceptError().isPresent()) {
                        throw controlListener.lastAcceptError().orElseThrow();
                    }
                    if (controlListener.lastConnectionError().isPresent()) {
                        throw controlListener.lastConnectionError().orElseThrow();
                    }
                    if (bulkReadListener.lastAcceptError().isPresent()) {
                        throw bulkReadListener.lastAcceptError().orElseThrow();
                    }
                    if (bulkReadListener.lastConnectionError().isPresent()) {
                        throw bulkReadListener.lastConnectionError().orElseThrow();
                    }
                    return 0;
                } finally {
                    cleanupReadyFile(readyFile, out);
                    stopHealthFileWriter(
                            healthWriter,
                            server,
                            controlListener,
                            bulkReadListener,
                            blockfsReadMissWorker,
                            out
                    );
                    stopBlockfsReadMissWorker(blockfsReadMissWorker, out);
                    stopShutdownWatcher(shutdownWatcher);
                }
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static DaemonLock acquireDaemonLockIfListening(
            Map<String, String> options,
            CxnFileServerConfig config
    ) throws IOException {
        if (!booleanOption(options, "--listen", false)) {
            return null;
        }
        return acquireDaemonLock(options, config);
    }

    private static DaemonLock acquireDaemonLock(Map<String, String> options, CxnFileServerConfig config) throws IOException {
        Path lockFile = daemonLockFile(options, config);
        Path parent = lockFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return createDaemonLock(lockFile);
    }

    private static DaemonLock createDaemonLock(Path lockFile) throws IOException {
        try {
            Files.createFile(lockFile);
        } catch (FileAlreadyExistsException e) {
            if (!deleteStaleDaemonLock(lockFile)) {
                throw new IOException("cxnfs daemon is already running: lock=" + lockFile, e);
            }
            return createDaemonLock(lockFile);
        }
        try {
            Files.writeString(
                    lockFile,
                    "pid=" + ProcessHandle.current().pid() + System.lineSeparator(),
                    StandardCharsets.UTF_8
            );
            return new DaemonLock(lockFile);
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(lockFile);
            throw e;
        }
    }

    private static boolean deleteStaleDaemonLock(Path lockFile) throws IOException {
        long pid = daemonLockPid(lockFile);
        if (pid > 0 && ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
            return false;
        }
        Files.deleteIfExists(lockFile);
        return true;
    }

    private static long daemonLockPid(Path lockFile) throws IOException {
        if (!Files.isRegularFile(lockFile)) {
            return -1;
        }
        for (String line : Files.readAllLines(lockFile, StandardCharsets.UTF_8)) {
            int separator = line.indexOf('=');
            if (separator <= 0 || !line.substring(0, separator).trim().equals("pid")) {
                continue;
            }
            try {
                return Long.parseLong(line.substring(separator + 1).trim());
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    private static Path daemonLockFile(Map<String, String> options, CxnFileServerConfig config) {
        String configured = options.get("--daemon-lock-file");
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured).toAbsolutePath().normalize();
        }
        InetSocketAddress control = config.controlBinding().address();
        InetSocketAddress bulk = config.containerTransferBinding().address();
        String host = control.getHostString().replaceAll("[^A-Za-z0-9_.-]", "_");
        String fileName = "cxnfs-" + host + "-" + control.getPort() + "-" + bulk.getPort() + ".lock";
        return Path.of(System.getProperty("java.io.tmpdir"), fileName).toAbsolutePath().normalize();
    }

    private static BulkServerListener bindBulkListener(
            CxnFileServerRuntime runtime,
            ControlTlsServerSocketFactory socketFactory,
            char[] keyStorePassword,
            Map<String, String> options
    ) throws IOException {
        String transport = bulkTransportOption(options);
        return switch (transport) {
            case "stream" -> BulkReadStreamServer.bind(runtime, socketFactory);
            case "quic" -> QuicBulkTransferServer.bind(
                    runtime,
                    loadKeyStore(runtime.server().config().containerTransferBinding().security(), keyStorePassword),
                    keyStorePassword
            );
            default -> throw new IllegalArgumentException("--bulk-transport must be stream or quic");
        };
    }

    private static void startBlockfsReadMissWorker(
            LinuxBlockfsReadMissWorker worker,
            PrintStream out
    ) {
        if (worker == null) {
            out.println("blockfs_miss_resolver_status=disabled");
            return;
        }
        worker.start();
        out.println("blockfs_miss_resolver_status=running");
    }

    private static void stopBlockfsReadMissWorker(
            LinuxBlockfsReadMissWorker worker,
            PrintStream out
    ) throws InterruptedException {
        if (worker == null) {
            return;
        }
        worker.stop();
        LinuxBlockfsReadMissWorkerSnapshot snapshot = worker.snapshot();
        out.println("blockfs_miss_resolver_stopped=true");
        out.println("blockfs_miss_resolver_drain_cycles=" + snapshot.drainCycles());
        out.println("blockfs_miss_resolver_events_resolved=" + snapshot.eventsResolved());
        out.println("blockfs_miss_resolver_published_extents=" + snapshot.publishedExtents());
        out.println("blockfs_miss_resolver_payload_bytes_written=" + snapshot.payloadBytesWritten());
        snapshot.lastError().ifPresent(error -> out.println("blockfs_miss_resolver_last_error=" + error));
    }

    private static String bulkTransportOption(Map<String, String> options) {
        String transport = options.getOrDefault("--bulk-transport", "stream")
                .trim()
                .toLowerCase(java.util.Locale.ROOT);
        if (!transport.equals("stream") && !transport.equals("quic")) {
            throw new IllegalArgumentException("--bulk-transport must be stream or quic");
        }
        return transport;
    }

    private static String bulkListenerScheme(BulkServerListener listener) {
        if (listener instanceof QuicBulkTransferServer) {
            return "quic-tls13-udp";
        }
        return "tls13-bulk-stream";
    }

    private static void rejectListenerFilesWithoutListener(Map<String, String> options) {
        if (booleanOption(options, "--listen", false)) {
            return;
        }
        String readyFileValue = options.get("--ready-file");
        if (readyFileValue != null && !readyFileValue.isBlank()) {
            throw new IllegalArgumentException("--ready-file requires --listen true");
        }
        String healthFileValue = options.get("--health-file");
        if (healthFileValue != null && !healthFileValue.isBlank()) {
            throw new IllegalArgumentException("--health-file requires --listen true");
        }
    }

    private static Path writeReadyFile(
            Map<String, String> options,
            CxnFileServer server,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker,
            PrintStream out
    ) throws IOException {
        String readyFileValue = options.get("--ready-file");
        if (readyFileValue == null || readyFileValue.isBlank()) {
            out.println("ready_file=disabled");
            return null;
        }
        Path readyFile = Path.of(readyFileValue).toAbsolutePath().normalize();
        Path parent = readyFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        String readiness = readinessFile(
                server,
                controlListener.boundAddress(),
                bulkReadListener.boundAddress(),
                bulkListenerScheme(bulkReadListener),
                blockfsReadMissWorker
        );
        Path temp = Files.createTempFile(parent == null ? Path.of(".") : parent, ".cxnfs-ready-", ".tmp");
        try {
            Files.writeString(temp, readiness, StandardCharsets.UTF_8);
            try {
                Files.move(temp, readyFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, readyFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
        out.println("ready_file=" + readyFile);
        return readyFile;
    }

    private static void cleanupReadyFile(Path readyFile, PrintStream out) {
        if (readyFile == null) {
            return;
        }
        try {
            Files.deleteIfExists(readyFile);
            out.println("ready_file_removed=" + readyFile);
        } catch (IOException e) {
            out.println("ready_file_remove_failed=" + readyFile);
        }
    }

    private static HealthFileWriter startHealthFileWriter(
            Map<String, String> options,
            CxnFileServer server,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker,
            PrintStream out
    ) throws IOException {
        String healthFileValue = options.get("--health-file");
        if (healthFileValue == null || healthFileValue.isBlank()) {
            out.println("health_file=disabled");
            return null;
        }
        Path healthFile = Path.of(healthFileValue).toAbsolutePath().normalize();
        long intervalMillis = longOption(options, "--health-interval-ms", 1_000L);
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("--health-interval-ms must be positive");
        }
        writeHealthFile(healthFile, "running", server, controlListener, bulkReadListener, blockfsReadMissWorker);
        out.println("health_file=" + healthFile);
        out.println("health_interval_ms=" + intervalMillis);
        Thread thread = Thread.ofVirtual()
                .name("cxnfs-health-file-write")
                .start(() -> writeHealthFileLoop(
                        healthFile,
                        Duration.ofMillis(intervalMillis),
                        server,
                        controlListener,
                        bulkReadListener,
                        blockfsReadMissWorker
                ));
        return new HealthFileWriter(healthFile, thread);
    }

    private static void writeHealthFileLoop(
            Path healthFile,
            Duration interval,
            CxnFileServer server,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker
    ) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(interval.toMillis());
                writeHealthFile(healthFile, "running", server, controlListener, bulkReadListener, blockfsReadMissWorker);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
            // The server reports listener errors through the health payload when it can write.
        }
    }

    private static void stopHealthFileWriter(
            HealthFileWriter healthWriter,
            CxnFileServer server,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker,
            PrintStream out
    ) throws InterruptedException {
        if (healthWriter == null) {
            return;
        }
        healthWriter.thread().interrupt();
        healthWriter.thread().join();
        try {
            writeHealthFile(healthWriter.path(), "stopped", server, controlListener, bulkReadListener, blockfsReadMissWorker);
            out.println("health_file_stopped=" + healthWriter.path());
        } catch (IOException e) {
            out.println("health_file_stop_write_failed=" + healthWriter.path());
        }
    }

    private static void writeHealthFile(
            Path healthFile,
            String status,
            CxnFileServer server,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker
    ) throws IOException {
        Path parent = healthFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        String health = healthFile(status, server, controlListener, bulkReadListener, blockfsReadMissWorker);
        Path temp = Files.createTempFile(parent == null ? Path.of(".") : parent, ".cxnfs-health-", ".tmp");
        try {
            Files.writeString(temp, health, StandardCharsets.UTF_8);
            try {
                Files.move(temp, healthFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, healthFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private static String healthFile(
            String status,
            CxnFileServer server,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker
    ) {
        boolean controlRunning = controlListener.running();
        boolean bulkRunning = bulkReadListener.running();
        boolean healthy = status.equals("running")
                && controlRunning
                && bulkRunning
                && controlListener.lastAcceptError().isEmpty()
                && controlListener.lastConnectionError().isEmpty()
                && bulkReadListener.lastAcceptError().isEmpty()
                && bulkReadListener.lastConnectionError().isEmpty();
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        builder.append("cxnfs.server.health.v1").append(lineSeparator)
                .append("status=").append(status).append(lineSeparator)
                .append("healthy=").append(healthy).append(lineSeparator)
                .append("sampled_at=").append(Instant.now()).append(lineSeparator)
                .append("process_id=").append(ProcessHandle.current().pid()).append(lineSeparator)
                .append("served_directory=").append(server.config().servedDirectory().toAbsolutePath().normalize()).append(lineSeparator)
                .append(servedRootsReport(server.config().servedRoots(), lineSeparator))
                .append("control_listener=").append(socketBinding("tls13-tcp", controlListener.boundAddress())).append(lineSeparator)
                .append("bulk_read_listener=").append(socketBinding(bulkListenerScheme(bulkReadListener), bulkReadListener.boundAddress())).append(lineSeparator)
                .append("cipher_suite=").append(server.config().controlBinding().security().cipherSuite().protocolName()).append(lineSeparator)
                .append("bulk_cipher_suite=").append(bulkCipherSuite(server, bulkReadListener)).append(lineSeparator)
                .append("control_running=").append(controlRunning).append(lineSeparator)
                .append("bulk_running=").append(bulkRunning).append(lineSeparator)
                .append("control_active_connections=").append(controlListener.activeConnectionCount()).append(lineSeparator)
                .append("bulk_active_connections=").append(bulkReadListener.activeConnectionCount()).append(lineSeparator);
        bulkReadListener.totalConnectionCount()
                .ifPresent(value -> builder.append("bulk_total_connections=").append(value).append(lineSeparator));
        bulkReadListener.totalBulkStreamCount()
                .ifPresent(value -> builder.append("bulk_total_streams=").append(value).append(lineSeparator));
        appendBlockfsMissWorkerHealth(builder, lineSeparator, blockfsReadMissWorker);
        builder.append("control_last_accept_error=").append(errorSummary(controlListener.lastAcceptError())).append(lineSeparator)
                .append("control_last_connection_error=").append(errorSummary(controlListener.lastConnectionError())).append(lineSeparator)
                .append("bulk_last_accept_error=").append(errorSummary(bulkReadListener.lastAcceptError())).append(lineSeparator)
                .append("bulk_last_connection_error=").append(errorSummary(bulkReadListener.lastConnectionError())).append(lineSeparator);
        return builder.toString();
    }

    private static void appendBlockfsMissWorkerHealth(
            StringBuilder builder,
            String lineSeparator,
            LinuxBlockfsReadMissWorker worker
    ) {
        if (worker == null) {
            builder.append("blockfs_miss_resolver=disabled").append(lineSeparator);
            return;
        }
        LinuxBlockfsReadMissWorkerSnapshot snapshot = worker.snapshot();
        builder.append("blockfs_miss_resolver=enabled").append(lineSeparator)
                .append("blockfs_miss_resolver_cords=").append(worker.cordCount()).append(lineSeparator)
                .append("blockfs_miss_resolver_running=").append(snapshot.running()).append(lineSeparator)
                .append("blockfs_miss_resolver_drain_cycles=").append(snapshot.drainCycles()).append(lineSeparator)
                .append("blockfs_miss_resolver_events_resolved=").append(snapshot.eventsResolved()).append(lineSeparator)
                .append("blockfs_miss_resolver_planned_chunks=").append(snapshot.plannedChunks()).append(lineSeparator)
                .append("blockfs_miss_resolver_fetched_chunks=").append(snapshot.fetchedChunks()).append(lineSeparator)
                .append("blockfs_miss_resolver_published_extents=").append(snapshot.publishedExtents()).append(lineSeparator)
                .append("blockfs_miss_resolver_unsupported_extents=").append(snapshot.unsupportedExtents()).append(lineSeparator)
                .append("blockfs_miss_resolver_payload_bytes_written=")
                .append(snapshot.payloadBytesWritten())
                .append(lineSeparator)
                .append("blockfs_miss_resolver_last_error=")
                .append(snapshot.lastError().orElse("none"))
                .append(lineSeparator);
    }

    private static String errorSummary(java.util.Optional<IOException> error) {
        if (error.isEmpty()) {
            return "none";
        }
        IOException value = error.orElseThrow();
        String message = value.getMessage() == null ? "" : value.getMessage();
        return value.getClass().getSimpleName() + ":" + message.replace('\n', ' ').replace('\r', ' ');
    }

    private static String readinessFile(
            CxnFileServer server,
            InetSocketAddress controlAddress,
            InetSocketAddress bulkAddress,
            String bulkScheme,
            LinuxBlockfsReadMissWorker blockfsReadMissWorker
    ) {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.readiness.v1" + lineSeparator
                + "status=ready" + lineSeparator
                + "process_id=" + ProcessHandle.current().pid() + lineSeparator
                + "started_at=" + Instant.now() + lineSeparator
                + "control_listener=" + socketBinding("tls13-tcp", controlAddress) + lineSeparator
                + "bulk_read_listener=" + socketBinding(bulkScheme, bulkAddress) + lineSeparator
                + "cipher_suite=" + server.config().controlBinding().security().cipherSuite().protocolName() + lineSeparator
                + "bulk_cipher_suite=" + server.config().containerTransferBinding().security().cipherSuite().protocolName() + lineSeparator
                + "served_directory=" + server.config().servedDirectory().toAbsolutePath().normalize() + lineSeparator
                + servedRootsReport(server.config().servedRoots(), lineSeparator)
                + "blockfs_miss_resolver="
                + (blockfsReadMissWorker == null ? "disabled" : "enabled")
                + lineSeparator;
    }

    private static String servedRootsReport(List<ServedRoot> roots, String lineSeparator) {
        StringBuilder builder = new StringBuilder();
        builder.append("served_roots=").append(roots.size()).append(lineSeparator);
        for (int index = 0; index < roots.size(); index++) {
            ServedRoot root = roots.get(index);
            builder.append("served_root.").append(index).append(".remote_prefix=")
                    .append(root.remotePrefix()).append(lineSeparator);
            builder.append("served_root.").append(index).append(".local_directory=")
                    .append(root.localDirectory()).append(lineSeparator);
        }
        return builder.toString();
    }

    private static String bulkCipherSuite(CxnFileServer server, BulkServerListener listener) {
        if (listener instanceof QuicBulkTransferServer quic) {
            return quic.cipherSuite().protocolName();
        }
        return server.config().containerTransferBinding().security().cipherSuite().protocolName();
    }

    private record ServeArguments(List<ServedRoot> roots, Map<String, String> options) {
    }

    private record DaemonLock(Path path) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            Files.deleteIfExists(path);
        }
    }

    private record HealthFileWriter(Path path, Thread thread) {
    }

    private static Thread startShutdownFileWatcher(
            Map<String, String> options,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener,
            PrintStream out
    ) {
        String shutdownFileValue = options.get("--shutdown-file");
        if (shutdownFileValue == null || shutdownFileValue.isBlank()) {
            out.println("shutdown_file=disabled");
            return null;
        }
        Path shutdownFile = Path.of(shutdownFileValue).toAbsolutePath().normalize();
        long pollMillis = longOption(options, "--shutdown-poll-ms", 250L);
        if (pollMillis <= 0) {
            throw new IllegalArgumentException("--shutdown-poll-ms must be positive");
        }
        out.println("shutdown_file=" + shutdownFile);
        out.println("shutdown_poll_ms=" + pollMillis);
        return Thread.ofVirtual()
                .name("cxnfs-shutdown-file-watch")
                .start(() -> watchShutdownFile(shutdownFile, Duration.ofMillis(pollMillis), controlListener, bulkReadListener));
    }

    private static void watchShutdownFile(
            Path shutdownFile,
            Duration pollInterval,
            ControlTcpServer controlListener,
            BulkServerListener bulkReadListener
    ) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                if (Files.exists(shutdownFile)) {
                    closeQuietly(bulkReadListener);
                    closeQuietly(controlListener);
                    return;
                }
                Thread.sleep(pollInterval.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void stopShutdownWatcher(Thread shutdownWatcher) throws InterruptedException {
        if (shutdownWatcher == null) {
            return;
        }
        shutdownWatcher.interrupt();
        shutdownWatcher.join();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best effort shutdown from an operational stop-file watcher.
        }
    }

    private static char[] keyStorePassword(Map<String, String> options) throws IOException {
        String passwordFile = options.get("--keystore-password-file");
        boolean readFromStdin = booleanOption(options, "--keystore-password-stdin", false);
        if (passwordFile != null && readFromStdin) {
            throw new IllegalArgumentException("use only one of --keystore-password-file or --keystore-password-stdin");
        }
        if (passwordFile != null) {
            return readPasswordBytes(Files.readAllBytes(Path.of(passwordFile)));
        }
        if (readFromStdin) {
            return readPasswordBytes(System.in.readAllBytes());
        }
        Console console = System.console();
        if (console != null) {
            char[] password = console.readPassword("CXNFS keystore password: ");
            return password == null ? new char[0] : password;
        }
        return new char[0];
    }

    private static KeyStore loadKeyStore(TransportSecurityConfig security, char[] keyStorePassword) throws IOException {
        Path keyStorePath = security.keyStore()
                .orElseThrow(() -> new IllegalArgumentException("bulk QUIC requires a keystore"));
        char[] password = keyStorePassword == null ? new char[0] : keyStorePassword.clone();
        try {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType(keyStorePath));
            try (InputStream input = Files.newInputStream(keyStorePath)) {
                keyStore.load(input, password);
            }
            return keyStore;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to load bulk QUIC keystore: " + keyStorePath, e);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static String keyStoreType(Path path) {
        String name = path.getFileName() == null ? "" : path.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx")) {
            return "PKCS12";
        }
        if (name.endsWith(".jks")) {
            return "JKS";
        }
        return KeyStore.getDefaultType();
    }

    private static void rejectCommandLinePassword(Map<String, String> options) {
        if (options.containsKey("--keystore-password")) {
            throw new IllegalArgumentException(
                    "do not pass keystore passwords on the command line; use --keystore-password-file, "
                            + "--keystore-password-stdin true, or the hidden interactive prompt"
            );
        }
    }

    private static char[] readPasswordBytes(byte[] bytes) {
        try {
            int end = bytes.length;
            while (end > 0 && (bytes[end - 1] == '\n' || bytes[end - 1] == '\r')) {
                end--;
            }
            return new String(bytes, 0, end, StandardCharsets.UTF_8).toCharArray();
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    private static String binding(String scheme, TransportBinding binding) {
        return scheme + "://" + binding.address().getHostString() + ":" + binding.address().getPort();
    }

    private static String socketBinding(String scheme, InetSocketAddress address) {
        return scheme + "://" + address.getHostString() + ":" + address.getPort();
    }

    private static String mechanisms(AuthenticationPolicy policy) {
        StringBuilder builder = new StringBuilder();
        for (AuthMechanism mechanism : policy.acceptedMechanisms()) {
            if (!builder.isEmpty()) {
                builder.append(',');
            }
            builder.append(mechanism.cliName());
        }
        return builder.toString();
    }

    private static String hexPreview(byte[] data, int maxBytes) {
        StringBuilder builder = new StringBuilder();
        int limit = Math.min(data.length, maxBytes);
        for (int i = 0; i < limit; i++) {
            int value = data[i] & 0xff;
            builder.append(Character.forDigit(value >>> 4, 16));
            builder.append(Character.forDigit(value & 0x0f, 16));
        }
        if (data.length > limit) {
            builder.append("...");
        }
        return builder.toString();
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs serve --config <path> [--root <local-dir>|<remote-prefix>=<local-dir> ...] [--host host] [--control-port port] [--transport-port port] [--listen true]");
        out.println("cxnfs server serve-blockfs <mount-root> --metadata-root <path> --config <path> [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--payload-staging-memory-bytes n] [--payload-spill-dir path] [--keystore path] [--keystore-password-file path|--keystore-password-stdin true] [--listen true|false] [--ready-file path] [--health-file path] [--health-interval-ms n] [--shutdown-file path] [--shutdown-poll-ms n] [--resolve-misses true|false --repository-root path --payload-device path --miss-resolver-cords n --miss-resolver-max-events n --miss-resolver-poll-ms n]");
        out.println("cxnfs server read-blockfs-range <mount-root> --metadata-root <path> --path <file> --offset n --length n [--output path]");
        out.println("cxnfs server audit-blockfs-metadata <metadata-root> [--report path]");
        out.println("cxnfs server build-blockfs-extent-hints <metadata-root> [--require-complete true|false] [--report path]");
        out.println("cxnfs server audit-repository <repository-root> [--fail-on-issues true|false] [--report path]");
        out.println("cxnfs server compact-repository <repository-root> [--dry-run true|false] [--orphan-container-min-age-ms n] [--temp-file-min-age-ms n] [--report path]");
        out.println("cxnfs server clean-payload-staging <spill-dir> [--dry-run true|false] [--min-age-ms n] [--report path]");
        out.println("cxnfs server check-health <health-file> [--max-age-ms n] [--fail-on-unhealthy true|false] [--report path]");
        out.println("cxnfs server health-metrics <health-file> [--max-age-ms n] [--fail-on-unhealthy true|false] [--report path]");
        out.println("cxnfs server sync-blockfs-namespace <metadata-root> [--ctl path] [--device path] [--ctl-timeout-ms n] [--allow-unsupported-extents true|false] [--report path]");
        out.println("cxnfs server resolve-blockfs-misses <metadata-root> --repository-root <path> --payload-device <path> [--ctl path] [--device path] [--ctl-timeout-ms n] [--max-events n] [--cords n] [--allow-unsupported-extents true|false] [--report path]");
    }
}
