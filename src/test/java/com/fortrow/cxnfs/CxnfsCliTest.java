package com.fortrow.cxnfs;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.storage.LinuxBlockfsFile;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtent;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtentHintStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsExtentHintsSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestChunk;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestEntry;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsPayloadAllocation;
import com.fortrow.cxnfs.storage.LinuxBlockfsPayloadAllocationSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsPayloadAllocationStore;
import com.fortrow.cxnfs.store.DiskCxnfsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CxnfsCliTest {
    @TempDir
    Path tempDir;

    @Test
    void dispatchesDatasetCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{"dataset", "send", "/outputs/checkpoint"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertTrue(out.toString().contains("format=safetensors"));
    }

    @Test
    void helpListsDatasetAndBenchmarkCommands() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(new String[]{"help"}, new PrintStream(out), System.err);

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("serve"));
        assertTrue(output.contains("client"));
        assertTrue(output.contains("mount"));
        assertTrue(output.contains("dataset"));
        assertTrue(output.contains("protocol"));
        assertTrue(output.contains("benchmark"));
        assertTrue(output.contains("serve-cxbfs"));
    }

    @Test
    void writesProtocolFixtureReportThroughTopLevelCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Path report = tempDir.resolve("reports/protocol-fixtures.txt");

        int exit = Cxnfs.run(
                new String[]{
                        "protocol",
                        "fixture-report",
                        "protocol-fixtures",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs.protocol.fixtures.v1"));
        assertTrue(output.contains("fixture_files=37"));
        assertTrue(output.contains("control_fixture_files=34"));
        assertTrue(output.contains("bulk_fixture_files=3"));
        assertTrue(output.contains("coverage_complete=true"));
        assertTrue(output.contains("status=clean"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.protocol.fixtures.v1"));
        assertTrue(reportText.contains("status=clean"));
    }

    @Test
    void protocolFixtureReportPassesWithCompleteOperationCoverageRequired() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "protocol",
                        "fixture-report",
                        "protocol-fixtures",
                        "--fail-on-missing",
                        "true"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("coverage_complete=true"));
        assertTrue(output.contains("control_operations_missing_count=0"));
    }

    @Test
    void protocolFixtureReportRejectsMissingFixtureDirectory() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "protocol",
                        "fixture-report",
                        tempDir.resolve("missing-fixtures").toString()
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("failed to scan protocol fixtures"));
    }

    @Test
    void serveLoadsDirectoryWithConfigFileAuth() throws Exception {
        Path root = tempDir.resolve("export");
        Files.createDirectories(root.resolve("nested"));
        Files.writeString(root.resolve("nested/sample.txt"), "hello");
        Path config = tempDir.resolve("cxnfs-server.properties");
        ServeRuntime runtime = writeServeRuntime("serve-loads-directory");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.payload-staging-memory-bytes=536870912
                %s
                """
                        .formatted(root, serveRuntimeProperties(runtime))
        );

        String output = runServeUntilReady(
                runtime,
                "serve",
                "--config",
                config.toString(),
                "--listen",
                "true"
        );

        assertTrue(output.contains("cxnfs serve plan"));
        assertTrue(output.contains("files_loaded=1"));
        assertTrue(output.contains("auth=username_password"));
        assertTrue(output.contains("control_binding=tls13-tcp://127.0.0.1:0"));
        assertTrue(output.contains("bulk_transport=stream"));
        assertTrue(output.contains("payload_staging_memory_bytes=536870912"));
        assertTrue(output.contains("payload_spill_dir="));
        assertTrue(output.contains("control_listener=running"));
        assertTrue(output.contains("bulk_read_listener=running"));
    }

    @Test
    void serveLoadsMultipleJailedRootsWithConfigFileAuth() throws Exception {
        Path datasets = tempDir.resolve("datasets");
        Path outputs = tempDir.resolve("outputs");
        Files.createDirectories(datasets.resolve("train"));
        Files.createDirectories(outputs.resolve("run-1"));
        Files.writeString(datasets.resolve("train/shard.mds"), "sample");
        Files.writeString(outputs.resolve("run-1/model.bin"), "weights");
        Path config = tempDir.resolve("cxnfs-server.properties");
        ServeRuntime runtime = writeServeRuntime("serve-loads-multiple-roots");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                %s
                """
                        .formatted(serveRuntimeProperties(runtime))
        );

        String output = runServeUntilReady(
                runtime,
                "serve",
                "--root",
                "/datasets=" + datasets,
                "--root",
                "/outputs=" + outputs,
                "--config",
                config.toString()
        );

        assertTrue(output.contains("served_roots=2"));
        assertTrue(output.contains("served_root.0.remote_prefix=/datasets"));
        assertTrue(output.contains("served_root.0.local_directory=" + datasets.toAbsolutePath().normalize()));
        assertTrue(output.contains("served_root.1.remote_prefix=/outputs"));
        assertTrue(output.contains("served_root.1.local_directory=" + outputs.toAbsolutePath().normalize()));
        assertTrue(output.contains("files_loaded=2"));
    }

    @Test
    void serveBareRootUsesActualServerPathAsRemotePrefix() throws Exception {
        Path root = tempDir.resolve("srv/datasets");
        Files.createDirectories(root);
        Files.writeString(root.resolve("sample.txt"), "hello");
        Path config = tempDir.resolve("cxnfs-server.properties");
        ServeRuntime runtime = writeServeRuntime("serve-bare-root");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                %s
                """
                        .formatted(serveRuntimeProperties(runtime))
        );
        String actualPrefix = root.toAbsolutePath().normalize().toString().replace('\\', '/');
        if (!actualPrefix.startsWith("/")) {
            actualPrefix = "/" + actualPrefix;
        }
        String output = runServeUntilReady(
                runtime,
                "serve",
                "--root",
                root.toString(),
                "--config",
                config.toString()
        );

        assertTrue(output.contains("served_roots=1"));
        assertTrue(output.contains("served_root.0.remote_prefix=" + actualPrefix));
        assertTrue(output.contains("served_root.0.local_directory=" + root.toAbsolutePath().normalize()));
        assertTrue(output.contains("files_loaded=1"));
    }

    @Test
    void serveReportsConfiguredPayloadSpillPolicy() throws Exception {
        Path root = tempDir.resolve("export-spill");
        Path spill = tempDir.resolve("payload-spill");
        Files.createDirectories(root);
        Files.writeString(root.resolve("sample.txt"), "hello");
        Path config = tempDir.resolve("cxnfs-server.properties");
        ServeRuntime runtime = writeServeRuntime("serve-payload-spill");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.payload-staging-memory-bytes=4096
                serve.payload-spill-dir=%s
                %s
                """
                        .formatted(root, spill, serveRuntimeProperties(runtime))
        );

        String output = runServeUntilReady(
                runtime,
                "serve",
                "--config",
                config.toString()
        );

        assertTrue(output.contains("payload_staging_memory_bytes=4096"));
        assertTrue(output.contains("payload_spill_dir=" + spill.toAbsolutePath().normalize()));
    }

    @Test
    void serveListenStartsAndStopsFromShutdownFile() throws Exception {
        Path root = tempDir.resolve("export-listen");
        Files.createDirectories(root);
        Files.writeString(root.resolve("sample.txt"), "hello");
        Path config = tempDir.resolve("cxnfs-server.properties");
        Path keyStore = tempDir.resolve("server.p12");
        Path passwordFile = tempDir.resolve("server.p12.pass");
        Path readyFile = tempDir.resolve("runtime/cxnfs.ready");
        Path healthFile = tempDir.resolve("runtime/cxnfs.health");
        Path daemonLockFile = tempDir.resolve("runtime/cxnfs.lock");
        Path shutdownFile = tempDir.resolve("cxnfs.stop");
        generateKeyStore(keyStore, "secret".toCharArray());
        Files.writeString(passwordFile, "secret\n");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.host=127.0.0.1
                serve.control-port=0
                serve.transport-port=0
                serve.keystore=%s
                serve.keystore-password-file=%s
                serve.ready-file=%s
                serve.health-file=%s
                serve.health-interval-ms=10
                serve.daemon-lock-file=%s
                serve.shutdown-file=%s
                serve.shutdown-poll-ms=10
                """
                        .formatted(root, keyStore, passwordFile, readyFile, healthFile, daemonLockFile, shutdownFile)
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> server = executor.submit(() -> Cxnfs.run(
                new String[]{
                        "serve",
                        "--config",
                        config.toString()
                },
                new PrintStream(out),
                System.err
        ));

        String readiness;
        String runningHealth;
        try {
            readiness = waitForFileText(readyFile, Duration.ofSeconds(5));
            runningHealth = waitForFileText(healthFile, Duration.ofSeconds(5));
            Files.writeString(shutdownFile, "stop");
            assertEquals(0, server.get(5, TimeUnit.SECONDS));
        } finally {
            if (!server.isDone()) {
                Files.writeString(shutdownFile, "stop");
                server.cancel(true);
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        String output = out.toString();
        assertTrue(output.contains("control_listener=running"));
        assertTrue(output.contains("bulk_read_listener=running"));
        assertTrue(output.contains("ready_file=" + readyFile.toAbsolutePath().normalize()));
        assertTrue(output.contains("ready_file_removed=" + readyFile.toAbsolutePath().normalize()));
        assertTrue(output.contains("health_file=" + healthFile.toAbsolutePath().normalize()));
        assertTrue(output.contains("health_interval_ms=10"));
        assertTrue(output.contains("health_file_stopped=" + healthFile.toAbsolutePath().normalize()));
        assertTrue(output.contains("daemon_lock_file=" + daemonLockFile.toAbsolutePath().normalize()));
        assertTrue(output.contains("shutdown_file=" + shutdownFile.toAbsolutePath().normalize()));
        assertTrue(output.contains("shutdown_poll_ms=10"));
        assertTrue(output.contains("status=server facade loaded; serving TLS control and bulk read frames"));
        assertTrue(readiness.contains("cxnfs.readiness.v1"));
        assertTrue(readiness.contains("status=ready"));
        assertTrue(readiness.contains("process_id="));
        assertTrue(readiness.contains("started_at="));
        assertTrue(readiness.contains("control_listener=tls13-tcp://127.0.0.1:"));
        assertTrue(readiness.contains("bulk_read_listener=tls13-bulk-stream://127.0.0.1:"));
        assertTrue(readiness.contains("cipher_suite=tls_aes_128_gcm_sha256"));
        assertTrue(readiness.contains("bulk_cipher_suite=tls_aes_128_gcm_sha256"));
        assertTrue(readiness.contains("served_directory=" + root.toAbsolutePath().normalize()));
        assertTrue(readiness.contains("served_roots=1"));
        assertTrue(readiness.contains("served_root.0.remote_prefix=/"));
        assertTrue(readiness.contains("served_root.0.local_directory=" + root.toAbsolutePath().normalize()));
        assertTrue(runningHealth.contains("cxnfs.server.health.v1"));
        assertTrue(runningHealth.contains("status=running"));
        assertTrue(runningHealth.contains("healthy=true"));
        assertTrue(runningHealth.contains("served_roots=1"));
        assertTrue(runningHealth.contains("served_root.0.remote_prefix=/"));
        assertTrue(runningHealth.contains("served_root.0.local_directory=" + root.toAbsolutePath().normalize()));
        assertTrue(runningHealth.contains("bulk_cipher_suite=tls_aes_128_gcm_sha256"));
        assertTrue(runningHealth.contains("control_running=true"));
        assertTrue(runningHealth.contains("bulk_running=true"));
        assertTrue(runningHealth.contains("control_active_connections=0"));
        assertTrue(runningHealth.contains("bulk_active_connections=0"));
        assertTrue(runningHealth.contains("bulk_total_connections=0"));
        assertTrue(runningHealth.contains("control_last_accept_error=none"));
        assertTrue(runningHealth.contains("bulk_last_connection_error=none"));
        String stoppedHealth = Files.readString(healthFile);
        assertTrue(stoppedHealth.contains("cxnfs.server.health.v1"));
        assertTrue(stoppedHealth.contains("status=stopped"));
        assertTrue(stoppedHealth.contains("healthy=false"));
        assertTrue(Files.notExists(readyFile));
        assertTrue(Files.notExists(daemonLockFile));
    }

    @Test
    void serveListenFailsWhenDaemonLockAlreadyExists() throws Exception {
        Path root = tempDir.resolve("export-lock-held");
        Files.createDirectories(root);
        Files.writeString(root.resolve("sample.txt"), "hello");
        Path config = tempDir.resolve("cxnfs-server.properties");
        Path lockFile = tempDir.resolve("runtime/server.lock");
        Path repositoryRoot = tempDir.resolve("repository-lock-held");
        Files.createDirectories(lockFile.getParent());
        Files.writeString(lockFile, "pid=1\n");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.daemon-lock-file=%s
                serve.repository-root=%s
                """
                        .formatted(root, lockFile, repositoryRoot)
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "serve",
                        "--config",
                        config.toString()
                },
                new PrintStream(out),
                new PrintStream(err)
        );

        assertEquals(1, exit);
        assertTrue(err.toString().contains("cxnfs daemon is already running"));
        assertEquals("", out.toString());
        assertTrue(Files.notExists(repositoryRoot));
    }

    @Test
    void serveRejectsListenFalse() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                """
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "serve",
                        "--config",
                        config.toString(),
                        "--listen",
                        "false"
                },
                new PrintStream(out),
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertEquals("", out.toString());
        assertTrue(err.toString().contains("--listen true is required"));
    }

    @Test
    void serveRejectsListenProperty() throws Exception {
        Path root = tempDir.resolve("export-health-disabled");
        Files.createDirectories(root);
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.listen=true
                """
                        .formatted(root)
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "serve",
                        "--config",
                        config.toString()
                },
                new PrintStream(out),
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertEquals("", out.toString());
        assertTrue(err.toString().contains("serve.listen is not supported"));
    }

    @Test
    void checksServerHealthFileAndWritesReport() throws Exception {
        Path health = tempDir.resolve("server.health");
        Path report = tempDir.resolve("reports/server-health.txt");
        writeServerHealthFile(health, "running", true, Instant.now().minusMillis(100));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "check-health",
                        health.toString(),
                        "--max-age-ms",
                        "60000",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs.server.health-check.v1"));
        assertTrue(output.contains("status=running"));
        assertTrue(output.contains("healthy=true"));
        assertTrue(output.contains("stale=false"));
        assertTrue(output.contains("check_passed=true"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.server.health-check.v1"));
        assertTrue(reportText.contains("check_passed=true"));
    }

    @Test
    void checkHealthReturnsUnhealthyExitForStaleSample() throws Exception {
        Path health = tempDir.resolve("stale-server.health");
        writeServerHealthFile(health, "running", true, Instant.now().minusSeconds(30));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "check-health",
                        health.toString(),
                        "--max-age-ms",
                        "1000"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(1, exit);
        String output = out.toString();
        assertTrue(output.contains("stale=true"));
        assertTrue(output.contains("check_passed=false"));
    }

    @Test
    void exportsServerHealthMetricsAndWritesReport() throws Exception {
        Path health = tempDir.resolve("server-metrics.health");
        Path report = tempDir.resolve("reports/server-health.prom");
        writeServerHealthFile(health, "running", true, Instant.now().minusMillis(100));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "health-metrics",
                        health.toString(),
                        "--max-age-ms",
                        "60000",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs_server_health_check_passed 1"));
        assertTrue(output.contains("cxnfs_server_health_stale 0"));
        assertTrue(output.contains("cxnfs_server_control_running 1"));
        assertTrue(output.contains("cxnfs_server_bulk_total_connections 0"));
        assertTrue(output.contains("cxnfs_server_bulk_total_streams 0"));
        assertTrue(output.contains("cxnfs_server_health_info{"));
        assertFalse(output.contains("report="));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs_server_health_check_passed 1"));
        assertTrue(reportText.contains("cipher_suite=\"tls_aes_128_gcm_sha256\""));
    }

    @Test
    void cleansPayloadStagingArtifactsThroughServerCommand() throws Exception {
        Path spill = tempDir.resolve("payload-staging-cleanup");
        Path report = tempDir.resolve("reports/payload-staging.txt");
        Files.createDirectories(spill);
        Path stale = spill.resolve("transfer-0-0.cxnfs-stage");
        Path fresh = spill.resolve("transfer-0-1.cxnfs-stage");
        Files.writeString(stale, "stale payload");
        Files.writeString(fresh, "fresh payload");
        Files.setLastModifiedTime(stale, FileTime.from(Instant.parse("2026-05-30T12:00:00Z")));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "clean-payload-staging",
                        spill.toString(),
                        "--dry-run",
                        "false",
                        "--min-age-ms",
                        "86400000",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("payload staging cleaned"));
        assertTrue(output.contains("spill_directory=" + spill.toAbsolutePath().normalize()));
        assertTrue(output.contains("dry_run=false"));
        assertTrue(output.contains("scanned_stage_files=2"));
        assertTrue(output.contains("removed_stage_files=1"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        assertTrue(Files.notExists(stale));
        assertTrue(Files.exists(fresh));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.payload-staging.cleanup.v1"));
        assertTrue(reportText.contains("removed_stage_files=1"));
    }

    @Test
    void serveWithRepositoryRootKeepsServedDirectorySourceBacked() throws Exception {
        Path root = tempDir.resolve("export-durable");
        Path repositoryRoot = tempDir.resolve("repository-root");
        Files.createDirectories(root.resolve("nested"));
        Files.writeString(root.resolve("nested/sample.txt"), "hello durable repository");
        Path config = tempDir.resolve("cxnfs-server.properties");
        ServeRuntime runtime = writeServeRuntime("serve-durable-repository");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.repository-root=%s
                %s
                """
                        .formatted(root, repositoryRoot, serveRuntimeProperties(runtime))
        );

        String output = runServeUntilReady(
                runtime,
                "serve",
                "--config",
                config.toString()
        );

        assertTrue(output.contains("repository_root=" + repositoryRoot.toAbsolutePath().normalize()));
        assertTrue(output.contains("files_loaded=1"));
        assertTrue(Files.isDirectory(repositoryRoot.resolve("containers")));
        assertTrue(Files.isDirectory(repositoryRoot.resolve("manifests")));
        try (var containers = Files.list(repositoryRoot.resolve("containers"));
             var manifests = Files.list(repositoryRoot.resolve("manifests"))) {
            assertTrue(containers.toList().isEmpty());
            assertTrue(manifests.toList().isEmpty());
        }
    }

    @Test
    void serveWithRepositoryRootDoesNotMirrorDeletedServedFiles() throws Exception {
        Path root = tempDir.resolve("export-prune");
        Path repositoryRoot = tempDir.resolve("repository-prune");
        Files.createDirectories(root.resolve("nested"));
        Path keep = root.resolve("nested/keep.txt");
        Path stale = root.resolve("nested/stale.txt");
        Files.writeString(keep, "keep");
        Files.writeString(stale, "stale");
        Path config = tempDir.resolve("cxnfs-server.properties");
        ServeRuntime firstRuntime = writeServeRuntime("serve-prune-first");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.repository-root=%s
                %s
                """
                        .formatted(root, repositoryRoot, serveRuntimeProperties(firstRuntime))
        );

        runServeUntilReady(firstRuntime, "serve", "--config", config.toString());
        Files.delete(stale);

        ServeRuntime secondRuntime = writeServeRuntime("serve-prune-second");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                serve.repository-root=%s
                %s
                """
                        .formatted(root, repositoryRoot, serveRuntimeProperties(secondRuntime))
        );
        runServeUntilReady(secondRuntime, "serve", "--config", config.toString());

        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        assertTrue(repository.manifest("/nested/keep.txt").isEmpty());
        assertTrue(repository.manifest("/nested/stale.txt").isEmpty());
        assertEquals(0, repository.list("/nested").entries().size());
        assertEquals(0, countRegularFiles(repositoryRoot.resolve("manifests")));
        assertTrue(Files.exists(keep));
        assertTrue(Files.notExists(stale));
    }

    @Test
    void compactsRepositoryArtifactsThroughServerCommand() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-compact");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "old durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        Files.writeString(repositoryRoot.resolve("tmp/stale.tmp"), "interrupted write");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "compact-repository",
                        repositoryRoot.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("repository compacted"));
        assertTrue(output.contains("repository_root=" + repositoryRoot.toAbsolutePath().normalize()));
        assertTrue(output.contains("dry_run=false"));
        assertTrue(output.contains("manifest_files=1"));
        assertTrue(output.contains("removed_containers="));
        assertTrue(output.contains("removed_temp_files=1"));
        assertEquals(0, countRegularFiles(repositoryRoot.resolve("tmp")));
    }

    @Test
    void inspectsRepositoryArtifactsWithoutDeletingAndWritesMaintenanceReport() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-inspect");
        Path report = tempDir.resolve("reports/maintenance.txt");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "old durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        Files.writeString(repositoryRoot.resolve("tmp/stale.tmp"), "interrupted write");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "compact-repository",
                        repositoryRoot.toString(),
                        "--dry-run",
                        "true",
                        "--orphan-container-min-age-ms",
                        "0",
                        "--temp-file-min-age-ms",
                        "0",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("repository compaction dry run"));
        assertTrue(output.contains("dry_run=true"));
        assertTrue(output.contains("removed_containers=1"));
        assertTrue(output.contains("removed_temp_files=1"));
        assertTrue(output.contains("orphan_container_min_age_ms=0"));
        assertTrue(output.contains("temp_file_min_age_ms=0"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        assertEquals(2, countRegularFiles(repositoryRoot.resolve("containers")));
        assertEquals(1, countRegularFiles(repositoryRoot.resolve("tmp")));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.repository.maintenance.v1"));
        assertTrue(reportText.contains("dry_run=true"));
        assertTrue(reportText.contains("removed_containers=1"));
        assertTrue(reportText.contains("orphan_container_min_age_ms=0"));
    }

    @Test
    void compactRepositoryCommandHonorsMaintenanceRetentionPolicy() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-retention-cli");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "old durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        Files.writeString(repositoryRoot.resolve("tmp/recent.tmp"), "recent interrupted write");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "compact-repository",
                        repositoryRoot.toString(),
                        "--orphan-container-min-age-ms",
                        Long.toString(Duration.ofDays(1).toMillis()),
                        "--temp-file-min-age-ms",
                        Long.toString(Duration.ofDays(1).toMillis())
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("repository compacted"));
        assertTrue(output.contains("removed_containers=0"));
        assertTrue(output.contains("retained_young_containers=1"));
        assertTrue(output.contains("removed_temp_files=0"));
        assertTrue(output.contains("retained_young_temp_files=1"));
        assertTrue(output.contains("orphan_container_min_age_ms=" + Duration.ofDays(1).toMillis()));
        assertTrue(output.contains("temp_file_min_age_ms=" + Duration.ofDays(1).toMillis()));
        assertEquals(2, countRegularFiles(repositoryRoot.resolve("containers")));
        assertEquals(1, countRegularFiles(repositoryRoot.resolve("tmp")));
    }

    @Test
    void compactRepositoryCommandFailsClosedWhenJournalsArePending() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-compact-journal");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "old durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        Files.createDirectories(repositoryRoot.resolve("journals"));
        Files.writeString(repositoryRoot.resolve("journals/pending.cxnfj"), "pending journal");
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "compact-repository",
                        repositoryRoot.toString()
                },
                System.out,
                new PrintStream(err)
        );

        assertEquals(1, exit);
        assertTrue(err.toString().contains("pending journals"));
        assertEquals(2, countRegularFiles(repositoryRoot.resolve("containers")));
    }

    @Test
    void auditsRepositoryArtifactsThroughServerCommandAndWritesReport() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-audit-clean");
        Path report = tempDir.resolve("reports/audit.txt");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "clean durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "audit-repository",
                        repositoryRoot.toString(),
                        "--fail-on-issues",
                        "true",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("repository audit"));
        assertTrue(output.contains("clean=true"));
        assertTrue(output.contains("issue_count=0"));
        assertTrue(output.contains("journal_files=0"));
        assertTrue(output.contains("replayable_journal_files=0"));
        assertTrue(output.contains("incomplete_journal_files=0"));
        assertTrue(output.contains("corrupt_journal_files=0"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.repository.audit.v1"));
        assertTrue(reportText.contains("clean=true"));
        assertTrue(reportText.contains("journal_files=0"));
    }

    @Test
    void auditRepositoryCommandReportsPendingJournalFiles() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-audit-journal");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "clean durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        Files.writeString(repositoryRoot.resolve("journals/corrupt.cxnfj"), "not a journal");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "audit-repository",
                        repositoryRoot.toString(),
                        "--fail-on-issues",
                        "true"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(1, exit);
        String output = out.toString();
        assertTrue(output.contains("repository audit"));
        assertTrue(output.contains("clean=false"));
        assertTrue(output.contains("journal_files=1"));
        assertTrue(output.contains("corrupt_journal_files=1"));
        assertTrue(output.contains("replayable_journal_files=0"));
        assertTrue(output.contains("incomplete_journal_files=0"));
    }

    @Test
    void auditRepositoryCanFailOnDetectedIssues() throws Exception {
        Path repositoryRoot = tempDir.resolve("repository-audit-dirty");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        repository.put("/datasets/train/current.mds", "old durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new durable bytes".repeat(256).getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "audit-repository",
                        repositoryRoot.toString(),
                        "--fail-on-issues",
                        "true"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(1, exit);
        String output = out.toString();
        assertTrue(output.contains("repository audit"));
        assertTrue(output.contains("clean=false"));
        assertTrue(output.contains("orphan_containers=1"));
    }

    @Test
    void serveRejectsConfigOwnedCliOptions() throws Exception {
        Path root = tempDir.resolve("export");
        Files.createDirectories(root);
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                serve.root=/=%s
                """
                        .formatted(root)
        );
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "serve",
                        "--config",
                        config.toString(),
                        "--repository-root",
                        tempDir.resolve("repository").toString()
                },
                System.out,
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("unsupported serve CLI option: --repository-root"));
    }

    @Test
    void syncsBlockfsNamespaceThroughServerCommand() throws Exception {
        Path metadataRoot = tempDir.resolve("metadata-root");
        Path report = tempDir.resolve("reports/blockfs-sync.txt");
        writeBlockfsMetadataRoot(metadataRoot);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "sync-blockfs-namespace",
                        metadataRoot.toString(),
                        "--ctl",
                        "/bin/echo",
                        "--device",
                        "/dev/test",
                        "--ctl-timeout-ms",
                        "5000",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("blockfs namespace sync"));
        assertTrue(output.contains("generation=12"));
        assertTrue(output.contains("files_published=1"));
        assertTrue(output.contains("manifest_files=1"));
        assertTrue(output.contains("manifest_chunks=1"));
        assertTrue(output.contains("extent_hints_audited=1"));
        assertTrue(output.contains("extents_attempted=1"));
        assertTrue(output.contains("extents_accepted=1"));
        assertTrue(output.contains("control_device=/dev/test"));
        assertTrue(output.contains("control_timeout_ms=5000"));
        assertTrue(output.contains("status=synced"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.blockfs.namespace-sync.v1"));
        assertTrue(reportText.contains("extents_accepted=1"));
        assertTrue(reportText.contains("control_timeout_ms=5000"));
        assertTrue(reportText.contains("status=synced"));
    }

    @Test
    void resolvesBlockfsMissesByWritingVerifiedPayloadCacheAndPublishingExtents() throws Exception {
        Path metadataRoot = tempDir.resolve("metadata-root-miss");
        Path repositoryRoot = tempDir.resolve("repository");
        Path payloadDevice = tempDir.resolve("payload.img");
        Path report = tempDir.resolve("reports/blockfs-miss-drain.txt");
        Path ctlLog = tempDir.resolve("ctl.log");
        Path ctlState = tempDir.resolve("ctl-state");
        byte[] data = "streamed training shard\n".repeat(64).getBytes(StandardCharsets.UTF_8);
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        FileManifest manifest = repository.put("/datasets/train/shard-00000.mds", data);
        int expectedChunks = manifest.chunks().size();
        writeBlockfsMetadataRootForManifest(metadataRoot, 2, manifest);
        Path ctl = writeMissControlTool(2, data.length, ctlLog, ctlState);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "resolve-blockfs-misses",
                        metadataRoot.toString(),
                        "--repository-root",
                        repositoryRoot.toString(),
                        "--payload-device",
                        payloadDevice.toString(),
                        "--ctl",
                        ctl.toString(),
                        "--device",
                        "/dev/test",
                        "--max-events",
                        "4",
                        "--cords",
                        "2",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs.blockfs.miss-drain.v1"));
        assertTrue(output.contains("events_resolved=1"));
        assertTrue(output.contains("planned_chunks=" + expectedChunks));
        assertTrue(output.contains("fetched_chunks=" + expectedChunks));
        assertTrue(output.contains("published_extents=" + expectedChunks));
        assertTrue(output.contains("cords=2"));
        assertTrue(output.contains("payload_bytes_written=" + data.length));
        assertTrue(output.contains("status=drained"));
        byte[] payloadBytes = Files.readAllBytes(payloadDevice);
        long fileOffset = 0;
        for (int index = 0; index < manifest.chunks().size(); index++) {
            ChunkRef chunk = manifest.chunks().get(index);
            int physicalOffset = (int) ((1L + index) * LinuxBlockfsPayloadAllocation.PAYLOAD_BLOCK_SIZE);
            assertArrayEquals(
                    Arrays.copyOfRange(data, Math.toIntExact(fileOffset), Math.toIntExact(fileOffset + chunk.size())),
                    Arrays.copyOfRange(payloadBytes, physicalOffset, physicalOffset + chunk.size())
            );
            fileOffset = Math.addExact(fileOffset, chunk.size());
        }
        String controlLog = Files.readString(ctlLog);
        assertTrue(controlLog.contains("next-miss --metadata-root " + metadataRoot + " --nonblock"));
        assertTrue(controlLog.contains("register-extent"));
        assertTrue(controlLog.contains("--metadata-root " + metadataRoot));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.blockfs.miss-drain.v1"));
        assertTrue(reportText.contains("events_resolved=1"));
        assertTrue(reportText.contains("published_extents=" + expectedChunks));
    }

    @Test
    void auditsBlockfsMetadataWithoutPublishingToControlDevice() throws Exception {
        Path metadataRoot = tempDir.resolve("metadata-root");
        Path report = tempDir.resolve("reports/blockfs-audit.txt");
        writeBlockfsMetadataRoot(metadataRoot);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "audit-blockfs-metadata",
                        metadataRoot.toString(),
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("blockfs metadata audit"));
        assertTrue(output.contains("namespace_files=1"));
        assertTrue(output.contains("manifest_files=1"));
        assertTrue(output.contains("manifest_chunks=1"));
        assertTrue(output.contains("extent_hints=1"));
        assertTrue(output.contains("manifest_index_present=true"));
        assertTrue(output.contains("extent_hints_present=true"));
        assertTrue(output.contains("status=clean"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.blockfs.metadata-audit.v1"));
        assertTrue(reportText.contains("namespace_files=1"));
        assertTrue(reportText.contains("extent_hints_present=true"));
        assertTrue(reportText.contains("status=clean"));
    }

    @Test
    void serveBlockfsLoadsMountedReadOnlyStoreWithConfigAuth() throws Exception {
        Path mountRoot = tempDir.resolve("mount");
        Path metadataRoot = tempDir.resolve("metadata-root");
        writeMountedBlockfsFixture(mountRoot, metadataRoot);
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                """
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "serve-blockfs",
                        mountRoot.toString(),
                        "--metadata-root",
                        metadataRoot.toString(),
                        "--config",
                        config.toString(),
                        "--host",
                        "127.0.0.1"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs serve-blockfs plan"));
        assertTrue(output.contains("files_indexed=1"));
        assertTrue(output.contains("auth=username_password"));
        assertTrue(output.contains("control_binding=tls13-tcp://127.0.0.1:4509"));
        assertTrue(output.contains("control_listener=disabled"));
        assertTrue(output.contains("bulk_read_listener=disabled"));
        assertTrue(output.contains("blockfs_miss_resolver=disabled"));
    }

    @Test
    void serveBlockfsCanConfigureContinuousMissResolver() throws Exception {
        Path mountRoot = tempDir.resolve("mount-miss-resolver");
        Path metadataRoot = tempDir.resolve("metadata-root-miss-resolver");
        Path repositoryRoot = tempDir.resolve("repository-miss-resolver");
        Path payloadDevice = tempDir.resolve("payload-cache.img");
        byte[] data = "mounted blockfs data\n".repeat(128).getBytes(StandardCharsets.UTF_8);
        Path mountedFile = mountRoot.resolve("datasets/train/sample.bin");
        Files.createDirectories(mountedFile.getParent());
        Files.write(mountedFile, data);
        DiskCxnfsRepository repository = new DiskCxnfsRepository(repositoryRoot, GearHashChunker.defaults());
        FileManifest manifest = repository.put("/datasets/train/sample.bin", data);
        writeBlockfsMetadataRootForManifest(metadataRoot, 2, manifest);
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                """
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "serve-blockfs",
                        mountRoot.toString(),
                        "--metadata-root",
                        metadataRoot.toString(),
                        "--config",
                        config.toString(),
                        "--host",
                        "127.0.0.1",
                        "--resolve-misses",
                        "true",
                        "--repository-root",
                        repositoryRoot.toString(),
                        "--payload-device",
                        payloadDevice.toString(),
                        "--miss-resolver-max-events",
                        "8",
                        "--miss-resolver-cords",
                        "4",
                        "--miss-resolver-poll-ms",
                        "3"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("blockfs_miss_resolver=configured"));
        assertTrue(output.contains("blockfs_miss_resolver_cords=4"));
        assertTrue(output.contains("blockfs_miss_resolver_max_events=8"));
        assertTrue(output.contains("blockfs_miss_resolver_poll_ms=3"));
        assertTrue(output.contains("blockfs_miss_resolver_status=configured; use --listen true to start"));
        assertTrue(output.contains("control_listener=disabled"));
    }

    @Test
    void serveCxbfsTopLevelAliasLoadsMountedReadOnlyStoreWithCxVocabulary() throws Exception {
        Path mountRoot = tempDir.resolve("mount-cxbfs");
        Path metadataRoot = tempDir.resolve("metadata-root-cxbfs");
        writeMountedBlockfsFixture(mountRoot, metadataRoot);
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                """
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "serve-cxbfs",
                        mountRoot.toString(),
                        "--metadata-root",
                        metadataRoot.toString(),
                        "--config",
                        config.toString(),
                        "--host",
                        "127.0.0.1"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs serve-cxbfs plan"));
        assertTrue(output.contains("project=cx"));
        assertTrue(output.contains("filesystem=cxbfs"));
        assertTrue(output.contains("compatibility_command=serve-blockfs"));
        assertTrue(output.contains("files_indexed=1"));
        assertTrue(output.contains("auth=username_password"));
    }

    @Test
    void auditCxbfsMetadataAliasWritesCxIdentityFields() throws Exception {
        Path metadataRoot = tempDir.resolve("metadata-root-cxbfs-audit");
        Path report = tempDir.resolve("reports/cxbfs-audit.txt");
        writeBlockfsMetadataRoot(metadataRoot);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "audit-cxbfs-metadata",
                        metadataRoot.toString(),
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxbfs metadata audit"));
        assertTrue(output.contains("project=cx"));
        assertTrue(output.contains("filesystem=cxbfs"));
        assertTrue(output.contains("compatibility_command=audit-blockfs-metadata"));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.blockfs.metadata-audit.v1"));
        assertTrue(reportText.contains("project=cx"));
        assertTrue(reportText.contains("filesystem=cxbfs"));
        assertTrue(reportText.contains("compatibility_report_format=cxnfs.blockfs.metadata-audit.v1"));
    }

    @Test
    void readsMountedBlockfsRangeByPathAndWritesOutput() throws Exception {
        Path mountRoot = tempDir.resolve("mount");
        Path metadataRoot = tempDir.resolve("metadata-root");
        writeMountedBlockfsFixture(mountRoot, metadataRoot);
        Path outputFile = tempDir.resolve("range.bin");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "read-blockfs-range",
                        mountRoot.toString(),
                        "--metadata-root",
                        metadataRoot.toString(),
                        "--path",
                        "/datasets/train/sample.bin",
                        "--offset",
                        "8",
                        "--length",
                        "64",
                        "--output",
                        outputFile.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("blockfs range read"));
        assertTrue(output.contains("path=/datasets/train/sample.bin"));
        assertTrue(output.contains("returned_offset=8"));
        assertTrue(output.contains("returned_bytes=64"));
        assertTrue(output.contains("chunks_verified=1"));
        byte[] expected = "mounted blockfs data\n".repeat(128).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertArrayEquals(Arrays.copyOfRange(expected, 8, 72), Files.readAllBytes(outputFile));
    }

    @Test
    void buildsBlockfsExtentHintsFromPayloadAllocationMap() throws Exception {
        Path metadataRoot = tempDir.resolve("metadata-root");
        Path report = tempDir.resolve("reports/blockfs-extent-hints.txt");
        writeBlockfsMetadataRootWithoutExtentHints(metadataRoot);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = Cxnfs.run(
                new String[]{
                        "server",
                        "build-blockfs-extent-hints",
                        metadataRoot.toString(),
                        "--require-complete",
                        "true",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("blockfs extent hints built"));
        assertTrue(output.contains("generation=12"));
        assertTrue(output.contains("manifest_chunks=1"));
        assertTrue(output.contains("payload_allocations=1"));
        assertTrue(output.contains("extent_hints=1"));
        assertTrue(output.contains("complete=true"));
        assertTrue(output.contains("status=built"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        LinuxBlockfsExtentHintsSnapshot hints = new LinuxBlockfsExtentHintStore().load(metadataRoot);
        assertEquals(hash(1)[0], hints.extents().getFirst().expectedHash()[0]);
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.blockfs.extent-hints.v1"));
        assertTrue(reportText.contains("payload_allocations=1"));
        assertTrue(reportText.contains("complete=true"));
        assertTrue(reportText.contains("status=built"));
    }

    private static void writeBlockfsMetadataRootForManifest(
            Path metadataRoot,
            long inodeNumber,
            FileManifest manifest
    ) throws Exception {
        new LinuxBlockfsNamespaceStore().write(
                metadataRoot,
                new LinuxBlockfsNamespaceSnapshot(
                        12,
                        List.of(LinuxBlockfsFile.readOnly(inodeNumber, relativePath(manifest.path()), manifest.size()))
                )
        );
        new LinuxBlockfsManifestIndexStore().write(
                metadataRoot,
                new LinuxBlockfsManifestIndexSnapshot(
                        12,
                        List.of(LinuxBlockfsManifestEntry.fromManifest(inodeNumber, manifest))
                )
        );
        long fileOffset = 0;
        List<LinuxBlockfsPayloadAllocation> allocations = new java.util.ArrayList<>();
        for (int index = 0; index < manifest.chunks().size(); index++) {
            ChunkRef chunk = manifest.chunks().get(index);
            allocations.add(new LinuxBlockfsPayloadAllocation(
                    inodeNumber,
                    fileOffset,
                    1L + index,
                    0,
                    chunk.size()
            ));
            fileOffset = Math.addExact(fileOffset, chunk.size());
        }
        new LinuxBlockfsPayloadAllocationStore().write(
                metadataRoot,
                new LinuxBlockfsPayloadAllocationSnapshot(12, allocations)
        );
    }

    private static Path writeMissControlTool(
            long inodeNumber,
            int length,
            Path log,
            Path state
    ) throws Exception {
        Path script = log.getParent().resolve("cxnfs-blockfs-ctl-test");
        Files.writeString(script, """
                #!/bin/sh
                LOG='%s'
                STATE='%s'
                printf '%%s\\n' "$*" >> "$LOG"
                if [ "$3" = "next-miss" ]; then
                  if [ ! -f "$STATE" ]; then
                    printf '1\\n' > "$STATE"
                    printf 'miss_event event_id=1 reason=read inode=%d offset=0 length=%d dropped_events=0\\n'
                  else
                    printf 'miss_event=none errno=11\\n'
                  fi
                  exit 0
                fi
                if [ "$3" = "register-extent" ]; then
                  printf 'registered_extent inode=%d file_offset=0 payload_block=1 payload_offset=0 payload_length=%d\\n'
                  exit 0
                fi
                exit 2
                """.formatted(
                shellQuoteValue(log),
                shellQuoteValue(state),
                inodeNumber,
                length,
                inodeNumber,
                length
        ));
        assertTrue(script.toFile().setExecutable(true));
        return script;
    }

    private static String relativePath(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

    private static String shellQuoteValue(Path path) {
        return path.toAbsolutePath().normalize().toString().replace("'", "'\"'\"'");
    }

    private static void writeBlockfsMetadataRoot(Path metadataRoot) throws Exception {
        writeBlockfsMetadataRootWithoutExtentHints(metadataRoot);
        new LinuxBlockfsExtentHintStore().write(
                metadataRoot,
                new LinuxBlockfsExtentHintsSnapshot(
                        12,
                        List.of(new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)))
                )
        );
    }

    private static long countRegularFiles(Path directory) throws Exception {
        try (var paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private ServeRuntime writeServeRuntime(String name) throws Exception {
        Path runtimeRoot = tempDir.resolve("runtime-" + name);
        Files.createDirectories(runtimeRoot);
        Path keyStore = runtimeRoot.resolve("server.p12");
        Path passwordFile = runtimeRoot.resolve("server.p12.pass");
        generateKeyStore(keyStore, "secret".toCharArray());
        Files.writeString(passwordFile, "secret\n");
        return new ServeRuntime(
                keyStore,
                passwordFile,
                runtimeRoot.resolve("server.ready"),
                runtimeRoot.resolve("server.health"),
                runtimeRoot.resolve("server.lock"),
                runtimeRoot.resolve("server.stop")
        );
    }

    private static String serveRuntimeProperties(ServeRuntime runtime) {
        return """
                serve.host=127.0.0.1
                serve.control-port=0
                serve.transport-port=0
                serve.keystore=%s
                serve.keystore-password-file=%s
                serve.ready-file=%s
                serve.shutdown-file=%s
                serve.shutdown-poll-ms=10
                serve.daemon-lock-file=%s
                """
                .formatted(
                        runtime.keyStore(),
                        runtime.passwordFile(),
                        runtime.readyFile(),
                        runtime.shutdownFile(),
                        runtime.daemonLockFile()
                );
    }

    private static String runServeUntilReady(ServeRuntime runtime, String... args) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> server = executor.submit(() -> Cxnfs.run(args, new PrintStream(out), new PrintStream(err)));
        try {
            waitForReadyFileOrServerExit(runtime.readyFile(), server, out, err, Duration.ofSeconds(5));
            Files.writeString(runtime.shutdownFile(), "stop");
            assertEquals(0, server.get(5, TimeUnit.SECONDS), err.toString());
            return out.toString();
        } finally {
            if (!server.isDone()) {
                Files.writeString(runtime.shutdownFile(), "stop");
                server.cancel(true);
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void waitForReadyFileOrServerExit(
            Path path,
            Future<Integer> server,
            ByteArrayOutputStream out,
            ByteArrayOutputStream err,
            Duration timeout
    ) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) {
                return;
            }
            if (server.isDone()) {
                int exit = server.get();
                throw new AssertionError("server exited before ready file: exit=" + exit
                        + " stdout=" + out + " stderr=" + err);
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for file: " + path + " stderr=" + err);
    }

    private static String waitForFileText(Path path, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) {
                return Files.readString(path);
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for file: " + path);
    }

    private static void generateKeyStore(Path path, char[] password) throws Exception {
        try {
            Process process = new ProcessBuilder(
                    "keytool",
                    "-genkeypair",
                    "-alias",
                    "cxnfs-test",
                    "-keyalg",
                    "RSA",
                    "-storetype",
                    "PKCS12",
                    "-keystore",
                    path.toString(),
                    "-storepass",
                    String.valueOf(password),
                    "-keypass",
                    String.valueOf(password),
                    "-dname",
                    "CN=localhost",
                    "-validity",
                    "1",
                    "-noprompt"
            ).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException("keytool failed: " + output);
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static void writeServerHealthFile(Path path, String status, boolean healthy, Instant sampledAt)
            throws Exception {
        Files.writeString(
                path,
                """
                cxnfs.server.health.v1
                status=%s
                healthy=%s
                sampled_at=%s
                process_id=123
                served_directory=/srv/datasets
                control_listener=tls13-tcp://127.0.0.1:4509
                bulk_read_listener=tls13-bulk-stream://127.0.0.1:4510
                cipher_suite=tls_aes_128_gcm_sha256
                bulk_cipher_suite=tls_aes_128_gcm_sha256
                control_running=%s
                bulk_running=%s
                control_active_connections=0
                bulk_active_connections=0
                bulk_total_connections=0
                bulk_total_streams=0
                control_last_accept_error=none
                control_last_connection_error=none
                bulk_last_accept_error=none
                bulk_last_connection_error=none
                """.formatted(status, healthy, sampledAt, healthy, healthy),
                StandardCharsets.UTF_8
        );
    }

    private record ServeRuntime(
            Path keyStore,
            Path passwordFile,
            Path readyFile,
            Path healthFile,
            Path daemonLockFile,
            Path shutdownFile
    ) {
    }

    private static void writeBlockfsMetadataRootWithoutExtentHints(Path metadataRoot) throws Exception {
        new LinuxBlockfsNamespaceStore().write(
                metadataRoot,
                new LinuxBlockfsNamespaceSnapshot(
                        12,
                        List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096))
                )
        );
        new LinuxBlockfsPayloadAllocationStore().write(
                metadataRoot,
                new LinuxBlockfsPayloadAllocationSnapshot(
                        12,
                        List.of(new LinuxBlockfsPayloadAllocation(2, 0, 1, 0, 4096))
                )
        );
        new LinuxBlockfsManifestIndexStore().write(
                metadataRoot,
                new LinuxBlockfsManifestIndexSnapshot(
                        12,
                        List.of(new LinuxBlockfsManifestEntry(
                                2,
                                "datasets/train/sample.bin",
                                4096,
                                com.fortrow.cxnfs.hash.ContentHash.of(hash(9)),
                                List.of(new LinuxBlockfsManifestChunk(
                                        0,
                                        4096,
                                        com.fortrow.cxnfs.hash.ContentHash.of(hash(1))
                                ))
                        ))
                )
        );
    }

    private static void writeMountedBlockfsFixture(Path mountRoot, Path metadataRoot) throws Exception {
        byte[] data = "mounted blockfs data\n".repeat(128).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Path file = mountRoot.resolve("datasets/train/sample.bin");
        Files.createDirectories(file.getParent());
        Files.write(file, data);
        ContentHash chunkHash = Hashing.chunk(data);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(data.length));
        FileManifest manifest = new FileManifest(
                "/datasets/train/sample.bin",
                fileHash,
                data.length,
                List.of(new ChunkRef(chunkHash, data.length))
        );
        new LinuxBlockfsNamespaceStore().write(
                metadataRoot,
                new LinuxBlockfsNamespaceSnapshot(
                        12,
                        List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", data.length))
                )
        );
        new LinuxBlockfsManifestIndexStore().write(
                metadataRoot,
                new LinuxBlockfsManifestIndexSnapshot(
                        12,
                        List.of(LinuxBlockfsManifestEntry.fromManifest(2, manifest))
                )
        );
    }

    private static byte[] hash(int seed) {
        byte[] hash = new byte[32];
        hash[0] = (byte) seed;
        return hash;
    }
}
