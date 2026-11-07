package com.fortrow.cxnfs.packaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SystemdPackagingTest {
    @TempDir
    Path tempDir;

    @Test
    void serviceRunsBlockingDaemonWithReadinessAndShutdownFiles() throws Exception {
        String service = Files.readString(Path.of("packaging/systemd/cxnfs.service"));

        assertTrue(service.contains("EnvironmentFile=/etc/cxnfs/cxnfs.env"));
        assertTrue(service.contains("ExecStart=/usr/local/bin/cxnfs serve --config ${CXNFS_AUTH_CONFIG}"));
        assertFalse(service.contains("--ready-file ${CXNFS_READY_FILE}"));
        assertFalse(service.contains("--health-file ${CXNFS_HEALTH_FILE}"));
        assertFalse(service.contains("--health-interval-ms ${CXNFS_HEALTH_INTERVAL_MS}"));
        assertFalse(service.contains("--shutdown-file ${CXNFS_SHUTDOWN_FILE}"));
        assertFalse(service.contains("--daemon-lock-file ${CXNFS_DAEMON_LOCK_FILE}"));
        assertFalse(service.contains("--bulk-transport ${CXNFS_BULK_TRANSPORT}"));
        assertFalse(service.contains("--keystore-password-file ${CXNFS_KEYSTORE_PASSWORD_FILE}"));
        assertFalse(service.contains("--repository-root ${CXNFS_REPOSITORY_ROOT}"));
        assertTrue(service.contains("ExecStartPre=/bin/rm -f ${CXNFS_READY_FILE} ${CXNFS_HEALTH_FILE} ${CXNFS_SHUTDOWN_FILE} ${CXNFS_DAEMON_LOCK_FILE}"));
        assertTrue(service.contains("ExecStartPost=/usr/bin/timeout ${CXNFS_START_TIMEOUT_SECONDS}"));
        assertTrue(service.contains("until test -s \"$CXNFS_HEALTH_FILE\""));
        assertTrue(service.contains("server check-health ${CXNFS_HEALTH_FILE}"));
        assertTrue(service.contains("--max-age-ms ${CXNFS_HEALTH_MAX_AGE_MS}"));
        assertTrue(service.contains("--fail-on-unhealthy true"));
        assertTrue(service.contains("--report ${CXNFS_HEALTH_REPORT}"));
        assertTrue(service.contains("ExecReload=/bin/sh -c 'rm -f \"$CXNFS_READY_FILE\"; touch \"$CXNFS_SHUTDOWN_FILE\"'"));
        assertTrue(service.contains("ExecStop=/usr/bin/touch ${CXNFS_SHUTDOWN_FILE}"));
        assertTrue(service.contains("Restart=always"));
        assertTrue(service.contains("NoNewPrivileges=true"));
        assertTrue(service.contains("ProtectSystem=strict"));
        assertTrue(service.contains("ReadWritePaths=/run/cxnfs /var/lib/cxnfs /var/cache/cxnfs /var/log/cxnfs /srv"));
        assertFalse(service.contains("MemoryDenyWriteExecute=true"));
        assertFalse(service.contains("/usr/bin/java"));
        assertFalse(service.contains("-cp ${CXNFS_JAR}"));
        assertFalse(service.contains("--keystore-password "));
        assertFalse(service.contains("--password "));
    }

    @Test
    void packagedLaunchersWrapTheJavaEntryPoint() throws Exception {
        Path unixLauncher = Path.of("packaging/bin/cxnfs");
        Path windowsLauncher = Path.of("packaging/bin/cxnfs.cmd");
        String unix = Files.readString(unixLauncher);
        String windows = Files.readString(windowsLauncher);

        assertTrue(Files.isExecutable(unixLauncher));
        assertTrue(unix.contains("CXNFS_CLASSPATH=\"${CXNFS_HOME}/cxnfs.jar:${CXNFS_HOME}/lib/*\""));
        assertTrue(unix.contains("exec \"$JAVA_BIN\" ${CXNFS_JAVA_OPTS:-} -cp \"$CXNFS_CLASSPATH\" com.fortrow.cxnfs.Cxnfs \"$@\""));
        assertTrue(windows.contains("set \"CXNFS_CLASSPATH=%CXNFS_HOME%\\cxnfs.jar;%CXNFS_HOME%\\lib\\*\""));
        assertTrue(windows.contains("%CXNFS_JAVA_OPTS%"));
        assertTrue(windows.contains("\"%JAVA_BIN%\" -cp \"%CXNFS_CLASSPATH%\" com.fortrow.cxnfs.Cxnfs %*"));
        assertFalse(unix.contains("password"));
        assertFalse(windows.contains("password"));
    }

    @Test
    void platformInstallScriptsStagePrebuiltArtifactsAndRuntimePaths() throws Exception {
        Path linuxPath = Path.of("install-linux.sh");
        Path macosPath = Path.of("install-macos.sh");
        Path windowsPath = Path.of("install-windows.bat");
        String linux = Files.readString(linuxPath);
        String macos = Files.readString(macosPath);
        String windows = Files.readString(windowsPath);

        assertTrue(Files.isExecutable(linuxPath));
        assertTrue(Files.isExecutable(macosPath));
        assertTrue(linux.contains("target/cxnfs-0.1.0.jar"));
        assertTrue(linux.contains("target/runtime-libs"));
        assertTrue(linux.contains("target/native/cxnfs-fuse"));
        assertTrue(linux.contains("packaging/bin/cxnfs"));
        assertTrue(linux.contains("cxnfs-fuse"));
        assertTrue(linux.contains("/usr/local/bin"));
        assertTrue(linux.contains("/etc/cxnfs"));
        assertTrue(linux.contains("/run/cxnfs"));
        assertTrue(linux.contains("server.properties.example"));
        assertTrue(linux.contains("serve.ready-file=$RUN_DIR/server.ready"));
        assertTrue(linux.contains("serve.health-file=$RUN_DIR/server.health"));
        assertTrue(linux.contains("serve.shutdown-file=$RUN_DIR/server.stop"));
        assertTrue(linux.contains("serve.daemon-lock-file=$RUN_DIR/server.lock"));
        assertTrue(linux.contains("rm -f \"$(target_path \"$RUN_DIR/server.ready\")\""));
        assertTrue(linux.contains("secure_config_if_present"));
        assertTrue(linux.contains("chown root:\"$SERVICE_USER\""));
        assertTrue(linux.contains("own_served_root_dirs_if_present"));
        assertTrue(linux.contains("jflux-1.2.1-core.jar"));
        assertFalse(linux.contains("mvn "));

        assertTrue(macos.contains("/var/db/cxnfs"));
        assertTrue(macos.contains("/var/run/cxnfs"));
        assertTrue(macos.contains("/usr/local/srv/datasets"));
        assertTrue(macos.contains("/usr/local/srv/cxnfs-outputs"));
        assertTrue(macos.contains("packaging/bin/cxnfs"));
        assertTrue(macos.contains("target/native/cxnfs-fuse-t"));
        assertTrue(macos.contains("cxnfs-fuse-t"));
        assertTrue(macos.contains("serve.ready-file=$RUN_DIR/server.ready"));
        assertTrue(macos.contains("serve.shutdown-file=$RUN_DIR/server.stop"));
        assertTrue(macos.contains("jflux-1.2.1-core.jar"));
        assertFalse(macos.contains("mvn "));

        assertTrue(windows.contains("%ProgramFiles%\\CXNFS"));
        assertTrue(windows.contains("%ProgramData%\\CXNFS"));
        assertTrue(windows.contains("packaging\\bin\\cxnfs.cmd"));
        assertTrue(windows.contains("target\\cxnfs-0.1.0.jar"));
        assertTrue(windows.contains("target\\runtime-libs"));
        assertTrue(windows.contains("serve.ready-file=%CONFIG_DIR_PROP%/run/server.ready"));
        assertTrue(windows.contains("serve.health-file=%CONFIG_DIR_PROP%/run/server.health"));
        assertTrue(windows.contains("serve.shutdown-file=%CONFIG_DIR_PROP%/run/server.stop"));
        assertTrue(windows.contains("serve.daemon-lock-file=%CONFIG_DIR_PROP%/run/server.lock"));
        assertTrue(windows.contains("del /F /Q \"%CONFIG_DIR%\\run\\server.ready\""));
        assertTrue(windows.contains("jflux-1.2.1-core.jar"));
        assertFalse(windows.contains("mvn "));
    }

    @Test
    void environmentExampleReferencesFilesButDoesNotCarrySecrets() throws Exception {
        String env = Files.readString(Path.of("packaging/systemd/cxnfs.env.example"));

        assertTrue(env.contains("CXNFS_CLASSPATH=/opt/cxnfs/cxnfs.jar:/opt/cxnfs/lib/*"));
        assertTrue(env.contains("CXNFS_AUTH_CONFIG=/etc/cxnfs/server.properties"));
        assertTrue(env.contains("CXNFS_REPOSITORY_ROOT=/var/lib/cxnfs/repository"));
        assertTrue(env.contains("CXNFS_PAYLOAD_SPILL_DIR=/var/cache/cxnfs/payload-staging"));
        assertTrue(env.contains("CXNFS_READY_FILE=/run/cxnfs/server.ready"));
        assertTrue(env.contains("CXNFS_HEALTH_FILE=/run/cxnfs/server.health"));
        assertTrue(env.contains("CXNFS_HEALTH_MAX_AGE_MS=5000"));
        assertTrue(env.contains("CXNFS_HEALTH_REPORT=/var/log/cxnfs/server-health.txt"));
        assertTrue(env.contains("CXNFS_SHUTDOWN_FILE=/run/cxnfs/server.stop"));
        assertTrue(env.contains("CXNFS_DAEMON_LOCK_FILE=/run/cxnfs/server.lock"));
        assertTrue(env.contains("CXNFS_MAINTENANCE_DRY_RUN=false"));
        assertTrue(env.contains("CXNFS_MAINTENANCE_FAIL_ON_AUDIT_ISSUES=false"));
        assertTrue(env.contains("CXNFS_REPOSITORY_AUDIT_REPORT=/var/log/cxnfs/repository-audit.txt"));
        assertTrue(env.contains("CXNFS_REPOSITORY_MAINTENANCE_REPORT=/var/log/cxnfs/repository-maintenance.txt"));
        assertTrue(env.contains("CXNFS_REPOSITORY_ORPHAN_CONTAINER_MIN_AGE_MS=86400000"));
        assertTrue(env.contains("CXNFS_REPOSITORY_TEMP_FILE_MIN_AGE_MS=86400000"));
        assertTrue(env.contains("CXNFS_PAYLOAD_STAGING_MIN_AGE_MS=86400000"));
        assertTrue(env.contains("CXNFS_PAYLOAD_STAGING_CLEANUP_REPORT=/var/log/cxnfs/payload-staging-cleanup.txt"));
        assertFalse(env.contains("CXNFS_JAR="));
        assertFalse(env.contains("CXNFS_SERVE_ROOT="));
        assertFalse(env.contains("CXNFS_HOST="));
        assertFalse(env.contains("CXNFS_CONTROL_PORT="));
        assertFalse(env.contains("CXNFS_TRANSPORT_PORT="));
        assertFalse(env.contains("CXNFS_BULK_TRANSPORT="));
        assertFalse(env.contains("CXNFS_KEYSTORE="));
        assertFalse(env.contains("CXNFS_KEYSTORE_PASSWORD_FILE="));
        assertFalse(env.contains("=secret"));
        assertFalse(env.contains("auth.password"));
    }

    @Test
    void authenticationConfigExampleShowsReplaceableSecretsOnly() throws Exception {
        String auth = Files.readString(Path.of("packaging/systemd/server.properties.example"));

        assertTrue(auth.contains("serve.root.0=/datasets=/srv/datasets"));
        assertTrue(auth.contains("serve.root.1=/outputs=/srv/cxnfs-outputs"));
        assertTrue(auth.contains("serve.host=0.0.0.0"));
        assertTrue(auth.contains("serve.control-port=4509"));
        assertTrue(auth.contains("serve.transport-port=4510"));
        assertTrue(auth.contains("serve.bulk-transport=quic"));
        assertTrue(auth.contains("serve.payload-staging-memory-bytes=536870912"));
        assertTrue(auth.contains("serve.payload-spill-dir=/var/cache/cxnfs/payload-staging"));
        assertTrue(auth.contains("serve.repository-root=/var/lib/cxnfs/repository"));
        assertTrue(auth.contains("serve.keystore=/etc/cxnfs/server.p12"));
        assertTrue(auth.contains("serve.keystore-password-file=/etc/cxnfs/server.p12.pass"));
        assertTrue(auth.contains("serve.ready-file=/run/cxnfs/server.ready"));
        assertTrue(auth.contains("serve.health-file=/run/cxnfs/server.health"));
        assertTrue(auth.contains("serve.health-interval-ms=1000"));
        assertTrue(auth.contains("serve.shutdown-file=/run/cxnfs/server.stop"));
        assertTrue(auth.contains("serve.shutdown-poll-ms=250"));
        assertTrue(auth.contains("serve.daemon-lock-file=/run/cxnfs/server.lock"));
        assertFalse(auth.contains("serve.listen"));
        assertTrue(auth.contains("auth.mechanisms=username_password"));
        assertTrue(auth.contains("auth.user.trainer.password=REPLACE_WITH_LONG_RANDOM_PASSWORD"));
        assertTrue(auth.contains("auth.user.eval.password=REPLACE_WITH_DIFFERENT_LONG_RANDOM_PASSWORD"));
        assertTrue(auth.contains("auth.user.trainer.directories=/datasets,/outputs"));
        assertTrue(auth.contains("auth.user.eval.directories=/datasets"));
        assertTrue(auth.contains("auth.bearer-token.training=REPLACE_WITH_LONG_RANDOM_TOKEN"));
        assertFalse(auth.contains("change-me"));
        assertFalse(auth.contains("secret"));
    }

    @Test
    void maintenanceServiceRunsRepositoryAndPayloadCleanupWithoutSecrets() throws Exception {
        String service = Files.readString(Path.of("packaging/systemd/cxnfs-maintenance.service"));

        assertTrue(service.contains("Type=oneshot"));
        assertTrue(service.contains("User=cxnfs"));
        assertTrue(service.contains("Group=cxnfs"));
        assertTrue(service.contains("EnvironmentFile=/etc/cxnfs/cxnfs.env"));
        assertTrue(service.contains("Conflicts=cxnfs.service"));
        assertTrue(service.contains("OnSuccess=cxnfs.service"));
        assertTrue(service.contains("OnFailure=cxnfs.service"));
        assertTrue(service.contains("server audit-repository ${CXNFS_REPOSITORY_ROOT}"));
        assertTrue(service.contains("--fail-on-issues ${CXNFS_MAINTENANCE_FAIL_ON_AUDIT_ISSUES}"));
        assertTrue(service.contains("--report ${CXNFS_REPOSITORY_AUDIT_REPORT}"));
        assertTrue(service.contains("server compact-repository ${CXNFS_REPOSITORY_ROOT}"));
        assertTrue(service.contains("--dry-run ${CXNFS_MAINTENANCE_DRY_RUN}"));
        assertTrue(service.contains("--orphan-container-min-age-ms ${CXNFS_REPOSITORY_ORPHAN_CONTAINER_MIN_AGE_MS}"));
        assertTrue(service.contains("--temp-file-min-age-ms ${CXNFS_REPOSITORY_TEMP_FILE_MIN_AGE_MS}"));
        assertTrue(service.contains("--report ${CXNFS_REPOSITORY_MAINTENANCE_REPORT}"));
        assertTrue(service.contains("server clean-payload-staging ${CXNFS_PAYLOAD_SPILL_DIR}"));
        assertTrue(service.contains("--min-age-ms ${CXNFS_PAYLOAD_STAGING_MIN_AGE_MS}"));
        assertTrue(service.contains("--report ${CXNFS_PAYLOAD_STAGING_CLEANUP_REPORT}"));
        assertTrue(service.contains("ProtectSystem=strict"));
        assertTrue(service.contains("ReadWritePaths=/var/lib/cxnfs /var/cache/cxnfs /var/log/cxnfs"));
        assertFalse(service.contains("MemoryDenyWriteExecute=true"));
        assertFalse(service.contains("/usr/bin/java"));
        assertFalse(service.contains("-cp ${CXNFS_JAR}"));
        assertFalse(service.contains("--password "));
        assertFalse(service.contains("--keystore-password "));
    }

    @Test
    void maintenanceTimerSchedulesPersistentJitteredRuns() throws Exception {
        String timer = Files.readString(Path.of("packaging/systemd/cxnfs-maintenance.timer"));

        assertTrue(timer.contains("OnCalendar=*-*-* 03:30:00"));
        assertTrue(timer.contains("RandomizedDelaySec=30min"));
        assertTrue(timer.contains("Persistent=true"));
        assertTrue(timer.contains("Unit=cxnfs-maintenance.service"));
        assertTrue(timer.contains("WantedBy=timers.target"));
    }

    @Test
    void tmpfilesCreatesLeastPrivilegeRuntimeAndStateDirectories() throws Exception {
        String tmpfiles = Files.readString(Path.of("packaging/systemd/cxnfs.tmpfiles"));

        assertTrue(tmpfiles.contains("d /etc/cxnfs 0750 root cxnfs -"));
        assertTrue(tmpfiles.contains("d /var/lib/cxnfs 0750 cxnfs cxnfs -"));
        assertTrue(tmpfiles.contains("d /var/cache/cxnfs 0750 cxnfs cxnfs -"));
        assertTrue(tmpfiles.contains("d /var/log/cxnfs 0750 cxnfs cxnfs -"));
        assertTrue(tmpfiles.contains("d /run/cxnfs 0750 cxnfs cxnfs -"));
    }

    @Test
    void sysusersDeclaresDedicatedServiceAccount() throws Exception {
        String sysusers = Files.readString(Path.of("packaging/systemd/cxnfs.sysusers"));

        assertTrue(sysusers.contains("u cxnfs - \"CXNFS file server\" /var/lib/cxnfs /usr/sbin/nologin"));
    }

    @Test
    void docsExposeProductionDeploymentRunbook() throws Exception {
        String sidebar = Files.readString(Path.of("docs/sidebars.js"));
        String doc = Files.readString(Path.of("docs/docs/ops/deployment.md"));

        assertTrue(sidebar.contains("label: 'Operations'"));
        assertTrue(sidebar.contains("'ops/deployment'"));
        assertTrue(doc.contains("CXNFS_CLASSPATH=/opt/cxnfs/cxnfs.jar:/opt/cxnfs/lib/*"));
        assertTrue(doc.contains("sudo ./install-linux.sh --jar target/cxnfs-0.1.0.jar --lib-dir target/runtime-libs"));
        assertTrue(doc.contains("sudo ./install-macos.sh --jar target/cxnfs-0.1.0.jar --lib-dir target/runtime-libs"));
        assertTrue(doc.contains("install-windows.bat --jar target\\cxnfs-0.1.0.jar --lib-dir target\\runtime-libs"));
        assertTrue(doc.contains("clear stale ready, health, shutdown"));
        assertTrue(doc.contains("install -m 0755 packaging/bin/cxnfs /usr/local/bin/cxnfs"));
        assertTrue(doc.contains("install -m 0755 target/native/cxnfs-fuse /usr/local/bin/cxnfs-fuse"));
        assertTrue(doc.contains("install -m 0755 target/native/cxnfs-fuse-t /usr/local/bin/cxnfs-fuse-t"));
        assertTrue(doc.contains("cxnfs install-service"));
        assertTrue(doc.contains("JFlux `PlatformUtils`"));
        assertTrue(doc.contains("packaging/systemd/server.properties.example"));
        assertTrue(doc.contains("/run/cxnfs/server.ready"));
        assertTrue(doc.contains("/run/cxnfs/server.health"));
        assertTrue(doc.contains("/run/cxnfs/server.lock"));
        assertTrue(doc.contains("/var/log/cxnfs/server-health.txt"));
        assertTrue(doc.contains("/var/log/cxnfs/repository-audit.txt"));
        assertTrue(doc.contains("/var/log/cxnfs/repository-maintenance.txt"));
        assertTrue(doc.contains("/var/log/cxnfs/payload-staging-cleanup.txt"));
        assertTrue(doc.contains("Certificates And Secrets"));
        assertTrue(doc.contains("Serving A Dataset Safely"));
        assertTrue(doc.contains("journalctl -u cxnfs.service"));
        assertTrue(doc.contains("packaging/release-check.sh"));
        assertTrue(doc.contains("packaging/live-quic-check.sh"));
        assertFalse(doc.contains("change-me"));
    }

    @Test
    void releaseCheckScriptRunsCrossStackProductionGate() throws Exception {
        Path script = Path.of("packaging/release-check.sh");
        String text = Files.readString(script);

        assertTrue(Files.isExecutable(script));
        assertTrue(text.contains("run \"$MVN_BIN\" -q test"));
        assertTrue(text.contains("run \"$MVN_BIN\" -q -DskipTests package"));
        assertTrue(text.contains("dependency:build-classpath"));
        assertTrue(text.contains("cxnfs-runtime-classpath.txt"));
        assertTrue(text.contains("server check-health \"$REPORT_DIR/server.health.fixture\""));
        assertTrue(text.contains("--report \"$REPORT_DIR/server-health-check.txt\""));
        assertTrue(text.contains("server health-metrics \"$REPORT_DIR/server.health.fixture\""));
        assertTrue(text.contains("server-health-metrics.prom"));
        assertTrue(text.contains("cxnfs_server_health_check_passed 1"));
        assertTrue(text.contains("bulk_cipher_suite=tls_aes_128_gcm_sha256"));
        assertTrue(text.contains("bulk_cipher_suite=\\\"tls_aes_128_gcm_sha256\\\""));
        assertTrue(text.contains("write_mount_handoff_fixture()"));
        assertTrue(text.contains("client mount-launch-plan \"$REPORT_DIR/cxnfs-mount.handoff\""));
        assertTrue(text.contains("mount-launch-plan.txt"));
        assertTrue(text.contains("cxnfs.mount.launch-plan.v1"));
        assertTrue(text.contains("mount-list-help.out"));
        assertTrue(text.contains("cxnfs client mount-list <handoff-report>"));
        assertTrue(text.contains("require_file_contains()"));
        assertTrue(text.contains("com.fortrow.cxnfs.CxnfsBenchmark"));
        assertTrue(text.contains("java-benchmark-read.txt"));
        assertTrue(text.contains("java-benchmark-compare.out"));
        assertTrue(text.contains("java-benchmark-compare.txt"));
        assertTrue(text.contains("--objective ai-training-25gbe"));
        assertTrue(text.contains("--fail-on-miss false"));
        assertTrue(text.contains("candidate_vs_baseline"));
        assertTrue(text.contains("cxnfs.benchmark.compare.v1"));
        assertTrue(text.contains("objective_baseline=legacy-mounted-share"));
        assertTrue(text.contains("objective_passed="));
        assertTrue(text.contains("write_blockfs_metadata_fixture()"));
        assertTrue(text.contains("server build-blockfs-extent-hints \"$REPORT_DIR/blockfs-metadata-root\""));
        assertTrue(text.contains("blockfs-extent-hints.txt"));
        assertTrue(text.contains("status=built"));
        assertTrue(text.contains("server audit-blockfs-metadata \"$REPORT_DIR/blockfs-metadata-root\""));
        assertTrue(text.contains("blockfs-metadata-audit.txt"));
        assertTrue(text.contains("status=clean"));
        assertTrue(text.contains("server sync-blockfs-namespace \"$REPORT_DIR/blockfs-metadata-root\""));
        assertTrue(text.contains("blockfs-namespace-sync.txt"));
        assertTrue(text.contains("status=synced"));
        assertTrue(text.contains("protocol fixture-report"));
        assertTrue(text.contains("--fail-on-missing true"));
        assertTrue(text.contains("python/src"));
        assertTrue(text.contains("PYTHON_SRC_PATH=\"$ROOT_DIR/python/src\""));
        assertTrue(text.contains("PYTHON_RUN_PATH=\"$PYTHON_SRC_PATH\""));
        assertTrue(text.contains("CXNFS_PYTHONPATH_EXTRA"));
        assertTrue(text.contains("PYTHONPATH=\"$PYTHON_RUN_PATH\""));
        assertTrue(text.contains("-m unittest discover"));
        assertTrue(text.contains("cxnfs-client = \"cxnfs.client_cli:main\""));
        assertTrue(text.contains("cxnfs-health = \"cxnfs.health:main\""));
        assertTrue(text.contains("cxnfs-trainer-network-preflight = \"cxnfs.torch_dataset:main_trainer_network_preflight\""));
        assertTrue(text.contains("cxnfs-trainer-integration-smoke = \"cxnfs.torch_dataset:main_trainer_integration_smoke\""));
        assertTrue(text.contains("cxnfs-trainer-pipeline-smoke = \"cxnfs.torch_dataset:main_trainer_pipeline_smoke\""));
        assertTrue(text.contains("check_python_quic_prerequisites()"));
        assertTrue(text.contains("aioquic"));
        assertTrue(text.contains("CXNFS_REQUIRE_QUIC_INTEGRATION"));
        assertTrue(text.contains("CXNFS_QUIC_INTEGRATION_REPORT_DIR"));
        assertTrue(text.contains("python.tests.test_quic_java_integration"));
        assertTrue(text.contains("python-quic-trainer-stream-benchmark.json"));
        assertTrue(text.contains("python-quic-trainer-workers.json"));
        assertTrue(text.contains("\"transport\": \"cxnfs-quic\""));
        assertTrue(text.contains("python-quic-trainer-workload-benchmark.json"));
        assertTrue(text.contains("python-quic-trainer-contract-workload-benchmark.json"));
        assertTrue(text.contains("\"bulk_transport\": \"quic\""));
        assertTrue(text.contains("\"tls_version\": \"TLSv1.3\""));
        assertTrue(text.contains("\"cipher_suite\": \"TLS_AES_128_GCM_SHA256\""));
        assertTrue(text.contains("\"trainer_batch_iterator\": \"trainer-dataloader\""));
        assertTrue(text.contains("\"data_loader_workers\": 2"));
        assertTrue(text.contains("\"worker_batch_read_evidence_observed\": true"));
        assertTrue(text.contains("\"produced_inside_torch_worker\": true"));
        assertTrue(text.contains("\"format\": \"cxnfs.trainer-integration-plan.v1\""));
        assertTrue(text.contains("\"stream_factory\": \"open_weighted_mds_trainer_stream\""));
        assertTrue(text.contains("\"remote_client_factory\": \"CxnfsTrainerIntegrationPlan.create_remote_client_factory\""));
        assertTrue(text.contains("\"remote_client_required\": true"));
        assertTrue(text.contains("\"raw_password_sent_over_control\": false"));
        assertTrue(text.contains("python-quic-trainer-integration-smoke.json"));
        assertTrue(text.contains("python-quic-trainer-loop-replay.json"));
        assertTrue(text.contains("cxnfs.trainer-loop-replay.v1"));
        assertTrue(text.contains("python-quic-trainer-readiness-bundle.json"));
        assertTrue(text.contains("cxnfs.trainer-readiness-bundle.v1"));
        assertTrue(text.contains("\"direct_python_streaming_verified\": true"));
        assertTrue(text.contains("\"distributed_rank_states_written\": true"));
        assertTrue(text.contains("\"resume_continuity_verified\": true"));
        assertTrue(text.contains("\"weighted_source_coverage_complete\": true"));
        assertTrue(text.contains("python-trainer-integration-smoke.json"));
        assertTrue(text.contains("\"format\": \"cxnfs.trainer-integration-smoke.v1\""));
        assertTrue(text.contains("python-trainer-integration-smoke-help.txt"));
        assertTrue(text.contains("python-trainer-network-preflight-help.txt"));
        assertTrue(text.contains("--control-port"));
        assertTrue(text.contains("--transport-port"));
        assertTrue(text.contains("--bulk-write-retries"));
        assertTrue(text.contains("require_file_not_contains \"$REPORT_DIR/python-trainer-integration-smoke-help.txt\" \"--password\""));
        assertTrue(text.contains("\"stream_opened\": true"));
        assertTrue(text.contains("\"stream_kwargs_accepted\": true"));
        assertTrue(text.contains("\"batch_object\": \"CxnfsTrainerBatch\""));
        assertTrue(text.contains("python-trainer-loop-replay-help.txt"));
        assertTrue(text.contains("python-trainer-loop-replay.json"));
        assertTrue(text.contains("cxnfs.trainer-loop-replay.v1"));
        assertTrue(text.contains("python-trainer-loop-replay-resume-state.json"));
        assertTrue(text.contains("python-trainer-loop-replay-final-state.json"));
        assertTrue(text.contains("require_file_not_contains \"$REPORT_DIR/python-trainer-loop-replay-help.txt\" \"--password\""));
        assertTrue(text.contains("\"resume_continuity_verified\": true"));
        assertTrue(text.contains("\"weighted_source_coverage_complete\": true"));
        assertTrue(text.contains("\"contract_applied\": true"));
        assertTrue(text.contains("\"read_path_evidence\""));
        assertTrue(text.contains("\"workload_fingerprint\""));
        assertTrue(text.contains("\"format\": \"cxnfs.trainer-tensor-memory-path.v1\""));
        assertTrue(text.contains("\"tensor_memory_path_complete\": true"));
        assertTrue(text.contains("\"python_byte_batch_avoided\": true"));
        assertTrue(text.contains("\"network_fills_tensor_destination\": true"));
        assertTrue(text.contains("\"device_copy_required\": false"));
        assertTrue(text.contains("\"transport_frame_sink_observed\": true"));
        assertTrue(text.contains("\"frame_sink_used_for_all_direct_destination_reads\": true"));
        assertTrue(text.contains("require_trainer_direct_destination_fanout"));
        assertTrue(text.contains("\"direct_destination_frame_sink_cord_fanout_verified\": true"));
        assertTrue(text.contains("\"direct_destination_max_observed_cords\": 64"));
        assertTrue(text.contains("\"direct_destination_parallel_frame_sink_read_operations\": 1"));
        assertTrue(text.contains("\"direct_destination_parallel_frame_sink_frames\":"));
        assertTrue(text.contains("python-client-cli-quic-smoke.json"));
        assertTrue(text.contains("\"format\": \"cxnfs.python-client-cli-quic-smoke.v1\""));
        assertTrue(text.contains("\"read_max_observed_cords\": 4"));
        assertTrue(text.contains("\"write_max_observed_cords\": 4"));
        assertTrue(text.contains("\"read_back_verified\": true"));
        assertTrue(text.contains("\"direct_file_write_verified\": true"));
        assertTrue(text.contains("\"direct_write_max_observed_cords\": 4"));
        assertTrue(text.contains("\"direct_write_cipher_suite\": \"TLS_AES_128_GCM_SHA256\""));
        assertTrue(text.contains("\"raw_password_visible\": false"));
        assertTrue(text.contains("python-quic-batch-direct-read.json"));
        assertTrue(text.contains("\"format\": \"cxnfs.quic-batch-direct-read.v1\""));
        assertTrue(text.contains("\"batch_read_count\": 2"));
        assertTrue(text.contains("\"batch_worker_count\": 2"));
        assertTrue(text.contains("\"ordered_batch_read_verified\": true"));
        assertTrue(text.contains("\"direct_buffer_read_operations\": 2"));
        assertTrue(text.contains("\"direct_destination_read_operations\": 2"));
        assertTrue(text.contains("\"transport_frame_sink_observed\": true"));
        assertTrue(text.contains("\"direct_destination_frame_sink_cord_fanout_verified\": true"));
        assertTrue(text.contains("python-quic-batch-direct-tensor-read.json"));
        assertTrue(text.contains("\"format\": \"cxnfs.quic-batch-direct-tensor-read.v1\""));
        assertTrue(text.contains("\"tensor_target\": \"pytorch-cpu\""));
        assertTrue(text.contains("\"tensor_read_count\": 2"));
        assertTrue(text.contains("\"tensor_worker_count\": 2"));
        assertTrue(text.contains("\"ordered_tensor_read_verified\": true"));
        assertTrue(text.contains("\"direct_tensor_read_operations\": 2"));
        assertTrue(text.contains("\"network_fills_tensor_destination\": true"));
        assertTrue(text.contains("\"max_observed_cords\": 2"));
        assertTrue(text.contains("--batch-direct-read-report \"$REPORT_DIR/python-quic-batch-direct-read.json\""));
        assertTrue(text.contains("--batch-direct-tensor-read-report \"$REPORT_DIR/python-quic-batch-direct-tensor-read.json\""));
        assertTrue(text.contains("--require-low-level-direct-read-proofs"));
        assertTrue(text.contains("\"format\": \"cxnfs.trainer-low-level-direct-read-proofs.v1\""));
        assertTrue(text.contains("low_level_direct_read_proofs_complete"));
        assertTrue(text.contains("batch_direct_read_proof_complete"));
        assertTrue(text.contains("batch_direct_tensor_read_proof_complete"));
        assertTrue(text.contains("\"batch_direct_read_cipher_suite\": \"TLS_AES_128_GCM_SHA256\""));
        assertTrue(text.contains("direct_quic_memory_path_verified"));
        assertTrue(text.contains("python-quic-workload-decision.json"));
        assertTrue(text.contains("python-quic-trainer-contract-surface-matrix.json"));
        assertTrue(text.contains("--input-workload-report \"$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json\""));
        assertTrue(text.contains("--input-matrix-report \"$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json\""));
        assertTrue(text.contains("--surface-contract \"$REPORT_DIR/python-quic-trainer-contract.json\""));
        assertTrue(text.contains("--surface-require-matrix"));
        assertTrue(text.contains("quic-workload-decision/decision.report"));
        assertTrue(text.contains("trainer_surface_matrix_passed=true"));
        assertTrue(text.contains("trainer_surface_check_passed=true"));
        assertTrue(text.contains("java_python_network_path_exercised=true"));
        assertTrue(text.contains("-m cxnfs.client_cli --help"));
        assertTrue(text.contains("python-client-help.txt"));
        assertTrue(text.contains("read-mds"));
        assertTrue(text.contains("send-checkpoint"));
        assertTrue(text.contains("send-file"));
        assertTrue(text.contains("require_file_not_contains \"$REPORT_DIR/python-client-help.txt\" \"--password\""));
        assertTrue(text.contains("-m cxnfs.benchmark --help"));
        assertTrue(text.contains("python-benchmark-help.txt"));
        assertTrue(text.contains("--baseline-mb-s"));
        assertTrue(text.contains("--fail-on-miss"));
        assertTrue(text.contains("write_workload_evidence_proc_fixture()"));
        assertTrue(text.contains("collect_workload_evidence_fixture()"));
        assertTrue(text.contains("-m cxnfs.workload_evidence --help"));
        assertTrue(text.contains("python-workload-evidence-help.txt"));
        assertTrue(text.contains("python-workload-evidence.out"));
        assertTrue(text.contains("--proc-root \"$REPORT_DIR/workload-evidence-proc\""));
        assertTrue(text.contains("--disk-device vda"));
        assertTrue(text.contains("cxnfs.workload-evidence.collect.v1"));
        assertTrue(text.contains("cxnfs.server.disk.v1"));
        assertTrue(text.contains("cxnfs.server.cache.v1"));
        assertTrue(text.contains("cxnfs.server.cpu.v1"));
        assertTrue(text.contains("python-trainer-contract-help.txt"));
        assertTrue(text.contains("python-trainer-contract.json"));
        assertTrue(text.contains("cxnfs.trainer-contract.v1"));
        assertTrue(text.contains("requires_mount"));
        assertTrue(text.contains("touches_trainer_code"));
        assertTrue(text.contains("raw_password_sent_over_control"));
        assertTrue(text.contains("workload_fingerprint"));
        assertTrue(text.contains("python-trainer-surface-check-help.txt"));
        assertTrue(text.contains("run_trainer_surface_check_cli"));
        assertTrue(text.contains("--require-quic"));
        assertTrue(text.contains("--min-workload-samples"));
        assertTrue(text.contains("python-trainer-contract-benchmark-help.txt"));
        assertTrue(text.contains("python-trainer-contract-workload-benchmark.json"));
        assertTrue(text.contains("contract_applied"));
        assertTrue(text.contains("trainer_contract"));
        assertTrue(text.contains("python-trainer-surface-check.json"));
        assertTrue(text.contains("cxnfs.trainer-surface-check.v1"));
        assertTrue(text.contains("workload_fingerprint_matches_contract"));
        assertTrue(text.contains("--min-sources 2"));
        assertTrue(text.contains("--min-workload-runs 4"));
        assertTrue(text.contains("python-trainer-stream-help.txt"));
        assertTrue(text.contains("python-trainer-stream-smoke.json"));
        assertTrue(text.contains("cxnfs.trainer-stream-smoke.v1"));
        assertTrue(text.contains("python-trainer-pipeline-help.txt"));
        assertTrue(text.contains("python-trainer-pipeline-smoke.json"));
        assertTrue(text.contains("cxnfs.trainer-pipeline-smoke.v1"));
        assertTrue(text.contains("run_trainer_pipeline_smoke_cli"));
        assertTrue(text.contains("\"training_pipeline_object\": \"CxnfsTrainingPipeline\""));
        assertTrue(text.contains("\"pipeline_descriptor_planning_used\": true"));
        assertTrue(text.contains("python-trainer-workload-help.txt"));
        assertTrue(text.contains("--cord-sweep"));
        assertTrue(text.contains("--server-disk-evidence"));
        assertTrue(text.contains("workload-evidence/disk.report"));
        assertTrue(text.contains("python-trainer-workload-benchmark.json"));
        assertTrue(text.contains("cxnfs.trainer-workload-benchmark.v1"));
        assertTrue(text.contains("\"run_count\": 4"));
        assertTrue(text.contains("server_evidence"));
        assertTrue(text.contains("\"complete\": true"));
        assertTrue(text.contains("blockfs_gate"));
        assertTrue(text.contains("kernel_module_allowed"));
        assertTrue(text.contains("-m cxnfs.workload_decision --help"));
        assertTrue(text.contains("python-workload-decision-help.txt"));
        assertTrue(text.contains("python-workload-decision.json"));
        assertTrue(text.contains("--collect-local-server-evidence"));
        assertTrue(text.contains("--surface-min-workload-samples"));
        assertTrue(text.contains("--contract \"$REPORT_DIR/python-trainer-contract.json\""));
        assertTrue(text.contains("cxnfs.workload-decision.v1"));
        assertTrue(text.contains("trainer_surface_check_passed"));
        assertTrue(text.contains("workload-decision/decision.report"));
        assertTrue(text.contains("trainer_surface_verified=true"));
        assertTrue(text.contains("trainer_surface_check_passed=true"));
        assertTrue(text.contains("workload-decision/trainer-surface-check.json"));
        assertTrue(text.contains("workload-decision/server-evidence/disk.report"));
        assertTrue(text.contains("-m cxnfs.mount_handoff --help"));
        assertTrue(text.contains("python-mount-help.txt"));
        assertTrue(text.contains("negotiate"));
        assertTrue(text.contains("python-mount-launch-plan.out"));
        assertTrue(text.contains("python-mount-launch-plan.txt"));
        assertTrue(text.contains("-m cxnfs.protocol_report"));
        assertTrue(text.contains("run \"$CMAKE_BIN\" -S"));
        assertTrue(text.contains("--target module"));
        assertTrue(text.contains("--target tool-smoke"));
        assertTrue(text.contains("tool-smoke-reports/mkfs-force.report"));
        assertTrue(text.contains("cxnfs.blockfs.mkfs.v1"));
        assertTrue(text.contains("tool-smoke-reports/fsck-force.report"));
        assertTrue(text.contains("cxnfs.blockfs.fsck.v1"));
        assertFalse(text.contains("--password "));
        assertFalse(text.contains("--keystore-password "));
    }

    @Test
    void pythonPackageDeclaresTrainerIntegrationSmokeEntrypoint() throws Exception {
        String pyproject = Files.readString(Path.of("python/pyproject.toml"));

        assertTrue(pyproject.contains("[project.scripts]"));
        assertTrue(pyproject.contains("cxnfs-client = \"cxnfs.client_cli:main\""));
        assertTrue(pyproject.contains("cxnfs-trainer-integration-smoke = \"cxnfs.torch_dataset:main_trainer_integration_smoke\""));
        assertTrue(pyproject.contains("cxnfs-trainer-loop-replay = \"cxnfs.torch_dataset:main_trainer_loop_replay\""));
        assertTrue(pyproject.contains("cxnfs-trainer-pipeline-smoke = \"cxnfs.torch_dataset:main_trainer_pipeline_smoke\""));
        assertTrue(pyproject.contains("cxnfs-trainer-handoff = \"cxnfs.torch_dataset:main_trainer_handoff\""));
        assertTrue(pyproject.contains("cxnfs-trainer-network-preflight = \"cxnfs.torch_dataset:main_trainer_network_preflight\""));
        assertTrue(pyproject.contains("cxnfs-trainer-handoff-acceptance = \"cxnfs.torch_dataset:main_trainer_handoff_acceptance\""));
        assertTrue(pyproject.contains("cxnfs-trainer-workload-benchmark = \"cxnfs.torch_dataset:main_trainer_workload_benchmark\""));
        assertFalse(pyproject.contains("--password"));
    }

    @Test
    void releaseCheckDryRunPrintsCommandsWithoutRunningTools() throws Exception {
        Path reportDir = tempDir.resolve("release-reports");
        Path outputPath = tempDir.resolve("release-check-dry-run.out");
        Process process = new ProcessBuilder(
                "packaging/release-check.sh",
                "--dry-run",
                "true",
                "--report-dir",
                reportDir.toString()
        )
                .redirectErrorStream(true)
                .redirectOutput(outputPath.toFile())
                .start();

        boolean finished = process.waitFor(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(finished, "release-check dry run should finish promptly");

        String text = Files.readString(outputPath);
        assertTrue(text.contains("mvn -q test"));
        assertTrue(text.contains("mvn -q -DskipTests package"));
        assertTrue(text.contains("dependency:build-classpath"));
        assertTrue(text.contains("cxnfs-runtime-classpath.txt"));
        assertTrue(text.contains("server check-health"));
        assertTrue(text.contains("server-health-check.txt"));
        assertTrue(text.contains("server health-metrics"));
        assertTrue(text.contains("server-health-metrics.prom"));
        assertTrue(text.contains("client mount-launch-plan"));
        assertTrue(text.contains("mount-launch-plan.txt"));
        assertTrue(text.contains("mount-list-help.out"));
        assertTrue(text.contains("CxnfsBenchmark"));
        assertTrue(text.contains("java-benchmark-read.txt"));
        assertTrue(text.contains("java-benchmark-compare.out"));
        assertTrue(text.contains("java-benchmark-compare.txt"));
        assertTrue(text.contains("--objective ai-training-25gbe"));
        assertTrue(text.contains("require"));
        assertTrue(text.contains("candidate_vs_baseline"));
        assertTrue(text.contains("cxnfs.benchmark.compare.v1"));
        assertTrue(text.contains("objective_baseline=legacy-mounted-share"));
        assertTrue(text.contains("objective_passed="));
        assertTrue(text.contains("blockfs-metadata-root"));
        assertTrue(text.contains("server build-blockfs-extent-hints"));
        assertTrue(text.contains("blockfs-extent-hints.txt"));
        assertTrue(text.contains("status=built"));
        assertTrue(text.contains("server audit-blockfs-metadata"));
        assertTrue(text.contains("blockfs-metadata-audit.txt"));
        assertTrue(text.contains("status=clean"));
        assertTrue(text.contains("server sync-blockfs-namespace"));
        assertTrue(text.contains("blockfs-namespace-sync.txt"));
        assertTrue(text.contains("status=synced"));
        assertTrue(text.contains("protocol fixture-report"));
        assertTrue(text.contains("-m unittest discover"));
        assertTrue(text.contains("python/pyproject.toml"));
        assertTrue(text.contains("cxnfs-trainer-integration-smoke"));
        assertTrue(text.contains("cxnfs-trainer-pipeline-smoke"));
        assertTrue(text.contains("main_trainer_integration_smoke"));
        assertTrue(text.contains("CXNFS_REQUIRE_QUIC_INTEGRATION"));
        assertTrue(text.contains("CXNFS_QUIC_INTEGRATION_REPORT_DIR"));
        assertTrue(text.contains("python.tests.test_quic_java_integration"));
        assertTrue(text.contains("-m cxnfs.client_cli --help"));
        assertTrue(text.contains("python-client-help.txt"));
        assertTrue(text.contains("read-mds"));
        assertTrue(text.contains("send-checkpoint"));
        assertTrue(text.contains("send-file"));
        assertTrue(text.contains("-m cxnfs.benchmark --help"));
        assertTrue(text.contains("python-benchmark-help.txt"));
        assertTrue(text.contains("--baseline-mb-s"));
        assertTrue(text.contains("--fail-on-miss"));
        assertTrue(text.contains("workload-evidence-proc"));
        assertTrue(text.contains("-m cxnfs.workload_evidence --help"));
        assertTrue(text.contains("python-workload-evidence-help.txt"));
        assertTrue(text.contains("python-workload-evidence.out"));
        assertTrue(text.contains("--proc-root"));
        assertTrue(text.contains("--disk-device"));
        assertTrue(text.contains("cxnfs.workload-evidence.collect.v1"));
        assertTrue(text.contains("cxnfs.server.disk.v1"));
        assertTrue(text.contains("cxnfs.server.cache.v1"));
        assertTrue(text.contains("cxnfs.server.cpu.v1"));
        assertTrue(text.contains("python-trainer-contract-help.txt"));
        assertTrue(text.contains("python-trainer-contract.json"));
        assertTrue(text.contains("cxnfs.trainer-contract.v1"));
        assertTrue(text.contains("requires_mount"));
        assertTrue(text.contains("touches_trainer_code"));
        assertTrue(text.contains("raw_password_sent_over_control"));
        assertTrue(text.contains("python-trainer-contract-benchmark-help.txt"));
        assertTrue(text.contains("python-trainer-contract-workload-benchmark.json"));
        assertTrue(text.contains("contract_applied"));
        assertTrue(text.contains("trainer_contract"));
        assertTrue(text.contains("python-trainer-stream-help.txt"));
        assertTrue(text.contains("--resume-cursor"));
        assertTrue(text.contains("--write-resume-cursor"));
        assertTrue(text.contains("--resume-state"));
        assertTrue(text.contains("--write-resume-state"));
        assertTrue(text.contains("python-trainer-stream-smoke.json"));
        assertTrue(text.contains("cxnfs.trainer-stream-smoke.v1"));
        assertTrue(text.contains("python-trainer-pipeline-smoke.json"));
        assertTrue(text.contains("cxnfs.trainer-pipeline-smoke.v1"));
        assertTrue(text.contains("python-trainer-workload-help.txt"));
        assertTrue(text.contains("--cord-sweep"));
        assertTrue(text.contains("--server-disk-evidence"));
        assertTrue(text.contains("workload-evidence/disk.report"));
        assertTrue(text.contains("python-trainer-workload-benchmark.json"));
        assertTrue(text.contains("cxnfs.trainer-workload-benchmark.v1"));
        assertTrue(text.contains("server_evidence"));
        assertTrue(text.contains("complete"));
        assertTrue(text.contains("blockfs_gate"));
        assertTrue(text.contains("kernel_module_allowed"));
        assertTrue(text.contains("-m cxnfs.workload_decision --help"));
        assertTrue(text.contains("python-workload-decision-help.txt"));
        assertTrue(text.contains("python-workload-decision.json"));
        assertTrue(text.contains("--collect-local-server-evidence"));
        assertTrue(text.contains("--contract"));
        assertTrue(text.contains("cxnfs.workload-decision.v1"));
        assertTrue(text.contains("workload-decision/decision.report"));
        assertTrue(text.contains("trainer_surface_verified=true"));
        assertTrue(text.contains("workload-decision/server-evidence/disk.report"));
        assertTrue(text.contains("-m cxnfs.mount_handoff --help"));
        assertTrue(text.contains("python-mount-help.txt"));
        assertTrue(text.contains("negotiate"));
        assertTrue(text.contains("python-mount-launch-plan.out"));
        assertTrue(text.contains("python-mount-launch-plan.txt"));
        assertTrue(text.contains("-m cxnfs.protocol_report"));
        assertTrue(text.contains("--target module") || text.contains("blockfs module build skipped on"));
        assertTrue(text.contains("--target tool-smoke"));
        assertTrue(text.contains("tool-smoke-reports/mkfs-force.report"));
        assertTrue(text.contains("cxnfs.blockfs.mkfs.v1"));
        assertTrue(text.contains("tool-smoke-reports/fsck-force.report"));
        assertTrue(text.contains("cxnfs.blockfs.fsck.v1"));
        assertTrue(text.contains("cxnfs release check dry run complete"));
        assertTrue(text.contains("reports=" + reportDir));
    }

    @Test
    void liveQuicCheckDryRunPrintsManifestStepWithoutSecrets() throws Exception {
        String script = Files.readString(Path.of("packaging/live-quic-check.sh"));
        Path reportDir = tempDir.resolve("live-quic-reports");
        Path outputPath = tempDir.resolve("live-quic-check-dry-run.out");
        Process process = new ProcessBuilder(
                "packaging/live-quic-check.sh",
                "--dry-run",
                "true",
                "--skip-maven",
                "true",
                "--report-dir",
                reportDir.toString(),
                "--test",
                "python.tests.test_quic_java_integration.PythonJavaQuicIntegrationTest.test_python_quic_bulk_client_reads_from_java_server"
        )
                .redirectErrorStream(true)
                .redirectOutput(outputPath.toFile())
                .start();

        boolean finished = process.waitFor(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(finished, "live QUIC check dry run should finish promptly");

        String text = Files.readString(outputPath);
        assertTrue(text.contains("CXNFS_REQUIRE_QUIC_INTEGRATION"));
        assertTrue(text.contains("CXNFS_QUIC_INTEGRATION_REPORT_DIR"));
        assertTrue(text.contains("python.tests.test_quic_java_integration"));
        assertTrue(text.contains("generate-live-quic-handoff-artifacts"));
        assertTrue(text.contains("-m cxnfs.workload_decision"));
        assertTrue(text.contains("python-quic-trainer-pipeline-smoke.json"));
        assertTrue(text.contains("cxnfs.trainer-pipeline-smoke.v1"));
        assertTrue(text.contains("cxnfs.trainer-pipeline-write-probe.v1"));
        assertTrue(text.contains("python-quic-batch-direct-read.json"));
        assertTrue(text.contains("cxnfs.quic-batch-direct-read.v1"));
        assertTrue(text.contains("python-quic-batch-direct-tensor-read.json"));
        assertTrue(text.contains("cxnfs.quic-batch-direct-tensor-read.v1"));
        assertTrue(text.contains("--require-low-level-direct-read-proofs"));
        assertTrue(text.contains("python-quic-trainer-readiness-bundle.json"));
        assertTrue(text.contains("cxnfs.trainer-readiness-bundle.v1"));
        assertTrue(text.contains("python-quic-trainer-handoff-acceptance.json"));
        assertTrue(text.contains("cxnfs.trainer-handoff-acceptance.v1"));
        assertTrue(text.contains("python-quic-trainer-network-preflight.json"));
        assertTrue(text.contains("cxnfs.trainer-network-preflight.v1"));
        assertTrue(text.contains("python-quic-workload-decision.json"));
        assertTrue(text.contains("quic-workload-decision/blockfs-handoff.json"));
        assertTrue(text.contains("python-quic-trainer-handoff.json"));
        assertTrue(text.contains("python-quic-trainer-handoff-surface-check.json"));
        assertTrue(text.contains("write-live-quic-manifest"));
        assertTrue(text.contains("live-quic-check.json"));
        assertTrue(text.contains("cxnfs live QUIC check dry-run complete"));
        assertTrue(text.contains("reports=" + reportDir));
        assertTrue(text.contains("manifest=" + reportDir.resolve("live-quic-check.json")));
        assertFalse(text.contains("--password "));
        assertFalse(text.contains("--keystore-password "));
        assertTrue(script.contains("PYTHONPATH=\"$PYTHON_RUN_PATH\""));
        assertTrue(script.contains("-m cxnfs.live_quic_manifest"));
        assertTrue(script.contains("-m cxnfs.workload_decision"));
        assertTrue(script.contains("run_trainer_handoff_cli"));
        assertTrue(script.contains("python-quic-trainer-handoff-acceptance.json"));
        assertTrue(script.contains("\"format\": \"cxnfs.trainer-handoff-acceptance.v1\""));
        assertTrue(script.contains("--batch-direct-read-report \"$batch_direct_read\""));
        assertTrue(script.contains("--batch-direct-tensor-read-report \"$batch_direct_tensor_read\""));
        assertTrue(script.contains("--require-low-level-direct-read-proofs"));
        assertTrue(script.contains("\"low_level_direct_read_proofs_complete\": true"));
        assertTrue(script.contains("\"batch_direct_read_proof_complete\": true"));
        assertTrue(script.contains("\"batch_direct_tensor_read_proof_complete\": true"));
        assertTrue(script.contains("\"direct_quic_memory_path_verified\": true"));
        assertTrue(script.contains("cxnfs.trainer-sustained-read-profile.v1"));
        assertTrue(script.contains("\"sustained_read_profile_complete\": true"));
        assertTrue(script.contains("\"quic_read_transport_complete\": true"));
        assertTrue(script.contains("\"quic_read_transport_stream_eof_verified\": true"));
        assertTrue(script.contains("\"quic_read_transport_all_reports_expected_cipher_suite\": true"));
        assertTrue(script.contains("\"TLS_AES_128_GCM_SHA256\""));
        assertTrue(script.contains("\"quic_read_transport_read_stream_eof_report_count\": 4"));
    }

    @Test
    void liveQuicCheckDryRunBootstrapsVenvWithoutSystemSitePackages() throws Exception {
        Path reportDir = tempDir.resolve("live-quic-bootstrap-reports");
        Path venv = tempDir.resolve("live-quic-venv");
        Path outputPath = tempDir.resolve("live-quic-bootstrap-dry-run.out");
        Process process = new ProcessBuilder(
                "packaging/live-quic-check.sh",
                "--dry-run",
                "true",
                "--skip-maven",
                "true",
                "--bootstrap-venv",
                venv.toString(),
                "--system-site-packages",
                "false",
                "--report-dir",
                reportDir.toString(),
                "--test",
                "python.tests.test_quic_java_integration.PythonJavaQuicIntegrationTest.test_python_quic_bulk_client_reads_sixty_four_cords_from_java_server"
        )
                .redirectErrorStream(true)
                .redirectOutput(outputPath.toFile())
                .start();

        boolean finished = process.waitFor(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(finished, "live QUIC bootstrap dry run should finish promptly");
        assertTrue(process.exitValue() == 0, "live QUIC bootstrap dry run should exit cleanly");

        String text = Files.readString(outputPath);
        assertTrue(text.contains("-m venv " + venv));
        assertTrue(text.contains(venv.resolve("bin/python") + " -m pip install"));
        assertTrue(text.contains("aioquic"));
        assertFalse(text.contains("--system-site-packages " + venv));
        assertTrue(text.contains("generate-live-quic-handoff-artifacts"));
        assertTrue(text.contains("write-live-quic-manifest"));
        assertTrue(text.contains("reports=" + reportDir));
    }
}
