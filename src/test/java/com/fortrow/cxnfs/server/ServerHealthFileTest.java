package com.fortrow.cxnfs.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ServerHealthFileTest {
    @TempDir
    Path tempDir;

    @Test
    void acceptsFreshRunningHealthySample() throws Exception {
        Path health = tempDir.resolve("server.health");
        Files.writeString(health, healthText("running", true, "2026-06-01T10:00:00Z"));

        ServerHealthCheckResult result = new ServerHealthFile().check(
                health,
                Duration.ofSeconds(5),
                Clock.fixed(Instant.parse("2026-06-01T10:00:03Z"), ZoneOffset.UTC)
        );

        assertEquals("cxnfs.server.health.v1", result.format());
        assertEquals("running", result.status());
        assertTrue(result.healthy());
        assertFalse(result.stale());
        assertTrue(result.checkPassed());
        assertEquals(3_000, result.sampleAge().toMillis());
        String report = result.toReportText();
        assertTrue(report.contains("cxnfs.server.health-check.v1"));
        assertTrue(report.contains("sample_age_ms=3000"));
        assertTrue(report.contains("check_passed=true"));
        assertTrue(report.contains("cipher_suite=tls_aes_128_gcm_sha256"));
        assertTrue(report.contains("bulk_cipher_suite=tls_aes_128_gcm_sha256"));
        assertTrue(report.contains("bulk_total_connections=0"));
        assertTrue(report.contains("bulk_total_streams=0"));
    }

    @Test
    void exportsMonitorMetricsForFreshHealthySample() throws Exception {
        Path health = tempDir.resolve("server-health-metrics.health");
        Files.writeString(health, healthText("running", true, "2026-06-01T10:00:00Z"));

        ServerHealthCheckResult result = new ServerHealthFile().check(
                health,
                Duration.ofSeconds(5),
                Clock.fixed(Instant.parse("2026-06-01T10:00:03Z"), ZoneOffset.UTC)
        );

        String metrics = result.toPrometheusText();
        assertTrue(metrics.contains("# TYPE cxnfs_server_health_check_passed gauge"));
        assertTrue(metrics.contains("cxnfs_server_health_check_passed 1"));
        assertTrue(metrics.contains("cxnfs_server_health_sample_age_milliseconds 3000"));
        assertTrue(metrics.contains("cxnfs_server_control_running 1"));
        assertTrue(metrics.contains("cxnfs_server_bulk_active_connections 0"));
        assertTrue(metrics.contains("cxnfs_server_bulk_total_connections 0"));
        assertTrue(metrics.contains("cxnfs_server_bulk_total_streams 0"));
        assertTrue(metrics.contains("cxnfs_server_listener_error{listener=\"control\",kind=\"accept\",value=\"none\"} 0"));
        assertTrue(metrics.contains("cxnfs_server_health_info{format=\"cxnfs.server.health.v1\",status=\"running\""));
        assertTrue(metrics.contains("cipher_suite=\"tls_aes_128_gcm_sha256\""));
        assertTrue(metrics.contains("bulk_cipher_suite=\"tls_aes_128_gcm_sha256\""));
    }

    @Test
    void exportsBlockfsMissResolverFields() throws Exception {
        Path health = tempDir.resolve("server-health-blockfs-miss.health");
        Files.writeString(health, healthText("running", true, "2026-06-01T10:00:00Z") + """
                blockfs_miss_resolver=enabled
                blockfs_miss_resolver_cords=4
                blockfs_miss_resolver_running=true
                blockfs_miss_resolver_drain_cycles=7
                blockfs_miss_resolver_events_resolved=5
                blockfs_miss_resolver_planned_chunks=9
                blockfs_miss_resolver_fetched_chunks=9
                blockfs_miss_resolver_published_extents=9
                blockfs_miss_resolver_unsupported_extents=0
                blockfs_miss_resolver_payload_bytes_written=36864
                blockfs_miss_resolver_last_error=none
                """);

        ServerHealthCheckResult result = new ServerHealthFile().check(
                health,
                Duration.ofSeconds(5),
                Clock.fixed(Instant.parse("2026-06-01T10:00:03Z"), ZoneOffset.UTC)
        );

        String report = result.toReportText();
        assertTrue(report.contains("blockfs_miss_resolver=enabled"));
        assertTrue(report.contains("blockfs_miss_resolver_cords=4"));
        assertTrue(report.contains("blockfs_miss_resolver_events_resolved=5"));
        assertTrue(report.contains("blockfs_miss_resolver_payload_bytes_written=36864"));
        String metrics = result.toPrometheusText();
        assertTrue(metrics.contains("cxnfs_server_blockfs_miss_resolver_running 1"));
        assertTrue(metrics.contains("cxnfs_server_blockfs_miss_resolver_cords 4"));
        assertTrue(metrics.contains("cxnfs_server_blockfs_miss_resolver_drain_cycles 7"));
        assertTrue(metrics.contains("cxnfs_server_blockfs_miss_resolver_events_resolved 5"));
        assertTrue(metrics.contains("cxnfs_server_blockfs_miss_resolver_published_extents 9"));
        assertTrue(metrics.contains("cxnfs_server_blockfs_miss_resolver_payload_bytes_written 36864"));
    }

    @Test
    void rejectsStaleSamples() throws Exception {
        Path health = tempDir.resolve("stale.health");
        Files.writeString(health, healthText("running", true, "2026-06-01T10:00:00Z"));

        ServerHealthCheckResult result = new ServerHealthFile().check(
                health,
                Duration.ofSeconds(5),
                Clock.fixed(Instant.parse("2026-06-01T10:00:06Z"), ZoneOffset.UTC)
        );

        assertTrue(result.stale());
        assertFalse(result.checkPassed());
        assertTrue(result.toReportText().contains("stale=true"));
    }

    @Test
    void rejectsStoppedSamples() throws Exception {
        Path health = tempDir.resolve("stopped.health");
        Files.writeString(health, healthText("stopped", false, "2026-06-01T10:00:00Z"));

        ServerHealthCheckResult result = new ServerHealthFile().check(
                health,
                Duration.ofSeconds(5),
                Clock.fixed(Instant.parse("2026-06-01T10:00:01Z"), ZoneOffset.UTC)
        );

        assertEquals("stopped", result.status());
        assertFalse(result.healthy());
        assertFalse(result.checkPassed());
    }

    @Test
    void rejectsUnknownFormat() throws Exception {
        Path health = tempDir.resolve("bad.health");
        Files.writeString(health, "cxnfs.unknown.v1\nstatus=running\n");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new ServerHealthFile().check(health, Duration.ofSeconds(5))
        );

        assertTrue(error.getMessage().contains("unsupported server health format"));
    }

    private static String healthText(String status, boolean healthy, String sampledAt) {
        return """
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
                """.formatted(status, healthy, sampledAt, healthy, healthy);
    }
}
