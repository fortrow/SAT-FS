package com.fortrow.cxnfs.server;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record ServerHealthCheckResult(
        String format,
        String status,
        boolean healthy,
        Instant sampledAt,
        Duration sampleAge,
        Duration maxAge,
        boolean stale,
        boolean checkPassed,
        Map<String, String> fields
) {
    public ServerHealthCheckResult {
        if (format == null || format.isBlank()) {
            throw new IllegalArgumentException("format is required");
        }
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("status is required");
        }
        if (sampledAt == null) {
            throw new IllegalArgumentException("sampledAt is required");
        }
        if (sampleAge == null) {
            throw new IllegalArgumentException("sampleAge is required");
        }
        if (maxAge == null) {
            throw new IllegalArgumentException("maxAge is required");
        }
        fields = Map.copyOf(fields);
    }

    public String toReportText() {
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        builder.append("cxnfs.server.health-check.v1").append(lineSeparator);
        builder.append("format=").append(format).append(lineSeparator);
        builder.append("status=").append(status).append(lineSeparator);
        builder.append("healthy=").append(healthy).append(lineSeparator);
        builder.append("sampled_at=").append(sampledAt).append(lineSeparator);
        builder.append("sample_age_ms=").append(Math.max(0L, sampleAge.toMillis())).append(lineSeparator);
        builder.append("max_age_ms=").append(maxAge.toMillis()).append(lineSeparator);
        builder.append("stale=").append(stale).append(lineSeparator);
        builder.append("check_passed=").append(checkPassed).append(lineSeparator);
        writeOptional(builder, lineSeparator, "served_directory");
        writeOptional(builder, lineSeparator, "control_listener");
        writeOptional(builder, lineSeparator, "bulk_read_listener");
        writeOptional(builder, lineSeparator, "cipher_suite");
        writeOptional(builder, lineSeparator, "bulk_cipher_suite");
        writeOptional(builder, lineSeparator, "control_running");
        writeOptional(builder, lineSeparator, "bulk_running");
        writeOptional(builder, lineSeparator, "control_active_connections");
        writeOptional(builder, lineSeparator, "bulk_active_connections");
        writeOptional(builder, lineSeparator, "bulk_total_connections");
        writeOptional(builder, lineSeparator, "bulk_total_streams");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_cords");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_running");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_drain_cycles");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_events_resolved");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_planned_chunks");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_fetched_chunks");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_published_extents");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_unsupported_extents");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_payload_bytes_written");
        writeOptional(builder, lineSeparator, "blockfs_miss_resolver_last_error");
        writeOptional(builder, lineSeparator, "control_last_accept_error");
        writeOptional(builder, lineSeparator, "control_last_connection_error");
        writeOptional(builder, lineSeparator, "bulk_last_accept_error");
        writeOptional(builder, lineSeparator, "bulk_last_connection_error");
        return builder.toString();
    }

    public String toPrometheusText() {
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        writeGauge(
                builder,
                lineSeparator,
                "cxnfs_server_health_check_passed",
                "1 when the current server health sample passes status, freshness, and healthy checks.",
                checkPassed ? 1 : 0
        );
        writeGauge(
                builder,
                lineSeparator,
                "cxnfs_server_health_healthy",
                "1 when the server health sample reports healthy=true.",
                healthy ? 1 : 0
        );
        writeGauge(
                builder,
                lineSeparator,
                "cxnfs_server_health_stale",
                "1 when the server health sample is older than the allowed maximum age.",
                stale ? 1 : 0
        );
        writeGauge(
                builder,
                lineSeparator,
                "cxnfs_server_health_sample_age_milliseconds",
                "Age of the server health sample in milliseconds.",
                Math.max(0L, sampleAge.toMillis())
        );
        writeGauge(
                builder,
                lineSeparator,
                "cxnfs_server_health_max_age_milliseconds",
                "Configured maximum accepted server health sample age in milliseconds.",
                maxAge.toMillis()
        );
        writeGauge(
                builder,
                lineSeparator,
                "cxnfs_server_health_sample_timestamp_seconds",
                "Unix timestamp for the server health sample.",
                sampledAt.getEpochSecond()
        );
        writeOptionalBooleanGauge(
                builder,
                lineSeparator,
                "cxnfs_server_control_running",
                "1 when the control listener reports running.",
                "control_running"
        );
        writeOptionalBooleanGauge(
                builder,
                lineSeparator,
                "cxnfs_server_bulk_running",
                "1 when the bulk listener reports running.",
                "bulk_running"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_control_active_connections",
                "Current active control listener connections.",
                "control_active_connections"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_bulk_active_connections",
                "Current active bulk listener connections.",
                "bulk_active_connections"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_bulk_total_connections",
                "Total accepted bulk listener connections in this server process.",
                "bulk_total_connections"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_bulk_total_streams",
                "Total accepted QUIC bulk streams in this server process.",
                "bulk_total_streams"
        );
        writeOptionalBooleanGauge(
                builder,
                lineSeparator,
                "cxnfs_server_blockfs_miss_resolver_running",
                "1 when the CXBFS read-miss resolver worker reports running.",
                "blockfs_miss_resolver_running"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_blockfs_miss_resolver_cords",
                "Configured CXBFS read-miss resolver cord count.",
                "blockfs_miss_resolver_cords"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_blockfs_miss_resolver_drain_cycles",
                "Total CXBFS read-miss resolver drain cycles in this server process.",
                "blockfs_miss_resolver_drain_cycles"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_blockfs_miss_resolver_events_resolved",
                "Total CXBFS kernel read-miss events resolved in this server process.",
                "blockfs_miss_resolver_events_resolved"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_blockfs_miss_resolver_published_extents",
                "Total verified CXBFS extents published by the read-miss resolver.",
                "blockfs_miss_resolver_published_extents"
        );
        writeOptionalLongGauge(
                builder,
                lineSeparator,
                "cxnfs_server_blockfs_miss_resolver_payload_bytes_written",
                "Total verified bytes written to the CXBFS payload cache/device by the read-miss resolver.",
                "blockfs_miss_resolver_payload_bytes_written"
        );
        if (hasAnyErrorField()) {
            builder.append("# HELP cxnfs_server_listener_error 1 when a listener reports a non-empty error value.")
                    .append(lineSeparator);
            builder.append("# TYPE cxnfs_server_listener_error gauge").append(lineSeparator);
            writeErrorGauge(builder, lineSeparator, "control", "accept", "control_last_accept_error");
            writeErrorGauge(builder, lineSeparator, "control", "connection", "control_last_connection_error");
            writeErrorGauge(builder, lineSeparator, "bulk", "accept", "bulk_last_accept_error");
            writeErrorGauge(builder, lineSeparator, "bulk", "connection", "bulk_last_connection_error");
        }
        builder.append("# HELP cxnfs_server_health_info Static labels from the current server health sample.")
                .append(lineSeparator);
        builder.append("# TYPE cxnfs_server_health_info gauge").append(lineSeparator);
        builder.append("cxnfs_server_health_info{")
                .append("format=\"").append(escapeLabel(format)).append("\",")
                .append("status=\"").append(escapeLabel(status)).append("\"");
        writeInfoLabel(builder, "served_directory");
        writeInfoLabel(builder, "control_listener");
        writeInfoLabel(builder, "bulk_read_listener");
        writeInfoLabel(builder, "cipher_suite");
        writeInfoLabel(builder, "bulk_cipher_suite");
        writeInfoLabel(builder, "blockfs_miss_resolver");
        writeInfoLabel(builder, "blockfs_miss_resolver_last_error");
        builder.append("} 1").append(lineSeparator);
        return builder.toString();
    }

    private void writeOptional(StringBuilder builder, String lineSeparator, String key) {
        String value = fields.get(key);
        if (value != null) {
            builder.append(key).append('=').append(value).append(lineSeparator);
        }
    }

    private boolean hasAnyErrorField() {
        return fields.containsKey("control_last_accept_error")
                || fields.containsKey("control_last_connection_error")
                || fields.containsKey("bulk_last_accept_error")
                || fields.containsKey("bulk_last_connection_error");
    }

    private void writeOptionalBooleanGauge(
            StringBuilder builder,
            String lineSeparator,
            String name,
            String help,
            String key
    ) {
        String value = fields.get(key);
        if (value == null) {
            return;
        }
        if ("true".equals(value)) {
            writeGauge(builder, lineSeparator, name, help, 1);
        } else if ("false".equals(value)) {
            writeGauge(builder, lineSeparator, name, help, 0);
        }
    }

    private void writeOptionalLongGauge(
            StringBuilder builder,
            String lineSeparator,
            String name,
            String help,
            String key
    ) {
        String value = fields.get(key);
        if (value == null) {
            return;
        }
        try {
            writeGauge(builder, lineSeparator, name, help, Long.parseLong(value));
        } catch (NumberFormatException ignored) {
            // Optional health fields that are malformed stay out of monitor output.
        }
    }

    private void writeErrorGauge(
            StringBuilder builder,
            String lineSeparator,
            String listener,
            String kind,
            String key
    ) {
        String value = fields.get(key);
        if (value == null) {
            return;
        }
        builder.append("cxnfs_server_listener_error{listener=\"")
                .append(escapeLabel(listener))
                .append("\",kind=\"")
                .append(escapeLabel(kind))
                .append("\",value=\"")
                .append(escapeLabel(value))
                .append("\"} ")
                .append("none".equals(value) || value.isBlank() ? 0 : 1)
                .append(lineSeparator);
    }

    private void writeInfoLabel(StringBuilder builder, String key) {
        String value = fields.get(key);
        if (value != null) {
            builder.append(',').append(key).append("=\"").append(escapeLabel(value)).append("\"");
        }
    }

    private static void writeGauge(
            StringBuilder builder,
            String lineSeparator,
            String name,
            String help,
            long value
    ) {
        builder.append("# HELP ").append(name).append(' ').append(help).append(lineSeparator);
        builder.append("# TYPE ").append(name).append(" gauge").append(lineSeparator);
        builder.append(name).append(' ').append(value).append(lineSeparator);
    }

    private static String escapeLabel(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\n", "\\n")
                .replace("\"", "\\\"");
    }
}
