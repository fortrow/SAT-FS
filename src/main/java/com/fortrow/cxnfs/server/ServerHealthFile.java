package com.fortrow.cxnfs.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ServerHealthFile {
    public static final String HEALTH_FORMAT = "cxnfs.server.health.v1";

    public ServerHealthCheckResult check(Path healthFile, Duration maxAge) throws IOException {
        return check(healthFile, maxAge, Clock.systemUTC());
    }

    public ServerHealthCheckResult check(Path healthFile, Duration maxAge, Clock clock) throws IOException {
        if (healthFile == null) {
            throw new IllegalArgumentException("healthFile is required");
        }
        if (maxAge == null || maxAge.isNegative() || maxAge.isZero()) {
            throw new IllegalArgumentException("maxAge must be positive");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock is required");
        }
        List<String> lines = Files.readAllLines(healthFile, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("health file is empty");
        }
        String format = lines.getFirst().trim();
        if (!HEALTH_FORMAT.equals(format)) {
            throw new IllegalArgumentException("unsupported server health format: " + format);
        }
        Map<String, String> fields = parseFields(lines);
        String status = required(fields, "status");
        boolean healthy = booleanValue(fields, "healthy");
        Instant sampledAt = Instant.parse(required(fields, "sampled_at"));
        Duration age = Duration.between(sampledAt, Instant.now(clock));
        if (age.isNegative()) {
            age = Duration.ZERO;
        }
        boolean stale = age.compareTo(maxAge) > 0;
        boolean checkPassed = "running".equals(status) && healthy && !stale;
        return new ServerHealthCheckResult(
                format,
                status,
                healthy,
                sampledAt,
                age,
                maxAge,
                stale,
                checkPassed,
                fields
        );
    }

    private static Map<String, String> parseFields(List<String> lines) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int index = 1; index < lines.size(); index++) {
            String line = lines.get(index);
            if (line == null || line.isBlank()) {
                continue;
            }
            int separator = line.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("invalid health field on line " + (index + 1));
            }
            String key = line.substring(0, separator);
            String value = line.substring(separator + 1);
            if (key.isBlank()) {
                throw new IllegalArgumentException("invalid health field on line " + (index + 1));
            }
            fields.put(key, value);
        }
        return fields;
    }

    private static String required(Map<String, String> fields, String key) {
        String value = fields.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("health file missing field: " + key);
        }
        return value;
    }

    private static boolean booleanValue(Map<String, String> fields, String key) {
        String value = required(fields, key);
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new IllegalArgumentException("health field is not boolean: " + key);
    }
}
