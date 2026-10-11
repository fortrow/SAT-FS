package com.fortrow.cxnfs.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public record LinuxBlockfsMissEvent(
        long eventId,
        String reason,
        long inodeNumber,
        long offset,
        long length,
        long droppedEvents,
        long mountId,
        String metadataRoot,
        int maxCords,
        int readaheadPages,
        int missWaitMs,
        boolean verifyHashes,
        long mountFlags
) {
    public LinuxBlockfsMissEvent {
        if (metadataRoot == null || "-".equals(metadataRoot)) {
            metadataRoot = "";
        }
        if (eventId <= 0) {
            throw new IllegalArgumentException("eventId must be positive");
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason is required");
        }
        if (inodeNumber <= 0) {
            throw new IllegalArgumentException("inodeNumber must be positive");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        if (droppedEvents < 0) {
            throw new IllegalArgumentException("droppedEvents must be non-negative");
        }
        if (mountId < 0) {
            throw new IllegalArgumentException("mountId must be non-negative");
        }
        if (maxCords < 0) {
            throw new IllegalArgumentException("maxCords must be non-negative");
        }
        if (readaheadPages < 0) {
            throw new IllegalArgumentException("readaheadPages must be non-negative");
        }
        if (missWaitMs < 0) {
            throw new IllegalArgumentException("missWaitMs must be non-negative");
        }
        if (mountFlags < 0) {
            throw new IllegalArgumentException("mountFlags must be non-negative");
        }
    }

    public LinuxBlockfsMissEvent(
            long eventId,
            String reason,
            long inodeNumber,
            long offset,
            long length,
            long droppedEvents
    ) {
        this(eventId, reason, inodeNumber, offset, length, droppedEvents, 0, "", 0, 0, 0, false, 0);
    }

    public static Optional<LinuxBlockfsMissEvent> parseOptional(String output) {
        if (output == null || output.isBlank()) {
            throw new IllegalArgumentException("miss-event output is required");
        }
        Map<String, String> values = keyValues(output);
        if ("none".equals(values.get("miss_event"))) {
            return Optional.empty();
        }
        if (!values.containsKey("event_id")) {
            throw new IllegalArgumentException("missing miss-event field: event_id");
        }
        return Optional.of(new LinuxBlockfsMissEvent(
                longValue(values, "event_id"),
                required(values, "reason"),
                longValue(values, "inode"),
                longValue(values, "offset"),
                longValue(values, "length"),
                longValue(values, "dropped_events"),
                optionalLongValue(values, "mount_id"),
                optionalTextValue(values, "metadata_root"),
                optionalIntValue(values, "max_cords"),
                optionalIntValue(values, "readahead_pages"),
                optionalIntValue(values, "miss_wait_ms"),
                optionalBooleanValue(values, "verify_hashes"),
                optionalLongValue(values, "mount_flags")
        ));
    }

    private static Map<String, String> keyValues(String output) {
        Map<String, String> values = new LinkedHashMap<>();
        for (String token : output.trim().split("\\s+")) {
            int equals = token.indexOf('=');
            if (equals <= 0 || equals == token.length() - 1) {
                continue;
            }
            values.put(token.substring(0, equals), token.substring(equals + 1));
        }
        return values;
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing miss-event field: " + key);
        }
        return value;
    }

    private static long longValue(Map<String, String> values, String key) {
        return Long.parseLong(required(values, key));
    }

    private static long optionalLongValue(Map<String, String> values, String key) {
        String value = values.get(key);
        return value == null || value.isBlank() ? 0 : Long.parseLong(value);
    }

    private static int optionalIntValue(Map<String, String> values, String key) {
        return Math.toIntExact(optionalLongValue(values, key));
    }

    private static String optionalTextValue(Map<String, String> values, String key) {
        String value = values.get(key);
        return value == null || value.isBlank() || "-".equals(value) ? "" : value;
    }

    private static boolean optionalBooleanValue(Map<String, String> values, String key) {
        String value = values.get(key);
        return value != null && Boolean.parseBoolean(value);
    }
}
