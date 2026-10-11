package com.fortrow.cxnfs.storage;

import java.util.LinkedHashMap;
import java.util.Map;

public record LinuxBlockfsStats(
        int abiVersion,
        long registeredExtents,
        long invalidations,
        long readFolioCalls,
        long readaheadCalls,
        long verifiedPageFills,
        long missEventsQueued,
        long missEventsDrained,
        long missEventsPending,
        long missEventsDropped,
        long missWaitSuccesses,
        long missWaitTimeouts,
        long missWaitInterruptions,
        long extentGeneration,
        long hashVerifications,
        long hashMismatches,
        long hashBytesVerified,
        long readaheadMissWindows,
        long readaheadMissBytes
) {
    public static LinuxBlockfsStats parse(String output) {
        if (output == null || output.isBlank()) {
            throw new IllegalArgumentException("stats output is required");
        }
        Map<String, String> values = new LinkedHashMap<>();
        for (String token : output.trim().split("\\s+")) {
            int equals = token.indexOf('=');
            if (equals <= 0 || equals == token.length() - 1) {
                continue;
            }
            values.put(token.substring(0, equals), token.substring(equals + 1));
        }
        return new LinuxBlockfsStats(
                intValue(values, "abi_version"),
                longValue(values, "registered_extents"),
                longValue(values, "invalidations"),
                longValue(values, "read_folio_calls"),
                longValue(values, "readahead_calls"),
                longValue(values, "verified_page_fills"),
                optionalLongValue(values, "miss_events_queued"),
                optionalLongValue(values, "miss_events_drained"),
                optionalLongValue(values, "miss_events_pending"),
                optionalLongValue(values, "miss_events_dropped"),
                optionalLongValue(values, "miss_wait_successes"),
                optionalLongValue(values, "miss_wait_timeouts"),
                optionalLongValue(values, "miss_wait_interruptions"),
                optionalLongValue(values, "extent_generation"),
                optionalLongValue(values, "hash_verifications"),
                optionalLongValue(values, "hash_mismatches"),
                optionalLongValue(values, "hash_bytes_verified"),
                optionalLongValue(values, "readahead_miss_windows"),
                optionalLongValue(values, "readahead_miss_bytes")
        );
    }

    private static int intValue(Map<String, String> values, String key) {
        return Math.toIntExact(longValue(values, key));
    }

    private static long longValue(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("missing stats field: " + key);
        }
        return Long.parseLong(value);
    }

    private static long optionalLongValue(Map<String, String> values, String key) {
        String value = values.get(key);
        return value == null ? 0 : Long.parseLong(value);
    }
}
