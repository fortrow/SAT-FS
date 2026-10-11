package com.fortrow.cxnfs.dataset;

import java.util.Locale;

public enum DatasetFormat {
    MDS_SHARDS("mds-shards", "MDS-Shards (Mosaic)"),
    PARQUET("parquet", "Apache Parquet"),
    SAFETENSORS("safetensors", "Safetensors");

    private final String cliName;
    private final String displayName;

    DatasetFormat(String cliName, String displayName) {
        this.cliName = cliName;
        this.displayName = displayName;
    }

    public String cliName() {
        return cliName;
    }

    public String displayName() {
        return displayName;
    }

    public boolean isDatasetReadFormat() {
        return this == MDS_SHARDS;
    }

    public boolean isCheckpointWriteFormat() {
        return this == SAFETENSORS;
    }

    public static DatasetFormat parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("dataset format is required");
        }
        String normalized = value.toLowerCase(Locale.ROOT).replace("_", "-");
        if (normalized.equals("mds")) {
            return MDS_SHARDS;
        }
        for (DatasetFormat format : values()) {
            String enumName = format.name().toLowerCase(Locale.ROOT).replace("_", "-");
            if (format.cliName.equals(normalized) || enumName.equals(normalized)) {
                return format;
            }
        }
        throw new IllegalArgumentException("unknown dataset format: " + value);
    }
}
