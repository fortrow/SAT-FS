package com.fortrow.cxnfs.dataset;

import java.util.Locale;

public enum DatasetSourceFormat {
    FILES("files", "loose files"),
    HDF5("hdf5", "HDF5"),
    TFRECORD("tfrecord", "TFRecord"),
    WEBDATASET("webdataset", "WebDataset"),
    PARQUET("parquet", "Apache Parquet");

    private final String cliName;
    private final String displayName;

    DatasetSourceFormat(String cliName, String displayName) {
        this.cliName = cliName;
        this.displayName = displayName;
    }

    public String cliName() {
        return cliName;
    }

    public String displayName() {
        return displayName;
    }

    public static DatasetSourceFormat parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("dataset source format is required");
        }
        String normalized = value.toLowerCase(Locale.ROOT).replace("_", "-");
        for (DatasetSourceFormat format : values()) {
            String enumName = format.name().toLowerCase(Locale.ROOT).replace("_", "-");
            if (format.cliName.equals(normalized) || enumName.equals(normalized)) {
                return format;
            }
        }
        throw new IllegalArgumentException("unknown dataset source format: " + value);
    }
}
