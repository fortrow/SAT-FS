package com.fortrow.cxnfs.dataset;

import java.util.Locale;

public enum DatasetUseCase {
    PYTORCH_TRAINING_READS("training-reads"),
    COLUMNAR_ANALYTICS("analytics"),
    CHECKPOINT_WRITES("checkpoint-writes"),
    MIXED("mixed");

    private final String cliName;

    DatasetUseCase(String cliName) {
        this.cliName = cliName;
    }

    public String cliName() {
        return cliName;
    }

    public static DatasetUseCase parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("dataset use case is required");
        }
        String normalized = value.toLowerCase(Locale.ROOT).replace("_", "-");
        for (DatasetUseCase useCase : values()) {
            if (useCase.cliName.equals(normalized) || useCase.name().toLowerCase(Locale.ROOT).replace("_", "-").equals(normalized)) {
                return useCase;
            }
        }
        throw new IllegalArgumentException("unknown dataset use case: " + value);
    }
}
