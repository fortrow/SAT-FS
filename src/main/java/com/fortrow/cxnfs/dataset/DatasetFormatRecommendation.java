package com.fortrow.cxnfs.dataset;

import java.util.List;

public record DatasetFormatRecommendation(
        DatasetUseCase useCase,
        DatasetFormat primaryFormat,
        List<DatasetFormat> companionFormats,
        String reason,
        boolean preferNativePythonAccess
) {
    public DatasetFormatRecommendation {
        if (useCase == null) {
            throw new IllegalArgumentException("useCase is required");
        }
        if (primaryFormat == null) {
            throw new IllegalArgumentException("primaryFormat is required");
        }
        companionFormats = List.copyOf(companionFormats == null ? List.of() : companionFormats);
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason is required");
        }
    }
}
