package com.fortrow.cxnfs.dataset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

public final class DatasetConversionService {
    public DatasetConversionPlan plan(DatasetPreparePlan preparePlan) throws IOException {
        if (preparePlan == null) {
            throw new IllegalArgumentException("preparePlan is required");
        }
        if (!Files.isDirectory(preparePlan.inputDirectory())) {
            throw new IllegalArgumentException("input directory does not exist: " + preparePlan.inputDirectory());
        }
        List<DatasetSourceFile> sources = inventorySources(preparePlan.inputDirectory(), preparePlan.sourceFormat());
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("no " + preparePlan.sourceFormat().cliName()
                    + " source files found in " + preparePlan.inputDirectory());
        }
        List<MdsShardPlanEntry> shards = groupIntoShards(
                preparePlan.datasetName(),
                sources,
                preparePlan.targetShardSizeBytes()
        );
        return new DatasetConversionPlan(
                preparePlan.inputDirectory(),
                preparePlan.datasetName(),
                preparePlan.sourceFormat(),
                preparePlan.format(),
                preparePlan.targetShardSizeBytes(),
                sources,
                shards
        );
    }

    private static List<DatasetSourceFile> inventorySources(Path inputDirectory, DatasetSourceFormat sourceFormat) throws IOException {
        try (Stream<Path> paths = Files.walk(inputDirectory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> matches(sourceFormat, inputDirectory.relativize(path).toString()))
                    .sorted()
                    .map(path -> sourceFile(inputDirectory, path))
                    .toList();
        }
    }

    private static DatasetSourceFile sourceFile(Path inputDirectory, Path path) {
        try {
            String relativePath = inputDirectory.relativize(path).toString().replace('\\', '/');
            return new DatasetSourceFile(relativePath, Files.size(path));
        } catch (IOException e) {
            throw new IllegalStateException("failed to inspect source file: " + path, e);
        }
    }

    private static boolean matches(DatasetSourceFormat sourceFormat, String relativePath) {
        String lowered = relativePath.toLowerCase(Locale.ROOT).replace('\\', '/');
        return switch (sourceFormat) {
            case FILES -> true;
            case HDF5 -> lowered.endsWith(".h5") || lowered.endsWith(".hdf5");
            case TFRECORD -> lowered.endsWith(".tfrecord") || lowered.endsWith(".tfrecords");
            case WEBDATASET -> lowered.endsWith(".tar") || lowered.endsWith(".tgz") || lowered.endsWith(".tar.gz");
            case PARQUET -> lowered.endsWith(".parquet");
        };
    }

    private static List<MdsShardPlanEntry> groupIntoShards(
            String datasetName,
            List<DatasetSourceFile> sources,
            long targetShardSizeBytes
    ) {
        List<MdsShardPlanEntry> shards = new ArrayList<>();
        List<DatasetSourceFile> current = new ArrayList<>();
        long currentSize = 0;
        for (DatasetSourceFile source : sources.stream().sorted(Comparator.comparing(DatasetSourceFile::relativePath)).toList()) {
            if (!current.isEmpty() && currentSize + source.sizeBytes() > targetShardSizeBytes) {
                shards.add(shard(datasetName, shards.size(), current, currentSize));
                current = new ArrayList<>();
                currentSize = 0;
            }
            current.add(source);
            currentSize += source.sizeBytes();
        }
        if (!current.isEmpty()) {
            shards.add(shard(datasetName, shards.size(), current, currentSize));
        }
        return List.copyOf(shards);
    }

    private static MdsShardPlanEntry shard(String datasetName, int index, List<DatasetSourceFile> sources, long sizeBytes) {
        return new MdsShardPlanEntry("%s-%06d.mds".formatted(datasetName, index), sources, sizeBytes);
    }
}
