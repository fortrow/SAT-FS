package com.fortrow.cxnfs.dataset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class DatasetConversionExecutor {
    private final MdsShardWriter shardWriter;
    private final MdsShardIndexWriter indexWriter;

    public DatasetConversionExecutor() {
        this(new MdsShardWriter(), new MdsShardIndexWriter());
    }

    public DatasetConversionExecutor(MdsShardWriter shardWriter, MdsShardIndexWriter indexWriter) {
        this.shardWriter = shardWriter;
        this.indexWriter = indexWriter;
    }

    public DatasetConversionResult execute(DatasetConversionPlan plan, Path outputDirectory) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required");
        }
        Files.createDirectories(outputDirectory);
        List<MdsShardArtifact> artifacts = new ArrayList<>();
        long bytesWritten = 0;
        long sampleCount = 0;
        for (MdsShardPlanEntry shard : plan.shards()) {
            MdsShardArtifact artifact = shardWriter.writeShard(plan, shard, outputDirectory);
            artifacts.add(artifact);
            bytesWritten += artifact.byteSize();
            sampleCount += artifact.sampleCount();
        }
        Path indexPath = indexWriter.write(plan, artifacts, outputDirectory.resolve("index.json"));
        bytesWritten += Files.size(indexPath);
        return new DatasetConversionResult(
                outputDirectory,
                indexPath,
                artifacts.size(),
                sampleCount,
                bytesWritten,
                artifacts
        );
    }
}
