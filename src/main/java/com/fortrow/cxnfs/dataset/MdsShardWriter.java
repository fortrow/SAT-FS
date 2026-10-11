package com.fortrow.cxnfs.dataset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class MdsShardWriter {
    private static final long UINT32_MAX = 0xFFFF_FFFFL;

    public MdsShardArtifact writeShard(
            DatasetConversionPlan plan,
            MdsShardPlanEntry shard,
            Path outputDirectory
    ) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (shard == null) {
            throw new IllegalArgumentException("shard is required");
        }
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required");
        }
        Files.createDirectories(outputDirectory);
        List<SampleLayout> samples = sampleLayouts(plan.inputDirectory(), shard.sourceFiles());
        byte[] config = shardConfigJson(plan.targetShardSizeBytes()).getBytes(StandardCharsets.UTF_8);
        long[] offsets = offsets(samples, config.length);
        Path outputFile = outputFile(outputDirectory, shard.shardName());
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }
        try (OutputStream output = Files.newOutputStream(outputFile)) {
            writeUInt32LE(output, samples.size());
            for (long offset : offsets) {
                writeUInt32LE(output, offset);
            }
            output.write(config);
            for (SampleLayout sample : samples) {
                writeUInt32LE(output, sample.dataSize());
                writeUInt32LE(output, sample.pathBytes().length);
                long copied = Files.copy(sample.sourcePath(), output);
                if (copied != sample.dataSize()) {
                    throw new IOException("source file changed during conversion: " + sample.sourcePath());
                }
                output.write(sample.pathBytes());
            }
        }
        return new MdsShardArtifact(shard.shardName(), outputFile, samples.size(), Files.size(outputFile));
    }

    static String shardConfigJson(long sizeLimit) {
        return "{"
                + "\"column_encodings\":[\"bytes\",\"str\"],"
                + "\"column_names\":[\"data\",\"path\"],"
                + "\"column_sizes\":[null,null],"
                + "\"compression\":null,"
                + "\"format\":\"mds\","
                + "\"hashes\":[],"
                + "\"size_limit\":" + sizeLimit + ","
                + "\"version\":2"
                + "}";
    }

    private static List<SampleLayout> sampleLayouts(Path inputDirectory, List<DatasetSourceFile> sourceFiles)
            throws IOException {
        List<SampleLayout> samples = new ArrayList<>();
        for (DatasetSourceFile sourceFile : sourceFiles) {
            Path sourcePath = inputDirectory.resolve(sourceFile.relativePath()).normalize();
            if (!sourcePath.startsWith(inputDirectory.normalize())) {
                throw new IllegalArgumentException("source file escapes input directory: " + sourceFile.relativePath());
            }
            long dataSize = Files.size(sourcePath);
            if (dataSize != sourceFile.sizeBytes()) {
                throw new IOException("source file changed during conversion: " + sourcePath);
            }
            byte[] pathBytes = sourceFile.relativePath().getBytes(StandardCharsets.UTF_8);
            validateUInt32(dataSize, "sample data size");
            validateUInt32(pathBytes.length, "sample path size");
            long sampleSize = Math.addExact(8L, Math.addExact(dataSize, pathBytes.length));
            validateUInt32(sampleSize, "sample record size");
            samples.add(new SampleLayout(sourceFile, sourcePath, pathBytes, dataSize, sampleSize));
        }
        return List.copyOf(samples);
    }

    private static long[] offsets(List<SampleLayout> samples, int configSize) {
        long headerSize = 4L + (4L * (samples.size() + 1L));
        long nextOffset = Math.addExact(headerSize, configSize);
        validateUInt32(nextOffset, "first sample offset");
        long[] offsets = new long[samples.size() + 1];
        offsets[0] = nextOffset;
        for (int i = 0; i < samples.size(); i++) {
            nextOffset = Math.addExact(nextOffset, samples.get(i).sampleSize());
            validateUInt32(nextOffset, "sample offset");
            offsets[i + 1] = nextOffset;
        }
        return offsets;
    }

    private static Path outputFile(Path outputDirectory, String shardName) {
        Path normalizedOutputDirectory = outputDirectory.normalize();
        Path outputFile = outputDirectory.resolve(shardName).normalize();
        if (!outputFile.startsWith(normalizedOutputDirectory)) {
            throw new IllegalArgumentException("shard output escapes output directory: " + shardName);
        }
        return outputFile;
    }

    private static void writeUInt32LE(OutputStream output, long value) throws IOException {
        validateUInt32(value, "uint32 value");
        output.write((int) (value & 0xFF));
        output.write((int) ((value >>> 8) & 0xFF));
        output.write((int) ((value >>> 16) & 0xFF));
        output.write((int) ((value >>> 24) & 0xFF));
    }

    private static void validateUInt32(long value, String name) {
        if (value < 0 || value > UINT32_MAX) {
            throw new IllegalArgumentException(name + " exceeds MDS uint32 limit: " + value);
        }
    }

    private record SampleLayout(
            DatasetSourceFile sourceFile,
            Path sourcePath,
            byte[] pathBytes,
            long dataSize,
            long sampleSize
    ) {
    }
}
