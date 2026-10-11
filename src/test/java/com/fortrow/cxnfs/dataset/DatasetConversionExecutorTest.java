package com.fortrow.cxnfs.dataset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DatasetConversionExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void convertsSourceFilesIntoMdsShardAndIndex() throws Exception {
        Path input = tempDir.resolve("parquet");
        Files.createDirectories(input.resolve("nested"));
        Files.writeString(input.resolve("part-000.parquet"), "alpha");
        Files.writeString(input.resolve("nested/part-001.parquet"), "beta");
        DatasetPreparePlan preparePlan = new DatasetPreparePlan(
                input,
                "train",
                DatasetSourceFormat.PARQUET,
                DatasetFormat.MDS_SHARDS,
                1024,
                true
        );
        DatasetConversionPlan plan = new DatasetConversionService().plan(preparePlan);

        DatasetConversionResult result = new DatasetConversionExecutor().execute(plan, tempDir.resolve("mds"));

        assertEquals(1, result.shardCount());
        assertEquals(2, result.sampleCount());
        assertTrue(Files.exists(result.indexPath()));
        String index = Files.readString(result.indexPath());
        assertTrue(index.contains("\"version\": 2"));
        assertTrue(index.contains("\"column_names\": [\"data\", \"path\"]"));
        assertTrue(index.contains("\"basename\": \"train-000000.mds\""));

        byte[] shard = Files.readAllBytes(result.shards().getFirst().path());
        ByteBuffer buffer = ByteBuffer.wrap(shard).order(ByteOrder.LITTLE_ENDIAN);
        int sampleCount = buffer.getInt();
        assertEquals(2, sampleCount);
        int firstOffset = buffer.getInt();
        int secondOffset = buffer.getInt();
        int endOffset = buffer.getInt();
        int configStart = 4 + (4 * (sampleCount + 1));
        String config = new String(shard, configStart, firstOffset - configStart, StandardCharsets.UTF_8);
        assertTrue(config.contains("\"format\":\"mds\""));
        assertTrue(config.contains("\"column_names\":[\"data\",\"path\"]"));

        DecodedSample first = sample(shard, firstOffset);
        DecodedSample second = sample(shard, secondOffset);
        assertEquals(endOffset, shard.length);
        assertEquals(List.of("nested/part-001.parquet", "part-000.parquet"), List.of(first.path(), second.path()));
        assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), first.data());
        assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), second.data());
    }

    private static DecodedSample sample(byte[] shard, int offset) {
        ByteBuffer buffer = ByteBuffer.wrap(shard).order(ByteOrder.LITTLE_ENDIAN);
        int dataSize = buffer.getInt(offset);
        int pathSize = buffer.getInt(offset + 4);
        byte[] data = new byte[dataSize];
        System.arraycopy(shard, offset + 8, data, 0, dataSize);
        String path = new String(shard, offset + 8 + dataSize, pathSize, StandardCharsets.UTF_8);
        return new DecodedSample(path, data);
    }

    private record DecodedSample(String path, byte[] data) {
    }
}
