package com.fortrow.cxnfs.dataset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DatasetConversionServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void plansParquetSourcesIntoMdsShards() throws Exception {
        Path input = tempDir.resolve("parquet");
        Files.createDirectories(input.resolve("nested"));
        Files.write(input.resolve("part-000.parquet"), new byte[4]);
        Files.write(input.resolve("nested/part-001.parquet"), new byte[4]);
        Files.write(input.resolve("notes.txt"), new byte[4]);
        DatasetPreparePlan preparePlan = new DatasetPreparePlan(
                input,
                "train",
                DatasetSourceFormat.PARQUET,
                DatasetFormat.MDS_SHARDS,
                5,
                true
        );

        DatasetConversionPlan plan = new DatasetConversionService().plan(preparePlan);

        assertEquals(2, plan.sourceFiles().size());
        assertEquals(8, plan.totalSourceBytes());
        assertEquals(2, plan.shards().size());
        assertEquals("train-000000.mds", plan.shards().getFirst().shardName());
        assertEquals("nested/part-001.parquet", plan.sourceFiles().getFirst().relativePath());
    }

    @Test
    void plansWebDatasetTarSources() throws Exception {
        Path input = tempDir.resolve("webdataset");
        Files.createDirectories(input);
        Files.write(input.resolve("samples-000.tar"), new byte[3]);
        Files.write(input.resolve("samples-001.tar.gz"), new byte[3]);
        Files.write(input.resolve("samples-002.tgz"), new byte[3]);
        Files.write(input.resolve("skip.parquet"), new byte[3]);
        DatasetPreparePlan preparePlan = new DatasetPreparePlan(
                input,
                "images",
                DatasetSourceFormat.WEBDATASET,
                DatasetFormat.MDS_SHARDS,
                1024,
                true
        );

        DatasetConversionPlan plan = new DatasetConversionService().plan(preparePlan);

        assertEquals(3, plan.sourceFiles().size());
        assertEquals(1, plan.shards().size());
        assertTrue(plan.sourceFiles().stream().allMatch(source -> source.relativePath().startsWith("samples-")));
    }

    @Test
    void rejectsMissingSourceFilesForSelectedFormat() throws Exception {
        Path input = tempDir.resolve("empty");
        Files.createDirectories(input);
        Files.write(input.resolve("notes.txt"), new byte[4]);
        DatasetPreparePlan preparePlan = new DatasetPreparePlan(
                input,
                "train",
                DatasetSourceFormat.PARQUET,
                DatasetFormat.MDS_SHARDS,
                1024,
                true
        );

        assertThrows(IllegalArgumentException.class, () -> new DatasetConversionService().plan(preparePlan));
    }

    @Test
    void writesConversionManifestJson() throws Exception {
        Path input = tempDir.resolve("parquet");
        Files.createDirectories(input);
        Files.write(input.resolve("part-000.parquet"), new byte[4]);
        DatasetPreparePlan preparePlan = new DatasetPreparePlan(
                input,
                "train",
                DatasetSourceFormat.PARQUET,
                DatasetFormat.MDS_SHARDS,
                1024,
                true
        );
        DatasetConversionPlan plan = new DatasetConversionService().plan(preparePlan);

        Path output = new DatasetConversionManifestWriter().write(plan, tempDir.resolve("plan.json"));

        String json = Files.readString(output);
        assertTrue(json.contains("\"dataset_name\": \"train\""));
        assertTrue(json.contains("\"target_format\": \"mds-shards\""));
        assertTrue(json.contains("\"source_files\""));
    }
}
