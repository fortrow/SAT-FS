package com.fortrow.cxnfs.benchmark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SequentialReadBenchmarkTest {
    @TempDir
    Path tempDir;

    @Test
    void readsTheWholeFileForEachMeasuredIteration() throws Exception {
        Path file = tempDir.resolve("dataset.bin");
        Files.write(file, bytes(4096));

        BenchmarkResult result = new SequentialReadBenchmark().run(
                new SequentialReadBenchmarkConfig("cxnfs", file, 3, 1, 512)
        );

        assertEquals("cxnfs", result.label());
        assertEquals(3, result.iterations());
        assertEquals(4096L * 3, result.bytesRead());
        assertTrue(result.bytesPerSecond() > 0);
        assertTrue(result.mebibytesPerSecond() > 0);
        assertTrue(result.gigabitsPerSecond() > 0);
    }

    private static byte[] bytes(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i * 17);
        }
        return bytes;
    }
}
