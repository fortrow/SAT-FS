package com.fortrow.cxnfs.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public final class SequentialReadBenchmark {
    public BenchmarkResult run(SequentialReadBenchmarkConfig config) throws IOException {
        if (!Files.isRegularFile(config.path())) {
            throw new IllegalArgumentException("benchmark path must be a regular file: " + config.path());
        }

        ByteBuffer buffer = ByteBuffer.allocateDirect(config.blockSizeBytes());
        for (int i = 0; i < config.warmupIterations(); i++) {
            readWholeFile(config, buffer);
        }

        long bytesRead = 0;
        long start = System.nanoTime();
        for (int i = 0; i < config.iterations(); i++) {
            bytesRead += readWholeFile(config, buffer);
        }
        long elapsed = Math.max(1, System.nanoTime() - start);

        return new BenchmarkResult(
                config.label(),
                config.path(),
                config.iterations(),
                config.warmupIterations(),
                config.blockSizeBytes(),
                bytesRead,
                elapsed
        );
    }

    private long readWholeFile(SequentialReadBenchmarkConfig config, ByteBuffer buffer) throws IOException {
        long bytesRead = 0;
        try (FileChannel channel = FileChannel.open(config.path(), StandardOpenOption.READ)) {
            while (true) {
                buffer.clear();
                int read = channel.read(buffer);
                if (read < 0) {
                    return bytesRead;
                }
                bytesRead += read;
            }
        }
    }
}
