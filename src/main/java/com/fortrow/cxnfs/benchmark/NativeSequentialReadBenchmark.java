package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.client.ClientCli;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

public final class NativeSequentialReadBenchmark {
    public BenchmarkResult run(
            NativeSequentialReadBenchmarkConfig config,
            ClientCli.ClientConnection client
    ) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("config is required");
        }
        if (client == null) {
            throw new IllegalArgumentException("client is required");
        }

        FileMetadata metadata = client.stat(config.path())
                .orElseThrow(() -> new IllegalArgumentException("benchmark path does not exist: " + config.path()));
        if (metadata.type() != FileType.REGULAR_FILE) {
            throw new IllegalArgumentException("benchmark path must be a regular file: " + config.path());
        }

        long length = readLength(config, metadata);
        for (int i = 0; i < config.warmupIterations(); i++) {
            readRange(config, client, length);
        }

        long bytesRead = 0;
        long start = System.nanoTime();
        for (int i = 0; i < config.iterations(); i++) {
            bytesRead += readRange(config, client, length);
        }
        long elapsed = Math.max(1, System.nanoTime() - start);

        return new BenchmarkResult(
                config.label(),
                Path.of(config.path()),
                config.iterations(),
                config.warmupIterations(),
                config.blockSizeBytes(),
                bytesRead,
                elapsed
        );
    }

    private static long readLength(NativeSequentialReadBenchmarkConfig config, FileMetadata metadata) {
        if (config.offset() > metadata.size()) {
            throw new IllegalArgumentException("offset is beyond end of file: " + config.offset());
        }
        long available = metadata.size() - config.offset();
        if (config.length() == NativeSequentialReadBenchmarkConfig.READ_TO_EOF) {
            return available;
        }
        return Math.min(config.length(), available);
    }

    private static long readRange(
            NativeSequentialReadBenchmarkConfig config,
            ClientCli.ClientConnection client,
            long length
    ) throws IOException {
        long remaining = length;
        long offset = config.offset();
        long bytesRead = 0;
        while (remaining > 0) {
            long requestLength = Math.min(remaining, config.blockSizeBytes());
            long returned = client.readRangeTo(
                    config.path(),
                    offset,
                    requestLength,
                    config.payloadFormat(),
                    OutputStream.nullOutputStream()
            );
            if (returned != requestLength) {
                throw new IOException("native read returned " + returned + " bytes for requested range of "
                        + requestLength + " bytes");
            }
            bytesRead += returned;
            offset += returned;
            remaining -= returned;
        }
        return bytesRead;
    }
}
