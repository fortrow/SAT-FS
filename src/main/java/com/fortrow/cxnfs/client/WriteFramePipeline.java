package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.function.LongConsumer;

public final class WriteFramePipeline implements AutoCloseable {
    private static final long MIN_WRITE_PIPELINE_WINDOW_BYTES = 16L * 1024 * 1024;
    private static final long MAX_DEFAULT_WRITE_PIPELINE_WINDOW_BYTES = 512L * 1024 * 1024;
    private static final long MAX_WRITE_PIPELINE_WINDOW_BYTES = 2L * 1024 * 1024 * 1024;
    private static final long WRITE_WINDOW_DECREASE_LATENCY_NANOS = 2_500_000_000L;
    private static final long WRITE_WINDOW_DECREASE_COOLDOWN_NANOS = 200_000_000L;

    private final CxnFileNetworkClient client;
    private final LongConsumer acknowledgedBytesListener;
    private final long maxWindowBytes;
    private long adaptiveWindowBytes;
    private final ExecutorService executor;
    private final ExecutorCompletionService<WriteFrameResult> completions;
    private long submitted;
    private long completed;
    private long inFlightBytes;
    private long capacityWaitNanos;
    private long acknowledgedBytes;
    private final long startedAtNanos = System.nanoTime();
    private long lastAckNanos;
    private int maxActiveCords = 1;
    private int maxInFlightFrames;
    private long maxInFlightBytes;
    private long maxAdaptiveWindowBytes;
    private long maxFrameAckNanos;
    private long lastWindowDecreaseNanos;
    private int slowFrames;
    private int windowDecreases;
    private IOException ioFailure;
    private RuntimeException runtimeFailure;

    public WriteFramePipeline(CxnFileNetworkClient client, long advertisedWindowBytes) {
        this(client, advertisedWindowBytes, null);
    }

    public WriteFramePipeline(
            CxnFileNetworkClient client,
            long advertisedWindowBytes,
            LongConsumer acknowledgedBytesListener
    ) {
        if (client == null) {
            throw new IllegalArgumentException("client is required");
        }
        this.client = client;
        this.acknowledgedBytesListener = acknowledgedBytesListener;
        this.maxWindowBytes = writePipelineWindowBytes(advertisedWindowBytes, BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
        this.adaptiveWindowBytes = this.maxWindowBytes;
        this.maxAdaptiveWindowBytes = adaptiveWindowBytes;
        this.executor = Executors.newCachedThreadPool(writeThreadFactory());
        this.completions = new ExecutorCompletionService<>(executor);
    }

    public void awaitCapacity(int activeCords, int nextFrameBytes) throws IOException {
        awaitCapacity(activeCords, Math.max(1, activeCords), nextFrameBytes);
    }

    public void awaitCapacity(int activeCords, int frameWindow, int nextFrameBytes) throws IOException {
        int activeCordCount = Math.max(1, activeCords);
        int inFlightFrameWindow = Math.max(activeCordCount, frameWindow);
        long byteWindow = Math.max(nextFrameBytes, Math.min(maxWindowBytes, adaptiveWindowBytes));
        maxActiveCords = Math.max(maxActiveCords, activeCordCount);
        reapCompleted();
        throwIfFailed();
        long waitStart = 0L;
        while ((submitted - completed) >= inFlightFrameWindow || inFlightBytes + nextFrameBytes > byteWindow) {
            if (waitStart == 0L) {
                waitStart = System.nanoTime();
            }
            awaitOne();
            throwIfFailed();
        }
        if (waitStart != 0L) {
            capacityWaitNanos += System.nanoTime() - waitStart;
        }
    }

    public void submit(BulkDataFrame frame) throws IOException {
        submit(frame, null);
    }

    public void submit(BulkDataFrame frame, byte[] encodedFrameBytes) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        throwIfFailed();
        submitted++;
        inFlightBytes += frame.payloadLength();
        maxInFlightFrames = Math.max(maxInFlightFrames, (int) (submitted - completed));
        maxInFlightBytes = Math.max(maxInFlightBytes, inFlightBytes);
        completions.submit(() -> {
            long started = System.nanoTime();
            try {
                if (encodedFrameBytes == null) {
                    client.sendWriteFrame(frame);
                } else {
                    client.sendEncodedWriteFrame(frame, encodedFrameBytes);
                }
                return WriteFrameResult.ok(frame.payloadLength(), System.nanoTime() - started);
            } catch (IOException e) {
                return WriteFrameResult.failed(frame.payloadLength(), System.nanoTime() - started, e, null);
            } catch (RuntimeException e) {
                return WriteFrameResult.failed(frame.payloadLength(), System.nanoTime() - started, null, e);
            }
        });
    }

    public void submitBatch(List<BulkDataFrame> frames) throws IOException {
        if (frames == null) {
            throw new IllegalArgumentException("frames are required");
        }
        if (frames.isEmpty()) {
            return;
        }
        List<BulkDataFrame> safeFrames = List.copyOf(frames);
        int frameCount = safeFrames.size();
        long payloadBytes = 0L;
        for (BulkDataFrame frame : safeFrames) {
            payloadBytes += frame.payloadLength();
        }
        throwIfFailed();
        submitted += frameCount;
        inFlightBytes += payloadBytes;
        maxInFlightFrames = Math.max(maxInFlightFrames, (int) (submitted - completed));
        maxInFlightBytes = Math.max(maxInFlightBytes, inFlightBytes);
        long batchPayloadBytes = payloadBytes;
        completions.submit(() -> {
            long started = System.nanoTime();
            try {
                client.sendWriteFrames(safeFrames);
                return WriteFrameResult.ok(frameCount, batchPayloadBytes, System.nanoTime() - started);
            } catch (IOException e) {
                return WriteFrameResult.failed(frameCount, batchPayloadBytes, System.nanoTime() - started, e, null);
            } catch (RuntimeException e) {
                return WriteFrameResult.failed(frameCount, batchPayloadBytes, System.nanoTime() - started, null, e);
            }
        });
    }

    public void submitEncodedBatch(List<EncodedWriteFrame> frames) throws IOException {
        if (frames == null) {
            throw new IllegalArgumentException("frames are required");
        }
        if (frames.isEmpty()) {
            return;
        }
        List<EncodedWriteFrame> safeFrames = List.copyOf(frames);
        int frameCount = safeFrames.size();
        long payloadBytes = 0L;
        for (EncodedWriteFrame frame : safeFrames) {
            payloadBytes += frame.frame().payloadLength();
        }
        throwIfFailed();
        submitted += frameCount;
        inFlightBytes += payloadBytes;
        maxInFlightFrames = Math.max(maxInFlightFrames, (int) (submitted - completed));
        maxInFlightBytes = Math.max(maxInFlightBytes, inFlightBytes);
        long batchPayloadBytes = payloadBytes;
        completions.submit(() -> {
            long started = System.nanoTime();
            try {
                client.sendEncodedWriteFrames(safeFrames);
                return WriteFrameResult.ok(frameCount, batchPayloadBytes, System.nanoTime() - started);
            } catch (IOException e) {
                return WriteFrameResult.failed(frameCount, batchPayloadBytes, System.nanoTime() - started, e, null);
            } catch (RuntimeException e) {
                return WriteFrameResult.failed(frameCount, batchPayloadBytes, System.nanoTime() - started, null, e);
            }
        });
    }

    public void complete() throws IOException {
        while (completed < submitted) {
            awaitOne();
        }
        throwIfFailed();
    }

    public void cancel() {
        executor.shutdownNow();
    }

    public WritePipelineMetrics metrics() {
        return new WritePipelineMetrics(
                capacityWaitNanos,
                maxActiveCords,
                maxInFlightFrames,
                maxInFlightBytes,
                maxAdaptiveWindowBytes,
                acknowledgedBytes,
                Math.max(0L, lastAckNanos - startedAtNanos),
                maxFrameAckNanos,
                slowFrames,
                windowDecreases
        );
    }

    private void reapCompleted() throws IOException {
        Future<WriteFrameResult> future;
        while ((future = completions.poll()) != null) {
            record(future);
        }
    }

    private void awaitOne() throws IOException {
        try {
            record(completions.take());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for write pipeline", e);
        }
    }

    private void record(Future<WriteFrameResult> future) throws IOException {
        WriteFrameResult result;
        try {
            result = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for write pipeline", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            String message = cause == null ? "unknown" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
            throw new IOException("write pipeline task failed: " + message, cause);
        }
        completed += result.frameCount();
        inFlightBytes = Math.max(0L, inFlightBytes - result.payloadBytes());
        maxFrameAckNanos = Math.max(maxFrameAckNanos, result.elapsedNanos());
        if (result.ioFailure() != null && ioFailure == null) {
            tuneWindow(result);
            ioFailure = result.ioFailure();
            return;
        }
        if (result.runtimeFailure() != null && runtimeFailure == null) {
            tuneWindow(result);
            runtimeFailure = result.runtimeFailure();
            return;
        }
        acknowledgedBytes += result.payloadBytes();
        notifyAcknowledged(result.payloadBytes());
        lastAckNanos = System.nanoTime();
        tuneWindow(result);
    }

    private void notifyAcknowledged(long payloadBytes) {
        if (payloadBytes <= 0L || acknowledgedBytesListener == null) {
            return;
        }
        acknowledgedBytesListener.accept(payloadBytes);
    }

    private void throwIfFailed() throws IOException {
        if (ioFailure != null) {
            throw ioFailure;
        }
        if (runtimeFailure != null) {
            throw runtimeFailure;
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void tuneWindow(WriteFrameResult result) {
        long minimum = Math.min(maxWindowBytes, Math.max(MIN_WRITE_PIPELINE_WINDOW_BYTES, result.payloadBytes()));
        boolean failed = result.ioFailure() != null || result.runtimeFailure() != null;
        boolean slow = result.elapsedNanos() > WRITE_WINDOW_DECREASE_LATENCY_NANOS;
        if (slow) {
            slowFrames++;
        }
        if (failed) {
            long now = System.nanoTime();
            if (now - lastWindowDecreaseNanos >= WRITE_WINDOW_DECREASE_COOLDOWN_NANOS) {
                int numerator = 3;
                int denominator = 4;
                adaptiveWindowBytes = Math.max(minimum, (adaptiveWindowBytes * numerator) / denominator);
                lastWindowDecreaseNanos = now;
                windowDecreases++;
            }
            return;
        }
        adaptiveWindowBytes = Math.min(maxWindowBytes, adaptiveWindowBytes + result.payloadBytes());
        maxAdaptiveWindowBytes = Math.max(maxAdaptiveWindowBytes, adaptiveWindowBytes);
    }

    private static long writePipelineWindowBytes(long advertisedWindowBytes, int nextFrameBytes) {
        long minimum = Math.max(MIN_WRITE_PIPELINE_WINDOW_BYTES, nextFrameBytes);
        if (advertisedWindowBytes <= 0) {
            return Math.max(minimum, MAX_DEFAULT_WRITE_PIPELINE_WINDOW_BYTES);
        }
        return Math.max(minimum, Math.min(advertisedWindowBytes, MAX_WRITE_PIPELINE_WINDOW_BYTES));
    }

    private static ThreadFactory writeThreadFactory() {
        return Thread.ofPlatform()
                .daemon(true)
                .name("cxnfs-write-frame-", 0)
                .factory();
    }

    private record WriteFrameResult(
            int frameCount,
            long payloadBytes,
            long elapsedNanos,
            IOException ioFailure,
            RuntimeException runtimeFailure
    ) {
        private static WriteFrameResult ok(long payloadBytes, long elapsedNanos) {
            return ok(1, payloadBytes, elapsedNanos);
        }

        private static WriteFrameResult ok(int frameCount, long payloadBytes, long elapsedNanos) {
            return new WriteFrameResult(frameCount, payloadBytes, elapsedNanos, null, null);
        }

        private static WriteFrameResult failed(
                long payloadBytes,
                long elapsedNanos,
                IOException ioFailure,
                RuntimeException runtimeFailure
        ) {
            return failed(1, payloadBytes, elapsedNanos, ioFailure, runtimeFailure);
        }

        private static WriteFrameResult failed(
                int frameCount,
                long payloadBytes,
                long elapsedNanos,
                IOException ioFailure,
                RuntimeException runtimeFailure
        ) {
            return new WriteFrameResult(frameCount, payloadBytes, elapsedNanos, ioFailure, runtimeFailure);
        }
    }
}
