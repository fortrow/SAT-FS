package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class BulkTransferSessionHandler {
    private static final byte[] READ_REQUEST_MAGIC = "CXNFSRDQ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] DATA_FRAME_MAGIC = "CXNFSDAT".getBytes(StandardCharsets.US_ASCII);
    private static final int WRITE_TIMING_LOG_FRAME_INTERVAL = 256;
    private static final ConcurrentMap<String, WriteTimingStats> WRITE_TIMINGS = new ConcurrentHashMap<>();

    private final BulkReadSessionHandler readHandler;
    private final BulkWriteFrameStager writeStager;
    private final BulkWriteAckRouter writeAckRouter = new BulkWriteAckRouter();

    public BulkTransferSessionHandler(BulkReadSessionHandler readHandler, BulkWriteFrameStager writeStager) {
        if (readHandler == null) {
            throw new IllegalArgumentException("readHandler is required");
        }
        if (writeStager == null) {
            throw new IllegalArgumentException("writeStager is required");
        }
        this.readHandler = readHandler;
        this.writeStager = writeStager;
    }

    public Optional<BulkTransferDeliveryResult> handleOnce(InputStream input, OutputStream output) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        long readStart = System.nanoTime();
        Optional<MessagePrefix> prefix = readMessagePrefix(input);
        long readNanos = System.nanoTime() - readStart;
        if (prefix.isEmpty()) {
            return Optional.empty();
        }
        MessagePrefix message = prefix.orElseThrow();
        if (startsWith(message.magic(), DATA_FRAME_MAGIC)) {
            return Optional.of(handleStreamingDataFrame(input, output, message.remainingBytes(), readNanos));
        }
        byte[] messageBytes = readRemainingMessageBytes(input, message);
        if (BulkWriteAckSubscriptionCodec.isSubscribe(messageBytes)) {
            handleAckSubscription(messageBytes, input, output);
            return Optional.empty();
        }
        return Optional.of(handleEncodedMessage(messageBytes, output, readNanos));
    }

    public List<BulkTransferDeliveryResult> handleUntilEof(InputStream input, OutputStream output) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }

        List<BulkTransferDeliveryResult> results = new ArrayList<>();
        while (true) {
            Optional<BulkTransferDeliveryResult> result = handleOnce(input, output);
            if (result.isEmpty()) {
                return List.copyOf(results);
            }
            results.add(result.orElseThrow());
        }
    }

    public void drainUntilEof(InputStream input, OutputStream output) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }

        while (handleOnce(input, output).isPresent()) {
            // Drain the protected bulk stream without retaining per-message results.
        }
    }

    private BulkTransferDeliveryResult handleEncodedMessage(
            byte[] message,
            OutputStream output,
            long readNanos
    ) throws IOException {
        if (startsWith(message, READ_REQUEST_MAGIC)) {
            return BulkTransferDeliveryResult.read(readHandler.handleEncodedRequest(message, output));
        }
        if (startsWith(message, DATA_FRAME_MAGIC)) {
            long decodeStart = System.nanoTime();
            BulkDataFrame frame = BulkDataFrameCodec.decode(message);
            long decodeNanos = System.nanoTime() - decodeStart;
            StagedPayload staged;
            try {
                long stageStart = System.nanoTime();
                staged = writeStager.stage(frame);
                long stageNanos = System.nanoTime() - stageStart;
                long ackStart = System.nanoTime();
                BulkWriteDeliveryResult result = new BulkWriteDeliveryResult(
                        frame.key(),
                        frame.payloadLength(),
                        staged.metadataVerified(),
                        staged.storage()
                );
                BulkWriteAck ack = BulkWriteAck.from(result);
                if (!writeAckRouter.publish(ack)) {
                    BulkWriteAckStream.write(output, BulkWriteAckCodec.encode(ack));
                }
                long ackNanos = System.nanoTime() - ackStart;
                recordWriteTiming(frame, message.length, readNanos, decodeNanos, stageNanos, ackNanos);
                return BulkTransferDeliveryResult.write(result);
            } catch (IOException | RuntimeException e) {
                logWriteFrameFailure(frame, e);
                throw e;
            }
        }
        throw new BinaryFormatException("unknown bulk transfer message magic");
    }

    private BulkTransferDeliveryResult handleStreamingDataFrame(
            InputStream input,
            OutputStream output,
            int remainingBytes,
            long readNanos
    ) throws IOException {
        long decodeStart = System.nanoTime();
        BulkDataFrame frame = BulkDataFrameCodec.decodeAfterMagic(input, remainingBytes);
        long decodeNanos = System.nanoTime() - decodeStart;
        StagedPayload staged;
        try {
            long stageStart = System.nanoTime();
            staged = writeStager.stage(frame);
            long stageNanos = System.nanoTime() - stageStart;
            long ackStart = System.nanoTime();
            BulkWriteDeliveryResult result = new BulkWriteDeliveryResult(
                    frame.key(),
                    frame.payloadLength(),
                    staged.metadataVerified(),
                    staged.storage()
            );
            BulkWriteAck ack = BulkWriteAck.from(result);
            if (!writeAckRouter.publish(ack)) {
                BulkWriteAckStream.write(output, BulkWriteAckCodec.encode(ack));
            }
            long ackNanos = System.nanoTime() - ackStart;
            recordWriteTiming(frame, remainingBytes + DATA_FRAME_MAGIC.length, readNanos, decodeNanos, stageNanos, ackNanos);
            return BulkTransferDeliveryResult.write(result);
        } catch (IOException | RuntimeException e) {
            logWriteFrameFailure(frame, e);
            throw e;
        }
    }

    private void handleAckSubscription(byte[] message, InputStream input, OutputStream output) throws IOException {
        writeAckRouter.serve(BulkWriteAckSubscriptionCodec.decodeSubscribe(message), input, output);
    }

    private static void recordWriteTiming(
            BulkDataFrame frame,
            int encodedLength,
            long readNanos,
            long decodeNanos,
            long stageNanos,
            long ackNanos
    ) {
        String key = frame.transferId() + ":" + frame.entrySequence();
        WriteTimingStats stats = WRITE_TIMINGS.computeIfAbsent(key, ignored -> new WriteTimingStats(
                frame.transferId().toString(),
                frame.entrySequence()
        ));
        stats.record(frame.payloadLength(), encodedLength, readNanos, decodeNanos, stageNanos, ackNanos);
    }

    private static void logWriteFrameFailure(BulkDataFrame frame, Throwable failure) {
        System.err.println("cxnfs-bulk: write frame failed transfer=" + frame.transferId()
                + " entry=" + frame.entrySequence()
                + " chunk=" + frame.chunkSequence()
                + " offset=" + frame.entryOffset()
                + " length=" + frame.payloadLength()
                + " pattern=" + frame.payloadPattern().map(pattern -> pattern.kind().name()).orElse("none")
                + " error=" + failure.getClass().getSimpleName()
                + ": " + failure.getMessage());
    }

    private static Optional<byte[]> readEncodedMessage(InputStream input) throws IOException {
        return BulkTransferMessageStream.read(input);
    }

    private static Optional<MessagePrefix> readMessagePrefix(InputStream input) throws IOException {
        DataInputStream in = new DataInputStream(input);
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            return Optional.empty();
        }
        if (length <= 0 || length > BulkTransferMessageStream.MAX_ENCODED_MESSAGE_BYTES) {
            throw new BinaryFormatException("invalid encoded bulk transfer message length: " + length);
        }
        if (length < DATA_FRAME_MAGIC.length) {
            throw new BinaryFormatException("invalid encoded bulk transfer message length: " + length);
        }
        byte[] magic = BinaryIO.readFixedBytes(in, DATA_FRAME_MAGIC.length);
        return Optional.of(new MessagePrefix(length, magic));
    }

    private static byte[] readRemainingMessageBytes(InputStream input, MessagePrefix prefix) throws IOException {
        byte[] remaining = BinaryIO.readFixedBytes(new DataInputStream(input), prefix.remainingBytes());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(prefix.length());
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.write(prefix.magic());
            out.write(remaining);
        }
        return bytes.toByteArray();
    }

    private static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private record MessagePrefix(int length, byte[] magic) {
        private MessagePrefix {
            magic = magic == null ? new byte[0] : magic.clone();
        }

        private int remainingBytes() {
            return length - magic.length;
        }

        @Override
        public byte[] magic() {
            return magic.clone();
        }
    }

    private static final class WriteTimingStats {
        private final String transferId;
        private final int entrySequence;
        private final long startedAtNanos = System.nanoTime();
        private final ProcessCpuSample processCpuStart = ProcessCpuSample.capture();
        private final AtomicInteger frames = new AtomicInteger();
        private final AtomicInteger lastLoggedFrames = new AtomicInteger();
        private final LongAdder payloadBytes = new LongAdder();
        private final LongAdder encodedBytes = new LongAdder();
        private final LongAdder readNanos = new LongAdder();
        private final LongAdder decodeNanos = new LongAdder();
        private final LongAdder stageNanos = new LongAdder();
        private final LongAdder ackNanos = new LongAdder();
        private final AtomicLong maxReadNanos = new AtomicLong();
        private final AtomicLong maxDecodeNanos = new AtomicLong();
        private final AtomicLong maxStageNanos = new AtomicLong();
        private final AtomicLong maxAckNanos = new AtomicLong();

        private WriteTimingStats(String transferId, int entrySequence) {
            this.transferId = transferId;
            this.entrySequence = entrySequence;
        }

        private void record(
                int payloadLength,
                int encodedLength,
                long readNanosValue,
                long decodeNanosValue,
                long stageNanosValue,
                long ackNanosValue
        ) {
            payloadBytes.add(payloadLength);
            encodedBytes.add(encodedLength);
            readNanos.add(readNanosValue);
            decodeNanos.add(decodeNanosValue);
            stageNanos.add(stageNanosValue);
            ackNanos.add(ackNanosValue);
            updateMax(maxReadNanos, readNanosValue);
            updateMax(maxDecodeNanos, decodeNanosValue);
            updateMax(maxStageNanos, stageNanosValue);
            updateMax(maxAckNanos, ackNanosValue);
            int frameCount = frames.incrementAndGet();
            int previous = lastLoggedFrames.get();
            if (frameCount - previous >= WRITE_TIMING_LOG_FRAME_INTERVAL
                    && lastLoggedFrames.compareAndSet(previous, frameCount)) {
                log(frameCount);
            }
        }

        private void log(int frameCount) {
            long elapsedNanos = Math.max(1L, System.nanoTime() - startedAtNanos);
            long processCpuNanos = processCpuStart.elapsedNanos();
            long payload = payloadBytes.sum();
            System.err.println(String.format(Locale.ROOT,
                    "cxnfs bulk write timings transfer=%s entry=%d frames=%d payload_bytes=%d "
                            + "encoded_bytes=%d elapsed_ms=%.1f app_payload_mb_s=%.1f "
                            + "process_cpu_ms=%s process_cpu_cores_avg=%s "
                            + "read_ms=%.1f decode_ms=%.1f stage_ms=%.1f ack_ms=%.1f "
                            + "avg_read_ms=%.1f avg_decode_ms=%.1f avg_stage_ms=%.1f avg_ack_ms=%.1f "
                            + "max_read_ms=%.1f max_decode_ms=%.1f max_stage_ms=%.1f max_ack_ms=%.1f",
                    transferId,
                    entrySequence,
                    frameCount,
                    payload,
                    encodedBytes.sum(),
                    elapsedNanos / 1_000_000.0d,
                    mbPerSecond(payload, elapsedNanos),
                    cpuMillis(processCpuNanos),
                    cpuCores(processCpuNanos, elapsedNanos),
                    readNanos.sum() / 1_000_000.0d,
                    decodeNanos.sum() / 1_000_000.0d,
                    stageNanos.sum() / 1_000_000.0d,
                    ackNanos.sum() / 1_000_000.0d,
                    averageMillis(readNanos.sum(), frameCount),
                    averageMillis(decodeNanos.sum(), frameCount),
                    averageMillis(stageNanos.sum(), frameCount),
                    averageMillis(ackNanos.sum(), frameCount),
                    maxReadNanos.get() / 1_000_000.0d,
                    maxDecodeNanos.get() / 1_000_000.0d,
                    maxStageNanos.get() / 1_000_000.0d,
                    maxAckNanos.get() / 1_000_000.0d));
        }

        private static void updateMax(AtomicLong target, long value) {
            target.accumulateAndGet(value, Math::max);
        }

        private static double averageMillis(long nanos, int count) {
            if (count <= 0) {
                return 0.0d;
            }
            return (nanos / 1_000_000.0d) / count;
        }

        private static double mbPerSecond(long bytes, long nanos) {
            if (nanos <= 0L) {
                return 0.0d;
            }
            return (bytes / 1_000_000.0d) / (nanos / 1_000_000_000.0d);
        }

        private static String cpuMillis(long nanos) {
            return nanos < 0L ? "unavailable" : String.format(Locale.ROOT, "%.1f", nanos / 1_000_000.0d);
        }

        private static String cpuCores(long cpuNanos, long wallNanos) {
            if (cpuNanos < 0L || wallNanos <= 0L) {
                return "unavailable";
            }
            return String.format(Locale.ROOT, "%.2f", cpuNanos / (double) wallNanos);
        }
    }

    private record ProcessCpuSample(long cpuNanos) {
        private static ProcessCpuSample capture() {
            java.lang.management.OperatingSystemMXBean bean =
                    java.lang.management.ManagementFactory.getOperatingSystemMXBean();
            if (bean instanceof com.sun.management.OperatingSystemMXBean operatingSystem) {
                return new ProcessCpuSample(operatingSystem.getProcessCpuTime());
            }
            return new ProcessCpuSample(-1L);
        }

        private long elapsedNanos() {
            ProcessCpuSample end = capture();
            if (cpuNanos < 0L || end.cpuNanos < 0L || end.cpuNanos < cpuNanos) {
                return -1L;
            }
            return end.cpuNanos - cpuNanos;
        }
    }
}
