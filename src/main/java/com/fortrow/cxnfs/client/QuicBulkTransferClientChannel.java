package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkFrameStream;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkReadRequestCodec;
import com.fortrow.cxnfs.transport.BulkReadRequestStream;
import com.fortrow.cxnfs.transport.BulkReadStreamingReceiver;
import com.fortrow.cxnfs.transport.BulkWriteAck;
import com.fortrow.cxnfs.transport.BulkWriteAckCodec;
import com.fortrow.cxnfs.transport.BulkWriteAckStream;
import com.fortrow.cxnfs.transport.BulkWriteAckSubscription;
import com.fortrow.cxnfs.transport.BulkWriteAckSubscriptionCodec;
import com.fortrow.cxnfs.transport.BulkTransferMessageStream;
import com.fortrow.cxnfs.transport.QuicStreamSendBufferTuner;
import com.fortrow.cxnfs.transport.QuicBulkProtocol;
import com.fortrow.cxnfs.transport.StagedPayloadStorage;
import com.fortrow.cxnfs.transport.TlsCipherSuite;
import tech.kwik.agent15.TlsConstants;
import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.log.NullLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class QuicBulkTransferClientChannel implements BulkTransferClientChannel {
    private static final int STREAM_SEND_BUFFER_BYTES = 16 * 1024 * 1024;
    private static final int PERSISTENT_STREAM_WRITE_AHEAD = 2;

    private final QuicClientConnection connection;
    private final Object streamCreateLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger lastReadStreamCount = new AtomicInteger();
    private final AtomicInteger lastAttemptedReadStreamCount = new AtomicInteger();
    private final AtomicInteger attemptedReadStreams = new AtomicInteger();
    private final AtomicInteger readStreamsCreated = new AtomicInteger();
    private final AtomicInteger readStreamsEofVerified = new AtomicInteger();
    private final AtomicInteger successfulReadOperations = new AtomicInteger();
    private final AtomicInteger failedReadOperations = new AtomicInteger();
    private final AtomicInteger attemptedWriteStreams = new AtomicInteger();
    private final AtomicInteger attemptedWriteFrames = new AtomicInteger();
    private final AtomicInteger writeStreamsCreated = new AtomicInteger();
    private final AtomicInteger successfulWriteOperations = new AtomicInteger();
    private final AtomicInteger failedWriteOperations = new AtomicInteger();
    private final AtomicInteger activeWriteStreams = new AtomicInteger();
    private final AtomicInteger maxActiveWriteStreams = new AtomicInteger();
    private final AtomicInteger writeAcksReceived = new AtomicInteger();
    private final AtomicInteger writeAcksVerified = new AtomicInteger();
    private final AtomicInteger writeAckVerificationFailures = new AtomicInteger();
    private final AtomicInteger writeAckStreamsEofVerified = new AtomicInteger();
    private final AtomicInteger writeAckMetadataVerified = new AtomicInteger();
    private final AtomicInteger writeAckMemory = new AtomicInteger();
    private final AtomicInteger writeAckSpill = new AtomicInteger();
    private final AtomicInteger streamSendBuffersTuned = new AtomicInteger();
    private final AtomicInteger streamSendBufferTuneFailures = new AtomicInteger();
    private final AtomicInteger maxPersistentWriteStreamDepth = new AtomicInteger();
    private final ConcurrentMap<Integer, PersistentWriteStream> persistentWriteStreams = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, WriteAckStream> writeAckStreams = new ConcurrentHashMap<>();
    private final AtomicInteger writeAckSubscribeStreamsCreated = new AtomicInteger();
    private final AtomicInteger writeAckSubscribeStreamsReady = new AtomicInteger();
    private final AtomicInteger writeAckSubscribeStreamFailures = new AtomicInteger();
    private final AtomicReference<String> lastReadError = new AtomicReference<>("");
    private final AtomicReference<String> lastWriteError = new AtomicReference<>("");
    private final AtomicReference<Map<String, Object>> lastReadPartitionEvidence = new AtomicReference<>(
            emptyReadPartitionEvidence()
    );

    public QuicBulkTransferClientChannel(QuicClientConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        this.connection = connection;
    }

    public static QuicBulkTransferClientChannel connect(
            InetSocketAddress address,
            CxnFileNetworkClientConfig config,
            Optional<KeyStore> trustStore,
            boolean skipServerCertificateCheck
    ) throws IOException {
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }
        CxnFileNetworkClientConfig clientConfig = config == null
                ? CxnFileNetworkClientConfig.defaults()
                : config;
        Optional<KeyStore> safeTrustStore = trustStore == null ? Optional.empty() : trustStore;
        QuicClientConnection.Builder builder = QuicClientConnection.newBuilder()
                .host(address.getHostString())
                .port(address.getPort())
                .applicationProtocol(QuicBulkProtocol.APPLICATION_PROTOCOL)
                .connectTimeout(Duration.ofMillis(clientConfig.connectTimeoutMillis()))
                .maxIdleTimeout(Duration.ofMillis(clientConfig.readTimeoutMillis()))
                .defaultStreamReceiveBufferSize((long) clientConfig.provisionalContainerSizeBytes())
                .maxOpenPeerInitiatedBidirectionalStreams(0)
                .maxOpenPeerInitiatedUnidirectionalStreams(0)
                .logger(new NullLogger())
                .cipherSuite(TlsConstants.CipherSuite.TLS_AES_128_GCM_SHA256);
        if (safeTrustStore.isPresent()) {
            builder.customTrustStore(safeTrustStore.orElseThrow());
        } else if (skipServerCertificateCheck) {
            builder.noServerCertificateCheck();
        }
        QuicClientConnection connection = builder.build();
        connection.connect();
        return new QuicBulkTransferClientChannel(connection);
    }

    @Override
    public long read(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output
    ) throws IOException {
        ensureOpen();
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        BulkReadRequest boundedRequest = boundReadRequest(request, transfer, plan);
        int streamCount = boundedRequest.activeCords();
        lastReadStreamCount.set(streamCount);
        lastAttemptedReadStreamCount.set(streamCount);
        attemptedReadStreams.addAndGet(streamCount);
        try {
            long bytesRead;
            if (streamCount > 1) {
                bytesRead = readWithMultipleStreams(boundedRequest, transfer, plan, output, streamCount);
            } else {
                bytesRead = readSingleStream(boundedRequest, transfer, plan, output);
            }
            readStreamsCreated.addAndGet(streamCount);
            successfulReadOperations.incrementAndGet();
            lastReadError.set("");
            return bytesRead;
        } catch (IOException | RuntimeException e) {
            failedReadOperations.incrementAndGet();
            lastReadError.set(errorDescription(e));
            closeAfterStreamFailure();
            throw e;
        }
    }

    public int lastReadStreamCount() {
        return lastReadStreamCount.get();
    }

    public int lastAttemptedReadStreamCount() {
        return lastAttemptedReadStreamCount.get();
    }

    public int attemptedReadStreamCount() {
        return attemptedReadStreams.get();
    }

    public int readStreamCount() {
        return readStreamsCreated.get();
    }

    public int successfulReadOperationCount() {
        return successfulReadOperations.get();
    }

    public int failedReadOperationCount() {
        return failedReadOperations.get();
    }

    public int attemptedWriteStreamCount() {
        return attemptedWriteStreams.get();
    }

    public int attemptedWriteFrameCount() {
        return attemptedWriteFrames.get();
    }

    public int writeStreamCount() {
        return writeStreamsCreated.get();
    }

    public int successfulWriteOperationCount() {
        return successfulWriteOperations.get();
    }

    public int failedWriteOperationCount() {
        return failedWriteOperations.get();
    }

    public int activeWriteStreamCount() {
        return activeWriteStreams.get();
    }

    public int maxActiveWriteStreamCount() {
        return maxActiveWriteStreams.get();
    }

    public int writeAckReceivedCount() {
        return writeAcksReceived.get();
    }

    public int writeAckVerifiedCount() {
        return writeAcksVerified.get();
    }

    public int writeAckVerificationFailureCount() {
        return writeAckVerificationFailures.get();
    }

    public int writeAckMetadataVerifiedCount() {
        return writeAckMetadataVerified.get();
    }

    public int writeAckMetadataPendingCount() {
        return Math.max(0, writeAcksReceived.get() - writeAckMetadataVerified.get());
    }

    public int writeAckMemoryCount() {
        return writeAckMemory.get();
    }

    public int writeAckSpillCount() {
        return writeAckSpill.get();
    }

    public boolean readStreamCompletionVerified() {
        return attemptedReadStreams.get() > 0
                && failedReadOperations.get() == 0
                && readStreamsCreated.get() == attemptedReadStreams.get();
    }

    public boolean readStreamEofVerified() {
        return attemptedReadStreams.get() > 0
                && failedReadOperations.get() == 0
                && readStreamsEofVerified.get() == attemptedReadStreams.get();
    }

    public boolean writeAckFrameIdentityVerified() {
        return attemptedWriteFrames.get() > 0
                && writeAckVerificationFailures.get() == 0
                && writeAcksReceived.get() == attemptedWriteFrames.get()
                && writeAcksVerified.get() == attemptedWriteFrames.get();
    }

    public boolean writeAckStreamCompletionVerified() {
        return attemptedWriteStreams.get() > 0
                && failedWriteOperations.get() == 0
                && writeStreamsCreated.get() == attemptedWriteStreams.get();
    }

    public boolean writeAckStreamEofVerified() {
        return attemptedWriteStreams.get() > 0
                && failedWriteOperations.get() == 0
                && writeAckStreamsEofVerified.get() == attemptedWriteStreams.get();
    }

    public boolean connectionReusedForStreams() {
        return readStreamsCreated.get() + writeStreamsCreated.get() > 1;
    }

    public boolean closed() {
        return closed.get();
    }

    public Map<String, Object> readPathEvidence() {
        Map<String, Object> evidence = new LinkedHashMap<>();
        int attemptedReads = attemptedReadStreams.get();
        int createdReads = readStreamsCreated.get();
        int attemptedWrites = attemptedWriteStreams.get();
        int createdWrites = writeStreamsCreated.get();
        int totalAttemptedStreams = attemptedReads + attemptedWrites;
        int totalSuccessfulStreams = createdReads + createdWrites;
        boolean readStreamsAllSucceeded = readStreamCompletionVerified();
        boolean readStreamsReachedEof = readStreamEofVerified();
        evidence.put("format", "cxnfs.quic-bulk-transport-evidence.v1");
        evidence.put("closed", closed.get());
        evidence.put("tls_version", "TLSv1.3");
        evidence.put("cipher_suite", TlsCipherSuite.TLS_AES_128_GCM_SHA256.javaName());
        evidence.put("connection_opened_count", 1);
        evidence.put("connection_attempt_count", 1);
        evidence.put("connection_retry_count", 0);
        evidence.put("successful_read_operations", successfulReadOperations.get());
        evidence.put("failed_read_operations", failedReadOperations.get());
        evidence.put("last_read_stream_count", lastReadStreamCount.get());
        evidence.put("last_attempted_read_stream_count", lastAttemptedReadStreamCount.get());
        evidence.put("attempted_read_streams", attemptedReads);
        evidence.put("read_streams_created", createdReads);
        evidence.put("read_streams_eof_verified", readStreamsEofVerified.get());
        evidence.put("read_streams_all_succeeded", readStreamsAllSucceeded);
        evidence.put("read_stream_completion_verified", readStreamsAllSucceeded);
        evidence.put("read_stream_eof_verified", readStreamsReachedEof);
        evidence.putAll(lastReadPartitionEvidence.get());
        evidence.put("attempted_write_streams", attemptedWrites);
        evidence.put("attempted_write_frames", attemptedWriteFrames.get());
        evidence.put("write_streams_created", createdWrites);
        evidence.put("total_attempted_streams", totalAttemptedStreams);
        evidence.put("total_successful_streams", totalSuccessfulStreams);
        evidence.put("connection_reused_for_streams", totalSuccessfulStreams > 1);
        evidence.put("last_read_error", lastReadError.get());
        return Collections.unmodifiableMap(evidence);
    }

    public Map<String, Object> writePathEvidence() {
        Map<String, Object> evidence = new LinkedHashMap<>();
        int attemptedReads = attemptedReadStreams.get();
        int createdReads = readStreamsCreated.get();
        int attemptedWrites = attemptedWriteStreams.get();
        int createdWrites = writeStreamsCreated.get();
        int totalAttemptedStreams = attemptedReads + attemptedWrites;
        int totalSuccessfulStreams = createdReads + createdWrites;
        boolean writeStreamsAllSucceeded = writeAckStreamCompletionVerified();
        boolean writeAckStreamsReachedEof = writeAckStreamEofVerified();
        int ackMetadataPending = writeAckMetadataPendingCount();
        evidence.put("format", "cxnfs.quic-bulk-write-transport-evidence.v1");
        evidence.put("closed", closed.get());
        evidence.put("tls_version", "TLSv1.3");
        evidence.put("cipher_suite", TlsCipherSuite.TLS_AES_128_GCM_SHA256.javaName());
        evidence.put("connection_opened_count", 1);
        evidence.put("connection_attempt_count", 1);
        evidence.put("connection_retry_count", 0);
        evidence.put("successful_write_operations", successfulWriteOperations.get());
        evidence.put("failed_write_operations", failedWriteOperations.get());
        evidence.put("attempted_write_streams", attemptedWrites);
        evidence.put("write_streams_created", createdWrites);
        evidence.put("write_ack_streams_eof_verified", writeAckStreamsEofVerified.get());
        evidence.put("write_streams_all_succeeded", writeStreamsAllSucceeded);
        evidence.put("write_ack_stream_completion_verified", writeStreamsAllSucceeded);
        evidence.put("write_ack_stream_eof_verified", writeAckStreamsReachedEof);
        evidence.put("write_acks_received", writeAcksReceived.get());
        evidence.put("write_acks_verified", writeAcksVerified.get());
        evidence.put("write_ack_verification_failures", writeAckVerificationFailures.get());
        evidence.put("write_ack_frame_identity_verified", writeAckFrameIdentityVerified());
        evidence.put("write_ack_metadata_verified", writeAckMetadataVerified.get());
        evidence.put("write_ack_metadata_pending", ackMetadataPending);
        evidence.put("write_ack_metadata_pending_observed", ackMetadataPending > 0);
        evidence.put("write_ack_memory", writeAckMemory.get());
        evidence.put("write_ack_spill", writeAckSpill.get());
        evidence.put("persistent_write_streams_open", persistentWriteStreams.size());
        evidence.put("write_ack_subscribe_streams_open", writeAckStreams.size());
        evidence.put("write_ack_subscribe_streams_created", writeAckSubscribeStreamsCreated.get());
        evidence.put("write_ack_subscribe_streams_ready", writeAckSubscribeStreamsReady.get());
        evidence.put("write_ack_subscribe_stream_failures", writeAckSubscribeStreamFailures.get());
        evidence.put("stream_send_buffer_bytes", STREAM_SEND_BUFFER_BYTES);
        evidence.put("stream_send_buffers_tuned", streamSendBuffersTuned.get());
        evidence.put("stream_send_buffer_tune_failures", streamSendBufferTuneFailures.get());
        evidence.put("persistent_write_stream_write_ahead", PERSISTENT_STREAM_WRITE_AHEAD);
        evidence.put("max_persistent_write_stream_depth", maxPersistentWriteStreamDepth.get());
        evidence.put("active_write_streams", activeWriteStreams.get());
        evidence.put("max_active_write_streams", maxActiveWriteStreams.get());
        evidence.put("concurrent_write_streams_observed", maxActiveWriteStreams.get() > 1);
        evidence.put("attempted_read_streams", attemptedReads);
        evidence.put("read_streams_created", createdReads);
        evidence.put("total_attempted_streams", totalAttemptedStreams);
        evidence.put("total_successful_streams", totalSuccessfulStreams);
        evidence.put("connection_reused_for_streams", totalSuccessfulStreams > 1);
        evidence.put("last_write_error", lastWriteError.get());
        return Collections.unmodifiableMap(evidence);
    }

    private long readSingleStream(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output
    ) throws IOException {
        QuicStream stream = createBidirectionalStream();
        OutputStream requestOutput = stream.getOutputStream();
        BulkReadRequestStream.write(requestOutput, BulkReadRequestCodec.encode(request));
        requestOutput.flush();
        requestOutput.close();

        BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(
                transfer,
                plan,
                output,
                0,
                request.activeCords()
        );
        lastReadPartitionEvidence.set(readPartitionEvidence(
                List.of(plan.chunks()),
                plan
        ));
        InputStream responseInput = stream.getInputStream();
        for (int index = 0; index < plan.chunks().size(); index++) {
            byte[] frameBytes = BulkFrameStream.read(responseInput)
                    .orElseThrow(() -> new IOException("QUIC bulk stream ended before read completed"));
            BulkDataFrame frame = BulkDataFrameCodec.decode(frameBytes);
            receiver.accept(frame);
        }
        verifyReadStreamComplete(responseInput);
        return receiver.complete();
    }

    private long readWithMultipleStreams(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output,
            int streamCount
    ) throws IOException {
        BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(
                transfer,
                plan,
                output,
                0,
                streamCount
        );
        List<List<PlannedChunkRead>> partitions = partitionChunks(plan.chunks(), streamCount);
        lastReadPartitionEvidence.set(readPartitionEvidence(partitions, plan));
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(partitions.size());
            for (List<PlannedChunkRead> partition : partitions) {
                BulkReadRequest partitionRequest = partitionRequest(request, partition, streamCount);
                futures.add(executor.submit(() -> {
                    readPartition(partitionRequest, transfer, partition, receiver);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                try {
                    awaitPartition(future);
                } catch (IOException | RuntimeException e) {
                    cancelPartitions(futures);
                    throw e;
                }
            }
        }
        return receiver.complete();
    }

    private void readPartition(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            List<PlannedChunkRead> chunks,
            BulkReadStreamingReceiver receiver
    ) throws IOException {
        QuicStream stream = createBidirectionalStream();
        OutputStream requestOutput = stream.getOutputStream();
        BulkReadRequestStream.write(requestOutput, BulkReadRequestCodec.encode(request));
        requestOutput.flush();
        requestOutput.close();

        InputStream responseInput = stream.getInputStream();
        for (int index = 0; index < chunks.size(); index++) {
            byte[] frameBytes = BulkFrameStream.read(responseInput)
                    .orElseThrow(() -> new IOException("QUIC bulk stream ended before read partition completed"));
            BulkDataFrame frame = BulkDataFrameCodec.decode(frameBytes);
            synchronized (receiver) {
                receiver.accept(frame);
            }
        }
        verifyReadStreamComplete(responseInput);
    }

    static void requireQuicReadStreamComplete(InputStream input) throws IOException {
        if (BulkFrameStream.read(input).isPresent()) {
            throw new IOException("QUIC bulk stream emitted extra frame after read completed");
        }
    }

    static void requireQuicWriteAckStreamComplete(InputStream input) throws IOException {
        if (BulkWriteAckStream.read(input).isPresent()) {
            throw new IOException("QUIC bulk stream emitted extra write ack after ack completed");
        }
    }

    static void requireQuicWriteAckMatchesFrame(BulkDataFrame frame, BulkWriteAck ack) throws IOException {
        if (!ack.key().equals(frame.key())) {
            throw new IOException("QUIC bulk write ack key does not match sent frame");
        }
        if (ack.payloadLength() != frame.payloadLength()) {
            throw new IOException("QUIC bulk write ack length does not match sent frame");
        }
    }

    private static void awaitPartition(Future<?> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while reading QUIC bulk partition", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("QUIC bulk partition failed", cause);
        }
    }

    private static BulkReadRequest boundReadRequest(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            ReadPlan plan
    ) {
        int activeCords = Math.max(1, Math.min(
                transfer.acceptedMaxCords(),
                Math.max(1, plan.chunks().size())
        ));
        if (activeCords == request.activeCords()) {
            return request;
        }
        return new BulkReadRequest(
                request.sessionId(),
                request.transferId(),
                request.path(),
                request.offset(),
                request.length(),
                activeCords
        );
    }

    private static List<List<PlannedChunkRead>> partitionChunks(List<PlannedChunkRead> chunks, int streamCount) {
        List<List<PlannedChunkRead>> partitions = new ArrayList<>(streamCount);
        int cursor = 0;
        for (int index = 0; index < streamCount; index++) {
            int remaining = chunks.size() - cursor;
            int remainingPartitions = streamCount - index;
            int partitionSize = (remaining + remainingPartitions - 1) / remainingPartitions;
            partitions.add(List.copyOf(chunks.subList(cursor, cursor + partitionSize)));
            cursor += partitionSize;
        }
        return partitions;
    }

    private static Map<String, Object> readPartitionEvidence(
            List<List<PlannedChunkRead>> partitions,
            ReadPlan plan
    ) {
        List<Map<String, Object>> entries = new ArrayList<>(partitions.size());
        long requestedBytes = 0;
        long payloadBytes = 0;
        int chunkCount = 0;
        for (int index = 0; index < partitions.size(); index++) {
            List<PlannedChunkRead> chunks = partitions.get(index);
            long partitionPayloadBytes = 0;
            for (PlannedChunkRead chunk : chunks) {
                partitionPayloadBytes += chunk.bytesToRead();
            }
            long offset = chunks.isEmpty() ? plan.requestedRange().offset() : chunks.get(0).readFileOffset();
            long length = chunks.isEmpty()
                    ? 0
                    : chunks.get(chunks.size() - 1).readFileOffset()
                    + chunks.get(chunks.size() - 1).bytesToRead()
                    - offset;
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("stream_index", index);
            entry.put("offset", offset);
            entry.put("length", length);
            entry.put("chunk_count", chunks.size());
            entry.put("payload_bytes", partitionPayloadBytes);
            entry.put("first_chunk_sequence", chunks.isEmpty() ? -1 : chunks.get(0).chunkIndex());
            entry.put("last_chunk_sequence", chunks.isEmpty() ? -1 : chunks.get(chunks.size() - 1).chunkIndex());
            entries.add(Collections.unmodifiableMap(entry));
            requestedBytes += length;
            payloadBytes += partitionPayloadBytes;
            chunkCount += chunks.size();
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("read_partition_count", partitions.size());
        evidence.put("read_partition_chunk_count", chunkCount);
        evidence.put("read_partition_requested_bytes", requestedBytes);
        evidence.put("read_partition_payload_bytes", payloadBytes);
        evidence.put("read_partition_covers_plan", chunkCount == plan.chunks().size()
                && payloadBytes == plan.requestedRange().length());
        evidence.put("read_partitions", List.copyOf(entries));
        return Collections.unmodifiableMap(evidence);
    }

    private static Map<String, Object> emptyReadPartitionEvidence() {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("read_partition_count", 0);
        evidence.put("read_partition_chunk_count", 0);
        evidence.put("read_partition_requested_bytes", 0L);
        evidence.put("read_partition_payload_bytes", 0L);
        evidence.put("read_partition_covers_plan", false);
        evidence.put("read_partitions", List.of());
        return Collections.unmodifiableMap(evidence);
    }

    private static BulkReadRequest partitionRequest(
            BulkReadRequest original,
            List<PlannedChunkRead> chunks,
            int activeCords
    ) {
        if (chunks.isEmpty()) {
            throw new IllegalArgumentException("chunks are required");
        }
        PlannedChunkRead first = chunks.get(0);
        PlannedChunkRead last = chunks.get(chunks.size() - 1);
        long offset = first.readFileOffset();
        long endExclusive = last.readFileOffset() + last.bytesToRead();
        return new BulkReadRequest(
                original.sessionId(),
                original.transferId(),
                original.path(),
                offset,
                endExclusive - offset,
                activeCords
        );
    }

    @Override
    public BulkWriteAck sendWriteFrame(byte[] frameBytes) throws IOException {
        if (frameBytes == null) {
            throw new IllegalArgumentException("frameBytes is required");
        }
        List<BulkWriteAck> acks = sendWriteFrames(List.of(frameBytes));
        if (acks.size() != 1) {
            throw new IOException("QUIC bulk write stream returned unexpected ack count: " + acks.size());
        }
        return acks.get(0);
    }

    @Override
    public List<BulkWriteAck> sendWriteFrames(List<byte[]> frameBytes) throws IOException {
        return sendWriteFrames(frameBytes, null);
    }

    @Override
    public BulkWriteAck sendWriteFrame(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        return sendWriteDataFrames(List.of(frame)).getFirst();
    }

    @Override
    public List<BulkWriteAck> sendWriteDataFrames(List<BulkDataFrame> frames) throws IOException {
        ensureOpen();
        if (frames == null) {
            throw new IllegalArgumentException("frames are required");
        }
        if (frames.isEmpty()) {
            return List.of();
        }
        attemptedWriteFrames.addAndGet(frames.size());
        List<BulkWriteAck> acks = new ArrayList<>(frames.size());
        List<BulkWriteFrameInfo> attemptedFrames = new ArrayList<>(frames.size());
        try {
            for (BulkDataFrame frame : frames) {
                BulkWriteFrameInfo info = BulkWriteFrameInfo.from(frame);
                attemptedFrames.add(info);
                BulkWriteAck ack = persistentWriteStream(frame.cordIndex()).send(frame, info);
                acks.add(ack);
                successfulWriteOperations.incrementAndGet();
            }
            lastWriteError.set("");
            return List.copyOf(acks);
        } catch (IOException | RuntimeException e) {
            failedWriteOperations.incrementAndGet();
            lastWriteError.set(errorDescription(e));
            closeAfterStreamFailure();
            throw describeWriteFrameFailure(attemptedFrames, e);
        }
    }

    @Override
    public List<BulkWriteAck> sendWriteFrames(
            List<byte[]> frameBytes,
            List<BulkWriteFrameInfo> expectedFrames
    ) throws IOException {
        ensureOpen();
        if (frameBytes == null) {
            throw new IllegalArgumentException("frameBytes are required");
        }
        if (expectedFrames != null && expectedFrames.size() != frameBytes.size()) {
            throw new IllegalArgumentException("expectedFrames must match frameBytes size");
        }
        if (frameBytes.isEmpty()) {
            return List.of();
        }
        attemptedWriteFrames.addAndGet(frameBytes.size());
        beginWriteStream();
        List<BulkWriteFrameInfo> frames = List.of();
        try {
            if (expectedFrames == null) {
                List<BulkWriteFrameInfo> decodedFrames = new ArrayList<>(frameBytes.size());
                for (byte[] bytes : frameBytes) {
                    decodedFrames.add(BulkWriteFrameInfo.from(BulkDataFrameCodec.decode(bytes)));
                }
                frames = List.copyOf(decodedFrames);
            } else {
                frames = List.copyOf(expectedFrames);
            }
            WriteAckStream ackStream = ensureWriteAckStream(frames.getFirst().key().transferId());
            List<AckWaiter> waiters = registerAckWaiters(ackStream, frames);
            QuicStream stream = createWriteDataStream();
            OutputStream requestOutput = stream.getOutputStream();
            try {
                for (byte[] bytes : frameBytes) {
                    BulkFrameStream.write(requestOutput, bytes);
                }
                requestOutput.flush();
            } finally {
                requestOutput.close();
            }
            List<BulkWriteAck> acks = awaitWriteAcks(waiters, frames);
            writeStreamsCreated.incrementAndGet();
            successfulWriteOperations.incrementAndGet();
            lastWriteError.set("");
            return List.copyOf(acks);
        } catch (IOException | RuntimeException e) {
            failedWriteOperations.incrementAndGet();
            lastWriteError.set(errorDescription(e));
            closeAfterStreamFailure();
            throw describeWriteFrameFailure(frames, e);
        } finally {
            finishWriteStream();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closePersistentWriteStreams();
            closeWriteAckStreams();
            connection.close();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("QUIC bulk client channel is closed");
        }
    }

    private QuicStream createBidirectionalStream() throws IOException {
        synchronized (streamCreateLock) {
            ensureOpen();
            QuicStream stream = connection.createStream(true);
            tuneStreamSendBuffer(stream);
            return stream;
        }
    }

    private QuicStream createWriteDataStream() throws IOException {
        QuicStream stream = createBidirectionalStream();
        stream.abortReading(0L);
        return stream;
    }

    private void tuneStreamSendBuffer(QuicStream stream) {
        if (QuicStreamSendBufferTuner.tune(stream, STREAM_SEND_BUFFER_BYTES)) {
            streamSendBuffersTuned.incrementAndGet();
        } else {
            streamSendBufferTuneFailures.incrementAndGet();
        }
    }

    private PersistentWriteStream persistentWriteStream(int cordIndex) throws IOException {
        PersistentWriteStream existing = persistentWriteStreams.get(cordIndex);
        if (existing != null && !existing.closed()) {
            return existing;
        }
        synchronized (persistentWriteStreams) {
            existing = persistentWriteStreams.get(cordIndex);
            if (existing != null && !existing.closed()) {
                return existing;
            }
            PersistentWriteStream created = openPersistentWriteStream(cordIndex);
            persistentWriteStreams.put(cordIndex, created);
            return created;
        }
    }

    private PersistentWriteStream openPersistentWriteStream(int cordIndex) throws IOException {
        beginWriteStream();
        try {
            PersistentWriteStream stream = new PersistentWriteStream(cordIndex, createWriteDataStream());
            writeStreamsCreated.incrementAndGet();
            return stream;
        } catch (IOException | RuntimeException e) {
            finishWriteStream();
            throw e;
        }
    }

    private void closePersistentWriteStreams() {
        List<PersistentWriteStream> streams = new ArrayList<>(persistentWriteStreams.values());
        persistentWriteStreams.clear();
        for (PersistentWriteStream stream : streams) {
            try {
                stream.close();
            } catch (IOException | RuntimeException ignored) {
                // Connection close follows; best effort evidence finalization only.
            }
        }
    }

    private WriteAckStream ensureWriteAckStream(UUID transferId) throws IOException {
        WriteAckStream existing = writeAckStreams.get(transferId);
        if (existing != null && !existing.closed()) {
            return existing;
        }
        synchronized (writeAckStreams) {
            existing = writeAckStreams.get(transferId);
            if (existing != null && !existing.closed()) {
                return existing;
            }
            WriteAckStream created = openWriteAckStream(transferId);
            writeAckStreams.put(transferId, created);
            return created;
        }
    }

    private WriteAckStream openWriteAckStream(UUID transferId) throws IOException {
        writeAckSubscribeStreamsCreated.incrementAndGet();
        try {
            WriteAckStream stream = new WriteAckStream(transferId, createBidirectionalStream());
            stream.start();
            writeAckSubscribeStreamsReady.incrementAndGet();
            return stream;
        } catch (IOException | RuntimeException e) {
            writeAckSubscribeStreamFailures.incrementAndGet();
            throw e;
        }
    }

    private void closeWriteAckStreams() {
        List<WriteAckStream> streams = new ArrayList<>(writeAckStreams.values());
        writeAckStreams.clear();
        for (WriteAckStream stream : streams) {
            stream.closeQuietly();
        }
    }

    private List<AckWaiter> registerAckWaiters(WriteAckStream ackStream, List<BulkWriteFrameInfo> frames)
            throws IOException {
        List<AckWaiter> waiters = new ArrayList<>(frames.size());
        try {
            for (BulkWriteFrameInfo frame : frames) {
                waiters.add(ackStream.register(frame));
            }
            return List.copyOf(waiters);
        } catch (IOException | RuntimeException e) {
            for (AckWaiter waiter : waiters) {
                ackStream.unregister(waiter);
            }
            throw e;
        }
    }

    private List<BulkWriteAck> awaitWriteAcks(List<AckWaiter> waiters, List<BulkWriteFrameInfo> frames)
            throws IOException {
        List<BulkWriteAck> acks = new ArrayList<>(waiters.size());
        for (int index = 0; index < waiters.size(); index++) {
            BulkWriteAck ack = waiters.get(index).await();
            verifyWriteAckMatchesFrame(frames.get(index), ack);
            writeAcksVerified.incrementAndGet();
            acks.add(ack);
        }
        return List.copyOf(acks);
    }

    private void beginWriteStream() {
        attemptedWriteStreams.incrementAndGet();
        int active = activeWriteStreams.incrementAndGet();
        maxActiveWriteStreams.accumulateAndGet(active, Math::max);
    }

    private void finishWriteStream() {
        activeWriteStreams.updateAndGet(value -> Math.max(0, value - 1));
    }

    private final class WriteAckStream implements AutoCloseable {
        private final UUID transferId;
        private final QuicStream stream;
        private final InputStream input;
        private final OutputStream output;
        private final ConcurrentMap<com.fortrow.cxnfs.transport.BulkDataFrameKey, AckWaiter> waiters =
                new ConcurrentHashMap<>();
        private final ConcurrentMap<com.fortrow.cxnfs.transport.BulkDataFrameKey, BulkWriteAck> pendingAcks =
                new ConcurrentHashMap<>();
        private volatile boolean closed;
        private Thread readerThread;

        private WriteAckStream(UUID transferId, QuicStream stream) {
            this.transferId = transferId;
            this.stream = stream;
            this.input = stream.getInputStream();
            this.output = stream.getOutputStream();
        }

        private void start() throws IOException {
            BulkTransferMessageStream.write(
                    output,
                    BulkWriteAckSubscriptionCodec.encodeSubscribe(new BulkWriteAckSubscription(transferId))
            );
            byte[] readyBytes = BulkWriteAckStream.read(input)
                    .orElseThrow(() -> new IOException("QUIC bulk write ack subscribe stream ended before ready"));
            UUID readyTransfer = BulkWriteAckSubscriptionCodec.decodeReady(readyBytes);
            if (!readyTransfer.equals(transferId)) {
                throw new IOException("QUIC bulk write ack subscribe ready transfer does not match");
            }
            readerThread = Thread.ofVirtual()
                    .name("cxnfs-quic-write-ack-" + transferId)
                    .start(this::readLoop);
        }

        private AckWaiter register(BulkWriteFrameInfo frame) throws IOException {
            requireOpen();
            AckWaiter waiter = new AckWaiter(frame);
            AckWaiter prior = waiters.putIfAbsent(frame.key(), waiter);
            if (prior != null) {
                throw new IOException("duplicate in-flight QUIC write ack waiter for " + frame.key());
            }
            BulkWriteAck pending = pendingAcks.remove(frame.key());
            if (pending != null) {
                waiters.remove(frame.key(), waiter);
                waiter.complete(pending);
            }
            return waiter;
        }

        private void unregister(AckWaiter waiter) {
            if (waiter != null) {
                waiters.remove(waiter.frame().key(), waiter);
            }
        }

        private boolean closed() {
            return closed;
        }

        private void requireOpen() throws IOException {
            if (closed) {
                throw new IOException("QUIC bulk write ack stream is closed for transfer " + transferId);
            }
        }

        private void readLoop() {
            try {
                while (!closed) {
                    Optional<byte[]> ackBytes = BulkWriteAckStream.read(input);
                    if (ackBytes.isEmpty()) {
                        break;
                    }
                    BulkWriteAck ack = BulkWriteAckCodec.decode(ackBytes.orElseThrow());
                    recordWriteAckReceived(ack);
                    AckWaiter waiter = waiters.remove(ack.key());
                    if (waiter == null) {
                        pendingAcks.put(ack.key(), ack);
                    } else {
                        waiter.complete(ack);
                    }
                }
                closeWaiters(new IOException("QUIC bulk write ack stream ended"));
            } catch (IOException | RuntimeException e) {
                if (!closed) {
                    writeAckVerificationFailures.incrementAndGet();
                    closeWaiters(e);
                }
            } finally {
                closed = true;
                writeAckStreams.remove(transferId, this);
            }
        }

        private void closeWaiters(Throwable failure) {
            for (AckWaiter waiter : waiters.values()) {
                waiter.fail(failure);
            }
            waiters.clear();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            try {
                output.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                input.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (readerThread != null) {
                readerThread.interrupt();
            }
            closeWaiters(new IOException("QUIC bulk write ack stream closed"));
            if (failure != null) {
                throw failure;
            }
        }

        private void closeQuietly() {
            try {
                close();
            } catch (IOException | RuntimeException ignored) {
                // Connection close follows; best effort ACK stream shutdown only.
            }
        }
    }

    private static final class AckWaiter {
        private final BulkWriteFrameInfo frame;
        private final CompletableFuture<BulkWriteAck> future = new CompletableFuture<>();

        private AckWaiter(BulkWriteFrameInfo frame) {
            this.frame = frame;
        }

        private BulkWriteFrameInfo frame() {
            return frame;
        }

        private void complete(BulkWriteAck ack) {
            future.complete(ack);
        }

        private void fail(Throwable failure) {
            future.completeExceptionally(failure);
        }

        private BulkWriteAck await() throws IOException {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for QUIC bulk write ack", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException ioException) {
                    throw ioException;
                }
                if (cause instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new IOException("QUIC bulk write ack wait failed", cause);
            }
        }
    }

    private final class PersistentWriteStream implements AutoCloseable {
        private final int cordIndex;
        private final QuicStream stream;
        private final InputStream input;
        private final OutputStream output;
        private final Semaphore writeCredits = new Semaphore(PERSISTENT_STREAM_WRITE_AHEAD);
        private final Object writeLock = new Object();
        private final Object ackLock = new Object();
        private long nextWriteOrder;
        private long nextAckOrder;
        private int outstandingWrites;
        private volatile boolean closed;

        private PersistentWriteStream(int cordIndex, QuicStream stream) {
            this.cordIndex = cordIndex;
            this.stream = stream;
            this.input = stream.getInputStream();
            this.output = stream.getOutputStream();
        }

        private BulkWriteAck send(BulkDataFrame frame, BulkWriteFrameInfo expectedFrame)
                throws IOException {
            WriteAckStream ackStream = ensureWriteAckStream(frame.transferId());
            AckWaiter waiter = ackStream.register(expectedFrame);
            acquireWriteCredit();
            long writeOrder = -1L;
            try {
                synchronized (writeLock) {
                    requirePersistentStreamOpen();
                    writeOrder = nextWriteOrder++;
                    outstandingWrites++;
                    maxPersistentWriteStreamDepth.accumulateAndGet(outstandingWrites, Math::max);
                    BulkFrameStream.write(output, frame);
                }
                BulkWriteAck ack = waiter.await();
                verifyWriteAckMatchesFrame(expectedFrame, ack);
                writeAcksVerified.incrementAndGet();
                return ack;
            } catch (IOException | RuntimeException e) {
                ackStream.unregister(waiter);
                closeQuietly();
                persistentWriteStreams.remove(cordIndex, this);
                throw e;
            } finally {
                if (writeOrder >= 0L) {
                    synchronized (writeLock) {
                        outstandingWrites = Math.max(0, outstandingWrites - 1);
                    }
                }
                writeCredits.release();
            }
        }

        private void acquireWriteCredit() throws IOException {
            try {
                writeCredits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for QUIC write credit on cord " + cordIndex, e);
            }
        }

        private BulkWriteAck readAndVerifyAckInWriteOrder(long writeOrder, BulkWriteFrameInfo expectedFrame)
                throws IOException {
            synchronized (ackLock) {
                while (writeOrder != nextAckOrder) {
                    requirePersistentStreamOpen();
                    try {
                        ackLock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted waiting for QUIC ack order on cord " + cordIndex, e);
                    }
                }
                try {
                    BulkWriteAck ack = readWriteAck(input);
                    recordWriteAckReceived(ack);
                    verifyWriteAckMatchesFrame(expectedFrame, ack);
                    writeAcksVerified.incrementAndGet();
                    return ack;
                } finally {
                    nextAckOrder++;
                    ackLock.notifyAll();
                }
            }
        }

        private void requirePersistentStreamOpen() throws IOException {
            if (closed) {
                throw new IOException("persistent QUIC bulk write stream is closed for cord " + cordIndex);
            }
        }

        private synchronized boolean closed() {
            return closed;
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            synchronized (ackLock) {
                ackLock.notifyAll();
            }
            IOException failure = null;
            try {
                output.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                verifyWriteAckStreamComplete(input);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            finishWriteStream();
            if (failure != null) {
                throw failure;
            }
        }

        private void closeQuietly() {
            try {
                close();
            } catch (IOException | RuntimeException ignored) {
                // The caller receives the original stream failure.
            }
        }
    }

    private BulkWriteAck readWriteAck(InputStream input) throws IOException {
        try {
            byte[] ackBytes = BulkWriteAckStream.read(input)
                    .orElseThrow(() -> new IOException("QUIC bulk stream ended before ack"));
            return BulkWriteAckCodec.decode(ackBytes);
        } catch (IOException | RuntimeException e) {
            writeAckVerificationFailures.incrementAndGet();
            throw e;
        }
    }

    private List<BulkWriteAck> readWriteAcks(InputStream input, List<BulkWriteFrameInfo> frames) throws IOException {
        List<BulkWriteAck> acks = new ArrayList<>(frames.size());
        for (BulkWriteFrameInfo frame : frames) {
            BulkWriteAck ack = readWriteAck(input);
            recordWriteAckReceived(ack);
            verifyWriteAckMatchesFrame(frame, ack);
            writeAcksVerified.incrementAndGet();
            acks.add(ack);
        }
        verifyWriteAckStreamComplete(input);
        return List.copyOf(acks);
    }

    private static List<BulkWriteAck> awaitWriteAcks(Future<List<BulkWriteAck>> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for QUIC bulk write acks", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("QUIC bulk write ack reader failed", cause);
        }
    }

    private void verifyWriteAckMatchesFrame(BulkWriteFrameInfo frame, BulkWriteAck ack) throws IOException {
        try {
            frame.verify(ack);
        } catch (IOException | RuntimeException e) {
            writeAckVerificationFailures.incrementAndGet();
            throw e;
        }
    }

    private void verifyWriteAckStreamComplete(InputStream input) throws IOException {
        try {
            requireQuicWriteAckStreamComplete(input);
            writeAckStreamsEofVerified.incrementAndGet();
        } catch (IOException | RuntimeException e) {
            writeAckVerificationFailures.incrementAndGet();
            throw e;
        }
    }

    private void verifyReadStreamComplete(InputStream input) throws IOException {
        requireQuicReadStreamComplete(input);
        readStreamsEofVerified.incrementAndGet();
    }

    private void recordWriteAckReceived(BulkWriteAck ack) {
        writeAcksReceived.incrementAndGet();
        if (ack.metadataVerified()) {
            writeAckMetadataVerified.incrementAndGet();
        }
        if (ack.storage() == StagedPayloadStorage.MEMORY) {
            writeAckMemory.incrementAndGet();
        } else if (ack.storage() == StagedPayloadStorage.SPILL) {
            writeAckSpill.incrementAndGet();
        }
    }

    private void closeAfterStreamFailure() {
        close();
    }

    private static void cancelPartitions(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    private static String errorDescription(Throwable failure) {
        return failure.getClass().getSimpleName() + ": " + failure.getMessage();
    }

    private static IOException describeWriteFrameFailure(List<BulkWriteFrameInfo> frames, Throwable failure) {
        String message = "QUIC bulk write failed";
        message += BulkWriteFrameInfo.describeRange(frames);
        message += ": " + failure.getMessage();
        return failure instanceof IOException
                ? new IOException(message, failure)
                : new IOException(message, failure);
    }
}
