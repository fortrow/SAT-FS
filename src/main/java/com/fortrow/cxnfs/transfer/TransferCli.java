package com.fortrow.cxnfs.transfer;

import com.fortrow.cxnfs.checksum.TransferChecksums;
import com.fortrow.cxnfs.client.BulkTransferClientChannel;
import com.fortrow.cxnfs.client.ClientCredentialPrompt;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.client.ClientTlsContextFactory;
import com.fortrow.cxnfs.client.ControlTlsClientSocketFactory;
import com.fortrow.cxnfs.client.CxnFileNetworkClient;
import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import com.fortrow.cxnfs.client.EncodedWriteFrame;
import com.fortrow.cxnfs.client.QuicBulkTransferClientChannel;
import com.fortrow.cxnfs.client.StripedBulkTransferClientChannel;
import com.fortrow.cxnfs.client.WriteFramePipeline;
import com.fortrow.cxnfs.client.WritePipelineMetrics;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryAction;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.protocol.TransferChecksum;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkPayloadPatternCodec;
import com.fortrow.cxnfs.transport.TlsCipherSuite;
import com.fortrow.cxnfs.transport.WriteCordRamp;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

public final class TransferCli {
    private static final int DEFAULT_CONTROL_PORT = 4509;
    private static final int DEFAULT_TRANSPORT_PORT = 4510;
    private static final int MIN_DIRECT_WRITE_FRAME_BYTES = 1 * 1024 * 1024;
    private static final int MAX_DIRECT_WRITE_FRAME_BYTES = 8 * 1024 * 1024;
    private static final int RAW_FRAME_BATCH_SIZE = 2;
    private static final int RAW_FRAME_WRITE_AHEAD = 2;
    private static final int PATTERN_SAMPLE_BYTES = 8 * 1024 * 1024;
    private static final int PATTERN_FRAME_BATCH_SIZE = 64;
    private static final int[] SYMBOL_LENGTHS = {4096, 2048, 1024, 512, 256, 128, 64};
    private static final int SLIDING_WINDOW_MULTIPLIER = 2;
    private static final long MIN_READ_AHEAD_BYTES = 64L * 1024 * 1024;
    private static final long MAX_READ_AHEAD_BYTES = 64L * 1024 * 1024;
    private static final long MIN_DIRECT_WRITE_PIPELINE_WINDOW_BYTES = 128L * 1024 * 1024;
    private static final long MAX_DIRECT_WRITE_PIPELINE_WINDOW_BYTES = 2L * 1024 * 1024 * 1024;
    private static final long PROGRESS_UPDATE_INTERVAL_NANOS = 100_000_000L;
    private static final int PROGRESS_BAR_WIDTH = 28;

    public int run(String commandGroup, String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(commandGroup, out);
            return 0;
        }
        try {
            return switch (args[0]) {
                case "send" -> send(commandGroup, args, out, err);
                case "read" -> read(commandGroup, args, out, err);
                default -> {
                    err.println("unknown " + commandGroup + " command: " + args[0]);
                    usage(commandGroup, err);
                    yield 2;
                }
            };
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println("cxnfs " + commandGroup + " " + args[0] + " failed: " + e.getMessage());
            return 1;
        }
    }

    private int send(String commandGroup, String[] args, PrintStream out, PrintStream err) throws IOException {
        requireArity(commandGroup, args, 3, "send");
        Map<String, String> options = parseOptions(args, 3);
        ClientOptions clientOptions = clientOptions(options);
        Path input = Path.of(args[1]).toAbsolutePath().normalize();
        String remoteOutput = normalizeRemotePath(args[2]);
        List<SendEntry> entries = switch (commandGroup) {
            case "file" -> List.of(fileEntry(input, remoteOutput, 0));
            case "directory" -> directoryEntries(input, remoteOutput);
            default -> throw new IllegalArgumentException("unsupported transfer command group: " + commandGroup);
        };
        long expectedBytes = sendTotalBytes(entries);
        String transferRoot = commandGroup.equals("file") ? remoteOutput : remoteOutput;
        try (ClientCredentials credentials = ClientCredentialPrompt.prompt()) {
            try (ConnectedClient connection = connect(clientOptions)) {
                CxnFileNetworkClient client = connection.client();
                client.authenticate(credentials);
                ProcessCpuSample cpuStart = ProcessCpuSample.capture();
                SendSummary summary;
                try (ProgressReporter progress = ProgressReporter.create(
                        err,
                        "cxnfs " + commandGroup + " send",
                        expectedBytes
                )) {
                    summary = sendEntries(
                            client,
                            transferRoot,
                            entries,
                            effectiveCordLaneMultiplier(connection),
                            progress
                    );
                    progress.finish();
                }
                ProcessCpuUsage cpuUsage = cpuStart.elapsed();
                out.println("cxnfs " + commandGroup + " send");
                out.println("host=" + clientOptions.host());
                out.println("control_port=" + clientOptions.controlPort());
                out.println("transport_port=" + clientOptions.transportPort());
                out.println("bulk_transport_preference=" + clientOptions.bulkTransport().optionName());
                out.println("bulk_transport=" + connection.bulkTransport().optionName());
                out.println("quic_connections=" + connection.quicConnections());
                out.println("accepted_initial_cords=" + summary.acceptedInitialCords());
                out.println("accepted_max_cords=" + summary.acceptedMaxCords());
                out.println("entries=" + summary.entries());
                out.println("bytes=" + summary.bytes());
                out.println("transfer_hash=" + summary.transferHash());
                out.println("checksum_algorithm=crc32c");
                out.println("frames=" + summary.frames());
                out.println("frame_payload_bytes=" + summary.framePayloadBytes());
                out.println("raw_frame_batch_size=" + RAW_FRAME_BATCH_SIZE);
                out.println("raw_frame_write_ahead=" + RAW_FRAME_WRITE_AHEAD);
                out.println("pattern_frames=" + summary.patternFrames());
                out.println("pattern_wire_bytes=" + summary.patternWireBytes());
                out.println("total_ms=" + millis(summary.timing().totalNanos()));
                out.println("prepare_ms=" + millis(summary.timing().prepareNanos()));
                out.println("pattern_prepare_ms=" + millis(summary.timing().patternPrepareNanos()));
                out.println("frame_prepare_ms=" + millis(summary.timing().framePrepareNanos()));
                out.println("host_write_ms=" + millis(summary.timing().hostWriteNanos()));
                out.println("bootstrap_ms=" + millis(summary.timing().bootstrapNanos()));
                out.println("entry_ms=" + millis(summary.timing().entryNanos()));
                out.println("metadata_ms=" + millis(summary.timing().metadataNanos()));
                out.println("entry_commit_ms=" + millis(summary.timing().entryCommitNanos()));
                out.println("transfer_commit_ms=" + millis(summary.timing().transferCommitNanos()));
                out.println("process_cpu_ms=" + cpuMillis(cpuUsage.cpuNanos()));
                out.println("process_cpu_cores_avg=" + cpuCores(cpuUsage.cpuNanos(), summary.timing().totalNanos()));
                out.println("pipeline_wait_ms=" + millis(summary.metrics().capacityWaitNanos()));
                out.println("ack_mb_s=" + megabytesPerSecond(summary.metrics().acknowledgedBytes(), summary.metrics().ackNanos()));
                out.println("max_active_cords=" + summary.metrics().maxActiveCords());
                out.println("max_inflight_frames=" + summary.metrics().maxInFlightFrames());
                out.println("max_inflight_mb=" + String.format(Locale.ROOT, "%.1f", summary.metrics().maxInFlightBytes() / 1_000_000.0d));
                out.println("max_adaptive_window_mb=" + String.format(Locale.ROOT, "%.1f", summary.metrics().maxAdaptiveWindowBytes() / 1_000_000.0d));
                out.println("max_frame_ack_ms=" + millis(summary.metrics().maxFrameAckNanos()));
                out.println("slow_frames=" + summary.metrics().slowFrames());
                out.println("window_decreases=" + summary.metrics().windowDecreases());
                return 0;
            }
        }
    }

    private int read(String commandGroup, String[] args, PrintStream out, PrintStream err) throws IOException {
        requireArity(commandGroup, args, 3, "read");
        Map<String, String> options = parseOptions(args, 3);
        ClientOptions clientOptions = clientOptions(options);
        String remoteInput = normalizeRemotePath(args[1]);
        Path output = Path.of(args[2]).toAbsolutePath().normalize();
        try (ClientCredentials credentials = ClientCredentialPrompt.prompt()) {
            try (ConnectedClient connection = connect(clientOptions)) {
                CxnFileNetworkClient client = connection.client();
                client.authenticate(credentials);
                long bytes;
                switch (commandGroup) {
                    case "file" -> {
                        FileMetadata metadata = regularFileMetadata(client, remoteInput);
                        try (ProgressReporter progress = ProgressReporter.create(
                                err,
                                "cxnfs file read",
                                metadata.size()
                        )) {
                            bytes = readFile(client, remoteInput, output, metadata.size(), progress);
                            progress.finish();
                        }
                    }
                    case "directory" -> {
                        ReadDirectoryPlan plan = readDirectoryPlan(client, remoteInput, output);
                        try (ProgressReporter progress = ProgressReporter.create(
                                err,
                                "cxnfs directory read",
                                plan.bytes()
                        )) {
                            bytes = readDirectory(client, plan, progress);
                            progress.finish();
                        }
                    }
                    default -> throw new IllegalArgumentException("unsupported transfer command group: " + commandGroup);
                }
                out.println("cxnfs " + commandGroup + " read");
                out.println("host=" + clientOptions.host());
                out.println("bulk_transport_preference=" + clientOptions.bulkTransport().optionName());
                out.println("bulk_transport=" + connection.bulkTransport().optionName());
                out.println("quic_connections=" + connection.quicConnections());
                out.println("bytes=" + bytes);
                return 0;
            }
        }
    }

    private static SendSummary sendEntries(
            CxnFileNetworkClient client,
            String transferRoot,
            List<SendEntry> entries,
            int cordLaneMultiplier,
            ProgressReporter progress
    ) throws IOException {
        long totalStart = System.nanoTime();
        SendTiming timing = SendTiming.empty();
        long phaseStart = System.nanoTime();
        TransferAcceptedResponse transfer = client.bootstrapWrite(transferRoot, TransferPayloadFormat.CONTAINER);
        timing = timing.plusBootstrap(System.nanoTime() - phaseStart);
        if (transfer.direction() != TransferDirection.WRITE || !transfer.writePayloadAdmissionGranted()) {
            throw new IOException("write transfer was not admitted");
        }
        List<ContentHash> entryHashes = new ArrayList<>(entries.size());
        List<Long> entrySizes = new ArrayList<>(entries.size());
        long totalBytes = 0L;
        long frames = 0L;
        long patternFrames = 0L;
        long patternWireBytes = 0L;
        int framePayloadBytes = directWriteFrameBytes(
                transfer.flowControlWindowBytes(),
                transfer.acceptedMaxCords(),
                cordLaneMultiplier,
                RAW_FRAME_WRITE_AHEAD
        );
        WritePipelineMetrics metrics = WritePipelineMetrics.empty();
        try (WriteFramePipeline pipeline = new WriteFramePipeline(
                client,
                effectiveFlowControlWindowBytes(
                        transfer.flowControlWindowBytes(),
                        transfer.acceptedMaxCords(),
                        cordLaneMultiplier,
                        framePayloadBytes,
                        RAW_FRAME_WRITE_AHEAD
                ),
                progress::add
        )) {
            for (SendEntry entry : entries) {
                SentEntry sent = sendEntry(
                        client,
                        transfer,
                        pipeline,
                        entry,
                        framePayloadBytes,
                        cordLaneMultiplier
                );
                entryHashes.add(sent.contentHash());
                entrySizes.add(entry.size());
                totalBytes += entry.size();
                frames += sent.frames();
                patternFrames += sent.patternFrames();
                patternWireBytes += sent.patternWireBytes();
                timing = timing.plus(sent.timing());
            }
            metrics = pipeline.metrics();
        }
        ContentHash transferHash = Hashing.transfer(entryHashes, entrySizes);
        phaseStart = System.nanoTime();
        TransferCommittedResponse transferCommitted = client.commitWriteTransfer(new TransferCommitRequest(
                transfer.transferId(),
                transferRoot,
                transferHash,
                totalBytes
        ));
        timing = timing.plusTransferCommit(System.nanoTime() - phaseStart);
        requireTransferCommitted(transferCommitted, transfer, transferHash, totalBytes);
        timing = timing.withTotal(System.nanoTime() - totalStart);
        return new SendSummary(
                entries.size(),
                totalBytes,
                transferHash,
                frames,
                patternFrames,
                patternWireBytes,
                framePayloadBytes,
                transfer.acceptedInitialCords(),
                transfer.acceptedMaxCords(),
                metrics,
                timing
        );
    }

    private static SentEntry sendEntry(
            CxnFileNetworkClient client,
            TransferAcceptedResponse transfer,
            WriteFramePipeline pipeline,
            SendEntry entry,
            int maxPayloadBytes,
            int cordLaneMultiplier
    ) throws IOException {
        SendTiming timing = SendTiming.empty();
        long phaseStart = System.nanoTime();
        TransferEntryAcceptedResponse entryAccepted = client.acceptWriteEntry(new TransferEntryUpdate(
                transfer.transferId(),
                entry.entrySequence(),
                TransferEntryAction.CREATE_OR_REPLACE,
                entry.remotePath(),
                "",
                entry.size(),
                TransferPayloadFormat.CONTAINER
        ));
        timing = timing.plusEntry(System.nanoTime() - phaseStart);
        requireEntryAccepted(entryAccepted, transfer, entry);

        List<ContentHash> chunkHashes = new ArrayList<>();
        List<Integer> chunkSizes = new ArrayList<>();
        long frames;
        long patternFrames;
        long patternWireBytes;
        WriteCordRamp cordRamp = new WriteCordRamp(transfer, entry.size());
        SentPayload payload = sendReadAheadPayload(
                transfer,
                pipeline,
                entry,
                maxPayloadBytes,
                cordRamp,
                cordLaneMultiplier,
                chunkHashes,
                chunkSizes
        );
        frames = payload.frames();
        patternFrames = payload.patternFrames();
        patternWireBytes = payload.patternWireBytes();
        timing = timing.plusPatternPrepare(payload.patternPrepareNanos());
        timing = timing.plusFramePrepare(payload.framePrepareNanos());
        timing = timing.plusHostWrite(hostWriteNanos(payload.firstHostWriteNanos()));
        phaseStart = System.nanoTime();
        pipeline.complete();
        timing = timing.plusHostWrite(System.nanoTime() - phaseStart);

        ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
        phaseStart = System.nanoTime();
        TransferMetadataAcceptedResponse metadataAccepted = client.acceptWriteMetadata(new TransferMetadataUpdate(
                transfer.transferId(),
                entry.entrySequence(),
                0,
                true,
                entry.size(),
                chunkHashes
        ));
        timing = timing.plusMetadata(System.nanoTime() - phaseStart);
        requireMetadataAccepted(metadataAccepted, transfer, entry, chunkHashes.size());
        phaseStart = System.nanoTime();
        TransferEntryCommittedResponse entryCommitted = client.commitWriteEntry(new TransferEntryCommitRequest(
                transfer.transferId(),
                entry.entrySequence(),
                entry.remotePath(),
                fileHash,
                entry.size(),
                payload.checksum()
        ));
        timing = timing.plusEntryCommit(System.nanoTime() - phaseStart);
        requireEntryCommitted(entryCommitted, transfer, entry, fileHash);
        return new SentEntry(fileHash, frames, patternFrames, patternWireBytes, payload.checksum(), timing);
    }

    private static long hostWriteNanos(long firstHostWriteNanos) {
        return firstHostWriteNanos == 0L ? 0L : System.nanoTime() - firstHostWriteNanos;
    }

    private static SentPayload sendReadAheadPayload(
            TransferAcceptedResponse transfer,
            WriteFramePipeline pipeline,
            SendEntry entry,
            int maxPayloadBytes,
            WriteCordRamp cordRamp,
            int cordLaneMultiplier,
            List<ContentHash> chunkHashes,
            List<Integer> chunkSizes
    ) throws IOException {
        BlockingQueue<ReadAheadPayload> queue = new ArrayBlockingQueue<>(
                readAheadFrameCapacity(entry.size(), maxPayloadBytes, transfer.flowControlWindowBytes())
        );
        Thread producer = Thread.ofVirtual()
                .name("cxnfs-transfer-read-ahead-" + entry.entrySequence())
                .start(() -> produceReadAheadPayloads(entry, maxPayloadBytes, queue));

        long frames = 0L;
        long patternFrames = 0L;
        long patternWireBytes = 0L;
        long framePrepareNanos = 0L;
        long patternPrepareNanos = 0L;
        long firstHostWriteNanos = 0L;
        TransferChecksum checksum = TransferChecksum.none();
        List<EncodedWriteFrame> encodedBatch = new ArrayList<>(PATTERN_FRAME_BATCH_SIZE);
        List<BulkDataFrame> rawBatch = new ArrayList<>(RAW_FRAME_BATCH_SIZE);
        long encodedBatchPayloadBytes = 0L;
        long rawBatchPayloadBytes = 0L;
        boolean completed = false;
        try {
            while (true) {
                ReadAheadPayload prepared = takeReadAheadPayload(queue);
                if (prepared.failure() != null) {
                    throw readAheadFailure(prepared.failure());
                }
                if (prepared.end()) {
                    flushRawBatch(
                            pipeline,
                            rawBatch,
                            rawBatchPayloadBytes,
                            effectiveActiveCords(cordRamp.activeCords(), cordLaneMultiplier)
                    );
                    rawBatch.clear();
                    rawBatchPayloadBytes = 0L;
                    flushEncodedBatch(pipeline, encodedBatch, encodedBatchPayloadBytes);
                    encodedBatch.clear();
                    encodedBatchPayloadBytes = 0L;
                    checksum = prepared.checksum();
                    completed = true;
                    break;
                }
                chunkHashes.add(prepared.chunkHash());
                chunkSizes.add(prepared.payloadLength());
                framePrepareNanos += prepared.framePrepareNanos();
                patternPrepareNanos += prepared.patternPrepareNanos();
                byte[] encodedFrame = null;
                Optional<BulkPayloadPatternCodec.EncodedPayload> encodedPayload = prepared.encodedPayload();
                int activeCords = cordRamp.activeCords();
                int cordIndex = cordIndex(prepared.chunkSequence(), activeCords, cordLaneMultiplier);
                BulkDataFrame frame = prepared.payloadPattern().isPresent()
                        ? new BulkDataFrame(
                                transfer.transferId(),
                                cordIndex,
                                entry.entrySequence(),
                                prepared.chunkSequence(),
                                prepared.offset(),
                                true,
                                prepared.payloadLength(),
                                prepared.payloadPattern().orElseThrow()
                        )
                        : BulkDataFrame.wrapPayloadForTransport(
                                transfer.transferId(),
                                cordIndex,
                                entry.entrySequence(),
                                prepared.chunkSequence(),
                                prepared.offset(),
                                true,
                                prepared.payload()
                        );
                if (encodedPayload.isPresent()) {
                    long patternEncodeStart = System.nanoTime();
                    encodedFrame = BulkDataFrameCodec.encodeWithPayload(frame, encodedPayload.orElseThrow());
                    patternPrepareNanos += System.nanoTime() - patternEncodeStart;
                    patternFrames++;
                    patternWireBytes += encodedFrame.length;
                }
                if (firstHostWriteNanos == 0L) {
                    firstHostWriteNanos = System.nanoTime();
                }
                if (encodedFrame == null) {
                    flushEncodedBatch(pipeline, encodedBatch, encodedBatchPayloadBytes);
                    encodedBatch.clear();
                    encodedBatchPayloadBytes = 0L;
                    rawBatch.add(frame);
                    rawBatchPayloadBytes += prepared.payloadLength();
                    if (rawBatch.size() >= Math.min(
                            RAW_FRAME_BATCH_SIZE,
                            effectiveActiveCords(activeCords, cordLaneMultiplier)
                    )) {
                        flushRawBatch(
                                pipeline,
                                rawBatch,
                                rawBatchPayloadBytes,
                                effectiveActiveCords(activeCords, cordLaneMultiplier)
                        );
                        rawBatch.clear();
                        rawBatchPayloadBytes = 0L;
                    }
                } else {
                    flushRawBatch(
                            pipeline,
                            rawBatch,
                            rawBatchPayloadBytes,
                            effectiveActiveCords(activeCords, cordLaneMultiplier)
                    );
                    rawBatch.clear();
                    rawBatchPayloadBytes = 0L;
                    encodedBatch.add(new EncodedWriteFrame(frame, encodedFrame));
                    encodedBatchPayloadBytes += prepared.payloadLength();
                    if (encodedBatch.size() >= PATTERN_FRAME_BATCH_SIZE) {
                        flushEncodedBatch(pipeline, encodedBatch, encodedBatchPayloadBytes);
                        encodedBatch.clear();
                        encodedBatchPayloadBytes = 0L;
                    }
                }
                frames++;
            }
        } finally {
            if (!completed) {
                producer.interrupt();
            }
            joinReadAheadProducer(producer);
        }
        return new SentPayload(
                frames,
                patternFrames,
                patternWireBytes,
                framePrepareNanos,
                patternPrepareNanos,
                firstHostWriteNanos,
                checksum
        );
    }

    private static void flushRawBatch(
            WriteFramePipeline pipeline,
            List<BulkDataFrame> rawBatch,
            long rawBatchPayloadBytes,
            int activeCords
    ) throws IOException {
        if (rawBatch.isEmpty()) {
            return;
        }
        int activeCordCount = Math.max(1, activeCords);
        int frameWindow = Math.max(
                saturatedMultiplyToInt(activeCordCount, RAW_FRAME_WRITE_AHEAD),
                rawBatch.size()
        );
        pipeline.awaitCapacity(activeCordCount, frameWindow, intPayloadBytes(rawBatchPayloadBytes));
        if (rawBatch.size() == 1) {
            pipeline.submit(rawBatch.getFirst());
        } else {
            pipeline.submitBatch(rawBatch);
        }
    }

    private static void flushEncodedBatch(
            WriteFramePipeline pipeline,
            List<EncodedWriteFrame> encodedBatch,
            long encodedBatchPayloadBytes
    ) throws IOException {
        if (encodedBatch.isEmpty()) {
            return;
        }
        int activeCords = Math.max(encodedBatch.size(), 1);
        pipeline.awaitCapacity(activeCords, intPayloadBytes(encodedBatchPayloadBytes));
        pipeline.submitEncodedBatch(encodedBatch);
    }

    private static int intPayloadBytes(long bytes) {
        if (bytes <= 0L) {
            return 1;
        }
        return bytes > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) bytes;
    }

    private static int effectiveCordLaneMultiplier(ConnectedClient connection) {
        return Math.max(1, connection.quicConnections());
    }

    private static int effectiveActiveCords(int activeCords, int cordLaneMultiplier) {
        long effective = (long) Math.max(1, activeCords) * Math.max(1, cordLaneMultiplier);
        return effective > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) effective;
    }

    private static int cordIndex(long chunkSequence, int activeCords, int cordLaneMultiplier) {
        int boundedActiveCords = Math.max(1, activeCords);
        int boundedLaneMultiplier = Math.max(1, cordLaneMultiplier);
        long laneLocalSequence = chunkSequence / boundedLaneMultiplier;
        return Math.floorMod(laneLocalSequence, boundedActiveCords);
    }

    private static int directWriteFrameBytes(
            long flowControlWindowBytes,
            int acceptedMaxCords,
            int cordLaneMultiplier,
            int writeAhead
    ) {
        long lanes = saturatedMultiply(
                (long) Math.max(1, acceptedMaxCords),
                Math.max(1, cordLaneMultiplier)
        );
        long target = heapSafeDirectWriteWindowBytes(
                flowControlWindowBytes,
                cordLaneMultiplier
        ) / Math.max(1L, lanes);
        target = Math.min(target, Math.max(1L, flowControlWindowBytes));
        target = Math.max(MIN_DIRECT_WRITE_FRAME_BYTES, Math.min(MAX_DIRECT_WRITE_FRAME_BYTES, target));
        target = Math.min(target, BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
        return (int) floorPowerOfTwo(target);
    }

    private static long effectiveFlowControlWindowBytes(
            long flowControlWindowBytes,
            int acceptedMaxCords,
            int cordLaneMultiplier,
            int framePayloadBytes,
            int writeAhead
    ) {
        long requested = saturatedMultiply(
                (long) framePayloadBytes,
                effectiveActiveCords(acceptedMaxCords, cordLaneMultiplier)
        );
        requested = saturatedMultiply(requested, Math.max(1, writeAhead));
        long advertised = saturatedMultiply(Math.max(1L, flowControlWindowBytes), Math.max(1, cordLaneMultiplier));
        long target = Math.min(Math.min(requested, advertised), heapSafeDirectWriteWindowBytes(
                flowControlWindowBytes,
                cordLaneMultiplier
        ));
        return Math.max(framePayloadBytes, target);
    }

    private static long heapSafeDirectWriteWindowBytes(
            long flowControlWindowBytes,
            int cordLaneMultiplier
    ) {
        long advertised = saturatedMultiply(Math.max(1L, flowControlWindowBytes), Math.max(1, cordLaneMultiplier));
        long heapMax = Runtime.getRuntime().maxMemory();
        long heapBudget = heapMax <= 0L || heapMax == Long.MAX_VALUE
                ? MAX_DIRECT_WRITE_PIPELINE_WINDOW_BYTES
                : Math.max(MIN_DIRECT_WRITE_PIPELINE_WINDOW_BYTES, heapMax / 2L);
        return Math.max(
                MIN_DIRECT_WRITE_PIPELINE_WINDOW_BYTES,
                Math.min(Math.min(advertised, heapBudget), MAX_DIRECT_WRITE_PIPELINE_WINDOW_BYTES)
        );
    }

    private static long saturatedMultiply(long value, long multiplier) {
        if (value > Long.MAX_VALUE / multiplier) {
            return Long.MAX_VALUE;
        }
        return value * multiplier;
    }

    private static int saturatedMultiplyToInt(int value, int multiplier) {
        long result = saturatedMultiply(Math.max(1L, value), Math.max(1L, multiplier));
        return result > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) result;
    }

    private static long floorPowerOfTwo(long value) {
        long result = 1L;
        while (result <= value / 2L && result < MAX_DIRECT_WRITE_FRAME_BYTES) {
            result *= 2L;
        }
        return result;
    }

    private static void produceReadAheadPayloads(
            SendEntry entry,
            int maxPayloadBytes,
            BlockingQueue<ReadAheadPayload> queue
    ) {
        byte[] window = new byte[Math.multiplyExact(maxPayloadBytes, SLIDING_WINDOW_MULTIPLIER)];
        long offset = 0L;
        long chunkSequence = 0L;
        CRC32C sourceChecksum = new CRC32C();
        try (InputStream input = Files.newInputStream(entry.localPath())) {
            while (true) {
                int windowLength = 0;
                while (windowLength < window.length) {
                    int read = input.read(window, windowLength, window.length - windowLength);
                    if (read < 0) {
                        break;
                    }
                    if (read == 0) {
                        continue;
                    }
                    sourceChecksum.update(window, windowLength, read);
                    windowLength += read;
                }
                if (windowLength <= 0) {
                    putReadAheadPayload(queue, ReadAheadPayload.done(TransferChecksums.crc32c(sourceChecksum)));
                    return;
                }
                int windowOffset = 0;
                while (windowOffset < windowLength) {
                    int emitLength = Math.min(maxPayloadBytes, windowLength - windowOffset);
                    ReadAheadPayload payload = prepareSlidingWindowPayload(
                            window,
                            windowOffset,
                            emitLength,
                            offset,
                            chunkSequence
                    );
                    if (!putReadAheadPayload(queue, ReadAheadPayload.frame(
                            offset,
                            chunkSequence,
                            payload.payload(),
                            payload.payloadLength(),
                            payload.chunkHash(),
                            payload.framePrepareNanos(),
                            payload.patternPrepareNanos(),
                            payload.encodedPayload(),
                            payload.payloadPattern()
                    ))) {
                        return;
                    }
                    offset += emitLength;
                    chunkSequence++;
                    windowOffset += emitLength;
                }
            }
        } catch (Throwable e) {
            putReadAheadPayload(queue, ReadAheadPayload.failure(e));
        }
    }

    private static ReadAheadPayload prepareSlidingWindowPayload(
            byte[] window,
            int windowOffset,
            int length,
            long offset,
            long chunkSequence
    ) throws IOException {
        long framePrepareStart = System.nanoTime();
        long patternPrepareNanos;
        ContentHash chunkHash;
        Optional<BulkPayloadPatternCodec.EncodedPayload> encodedPayload;
        Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern;
        byte[] queuedPayload;

        long patternStart = System.nanoTime();
        Optional<RunPattern> run = optimisticWindowRun(window, windowOffset, length);
        if (run.isPresent()) {
            byte value = run.orElseThrow().value();
            chunkHash = Hashing.chunkRun(value, length);
            encodedPayload = Optional.of(BulkPayloadPatternCodec.encodeRunPayload(length, value));
            payloadPattern = Optional.of(new BulkPayloadPatternCodec.PayloadPattern(
                    BulkPayloadPatternCodec.PayloadPatternKind.RUN,
                    length,
                    value,
                    new byte[0]
            ));
            queuedPayload = new byte[0];
            patternPrepareNanos = System.nanoTime() - patternStart;
            return ReadAheadPayload.frame(
                    offset,
                    chunkSequence,
                    queuedPayload,
                    length,
                    chunkHash,
                    System.nanoTime() - framePrepareStart - patternPrepareNanos,
                    patternPrepareNanos,
                    encodedPayload,
                    payloadPattern
            );
        }

        Optional<SymbolPattern> symbol = optimisticWindowRepeatedSymbol(window, windowOffset, length);
        if (symbol.isPresent()) {
            byte[] bytes = symbol.orElseThrow().symbol();
            chunkHash = Hashing.chunkRepeatedSymbol(bytes, length);
            encodedPayload = Optional.of(BulkPayloadPatternCodec.encodeRepeatedSymbolPayload(length, bytes));
            payloadPattern = Optional.of(new BulkPayloadPatternCodec.PayloadPattern(
                    BulkPayloadPatternCodec.PayloadPatternKind.REPEATED_SYMBOL,
                    length,
                    (byte) 0,
                    bytes
            ));
            queuedPayload = new byte[0];
            patternPrepareNanos = System.nanoTime() - patternStart;
            return ReadAheadPayload.frame(
                    offset,
                    chunkSequence,
                    queuedPayload,
                    length,
                    chunkHash,
                    System.nanoTime() - framePrepareStart - patternPrepareNanos,
                    patternPrepareNanos,
                    encodedPayload,
                    payloadPattern
            );
        }

        patternPrepareNanos = System.nanoTime() - patternStart;
        queuedPayload = Arrays.copyOfRange(window, windowOffset, windowOffset + length);
        chunkHash = Hashing.chunk(queuedPayload);
        return ReadAheadPayload.frame(
                offset,
                chunkSequence,
                queuedPayload,
                length,
                chunkHash,
                System.nanoTime() - framePrepareStart - patternPrepareNanos,
                patternPrepareNanos,
                Optional.empty(),
                Optional.empty()
        );
    }

    static SlidingPayloadSummary analyzeSlidingWindowPayload(byte[] window, int length) throws IOException {
        return analyzeSlidingWindowPayload(window, 0, length);
    }

    static SlidingPayloadSummary analyzeSlidingWindowPayload(byte[] window, int windowOffset, int length)
            throws IOException {
        ReadAheadPayload payload = prepareSlidingWindowPayload(window, windowOffset, length, 0L, 0L);
        return new SlidingPayloadSummary(
                payload.payloadLength(),
                payload.payload().length,
                payload.encodedPayload().isPresent(),
                payload.payloadPattern().map(BulkPayloadPatternCodec.PayloadPattern::kind),
                payload.chunkHash()
        );
    }

    private static Optional<RunPattern> optimisticWindowRun(byte[] bytes, int offset, int length) {
        if (length <= 0) {
            return Optional.empty();
        }
        byte value = bytes[offset];
        for (int sampleOffset : sampleOffsets(length)) {
            if (bytes[offset + sampleOffset] != value) {
                return Optional.empty();
            }
        }
        return Optional.of(new RunPattern(value));
    }

    private static Optional<SymbolPattern> optimisticWindowRepeatedSymbol(byte[] bytes, int offset, int length) {
        for (int symbolLength : SYMBOL_LENGTHS) {
            if (symbolLength <= 0 || symbolLength >= length || length % symbolLength != 0) {
                continue;
            }
            if (sampledSymbolIsUniform(bytes, offset, symbolLength)) {
                continue;
            }
            if (!symbolSamplesMatch(bytes, offset, length, symbolLength)) {
                continue;
            }
            return Optional.of(new SymbolPattern(Arrays.copyOfRange(bytes, offset, offset + symbolLength)));
        }
        return Optional.empty();
    }

    private static boolean sampledSymbolIsUniform(byte[] bytes, int offset, int length) {
        byte value = bytes[offset];
        for (int sampleOffset : sampleOffsets(length)) {
            if (bytes[offset + sampleOffset] != value) {
                return false;
            }
        }
        return true;
    }

    private static boolean symbolSamplesMatch(byte[] bytes, int offset, int length, int symbolLength) {
        int middle = (length / (2 * symbolLength)) * symbolLength;
        int last = length - symbolLength;
        return symbolAtMatches(bytes, offset, offset + middle, symbolLength)
                && symbolAtMatches(bytes, offset, offset + last, symbolLength);
    }

    private static boolean symbolAtMatches(byte[] bytes, int symbolOffset, int candidateOffset, int symbolLength) {
        for (int index = 0; index < symbolLength; index++) {
            if (bytes[symbolOffset + index] != bytes[candidateOffset + index]) {
                return false;
            }
        }
        return true;
    }

    private static int[] sampleOffsets(int length) {
        if (length <= 1) {
            return new int[]{0};
        }
        return new int[]{
                0,
                Math.min(4096, length - 1),
                Math.max(0, (length / 2) - 1),
                length / 2,
                length - 1
        };
    }

    private static int readAheadFrameCapacity(long entrySize, int maxPayloadBytes, long advertisedWindowBytes) {
        long targetBytes = Math.max(maxPayloadBytes, Math.max(MIN_READ_AHEAD_BYTES, entrySize / 10L));
        targetBytes = Math.min(targetBytes, MAX_READ_AHEAD_BYTES);
        if (advertisedWindowBytes > 0) {
            targetBytes = Math.min(targetBytes, Math.max(maxPayloadBytes, advertisedWindowBytes));
        }
        long frames = (targetBytes + maxPayloadBytes - 1L) / maxPayloadBytes;
        return (int) Math.max(1L, Math.min(frames, Integer.MAX_VALUE));
    }

    private static boolean putReadAheadPayload(
            BlockingQueue<ReadAheadPayload> queue,
            ReadAheadPayload payload
    ) {
        try {
            queue.put(payload);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static ReadAheadPayload takeReadAheadPayload(BlockingQueue<ReadAheadPayload> queue) throws IOException {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for read-ahead payload", e);
        }
    }

    private static IOException readAheadFailure(Throwable failure) {
        if (failure instanceof IOException e) {
            return e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        return new IOException("read-ahead producer failed", failure);
    }

    private static void joinReadAheadProducer(Thread producer) throws IOException {
        boolean interrupted = false;
        try {
            while (producer.isAlive()) {
                try {
                    producer.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                    producer.interrupt();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for read-ahead producer");
            }
        }
    }

    private static String millis(long nanos) {
        return String.format(Locale.ROOT, "%.1f", nanos / 1_000_000.0d);
    }

    private static String megabytesPerSecond(long bytes, long nanos) {
        double seconds = Math.max(1.0d, nanos) / 1_000_000_000.0d;
        return String.format(Locale.ROOT, "%.1f", (bytes / 1_000_000.0d) / seconds);
    }

    private static String cpuMillis(long nanos) {
        return nanos < 0L ? "unavailable" : millis(nanos);
    }

    private static String cpuCores(long cpuNanos, long wallNanos) {
        if (cpuNanos < 0L || wallNanos <= 0L) {
            return "unavailable";
        }
        return String.format(Locale.ROOT, "%.2f", cpuNanos / (double) wallNanos);
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

        private ProcessCpuUsage elapsed() {
            ProcessCpuSample end = capture();
            if (cpuNanos < 0L || end.cpuNanos < 0L || end.cpuNanos < cpuNanos) {
                return new ProcessCpuUsage(-1L);
            }
            return new ProcessCpuUsage(end.cpuNanos - cpuNanos);
        }
    }

    private record ProcessCpuUsage(long cpuNanos) {
    }

    private static FileMetadata regularFileMetadata(CxnFileNetworkClient client, String remotePath) throws IOException {
        FileMetadata metadata = client.stat(remotePath)
                .orElseThrow(() -> new IOException("remote file does not exist: " + remotePath));
        if (metadata.type() != FileType.REGULAR_FILE) {
            throw new IOException("remote path is not a file: " + remotePath);
        }
        return metadata;
    }

    private static long readFile(
            CxnFileNetworkClient client,
            String remotePath,
            Path output,
            long size,
            ProgressReporter progress
    ) throws IOException {
        Path parent = output.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (OutputStream fileOutput = Files.newOutputStream(output)) {
            return client.readRangeTo(
                    remotePath,
                    0,
                    size,
                    TransferPayloadFormat.CONTAINER,
                    new ProgressOutputStream(fileOutput, progress)
            );
        }
    }

    private static ReadDirectoryPlan readDirectoryPlan(
            CxnFileNetworkClient client,
            String remotePath,
            Path output
    ) throws IOException {
        List<ReadEntry> entries = new ArrayList<>();
        long[] bytes = {0L};
        Files.createDirectories(output);
        readDirectoryPlanRecursive(client, remotePath, output, entries, bytes);
        return new ReadDirectoryPlan(List.copyOf(entries), bytes[0]);
    }

    private static void readDirectoryPlanRecursive(
            CxnFileNetworkClient client,
            String remotePath,
            Path output,
            List<ReadEntry> entries,
            long[] bytes
    ) throws IOException {
        DirectoryListing listing = client.list(remotePath);
        for (FileMetadata entry : listing.entries()) {
            String childRemotePath = joinRemotePath(remotePath, entry.name());
            Path childOutput = output.resolve(entry.name()).normalize();
            if (!childOutput.startsWith(output)) {
                throw new IOException("remote entry escapes output directory: " + entry.name());
            }
            if (entry.type() == FileType.DIRECTORY) {
                Files.createDirectories(childOutput);
                readDirectoryPlanRecursive(client, childRemotePath, childOutput, entries, bytes);
            } else if (entry.type() == FileType.REGULAR_FILE) {
                entries.add(new ReadEntry(childRemotePath, childOutput, entry.size()));
                bytes[0] += entry.size();
            }
        }
    }

    private static long readDirectory(
            CxnFileNetworkClient client,
            ReadDirectoryPlan plan,
            ProgressReporter progress
    ) throws IOException {
        long bytes = 0L;
        for (ReadEntry entry : plan.entries()) {
            bytes += readFile(client, entry.remotePath(), entry.output(), entry.size(), progress);
        }
        return bytes;
    }

    private static ConnectedClient connect(ClientOptions options) throws IOException {
        ClientTlsContextFactory contextFactory = new ClientTlsContextFactory();
        SSLContext context = contextFactory.createInsecureTrustAll();
        SocketFactory socketFactory = new ControlTlsClientSocketFactory(
                context,
                TlsCipherSuite.TLS_AES_128_GCM_SHA256,
                false
        );
        if (options.bulkTransport() == BulkTransportPreference.QUIC) {
            return new ConnectedClient(
                    connectQuic(options, socketFactory),
                    BulkTransportPreference.QUIC,
                    options.quicConnections()
            );
        }
        if (options.bulkTransport() == BulkTransportPreference.STREAM) {
            return new ConnectedClient(connectStream(options, socketFactory), BulkTransportPreference.STREAM, 0);
        }
        IOException quicFailure;
        try {
            return new ConnectedClient(
                    connectQuic(options, socketFactory),
                    BulkTransportPreference.QUIC,
                    options.quicConnections()
            );
        } catch (IOException e) {
            quicFailure = e;
        }
        try {
            return new ConnectedClient(connectStream(options, socketFactory), BulkTransportPreference.STREAM, 0);
        } catch (IOException e) {
            IOException failure = new IOException(
                    "failed to connect bulk transport; quic="
                            + quicFailure.getMessage()
                            + "; stream=" + e.getMessage(),
                    e
            );
            failure.addSuppressed(quicFailure);
            throw failure;
        }
    }

    private static CxnFileNetworkClient connectStream(ClientOptions options, SocketFactory socketFactory)
            throws IOException {
        return CxnFileNetworkClient.connect(
                new InetSocketAddress(options.host(), options.controlPort()),
                new InetSocketAddress(options.host(), options.transportPort()),
                socketFactory,
                socketFactory,
                options.config()
        );
    }

    private static CxnFileNetworkClient connectQuic(ClientOptions options, SocketFactory socketFactory) throws IOException {
        BulkTransferClientChannel bulkChannel = connectQuicBulkChannel(options);
        try {
            return CxnFileNetworkClient.connect(
                    new InetSocketAddress(options.host(), options.controlPort()),
                    socketFactory,
                    options.config(),
                    bulkChannel
            );
        } catch (IOException | RuntimeException e) {
            try {
                bulkChannel.close();
            } catch (Exception closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    private static BulkTransferClientChannel connectQuicBulkChannel(ClientOptions options) throws IOException {
        InetSocketAddress address = new InetSocketAddress(options.host(), options.transportPort());
        if (options.quicConnections() == 1) {
            return QuicBulkTransferClientChannel.connect(
                    address,
                    options.config(),
                    Optional.<KeyStore>empty(),
                    true
            );
        }
        List<BulkTransferClientChannel> channels = new ArrayList<>(options.quicConnections());
        try {
            for (int index = 0; index < options.quicConnections(); index++) {
                channels.add(QuicBulkTransferClientChannel.connect(
                        address,
                        options.config(),
                        Optional.<KeyStore>empty(),
                        true
                ));
            }
            return new StripedBulkTransferClientChannel(channels);
        } catch (IOException | RuntimeException e) {
            closeAll(channels, e);
            throw e;
        }
    }

    private static void closeAll(List<? extends AutoCloseable> closeables, Throwable failure) {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                failure.addSuppressed(e);
            }
        }
    }

    private static SendEntry fileEntry(Path input, String remotePath, int entrySequence) throws IOException {
        if (!Files.isRegularFile(input, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("input file does not exist: " + input);
        }
        return new SendEntry(input, remotePath, entrySequence, Files.size(input));
    }

    private static List<SendEntry> directoryEntries(Path input, String remoteRoot) throws IOException {
        if (!Files.isDirectory(input, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("input directory does not exist: " + input);
        }
        List<Path> files;
        try (Stream<Path> paths = Files.walk(input)) {
            files = paths
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .sorted(Comparator.comparing(path -> input.relativize(path).toString()))
                    .toList();
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException("input directory has no files: " + input);
        }
        List<SendEntry> entries = new ArrayList<>(files.size());
        for (int index = 0; index < files.size(); index++) {
            Path file = files.get(index);
            String relative = input.relativize(file).toString().replace('\\', '/');
            validateRelativePath(relative);
            entries.add(fileEntry(file, joinRemotePath(remoteRoot, relative), index));
        }
        return List.copyOf(entries);
    }

    private static long sendTotalBytes(List<SendEntry> entries) {
        long bytes = 0L;
        for (SendEntry entry : entries) {
            bytes += entry.size();
        }
        return bytes;
    }

    private static ClientOptions clientOptions(Map<String, String> options) {
        String host = options.get("--host");
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("--host is required");
        }
        CxnFileNetworkClientConfig defaults = CxnFileNetworkClientConfig.defaults();
        CxnFileNetworkClientConfig config = new CxnFileNetworkClientConfig(
                intOption(options, "--initial-cords", defaults.initialCords()),
                intOption(options, "--max-cords", defaults.maxCords()),
                defaults.provisionalContainerSizeBytes(),
                defaults.expectedSizeLimitBytes(),
                defaults.connectTimeoutMillis(),
                defaults.readTimeoutMillis()
        );
        return new ClientOptions(
                host,
                intOption(options, "--control-port", DEFAULT_CONTROL_PORT),
                intOption(options, "--transport-port", DEFAULT_TRANSPORT_PORT),
                bulkTransportOption(options.getOrDefault("--bulk-transport", "auto")),
                intOption(options, "--quic-connections", 1),
                config
        );
    }

    private static Map<String, String> parseOptions(String[] args, int startIndex) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int index = startIndex; index < args.length; index++) {
            String key = args[index];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("expected option but found: " + key);
            }
            if (index + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for option: " + key);
            }
            if (!key.equals("--host")
                    && !key.equals("--control-port")
                    && !key.equals("--transport-port")
                    && !key.equals("--initial-cords")
                    && !key.equals("--max-cords")
                    && !key.equals("--bulk-transport")
                    && !key.equals("--quic-connections")) {
                throw new IllegalArgumentException("unknown option: " + key);
            }
            options.put(key, args[++index]);
        }
        return options;
    }

    private static int intOption(Map<String, String> options, String name, int defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static BulkTransportPreference bulkTransportOption(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "auto" -> BulkTransportPreference.AUTO;
            case "quic" -> BulkTransportPreference.QUIC;
            case "stream", "tcp" -> BulkTransportPreference.STREAM;
            default -> throw new IllegalArgumentException("--bulk-transport must be auto, quic, or stream");
        };
    }

    private static void requireArity(String commandGroup, String[] args, int minimum, String command) {
        if (args.length < minimum) {
            throw new IllegalArgumentException("cxnfs " + commandGroup + " " + command
                    + " requires input and output paths");
        }
    }

    private static String normalizeRemotePath(String value) {
        return NamespacePath.normalize(value);
    }

    private static String joinRemotePath(String remoteRoot, String relativePath) {
        String normalizedRoot = normalizeRemotePath(remoteRoot);
        String normalizedRelative = relativePath.replace('\\', '/');
        validateRelativePath(normalizedRelative);
        return normalizedRoot.equals("/") ? "/" + normalizedRelative : normalizedRoot + "/" + normalizedRelative;
    }

    private static void validateRelativePath(String path) {
        if (path == null || path.isBlank() || path.startsWith("/")) {
            throw new IllegalArgumentException("relative file path is required");
        }
        for (String part : path.split("/")) {
            if (part.isBlank() || part.equals(".") || part.equals("..")) {
                throw new IllegalArgumentException("relative file paths must not contain empty, current, or parent segments");
            }
        }
    }

    private static void requireEntryAccepted(
            TransferEntryAcceptedResponse response,
            TransferAcceptedResponse transfer,
            SendEntry entry
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || response.entrySequence() != entry.entrySequence()
                || !response.path().equals(entry.remotePath())) {
            throw new IOException("write entry acknowledgement does not match request");
        }
    }

    private static void requireMetadataAccepted(
            TransferMetadataAcceptedResponse response,
            TransferAcceptedResponse transfer,
            SendEntry entry,
            int chunkHashCount
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || response.entrySequence() != entry.entrySequence()
                || response.metadataSequence() != 0
                || response.chunkHashesAccepted() != chunkHashCount
                || response.stagedPayloadsVerified() != chunkHashCount
                || !response.finalMetadata()) {
            throw new IOException("write metadata acknowledgement does not match request");
        }
    }

    private static void requireEntryCommitted(
            TransferEntryCommittedResponse response,
            TransferAcceptedResponse transfer,
            SendEntry entry,
            ContentHash fileHash
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || response.entrySequence() != entry.entrySequence()
                || !response.path().equals(entry.remotePath())
                || !response.contentHash().equals(fileHash)
                || response.bytesWritten() != entry.size()
                || !response.checksum().present()) {
            throw new IOException("write entry commit acknowledgement does not match request");
        }
    }

    private static void requireTransferCommitted(
            TransferCommittedResponse response,
            TransferAcceptedResponse transfer,
            ContentHash transferHash,
            long bytes
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || !response.contentHash().equals(transferHash)
                || response.bytesWritten() != bytes) {
            throw new IOException("write transfer commit acknowledgement does not match request");
        }
    }

    private static void usage(String commandGroup, PrintStream out) {
        out.println("cxnfs " + commandGroup + " send <input-" + commandGroup + "> <output-path> --host <remote-ip> [--bulk-transport auto|quic|stream] [--quic-connections <count>]");
        out.println("cxnfs " + commandGroup + " read <input-path> <output-" + commandGroup + "> --host <remote-ip> [--bulk-transport auto|quic|stream] [--quic-connections <count>]");
    }

    private enum BulkTransportPreference {
        AUTO("auto"),
        QUIC("quic"),
        STREAM("stream");

        private final String optionName;

        BulkTransportPreference(String optionName) {
            this.optionName = optionName;
        }

        private String optionName() {
            return optionName;
        }
    }

    private enum FilePatternKind {
        RAW,
        RUN,
        SYMBOL
    }

    private record FilePatternPlan(FilePatternKind kind, byte value, byte[] symbol) {
        private static FilePatternPlan raw() {
            return new FilePatternPlan(FilePatternKind.RAW, (byte) 0, null);
        }

        private static FilePatternPlan run(byte value) {
            return new FilePatternPlan(FilePatternKind.RUN, value, null);
        }

        private static FilePatternPlan symbol(byte[] symbol) {
            return new FilePatternPlan(FilePatternKind.SYMBOL, (byte) 0, Arrays.copyOf(symbol, symbol.length));
        }

        private static FilePatternPlan analyze(Path file, long size) throws IOException {
            if (size <= 0) {
                return raw();
            }
            int sampleLength = Math.toIntExact(Math.min(size, PATTERN_SAMPLE_BYTES));
            byte[] sample;
            try (InputStream input = Files.newInputStream(file)) {
                sample = input.readNBytes(sampleLength);
            }
            if (sample.length == 0) {
                return raw();
            }
            if (sampleIsUniform(sample) && verifyUniform(file, sample[0])) {
                return run(sample[0]);
            }
            for (int length : SYMBOL_LENGTHS) {
                if (size % length != 0 || size < (long) length * 2 || sample.length < length * 2) {
                    continue;
                }
                if (!sampleRepeats(sample, length)) {
                    continue;
                }
                byte[] symbol = Arrays.copyOf(sample, length);
                if (verifyRepeatedSymbol(file, symbol)) {
                    return symbol(symbol);
                }
            }
            return raw();
        }

        private int chunkPayloadBytes(int maxPayloadBytes) {
            if (kind != FilePatternKind.SYMBOL) {
                return maxPayloadBytes;
            }
            int length = symbol.length;
            int chunks = Math.max(1, maxPayloadBytes / length);
            return chunks * length;
        }

        private byte[] materialize(int length) {
            byte[] bytes = new byte[length];
            if (kind == FilePatternKind.RUN) {
                Arrays.fill(bytes, value);
                return bytes;
            }
            if (kind == FilePatternKind.SYMBOL) {
                for (int offset = 0; offset < length; offset += symbol.length) {
                    System.arraycopy(symbol, 0, bytes, offset, symbol.length);
                }
                return bytes;
            }
            throw new IllegalStateException("raw pattern plan cannot materialize synthetic payload");
        }

        private BulkPayloadPatternCodec.EncodedPayload encodedPayload(int length) throws IOException {
            return switch (kind) {
                case RUN -> BulkPayloadPatternCodec.encodeRunPayload(length, value);
                case SYMBOL -> BulkPayloadPatternCodec.encodeRepeatedSymbolPayload(length, symbol);
                case RAW -> throw new IllegalStateException("raw pattern plan has no encoded payload");
            };
        }

        private static boolean sampleIsUniform(byte[] sample) {
            byte first = sample[0];
            for (byte value : sample) {
                if (value != first) {
                    return false;
                }
            }
            return true;
        }

        private static boolean verifyUniform(Path file, byte value) throws IOException {
            byte[] buffer = new byte[PATTERN_SAMPLE_BYTES];
            try (InputStream input = Files.newInputStream(file)) {
                while (true) {
                    int read = input.read(buffer);
                    if (read < 0) {
                        return true;
                    }
                    for (int index = 0; index < read; index++) {
                        if (buffer[index] != value) {
                            return false;
                        }
                    }
                }
            }
        }

        private static boolean sampleRepeats(byte[] sample, int symbolLength) {
            for (int index = symbolLength; index < sample.length; index++) {
                if (sample[index] != sample[index % symbolLength]) {
                    return false;
                }
            }
            return true;
        }

        private static boolean verifyRepeatedSymbol(Path file, byte[] symbol) throws IOException {
            byte[] buffer = new byte[PATTERN_SAMPLE_BYTES];
            long offset = 0L;
            try (InputStream input = Files.newInputStream(file)) {
                while (true) {
                    int read = input.read(buffer);
                    if (read < 0) {
                        return true;
                    }
                    for (int index = 0; index < read; index++) {
                        if (buffer[index] != symbol[(int) ((offset + index) % symbol.length)]) {
                            return false;
                        }
                    }
                    offset += read;
                }
            }
        }
    }

    private record ClientOptions(
            String host,
            int controlPort,
            int transportPort,
            BulkTransportPreference bulkTransport,
            int quicConnections,
            CxnFileNetworkClientConfig config
    ) {
        private ClientOptions {
            if (quicConnections <= 0 || quicConnections > 16) {
                throw new IllegalArgumentException("--quic-connections must be between 1 and 16");
            }
        }
    }

    private record ConnectedClient(
            CxnFileNetworkClient client,
            BulkTransportPreference bulkTransport,
            int quicConnections
    )
            implements AutoCloseable {
        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    private record SendEntry(Path localPath, String remotePath, int entrySequence, long size) {
    }

    private record ReadEntry(String remotePath, Path output, long size) {
    }

    private record ReadDirectoryPlan(List<ReadEntry> entries, long bytes) {
    }

    private record SentEntry(
            ContentHash contentHash,
            long frames,
            long patternFrames,
            long patternWireBytes,
            TransferChecksum checksum,
            SendTiming timing
    ) {
    }

    private record SentPayload(
            long frames,
            long patternFrames,
            long patternWireBytes,
            long framePrepareNanos,
            long patternPrepareNanos,
            long firstHostWriteNanos,
            TransferChecksum checksum
    ) {
    }

    private record RunPattern(byte value) {
    }

    private record SymbolPattern(byte[] symbol) {
        private SymbolPattern {
            symbol = symbol == null ? new byte[0] : Arrays.copyOf(symbol, symbol.length);
        }

        @Override
        public byte[] symbol() {
            return Arrays.copyOf(symbol, symbol.length);
        }
    }

    record SlidingPayloadSummary(
            int payloadLength,
            int queuedPayloadBytes,
            boolean encoded,
            Optional<BulkPayloadPatternCodec.PayloadPatternKind> patternKind,
            ContentHash chunkHash
    ) {
    }

    private record ReadAheadPayload(
            long offset,
            long chunkSequence,
            byte[] payload,
            ContentHash chunkHash,
            int payloadLength,
            long framePrepareNanos,
            long patternPrepareNanos,
            Optional<BulkPayloadPatternCodec.EncodedPayload> encodedPayload,
            Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern,
            TransferChecksum checksum,
            Throwable failure,
            boolean end
    ) {
        private static ReadAheadPayload frame(
                long offset,
                long chunkSequence,
                byte[] payload,
                int payloadLength,
                ContentHash chunkHash,
                long framePrepareNanos,
                long patternPrepareNanos,
                Optional<BulkPayloadPatternCodec.EncodedPayload> encodedPayload,
                Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern
        ) {
            return new ReadAheadPayload(
                    offset,
                    chunkSequence,
                    payload,
                    chunkHash,
                    payloadLength,
                    framePrepareNanos,
                    patternPrepareNanos,
                    encodedPayload,
                    payloadPattern,
                    TransferChecksum.none(),
                    null,
                    false
            );
        }

        private static ReadAheadPayload failure(Throwable failure) {
            return new ReadAheadPayload(
                    0L,
                    0L,
                    new byte[0],
                    null,
                    0,
                    0L,
                    0L,
                    Optional.empty(),
                    Optional.empty(),
                    TransferChecksum.none(),
                    failure,
                    false
            );
        }

        private static ReadAheadPayload done(TransferChecksum checksum) {
            return new ReadAheadPayload(
                    0L,
                    0L,
                    new byte[0],
                    null,
                    0,
                    0L,
                    0L,
                    Optional.empty(),
                    Optional.empty(),
                    checksum == null ? TransferChecksum.none() : checksum,
                    null,
                    true
            );
        }
    }

    private record SendSummary(
            int entries,
            long bytes,
            ContentHash transferHash,
            long frames,
            long patternFrames,
            long patternWireBytes,
            int framePayloadBytes,
            int acceptedInitialCords,
            int acceptedMaxCords,
            WritePipelineMetrics metrics,
            SendTiming timing
    ) {
    }

    private record SendTiming(
            long totalNanos,
            long bootstrapNanos,
            long entryNanos,
            long patternPrepareNanos,
            long framePrepareNanos,
            long hostWriteNanos,
            long metadataNanos,
            long entryCommitNanos,
            long transferCommitNanos
    ) {
        private static SendTiming empty() {
            return new SendTiming(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        private long prepareNanos() {
            return patternPrepareNanos + framePrepareNanos;
        }

        private SendTiming withTotal(long nanos) {
            return new SendTiming(
                    nanos,
                    bootstrapNanos,
                    entryNanos,
                    patternPrepareNanos,
                    framePrepareNanos,
                    hostWriteNanos,
                    metadataNanos,
                    entryCommitNanos,
                    transferCommitNanos
            );
        }

        private SendTiming plus(SendTiming other) {
            return new SendTiming(
                    totalNanos + other.totalNanos,
                    bootstrapNanos + other.bootstrapNanos,
                    entryNanos + other.entryNanos,
                    patternPrepareNanos + other.patternPrepareNanos,
                    framePrepareNanos + other.framePrepareNanos,
                    hostWriteNanos + other.hostWriteNanos,
                    metadataNanos + other.metadataNanos,
                    entryCommitNanos + other.entryCommitNanos,
                    transferCommitNanos + other.transferCommitNanos
            );
        }

        private SendTiming plusBootstrap(long nanos) {
            return add(0L, nanos, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        private SendTiming plusEntry(long nanos) {
            return add(0L, 0L, nanos, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        private SendTiming plusPatternPrepare(long nanos) {
            return add(0L, 0L, 0L, nanos, 0L, 0L, 0L, 0L, 0L);
        }

        private SendTiming plusFramePrepare(long nanos) {
            return add(0L, 0L, 0L, 0L, nanos, 0L, 0L, 0L, 0L);
        }

        private SendTiming plusHostWrite(long nanos) {
            return add(0L, 0L, 0L, 0L, 0L, nanos, 0L, 0L, 0L);
        }

        private SendTiming plusMetadata(long nanos) {
            return add(0L, 0L, 0L, 0L, 0L, 0L, nanos, 0L, 0L);
        }

        private SendTiming plusEntryCommit(long nanos) {
            return add(0L, 0L, 0L, 0L, 0L, 0L, 0L, nanos, 0L);
        }

        private SendTiming plusTransferCommit(long nanos) {
            return add(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, nanos);
        }

        private SendTiming add(
                long total,
                long bootstrap,
                long entry,
                long patternPrepare,
                long framePrepare,
                long hostWrite,
                long metadata,
                long entryCommit,
                long transferCommit
        ) {
            return new SendTiming(
                    totalNanos + total,
                    bootstrapNanos + bootstrap,
                    entryNanos + entry,
                    patternPrepareNanos + patternPrepare,
                    framePrepareNanos + framePrepare,
                    hostWriteNanos + hostWrite,
                    metadataNanos + metadata,
                    entryCommitNanos + entryCommit,
                    transferCommitNanos + transferCommit
            );
        }
    }

    private static final class ProgressOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final ProgressReporter progress;

        private ProgressOutputStream(OutputStream delegate, ProgressReporter progress) {
            if (delegate == null) {
                throw new IllegalArgumentException("delegate output is required");
            }
            this.delegate = delegate;
            this.progress = progress == null ? ProgressReporter.disabled() : progress;
        }

        @Override
        public void write(int value) throws IOException {
            delegate.write(value);
            progress.add(1L);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            delegate.write(bytes, offset, length);
            progress.add(length);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class ProgressReporter implements AutoCloseable {
        private static final ProgressReporter DISABLED = new ProgressReporter(null, "", 0L, false);

        private final PrintStream err;
        private final String label;
        private final long totalBytes;
        private final boolean enabled;
        private final long startedAtNanos;
        private long completedBytes;
        private long lastRenderNanos;
        private int lastLineLength;
        private boolean rendered;
        private boolean finished;

        private ProgressReporter(PrintStream err, String label, long totalBytes, boolean enabled) {
            this.err = err;
            this.label = label == null || label.isBlank() ? "cxnfs transfer" : label;
            this.totalBytes = Math.max(0L, totalBytes);
            this.enabled = enabled;
            this.startedAtNanos = System.nanoTime();
            this.lastRenderNanos = 0L;
        }

        private static ProgressReporter create(PrintStream err, String label, long totalBytes) {
            boolean enabled = err != null && System.console() != null;
            ProgressReporter reporter = new ProgressReporter(err, label, totalBytes, enabled);
            reporter.render(true, false);
            return reporter;
        }

        private static ProgressReporter disabled() {
            return DISABLED;
        }

        private synchronized void add(long bytes) {
            if (bytes <= 0L) {
                return;
            }
            completedBytes = totalBytes <= 0L
                    ? completedBytes + bytes
                    : Math.min(totalBytes, completedBytes + bytes);
            render(false, false);
        }

        private synchronized void finish() {
            if (finished) {
                return;
            }
            completedBytes = totalBytes;
            render(true, true);
            if (enabled && rendered) {
                err.println();
            }
            finished = true;
        }

        @Override
        public synchronized void close() {
            if (!enabled || finished || !rendered) {
                return;
            }
            err.println();
            finished = true;
        }

        private void render(boolean force, boolean finalLine) {
            if (!enabled) {
                return;
            }
            long now = System.nanoTime();
            if (!force && now - lastRenderNanos < PROGRESS_UPDATE_INTERVAL_NANOS) {
                return;
            }
            lastRenderNanos = now;
            String line = line(now, finalLine);
            int padding = Math.max(0, lastLineLength - line.length());
            err.print('\r');
            err.print(line);
            if (padding > 0) {
                err.print(" ".repeat(padding));
            }
            err.flush();
            lastLineLength = line.length();
            rendered = true;
        }

        private String line(long now, boolean finalLine) {
            double ratio = totalBytes <= 0L
                    ? (finalLine ? 1.0d : 0.0d)
                    : Math.min(1.0d, completedBytes / (double) totalBytes);
            int filled = (int) Math.round(ratio * PROGRESS_BAR_WIDTH);
            filled = Math.max(0, Math.min(PROGRESS_BAR_WIDTH, filled));
            String bar = "#".repeat(filled) + "-".repeat(PROGRESS_BAR_WIDTH - filled);
            double percent = ratio * 100.0d;
            long elapsedNanos = Math.max(1L, now - startedAtNanos);
            String speed = formatBytesPerSecond(completedBytes, elapsedNanos);
            String eta = finalLine ? "done" : estimateRemaining(elapsedNanos);
            return String.format(
                    Locale.ROOT,
                    "%s [%s] %5.1f%% %s/%s %s eta %s",
                    label,
                    bar,
                    percent,
                    formatBytes(completedBytes),
                    formatBytes(totalBytes),
                    speed,
                    eta
            );
        }

        private String estimateRemaining(long elapsedNanos) {
            if (totalBytes <= 0L || completedBytes <= 0L || completedBytes >= totalBytes) {
                return "--:--";
            }
            double bytesPerNano = completedBytes / (double) elapsedNanos;
            if (bytesPerNano <= 0.0d || Double.isNaN(bytesPerNano) || Double.isInfinite(bytesPerNano)) {
                return "--:--";
            }
            long remainingNanos = (long) ((totalBytes - completedBytes) / bytesPerNano);
            return formatDuration(remainingNanos);
        }

        private static String formatBytesPerSecond(long bytes, long elapsedNanos) {
            double seconds = Math.max(1.0d, elapsedNanos) / 1_000_000_000.0d;
            return formatBytes((long) (bytes / seconds)) + "/s";
        }

        private static String formatBytes(long bytes) {
            double value = Math.max(0L, bytes);
            String[] units = {"B", "KiB", "MiB", "GiB", "TiB"};
            int unit = 0;
            while (value >= 1024.0d && unit < units.length - 1) {
                value /= 1024.0d;
                unit++;
            }
            if (unit == 0) {
                return String.format(Locale.ROOT, "%d %s", bytes, units[unit]);
            }
            return String.format(Locale.ROOT, "%.1f %s", value, units[unit]);
        }

        private static String formatDuration(long nanos) {
            long seconds = Math.max(0L, Math.round(nanos / 1_000_000_000.0d));
            long hours = seconds / 3600L;
            long minutes = (seconds % 3600L) / 60L;
            long remainingSeconds = seconds % 60L;
            if (hours > 0L) {
                return String.format(Locale.ROOT, "%d:%02d:%02d", hours, minutes, remainingSeconds);
            }
            return String.format(Locale.ROOT, "%02d:%02d", minutes, remainingSeconds);
        }
    }
}
