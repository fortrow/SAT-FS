package com.fortrow.cxnfs.mount;

import com.fortrow.cxnfs.client.ClientCredentialPrompt;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.client.ClientTlsContextFactory;
import com.fortrow.cxnfs.client.ControlTlsClientSocketFactory;
import com.fortrow.cxnfs.client.BulkTransferClientChannel;
import com.fortrow.cxnfs.client.CxnFileNetworkClient;
import com.fortrow.cxnfs.client.QuicBulkTransferClientChannel;
import com.fortrow.cxnfs.client.WriteFramePipeline;
import com.fortrow.cxnfs.client.WritePipelineMetrics;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
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
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.TlsCipherSuite;
import com.fortrow.cxnfs.transport.WriteCordRamp;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Instant;
import java.util.Optional;

public final class MountBridgeCli {
    private static final String READY = "READY";

    public int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }
        try {
            if (args.length < 1 || args.length > 2) {
                throw new IllegalArgumentException("cxnfs fuse-bridge requires a handoff report path");
            }
            boolean credentialsStdin = false;
            if (args.length == 2) {
                if (!args[1].equals("--credentials-stdin")) {
                    throw new IllegalArgumentException("unknown fuse-bridge option: " + args[1]);
                }
                credentialsStdin = true;
            }
            MountAdapterHandoff handoff = MountAdapterHandoff.read(Path.of(args[0]));
            BridgeInput input = new BridgeInput(System.in);
            if (handoff.trustStorePath().isEmpty()) {
                err.println("mount bridge warning: server certificate verification disabled for direct mount");
            }
            PrintStream originalSystemOut = System.out;
            if (credentialsStdin) {
                System.setOut(err);
            }
            try {
                try (ClientCredentials credentials = credentialsStdin ? readCredentials(input) : ClientCredentialPrompt.prompt();
                     CxnFileNetworkClient client = connect(handoff)) {
                    client.authenticate(credentials);
                    client.list(handoff.remoteRoot());
                    writeLine(out, READY);
                    loop(client, input, out, err);
                }
            } finally {
                if (credentialsStdin) {
                    System.setOut(originalSystemOut);
                }
            }
            return 0;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println("mount bridge failed: " + e.getMessage());
            return 1;
        }
    }

    private static ClientCredentials readCredentials(BridgeInput input) throws IOException {
        String line = input.readLine();
        if (line == null) {
            throw new IOException("missing bridge credentials");
        }
        String[] parts = line.split("\t", -1);
        if (parts.length != 3 || !parts[0].equals("AUTH")) {
            throw new IOException("invalid bridge credentials");
        }
        return new ClientCredentials(parts[1], parts[2].toCharArray());
    }

    private static CxnFileNetworkClient connect(MountAdapterHandoff handoff) throws IOException {
        ClientTlsContextFactory contextFactory = new ClientTlsContextFactory();
        boolean strictServerCertificate = handoff.trustStorePath().isPresent();
        SSLContext context = handoff.trustStorePath().isPresent()
                ? contextFactory.create(handoff.trustStorePath().orElseThrow(), new char[0])
                : contextFactory.createInsecureTrustAll();
        SocketFactory socketFactory = new ControlTlsClientSocketFactory(
                context,
                TlsCipherSuite.TLS_AES_128_GCM_SHA256,
                strictServerCertificate
        );
        IOException quicFailure;
        try {
            return connectQuic(handoff, socketFactory);
        } catch (IOException e) {
            quicFailure = e;
        }
        try {
            return connectStream(handoff, socketFactory);
        } catch (IOException e) {
            IOException failure = new IOException(
                    "failed to connect mount bridge bulk transport; quic="
                            + quicFailure.getMessage()
                            + "; stream=" + e.getMessage(),
                    e
            );
            failure.addSuppressed(quicFailure);
            throw failure;
        }
    }

    private static CxnFileNetworkClient connectStream(
            MountAdapterHandoff handoff,
            SocketFactory socketFactory
    ) throws IOException {
        return CxnFileNetworkClient.connect(
                new InetSocketAddress(handoff.host(), handoff.controlPort()),
                new InetSocketAddress(handoff.host(), handoff.transportPort()),
                socketFactory,
                socketFactory,
                handoff.clientConfig()
        );
    }

    private static CxnFileNetworkClient connectQuic(
            MountAdapterHandoff handoff,
            SocketFactory socketFactory
    ) throws IOException {
        KeyStore trustStore = handoff.trustStorePath().isPresent()
                ? loadTrustStore(handoff.trustStorePath().orElseThrow())
                : null;
        BulkTransferClientChannel bulkChannel;
        boolean strictServerCertificate = trustStore != null;
        try {
            bulkChannel = QuicBulkTransferClientChannel.connect(
                    new InetSocketAddress(handoff.host(), handoff.transportPort()),
                    handoff.clientConfig(),
                    Optional.ofNullable(trustStore),
                    !strictServerCertificate
            );
        } catch (IOException e) {
            throw new IOException(
                    "failed to connect quic bulk endpoint "
                            + handoff.host() + ":" + handoff.transportPort()
                            + ": " + e.getMessage(),
                    e
            );
        }
        try {
            return CxnFileNetworkClient.connect(
                    new InetSocketAddress(handoff.host(), handoff.controlPort()),
                    socketFactory,
                    handoff.clientConfig(),
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

    private static KeyStore loadTrustStore(Path trustStorePath) throws IOException {
        try {
            KeyStore trustStore = KeyStore.getInstance(keyStoreType(trustStorePath));
            try (InputStream input = Files.newInputStream(trustStorePath)) {
                trustStore.load(input, new char[0]);
            }
            return trustStore;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to load client truststore: " + trustStorePath, e);
        }
    }

    private static String keyStoreType(Path path) {
        String name = path.getFileName() == null
                ? ""
                : path.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx")) {
            return "PKCS12";
        }
        if (name.endsWith(".jks")) {
            return "JKS";
        }
        return KeyStore.getDefaultType();
    }

    private static void loop(CxnFileNetworkClient client, BridgeInput in, PrintStream out, PrintStream err)
            throws IOException {
        BridgeWriteStreams writeStreams = new BridgeWriteStreams(client, err);
        String line;
        try {
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    handle(client, writeStreams, in, line, out, err);
                } catch (IllegalArgumentException e) {
                    writeLine(out, "ERR\t" + clean(e.getMessage()));
                } catch (UncheckedIOException e) {
                    writeLine(out, "ERR\t" + clean(e.getCause().getMessage()));
                } catch (IOException e) {
                    writeLine(out, "ERR\t" + clean(e.getMessage()));
                }
            }
        } finally {
            writeStreams.abortAll();
        }
    }

    private static void handle(
            CxnFileNetworkClient client,
            BridgeWriteStreams writeStreams,
            BridgeInput input,
            String line,
            PrintStream out,
            PrintStream err
    )
            throws IOException {
        String[] parts = line.split("\t", -1);
        String command = parts[0];
        switch (command) {
            case "STAT" -> {
                requireArity(parts, 2, command);
                Optional<FileMetadata> metadata = client.stat(parts[1]);
                if (metadata.isEmpty()) {
                    writeLine(out, "NOT_FOUND");
                    return;
                }
                writeMetadata(out, metadata.orElseThrow());
            }
            case "LIST" -> {
                requireArity(parts, 2, command);
                DirectoryListing listing = client.list(parts[1]);
                writeLine(out, "OK\t" + listing.entries().size());
                for (FileMetadata entry : listing.entries()) {
                    writeLine(out, "ENTRY\t"
                            + entry.type().name()
                            + "\t" + entry.size()
                            + "\t" + epochMillis(entry.modifiedAt())
                            + "\t" + clean(entry.name()));
                }
                writeLine(out, "END");
            }
            case "READ" -> {
                requireArity(parts, 4, command);
                String path = parts[1];
                long offset = Long.parseLong(parts[2]);
                long length = Long.parseLong(parts[3]);
                if (offset < 0 || length < 0 || length > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("invalid read range");
                }
                byte[] data = client.readRange(path, offset, length, TransferPayloadFormat.CONTAINER);
                writeLine(out, "DATA\t" + data.length);
                out.write(data);
                out.write('\n');
                writeLine(out, "END");
            }
            case "WRITE" -> {
                requireArity(parts, 3, command);
                String path = parts[1];
                long length = parseWriteLength(parts[2]);
                ContentHash fileHash = null;
                Exception failure = null;
                try {
                    fileHash = writeFile(client, path, input, length, err);
                } catch (IOException | RuntimeException e) {
                    failure = e;
                }
                try {
                    consumeWriteTerminator(input);
                } catch (IOException e) {
                    if (failure != null) {
                        failure.addSuppressed(e);
                    } else {
                        throw e;
                    }
                }
                if (failure instanceof IOException e) {
                    throw e;
                }
                if (failure instanceof RuntimeException e) {
                    throw e;
                }
                if (fileHash == null) {
                    throw new IOException("write completed without a file hash");
                }
                writeLine(out, "OK\t" + length + "\t" + fileHash);
            }
            case "WRITE_CHUNK" -> {
                requireArity(parts, 4, command);
                String path = parts[1];
                long offset = Long.parseLong(parts[2]);
                int length = parseWriteChunkLength(parts[3]);
                byte[] data = input.readExact(length);
                writeStreams.writeChunk(path, offset, data);
                writeLine(out, "OK\t" + length);
            }
            case "WRITE_COMMIT" -> {
                requireArity(parts, 3, command);
                String path = parts[1];
                long length = parseWriteLength(parts[2]);
                ContentHash fileHash = writeStreams.commit(path, length);
                writeLine(out, "OK\t" + length + "\t" + fileHash);
            }
            case "WRITE_ABORT" -> {
                requireArity(parts, 2, command);
                writeStreams.abort(parts[1]);
                writeLine(out, "OK");
            }
            case "DELETE" -> {
                requireArity(parts, 3, command);
                DeletePathResponse response = client.deletePath(parts[1], Boolean.parseBoolean(parts[2]));
                writeLine(out, "OK\t" + response.deletedEntries());
            }
            case "RENAME" -> {
                requireArity(parts, 3, command);
                RenamePathResponse response = client.renamePath(parts[1], parts[2]);
                writeLine(out, "OK\t" + clean(response.targetPath()));
            }
            case "QUIT" -> writeLine(out, "OK");
            default -> throw new IllegalArgumentException("unknown bridge command: " + command);
        }
    }

    private static void consumeWriteTerminator(BridgeInput input) throws IOException {
        int separator = input.read();
        if (separator != '\n') {
            throw new IOException("invalid bridge write payload separator");
        }
        String end = input.readLine();
        if (!"END".equals(end)) {
            throw new IOException("invalid bridge write payload terminator");
        }
    }

    private static long parseWriteLength(String value) {
        long length = Long.parseLong(value);
        if (length < 0) {
            throw new IllegalArgumentException("invalid write length");
        }
        return length;
    }

    private static int parseWriteChunkLength(String value) {
        long length = parseWriteLength(value);
        if (length <= 0 || length > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid write chunk length");
        }
        return (int) length;
    }

    private static ContentHash writeFile(
            CxnFileNetworkClient client,
            String path,
            BridgeInput input,
            long length,
            PrintStream diagnostics
    ) throws IOException {
        long totalStart = System.nanoTime();
        long bootstrapNanos = 0L;
        long entryNanos = 0L;
        long payloadNanos = 0L;
        long metadataNanos = 0L;
        long entryCommitNanos = 0L;
        long transferCommitNanos = 0L;
        long frames = 0L;
        TransferAcceptedResponse transfer = null;
        Exception failure = null;
        WriteFramePipeline pipeline = null;
        WritePipelineMetrics pipelineMetrics = WritePipelineMetrics.empty();
        try {
            long phaseStart = System.nanoTime();
            transfer = client.bootstrapWrite(path, TransferPayloadFormat.CONTAINER);
            if (transfer.direction() != TransferDirection.WRITE || !transfer.writePayloadAdmissionGranted()) {
                throw new IOException("write transfer was not admitted");
            }
            bootstrapNanos = System.nanoTime() - phaseStart;
        } catch (IOException | RuntimeException e) {
            failure = e;
        }
        List<ContentHash> chunkHashes = new ArrayList<>();
        List<Integer> chunkSizes = new ArrayList<>();
        if (failure == null) {
            long phaseStart = System.nanoTime();
            TransferEntryAcceptedResponse entryAccepted = client.acceptWriteEntry(new TransferEntryUpdate(
                    transfer.transferId(),
                    0,
                    TransferEntryAction.CREATE_OR_REPLACE,
                    path,
                    "",
                    length,
                    TransferPayloadFormat.CONTAINER
            ));
            requireWriteEntryAccepted(entryAccepted, transfer, path);
            entryNanos = System.nanoTime() - phaseStart;
        }
        int maxPayloadBytes = transfer == null
                ? BulkDataFrameCodec.MAX_PAYLOAD_BYTES
                : (int) Math.min(
                        BulkDataFrameCodec.MAX_PAYLOAD_BYTES,
                        Math.max(1L, transfer.flowControlWindowBytes())
                );
        long offset = 0L;
        long chunkSequence = 0L;
        WriteCordRamp cordRamp = transfer == null ? null : new WriteCordRamp(transfer, length);
        if (failure == null && transfer != null) {
            pipeline = new WriteFramePipeline(client, transfer.flowControlWindowBytes());
        }
        long payloadStart = System.nanoTime();
        try {
            while (offset < length) {
                int chunkLength = (int) Math.min(maxPayloadBytes, length - offset);
                if (failure == null && pipeline != null) {
                    try {
                        pipeline.awaitCapacity(cordRamp.activeCords(), chunkLength);
                    } catch (IOException | RuntimeException e) {
                        failure = e;
                        pipeline.cancel();
                    }
                }
                byte[] payload = input.readExact(chunkLength);
                ContentHash chunkHash = Hashing.chunk(payload);
                chunkHashes.add(chunkHash);
                chunkSizes.add(chunkLength);
                if (failure == null && pipeline != null) {
                    try {
                        pipeline.submit(new BulkDataFrame(
                                transfer.transferId(),
                                cordRamp.cordIndex(chunkSequence),
                                0,
                                chunkSequence,
                                offset,
                                true,
                                payload
                        ));
                    } catch (IOException | RuntimeException e) {
                        failure = e;
                        pipeline.cancel();
                    }
                }
                offset += chunkLength;
                chunkSequence++;
                frames++;
            }
            if (failure == null && pipeline != null) {
                try {
                    pipeline.complete();
                } catch (IOException | RuntimeException e) {
                    failure = e;
                }
            }
        } finally {
            if (pipeline != null) {
                pipelineMetrics = pipeline.metrics();
                pipeline.close();
            }
        }
        payloadNanos = System.nanoTime() - payloadStart;
        if (failure instanceof IOException e) {
            logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics, "failed");
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics, "failed");
            throw e;
        }
        if (transfer == null) {
            logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics, "failed");
            throw new IOException("write transfer was not admitted");
        }

        ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
        long phaseStart = System.nanoTime();
        try {
            TransferMetadataAcceptedResponse metadataAccepted = client.acceptWriteMetadata(new TransferMetadataUpdate(
                    transfer.transferId(),
                    0,
                    0,
                    true,
                    length,
                    chunkHashes
            ));
            requireMetadataAccepted(metadataAccepted, transfer, chunkHashes.size());
            metadataNanos = System.nanoTime() - phaseStart;
        } catch (IOException | RuntimeException e) {
            metadataNanos = System.nanoTime() - phaseStart;
            logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics,
                    "failed_metadata");
            throw e;
        }
        phaseStart = System.nanoTime();
        try {
            TransferEntryCommittedResponse entryCommitted = client.commitWriteEntry(new TransferEntryCommitRequest(
                    transfer.transferId(),
                    0,
                    path,
                    fileHash,
                    length
            ));
            requireEntryCommitted(entryCommitted, transfer, path, fileHash, length);
            entryCommitNanos = System.nanoTime() - phaseStart;
        } catch (IOException | RuntimeException e) {
            entryCommitNanos = System.nanoTime() - phaseStart;
            logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics,
                    "failed_entry_commit");
            throw e;
        }
        ContentHash transferHash = Hashing.transfer(List.of(fileHash), List.of(length));
        phaseStart = System.nanoTime();
        try {
            TransferCommittedResponse transferCommitted = client.commitWriteTransfer(new TransferCommitRequest(
                    transfer.transferId(),
                    path,
                    transferHash,
                    length
            ));
            requireTransferCommitted(transferCommitted, transfer, transferHash, length);
            transferCommitNanos = System.nanoTime() - phaseStart;
        } catch (IOException | RuntimeException e) {
            transferCommitNanos = System.nanoTime() - phaseStart;
            logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics,
                    "failed_transfer_commit");
            throw e;
        }
        logCopyTimer(diagnostics, path, length, frames, totalStart, bootstrapNanos, entryNanos,
                payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics, "ok");
        return fileHash;
    }

    private static void logCopyTimer(
            PrintStream diagnostics,
            String path,
            long bytes,
            long frames,
            long totalStart,
            long bootstrapNanos,
            long entryNanos,
            long payloadNanos,
            long metadataNanos,
            long entryCommitNanos,
            long transferCommitNanos,
            WritePipelineMetrics pipelineMetrics,
            String status
    ) {
        if (diagnostics == null) {
            return;
        }
        long totalNanos = System.nanoTime() - totalStart;
        double payloadSeconds = Math.max(1.0d, payloadNanos) / 1_000_000_000.0d;
        double payloadMbPerSecond = (bytes / 1_000_000.0d) / payloadSeconds;
        double ackSeconds = Math.max(1.0d, pipelineMetrics.ackNanos()) / 1_000_000_000.0d;
        double ackMbPerSecond = (pipelineMetrics.acknowledgedBytes() / 1_000_000.0d) / ackSeconds;
        diagnostics.printf(
                Locale.ROOT,
                "cxnfs copy timer status=%s path=%s bytes=%d frames=%d total_ms=%.1f bootstrap_ms=%.1f entry_ms=%.1f payload_ms=%.1f payload_mb_s=%.1f ack_mb_s=%.1f mode=adaptive metadata_ms=%.1f entry_commit_ms=%.1f transfer_commit_ms=%.1f pipeline_wait_ms=%.1f max_active_cords=%d max_inflight_frames=%d max_inflight_mb=%.1f max_adaptive_window_mb=%.1f max_frame_ack_ms=%.1f slow_frames=%d window_decreases=%d%n",
                status,
                clean(path),
                bytes,
                frames,
                nanosToMillis(totalNanos),
                nanosToMillis(bootstrapNanos),
                nanosToMillis(entryNanos),
                nanosToMillis(payloadNanos),
                payloadMbPerSecond,
                ackMbPerSecond,
                nanosToMillis(metadataNanos),
                nanosToMillis(entryCommitNanos),
                nanosToMillis(transferCommitNanos),
                nanosToMillis(pipelineMetrics.capacityWaitNanos()),
                pipelineMetrics.maxActiveCords(),
                pipelineMetrics.maxInFlightFrames(),
                pipelineMetrics.maxInFlightBytes() / 1_000_000.0d,
                pipelineMetrics.maxAdaptiveWindowBytes() / 1_000_000.0d,
                nanosToMillis(pipelineMetrics.maxFrameAckNanos()),
                pipelineMetrics.slowFrames(),
                pipelineMetrics.windowDecreases()
        );
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0d;
    }

    private static final class BridgeWriteStreams {
        private final CxnFileNetworkClient client;
        private final PrintStream diagnostics;
        private final Map<String, StreamingWrite> streams = new LinkedHashMap<>();

        private BridgeWriteStreams(CxnFileNetworkClient client, PrintStream diagnostics) {
            this.client = client;
            this.diagnostics = diagnostics;
        }

        private synchronized void writeChunk(String path, long offset, byte[] data) throws IOException {
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("write path is required");
            }
            if (offset < 0) {
                throw new IllegalArgumentException("write chunk offset must be non-negative");
            }
            if (data == null || data.length == 0) {
                throw new IllegalArgumentException("write chunk payload is required");
            }
            StreamingWrite stream = streams.get(path);
            if (stream == null) {
                stream = new StreamingWrite(client, path, diagnostics);
                streams.put(path, stream);
            }
            try {
                stream.writeChunk(offset, data);
            } catch (IOException | RuntimeException e) {
                streams.remove(path);
                stream.abort();
                throw e;
            }
        }

        private synchronized ContentHash commit(String path, long length) throws IOException {
            StreamingWrite stream = streams.remove(path);
            if (stream == null) {
                if (length != 0) {
                    throw new IOException("no active streaming write: " + path);
                }
                stream = new StreamingWrite(client, path, diagnostics);
            }
            try {
                return stream.commit(length);
            } catch (IOException | RuntimeException e) {
                stream.abort();
                throw e;
            }
        }

        private synchronized void abort(String path) {
            StreamingWrite stream = streams.remove(path);
            if (stream != null) {
                stream.abort();
            }
        }

        private synchronized void abortAll() {
            for (StreamingWrite stream : streams.values()) {
                stream.abort();
            }
            streams.clear();
        }
    }

    private static final class StreamingWrite {
        private final CxnFileNetworkClient client;
        private final String path;
        private final PrintStream diagnostics;
        private final TransferAcceptedResponse transfer;
        private final int maxPayloadBytes;
        private final WriteCordRamp cordRamp;
        private final WriteFramePipeline pipeline;
        private final List<ContentHash> chunkHashes = new ArrayList<>();
        private final List<Integer> chunkSizes = new ArrayList<>();
        private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
        private final long totalStart = System.nanoTime();
        private final long payloadStart;
        private long bootstrapNanos;
        private long entryNanos;
        private long payloadNanos;
        private long metadataNanos;
        private long entryCommitNanos;
        private long transferCommitNanos;
        private long pendingOffset;
        private long nextOffset;
        private long frameSequence;
        private long frames;
        private WritePipelineMetrics pipelineMetrics = WritePipelineMetrics.empty();
        private boolean closed;
        private boolean logged;

        private StreamingWrite(CxnFileNetworkClient client, String path, PrintStream diagnostics) throws IOException {
            this.client = client;
            this.path = path;
            this.diagnostics = diagnostics;

            long phaseStart = System.nanoTime();
            this.transfer = client.bootstrapWrite(path, TransferPayloadFormat.CONTAINER);
            if (transfer.direction() != TransferDirection.WRITE || !transfer.writePayloadAdmissionGranted()) {
                throw new IOException("write transfer was not admitted");
            }
            bootstrapNanos = System.nanoTime() - phaseStart;

            phaseStart = System.nanoTime();
            TransferEntryAcceptedResponse entryAccepted = client.acceptWriteEntry(new TransferEntryUpdate(
                    transfer.transferId(),
                    0,
                    TransferEntryAction.CREATE_OR_REPLACE,
                    path,
                    "",
                    0,
                    TransferPayloadFormat.CONTAINER
            ));
            requireWriteEntryAccepted(entryAccepted, transfer, path);
            entryNanos = System.nanoTime() - phaseStart;

            this.maxPayloadBytes = (int) Math.min(
                    BulkDataFrameCodec.MAX_PAYLOAD_BYTES,
                    Math.max(1L, transfer.flowControlWindowBytes())
            );
            this.cordRamp = new WriteCordRamp(transfer, Long.MAX_VALUE);
            this.pipeline = new WriteFramePipeline(client, transfer.flowControlWindowBytes());
            this.payloadStart = System.nanoTime();
        }

        private void writeChunk(long offset, byte[] data) throws IOException {
            if (closed) {
                throw new IOException("streaming write is already closed: " + path);
            }
            if (offset != nextOffset) {
                throw new IOException("streaming write expected offset " + nextOffset + " but got " + offset);
            }
            int cursor = 0;
            while (cursor < data.length) {
                if (pending.size() == 0) {
                    pendingOffset = offset + cursor;
                }
                int writable = Math.min(maxPayloadBytes - pending.size(), data.length - cursor);
                pending.write(data, cursor, writable);
                cursor += writable;
                if (pending.size() == maxPayloadBytes) {
                    flushPendingFrame();
                }
            }
            nextOffset += data.length;
        }

        private ContentHash commit(long length) throws IOException {
            String status = "failed_stream";
            try {
                if (closed) {
                    throw new IOException("streaming write is already closed: " + path);
                }
                if (length != nextOffset) {
                    throw new IOException("streaming write length " + nextOffset
                            + " does not match commit length " + length);
                }
                flushPendingFrame();
                pipeline.complete();
                pipelineMetrics = pipeline.metrics();
                payloadNanos = System.nanoTime() - payloadStart;

                ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
                status = "failed_metadata";
                long phaseStart = System.nanoTime();
                TransferMetadataAcceptedResponse metadataAccepted = client.acceptWriteMetadata(new TransferMetadataUpdate(
                        transfer.transferId(),
                        0,
                        0,
                        true,
                        length,
                        chunkHashes
                ));
                requireMetadataAccepted(metadataAccepted, transfer, chunkHashes.size());
                metadataNanos = System.nanoTime() - phaseStart;

                status = "failed_entry_commit";
                phaseStart = System.nanoTime();
                TransferEntryCommittedResponse entryCommitted = client.commitWriteEntry(new TransferEntryCommitRequest(
                        transfer.transferId(),
                        0,
                        path,
                        fileHash,
                        length
                ));
                requireEntryCommitted(entryCommitted, transfer, path, fileHash, length);
                entryCommitNanos = System.nanoTime() - phaseStart;

                status = "failed_transfer_commit";
                ContentHash transferHash = Hashing.transfer(List.of(fileHash), List.of(length));
                phaseStart = System.nanoTime();
                TransferCommittedResponse transferCommitted = client.commitWriteTransfer(new TransferCommitRequest(
                        transfer.transferId(),
                        path,
                        transferHash,
                        length
                ));
                requireTransferCommitted(transferCommitted, transfer, transferHash, length);
                transferCommitNanos = System.nanoTime() - phaseStart;

                closed = true;
                pipeline.close();
                log("stream_ok", length);
                return fileHash;
            } catch (IOException | RuntimeException e) {
                pipelineMetrics = pipeline.metrics();
                payloadNanos = payloadNanos == 0L ? System.nanoTime() - payloadStart : payloadNanos;
                log(status, Math.max(length, nextOffset));
                throw e;
            }
        }

        private void abort() {
            if (closed) {
                return;
            }
            closed = true;
            pipeline.cancel();
            pipelineMetrics = pipeline.metrics();
            pipeline.close();
        }

        private void flushPendingFrame() throws IOException {
            if (pending.size() == 0) {
                return;
            }
            byte[] payload = pending.toByteArray();
            long frameOffset = pendingOffset;
            pending.reset();
            ContentHash chunkHash = Hashing.chunk(payload);
            chunkHashes.add(chunkHash);
            chunkSizes.add(payload.length);
            pipeline.awaitCapacity(cordRamp.activeCords(), payload.length);
            pipeline.submit(new BulkDataFrame(
                    transfer.transferId(),
                    cordRamp.cordIndex(frameSequence),
                    0,
                    frameSequence,
                    frameOffset,
                    true,
                    payload
            ));
            frameSequence++;
            frames++;
        }

        private void log(String status, long bytes) {
            if (logged) {
                return;
            }
            logged = true;
            logCopyTimer(diagnostics, path, bytes, frames, totalStart, bootstrapNanos, entryNanos,
                    payloadNanos, metadataNanos, entryCommitNanos, transferCommitNanos, pipelineMetrics, status);
        }
    }

    private static void requireWriteEntryAccepted(
            TransferEntryAcceptedResponse response,
            TransferAcceptedResponse transfer,
            String path
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || response.entrySequence() != 0
                || !response.path().equals(path)) {
            throw new IOException("write entry acknowledgement does not match request");
        }
    }

    private static void requireMetadataAccepted(
            TransferMetadataAcceptedResponse response,
            TransferAcceptedResponse transfer,
            int chunkHashCount
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || response.entrySequence() != 0
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
            String path,
            ContentHash fileHash,
            long bytesWritten
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || response.entrySequence() != 0
                || !response.path().equals(path)
                || !response.contentHash().equals(fileHash)
                || response.bytesWritten() != bytesWritten) {
            throw new IOException("write entry commit acknowledgement does not match request");
        }
    }

    private static void requireTransferCommitted(
            TransferCommittedResponse response,
            TransferAcceptedResponse transfer,
            ContentHash transferHash,
            long bytesWritten
    ) throws IOException {
        if (!response.transferId().equals(transfer.transferId())
                || !response.contentHash().equals(transferHash)
                || response.bytesWritten() != bytesWritten) {
            throw new IOException("write transfer commit acknowledgement does not match request");
        }
    }

    private static void writeMetadata(OutputStream out, FileMetadata metadata) throws IOException {
        writeLine(out, "OK\t"
                + metadata.type().name()
                + "\t" + metadata.size()
                + "\t" + epochMillis(metadata.modifiedAt()));
    }

    private static long epochMillis(Instant instant) {
        return instant == null ? 0L : instant.toEpochMilli();
    }

    private static void requireArity(String[] parts, int expected, String command) {
        if (parts.length != expected) {
            throw new IllegalArgumentException(command + " expects " + (expected - 1) + " argument(s)");
        }
    }

    private static String clean(String value) {
        return value == null ? "" : value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.UTF_8));
        out.write('\n');
        out.flush();
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs fuse-bridge <handoff-report> [--credentials-stdin]");
    }

    private static final class BridgeInput {
        private final InputStream input;

        private BridgeInput(InputStream input) {
            this.input = input;
        }

        private String readLine() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            while (true) {
                int value = input.read();
                if (value < 0) {
                    if (bytes.size() == 0) {
                        return null;
                    }
                    break;
                }
                if (value == '\n') {
                    break;
                }
                bytes.write(value);
            }
            return bytes.toString(StandardCharsets.UTF_8);
        }

        private byte[] readExact(int length) throws IOException {
            byte[] data = new byte[length];
            int offset = 0;
            while (offset < length) {
                int read = input.read(data, offset, length - offset);
                if (read < 0) {
                    throw new IOException("bridge input ended before payload completed");
                }
                offset += read;
            }
            return data;
        }

        private int read() throws IOException {
            return input.read();
        }
    }
}
