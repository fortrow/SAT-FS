package com.fortrow.cxnfs.dataset;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
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
import com.fortrow.cxnfs.transport.WriteCordRamp;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public final class SafetensorsOutputSender {
    private final int maxPayloadBytes;

    public SafetensorsOutputSender() {
        this(BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
    }

    SafetensorsOutputSender(int maxPayloadBytes) {
        if (maxPayloadBytes <= 0 || maxPayloadBytes > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("maxPayloadBytes must be between 1 and "
                    + BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
        }
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public DatasetSendResult send(DatasetSendPlan plan, String remoteRoot, WriteClient client) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (client == null) {
            throw new IllegalArgumentException("client is required");
        }
        if (plan.format() != DatasetFormat.SAFETENSORS) {
            throw new IllegalArgumentException("checkpoint sends must use safetensors");
        }
        Path root = plan.outputDirectory().toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("output directory does not exist: " + root);
        }
        String normalizedRemoteRoot = normalizeRemoteRoot(remoteRoot);
        List<Path> files = inventoryFiles(root);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("output directory has no files: " + root);
        }
        int tensorFileCount = (int) files.stream().filter(SafetensorsOutputSender::isSafetensorsFile).count();
        if (tensorFileCount == 0) {
            throw new IllegalArgumentException("at least one .safetensors file is required");
        }

        TransferAcceptedResponse transfer = client.bootstrapWrite(normalizedRemoteRoot, TransferPayloadFormat.SAFETENSORS);
        if (transfer.direction() != TransferDirection.WRITE || !transfer.writePayloadAdmissionGranted()) {
            throw new IllegalStateException("server did not admit write payload transfer");
        }

        long totalBytes = 0;
        List<ContentHash> entryHashes = new ArrayList<>(files.size());
        List<Long> entrySizes = new ArrayList<>(files.size());
        try (ExecutorService frameExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int entrySequence = 0; entrySequence < files.size(); entrySequence++) {
                Path file = files.get(entrySequence);
                String relativePath = relativePath(root, file);
                long size = Files.size(file);
                SentEntry entry = sendEntry(
                        client,
                        transfer,
                        frameExecutor,
                        file,
                        normalizedRemoteRoot,
                        relativePath,
                        entrySequence,
                        size
                );
                entryHashes.add(entry.contentHash());
                entrySizes.add(size);
                totalBytes += size;
            }
        }

        ContentHash transferHash = Hashing.transfer(entryHashes, entrySizes);
        TransferCommittedResponse transferCommitted = client.commitWriteTransfer(new TransferCommitRequest(
                transfer.transferId(),
                normalizedRemoteRoot,
                transferHash,
                totalBytes
        ));
        requireTransferCommitted(transferCommitted, transfer.transferId(), transferHash, totalBytes);
        return new DatasetSendResult(root, normalizedRemoteRoot, files.size(), tensorFileCount, totalBytes, transferHash);
    }

    private SentEntry sendEntry(
            WriteClient client,
            TransferAcceptedResponse transfer,
            ExecutorService frameExecutor,
            Path file,
            String remoteRoot,
            String relativePath,
            int entrySequence,
            long size
    ) throws IOException {
        List<ContentHash> chunkHashes = new ArrayList<>();
        List<Integer> chunkSizes = new ArrayList<>();
        int payloadBufferBytes = (int) Math.min(
                maxPayloadBytes,
                Math.min((long) BulkDataFrameCodec.MAX_PAYLOAD_BYTES, transfer.flowControlWindowBytes())
        );
        byte[] buffer = new byte[payloadBufferBytes];
        long offset = 0;
        long chunkSequence = 0;
        WriteCordRamp cordRamp = new WriteCordRamp(transfer, size);

        try (InputStream input = Files.newInputStream(file)) {
            ConcurrentWriteFrameSender frameSender = new ConcurrentWriteFrameSender(
                    frameExecutor,
                    client
            );
            try {
                while (true) {
                    int read = input.readNBytes(buffer, 0, buffer.length);
                    if (read <= 0) {
                        break;
                    }
                    byte[] payload = java.util.Arrays.copyOf(buffer, read);
                    ContentHash chunkHash = Hashing.chunk(payload);
                    chunkHashes.add(chunkHash);
                    chunkSizes.add(read);
                    int activeCords = cordRamp.activeCords();
                    BulkDataFrame frame = new BulkDataFrame(
                            transfer.transferId(),
                            Math.floorMod(chunkSequence, activeCords),
                            entrySequence,
                            chunkSequence,
                            offset,
                            true,
                            payload
                    );
                    frameSender.submit(frame, activeCords);
                    offset += read;
                    chunkSequence++;
                }
                frameSender.drain();
            } catch (IOException | RuntimeException | Error e) {
                abortAfterFailure(frameSender, e);
                throw e;
            }
        }

        ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
        String remotePath = joinRemotePath(remoteRoot, relativePath);
        TransferEntryAcceptedResponse entryAccepted = client.acceptWriteEntry(new TransferEntryUpdate(
                transfer.transferId(),
                entrySequence,
                TransferEntryAction.CREATE_OR_REPLACE,
                remotePath,
                "",
                size,
                TransferPayloadFormat.SAFETENSORS
        ));
        requireEntryAccepted(entryAccepted, transfer.transferId(), entrySequence, remotePath);
        TransferMetadataAcceptedResponse metadataAccepted = client.acceptWriteMetadata(new TransferMetadataUpdate(
                transfer.transferId(),
                entrySequence,
                0,
                true,
                size,
                chunkHashes
        ));
        requireMetadataAccepted(metadataAccepted, transfer.transferId(), entrySequence, 0, chunkHashes.size(), true);
        TransferEntryCommittedResponse entryCommitted = client.commitWriteEntry(new TransferEntryCommitRequest(
                transfer.transferId(),
                entrySequence,
                remotePath,
                fileHash,
                size
        ));
        requireEntryCommitted(entryCommitted, transfer.transferId(), entrySequence, remotePath, fileHash, size);
        return new SentEntry(fileHash);
    }

    private static final class ConcurrentWriteFrameSender {
        private final ExecutorCompletionService<Void> completions;
        private final WriteClient client;
        private final Deque<Future<Void>> pendingFrames = new ArrayDeque<>();
        private int inFlight;
        private boolean aborted;

        private ConcurrentWriteFrameSender(
                ExecutorService executor,
                WriteClient client
        ) {
            if (executor == null) {
                throw new IllegalArgumentException("executor is required");
            }
            if (client == null) {
                throw new IllegalArgumentException("client is required");
            }
            this.completions = new ExecutorCompletionService<>(executor);
            this.client = client;
        }

        private void submit(BulkDataFrame frame, int inFlightLimit) throws IOException {
            if (aborted) {
                throw new IOException("bulk write frame sender is closed");
            }
            if (inFlightLimit <= 0) {
                throw new IllegalArgumentException("inFlightLimit must be positive");
            }
            Future<Void> future = completions.submit(() -> {
                client.sendWriteFrame(frame);
                return null;
            });
            pendingFrames.addLast(future);
            inFlight++;
            if (inFlight >= inFlightLimit) {
                awaitOneCompletedFrame();
            }
        }

        private void drain() throws IOException {
            while (inFlight > 0) {
                awaitOneCompletedFrame();
            }
        }

        private void abort() throws IOException {
            if (aborted) {
                return;
            }
            aborted = true;
            cancelFrames(pendingFrames);
            client.abortWriteFrames();
        }

        private void awaitOneCompletedFrame() throws IOException {
            Future<Void> completed;
            try {
                completed = completions.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while sending write frame", e);
            }
            pendingFrames.remove(completed);
            inFlight--;
            awaitFrame(completed);
        }
    }

    private static void awaitFrame(Future<Void> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while sending write frame", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("write frame send failed", cause);
        }
    }

    private static void cancelFrames(Deque<Future<Void>> inFlightFrames) {
        for (Future<Void> future : inFlightFrames) {
            future.cancel(true);
        }
        inFlightFrames.clear();
    }

    private static void abortAfterFailure(ConcurrentWriteFrameSender frameSender, Throwable failure) {
        try {
            frameSender.abort();
        } catch (IOException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    private static void requireEntryAccepted(
            TransferEntryAcceptedResponse response,
            java.util.UUID transferId,
            int entrySequence,
            String path
    ) throws IOException {
        if (!transferId.equals(response.transferId())
                || response.entrySequence() != entrySequence
                || !path.equals(response.path())) {
            throw new IOException("write entry acknowledgement does not match request");
        }
    }

    private static void requireMetadataAccepted(
            TransferMetadataAcceptedResponse response,
            java.util.UUID transferId,
            int entrySequence,
            int metadataSequence,
            int chunkHashCount,
            boolean finalMetadata
    ) throws IOException {
        if (!transferId.equals(response.transferId())
                || response.entrySequence() != entrySequence
                || response.metadataSequence() != metadataSequence
                || response.chunkHashesAccepted() != chunkHashCount
                || response.stagedPayloadsVerified() != chunkHashCount
                || response.finalMetadata() != finalMetadata) {
            throw new IOException("write metadata acknowledgement does not match request");
        }
    }

    private static void requireEntryCommitted(
            TransferEntryCommittedResponse response,
            java.util.UUID transferId,
            int entrySequence,
            String path,
            ContentHash contentHash,
            long bytesWritten
    ) throws IOException {
        if (!transferId.equals(response.transferId())
                || response.entrySequence() != entrySequence
                || !path.equals(response.path())
                || !contentHash.equals(response.contentHash())
                || response.bytesWritten() != bytesWritten) {
            throw new IOException("write entry commit acknowledgement does not match request");
        }
    }

    private static void requireTransferCommitted(
            TransferCommittedResponse response,
            java.util.UUID transferId,
            ContentHash contentHash,
            long bytesWritten
    ) throws IOException {
        if (!transferId.equals(response.transferId())
                || !contentHash.equals(response.contentHash())
                || response.bytesWritten() != bytesWritten) {
            throw new IOException("write transfer commit acknowledgement does not match request");
        }
    }

    private static List<Path> inventoryFiles(Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .sorted(Comparator.comparing(path -> relativePath(root, path)))
                    .toList();
        }
    }

    private static boolean isSafetensorsFile(Path path) {
        return path.getFileName() != null
                && path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".safetensors");
    }

    private static String relativePath(Path root, Path file) {
        String relative = root.relativize(file.toAbsolutePath().normalize()).toString().replace('\\', '/');
        validateRelativePath(relative);
        return relative;
    }

    private static String normalizeRemoteRoot(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("remote root is required");
        }
        String normalized = value.trim().replace('\\', '/');
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (!normalized.equals("/")) {
            validateRemotePath(normalized);
        }
        return normalized;
    }

    private static String joinRemotePath(String remoteRoot, String relativePath) {
        return remoteRoot.equals("/") ? "/" + relativePath : remoteRoot + "/" + relativePath;
    }

    private static void validateRemotePath(String path) {
        String[] parts = path.substring(1).split("/");
        for (String part : parts) {
            if (part.isBlank() || part.equals(".") || part.equals("..")) {
                throw new IllegalArgumentException("remote root must not contain empty, current, or parent segments");
            }
        }
    }

    private static void validateRelativePath(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("relative file path is required");
        }
        for (String part : path.split("/")) {
            if (part.isBlank() || part.equals(".") || part.equals("..")) {
                throw new IllegalArgumentException("relative file paths must not contain empty, current, or parent segments");
            }
        }
    }

    public interface WriteClient {
        TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) throws IOException;

        void sendWriteFrame(BulkDataFrame frame) throws IOException;

        default void abortWriteFrames() throws IOException {
            if (this instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException("failed to abort write frames", e);
                }
            }
        }

        TransferEntryAcceptedResponse acceptWriteEntry(TransferEntryUpdate update) throws IOException;

        TransferMetadataAcceptedResponse acceptWriteMetadata(TransferMetadataUpdate update) throws IOException;

        TransferEntryCommittedResponse commitWriteEntry(TransferEntryCommitRequest request) throws IOException;

        TransferCommittedResponse commitWriteTransfer(TransferCommitRequest request) throws IOException;
    }

    private record SentEntry(ContentHash contentHash) {
    }
}
