package com.fortrow.cxnfs.dataset;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SafetensorsOutputSenderTest {
    @TempDir
    Path tempDir;

    @Test
    void sendsPayloadFramesBeforeLateMetadataAndCommit() throws Exception {
        Path output = tempDir.resolve("checkpoint");
        Files.createDirectories(output);
        Files.writeString(output.resolve("config.json"), "{\"hidden\":16}");
        Files.write(output.resolve("model.safetensors"), "tensor-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        RecordingWriteClient client = new RecordingWriteClient();

        DatasetSendResult result = new SafetensorsOutputSender().send(
                DatasetSendPlan.safetensors(output),
                "/outputs/run-1",
                client
        );

        assertEquals("/outputs/run-1", client.bootstrapRoot);
        assertEquals(2, result.fileCount());
        assertEquals(1, result.tensorFileCount());
        assertEquals(Files.size(output.resolve("config.json")) + Files.size(output.resolve("model.safetensors")),
                result.bytesSent());
        assertEquals(List.of(
                "bootstrap",
                "frame:0:0",
                "entry:0",
                "metadata:0",
                "entry-commit:0",
                "frame:1:0",
                "entry:1",
                "metadata:1",
                "entry-commit:1",
                "transfer-commit"
        ), client.events);
        assertEquals("/outputs/run-1/config.json", client.entryUpdates.get(0).path());
        assertEquals("/outputs/run-1/model.safetensors", client.entryUpdates.get(1).path());
        assertEquals(Hashing.chunk(client.frames.get(0).payload()), client.metadataUpdates.get(0).chunkHashes().get(0));
        assertEquals(Hashing.chunk(client.frames.get(1).payload()), client.metadataUpdates.get(1).chunkHashes().get(0));
        ContentHash expectedTransferHash = Hashing.transfer(
                client.entryCommitRequests.stream().map(TransferEntryCommitRequest::contentHash).toList(),
                client.entryCommitRequests.stream().map(TransferEntryCommitRequest::bytesWritten).toList()
        );
        assertEquals(expectedTransferHash, client.transferCommit.contentHash());
        assertEquals(expectedTransferHash, result.contentHash());
    }

    @Test
    void rejectsOutputWithoutTensorFile() throws Exception {
        Path output = tempDir.resolve("metadata-only");
        Files.createDirectories(output);
        Files.writeString(output.resolve("config.json"), "{}");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () ->
                new SafetensorsOutputSender().send(DatasetSendPlan.safetensors(output), "/outputs/run-1", new RecordingWriteClient())
        );

        assertTrue(failure.getMessage().contains(".safetensors"));
    }

    @Test
    void rejectsRemoteRootTraversal() throws Exception {
        Path output = tempDir.resolve("checkpoint");
        Files.createDirectories(output);
        Files.write(output.resolve("model.safetensors"), new byte[]{1});

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () ->
                new SafetensorsOutputSender().send(DatasetSendPlan.safetensors(output), "/outputs/../run", new RecordingWriteClient())
        );

        assertTrue(failure.getMessage().contains("parent"));
    }

    @Test
    void keepsTinyChunkFramesOnOneCordBeforeLateMetadata() throws Exception {
        Path output = tempDir.resolve("multi-chunk-checkpoint");
        Files.createDirectories(output);
        Files.write(output.resolve("model.safetensors"), "0123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        RecordingWriteClient client = new RecordingWriteClient(2, 4);

        DatasetSendResult result = new SafetensorsOutputSender(4).send(
                DatasetSendPlan.safetensors(output),
                "/outputs/run-1",
                client
        );

        assertEquals(1, result.fileCount());
        assertEquals(3, client.frames.size());
        assertEquals(List.of(0L, 1L, 2L), client.frames.stream()
                .map(BulkDataFrame::chunkSequence)
                .sorted()
                .toList());
        assertEquals(List.of(0, 0, 0), client.frames.stream()
                .sorted(java.util.Comparator.comparingLong(BulkDataFrame::chunkSequence))
                .map(BulkDataFrame::cordIndex)
                .toList());
        assertEquals(3, client.metadataUpdates.getFirst().chunkHashes().size());
        assertTrue(client.events.indexOf("entry:0") > client.events.lastIndexOf("frame:0:2"));
        assertTrue(client.events.indexOf("metadata:0") > client.events.indexOf("entry:0"));
    }

    @Test
    void dispatchesLargerFramesAcrossAcceptedInitialCordsBeforeLateMetadata() throws Exception {
        Path output = tempDir.resolve("default-window-checkpoint");
        Files.createDirectories(output);
        Files.write(output.resolve("model.safetensors"), new byte[2 * 1024 * 1024]);
        ConcurrentWriteClient client = new ConcurrentWriteClient(2, 4);

        DatasetSendResult result = new SafetensorsOutputSender(512 * 1024).send(
                DatasetSendPlan.safetensors(output),
                "/outputs/run-1",
                client
        );

        assertEquals(1, result.fileCount());
        assertEquals(4, client.frames.size());
        assertTrue(client.maxActiveSends.get() >= 2);
        assertEquals(List.of(0L, 1L, 2L, 3L), client.frames.stream()
                .map(BulkDataFrame::chunkSequence)
                .sorted()
                .toList());
        assertEquals(List.of(0, 1), client.frames.stream()
                .sorted(java.util.Comparator.comparingLong(BulkDataFrame::chunkSequence))
                .limit(2)
                .map(BulkDataFrame::cordIndex)
                .toList());
        assertTrue(client.events.indexOf("entry:0") > client.events.lastIndexOf("frame:0:2"));
        assertTrue(client.events.indexOf("metadata:0") > client.events.indexOf("entry:0"));
    }

    @Test
    void abortsWriteClientWhenConcurrentFrameSendFails() throws Exception {
        Path output = tempDir.resolve("failing-cord-checkpoint");
        Files.createDirectories(output);
        Files.write(output.resolve("model.safetensors"), new byte[2 * 1024 * 1024]);
        AbortAwareFailingWriteClient client = new AbortAwareFailingWriteClient();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<DatasetSendResult> send = executor.submit(() -> new SafetensorsOutputSender(512 * 1024).send(
                    DatasetSendPlan.safetensors(output),
                    "/outputs/run-1",
                    client
            ));

            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> send.get(1, TimeUnit.SECONDS)
            );

            assertTrue(failure.getCause() instanceof IOException);
            assertTrue(failure.getCause().getMessage().contains("cord failed"));
            assertEquals(1, client.abortCalls.get());
            assertTrue(client.entryUpdates.isEmpty());
            assertTrue(client.metadataUpdates.isEmpty());
            assertTrue(client.entryCommitRequests.isEmpty());
            assertEquals(null, client.transferCommit);
        } finally {
            client.abortWriteFrames();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectsMismatchedWriteEntryAcknowledgement() throws Exception {
        Path output = checkpointWithModel();
        MismatchedAckWriteClient client = new MismatchedAckWriteClient("entry");

        IOException failure = assertThrows(IOException.class, () ->
                new SafetensorsOutputSender().send(DatasetSendPlan.safetensors(output), "/outputs/run-1", client)
        );

        assertTrue(failure.getMessage().contains("write entry acknowledgement"));
        assertTrue(client.metadataUpdates.isEmpty());
        assertTrue(client.entryCommitRequests.isEmpty());
    }

    @Test
    void rejectsMismatchedWriteMetadataAcknowledgement() throws Exception {
        Path output = checkpointWithModel();
        MismatchedAckWriteClient client = new MismatchedAckWriteClient("metadata");

        IOException failure = assertThrows(IOException.class, () ->
                new SafetensorsOutputSender().send(DatasetSendPlan.safetensors(output), "/outputs/run-1", client)
        );

        assertTrue(failure.getMessage().contains("write metadata acknowledgement"));
        assertTrue(client.entryCommitRequests.isEmpty());
    }

    @Test
    void rejectsMismatchedWriteEntryCommitAcknowledgement() throws Exception {
        Path output = checkpointWithModel();
        MismatchedAckWriteClient client = new MismatchedAckWriteClient("entry-commit");

        IOException failure = assertThrows(IOException.class, () ->
                new SafetensorsOutputSender().send(DatasetSendPlan.safetensors(output), "/outputs/run-1", client)
        );

        assertTrue(failure.getMessage().contains("write entry commit acknowledgement"));
        assertEquals(null, client.transferCommit);
    }

    @Test
    void rejectsMismatchedWriteTransferCommitAcknowledgement() throws Exception {
        Path output = checkpointWithModel();
        MismatchedAckWriteClient client = new MismatchedAckWriteClient("transfer-commit");

        IOException failure = assertThrows(IOException.class, () ->
                new SafetensorsOutputSender().send(DatasetSendPlan.safetensors(output), "/outputs/run-1", client)
        );

        assertTrue(failure.getMessage().contains("write transfer commit acknowledgement"));
        assertEquals(1, client.entryCommitRequests.size());
    }

    private Path checkpointWithModel() throws IOException {
        Path output = tempDir.resolve("checkpoint-" + UUID.randomUUID());
        Files.createDirectories(output);
        Files.write(output.resolve("model.safetensors"), "tensor-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return output;
    }

    private static class RecordingWriteClient implements SafetensorsOutputSender.WriteClient {
        private final UUID transferId = UUID.randomUUID();
        final List<String> events = new ArrayList<>();
        final List<BulkDataFrame> frames = new ArrayList<>();
        final List<TransferEntryUpdate> entryUpdates = new ArrayList<>();
        final List<TransferMetadataUpdate> metadataUpdates = new ArrayList<>();
        final List<TransferEntryCommitRequest> entryCommitRequests = new ArrayList<>();
        private final int acceptedInitialCords;
        private final int acceptedMaxCords;
        private String bootstrapRoot;
        TransferCommitRequest transferCommit;

        private RecordingWriteClient() {
            this(2, 4);
        }

        private RecordingWriteClient(int acceptedInitialCords, int acceptedMaxCords) {
            this.acceptedInitialCords = acceptedInitialCords;
            this.acceptedMaxCords = acceptedMaxCords;
        }

        @Override
        public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) {
            events.add("bootstrap");
            bootstrapRoot = rootPath;
            assertEquals(TransferPayloadFormat.SAFETENSORS, payloadFormat);
            return new TransferAcceptedResponse(
                    transferId,
                    TransferDirection.WRITE,
                    acceptedInitialCords,
                    acceptedMaxCords,
                    64 * 1024 * 1024,
                    true
            );
        }

        @Override
        public void sendWriteFrame(BulkDataFrame frame) throws IOException {
            events.add("frame:" + frame.entrySequence() + ":" + frame.chunkSequence());
            frames.add(frame);
        }

        @Override
        public TransferEntryAcceptedResponse acceptWriteEntry(TransferEntryUpdate update) {
            events.add("entry:" + update.entrySequence());
            entryUpdates.add(update);
            return new TransferEntryAcceptedResponse(update.transferId(), update.entrySequence(), update.path());
        }

        @Override
        public TransferMetadataAcceptedResponse acceptWriteMetadata(TransferMetadataUpdate update) {
            events.add("metadata:" + update.entrySequence());
            metadataUpdates.add(update);
            return new TransferMetadataAcceptedResponse(
                    update.transferId(),
                    update.entrySequence(),
                    update.metadataSequence(),
                    update.chunkHashes().size(),
                    update.chunkHashes().size(),
                    update.finalMetadata()
            );
        }

        @Override
        public TransferEntryCommittedResponse commitWriteEntry(TransferEntryCommitRequest request) {
            events.add("entry-commit:" + request.entrySequence());
            entryCommitRequests.add(request);
            return new TransferEntryCommittedResponse(
                    request.transferId(),
                    request.entrySequence(),
                    request.path(),
                    request.contentHash(),
                    request.bytesWritten()
            );
        }

        @Override
        public TransferCommittedResponse commitWriteTransfer(TransferCommitRequest request) throws IOException {
            events.add("transfer-commit");
            transferCommit = request;
            return new TransferCommittedResponse(request.transferId(), request.contentHash(), request.bytesWritten());
        }
    }

    private static final class ConcurrentWriteClient extends RecordingWriteClient {
        private final CountDownLatch firstTwoFramesInFlight = new CountDownLatch(2);
        private final AtomicInteger activeSends = new AtomicInteger();
        private final AtomicInteger maxActiveSends = new AtomicInteger();

        private ConcurrentWriteClient(int acceptedInitialCords, int acceptedMaxCords) {
            super(acceptedInitialCords, acceptedMaxCords);
        }

        @Override
        public void sendWriteFrame(BulkDataFrame frame) throws IOException {
            int active = activeSends.incrementAndGet();
            maxActiveSends.accumulateAndGet(active, Math::max);
            try {
                if (frame.entrySequence() == 0 && frame.chunkSequence() < 2) {
                    firstTwoFramesInFlight.countDown();
                    if (!firstTwoFramesInFlight.await(2, TimeUnit.SECONDS)) {
                        throw new IOException("timed out waiting for concurrent frame dispatch");
                    }
                }
                synchronized (this) {
                    super.sendWriteFrame(frame);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for concurrent frame dispatch", e);
            } finally {
                activeSends.decrementAndGet();
            }
        }
    }

    private static final class MismatchedAckWriteClient extends RecordingWriteClient {
        private final String mode;

        private MismatchedAckWriteClient(String mode) {
            this.mode = mode;
        }

        @Override
        public TransferEntryAcceptedResponse acceptWriteEntry(TransferEntryUpdate update) {
            super.acceptWriteEntry(update);
            if (mode.equals("entry")) {
                return new TransferEntryAcceptedResponse(update.transferId(), update.entrySequence(), update.path() + ".wrong");
            }
            return new TransferEntryAcceptedResponse(update.transferId(), update.entrySequence(), update.path());
        }

        @Override
        public TransferMetadataAcceptedResponse acceptWriteMetadata(TransferMetadataUpdate update) {
            super.acceptWriteMetadata(update);
            if (mode.equals("metadata")) {
                return new TransferMetadataAcceptedResponse(
                        update.transferId(),
                        update.entrySequence(),
                        update.metadataSequence(),
                        update.chunkHashes().size(),
                        Math.max(0, update.chunkHashes().size() - 1),
                        update.finalMetadata()
                );
            }
            return new TransferMetadataAcceptedResponse(
                    update.transferId(),
                    update.entrySequence(),
                    update.metadataSequence(),
                    update.chunkHashes().size(),
                    update.chunkHashes().size(),
                    update.finalMetadata()
            );
        }

        @Override
        public TransferEntryCommittedResponse commitWriteEntry(TransferEntryCommitRequest request) {
            super.commitWriteEntry(request);
            if (mode.equals("entry-commit")) {
                return new TransferEntryCommittedResponse(
                        request.transferId(),
                        request.entrySequence(),
                        request.path(),
                        Hashing.chunk(new byte[]{1}),
                        request.bytesWritten()
                );
            }
            return new TransferEntryCommittedResponse(
                    request.transferId(),
                    request.entrySequence(),
                    request.path(),
                    request.contentHash(),
                    request.bytesWritten()
            );
        }

        @Override
        public TransferCommittedResponse commitWriteTransfer(TransferCommitRequest request) throws IOException {
            super.commitWriteTransfer(request);
            if (mode.equals("transfer-commit")) {
                return new TransferCommittedResponse(request.transferId(), Hashing.chunk(new byte[]{2}), request.bytesWritten());
            }
            return new TransferCommittedResponse(request.transferId(), request.contentHash(), request.bytesWritten());
        }
    }

    private static final class AbortAwareFailingWriteClient extends RecordingWriteClient {
        private final CountDownLatch secondFrameStarted = new CountDownLatch(1);
        private final CountDownLatch aborted = new CountDownLatch(1);
        private final AtomicInteger abortCalls = new AtomicInteger();

        private AbortAwareFailingWriteClient() {
            super(2, 4);
        }

        @Override
        public void sendWriteFrame(BulkDataFrame frame) throws IOException {
            if (frame.chunkSequence() == 0) {
                await(secondFrameStarted, "second frame did not start");
                await(aborted, "write client was not aborted");
                throw new IOException("transport closed after abort");
            }
            if (frame.chunkSequence() == 1) {
                secondFrameStarted.countDown();
                throw new IOException("cord failed");
            }
            super.sendWriteFrame(frame);
        }

        @Override
        public void abortWriteFrames() {
            abortCalls.incrementAndGet();
            aborted.countDown();
        }

        private static void await(CountDownLatch latch, String message) throws IOException {
            try {
                if (!latch.await(2, TimeUnit.SECONDS)) {
                    throw new IOException(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for abort test", e);
            }
        }
    }
}
