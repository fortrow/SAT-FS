package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public final class BoundedPayloadStagingArea {
    private final WriteAdmissionController admissionController;
    private final PayloadStagingConfig config;
    private final Map<BulkDataFrameKey, byte[]> memoryPayloads = new LinkedHashMap<>();
    private final Map<BulkDataFrameKey, StagedPayload> stagedPayloads = new LinkedHashMap<>();
    private final Map<BulkDataFrameKey, ContentHash> pendingChunkHashes = new LinkedHashMap<>();
    private long memoryBytes;
    private long spilledBytes;

    public BoundedPayloadStagingArea(WriteAdmissionController admissionController, PayloadStagingConfig config) {
        if (admissionController == null) {
            throw new IllegalArgumentException("admissionController is required");
        }
        if (config == null) {
            throw new IllegalArgumentException("config is required");
        }
        this.admissionController = admissionController;
        this.config = config;
    }

    public synchronized StagedPayload stage(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        admissionController.requirePayloadAdmission(frame.transferId());
        if (frame.payloadLength() > config.maxSinglePayloadBytes()) {
            throw new IllegalArgumentException("payload exceeds maximum staging payload size");
        }
        StagedPayload existing = stagedPayloads.get(frame.key());
        if (existing != null) {
            return requireDuplicatePayloadMatches(frame, existing);
        }

        byte[] payload = frame.payload();
        boolean metadataVerified = false;
        ContentHash pendingHash = pendingChunkHashes.get(frame.key());
        if (pendingHash != null) {
            verifyHashBytes(frame.key(), payload, pendingHash);
            metadataVerified = true;
        }

        admissionController.reservePayload(frame);
        StagedPayload staged;
        try {
            if (canStoreInMemory(payload.length)) {
                memoryPayloads.put(frame.key(), payload);
                memoryBytes += payload.length;
                staged = new StagedPayload(
                        frame.key(),
                        frame.cordIndex(),
                        frame.entryOffset(),
                        payload.length,
                        frame.finalFrameForChunk(),
                        StagedPayloadStorage.MEMORY,
                        Optional.empty(),
                        metadataVerified
                );
            } else {
                Path spillPath = spill(frame, payload);
                spilledBytes += payload.length;
                staged = new StagedPayload(
                        frame.key(),
                        frame.cordIndex(),
                        frame.entryOffset(),
                        payload.length,
                        frame.finalFrameForChunk(),
                        StagedPayloadStorage.SPILL,
                        Optional.of(spillPath),
                        metadataVerified
                );
            }
        } catch (IOException | RuntimeException e) {
            admissionController.rollbackPayloadReservation(frame);
            throw e;
        }
        stagedPayloads.put(frame.key(), staged);
        if (metadataVerified) {
            pendingChunkHashes.remove(frame.key());
        }
        return staged;
    }

    public synchronized int acceptMetadata(TransferMetadataUpdate update) throws IOException {
        if (update == null) {
            throw new IllegalArgumentException("update is required");
        }
        admissionController.requirePayloadAdmission(update.transferId());

        int verified = 0;
        for (int i = 0; i < update.chunkHashes().size(); i++) {
            long chunkSequence = update.metadataSequence() + i;
            BulkDataFrameKey key = new BulkDataFrameKey(update.transferId(), update.entrySequence(), chunkSequence);
            ContentHash expectedHash = update.chunkHashes().get(i);
            ContentHash priorHash = pendingChunkHashes.putIfAbsent(key, expectedHash);
            if (priorHash != null && !priorHash.equals(expectedHash)) {
                throw new IOException("conflicting chunk metadata for " + key);
            }
            StagedPayload staged = stagedPayloads.get(key);
            if (staged == null) {
                continue;
            }
            verifyHash(staged, expectedHash);
            stagedPayloads.put(key, staged.verified());
            pendingChunkHashes.remove(key);
            verified++;
        }
        return verified;
    }

    public synchronized byte[] readPayload(BulkDataFrameKey key) throws IOException {
        StagedPayload staged = stagedPayloads.get(key);
        if (staged == null) {
            throw new IllegalArgumentException("unknown staged payload: " + key);
        }
        byte[] memory = memoryPayloads.get(key);
        if (memory != null) {
            return Arrays.copyOf(memory, memory.length);
        }
        return Files.readAllBytes(staged.spillPath().orElseThrow());
    }

    public synchronized List<StagedPayload> verifiedPayloads(UUID transferId, int entrySequence) {
        List<StagedPayload> result = new ArrayList<>();
        for (StagedPayload payload : stagedPayloads.values()) {
            if (payload.key().transferId().equals(transferId)
                    && payload.key().entrySequence() == entrySequence
                    && payload.metadataVerified()) {
                result.add(payload);
            }
        }
        return List.copyOf(result);
    }

    public synchronized int releaseEntry(UUID transferId, int entrySequence) throws IOException {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }

        List<BulkDataFrameKey> stagedKeys = stagedPayloads.keySet().stream()
                .filter(key -> key.transferId().equals(transferId) && key.entrySequence() == entrySequence)
                .toList();
        for (BulkDataFrameKey key : stagedKeys) {
            releasePayload(key);
        }
        pendingChunkHashes.keySet().removeIf(key -> key.transferId().equals(transferId) && key.entrySequence() == entrySequence);
        return stagedKeys.size();
    }

    public synchronized int releaseTransfer(UUID transferId) throws IOException {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }

        List<BulkDataFrameKey> stagedKeys = stagedPayloads.keySet().stream()
                .filter(key -> key.transferId().equals(transferId))
                .toList();
        for (BulkDataFrameKey key : stagedKeys) {
            releasePayload(key);
        }
        pendingChunkHashes.keySet().removeIf(key -> key.transferId().equals(transferId));
        return stagedKeys.size();
    }

    public synchronized long memoryBytes() {
        return memoryBytes;
    }

    public synchronized long spilledBytes() {
        return spilledBytes;
    }

    public synchronized int stagedPayloadCount() {
        return stagedPayloads.size();
    }

    public synchronized int pendingMetadataCount() {
        return pendingChunkHashes.size();
    }

    private boolean canStoreInMemory(int payloadLength) {
        return memoryBytes + payloadLength <= config.maxMemoryBytes();
    }

    private StagedPayload requireDuplicatePayloadMatches(BulkDataFrame frame, StagedPayload existing) throws IOException {
        if (existing.payloadLength() != frame.payloadLength()) {
            throw new IOException("conflicting duplicate staged payload key: " + frame.key());
        }
        byte[] payload = readPayload(frame.key());
        if (!Arrays.equals(payload, frame.payload())) {
            throw new IOException("conflicting duplicate staged payload key: " + frame.key());
        }
        return existing;
    }

    private Path spill(BulkDataFrame frame, byte[] payload) throws IOException {
        Path directory = config.spillDirectory()
                .orElseThrow(() -> new IllegalStateException("payload staging memory limit exceeded and spill is disabled"));
        Files.createDirectories(directory);
        String filename = frame.transferId()
                + "-"
                + frame.entrySequence()
                + "-"
                + frame.chunkSequence()
                + PayloadStagingCleaner.STAGE_SUFFIX;
        Path path = directory.resolve(filename);
        Files.write(path, payload);
        return path;
    }

    private void verifyHash(StagedPayload staged, ContentHash expectedHash) throws IOException {
        byte[] payload = readPayload(staged.key());
        verifyHashBytes(staged.key(), payload, expectedHash);
    }

    private static void verifyHashBytes(BulkDataFrameKey key, byte[] payload, ContentHash expectedHash) throws IOException {
        ContentHash actualHash = Hashing.chunk(payload);
        if (!actualHash.equals(expectedHash)) {
            throw new IOException("staged payload hash verification failed for " + key);
        }
    }

    public synchronized void releasePayload(BulkDataFrameKey key) throws IOException {
        StagedPayload staged = stagedPayloads.remove(key);
        if (staged == null) {
            return;
        }
        byte[] memory = memoryPayloads.remove(key);
        if (memory != null) {
            memoryBytes -= memory.length;
        }
        if (staged.storage() == StagedPayloadStorage.SPILL) {
            Files.deleteIfExists(staged.spillPath().orElseThrow());
            spilledBytes -= staged.payloadLength();
        }
    }
}
