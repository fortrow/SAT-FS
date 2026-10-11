package com.fortrow.cxnfs.transport;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class BulkDataFrame {
    private final UUID transferId;
    private final int cordIndex;
    private final int entrySequence;
    private final long chunkSequence;
    private final long entryOffset;
    private final boolean finalFrameForChunk;
    private final int payloadLength;
    private final byte[] payload;
    private final Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern;

    public BulkDataFrame(
            UUID transferId,
            int cordIndex,
            int entrySequence,
            long chunkSequence,
            long entryOffset,
            boolean finalFrameForChunk,
            byte[] payload
    ) {
        this(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                payload == null ? 0 : payload.length,
                payload,
                Optional.empty()
        );
    }

    public static BulkDataFrame wrapPayloadForTransport(
            UUID transferId,
            int cordIndex,
            int entrySequence,
            long chunkSequence,
            long entryOffset,
            boolean finalFrameForChunk,
            byte[] payload
    ) {
        return new BulkDataFrame(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                payload == null ? 0 : payload.length,
                payload,
                Optional.empty(),
                false
        );
    }

    public BulkDataFrame(
            UUID transferId,
            int cordIndex,
            int entrySequence,
            long chunkSequence,
            long entryOffset,
            boolean finalFrameForChunk,
            BulkPayloadPatternCodec.DecodedPayload decodedPayload
    ) {
        this(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                decodedPayload == null ? 0 : decodedPayload.originalLength(),
                decodedPayload == null ? null : decodedPayload.payloadViewForTransport(),
                decodedPayload == null ? Optional.empty() : decodedPayload.pattern(),
                false
        );
    }

    public BulkDataFrame(
            UUID transferId,
            int cordIndex,
            int entrySequence,
            long chunkSequence,
            long entryOffset,
            boolean finalFrameForChunk,
            int payloadLength,
            BulkPayloadPatternCodec.PayloadPattern payloadPattern
    ) {
        this(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                payloadLength,
                null,
                Optional.ofNullable(payloadPattern)
        );
    }

    private BulkDataFrame(
            UUID transferId,
            int cordIndex,
            int entrySequence,
            long chunkSequence,
            long entryOffset,
            boolean finalFrameForChunk,
            int payloadLength,
            byte[] payload,
            Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern
    ) {
        this(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                payloadLength,
                payload,
                payloadPattern,
                true
        );
    }

    private BulkDataFrame(
            UUID transferId,
            int cordIndex,
            int entrySequence,
            long chunkSequence,
            long entryOffset,
            boolean finalFrameForChunk,
            int payloadLength,
            byte[] payload,
            Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern,
            boolean copyPayload
    ) {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (cordIndex < 0 || cordIndex >= CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("cordIndex must be between 0 " +
                    "and " + (CordPoolConfig.ABSOLUTE_MAX_CORDS - 1));
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (chunkSequence < 0) {
            throw new IllegalArgumentException("chunkSequence must be non-negative");
        }
        if (entryOffset < 0) {
            throw new IllegalArgumentException("entryOffset must be non-negative");
        }
        if (payloadLength <= 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
        payloadPattern = payloadPattern == null ? Optional.empty() : payloadPattern;
        if (payload == null && payloadPattern.isEmpty()) {
            throw new IllegalArgumentException("payload or payloadPattern is required");
        }
        if (payload != null && payload.length != payloadLength) {
            throw new IllegalArgumentException("payload length must match payloadLength");
        }
        if (payloadPattern.isPresent() && payloadPattern.orElseThrow().originalLength() != payloadLength) {
            throw new IllegalArgumentException("payload pattern length must match payloadLength");
        }
        this.transferId = transferId;
        this.cordIndex = cordIndex;
        this.entrySequence = entrySequence;
        this.chunkSequence = chunkSequence;
        this.entryOffset = entryOffset;
        this.finalFrameForChunk = finalFrameForChunk;
        this.payloadLength = payloadLength;
        this.payload = payload == null ? null : copyPayload ? Arrays.copyOf(payload, payload.length) : payload;
        this.payloadPattern = payloadPattern;
    }

    public UUID transferId() {
        return transferId;
    }

    public int cordIndex() {
        return cordIndex;
    }

    public int entrySequence() {
        return entrySequence;
    }

    public long chunkSequence() {
        return chunkSequence;
    }

    public long entryOffset() {
        return entryOffset;
    }

    public boolean finalFrameForChunk() {
        return finalFrameForChunk;
    }

    public int payloadLength() {
        return payloadLength;
    }

    public Optional<BulkPayloadPatternCodec.PayloadPattern> payloadPattern() {
        return payloadPattern;
    }

    public BulkDataFrameKey key() {
        return new BulkDataFrameKey(transferId, entrySequence, chunkSequence);
    }

    public byte[] payload() {
        if (payload != null) {
            return Arrays.copyOf(payload, payload.length);
        }
        return BulkPayloadPatternCodec.materializePayloadPattern(payloadPattern.orElseThrow());
    }

    public byte[] payloadView() {
        if (payload != null) {
            return payload;
        }
        return BulkPayloadPatternCodec.materializePayloadPattern(payloadPattern.orElseThrow());
    }

    byte[] payloadForEncoding() {
        if (payload != null) {
            return payload;
        }
        return BulkPayloadPatternCodec.materializePayloadPattern(payloadPattern.orElseThrow());
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        }
        if (!(value instanceof BulkDataFrame other)) {
            return false;
        }
        return cordIndex == other.cordIndex
                && entrySequence == other.entrySequence
                && chunkSequence == other.chunkSequence
                && entryOffset == other.entryOffset
                && finalFrameForChunk == other.finalFrameForChunk
                && payloadLength == other.payloadLength
                && transferId.equals(other.transferId)
                && Arrays.equals(payload(), other.payload());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                transferId,
                cordIndex,
                entrySequence,
                chunkSequence,
                entryOffset,
                finalFrameForChunk,
                payloadLength
        );
        result = 31 * result + Arrays.hashCode(payload());
        return result;
    }

    @Override
    public String toString() {
        return "BulkDataFrame["
                + "transferId=" + transferId
                + ", cordIndex=" + cordIndex
                + ", entrySequence=" + entrySequence
                + ", chunkSequence=" + chunkSequence
                + ", entryOffset=" + entryOffset
                + ", finalFrameForChunk=" + finalFrameForChunk
                + ", payloadLength=" + payloadLength
                + ", payloadPattern=" + payloadPattern
                + ']';
    }
}
