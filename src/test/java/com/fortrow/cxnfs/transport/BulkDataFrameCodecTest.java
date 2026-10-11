package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BulkDataFrameCodecTest {
    @Test
    void encodesAndDecodesBulkDataFrameIdentityAndPayload() throws Exception {
        UUID transferId = UUID.randomUUID();
        byte[] payload = "chunk bytes".getBytes(StandardCharsets.UTF_8);
        BulkDataFrame frame = new BulkDataFrame(
                transferId,
                7,
                3,
                11,
                4096,
                true,
                payload
        );

        BulkDataFrame decoded = BulkDataFrameCodec.decode(BulkDataFrameCodec.encode(frame));

        assertEquals(transferId, decoded.transferId());
        assertEquals(7, decoded.cordIndex());
        assertEquals(3, decoded.entrySequence());
        assertEquals(11, decoded.chunkSequence());
        assertEquals(4096, decoded.entryOffset());
        assertEquals(true, decoded.finalFrameForChunk());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void encodesByteRunsAsRawBulkPayload() throws Exception {
        byte[] payload = new byte[4096];
        BulkDataFrame frame = new BulkDataFrame(UUID.randomUUID(), 0, 0, 0, 0, true, payload);

        byte[] encoded = BulkDataFrameCodec.encode(frame);
        BulkDataFrame decoded = BulkDataFrameCodec.decode(encoded);

        assertArrayEquals(payload, decoded.payload());
        assertTrue(encoded.length > payload.length);
    }

    @Test
    void decodesExplicitCompactRunPayload() throws Exception {
        byte[] payload = new byte[4096];
        BulkDataFrame frame = new BulkDataFrame(UUID.randomUUID(), 0, 0, 0, 0, true, payload);

        byte[] encoded = BulkDataFrameCodec.encodeWithPayload(
                frame,
                BulkPayloadPatternCodec.encodeRunPayload(payload.length, (byte) 0)
        );
        BulkDataFrame decoded = BulkDataFrameCodec.decode(encoded);

        assertArrayEquals(payload, decoded.payload());
        assertTrue(decoded.payloadPattern().orElseThrow().zeroRun());
        assertTrue(encoded.length < payload.length / 8);
    }

    @Test
    void encodesCompactRunFromPatternOnlyFrame() throws Exception {
        int length = 16 * 1024 * 1024;
        BulkPayloadPatternCodec.EncodedPayload encodedPayload =
                BulkPayloadPatternCodec.encodeRunPayload(length, (byte) 0);
        BulkPayloadPatternCodec.PayloadPattern pattern =
                BulkPayloadPatternCodec.payloadPattern(encodedPayload).orElseThrow();
        BulkDataFrame frame = new BulkDataFrame(UUID.randomUUID(), 0, 0, 0, 0, true, length, pattern);

        byte[] encoded = BulkDataFrameCodec.encodeWithPayload(frame, encodedPayload);
        BulkDataFrame decoded = BulkDataFrameCodec.decode(encoded);

        assertEquals(length, decoded.payloadLength());
        assertTrue(decoded.payloadPattern().orElseThrow().zeroRun());
        assertTrue(encoded.length < 128);
    }

    @Test
    void detectsLargeZeroRunWithoutSymbolScanCliff() {
        byte[] payload = new byte[BulkDataFrameCodec.MAX_PAYLOAD_BYTES];

        Optional<BulkPayloadPatternCodec.EncodedPayload> encoded = assertTimeoutPreemptively(
                Duration.ofSeconds(2),
                () -> BulkPayloadPatternCodec.maybeEncodePayload(payload)
        );

        assertTrue(encoded.isPresent());
        assertTrue(encoded.orElseThrow().encodedBytes().length < 64);
    }

    @Test
    void rejectsBadBulkDataFrameMagic() throws Exception {
        BulkDataFrame frame = new BulkDataFrame(UUID.randomUUID(), 0, 0, 0, 0, true, new byte[]{1});
        byte[] encoded = BulkDataFrameCodec.encode(frame);
        encoded[0] = 'X';

        assertThrows(BinaryFormatException.class, () -> BulkDataFrameCodec.decode(encoded));
    }

    @Test
    void rejectsInvalidCordIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BulkDataFrame(
                        UUID.randomUUID(),
                        CordPoolConfig.ABSOLUTE_MAX_CORDS,
                        0,
                        0,
                        0,
                        true,
                        new byte[]{1}
                )
        );
    }

}
