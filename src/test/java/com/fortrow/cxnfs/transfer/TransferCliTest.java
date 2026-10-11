package com.fortrow.cxnfs.transfer;

import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.transport.BulkPayloadPatternCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TransferCliTest {
    @Test
    void slidingWindowEncoderDropsPayloadBytesForZeroRuns() throws Exception {
        byte[] window = new byte[32 * 1024 * 1024];

        TransferCli.SlidingPayloadSummary summary = TransferCli.analyzeSlidingWindowPayload(
                window,
                16 * 1024 * 1024
        );

        assertEquals(16 * 1024 * 1024, summary.payloadLength());
        assertEquals(0, summary.queuedPayloadBytes());
        assertTrue(summary.encoded());
        assertEquals(
                BulkPayloadPatternCodec.PayloadPatternKind.RUN,
                summary.patternKind().orElseThrow()
        );
        assertEquals(Hashing.chunkRun((byte) 0, 16 * 1024 * 1024), summary.chunkHash());
    }

    @Test
    void slidingWindowEncoderHandlesSecondFrameInWindow() throws Exception {
        byte[] window = new byte[32 * 1024 * 1024];
        Arrays.fill(window, 16 * 1024 * 1024, 32 * 1024 * 1024, (byte) 7);

        TransferCli.SlidingPayloadSummary summary = TransferCli.analyzeSlidingWindowPayload(
                window,
                16 * 1024 * 1024,
                16 * 1024 * 1024
        );

        assertEquals(16 * 1024 * 1024, summary.payloadLength());
        assertEquals(0, summary.queuedPayloadBytes());
        assertTrue(summary.encoded());
        assertEquals(Hashing.chunkRun((byte) 7, 16 * 1024 * 1024), summary.chunkHash());
    }

    @Test
    void slidingWindowEncoderDropsPayloadBytesForRepeatedSymbols() throws Exception {
        byte[] symbol = new byte[4096];
        byte[] seed = "abcd-efgh-ijkl-mnop".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int index = 0; index < symbol.length; index++) {
            symbol[index] = seed[index % seed.length];
        }
        byte[] window = new byte[32 * 1024 * 1024];
        for (int offset = 0; offset < window.length; offset += symbol.length) {
            System.arraycopy(symbol, 0, window, offset, Math.min(symbol.length, window.length - offset));
        }

        TransferCli.SlidingPayloadSummary summary = TransferCli.analyzeSlidingWindowPayload(
                window,
                16 * 1024 * 1024
        );

        assertEquals(16 * 1024 * 1024, summary.payloadLength());
        assertEquals(0, summary.queuedPayloadBytes());
        assertTrue(summary.encoded());
        assertEquals(
                BulkPayloadPatternCodec.PayloadPatternKind.REPEATED_SYMBOL,
                summary.patternKind().orElseThrow()
        );
        assertEquals(Hashing.chunkRepeatedSymbol(symbol, 16 * 1024 * 1024), summary.chunkHash());
    }

    @Test
    void slidingWindowEncoderKeepsPayloadBytesForRawData() throws Exception {
        byte[] window = new byte[32 * 1024 * 1024];
        new java.util.Random(1234L).nextBytes(window);

        TransferCli.SlidingPayloadSummary summary = TransferCli.analyzeSlidingWindowPayload(
                window,
                16 * 1024 * 1024
        );

        assertEquals(16 * 1024 * 1024, summary.payloadLength());
        assertEquals(16 * 1024 * 1024, summary.queuedPayloadBytes());
        assertFalse(summary.encoded());
        assertTrue(summary.patternKind().isEmpty());
        assertEquals(Hashing.chunk(Arrays.copyOf(window, 16 * 1024 * 1024)), summary.chunkHash());
    }
}
