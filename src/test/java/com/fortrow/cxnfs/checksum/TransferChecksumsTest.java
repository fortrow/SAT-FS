package com.fortrow.cxnfs.checksum;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TransferChecksumsTest {
    @Test
    void combinesCrc32cSegments() {
        byte[] first = new byte[65_537];
        byte[] second = new byte[131_071];
        Random random = new Random(12345L);
        random.nextBytes(first);
        random.nextBytes(second);

        CRC32C expected = new CRC32C();
        expected.update(first, 0, first.length);
        expected.update(second, 0, second.length);

        CRC32C firstChecksum = new CRC32C();
        firstChecksum.update(first, 0, first.length);
        CRC32C secondChecksum = new CRC32C();
        secondChecksum.update(second, 0, second.length);

        assertEquals(
                expected.getValue(),
                TransferChecksums.combine(firstChecksum.getValue(), secondChecksum.getValue(), second.length)
        );
    }

    @Test
    void computesZeroCrcWithoutWalkingTheWholeRange() {
        byte[] zeros = new byte[2_500_000];
        CRC32C expected = new CRC32C();
        expected.update(zeros, 0, zeros.length);

        assertEquals(expected.getValue(), TransferChecksums.crc32cOfZeros(zeros.length));
    }

    @Test
    void accumulatorMatchesRegularCrc32cWithMixedDataAndZeroRanges() {
        byte[] prefix = "prefix".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] suffix = "suffix".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] zeros = new byte[3_000_000];

        CRC32C expected = new CRC32C();
        expected.update(prefix, 0, prefix.length);
        expected.update(zeros, 0, zeros.length);
        expected.update(suffix, 0, suffix.length);

        TransferChecksums.Crc32cAccumulator accumulator = TransferChecksums.crc32cAccumulator();
        accumulator.update(prefix, 0, prefix.length);
        accumulator.updateZeros(zeros.length);
        accumulator.update(suffix, 0, suffix.length);

        assertEquals(prefix.length + zeros.length + suffix.length, accumulator.bytes());
        assertEquals(expected.getValue(), accumulator.value());
    }

    @Test
    void accumulatorCanAppendLargeLogicalZeroRangeQuickly() {
        TransferChecksums.Crc32cAccumulator accumulator = TransferChecksums.crc32cAccumulator();

        accumulator.updateZeros(10L * 1024L * 1024L * 1024L);

        assertEquals(10L * 1024L * 1024L * 1024L, accumulator.bytes());
        assertEquals(
                TransferChecksums.crc32cOfZeros(10L * 1024L * 1024L * 1024L),
                accumulator.value()
        );
    }
}
