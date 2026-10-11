package com.fortrow.cxnfs.checksum;

import com.fortrow.cxnfs.protocol.TransferChecksum;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

public final class TransferChecksums {
    private static final int BUFFER_BYTES = 1024 * 1024;
    private static final byte[] ZERO_BLOCK = new byte[8192];
    private static final long CRC_MASK = 0xffff_ffffL;
    private static final long CRC32C_REVERSED_POLY = 0x82f63b78L;
    private static final Object ZERO_POWER_LOCK = new Object();
    private static final List<ZeroPower> ZERO_POWERS = new ArrayList<>();

    static {
        CRC32C checksum = new CRC32C();
        checksum.update(ZERO_BLOCK, 0, 1);
        ZERO_POWERS.add(new ZeroPower(1L, checksum.getValue()));
    }

    private TransferChecksums() {
    }

    public static TransferChecksum crc32c(CRC32C checksum) {
        if (checksum == null) {
            throw new IllegalArgumentException("checksum is required");
        }
        return TransferChecksum.crc32c(checksum.getValue());
    }

    public static TransferChecksum crc32c(Crc32cAccumulator checksum) {
        if (checksum == null) {
            throw new IllegalArgumentException("checksum is required");
        }
        return TransferChecksum.crc32c(checksum.value());
    }

    public static Crc32cAccumulator crc32cAccumulator() {
        return new Crc32cAccumulator();
    }

    public static TransferChecksum crc32c(Path path, long expectedBytes) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        CRC32C checksum = new CRC32C();
        byte[] buffer = new byte[BUFFER_BYTES];
        long total = 0L;
        try (InputStream input = Files.newInputStream(path)) {
            while (true) {
                int read = input.read(buffer);
                if (read < 0) {
                    break;
                }
                checksum.update(buffer, 0, read);
                total += read;
            }
        }
        if (expectedBytes >= 0 && total != expectedBytes) {
            throw new IOException("checksum byte count mismatch: expected " + expectedBytes + ", got " + total);
        }
        return crc32c(checksum);
    }

    public static long crc32cValue(byte[] data, int offset, int length) {
        if (data == null) {
            throw new IllegalArgumentException("data is required");
        }
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException("invalid checksum range");
        }
        CRC32C checksum = new CRC32C();
        checksum.update(data, offset, length);
        return checksum.getValue() & CRC_MASK;
    }

    public static void updateZeros(CRC32C checksum, int length) {
        if (checksum == null) {
            throw new IllegalArgumentException("checksum is required");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        int remaining = length;
        while (remaining > 0) {
            int count = Math.min(remaining, ZERO_BLOCK.length);
            checksum.update(ZERO_BLOCK, 0, count);
            remaining -= count;
        }
    }

    public static long crc32cOfZeros(long length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return 0L;
        }
        ensureZeroPowers(63 - Long.numberOfLeadingZeros(length));
        long crc = 0L;
        long remaining = length;
        int bit = 0;
        while (remaining != 0) {
            if ((remaining & 1L) != 0) {
                ZeroPower power = ZERO_POWERS.get(bit);
                crc = combine(crc, power.crc(), power.length());
            }
            remaining >>>= 1;
            bit++;
        }
        return crc;
    }

    public static long combine(long firstCrc, long secondCrc, long secondLengthBytes) {
        if (secondLengthBytes < 0) {
            throw new IllegalArgumentException("second length must be non-negative");
        }
        firstCrc &= CRC_MASK;
        secondCrc &= CRC_MASK;
        if (secondLengthBytes == 0) {
            return firstCrc;
        }

        long[] odd = new long[32];
        long[] even = new long[32];

        odd[0] = CRC32C_REVERSED_POLY;
        long row = 1L;
        for (int index = 1; index < odd.length; index++) {
            odd[index] = row;
            row <<= 1;
        }

        gf2MatrixSquare(even, odd);
        gf2MatrixSquare(odd, even);

        long length = secondLengthBytes;
        do {
            gf2MatrixSquare(even, odd);
            if ((length & 1L) != 0) {
                firstCrc = gf2MatrixTimes(even, firstCrc);
            }
            length >>>= 1;
            if (length == 0) {
                break;
            }
            gf2MatrixSquare(odd, even);
            if ((length & 1L) != 0) {
                firstCrc = gf2MatrixTimes(odd, firstCrc);
            }
            length >>>= 1;
        } while (length != 0);

        return (firstCrc ^ secondCrc) & CRC_MASK;
    }

    private static void ensureZeroPowers(int maxBit) {
        synchronized (ZERO_POWER_LOCK) {
            while (ZERO_POWERS.size() <= maxBit) {
                ZeroPower previous = ZERO_POWERS.getLast();
                ZERO_POWERS.add(new ZeroPower(
                        previous.length() << 1,
                        combine(previous.crc(), previous.crc(), previous.length())
                ));
            }
        }
    }

    private static long gf2MatrixTimes(long[] matrix, long vector) {
        long sum = 0L;
        int index = 0;
        long remaining = vector & CRC_MASK;
        while (remaining != 0) {
            if ((remaining & 1L) != 0) {
                sum ^= matrix[index];
            }
            remaining >>>= 1;
            index++;
        }
        return sum & CRC_MASK;
    }

    private static void gf2MatrixSquare(long[] square, long[] matrix) {
        for (int index = 0; index < square.length; index++) {
            square[index] = gf2MatrixTimes(matrix, matrix[index]);
        }
    }

    public static final class Crc32cAccumulator {
        private long value;
        private long bytes;

        private Crc32cAccumulator() {
        }

        public void update(byte[] data, int offset, int length) {
            if (data == null) {
                throw new IllegalArgumentException("data is required");
            }
            if (offset < 0 || length < 0 || offset + length > data.length) {
                throw new IndexOutOfBoundsException("invalid checksum update range");
            }
            if (length == 0) {
                return;
            }
            CRC32C checksum = new CRC32C();
            checksum.update(data, offset, length);
            value = combine(value, checksum.getValue(), length);
            bytes += length;
        }

        public void updateZeros(long length) {
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative");
            }
            if (length == 0) {
                return;
            }
            value = combine(value, crc32cOfZeros(length), length);
            bytes += length;
        }

        public void updateCrc32c(long crc32c, long length) {
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative");
            }
            if ((crc32c & ~CRC_MASK) != 0L) {
                throw new IllegalArgumentException("crc32c value must fit in 32 bits");
            }
            if (length == 0) {
                return;
            }
            value = combine(value, crc32c, length);
            bytes += length;
        }

        public long value() {
            return value & CRC_MASK;
        }

        public long bytes() {
            return bytes;
        }
    }

    private record ZeroPower(long length, long crc) {
    }
}
