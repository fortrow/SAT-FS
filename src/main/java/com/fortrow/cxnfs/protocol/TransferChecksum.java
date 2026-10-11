package com.fortrow.cxnfs.protocol;

import java.util.Locale;

public record TransferChecksum(String algorithm, long value) {
    public static final String CRC32C = "crc32c";

    public TransferChecksum {
        algorithm = algorithm == null ? "" : algorithm.trim().toLowerCase(Locale.ROOT);
        if (algorithm.isBlank()) {
            value = 0L;
        } else if (!algorithm.equals(CRC32C)) {
            throw new IllegalArgumentException("unsupported transfer checksum algorithm: " + algorithm);
        } else if ((value & ~0xffff_ffffL) != 0L) {
            throw new IllegalArgumentException("crc32c checksum must fit in 32 bits");
        }
    }

    public static TransferChecksum none() {
        return new TransferChecksum("", 0L);
    }

    public static TransferChecksum crc32c(long value) {
        return new TransferChecksum(CRC32C, value & 0xffff_ffffL);
    }

    public boolean present() {
        return !algorithm.isBlank();
    }

    public String display() {
        return present() ? algorithm + ":" + String.format(Locale.ROOT, "%08x", value) : "none";
    }
}
