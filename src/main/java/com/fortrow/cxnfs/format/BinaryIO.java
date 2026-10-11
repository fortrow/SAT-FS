package com.fortrow.cxnfs.format;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class BinaryIO {
    private BinaryIO() {
    }

    public static void writeMagic(DataOutputStream out, byte[] magic) throws IOException {
        out.write(magic);
    }

    public static void requireMagic(DataInputStream in, byte[] expected) throws IOException {
        byte[] actual = in.readNBytes(expected.length);
        if (!Arrays.equals(expected, actual)) {
            throw new BinaryFormatException("invalid binary format magic");
        }
    }

    public static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] readBytes(DataInputStream in, int maxLength) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxLength) {
            throw new BinaryFormatException("invalid byte array length: " + length);
        }
        return readFixedBytes(in, length);
    }

    public static byte[] readFixedBytes(DataInputStream in, int length) throws IOException {
        if (length < 0) {
            throw new BinaryFormatException("invalid fixed byte length: " + length);
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new BinaryFormatException("unexpected end of binary input");
        }
        return bytes;
    }

    public static void writeUtf8(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    public static String readUtf8(DataInputStream in, int maxLength) throws IOException {
        return new String(readBytes(in, maxLength), StandardCharsets.UTF_8);
    }

    public static void requireVersion(int actual, int expected) throws IOException {
        if (actual != expected) {
            throw new BinaryFormatException("unsupported binary format version: " + actual);
        }
    }
}
