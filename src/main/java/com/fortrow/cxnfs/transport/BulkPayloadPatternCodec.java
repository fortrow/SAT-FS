package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class BulkPayloadPatternCodec {
    private static final int COMPRESSED_PAYLOAD_SENTINEL = -1;
    private static final int COMPRESSED_VERSION = 1;
    private static final int MIN_COMPRESSIBLE_PAYLOAD_BYTES = 128;
    private static final int MIN_RUN_LENGTH = 8;
    private static final int MIN_SYMBOL_LENGTH = 64;
    private static final int MAX_SYMBOL_LENGTH = 4096;
    private static final int MAX_SYMBOLS = 64;
    private static final int LARGE_PAYLOAD_SYMBOL_SCAN_STRIDE = 64;
    private static final int LARGE_PAYLOAD_THRESHOLD = 1024 * 1024;
    private static final long HASH_BASE = 257L;
    private static final String ENABLED_PROPERTY = "cxnfs.bulk.patternCompression";
    private static final String ENABLED_ENV = "CXNFS_BULK_PATTERN_COMPRESSION";

    private static final byte TOKEN_END = 0;
    private static final byte TOKEN_RAW = 1;
    private static final byte TOKEN_RUN = 2;
    private static final byte TOKEN_SYMBOL = 3;

    private BulkPayloadPatternCodec() {
    }

    public record EncodedPayload(int originalLength, byte[] encodedBytes) {
        public EncodedPayload {
            if (originalLength <= 0) {
                throw new IllegalArgumentException("originalLength must be positive");
            }
            if (encodedBytes == null || encodedBytes.length == 0) {
                throw new IllegalArgumentException("encodedBytes is required");
            }
            encodedBytes = Arrays.copyOf(encodedBytes, encodedBytes.length);
        }

        @Override
        public byte[] encodedBytes() {
            return Arrays.copyOf(encodedBytes, encodedBytes.length);
        }
    }

    public enum PayloadPatternKind {
        RUN,
        REPEATED_SYMBOL
    }

    public record PayloadPattern(
            PayloadPatternKind kind,
            int originalLength,
            byte value,
            byte[] symbol
    ) {
        public PayloadPattern {
            if (kind == null) {
                throw new IllegalArgumentException("kind is required");
            }
            if (originalLength <= 0 || originalLength > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("originalLength must be between 1 and "
                        + BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
            }
            symbol = symbol == null ? new byte[0] : Arrays.copyOf(symbol, symbol.length);
            if (kind == PayloadPatternKind.REPEATED_SYMBOL && symbol.length == 0) {
                throw new IllegalArgumentException("symbol is required for repeated-symbol payloads");
            }
        }

        public boolean zeroRun() {
            return kind == PayloadPatternKind.RUN && value == 0;
        }

        @Override
        public byte[] symbol() {
            return Arrays.copyOf(symbol, symbol.length);
        }
    }

    public record DecodedPayload(
            int originalLength,
            byte[] payload,
            Optional<PayloadPattern> pattern
    ) {
        public DecodedPayload {
            if (originalLength <= 0 || originalLength > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("originalLength must be between 1 and "
                        + BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
            }
            pattern = pattern == null ? Optional.empty() : pattern;
            payload = payload == null ? null : Arrays.copyOf(payload, payload.length);
            if (payload == null && pattern.isEmpty()) {
                throw new IllegalArgumentException("payload or pattern is required");
            }
            if (payload != null && payload.length != originalLength) {
                throw new IllegalArgumentException("payload length must match originalLength");
            }
            if (pattern.isPresent() && pattern.orElseThrow().originalLength() != originalLength) {
                throw new IllegalArgumentException("pattern length must match originalLength");
            }
        }

        public byte[] payloadBytes() {
            return payload == null ? materialize(pattern.orElseThrow()) : Arrays.copyOf(payload, payload.length);
        }

        @Override
        public byte[] payload() {
            return payload == null ? null : Arrays.copyOf(payload, payload.length);
        }

        byte[] payloadViewForTransport() {
            return payload;
        }
    }

    public static EncodedPayload encodeRunPayload(int originalLength, byte value) throws IOException {
        if (originalLength <= 0 || originalLength > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("originalLength must be between 1 and "
                    + BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(COMPRESSED_VERSION);
            out.writeInt(0);
            out.writeByte(TOKEN_RUN);
            out.writeByte(value);
            out.writeInt(originalLength);
            out.writeByte(TOKEN_END);
        }
        return new EncodedPayload(originalLength, bytes.toByteArray());
    }

    public static EncodedPayload encodeRepeatedSymbolPayload(int originalLength, byte[] symbol) throws IOException {
        if (originalLength <= 0 || originalLength > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("originalLength must be between 1 and "
                    + BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
        }
        if (symbol == null || symbol.length < MIN_SYMBOL_LENGTH || symbol.length > MAX_SYMBOL_LENGTH) {
            throw new IllegalArgumentException("symbol length must be between "
                    + MIN_SYMBOL_LENGTH + " and " + MAX_SYMBOL_LENGTH);
        }
        if (originalLength % symbol.length != 0) {
            throw new IllegalArgumentException("originalLength must be a multiple of symbol length");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(COMPRESSED_VERSION);
            out.writeInt(1);
            BinaryIO.writeBytes(out, symbol);
            int occurrences = originalLength / symbol.length;
            for (int index = 0; index < occurrences; index++) {
                out.writeByte(TOKEN_SYMBOL);
                out.writeInt(0);
            }
            out.writeByte(TOKEN_END);
        }
        return new EncodedPayload(originalLength, bytes.toByteArray());
    }

    public static Optional<EncodedPayload> maybeEncodePayload(byte[] payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        byte[] compressed = maybeCompress(payload);
        return compressed == null
                ? Optional.empty()
                : Optional.of(new EncodedPayload(payload.length, compressed));
    }

    public static Optional<PayloadPattern> payloadPattern(EncodedPayload payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        return simplePattern(payload.encodedBytes(), payload.originalLength());
    }

    public static byte[] materializePayloadPattern(PayloadPattern pattern) {
        return materialize(pattern);
    }

    static void writeEncodedPayload(DataOutputStream out, EncodedPayload payload) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("out is required");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        out.writeInt(COMPRESSED_PAYLOAD_SENTINEL);
        out.writeInt(payload.originalLength());
        out.writeInt(payload.encodedBytes().length);
        out.write(payload.encodedBytes());
    }

    static void writePayload(DataOutputStream out, byte[] payload) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("out is required");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        byte[] compressed = maybeCompress(payload);
        if (compressed == null) {
            BinaryIO.writeBytes(out, payload);
            return;
        }
        out.writeInt(COMPRESSED_PAYLOAD_SENTINEL);
        out.writeInt(payload.length);
        out.writeInt(compressed.length);
        out.write(compressed);
    }

    static byte[] readPayload(DataInputStream in, int maxLength) throws IOException {
        return readDecodedPayload(in, maxLength).payloadBytes();
    }

    static DecodedPayload readDecodedPayload(DataInputStream in, int maxLength) throws IOException {
        if (in == null) {
            throw new IllegalArgumentException("in is required");
        }
        int length = in.readInt();
        if (length >= 0) {
            if (length > maxLength) {
                throw new BinaryFormatException("invalid byte array length: " + length);
            }
            return new DecodedPayload(length, BinaryIO.readFixedBytes(in, length), Optional.empty());
        }
        if (length != COMPRESSED_PAYLOAD_SENTINEL) {
            throw new BinaryFormatException("invalid compressed payload sentinel: " + length);
        }
        int originalLength = in.readInt();
        if (originalLength < 0 || originalLength > maxLength) {
            throw new BinaryFormatException("invalid compressed payload original length: " + originalLength);
        }
        int encodedLength = in.readInt();
        if (encodedLength <= 0 || encodedLength > maxCompressedPayloadBytes(originalLength)) {
            throw new BinaryFormatException("invalid compressed payload length: " + encodedLength);
        }
        byte[] encoded = BinaryIO.readFixedBytes(in, encodedLength);
        Optional<PayloadPattern> pattern = simplePattern(encoded, originalLength);
        if (pattern.isPresent()) {
            return new DecodedPayload(originalLength, null, pattern);
        }
        return new DecodedPayload(originalLength, decompress(encoded, originalLength), Optional.empty());
    }

    private static byte[] maybeCompress(byte[] payload) throws IOException {
        if (!compressionEnabled() || payload.length < MIN_COMPRESSIBLE_PAYLOAD_BYTES) {
            return null;
        }
        byte[] run = wholePayloadRun(payload);
        if (run != null) {
            return run;
        }
        CompressionPlan plan = CompressionPlan.build(payload);
        if (plan.spansByOffset().isEmpty()) {
            return null;
        }
        byte[] compressed = encodeCompressed(payload, plan);
        int legacyBlockBytes = Integer.BYTES + payload.length;
        int compressedBlockBytes = Integer.BYTES + Integer.BYTES + Integer.BYTES + compressed.length;
        return compressedBlockBytes < legacyBlockBytes ? compressed : null;
    }

    private static byte[] wholePayloadRun(byte[] payload) throws IOException {
        byte value = payload[0];
        for (int index = 1; index < payload.length; index++) {
            if (payload[index] != value) {
                return null;
            }
        }
        return encodeRunPayload(payload.length, value).encodedBytes();
    }

    private static boolean compressionEnabled() {
        String property = System.getProperty(ENABLED_PROPERTY);
        if (property != null) {
            return parseEnabled(property);
        }
        String env = System.getenv(ENABLED_ENV);
        return env == null || parseEnabled(env);
    }

    private static boolean parseEnabled(String value) {
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        return !(normalized.equals("0")
                || normalized.equals("false")
                || normalized.equals("off")
                || normalized.equals("no"));
    }

    private static int maxCompressedPayloadBytes(int originalLength) {
        return Integer.BYTES + originalLength;
    }

    private static byte[] encodeCompressed(byte[] payload, CompressionPlan plan) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(COMPRESSED_VERSION);
            out.writeInt(plan.symbols().size());
            for (byte[] symbol : plan.symbols()) {
                BinaryIO.writeBytes(out, symbol);
            }

            int cursor = 0;
            while (cursor < payload.length) {
                SelectedSpan span = plan.spansByOffset().get(cursor);
                if (span == null) {
                    int rawStart = cursor;
                    cursor++;
                    while (cursor < payload.length && plan.spansByOffset().get(cursor) == null) {
                        cursor++;
                    }
                    out.writeByte(TOKEN_RAW);
                    out.writeInt(cursor - rawStart);
                    out.write(payload, rawStart, cursor - rawStart);
                    continue;
                }
                if (span.kind() == PatternKind.RUN) {
                    out.writeByte(TOKEN_RUN);
                    out.writeByte(span.value());
                    out.writeInt(span.length());
                } else {
                    out.writeByte(TOKEN_SYMBOL);
                    out.writeInt(span.symbolIndex());
                }
                cursor += span.length();
            }
            out.writeByte(TOKEN_END);
        }
        return bytes.toByteArray();
    }

    private static byte[] decompress(byte[] encoded, int originalLength) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(encoded);
        ByteArrayOutputStream output = new ByteArrayOutputStream(originalLength);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireVersion(in.readInt(), COMPRESSED_VERSION);
            int symbolCount = in.readInt();
            if (symbolCount < 0 || symbolCount > MAX_SYMBOLS) {
                throw new BinaryFormatException("invalid compressed payload symbol count: " + symbolCount);
            }
            List<byte[]> symbols = new ArrayList<>(symbolCount);
            for (int index = 0; index < symbolCount; index++) {
                byte[] symbol = BinaryIO.readBytes(in, MAX_SYMBOL_LENGTH);
                if (symbol.length == 0) {
                    throw new BinaryFormatException("compressed payload symbol must not be empty");
                }
                symbols.add(symbol);
            }

            boolean ended = false;
            while (!ended) {
                int token = in.read();
                if (token < 0) {
                    throw new BinaryFormatException("compressed payload ended before END token");
                }
                switch ((byte) token) {
                    case TOKEN_END -> ended = true;
                    case TOKEN_RAW -> {
                        int length = in.readInt();
                        requirePositiveLength(length, originalLength, output.size());
                        byte[] raw = BinaryIO.readFixedBytes(in, length);
                        output.write(raw);
                    }
                    case TOKEN_RUN -> {
                        int value = in.readUnsignedByte();
                        int length = in.readInt();
                        requirePositiveLength(length, originalLength, output.size());
                        writeRun(output, (byte) value, length);
                    }
                    case TOKEN_SYMBOL -> {
                        int symbolIndex = in.readInt();
                        if (symbolIndex < 0 || symbolIndex >= symbols.size()) {
                            throw new BinaryFormatException("invalid compressed payload symbol index: " + symbolIndex);
                        }
                        byte[] symbol = symbols.get(symbolIndex);
                        requirePositiveLength(symbol.length, originalLength, output.size());
                        output.write(symbol);
                    }
                    default -> throw new BinaryFormatException("unknown compressed payload token: " + token);
                }
            }
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after compressed payload");
            }
            if (output.size() != originalLength) {
                throw new BinaryFormatException("compressed payload expanded to "
                        + output.size() + " bytes, expected " + originalLength);
            }
            return output.toByteArray();
        }
    }

    private static Optional<PayloadPattern> simplePattern(byte[] encoded, int originalLength) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(encoded);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireVersion(in.readInt(), COMPRESSED_VERSION);
            int symbolCount = in.readInt();
            if (symbolCount < 0 || symbolCount > MAX_SYMBOLS) {
                throw new BinaryFormatException("invalid compressed payload symbol count: " + symbolCount);
            }
            List<byte[]> symbols = new ArrayList<>(symbolCount);
            for (int index = 0; index < symbolCount; index++) {
                byte[] symbol = BinaryIO.readBytes(in, MAX_SYMBOL_LENGTH);
                if (symbol.length == 0) {
                    throw new BinaryFormatException("compressed payload symbol must not be empty");
                }
                symbols.add(symbol);
            }
            int token = in.read();
            if (token < 0) {
                throw new BinaryFormatException("compressed payload ended before token");
            }
            Optional<PayloadPattern> pattern;
            switch ((byte) token) {
                case TOKEN_RUN -> {
                    int value = in.readUnsignedByte();
                    int length = in.readInt();
                    pattern = length == originalLength
                            ? Optional.of(new PayloadPattern(
                                    PayloadPatternKind.RUN,
                                    originalLength,
                                    (byte) value,
                                    new byte[0]
                            ))
                            : Optional.empty();
                }
                case TOKEN_SYMBOL -> {
                    int symbolIndex = in.readInt();
                    if (symbolIndex < 0 || symbolIndex >= symbols.size()) {
                        throw new BinaryFormatException("invalid compressed payload symbol index: " + symbolIndex);
                    }
                    byte[] symbol = symbols.get(symbolIndex);
                    int total = symbol.length;
                    boolean sameSymbol = true;
                    while (total < originalLength) {
                        int next = in.read();
                        if (next != TOKEN_SYMBOL) {
                            sameSymbol = false;
                            break;
                        }
                        int nextSymbolIndex = in.readInt();
                        if (nextSymbolIndex != symbolIndex || total + symbol.length > originalLength) {
                            sameSymbol = false;
                            break;
                        }
                        total += symbol.length;
                    }
                    pattern = sameSymbol && total == originalLength
                            ? Optional.of(new PayloadPattern(
                                    PayloadPatternKind.REPEATED_SYMBOL,
                                    originalLength,
                                    (byte) 0,
                                    symbol
                            ))
                            : Optional.empty();
                }
                default -> pattern = Optional.empty();
            }
            if (pattern.isEmpty()) {
                return Optional.empty();
            }
            int end = in.read();
            if (end != TOKEN_END || input.available() != 0) {
                return Optional.empty();
            }
            return pattern;
        }
    }

    private static byte[] materialize(PayloadPattern pattern) {
        byte[] bytes = new byte[pattern.originalLength()];
        if (pattern.kind() == PayloadPatternKind.RUN) {
            Arrays.fill(bytes, pattern.value());
            return bytes;
        }
        byte[] symbol = pattern.symbol();
        for (int offset = 0; offset < bytes.length; offset += symbol.length) {
            System.arraycopy(symbol, 0, bytes, offset, Math.min(symbol.length, bytes.length - offset));
        }
        return bytes;
    }

    private static void requirePositiveLength(int length, int originalLength, int currentOutputSize) throws IOException {
        if (length <= 0 || currentOutputSize + length > originalLength) {
            throw new BinaryFormatException("invalid compressed payload token length: " + length);
        }
    }

    private static void writeRun(ByteArrayOutputStream output, byte value, int length) {
        byte[] buffer = new byte[Math.min(length, 8192)];
        Arrays.fill(buffer, value);
        int remaining = length;
        while (remaining > 0) {
            int count = Math.min(remaining, buffer.length);
            output.write(buffer, 0, count);
            remaining -= count;
        }
    }

    private enum PatternKind {
        RUN,
        SYMBOL
    }

    private record PatternScore(
            int matchedByteCount,
            int uniqueByteCount,
            int redundantByteCount,
            double redundancyRatio,
            int occurrenceCount
    ) {
        private static PatternScore forRun(int length) {
            return of(length, 1, length);
        }

        private static PatternScore forRepeatedSymbol(int symbolLength, int occurrenceCount) {
            return of(symbolLength * occurrenceCount, symbolLength, occurrenceCount);
        }

        private static PatternScore of(int matchedByteCount, int uniqueByteCount, int occurrenceCount) {
            int redundantByteCount = matchedByteCount - uniqueByteCount;
            double redundancyRatio = matchedByteCount == 0 ? 0.0 : (double) redundantByteCount / matchedByteCount;
            return new PatternScore(
                    matchedByteCount,
                    uniqueByteCount,
                    redundantByteCount,
                    redundancyRatio,
                    occurrenceCount
            );
        }
    }

    private record Candidate(
            PatternKind kind,
            byte[] symbol,
            int value,
            int length,
            List<Integer> offsets,
            PatternScore score,
            int estimatedBenefit
    ) {
        private int firstOffset() {
            return offsets.getFirst();
        }
    }

    private record SelectedSpan(
            PatternKind kind,
            int length,
            int value,
            int symbolIndex
    ) {
        private static SelectedSpan run(int length, int value) {
            return new SelectedSpan(PatternKind.RUN, length, value, -1);
        }

        private static SelectedSpan symbol(int length, int symbolIndex) {
            return new SelectedSpan(PatternKind.SYMBOL, length, -1, symbolIndex);
        }
    }

    private record CompressionPlan(Map<Integer, SelectedSpan> spansByOffset, List<byte[]> symbols) {
        private static CompressionPlan build(byte[] payload) {
            List<Candidate> candidates = new ArrayList<>();
            candidates.addAll(findRunCandidates(payload));
            candidates.addAll(findSymbolCandidates(payload));
            if (candidates.isEmpty()) {
                return new CompressionPlan(Map.of(), List.of());
            }

            candidates.sort(Comparator
                    .comparingInt(Candidate::estimatedBenefit).reversed()
                    .thenComparing(Comparator.comparingInt((Candidate candidate) -> candidate.score().redundantByteCount()).reversed())
                    .thenComparing(Comparator.comparingInt(Candidate::length).reversed())
                    .thenComparingInt(Candidate::firstOffset));

            boolean[] covered = new boolean[payload.length];
            Map<Integer, SelectedSpan> spansByOffset = new HashMap<>();
            List<byte[]> symbols = new ArrayList<>();
            for (Candidate candidate : candidates) {
                if (candidate.estimatedBenefit() <= 0) {
                    continue;
                }
                if (candidate.kind() == PatternKind.SYMBOL && symbols.size() >= MAX_SYMBOLS) {
                    continue;
                }
                if (overlapsCovered(candidate, covered)) {
                    continue;
                }
                int symbolIndex = -1;
                if (candidate.kind() == PatternKind.SYMBOL) {
                    symbolIndex = symbols.size();
                    symbols.add(candidate.symbol());
                }
                for (int offset : candidate.offsets()) {
                    markCovered(covered, offset, candidate.length());
                    spansByOffset.put(offset, candidate.kind() == PatternKind.RUN
                            ? SelectedSpan.run(candidate.length(), candidate.value())
                            : SelectedSpan.symbol(candidate.length(), symbolIndex));
                }
            }
            return new CompressionPlan(Map.copyOf(spansByOffset), List.copyOf(symbols));
        }

        private static boolean overlapsCovered(Candidate candidate, boolean[] covered) {
            for (int offset : candidate.offsets()) {
                if (offset < 0 || offset + candidate.length() > covered.length) {
                    return true;
                }
                for (int index = offset; index < offset + candidate.length(); index++) {
                    if (covered[index]) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static void markCovered(boolean[] covered, int offset, int length) {
            Arrays.fill(covered, offset, offset + length, true);
        }

        private static List<Candidate> findRunCandidates(byte[] payload) {
            List<Candidate> candidates = new ArrayList<>();
            int start = 0;
            for (int index = 1; index <= payload.length; index++) {
                if (index < payload.length && payload[index] == payload[start]) {
                    continue;
                }
                int length = index - start;
                if (length >= MIN_RUN_LENGTH) {
                    PatternScore score = PatternScore.forRun(length);
                    int estimatedBenefit = length - runCommandBytes();
                    candidates.add(new Candidate(
                            PatternKind.RUN,
                            null,
                            payload[start] & 0xff,
                            length,
                            List.of(start),
                            score,
                            estimatedBenefit
                    ));
                }
                start = index;
            }
            return candidates;
        }

        private static List<Candidate> findSymbolCandidates(byte[] payload) {
            List<Candidate> candidates = new ArrayList<>();
            for (int length : symbolLengths(payload.length)) {
                candidates.addAll(findSymbolCandidates(payload, length));
            }
            return candidates;
        }

        private static List<Candidate> findSymbolCandidates(byte[] payload, int symbolLength) {
            int stride = payload.length > LARGE_PAYLOAD_THRESHOLD ? LARGE_PAYLOAD_SYMBOL_SCAN_STRIDE : 1;
            Map<Long, IntList> offsetsByHash = hashOffsets(payload, symbolLength, stride);
            List<Candidate> candidates = new ArrayList<>();
            for (IntList offsets : offsetsByHash.values()) {
                if (offsets.size() < 2) {
                    continue;
                }
                Map<ByteArrayKey, IntList> exact = new HashMap<>();
                for (int offset : offsets.values()) {
                    if (isUniform(payload, offset, symbolLength)) {
                        continue;
                    }
                    ByteArrayKey key = ByteArrayKey.copyOf(payload, offset, symbolLength);
                    exact.computeIfAbsent(key, ignored -> new IntList()).add(offset);
                }
                for (Map.Entry<ByteArrayKey, IntList> entry : exact.entrySet()) {
                    List<Integer> nonOverlappingOffsets = nonOverlappingOffsets(entry.getValue(), symbolLength);
                    if (nonOverlappingOffsets.size() < 2) {
                        continue;
                    }
                    PatternScore score = PatternScore.forRepeatedSymbol(symbolLength, nonOverlappingOffsets.size());
                    int matchedBytes = symbolLength * nonOverlappingOffsets.size();
                    int compressedBytes = symbolTableEntryBytes(symbolLength)
                            + symbolCommandBytes() * nonOverlappingOffsets.size();
                    candidates.add(new Candidate(
                            PatternKind.SYMBOL,
                            entry.getKey().bytes(),
                            -1,
                            symbolLength,
                            nonOverlappingOffsets,
                            score,
                            matchedBytes - compressedBytes
                    ));
                }
            }
            return candidates;
        }

        private static int[] symbolLengths(int payloadLength) {
            int largest = Math.min(MAX_SYMBOL_LENGTH, Math.max(0, payloadLength - 1));
            int[] lengths = payloadLength > LARGE_PAYLOAD_THRESHOLD
                    ? new int[]{4096, 1024, 256}
                    : new int[]{4096, 2048, 1024, 512, 256, 128, 64};
            return Arrays.stream(lengths)
                    .filter(length -> length >= MIN_SYMBOL_LENGTH && length <= largest)
                    .toArray();
        }

        private static Map<Long, IntList> hashOffsets(byte[] payload, int symbolLength, int stride) {
            Map<Long, IntList> offsetsByHash = new HashMap<>();
            if (payload.length < symbolLength) {
                return offsetsByHash;
            }
            long power = 1L;
            for (int index = 1; index < symbolLength; index++) {
                power *= HASH_BASE;
            }

            long hash = 0L;
            for (int index = 0; index < symbolLength; index++) {
                hash = hash * HASH_BASE + unsigned(payload[index]);
            }

            for (int offset = 0; offset <= payload.length - symbolLength; offset++) {
                if (offset % stride == 0) {
                    offsetsByHash.computeIfAbsent(hash, ignored -> new IntList()).add(offset);
                }
                if (offset < payload.length - symbolLength) {
                    hash = (hash - unsigned(payload[offset]) * power) * HASH_BASE
                            + unsigned(payload[offset + symbolLength]);
                }
            }
            return offsetsByHash;
        }

        private static int unsigned(byte value) {
            return (value & 0xff) + 1;
        }

        private static boolean isUniform(byte[] payload, int offset, int length) {
            byte first = payload[offset];
            for (int index = 1; index < length; index++) {
                if (payload[offset + index] != first) {
                    return false;
                }
            }
            return true;
        }

        private static List<Integer> nonOverlappingOffsets(IntList offsets, int symbolLength) {
            int[] sorted = offsets.values();
            Arrays.sort(sorted);
            List<Integer> result = new ArrayList<>();
            int nextAllowedOffset = -1;
            for (int offset : sorted) {
                if (offset >= nextAllowedOffset) {
                    result.add(offset);
                    nextAllowedOffset = offset + symbolLength;
                }
            }
            return List.copyOf(result);
        }

        private static int runCommandBytes() {
            return Byte.BYTES + Byte.BYTES + Integer.BYTES;
        }

        private static int symbolCommandBytes() {
            return Byte.BYTES + Integer.BYTES;
        }

        private static int symbolTableEntryBytes(int symbolLength) {
            return Integer.BYTES + symbolLength;
        }
    }

    private static final class IntList {
        private int[] values = new int[4];
        private int size;

        private void add(int value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        private int size() {
            return size;
        }

        private int[] values() {
            return Arrays.copyOf(values, size);
        }
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hashCode;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hashCode = Arrays.hashCode(bytes);
        }

        private static ByteArrayKey copyOf(byte[] payload, int offset, int length) {
            return new ByteArrayKey(Arrays.copyOfRange(payload, offset, offset + length));
        }

        private byte[] bytes() {
            return Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public boolean equals(Object value) {
            return value instanceof ByteArrayKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
