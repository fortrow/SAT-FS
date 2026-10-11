package com.fortrow.cxnfs.chunk;

import com.fortrow.cxnfs.hash.Hashing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class GearHashChunker implements Chunker {
    private static final long[] GEAR_TABLE = buildGearTable();

    private final ChunkSettings settings;

    public GearHashChunker(ChunkSettings settings) {
        this.settings = settings;
    }

    public static GearHashChunker defaults() {
        return new GearHashChunker(ChunkSettings.xetLikeDefaults());
    }

    @Override
    public List<Chunk> split(byte[] data) {
        List<Chunk> chunks = new ArrayList<>();
        int chunkStart = 0;
        long rolling = 0;

        for (int i = 0; i < data.length; i++) {
            rolling = (rolling << 1) + GEAR_TABLE[Byte.toUnsignedInt(data[i])];
            int chunkSize = i - chunkStart + 1;

            if (shouldCut(chunkSize, rolling)) {
                chunks.add(chunk(data, chunkStart, i + 1));
                chunkStart = i + 1;
                rolling = 0;
            }
        }

        if (chunkStart < data.length || data.length == 0) {
            chunks.add(chunk(data, chunkStart, data.length));
        }

        return List.copyOf(chunks);
    }

    @Override
    public void split(InputStream input, ChunkHandler handler) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler is required");
        }

        ByteArrayOutputStream currentChunk = new ByteArrayOutputStream(settings.maximumSize());
        byte[] buffer = new byte[16 * 1024];
        long chunkStart = 0;
        long position = 0;
        long rolling = 0;

        int read;
        while ((read = input.read(buffer)) != -1) {
            for (int i = 0; i < read; i++) {
                byte value = buffer[i];
                currentChunk.write(value);
                rolling = (rolling << 1) + GEAR_TABLE[Byte.toUnsignedInt(value)];
                int chunkSize = currentChunk.size();

                if (shouldCut(chunkSize, rolling)) {
                    emit(currentChunk, chunkStart, handler);
                    chunkStart = position + 1;
                    rolling = 0;
                }
                position++;
            }
        }

        if (currentChunk.size() > 0 || position == 0) {
            emit(currentChunk, chunkStart, handler);
        }
    }

    private boolean shouldCut(int chunkSize, long rolling) {
        if (chunkSize < settings.minimumSize()) {
            return false;
        }
        return chunkSize >= settings.maximumSize() || (rolling & settings.boundaryMask()) == 0;
    }

    private static Chunk chunk(byte[] source, int startInclusive, int endExclusive) {
        byte[] data = Arrays.copyOfRange(source, startInclusive, endExclusive);
        return new Chunk(Hashing.chunk(data), startInclusive, data);
    }

    private static void emit(ByteArrayOutputStream currentChunk, long sourceOffset, ChunkHandler handler)
            throws IOException {
        byte[] data = currentChunk.toByteArray();
        handler.accept(new Chunk(Hashing.chunk(data), sourceOffset, data));
        currentChunk.reset();
    }

    private static long[] buildGearTable() {
        long[] table = new long[256];
        long state = 0x243f_6a88_85a3_08d3L;
        for (int i = 0; i < table.length; i++) {
            state = splitMix64(state);
            table[i] = state;
        }
        return table;
    }

    private static long splitMix64(long value) {
        long z = value + 0x9e37_79b9_7f4a_7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58_476d_1ce4_e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d0_49bb_1331_11ebL;
        return z ^ (z >>> 31);
    }
}
