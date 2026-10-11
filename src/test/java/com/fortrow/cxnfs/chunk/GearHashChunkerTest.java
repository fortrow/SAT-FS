package com.fortrow.cxnfs.chunk;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GearHashChunkerTest {
    @Test
    void emptyInputProducesOneEmptyChunk() {
        List<Chunk> chunks = GearHashChunker.defaults().split(new byte[0]);

        assertEquals(1, chunks.size());
        assertEquals(0, chunks.getFirst().size());
        assertEquals(0, chunks.getFirst().sourceOffset());
    }

    @Test
    void chunkingIsDeterministic() {
        byte[] data = bytes(512 * 1024);
        GearHashChunker chunker = GearHashChunker.defaults();

        List<Chunk> first = chunker.split(data);
        List<Chunk> second = chunker.split(data);

        assertEquals(first.stream().map(Chunk::hash).toList(), second.stream().map(Chunk::hash).toList());
        assertEquals(first.stream().map(Chunk::size).toList(), second.stream().map(Chunk::size).toList());
    }

    @Test
    void streamingChunkingMatchesByteArrayChunking() throws Exception {
        byte[] data = bytes(1024 * 1024 + 333);
        GearHashChunker chunker = GearHashChunker.defaults();
        List<Chunk> expected = chunker.split(data);
        List<Chunk> actual = new ArrayList<>();

        chunker.split(new ByteArrayInputStream(data), actual::add);

        assertEquals(expected.stream().map(Chunk::hash).toList(), actual.stream().map(Chunk::hash).toList());
        assertEquals(expected.stream().map(Chunk::size).toList(), actual.stream().map(Chunk::size).toList());
        assertEquals(expected.stream().map(Chunk::sourceOffset).toList(), actual.stream().map(Chunk::sourceOffset).toList());
        assertArrayEquals(data, reassemble(actual));
    }

    @Test
    void chunksStayWithinConfiguredMaximumAndReassemble() {
        byte[] data = bytes(251);
        ChunkSettings settings = new ChunkSettings(8, 32, 40, -1L);
        List<Chunk> chunks = new GearHashChunker(settings).split(data);

        assertTrue(chunks.stream().allMatch(chunk -> chunk.size() <= settings.maximumSize()));
        assertArrayEquals(data, reassemble(chunks));
    }

    private static byte[] bytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        return data;
    }

    private static byte[] reassemble(List<Chunk> chunks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        chunks.forEach(chunk -> out.writeBytes(chunk.data()));
        return out.toByteArray();
    }
}
