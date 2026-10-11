package com.fortrow.cxnfs.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WriteSessionTest {
    @TempDir
    Path tempDir;

    @Test
    void createOrReplaceMaterializesSparseWrites() {
        WriteSession session = WriteSession.createOrReplace("/new.bin");
        session.write(4, "abc".getBytes(StandardCharsets.UTF_8));

        StagedWrite staged = session.stage();

        assertEquals("/new.bin", staged.path());
        assertEquals(WriteMode.CREATE_OR_REPLACE, staged.mode());
        assertEquals(7, staged.declaredSize());
        assertArrayEquals(new byte[]{0, 0, 0, 0, 'a', 'b', 'c'}, staged.materializedContent());
    }

    @Test
    void copyOnWritePreservesUnmodifiedBytes() {
        WriteSession session = WriteSession.copyOnWrite(
                "/existing.bin",
                "0123456789".getBytes(StandardCharsets.UTF_8)
        );
        session.write(3, "XYZ".getBytes(StandardCharsets.UTF_8));

        StagedWrite staged = session.stage();

        assertEquals(WriteMode.COPY_ON_WRITE, staged.mode());
        assertEquals("012XYZ6789", new String(staged.materializedContent(), StandardCharsets.UTF_8));
    }

    @Test
    void truncateLimitsMaterializedExtents() {
        WriteSession session = WriteSession.copyOnWrite(
                "/existing.bin",
                "0123456789".getBytes(StandardCharsets.UTF_8)
        );
        session.write(3, "ABCDE".getBytes(StandardCharsets.UTF_8));
        session.truncate(5);

        StagedWrite staged = session.stage();

        assertEquals(5, staged.declaredSize());
        assertEquals("012AB", new String(staged.materializedContent(), StandardCharsets.UTF_8));
    }

    @Test
    void stageToFileMaterializesSmallWritesAndDeletesOnClose() throws Exception {
        WriteSession session = WriteSession.copyOnWrite(
                "/existing.bin",
                "0123456789".getBytes(StandardCharsets.UTF_8)
        );
        session.write(3, "XYZ".getBytes(StandardCharsets.UTF_8));

        Path stagedPath;
        try (StagedWrite staged = session.stageToFile(tempDir)) {
            stagedPath = staged.materializedPath().orElseThrow();
            assertEquals(WriteMode.COPY_ON_WRITE, staged.mode());
            assertEquals("012XYZ6789", new String(staged.materializedContent(), StandardCharsets.UTF_8));
            assertTrue(Files.exists(stagedPath));
        }

        assertTrue(Files.notExists(stagedPath));
    }

    @Test
    void stageToFileSupportsSparseWritesPastInMemoryLimit() throws Exception {
        long offset = (long) Integer.MAX_VALUE + 1024;
        WriteSession session = WriteSession.createOrReplace("/large.bin");
        session.write(offset, "end".getBytes(StandardCharsets.UTF_8));

        try (StagedWrite staged = session.stageToFile(tempDir)) {
            assertEquals(offset + 3, staged.declaredSize());
            assertEquals(offset + 3, Files.size(staged.materializedPath().orElseThrow()));
            try (SeekableByteChannel channel = Files.newByteChannel(
                    staged.materializedPath().orElseThrow(),
                    StandardOpenOption.READ
            )) {
                ByteBuffer buffer = ByteBuffer.allocate(3);
                channel.position(offset);
                while (buffer.hasRemaining()) {
                    channel.read(buffer);
                }
                assertArrayEquals("end".getBytes(StandardCharsets.UTF_8), buffer.array());
            }
        }
    }
}
