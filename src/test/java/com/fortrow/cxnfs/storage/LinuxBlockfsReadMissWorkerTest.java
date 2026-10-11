package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.Hashing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LinuxBlockfsReadMissWorkerTest {
    @TempDir
    Path tempDir;

    @Test
    void drainOnceAccumulatesMissResolutionCounters() throws Exception {
        byte[] chunk = bytes("worker drain chunk");
        LinuxBlockfsReadMissWorker worker = worker(
                new QueueRunner(
                        new LinuxBlockfsControlCommand.CommandResult(
                                0,
                                "miss_event event_id=1 reason=read inode=77 offset=0 length="
                                        + chunk.length
                                        + " dropped_events=0\n",
                                ""
                        ),
                        registeredExtent(chunk.length),
                        new LinuxBlockfsControlCommand.CommandResult(0, "miss_event=none errno=11\n", "")
                ),
                chunk,
                tempDir.resolve("payload.img")
        );

        List<LinuxBlockfsReadMissResolution> resolutions = worker.drainOnce();

        LinuxBlockfsReadMissWorkerSnapshot snapshot = worker.snapshot();
        assertEquals(1, resolutions.size());
        assertEquals(1, snapshot.drainCycles());
        assertEquals(1, snapshot.eventsResolved());
        assertEquals(1, snapshot.plannedChunks());
        assertEquals(1, snapshot.fetchedChunks());
        assertEquals(1, snapshot.publishedExtents());
        assertEquals(chunk.length, snapshot.payloadBytesWritten());
        assertTrue(snapshot.lastDrainAt().isPresent());
        assertTrue(snapshot.lastError().isEmpty());
        assertFalse(snapshot.running());
    }

    @Test
    void startRunsBackgroundDrainLoopUntilStopped() throws Exception {
        byte[] chunk = bytes("background worker chunk");
        CountDownLatch registered = new CountDownLatch(1);
        LinuxBlockfsReadMissWorker worker = worker(
                new QueueRunner(registered,
                        new LinuxBlockfsControlCommand.CommandResult(
                                0,
                                "miss_event event_id=2 reason=readahead inode=77 offset=0 length="
                                        + chunk.length
                                        + " dropped_events=0\n",
                                ""
                        ),
                        registeredExtent(chunk.length)
                ),
                chunk,
                tempDir.resolve("payload.img")
        );

        worker.start();
        assertTrue(registered.await(2, TimeUnit.SECONDS));
        worker.stop();

        LinuxBlockfsReadMissWorkerSnapshot snapshot = worker.snapshot();
        assertFalse(snapshot.running());
        assertEquals(1, snapshot.eventsResolved());
        assertEquals(1, snapshot.publishedExtents());
        assertEquals(chunk.length, snapshot.payloadBytesWritten());
    }

    private static LinuxBlockfsReadMissWorker worker(
            LinuxBlockfsControlCommand.CommandRunner runner,
            byte[] chunk,
            Path payloadDevice
    ) {
        LinuxBlockfsManifestIndexSnapshot manifestIndex = new LinuxBlockfsManifestIndexSnapshot(
                7,
                List.of(new LinuxBlockfsManifestEntry(
                        77,
                        "datasets/train/shard-00000.mds",
                        chunk.length,
                        Hashing.file(List.of(Hashing.chunk(chunk)), List.of(chunk.length)),
                        List.of(new LinuxBlockfsManifestChunk(0, chunk.length, Hashing.chunk(chunk)))
                ))
        );
        LinuxBlockfsPayloadAllocationSnapshot allocations = new LinuxBlockfsPayloadAllocationSnapshot(
                7,
                List.of(new LinuxBlockfsPayloadAllocation(77, 0, 1, 0, chunk.length))
        );
        LinuxBlockfsReadMissService service = new LinuxBlockfsReadMissService(
                new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                new LinuxBlockfsReadMissPlanner(manifestIndex, allocations),
                new LinuxBlockfsPayloadCacheWriter(payloadDevice),
                (entry, manifestChunk) -> chunk
        );
        return new LinuxBlockfsReadMissWorker(service, 64, Duration.ofMillis(1), false);
    }

    private static LinuxBlockfsControlCommand.CommandResult registeredExtent(int length) {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_extent inode=77 file_offset=0 payload_block=1 payload_offset=0 payload_length="
                        + length
                        + "\n",
                ""
        );
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static final class QueueRunner implements LinuxBlockfsControlCommand.CommandRunner {
        private final List<LinuxBlockfsControlCommand.CommandResult> results;
        private final CountDownLatch registerLatch;
        private int index;

        QueueRunner(LinuxBlockfsControlCommand.CommandResult... results) {
            this(null, results);
        }

        QueueRunner(CountDownLatch registerLatch, LinuxBlockfsControlCommand.CommandResult... results) {
            this.results = new ArrayList<>(List.of(results));
            this.registerLatch = registerLatch;
        }

        @Override
        public synchronized LinuxBlockfsControlCommand.CommandResult run(List<String> command) {
            if (command.contains("register-extent") && registerLatch != null) {
                registerLatch.countDown();
            }
            if (index >= results.size()) {
                return new LinuxBlockfsControlCommand.CommandResult(0, "miss_event=none errno=11\n", "");
            }
            return results.get(index++);
        }
    }
}
