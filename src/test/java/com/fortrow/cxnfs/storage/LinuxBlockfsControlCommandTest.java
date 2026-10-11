package com.fortrow.cxnfs.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class LinuxBlockfsControlCommandTest {
    @Test
    void statsParsesStableControlToolOutput() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "abi_version=1 registered_extents=2 invalidations=3 "
                        + "read_folio_calls=4 readahead_calls=5 verified_page_fills=6 "
                        + "miss_events_queued=7 miss_events_drained=8 "
                        + "miss_events_pending=9 miss_events_dropped=10 "
                        + "miss_wait_successes=11 miss_wait_timeouts=12 "
                        + "miss_wait_interruptions=13 extent_generation=14 "
                        + "hash_verifications=15 hash_mismatches=16 "
                        + "hash_bytes_verified=17 readahead_miss_windows=18 "
                        + "readahead_miss_bytes=19\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("/usr/local/bin/cxnfs-blockfs-ctl"),
                Path.of("/dev/cxnfs_blockfs_ctl"),
                runner
        );

        LinuxBlockfsStats stats = client.stats();

        assertEquals(1, stats.abiVersion());
        assertEquals(2, stats.registeredExtents());
        assertEquals(3, stats.invalidations());
        assertEquals(4, stats.readFolioCalls());
        assertEquals(5, stats.readaheadCalls());
        assertEquals(6, stats.verifiedPageFills());
        assertEquals(7, stats.missEventsQueued());
        assertEquals(8, stats.missEventsDrained());
        assertEquals(9, stats.missEventsPending());
        assertEquals(10, stats.missEventsDropped());
        assertEquals(11, stats.missWaitSuccesses());
        assertEquals(12, stats.missWaitTimeouts());
        assertEquals(13, stats.missWaitInterruptions());
        assertEquals(14, stats.extentGeneration());
        assertEquals(15, stats.hashVerifications());
        assertEquals(16, stats.hashMismatches());
        assertEquals(17, stats.hashBytesVerified());
        assertEquals(18, stats.readaheadMissWindows());
        assertEquals(19, stats.readaheadMissBytes());
        assertEquals(List.of(
                "/usr/local/bin/cxnfs-blockfs-ctl",
                "--device",
                "/dev/cxnfs_blockfs_ctl",
                "stats"
        ), runner.commands.getFirst());
    }

    @Test
    void statsDefaultsExtendedFieldsForOlderControlToolOutput() {
        LinuxBlockfsStats stats = LinuxBlockfsStats.parse(
                "abi_version=1 registered_extents=2 invalidations=3 "
                        + "read_folio_calls=4 readahead_calls=5 verified_page_fills=6\n"
        );

        assertEquals(0, stats.missEventsQueued());
        assertEquals(0, stats.missEventsDrained());
        assertEquals(0, stats.missEventsPending());
        assertEquals(0, stats.missEventsDropped());
        assertEquals(0, stats.missWaitSuccesses());
        assertEquals(0, stats.missWaitTimeouts());
        assertEquals(0, stats.missWaitInterruptions());
        assertEquals(0, stats.extentGeneration());
        assertEquals(0, stats.hashVerifications());
        assertEquals(0, stats.hashMismatches());
        assertEquals(0, stats.hashBytesVerified());
        assertEquals(0, stats.readaheadMissWindows());
        assertEquals(0, stats.readaheadMissBytes());
    }

    @Test
    void registerExtentBuildsControlToolCommandAndDetectsScaffoldUnsupported() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "unsupported=register-extent errno=95\n",
                ""
        ));
        byte[] hash = new byte[32];
        hash[0] = 0x42;
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.registerExtent(
                new LinuxBlockfsExtent(11, 128, 5, 16, 4096, hash),
                true
        );

        assertFalse(result.accepted());
        assertTrue(result.unsupported());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "register-extent",
                "--inode",
                "11",
                "--file-offset",
                "128",
                "--payload-block",
                "5",
                "--payload-offset",
                "16",
                "--payload-length",
                "4096",
                "--hash",
                HexFormat.of().formatHex(hash),
                "--allow-unsupported"
        ), runner.commands.getFirst());
    }

    @Test
    void registerExtentAcceptsSupportedKernelOutput() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_extent inode=11 file_offset=128 payload_block=5 payload_offset=16 payload_length=4096\n",
                ""
        ));
        byte[] hash = new byte[32];
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.registerExtent(
                new LinuxBlockfsExtent(11, 128, 5, 16, 4096, hash),
                false
        );

        assertTrue(result.accepted());
        assertFalse(result.unsupported());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "register-extent",
                "--inode",
                "11",
                "--file-offset",
                "128",
                "--payload-block",
                "5",
                "--payload-offset",
                "16",
                "--payload-length",
                "4096",
                "--hash",
                HexFormat.of().formatHex(hash)
        ), runner.commands.getFirst());
    }

    @Test
    void registerExtentCanTargetMountedMetadataRoot() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_extent metadata_root=/var/lib/cxnfs/disk-a inode=11 "
                        + "file_offset=128 payload_block=5 payload_offset=16 payload_length=4096\n",
                ""
        ));
        byte[] hash = new byte[32];
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.registerExtent(
                LinuxBlockfsMountSelector.metadataRoot("/var/lib/cxnfs/disk-a"),
                new LinuxBlockfsExtent(11, 128, 5, 16, 4096, hash),
                false
        );

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "register-extent",
                "--metadata-root",
                "/var/lib/cxnfs/disk-a",
                "--inode",
                "11",
                "--file-offset",
                "128",
                "--payload-block",
                "5",
                "--payload-offset",
                "16",
                "--payload-length",
                "4096",
                "--hash",
                HexFormat.of().formatHex(hash)
        ), runner.commands.getFirst());
    }

    @Test
    void registerFileBuildsControlToolCommand() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_file inode=22 path=sample.bin size=4096 mode=444\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.registerFile(
                LinuxBlockfsFile.readOnly(22, "datasets/train/sample.bin", 4096)
        );

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "register-file",
                "--inode",
                "22",
                "--path",
                "datasets/train/sample.bin",
                "--size",
                "4096",
                "--mode",
                "0444"
        ), runner.commands.getFirst());
    }

    @Test
    void clearNamespaceBuildsControlToolCommand() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "cleared_namespace=true\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.clearNamespace();

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "clear-namespace"
        ), runner.commands.getFirst());
    }

    @Test
    void clearNamespaceCanTargetMountedMetadataRoot() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "cleared_namespace=true metadata_root=/var/lib/cxnfs/disk-a scope=mount\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.clearNamespace(
                LinuxBlockfsMountSelector.metadataRoot("/var/lib/cxnfs/disk-a")
        );

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "clear-namespace",
                "--metadata-root",
                "/var/lib/cxnfs/disk-a"
        ), runner.commands.getFirst());
    }

    @Test
    void nextMissEventParsesKernelReadMiss() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "miss_event event_id=9 reason=readahead inode=77 offset=4096 length=8192 "
                        + "dropped_events=1 mount_id=3 metadata_root=/var/lib/cx/cxbfs "
                        + "max_cords=64 readahead_pages=128 miss_wait_ms=3000 "
                        + "verify_hashes=true mount_flags=1\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsMissEvent event = client.nextMissEvent(false).orElseThrow();

        assertEquals(9, event.eventId());
        assertEquals("readahead", event.reason());
        assertEquals(77, event.inodeNumber());
        assertEquals(4096, event.offset());
        assertEquals(8192, event.length());
        assertEquals(1, event.droppedEvents());
        assertEquals(3, event.mountId());
        assertEquals("/var/lib/cx/cxbfs", event.metadataRoot());
        assertEquals(64, event.maxCords());
        assertEquals(128, event.readaheadPages());
        assertEquals(3000, event.missWaitMs());
        assertTrue(event.verifyHashes());
        assertEquals(1, event.mountFlags());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "next-miss"
        ), runner.commands.getFirst());
    }

    @Test
    void nextMissEventReturnsEmptyForNonblockingNoEvent() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "miss_event=none errno=11\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        assertTrue(client.nextMissEvent(true).isEmpty());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "next-miss",
                "--nonblock"
        ), runner.commands.getFirst());
    }

    @Test
    void nextMissEventCanTargetMountedMetadataRoot() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "miss_event=none errno=11\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        assertTrue(client.nextMissEvent(
                LinuxBlockfsMountSelector.metadataRoot("/var/lib/cxnfs/disk-a"),
                true
        ).isEmpty());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "next-miss",
                "--metadata-root",
                "/var/lib/cxnfs/disk-a",
                "--nonblock"
        ), runner.commands.getFirst());
    }

    @Test
    void invalidateBuildsControlToolCommand() throws Exception {
        RecordingRunner runner = new RecordingRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "invalidated inode=11 offset=128 length=4096\n",
                ""
        ));
        LinuxBlockfsControlCommand client = new LinuxBlockfsControlCommand(
                Path.of("cxnfs-blockfs-ctl"),
                Path.of("/dev/test"),
                runner
        );

        LinuxBlockfsControlResult result = client.invalidate(new LinuxBlockfsInvalidation(11, 128, 4096), false);

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "invalidate",
                "--inode",
                "11",
                "--offset",
                "128",
                "--length",
                "4096"
        ), runner.commands.getFirst());
    }

    @Test
    void processRunnerCapturesStdoutAndStderrSeparately() throws Exception {
        assumeTrue(Path.of("/bin/sh").toFile().canExecute());

        LinuxBlockfsControlCommand.CommandResult result = LinuxBlockfsControlCommand.runProcess(
                List.of("/bin/sh", "-c", "printf out; printf err >&2"),
                Duration.ofSeconds(5)
        );

        assertEquals(0, result.exitCode());
        assertEquals("out", result.stdout());
        assertEquals("err", result.stderr());
    }

    @Test
    void processRunnerTimesOutHungControlTool() throws Exception {
        assumeTrue(Path.of("/bin/sh").toFile().canExecute());

        LinuxBlockfsControlCommand.CommandResult result = LinuxBlockfsControlCommand.runProcess(
                List.of("/bin/sh", "-c", "sleep 2"),
                Duration.ofMillis(100)
        );

        assertEquals(124, result.exitCode());
        assertTrue(result.stderr().contains("command timed out after"));
    }

    @Test
    void constructorRejectsNonPositiveProcessTimeout() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsControlCommand(
                        Path.of("cxnfs-blockfs-ctl"),
                        Path.of("/dev/test"),
                        Duration.ZERO
                )
        );
    }

    @Test
    void statsRejectsMissingFields() {
        assertThrows(IllegalArgumentException.class, () -> LinuxBlockfsStats.parse("abi_version=1"));
    }

    @Test
    void extentDefensivelyCopiesHash() {
        byte[] hash = new byte[32];
        LinuxBlockfsExtent extent = new LinuxBlockfsExtent(1, 0, 1, 0, 4096, hash);
        hash[0] = 0x7f;

        assertEquals(0, extent.expectedHash()[0]);
    }

    private static final class RecordingRunner implements LinuxBlockfsControlCommand.CommandRunner {
        private final LinuxBlockfsControlCommand.CommandResult result;
        private final List<List<String>> commands = new ArrayList<>();

        private RecordingRunner(LinuxBlockfsControlCommand.CommandResult result) {
            this.result = result;
        }

        @Override
        public LinuxBlockfsControlCommand.CommandResult run(List<String> command) {
            commands.add(command);
            return result;
        }
    }
}
