import errno
import io
import os
from pathlib import Path
import tempfile
import unittest

import cxnfs
from cxnfs.blockfs import (
    ABI_VERSION,
    CXBFS_ABI_VERSION,
    CXBFS_DEFAULT_EXTENT_FLAGS,
    DEFAULT_EXTENT_FLAGS,
    EXTENT_STRUCT,
    FILE_STRUCT,
    INVALIDATE_STRUCT,
    MISS_EVENT_NONBLOCK,
    MISS_EVENT_STRUCT,
    MOUNT_VERIFY_HASHES,
    IOCTL_REGISTER_FILE,
    STATS_STRUCT,
    STATS_V2_STRUCT,
    IOCTL_INVALIDATE,
    IOCTL_NEXT_MISS_EVENT,
    IOCTL_REGISTER_EXTENT,
    IOCTL_STATS,
    IOCTL_STATS_V2,
    BlockfsExtent,
    BlockfsExtentHintsSnapshot,
    BlockfsExtentHintStore,
    BlockfsFile,
    BlockfsInvalidation,
    BlockfsManifestChunk,
    BlockfsManifestEntry,
    BlockfsManifestIndexSnapshot,
    BlockfsManifestIndexStore,
    BlockfsMissEvent,
    BlockfsNamespaceSnapshot,
    BlockfsNamespaceStore,
    BlockfsPayloadAllocation,
    BlockfsPayloadAllocationSnapshot,
    BlockfsPayloadAllocationStore,
    BlockfsStageResult,
    audit_blockfs_metadata,
    build_blockfs_extent_hints,
    describe_blockfs_extent_hint_build,
    stage_download_to_blockfs,
    stage_file_to_blockfs,
    stage_from_network_server,
    stage_stream_to_blockfs,
    LinuxBlockfsControl,
    LinuxCxbfsControl,
    CxbfsExtent,
    CxbfsFile,
    CxbfsMissEvent,
    audit_cxbfs_metadata,
    build_cxbfs_extent_hints,
    describe_cxbfs_extent_hint_build,
)


class FakeIoctl:
    def __init__(
        self,
        *,
        unsupported=False,
        stats_v2_supported=True,
        miss_event_errno=None,
    ):
        self.calls = []
        self.unsupported = unsupported
        self.stats_v2_supported = stats_v2_supported
        self.miss_event_errno = miss_event_errno

    def __call__(self, fd, request, payload, mutate_flag=False):
        self.calls.append((fd, request, bytes(payload), mutate_flag))
        if request == IOCTL_NEXT_MISS_EVENT:
            if self.miss_event_errno is not None:
                raise OSError(self.miss_event_errno, "no miss event")
            flags = MISS_EVENT_STRUCT.unpack(payload)[1]
            payload[:] = MISS_EVENT_STRUCT.pack(
                ABI_VERSION,
                flags,
                21,
                77,
                4096,
                8192,
                2,
                0,
                1,
                3,
                64,
                128,
                3000,
                MOUNT_VERIFY_HASHES,
                b"/var/lib/cx/cxbfs\0" + (b"\0" * (256 - len("/var/lib/cx/cxbfs") - 1)),
            )
            return 0
        if request == IOCTL_STATS_V2:
            if not self.stats_v2_supported:
                raise OSError(errno.ENOTTY, "not supported")
            payload[:] = STATS_V2_STRUCT.pack(
                ABI_VERSION,
                0,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
            )
            return 0
        if request == IOCTL_STATS:
            payload[:] = STATS_STRUCT.pack(ABI_VERSION, 0, 3, 4, 5, 6, 7)
            return 0
        if self.unsupported and request in (IOCTL_REGISTER_EXTENT, IOCTL_INVALIDATE):
            raise OSError(errno.EOPNOTSUPP, "not yet supported")
        return 0


class LinuxBlockfsControlTest(unittest.TestCase):
    def test_cxbfs_aliases_preserve_existing_blockfs_contracts(self):
        self.assertEqual(ABI_VERSION, CXBFS_ABI_VERSION)
        self.assertEqual(DEFAULT_EXTENT_FLAGS, CXBFS_DEFAULT_EXTENT_FLAGS)
        self.assertIs(CxbfsExtent, BlockfsExtent)
        self.assertIs(CxbfsFile, BlockfsFile)
        self.assertIs(CxbfsMissEvent, BlockfsMissEvent)
        self.assertIs(LinuxCxbfsControl, LinuxBlockfsControl)
        self.assertIs(audit_cxbfs_metadata, audit_blockfs_metadata)
        self.assertIs(build_cxbfs_extent_hints, build_blockfs_extent_hints)
        self.assertIs(describe_cxbfs_extent_hint_build, describe_blockfs_extent_hint_build)
        self.assertIs(cxnfs.CxbfsExtent, BlockfsExtent)
        self.assertIs(cxnfs.CxbfsMissEvent, BlockfsMissEvent)
        self.assertIs(cxnfs.LinuxCxbfsControl, LinuxBlockfsControl)
        self.assertEqual(cxnfs.CXBFS_ABI_VERSION, ABI_VERSION)

    def test_stats_reads_mutated_ioctl_buffer(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            "/tmp/fake",
            opener=lambda path, flags: 99,
            closer=lambda fd: None,
            ioctl=fake,
        )

        stats = client.stats()

        self.assertEqual(3, stats.registered_extents)
        self.assertEqual(4, stats.invalidations)
        self.assertEqual(5, stats.read_folio_calls)
        self.assertEqual(6, stats.readahead_calls)
        self.assertEqual(7, stats.verified_page_fills)
        self.assertEqual(8, stats.miss_events_queued)
        self.assertEqual(9, stats.miss_events_drained)
        self.assertEqual(10, stats.miss_events_pending)
        self.assertEqual(11, stats.miss_events_dropped)
        self.assertEqual(12, stats.miss_wait_successes)
        self.assertEqual(13, stats.miss_wait_timeouts)
        self.assertEqual(14, stats.miss_wait_interruptions)
        self.assertEqual(15, stats.extent_generation)
        self.assertEqual(16, stats.hash_verifications)
        self.assertEqual(17, stats.hash_mismatches)
        self.assertEqual(18, stats.hash_bytes_verified)
        self.assertEqual(19, stats.readahead_miss_windows)
        self.assertEqual(20, stats.readahead_miss_bytes)
        self.assertEqual(IOCTL_STATS_V2, fake.calls[0][1])
        self.assertTrue(fake.calls[0][3])

    def test_next_miss_reads_mutated_ioctl_buffer(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            "/tmp/fake",
            opener=lambda path, flags: 99,
            closer=lambda fd: None,
            ioctl=fake,
        )

        event = client.next_miss(nonblock=True)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(21, event.event_id)
        self.assertEqual("readahead", event.reason)
        self.assertEqual(77, event.inode_number)
        self.assertEqual(4096, event.offset)
        self.assertEqual(8192, event.length)
        self.assertEqual(1, event.dropped_events)
        self.assertEqual(3, event.mount_id)
        self.assertEqual("/var/lib/cx/cxbfs", event.metadata_root)
        self.assertEqual(64, event.max_cords)
        self.assertEqual(128, event.readahead_pages)
        self.assertEqual(3000, event.miss_wait_ms)
        self.assertTrue(event.verify_hashes)
        self.assertEqual(MOUNT_VERIFY_HASHES, event.mount_flags)
        self.assertEqual(IOCTL_NEXT_MISS_EVENT, fake.calls[0][1])
        self.assertEqual(MISS_EVENT_NONBLOCK, MISS_EVENT_STRUCT.unpack(fake.calls[0][2])[1])
        self.assertTrue(fake.calls[0][3])

    def test_next_miss_returns_none_for_nonblocking_empty_queue(self):
        fake = FakeIoctl(miss_event_errno=errno.EAGAIN)
        client = LinuxBlockfsControl(
            "/tmp/fake",
            opener=lambda path, flags: 99,
            closer=lambda fd: None,
            ioctl=fake,
        )

        self.assertIsNone(client.next_miss(nonblock=True))
        self.assertEqual(IOCTL_NEXT_MISS_EVENT, fake.calls[0][1])

    def test_stats_falls_back_to_legacy_ioctl(self):
        fake = FakeIoctl(stats_v2_supported=False)
        client = LinuxBlockfsControl(
            "/tmp/fake",
            opener=lambda path, flags: 99,
            closer=lambda fd: None,
            ioctl=fake,
        )

        stats = client.stats()

        self.assertEqual(3, stats.registered_extents)
        self.assertEqual(7, stats.verified_page_fills)
        self.assertEqual(0, stats.miss_wait_successes)
        self.assertEqual(0, stats.hash_mismatches)
        self.assertEqual(0, stats.readahead_miss_windows)
        self.assertEqual([IOCTL_STATS_V2, IOCTL_STATS], [call[1] for call in fake.calls])

    def test_register_extent_packs_uapi_struct_and_accepts_supported_kernel(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )
        extent = BlockfsExtent(
            inode_number=11,
            file_offset=128,
            payload_block=5,
            payload_offset=16,
            payload_length=4096,
            expected_hash=b"a" * 32,
        )

        accepted = client.register_extent(extent, allow_unsupported=True)

        self.assertTrue(accepted)
        self.assertEqual(IOCTL_REGISTER_EXTENT, fake.calls[0][1])
        unpacked = EXTENT_STRUCT.unpack(fake.calls[0][2])
        self.assertEqual((ABI_VERSION, DEFAULT_EXTENT_FLAGS, 11, 128, 5, 16, 4096, b"a" * 32), unpacked)

    def test_register_extent_can_tolerate_older_unsupported_kernel(self):
        fake = FakeIoctl(unsupported=True)
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )
        extent = BlockfsExtent(
            inode_number=11,
            file_offset=128,
            payload_block=5,
            payload_offset=16,
            payload_length=4096,
            expected_hash=b"a" * 32,
        )

        accepted = client.register_extent(extent, allow_unsupported=True)

        self.assertFalse(accepted)
        self.assertEqual(IOCTL_REGISTER_EXTENT, fake.calls[0][1])
        unpacked = EXTENT_STRUCT.unpack(fake.calls[0][2])
        self.assertEqual((ABI_VERSION, DEFAULT_EXTENT_FLAGS, 11, 128, 5, 16, 4096, b"a" * 32), unpacked)

    def test_invalidate_packs_uapi_struct_and_accepts_supported_kernel(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )

        accepted = client.invalidate(
            BlockfsInvalidation(inode_number=11, offset=128, length=4096),
            allow_unsupported=True,
        )

        self.assertTrue(accepted)
        self.assertEqual(IOCTL_INVALIDATE, fake.calls[0][1])
        self.assertEqual((ABI_VERSION, 0, 11, 128, 4096), INVALIDATE_STRUCT.unpack(fake.calls[0][2]))

    def test_invalidate_can_tolerate_older_unsupported_kernel(self):
        fake = FakeIoctl(unsupported=True)
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )

        accepted = client.invalidate(
            BlockfsInvalidation(inode_number=11, offset=128, length=4096),
            allow_unsupported=True,
        )

        self.assertFalse(accepted)
        self.assertEqual(IOCTL_INVALIDATE, fake.calls[0][1])
        self.assertEqual((ABI_VERSION, 0, 11, 128, 4096), INVALIDATE_STRUCT.unpack(fake.calls[0][2]))

    def test_register_file_packs_relative_vfs_record(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )

        accepted = client.register_file(BlockfsFile(22, "datasets/train/sample.bin", 4096, 0o444))

        self.assertTrue(accepted)
        self.assertEqual(IOCTL_REGISTER_FILE, fake.calls[0][1])
        abi, mode, inode_number, size, path = FILE_STRUCT.unpack(fake.calls[0][2])
        self.assertEqual(ABI_VERSION, abi)
        self.assertEqual(0o444, mode)
        self.assertEqual(22, inode_number)
        self.assertEqual(4096, size)
        self.assertEqual(b"datasets/train/sample.bin", path.split(b"\0", 1)[0])

    def test_extent_requires_verified_hash_contract(self):
        with self.assertRaisesRegex(ValueError, "32 bytes"):
            BlockfsExtent(1, 0, 1, 0, 4096, b"short")

    def test_file_accepts_nested_relative_paths_and_rejects_unsafe_paths(self):
        self.assertEqual("nested/sample.bin", BlockfsFile(1, "nested/sample.bin", 1).path)
        self.assertEqual(".hidden", BlockfsFile(2, ".hidden", 1).path)
        with self.assertRaisesRegex(ValueError, "relative"):
            BlockfsFile(3, "/nested/sample.bin", 1)
        with self.assertRaisesRegex(ValueError, "parent"):
            BlockfsFile(4, "nested/../sample.bin", 1)
        with self.assertRaisesRegex(ValueError, "control"):
            BlockfsFile(5, "nested/\x07sample.bin", 1)

    def test_namespace_store_round_trips_current_snapshot(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = BlockfsNamespaceSnapshot(
                4,
                (
                    BlockfsFile(2, "datasets/train/sample.bin", 4096),
                    BlockfsFile(3, "datasets/train/labels.mds", 8192, 0o400),
                ),
            )

            path = BlockfsNamespaceStore().write(root, snapshot)
            loaded = BlockfsNamespaceStore().load(root)

            self.assertEqual(root / "namespace/current.cxn", path)
            self.assertEqual(snapshot, loaded)

    def test_publish_namespace_registers_each_file(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )
        snapshot = BlockfsNamespaceSnapshot(
            4,
            (
                BlockfsFile(2, "datasets/train/sample.bin", 4096),
                BlockfsFile(3, "datasets/train/labels.mds", 8192),
            ),
        )

        published = client.publish_namespace(snapshot)

        self.assertEqual(2, published)
        self.assertEqual(IOCTL_REGISTER_FILE, fake.calls[0][1])
        self.assertEqual(IOCTL_REGISTER_FILE, fake.calls[1][1])

    def test_extent_hint_store_round_trips_current_snapshot(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = BlockfsExtentHintsSnapshot(
                4,
                (
                    BlockfsExtent(2, 0, 1, 0, 4096, b"a" * 32),
                    BlockfsExtent(2, 4096, 2, 128, 2048, b"b" * 32),
                ),
            )

            path = BlockfsExtentHintStore().write(root, snapshot)
            loaded = BlockfsExtentHintStore().load(root)

            self.assertEqual(root / "extent-hints/current.cxn", path)
            self.assertEqual(4, loaded.generation)
            self.assertEqual(2, len(loaded.extents))
            self.assertEqual(2048, loaded.extents[1].payload_length)
            self.assertEqual(b"b" * 32, loaded.extents[1].expected_hash)

    def test_extent_hint_store_missing_file_is_optional(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp:
            self.assertIsNone(BlockfsExtentHintStore().load_if_exists(temp))

    def test_extent_hint_snapshot_rejects_overlapping_inodes(self):
        with self.assertRaisesRegex(ValueError, "overlapping"):
            BlockfsExtentHintsSnapshot(
                4,
                (
                    BlockfsExtent(2, 0, 1, 0, 4096, b"a" * 32),
                    BlockfsExtent(2, 2048, 2, 0, 4096, b"b" * 32),
                ),
            )

    def test_publish_extent_hints_registers_each_extent(self):
        fake = FakeIoctl()
        client = LinuxBlockfsControl(
            ioctl=fake,
            opener=lambda path, flags: 7,
            closer=lambda fd: None,
        )
        snapshot = BlockfsExtentHintsSnapshot(
            4,
            (
                BlockfsExtent(2, 0, 1, 0, 4096, b"a" * 32),
                BlockfsExtent(2, 4096, 2, 0, 2048, b"b" * 32),
            ),
        )

        published = client.publish_extent_hints(snapshot)

        self.assertEqual(2, published)
        self.assertEqual(IOCTL_REGISTER_EXTENT, fake.calls[0][1])
        self.assertEqual(IOCTL_REGISTER_EXTENT, fake.calls[1][1])

    def test_manifest_index_store_round_trips_current_snapshot(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = BlockfsManifestIndexSnapshot(
                4,
                (
                    BlockfsManifestEntry(
                        2,
                        "datasets/train/sample.bin",
                        6144,
                        b"f" * 32,
                        (
                            BlockfsManifestChunk(0, 4096, b"a" * 32),
                            BlockfsManifestChunk(4096, 2048, b"b" * 32),
                        ),
                    ),
                ),
            )

            path = BlockfsManifestIndexStore().write(root, snapshot)
            loaded = BlockfsManifestIndexStore().load(root)

            self.assertEqual(root / "manifest-index/current.cxn", path)
            self.assertEqual(4, loaded.generation)
            self.assertEqual(1, len(loaded.entries))
            self.assertEqual(2, loaded.chunk_count)
            self.assertEqual(b"b" * 32, loaded.entries[0].chunks[1].chunk_hash)

    def test_payload_allocation_store_round_trips_current_snapshot(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = BlockfsPayloadAllocationSnapshot(
                4,
                (
                    BlockfsPayloadAllocation(2, 0, 1, 0, 4096),
                    BlockfsPayloadAllocation(2, 4096, 2, 128, 2048),
                ),
            )

            path = BlockfsPayloadAllocationStore().write(root, snapshot)
            loaded = BlockfsPayloadAllocationStore().load(root)

            self.assertEqual(root / "payload-map/current.cxn", path)
            self.assertEqual(4, loaded.generation)
            self.assertEqual(2, len(loaded.allocations))
            self.assertEqual(128, loaded.allocations[1].payload_offset)

    def test_payload_allocation_snapshot_rejects_overlapping_inodes(self):
        with self.assertRaisesRegex(ValueError, "overlapping"):
            BlockfsPayloadAllocationSnapshot(
                4,
                (
                    BlockfsPayloadAllocation(2, 0, 1, 0, 4096),
                    BlockfsPayloadAllocation(2, 2048, 2, 0, 4096),
                ),
            )

    def test_manifest_index_missing_file_is_optional(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp:
            self.assertIsNone(BlockfsManifestIndexStore().load_if_exists(temp))

    def test_manifest_entry_rejects_non_contiguous_chunks(self):
        with self.assertRaisesRegex(ValueError, "contiguous"):
            BlockfsManifestEntry(
                2,
                "datasets/train/sample.bin",
                4096,
                b"f" * 32,
                (BlockfsManifestChunk(128, 4096, b"a" * 32),),
            )

    def test_build_blockfs_extent_hints_from_payload_allocations(self):
        manifest = BlockfsManifestIndexSnapshot(
            4,
            (
                BlockfsManifestEntry(
                    2,
                    "datasets/train/sample.bin",
                    6144,
                    b"f" * 32,
                    (
                        BlockfsManifestChunk(0, 4096, b"a" * 32),
                        BlockfsManifestChunk(4096, 2048, b"b" * 32),
                    ),
                ),
            ),
        )
        allocations = BlockfsPayloadAllocationSnapshot(
            4,
            (BlockfsPayloadAllocation(2, 0, 1, 0, 4096),),
        )

        hints = build_blockfs_extent_hints(manifest, allocations)
        result = describe_blockfs_extent_hint_build(manifest, allocations, hints)

        self.assertEqual(1, len(hints.extents))
        self.assertEqual(b"a" * 32, hints.extents[0].expected_hash)
        self.assertFalse(result.complete)
        with self.assertRaisesRegex(ValueError, "cover every manifest chunk"):
            build_blockfs_extent_hints(manifest, allocations, require_complete=True)

    def test_build_blockfs_extent_hints_rejects_non_manifest_allocation(self):
        manifest = BlockfsManifestIndexSnapshot(
            4,
            (
                BlockfsManifestEntry(
                    2,
                    "datasets/train/sample.bin",
                    4096,
                    b"f" * 32,
                    (BlockfsManifestChunk(0, 4096, b"a" * 32),),
                ),
            ),
        )
        allocations = BlockfsPayloadAllocationSnapshot(
            4,
            (BlockfsPayloadAllocation(2, 128, 1, 0, 4096),),
        )

        with self.assertRaisesRegex(ValueError, "manifest chunk"):
            build_blockfs_extent_hints(manifest, allocations)

    def test_audit_blockfs_metadata_accepts_matching_chain(self):
        result = audit_blockfs_metadata(
            BlockfsNamespaceSnapshot(
                4,
                (BlockfsFile(2, "datasets/train/sample.bin", 4096),),
            ),
            BlockfsManifestIndexSnapshot(
                4,
                (
                    BlockfsManifestEntry(
                        2,
                        "datasets/train/sample.bin",
                        4096,
                        b"f" * 32,
                        (BlockfsManifestChunk(0, 4096, b"a" * 32),),
                    ),
                ),
            ),
            BlockfsExtentHintsSnapshot(
                4,
                (BlockfsExtent(2, 0, 1, 0, 4096, b"a" * 32),),
            ),
        )

        self.assertEqual(1, result.namespace_files)
        self.assertEqual(1, result.manifest_files)
        self.assertEqual(1, result.manifest_chunks)
        self.assertEqual(1, result.extent_hints)

    def test_audit_blockfs_metadata_rejects_extent_hints_without_manifest_index(self):
        with self.assertRaisesRegex(ValueError, "manifest index"):
            audit_blockfs_metadata(
                BlockfsNamespaceSnapshot(
                    4,
                    (BlockfsFile(2, "datasets/train/sample.bin", 4096),),
                ),
                None,
                BlockfsExtentHintsSnapshot(
                    4,
                    (BlockfsExtent(2, 0, 1, 0, 4096, b"a" * 32),),
                ),
            )

    def test_audit_blockfs_metadata_rejects_hash_mismatch(self):
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            audit_blockfs_metadata(
                BlockfsNamespaceSnapshot(
                    4,
                    (BlockfsFile(2, "datasets/train/sample.bin", 4096),),
                ),
                BlockfsManifestIndexSnapshot(
                    4,
                    (
                        BlockfsManifestEntry(
                            2,
                            "datasets/train/sample.bin",
                            4096,
                            b"f" * 32,
                            (BlockfsManifestChunk(0, 4096, b"a" * 32),),
                        ),
                    ),
                ),
                BlockfsExtentHintsSnapshot(
                    4,
                    (BlockfsExtent(2, 0, 1, 0, 4096, b"b" * 32),),
                ),
            )

    def test_namespace_rejects_file_directory_conflicts(self):
        with self.assertRaisesRegex(ValueError, "implicit directory"):
            BlockfsNamespaceSnapshot(
                4,
                (
                    BlockfsFile(2, "datasets", 4096),
                    BlockfsFile(3, "datasets/train/labels.mds", 8192),
                ),
            )

    def test_default_opener_uses_read_write_device_flag(self):
        opened = {}

        def opener(path, flags):
            opened["path"] = path
            opened["flags"] = flags
            return 1

        client = LinuxBlockfsControl(
            "/dev/custom",
            opener=opener,
            closer=lambda fd: None,
            ioctl=FakeIoctl(),
        )
        client.stats()

        self.assertEqual("/dev/custom", opened["path"])
        self.assertEqual(os.O_RDWR, opened["flags"])


class BlockfsDatasetStagingTest(unittest.TestCase):
    def test_stage_stream_and_audit_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "metadata"
            payload = Path(temp) / "payload.bin"
            sample_data = b"Hello, SAT-FS dataset block staging!" * 500

            res = stage_stream_to_blockfs(
                metadata_root=root,
                payload_target=payload,
                stream=io.BytesIO(sample_data),
                target_relative_path="train/audio.wav",
                chunk_size=4096,
            )

            self.assertEqual("train/audio.wav", res.path)
            self.assertEqual(len(sample_data), res.size)
            self.assertEqual(5, res.chunks_count)
            self.assertEqual(5, res.extents_count)
            self.assertFalse(res.published_to_kernel)

            self.assertTrue((root / "namespace/current.sat").exists())
            self.assertTrue((root / "namespace/current.cxn").exists())
            self.assertTrue((root / "manifest-index/current.sat").exists())
            self.assertTrue((root / "manifest-index/current.cxn").exists())
            self.assertTrue((root / "payload-map/current.sat").exists())
            self.assertTrue((root / "payload-map/current.cxn").exists())
            self.assertTrue((root / "extent-hints/current.sat").exists())
            self.assertTrue((root / "extent-hints/current.cxn").exists())

            ns = BlockfsNamespaceStore().load(root)
            manifest = BlockfsManifestIndexStore().load(root)
            hints = BlockfsExtentHintStore().load(root)
            audit = audit_blockfs_metadata(ns, manifest, hints)
            self.assertEqual(1, audit.namespace_files)
            self.assertEqual(1, audit.manifest_files)
            self.assertEqual(5, audit.manifest_chunks)
            self.assertEqual(5, audit.extent_hints)

            self.assertEqual(sample_data, payload.read_bytes())

    def test_stage_file_and_subsequent_staging_allocations(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "metadata"
            payload = Path(temp) / "payload.bin"
            file1 = Path(temp) / "source1.bin"
            file2 = Path(temp) / "source2.bin"

            data1 = b"ABC" * 2000
            data2 = b"XYZ" * 1500
            file1.write_bytes(data1)
            file2.write_bytes(data2)

            res1 = stage_file_to_blockfs(
                metadata_root=root,
                payload_target=payload,
                source_file_path=file1,
                target_relative_path="data/part1.bin",
                chunk_size=4096,
            )
            self.assertEqual(2, res1.inode_number)

            res2 = stage_file_to_blockfs(
                metadata_root=root,
                payload_target=payload,
                source_file_path=file2,
                target_relative_path="data/part2.bin",
                chunk_size=4096,
            )
            self.assertEqual(3, res2.inode_number)

            ns = BlockfsNamespaceStore().load(root)
            self.assertEqual(2, len(ns.files))
            allocs = BlockfsPayloadAllocationStore().load(root)
            self.assertEqual(4, len(allocs.allocations))

            audit = audit_blockfs_metadata(
                ns,
                BlockfsManifestIndexStore().load(root),
                BlockfsExtentHintStore().load(root),
            )
            self.assertEqual(2, audit.namespace_files)
            self.assertEqual(4, audit.manifest_chunks)

    def test_stage_from_network_server_with_ctl_client(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "metadata"
            payload = Path(temp) / "payload.bin"
            data = b"Remote server mock data" * 100

            fake = FakeIoctl()
            ctl = LinuxBlockfsControl(
                "/dev/fake",
                opener=lambda path, flags: 88,
                closer=lambda fd: None,
                ioctl=fake,
            )

            def mock_reader(path: str, offset: int, length: int) -> bytes:
                return data[offset : offset + length]

            res = stage_from_network_server(
                metadata_root=root,
                payload_target=payload,
                reader=mock_reader,
                remote_path="/remote/dataset/train.tar",
                target_relative_path="train.tar",
                remote_size=len(data),
                chunk_size=512,
                ctl_client=ctl,
            )

            self.assertTrue(res.published_to_kernel)
            self.assertTrue(any(call[1] == IOCTL_REGISTER_FILE for call in fake.calls))
            self.assertTrue(any(call[1] == IOCTL_REGISTER_EXTENT for call in fake.calls))

    def test_stage_duplicate_file_raises_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "metadata"
            payload = Path(temp) / "payload.bin"
            data = b"test data"

            stage_stream_to_blockfs(
                metadata_root=root,
                payload_target=payload,
                stream=io.BytesIO(data),
                target_relative_path="duplicate.txt",
            )

            with self.assertRaises(FileExistsError):
                stage_stream_to_blockfs(
                    metadata_root=root,
                    payload_target=payload,
                    stream=io.BytesIO(b"other"),
                    target_relative_path="duplicate.txt",
                    allow_overwrite=False,
                )

            res = stage_stream_to_blockfs(
                metadata_root=root,
                payload_target=payload,
                stream=io.BytesIO(b"updated data"),
                target_relative_path="duplicate.txt",
                allow_overwrite=True,
            )
            self.assertEqual(len(b"updated data"), res.size)


if __name__ == "__main__":
    unittest.main()
