from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import io
import json
import pickle
import tarfile
import unittest

from cxnfs import (
    CxnfsDatasetClient,
    CxnfsRangeCacheClient,
    DatasetConverter,
    DatasetPreparer,
    LocalFileTransport,
    MdsDataReadPlan,
    MdsDataset,
    MdsDirectTensorSample,
)


class RecordingClient:
    def __init__(self, root: Path) -> None:
        self.delegate = CxnfsDatasetClient(LocalFileTransport(root))
        self.reads = []

    def read_mds_shard(self, shard_path, offset=0, length=None):
        self.reads.append((str(shard_path), offset, length))
        return self.delegate.read_mds_shard(shard_path, offset, length)


class ClosableRecordingClient(RecordingClient):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.closed = False

    def close(self):
        self.closed = True


class DirectRecordingClient(RecordingClient):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.buffer_reads = []
        self.tensor_reads = []

    def read_into_buffer(self, shard_path, offset, buffer, nbytes=None):
        count = memoryview(buffer).nbytes if nbytes is None else nbytes
        self.buffer_reads.append((str(shard_path), offset, count))
        payload = self.delegate.read_mds_shard(shard_path, offset, count)
        view = memoryview(getattr(buffer, "data", buffer))
        writable = view if view.format == "B" and view.ndim == 1 else view.cast("B")
        writable[:len(payload)] = payload
        return len(payload)

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        self.tensor_reads.append((str(shard_path), offset, nbytes, target, device, non_blocking))
        return self.read_into_buffer(shard_path, offset, tensor, nbytes)


class FakeTensor:
    def __init__(self, nbytes: int) -> None:
        self.data = bytearray(nbytes)
        self.device = "cpu"
        self.is_cuda = False


class ByteClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.reads = []

    def read_mds_shard(self, shard_path, offset=0, length=None):
        self.reads.append((str(shard_path), offset, length))
        return self.payload[offset:] if length is None else self.payload[offset:offset + length]


class MdsDatasetTest(unittest.TestCase):
    def test_reads_generated_webdataset_samples_by_index(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            make_tar(
                root / "samples-000.tar",
                {
                    "000001.jpg": b"image-one",
                    "000001.txt": b"caption one",
                    "000002.jpg": b"image-two",
                },
            )
            plan = DatasetPreparer(target_shard_size_bytes=65536).plan(root, "images", "webdataset")
            result = DatasetConverter().execute(plan, root / "mds")

            dataset = MdsDataset(result.output_dir)

            self.assertEqual(2, len(dataset))
            first = dataset[0]
            second = dataset.read_sample(1)
            self.assertEqual("samples-000.tar#000001", first.path)
            self.assertEqual("samples-000.tar#000002", second.path)
            self.assertEqual(
                {"000001.jpg": b"image-one", "000001.txt": b"caption one"},
                read_tar_payload(first.data),
            )
            self.assertEqual({"000002.jpg": b"image-two"}, read_tar_payload(second.data))

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow is required for parquet conversion")
    def test_reads_generated_parquet_rows_by_negative_index(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as directory:
            root = Path(directory)
            pq.write_table(
                pa.table({"id": [1, 2], "text": ["alpha", "beta"]}),
                root / "part-000.parquet",
            )
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "train", "parquet")
            result = DatasetConverter().execute(plan, root / "mds")

            dataset = MdsDataset(result.output_dir)
            sample = dataset[-1]

            self.assertEqual("part-000.parquet#row000000000001", sample.path)
            self.assertEqual({"id": 2, "text": "beta"}, json.loads(sample.data.decode("utf-8")))

    def test_reads_sample_payloads_through_cxnfs_client(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = RecordingClient(Path(result.output_dir))

            sample = MdsDataset(result.output_dir, client=client).read_sample(0)

            self.assertEqual("a.bin", sample.path)
            self.assertEqual(b"alpha", sample.data)
            self.assertGreaterEqual(len(client.reads), 2)
            self.assertEqual("files-000000.mds", client.reads[0][0])
            self.assertEqual(0, client.reads[0][1])
            self.assertIsNotNone(client.reads[1][2])

    def test_plans_sample_locations_and_reads_batch_in_requested_order(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            (root / "c.bin").write_bytes(b"gamma")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = RecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)

            locations = dataset.sample_locations((2, 0))
            samples = dataset.read_samples((2, 0), max_workers=2)

            self.assertEqual(("files-000000.mds", "files-000000.mds"), tuple(location.shard_name for location in locations))
            self.assertEqual((2, 0), tuple(location.global_index for location in locations))
            self.assertTrue(all(location.length > 8 for location in locations))
            self.assertEqual(("c.bin", "a.bin"), tuple(sample.path for sample in samples))
            self.assertEqual((b"gamma", b"alpha"), tuple(sample.data for sample in samples))
            self.assertEqual(1, len([read for read in client.reads if read[1] == 0]))

    def test_plans_data_payload_reads_without_reading_sample_bodies(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = RecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)

            reads = dataset.plan_data_reads((1, 0), max_workers=2, include_paths=True)

            self.assertEqual(2, len(reads))
            self.assertTrue(all(isinstance(read, MdsDataReadPlan) for read in reads))
            self.assertEqual((1, 0), tuple(read.global_index for read in reads))
            self.assertEqual(("b.bin", "a.bin"), tuple(read.path for read in reads))
            self.assertEqual((4, 5), tuple(read.data_length for read in reads))
            self.assertTrue(all(read.data_offset > 0 for read in reads))
            self.assertNotIn(
                ("files-000000.mds", reads[0].data_offset, reads[0].data_length),
                client.reads,
            )

    def test_coalesces_adjacent_sample_reads_into_one_payload_range(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            (root / "c.bin").write_bytes(b"gamma")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = RecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)

            spans = dataset.read_spans((0, 1, 2))
            client.reads.clear()
            samples = dataset.read_samples((0, 1, 2), max_workers=4)

            self.assertEqual(1, len(spans))
            self.assertEqual((0, 1, 2), spans[0].result_positions)
            self.assertEqual(3, len(spans[0].locations))
            self.assertEqual(("a.bin", "b.bin", "c.bin"), tuple(sample.path for sample in samples))
            self.assertEqual(1, len(client.reads))
            self.assertEqual(spans[0].offset, client.reads[0][1])
            self.assertEqual(spans[0].length, client.reads[0][2])

    def test_can_disable_batch_coalescing(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = RecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)

            dataset.read_spans((0, 1))
            client.reads.clear()
            samples = dataset.read_samples((0, 1), max_workers=4, coalesce=False)

            self.assertEqual(("a.bin", "b.bin"), tuple(sample.path for sample in samples))
            self.assertEqual(2, len(client.reads))

    def test_reads_data_column_into_caller_owned_buffers(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            (root / "c.bin").write_bytes(b"gamma")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = DirectRecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)
            first = bytearray(5)
            second = bytearray(5)

            samples = dataset.read_data_into_buffers(
                (2, 0),
                (first, second),
                max_workers=2,
                include_paths=True,
            )

            self.assertEqual(b"gamma", bytes(first))
            self.assertEqual(b"alpha", bytes(second))
            self.assertEqual(("c.bin", "a.bin"), tuple(sample.path for sample in samples))
            self.assertEqual((5, 5), tuple(sample.bytes_read for sample in samples))
            self.assertEqual((2, 0), tuple(sample.global_index for sample in samples))
            self.assertEqual(2, len(client.buffer_reads))
            self.assertEqual(
                sorted((sample.shard_name, sample.data_offset, sample.data_length) for sample in samples),
                sorted(client.buffer_reads),
            )

    def test_direct_data_reads_cache_sample_descriptors_across_repeated_epochs(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = DirectRecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)

            def destination_factory(location, data_length):
                return bytearray(data_length)

            first = dataset.read_data_with_buffer_factory(
                (0, 1),
                destination_factory,
                max_workers=1,
                include_paths=True,
            )
            descriptor_reads_after_first_epoch = list(client.reads)
            second = dataset.read_data_with_buffer_factory(
                (0, 1),
                destination_factory,
                max_workers=1,
                include_paths=True,
            )

            self.assertEqual(("a.bin", "b.bin"), tuple(sample.path for sample in first))
            self.assertEqual(("a.bin", "b.bin"), tuple(sample.path for sample in second))
            self.assertEqual((b"alpha", b"beta"), tuple(bytes(sample.data) for sample in first))
            self.assertEqual((b"alpha", b"beta"), tuple(bytes(sample.data) for sample in second))
            self.assertEqual(descriptor_reads_after_first_epoch, client.reads)
            self.assertEqual(4, len(client.buffer_reads))
            evidence = dataset.descriptor_cache_evidence()
            self.assertEqual("cxnfs.mds-descriptor-cache.v1", evidence["format"])
            self.assertEqual(2, evidence["sample_header_cache_entries"])
            self.assertEqual(2, evidence["sample_header_cache_misses"])
            self.assertEqual(2, evidence["sample_header_cache_hits"])
            self.assertTrue(evidence["sample_header_cache_reuse_observed"])
            self.assertEqual(2, evidence["sample_path_cache_entries"])
            self.assertEqual(2, evidence["sample_path_cache_misses"])
            self.assertEqual(2, evidence["sample_path_cache_hits"])
            self.assertTrue(evidence["sample_path_cache_reuse_observed"])

    def test_mds_reader_descriptor_cache_survives_dataloader_pickling(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = MdsDataset(result.output_dir)
            first = bytearray(5)

            dataset.read_data_into_buffers((0,), (first,), max_workers=1, include_paths=True)
            restored = pickle.loads(pickle.dumps(dataset))
            second = bytearray(5)
            samples = restored.read_data_into_buffers((0,), (second,), max_workers=1, include_paths=True)

            self.assertEqual(b"alpha", bytes(first))
            self.assertEqual(b"alpha", bytes(second))
            self.assertEqual(("a.bin",), tuple(sample.path for sample in samples))

    def test_reads_batch_data_column_into_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = DirectRecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)
            batch = dataset.plan_batches(2)[0]
            first = bytearray(5)
            second = bytearray(4)

            samples = dataset.read_batch_data_into_tensors(
                batch,
                (first, second),
                max_workers=2,
                non_blocking=False,
            )

            self.assertEqual(b"alpha", bytes(first))
            self.assertEqual(b"beta", bytes(second))
            self.assertEqual((None, None), tuple(sample.path for sample in samples))
            self.assertEqual((5, 4), tuple(sample.data_length for sample in samples))
            self.assertEqual(2, len(client.tensor_reads))
            self.assertTrue(all(read[-1] is False for read in client.tensor_reads))

    def test_reads_data_column_with_tensor_factory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = DirectRecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client)
            allocations = []

            def tensor_factory(location, data_length):
                tensor = FakeTensor(data_length)
                allocations.append((location.global_index, data_length, tensor))
                return tensor

            samples = dataset.read_data_with_tensor_factory(
                (1, 0),
                tensor_factory,
                max_workers=2,
                include_paths=True,
                non_blocking=False,
            )

            self.assertEqual(2, len(samples))
            self.assertTrue(all(isinstance(sample, MdsDirectTensorSample) for sample in samples))
            self.assertEqual((1, 0), tuple(sample.metadata.global_index for sample in samples))
            self.assertEqual(("b.bin", "a.bin"), tuple(sample.path for sample in samples))
            self.assertEqual((b"beta", b"alpha"), tuple(bytes(sample.tensor.data) for sample in samples))
            self.assertEqual([(0, 5), (1, 4)], sorted((index, length) for index, length, _ in allocations))
            self.assertEqual(2, len(client.tensor_reads))
            self.assertTrue(all(read[-1] is False for read in client.tensor_reads))

    def test_direct_data_reads_reject_small_destinations(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = MdsDataset(result.output_dir)

            with self.assertRaisesRegex(ValueError, "smaller"):
                dataset.read_data_into_buffers((0,), (bytearray(4),))

            with self.assertRaisesRegex(ValueError, "destinations"):
                dataset.read_data_into_buffers((0,), ())

    def test_range_cache_serves_sample_subranges_from_cached_span(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            (root / "c.bin").write_bytes(b"gamma")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            recording = RecordingClient(Path(result.output_dir))
            cache = CxnfsRangeCacheClient(recording, max_bytes=4096)
            dataset = MdsDataset(result.output_dir, client=cache)

            dataset.read_samples((0, 1, 2), max_workers=1)
            recording.reads.clear()
            cached_sample = dataset.read_sample(1)

            self.assertEqual("b.bin", cached_sample.path)
            self.assertEqual(b"beta", cached_sample.data)
            self.assertEqual([], recording.reads)
            self.assertGreaterEqual(cache.stats.hits, 1)

    def test_range_cache_evicts_to_byte_budget(self):
        delegate = ByteClient(b"abcdefgh")
        cache = CxnfsRangeCacheClient(delegate, max_bytes=4)

        self.assertEqual(b"abcd", cache.read_mds_shard("s.mds", 0, 4))
        self.assertEqual(b"efgh", cache.read_mds_shard("s.mds", 4, 4))
        delegate.reads.clear()
        self.assertEqual(b"abcd", cache.read_mds_shard("s.mds", 0, 4))

        self.assertEqual([("s.mds", 0, 4)], delegate.reads)
        self.assertGreaterEqual(cache.stats.evictions, 1)
        self.assertLessEqual(cache.stats.bytes_cached, 4)

    def test_range_cache_is_safe_for_parallel_subrange_hits(self):
        delegate = ByteClient(b"abcdefghijklmnopqrstuvwxyz")
        cache = CxnfsRangeCacheClient(delegate, max_bytes=64)
        cache.read_mds_shard("s.mds", 0, 26)
        delegate.reads.clear()

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = tuple(executor.map(lambda pair: cache.read_mds_shard("s.mds", pair[0], pair[1]), ((0, 3), (3, 3), (6, 3), (9, 3))))

        self.assertEqual((b"abc", b"def", b"ghi", b"jkl"), results)
        self.assertEqual([], delegate.reads)
        self.assertEqual(4, cache.stats.hits)
        cache.reset_stats()
        self.assertEqual(0, cache.stats.hits)

    def test_mds_dataset_close_releases_cached_delegate_client(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            client = ClosableRecordingClient(Path(result.output_dir))
            dataset = MdsDataset(result.output_dir, client=client, cache_bytes=4096)
            dataset.read_sample(0)

            dataset.close()

            self.assertTrue(client.closed)
            self.assertEqual(0, dataset.client.stats.entries)

    def test_plans_batches_and_prefetches_future_batch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("a.bin", "b.bin", "c.bin", "d.bin", "e.bin"):
                (root / name).write_bytes(name.encode("utf-8"))
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            recording = RecordingClient(Path(result.output_dir))
            cache = CxnfsRangeCacheClient(recording, max_bytes=4096)
            dataset = MdsDataset(result.output_dir, client=cache)

            batches = dataset.plan_batches(2)
            shuffled_a = dataset.plan_batches(2, shuffle=True, seed=7)
            shuffled_b = dataset.plan_batches(2, shuffle=True, seed=7)
            recording.reads.clear()
            prefetch = dataset.prefetch_batch(batches[1], max_workers=1)
            recording.reads.clear()
            samples = dataset.read_batch(batches[1], max_workers=1)

            self.assertEqual(((0, 1), (2, 3), (4,)), tuple(batch.indices for batch in batches))
            self.assertEqual(tuple(batch.indices for batch in shuffled_a), tuple(batch.indices for batch in shuffled_b))
            self.assertEqual((2, 3), batches[1].indices)
            self.assertEqual(2, prefetch.sample_count)
            self.assertEqual(1, prefetch.ranges_fetched)
            self.assertEqual(("c.bin", "d.bin"), tuple(sample.path for sample in samples))
            self.assertEqual([], recording.reads)

    def test_drop_last_batch_planning(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("a.bin", "b.bin", "c.bin"):
                (root / name).write_bytes(name.encode("utf-8"))
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = MdsDataset(result.output_dir)

            batches = dataset.plan_batches(2, drop_last=True)

            self.assertEqual(((0, 1),), tuple(batch.indices for batch in batches))

    def test_rejects_invalid_batch_worker_count(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = MdsDataset(result.output_dir)

            with self.assertRaisesRegex(ValueError, "max_workers"):
                dataset.read_samples((0,), max_workers=0)

    def test_rejects_out_of_range_sample_index(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = MdsDataset(result.output_dir)

            with self.assertRaises(IndexError):
                dataset.read_sample(1)


def make_tar(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))


def read_tar_payload(payload: bytes) -> dict[str, bytes]:
    result = {}
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as archive:
        for member in archive:
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            if extracted is not None:
                result[member.name] = extracted.read()
    return result


if __name__ == "__main__":
    unittest.main()
