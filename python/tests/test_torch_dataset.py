from argparse import Namespace
import contextlib
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import io
import itertools
import json
import os
import pickle
import ssl
import tarfile
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import UUID

import cxnfs.torch_dataset as torch_dataset_module
import cxnfs.tensor_memory as tensor_memory_module
from cxnfs.auth import AuthMechanism
from cxnfs.control import AuthAccepted, DirectoryListing, DirectoryListResponse, FileMetadata, FileType
from cxnfs import (
    ClientCredentials,
    CxnfsBulkReadConfig,
    CxnfsBulkRetryConfig,
    CxnfsDatasetClient,
    CxnfsPipelineConfig,
    CxnfsPythonClient,
    CxnfsRemoteTrainerClientFactory,
    CxnfsTorchDataset,
    CxnfsTrainerBatch,
    CxnfsTrainerBatchSample,
    CxnfsTrainerCursor,
    CxnfsTrainerDirectBatchSample,
    CxnfsTrainerIntegrationPlan,
    CxnfsTrainerPipelineBatchSample,
    CxnfsTrainerPipelineLoop,
    CxnfsTrainerPipelineStream,
    CxnfsTrainerPrefetchStream,
    CxnfsTrainerRuntimeBinding,
    CxnfsTrainerStream,
    CxnfsTrainerSurfaceError,
    CxnfsTrainerTensorAllocation,
    CxnfsTrainingPipeline,
    CxnfsTrainingShard,
    CxnfsWeightedSource,
    CxnfsWeightedTorchDataset,
    DatasetConverter,
    DatasetPreparer,
    LocalFileTransport,
    MdsDataset,
    MdsSampleFormat,
    SendResult,
    TRAINER_CONTRACT_FORMAT,
    TRAINER_CURSOR_FORMAT,
    TRAINER_DISTRIBUTED_PLAN_FORMAT,
    TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT,
    TRAINER_HANDOFF_FORMAT,
    TRAINER_HANDOFF_ACCEPTANCE_FORMAT,
    TRAINER_INTEGRATION_PLAN_FORMAT,
    TRAINER_NETWORK_PREFLIGHT_FORMAT,
    TRAINER_INTEGRATION_PREFLIGHT_FORMAT,
    TRAINER_INTEGRATION_SMOKE_FORMAT,
    TRAINER_ACCEPTANCE_FIXTURE_FORMAT,
    TRAINER_LOOP_CONTRACT_FORMAT,
    TRAINER_LOOP_CONTRACT_CHECK_FORMAT,
    TRAINER_READINESS_BUNDLE_FORMAT,
    TRAINER_LOOP_REPLAY_FORMAT,
    TRAINER_PIPELINE_SMOKE_FORMAT,
    TRAINER_READ_PATH_RELIABILITY_FORMAT,
    TRAINER_RUNTIME_BINDING_FORMAT,
    TRAINER_RUNTIME_SMOKE_FORMAT,
    TRAINER_STREAM_STATE_FORMAT,
    TRAINER_SUSTAINED_READ_PROFILE_FORMAT,
    TRAINER_SURFACE_CHECK_FORMAT,
    TRAINER_SURFACE_MATRIX_FORMAT,
    TRAINER_TENSOR_MEMORY_PATH_FORMAT,
    TRAINER_WORKER_PARTITION_FORMAT,
    TensorMemoryTarget,
    WEIGHTED_MDS_WORKLOAD_FINGERPRINT_FORMAT,
    build_trainer_distributed_runtime_smoke_payload,
    build_trainer_acceptance_fixture_payload,
    build_trainer_handoff_acceptance_payload,
    build_trainer_handoff_payload,
    build_trainer_network_preflight_payload,
    build_trainer_integration_smoke_payload,
    build_trainer_readiness_bundle_payload,
    build_trainer_loop_replay_payload,
    build_trainer_runtime_smoke_payload,
    build_trainer_contract_payload,
    build_trainer_surface_check_payload,
    create_remote_trainer_client_factory,
    open_weighted_mds_trainer_stream,
    parse_weighted_mds_spec,
    plan_weighted_mds_sources,
    run_trainer_contract_benchmark_cli,
    run_trainer_contract_cli,
    run_trainer_acceptance_fixture_cli,
    run_trainer_distributed_plan_cli,
    run_trainer_distributed_runtime_smoke_cli,
    run_trainer_handoff_acceptance_cli,
    run_trainer_handoff_cli,
    run_trainer_loop_contract_check_cli,
    run_trainer_network_preflight_cli,
    run_trainer_preflight_cli,
    run_trainer_readiness_bundle_cli,
    run_trainer_runtime_smoke_cli,
    run_trainer_integration_smoke_cli,
    run_trainer_loop_replay_cli,
    run_trainer_pipeline_smoke_cli,
    run_trainer_surface_check_cli,
    run_trainer_surface_matrix_cli,
    run_trainer_stream_smoke_cli,
    run_trainer_workload_benchmark_cli,
    run_weighted_mds_plan_cli,
    weighted_mds_plan_fingerprint,
    weighted_mds_sources_from_root,
)


class CxnfsTorchDatasetTest(unittest.TestCase):
    def test_torch_empty_skips_pinned_memory_without_cuda(self):
        class FakeCuda:
            @staticmethod
            def is_available():
                return False

        class FakeTorch:
            cuda = FakeCuda()
            calls = []

            @staticmethod
            def empty(shape, *, dtype, device, pin_memory=False):
                FakeTorch.calls.append({
                    "shape": shape,
                    "dtype": dtype,
                    "device": device,
                    "pin_memory": pin_memory,
                })
                return bytearray(shape[0])

        tensor = tensor_memory_module.torch_empty(
            FakeTorch,
            (8,),
            dtype="uint8",
            device="cpu",
            pin_memory=True,
        )

        self.assertEqual(8, len(tensor))
        self.assertFalse(FakeTorch.calls[0]["pin_memory"])

    def test_decodes_webdataset_samples_for_map_style_reads(self):
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

            dataset = CxnfsTorchDataset(result.output_dir, decoder=MdsSampleFormat.WEBDATASET_TAR)

            self.assertEqual(2, len(dataset))
            self.assertEqual(
                {"000001.jpg": b"image-one", "000001.txt": b"caption one"},
                dataset[0],
            )
            self.assertEqual({"000002.jpg": b"image-two"}, dataset[1])

    def test_applies_transform_and_can_return_metadata(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sample.bin").write_bytes(b"abc")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")

            dataset = CxnfsTorchDataset(
                result.output_dir,
                transform=lambda sample: sample.data.upper(),
                include_metadata=True,
            )

            item = dataset[0]
            self.assertEqual(b"ABC", item["data"])
            self.assertEqual("sample.bin", item["path"])
            self.assertEqual(0, item["global_index"])
            self.assertEqual("files-000000.mds", item["shard_name"])

    def test_reads_batch_in_requested_order(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            (root / "c.bin").write_bytes(b"gamma")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = CxnfsTorchDataset(result.output_dir, transform=lambda sample: sample.data.upper())

            batch = dataset.read_batch((2, 0), max_workers=2)

            self.assertEqual((b"GAMMA", b"ALPHA"), batch)

    def test_can_fill_caller_owned_buffers_from_planned_batch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = CxnfsTorchDataset(result.output_dir)
            batch = dataset.plan_batches(2)[0]
            first = bytearray(5)
            second = bytearray(4)

            samples = dataset.read_data_into_buffers(
                batch,
                (first, second),
                max_workers=2,
                include_paths=True,
            )

            self.assertEqual(b"alpha", bytes(first))
            self.assertEqual(b"beta", bytes(second))
            self.assertEqual(("a.bin", "b.bin"), tuple(sample.path for sample in samples))
            self.assertEqual((5, 4), tuple(sample.data_length for sample in samples))

    def test_plans_prefetches_and_reads_planned_batch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("a.bin", "b.bin", "c.bin"):
                (root / name).write_bytes(name.encode("utf-8"))
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = CxnfsTorchDataset(
                result.output_dir,
                transform=lambda sample: sample.data,
                cache_bytes=4096,
            )

            batches = dataset.plan_batches(2)
            prefetch = dataset.prefetch_batch(batches[0], max_workers=1)
            batch = dataset.read_batch(batches[0], max_workers=1)

            self.assertEqual((0, 1), batches[0].indices)
            self.assertEqual(2, prefetch.sample_count)
            self.assertEqual((b"a.bin", b"b.bin"), batch)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow is required for parquet conversion")
    def test_decodes_parquet_json_rows(self):
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

            dataset = CxnfsTorchDataset(result.output_dir, decoder="parquet-json")

            self.assertEqual({"id": 1, "text": "alpha"}, dataset[0])
            self.assertEqual({"id": 2, "text": "beta"}, dataset[1])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for DataLoader smoke test")
    def test_works_with_torch_dataloader(self):
        from torch.utils.data import DataLoader

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.bin").write_bytes(b"alpha")
            (root / "b.bin").write_bytes(b"beta")
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "files", "files")
            result = DatasetConverter().execute(plan, root / "mds")
            dataset = CxnfsTorchDataset(
                result.output_dir,
                transform=lambda sample: sample.data,
            )

            batches = list(DataLoader(dataset, batch_size=2, collate_fn=lambda items: items))

            self.assertEqual([[b"alpha", b"beta"]], batches)

    def test_weighted_mds_sources_stream_without_duplicate_directories(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))

            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                include_source_metadata=True,
            )

            items = list(itertools.islice(dataset, 6))

            self.assertEqual(["chat", "chat", "summary", "chat", "chat", "summary"], [item.source_name for item in items])
            self.assertEqual(["chat-0", "chat-1", "summary-0", "chat-2", "chat-0", "summary-1"], [item.data for item in items])

    def test_weighted_mds_dataset_drops_runtime_lock_for_worker_pickle(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False, prefetch_chunks=2)
            next(dataset.iter_trainer_batches(1))

            restored = pickle.loads(pickle.dumps(dataset))

            self.assertIsInstance(restored, CxnfsWeightedTorchDataset)
            self.assertEqual(dataset.prefetch_admission_evidence(), restored.prefetch_admission_evidence())
            self.assertEqual(["chat-0"], [sample.data.data.decode("utf-8") for sample in next(restored.iter_trainer_batches(1)).samples])

    def test_weighted_mds_trainer_batches_preserve_metadata_and_sizes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")
            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
            )

            batches = list(dataset.iter_trainer_batches(4, epoch=2, samples_per_epoch=5))

            self.assertEqual(2, len(batches))
            self.assertIsInstance(batches[0], CxnfsTrainerBatch)
            self.assertEqual(0, batches[0].batch_index)
            self.assertEqual(2, batches[0].epoch)
            self.assertEqual(4, batches[0].sample_count)
            self.assertEqual({"chat": 3, "summary": 1}, batches[0].source_counts)
            self.assertEqual(["chat", "chat", "summary", "chat"], [sample.source_name for sample in batches[0].samples])
            self.assertEqual([0, 1, 0, 2], [sample.sample_index for sample in batches[0].samples])
            self.assertEqual(sum(len(sample.data.data) for sample in batches[0].samples), batches[0].byte_size)
            self.assertIsInstance(batches[0].samples[0], CxnfsTrainerBatchSample)
            self.assertEqual(1, batches[1].sample_count)
            self.assertEqual({"summary": 1}, batches[1].source_counts)

    def test_weighted_mds_trainer_batches_can_drop_partial_batch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            sources = weighted_mds_sources_from_root(root / "mds", {"chat": 1})
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)

            batches = list(dataset.iter_trainer_batches(2, samples_per_epoch=3, drop_last=True))

            self.assertEqual(1, len(batches))
            self.assertEqual(2, batches[0].sample_count)
            self.assertEqual([0, 1], [sample.sample_index for sample in batches[0].samples])

    def test_weighted_mds_trainer_cursor_resumes_mid_schedule(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=False,
                prefetch_chunks=2,
            )

            first = next(dataset.iter_trainer_batches(2, samples_per_epoch=6))
            cursor = first.resume_cursor
            resumed = next(dataset.iter_trainer_batches(2, cursor=cursor))

            self.assertIsInstance(cursor, CxnfsTrainerCursor)
            self.assertEqual(TRAINER_CURSOR_FORMAT, cursor.to_dict()["format"])
            self.assertEqual(2, cursor.emitted)
            self.assertEqual(2, cursor.schedule_position)
            self.assertEqual([2, 0], list(cursor.cursors))
            self.assertEqual(["chat-0", "chat-1"], [sample.data for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [sample.data for sample in resumed.samples])
            self.assertEqual(4, resumed.resume_cursor.emitted)
            self.assertEqual(("chat", "summary"), resumed.resume_cursor.source_names)

    def test_weighted_mds_trainer_cursor_advances_across_schedule_boundary(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False, samples_per_epoch=3)
            cursor = dataset.initial_trainer_cursor()

            first = dataset.advance_trainer_cursor(cursor, 1)
            second = dataset.advance_trainer_cursor(first, 1)
            third = dataset.advance_trainer_cursor(second, 1)

            self.assertEqual(1, first.emitted)
            self.assertEqual(2, second.emitted)
            self.assertEqual(3, third.emitted)
            self.assertEqual(0, third.schedule_position)

    def test_weighted_mds_trainer_cursor_rejects_wrong_sources(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            chat_sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            mixed_sources = weighted_mds_sources_from_root(root / "mds", "chat=1,summary=1")
            chat_dataset = CxnfsWeightedTorchDataset(chat_sources)
            mixed_dataset = CxnfsWeightedTorchDataset(mixed_sources)
            cursor = next(chat_dataset.iter_trainer_batches(1)).resume_cursor

            with self.assertRaisesRegex(ValueError, "source count"):
                next(mixed_dataset.iter_trainer_batches(1, cursor=cursor))

    def test_weighted_mds_trainer_direct_batches_fill_factory_destinations(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = (
                CxnfsWeightedSource(
                    "chat",
                    MdsDataset(
                        root / "mds" / "chat",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "chat")),
                    ),
                    weight=2,
                ),
                CxnfsWeightedSource(
                    "summary",
                    MdsDataset(
                        root / "mds" / "summary",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "summary")),
                    ),
                    weight=1,
                ),
            )
            allocations = []

            def destination_factory(source_name, source_index, sample_index, data_length):
                buffer = bytearray(data_length)
                allocations.append((source_name, source_index, sample_index, data_length, buffer))
                return buffer

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )

            batches = list(dataset.iter_trainer_direct_batches(
                3,
                destination_factory=destination_factory,
                include_paths=True,
            ))

            self.assertEqual(2, len(batches))
            self.assertEqual(3, batches[0].sample_count)
            self.assertEqual({"chat": 2, "summary": 1}, batches[0].source_counts)
            self.assertIsInstance(batches[0].samples[0], CxnfsTrainerDirectBatchSample)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in batches[0].samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [bytes(sample.data) for sample in batches[0].samples])
            self.assertEqual(["0000.txt", "0001.txt", "0000.txt"], [sample.path for sample in batches[0].samples])
            self.assertEqual(sum(sample.byte_size for sample in batches[0].samples), batches[0].byte_size)
            self.assertEqual(4, len(allocations))

    def test_weighted_mds_trainer_read_batches_select_direct_buffer_mode(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = (
                CxnfsWeightedSource(
                    "chat",
                    MdsDataset(
                        root / "mds" / "chat",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "chat")),
                    ),
                    weight=2,
                ),
                CxnfsWeightedSource(
                    "summary",
                    MdsDataset(
                        root / "mds" / "summary",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "summary")),
                    ),
                    weight=1,
                ),
            )
            allocations = []

            def destination_factory(source_name, source_index, sample_index, data_length):
                buffer = bytearray(data_length)
                allocations.append((source_name, source_index, sample_index, data_length, buffer))
                return buffer

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )

            batches = list(dataset.iter_trainer_read_batches(
                3,
                read_mode="direct-buffers",
                destination_factory=destination_factory,
                include_paths=True,
            ))

            self.assertEqual(2, len(batches))
            self.assertEqual(3, batches[0].sample_count)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in batches[0].samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [bytes(sample.data) for sample in batches[0].samples])
            self.assertEqual(["0000.txt", "0001.txt", "0000.txt"], [sample.path for sample in batches[0].samples])
            self.assertEqual({"chat": 2, "summary": 1}, batches[0].source_counts)
            self.assertEqual(4, len(allocations))

    def test_weighted_mds_read_path_evidence_reports_descriptor_cache_reuse(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=True,
                rounds_per_read=1,
                samples_per_epoch=2,
            )

            batches = list(dataset.iter_trainer_read_batches(
                1,
                read_mode="direct-buffers",
                include_paths=True,
            ))
            evidence = torch_dataset_module._weighted_dataset_read_path_evidence(
                dataset,
                torch_dataset_module.TrainerReadMode.DIRECT_BUFFERS,
            )

            self.assertEqual(2, len(batches))
            self.assertEqual([b"chat-0", b"chat-0"], [bytes(batch.samples[0].data) for batch in batches])
            self.assertTrue(evidence["mds_descriptor_cache_observed"])
            self.assertEqual(1, evidence["mds_descriptor_cache_source_count"])
            self.assertEqual(1, evidence["mds_sample_header_cache_entries"])
            self.assertEqual(1, evidence["mds_sample_header_cache_misses"])
            self.assertEqual(1, evidence["mds_sample_header_cache_hits"])
            self.assertTrue(evidence["mds_sample_header_cache_reuse_observed"])
            self.assertEqual(1, evidence["mds_sample_path_cache_entries"])
            self.assertEqual(1, evidence["mds_sample_path_cache_misses"])
            self.assertEqual(1, evidence["mds_sample_path_cache_hits"])
            self.assertTrue(evidence["mds_sample_path_cache_reuse_observed"])
            self.assertEqual(
                "cxnfs.mds-descriptor-cache.v1",
                evidence["sources"][0]["mds_descriptor_cache"]["format"],
            )

    def test_weighted_mds_trainer_read_batches_select_direct_tensor_mode(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = (
                CxnfsWeightedSource(
                    "chat",
                    MdsDataset(
                        root / "mds" / "chat",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "chat")),
                    ),
                    weight=2,
                ),
                CxnfsWeightedSource(
                    "summary",
                    MdsDataset(
                        root / "mds" / "summary",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "summary")),
                    ),
                    weight=1,
                ),
            )
            allocations = []

            def tensor_factory(source_name, source_index, sample_index, data_length):
                tensor = FakeTensor(data_length)
                allocations.append((source_name, source_index, sample_index, data_length, tensor))
                return tensor

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )

            batches = list(dataset.iter_trainer_read_batches(
                3,
                read_mode="direct-tensors",
                tensor_factory=tensor_factory,
                include_paths=True,
                non_blocking=False,
            ))

            self.assertEqual(2, len(batches))
            self.assertEqual(3, batches[0].sample_count)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in batches[0].samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [bytes(sample.data) for sample in batches[0].samples])
            self.assertEqual(["0000.txt", "0001.txt", "0000.txt"], [sample.path for sample in batches[0].samples])
            self.assertEqual({"chat": 2, "summary": 1}, batches[0].source_counts)
            self.assertEqual(4, len(allocations))

    def test_weighted_mds_trainer_direct_tensor_batches_fill_factory_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = (
                CxnfsWeightedSource(
                    "chat",
                    MdsDataset(
                        root / "mds" / "chat",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "chat")),
                    ),
                    weight=2,
                ),
                CxnfsWeightedSource(
                    "summary",
                    MdsDataset(
                        root / "mds" / "summary",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "summary")),
                    ),
                    weight=1,
                ),
            )
            allocations = []

            def tensor_factory(source_name, source_index, sample_index, data_length):
                tensor = FakeTensor(data_length)
                allocations.append((source_name, source_index, sample_index, data_length, tensor))
                return tensor

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )

            batches = list(dataset.iter_trainer_direct_tensor_batches(
                3,
                tensor_factory=tensor_factory,
                include_paths=True,
                non_blocking=False,
            ))

            self.assertEqual(2, len(batches))
            self.assertEqual(3, batches[0].sample_count)
            self.assertEqual({"chat": 2, "summary": 1}, batches[0].source_counts)
            self.assertIsInstance(batches[0].samples[0], CxnfsTrainerDirectBatchSample)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in batches[0].samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [bytes(sample.data) for sample in batches[0].samples])
            self.assertEqual(["0000.txt", "0001.txt", "0000.txt"], [sample.path for sample in batches[0].samples])
            self.assertEqual(sum(sample.byte_size for sample in batches[0].samples), batches[0].byte_size)
            self.assertEqual(4, len(allocations))

    def test_weighted_mds_trainer_pipeline_tensor_batches_fill_factory_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")
            allocations = []

            def tensor_factory(source_name, source_index, sample_index, data_length):
                tensor = FakeTensor(data_length)
                allocations.append((source_name, source_index, sample_index, data_length, tensor))
                return tensor

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )
            pipeline_client = PipelineRecordingClient(root / "mds")
            pipeline = CxnfsTrainingPipeline(
                pipeline_client,
                CxnfsPipelineConfig(network_read_workers=2, max_cords=4),
            )

            batches = list(dataset.iter_trainer_pipeline_tensor_batches(
                3,
                pipeline,
                tensor_factory=tensor_factory,
                include_paths=True,
                non_blocking=False,
            ))

            self.assertEqual(2, len(batches))
            self.assertIsInstance(batches[0].samples[0], CxnfsTrainerPipelineBatchSample)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in batches[0].samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [bytes(sample.data) for sample in batches[0].samples])
            self.assertEqual(["0000.txt", "0001.txt", "0000.txt"], [sample.path for sample in batches[0].samples])
            self.assertEqual(["/chat/chat-000000.mds", "/chat/chat-000000.mds", "/summary/summary-000000.mds"], [
                sample.read.shard_path for sample in batches[0].samples
            ])
            self.assertEqual([False, False, False, False], [read[-1] for read in pipeline_client.tensor_reads])
            self.assertEqual(4, len(allocations))

    def test_weighted_mds_trainer_pipeline_stream_prefetches_through_training_pipeline(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")

            def tensor_factory(source_name, source_index, sample_index, data_length):
                return FakeTensor(data_length)

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )
            pipeline_client = PipelineRecordingClient(root / "mds")
            pipeline = CxnfsTrainingPipeline(
                pipeline_client,
                CxnfsPipelineConfig(network_read_workers=2, max_cords=4, prefetch_batches=2),
            )
            stream = CxnfsTrainerPipelineStream(
                dataset,
                3,
                tensor_factory=tensor_factory,
                include_paths=True,
            )

            with pipeline.prefetch_trainer_stream(stream, max_prefetch_batches=2, non_blocking=False) as prefetched:
                first = next(prefetched)
                second = next(prefetched)
                with self.assertRaises(StopIteration):
                    next(prefetched)

            self.assertEqual(3, first.sample_count)
            self.assertEqual(1, second.sample_count)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in first.batch.samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [
                bytes(sample.data)
                for sample in first.batch.samples
            ])
            self.assertEqual([b"chat-2"], [bytes(sample.data) for sample in second.batch.samples])
            self.assertEqual(4, stream.cursor.emitted)
            self.assertEqual([False, False, False, False], [read[-1] for read in pipeline_client.tensor_reads])

    def test_trainer_pipeline_stream_loads_state_for_direct_tensor_resume(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)
            stream = CxnfsTrainerPipelineStream(
                dataset,
                2,
                tensor_factory=lambda _source, _source_index, _sample_index, nbytes: FakeTensor(nbytes),
                include_paths=True,
                source_path_prefixes={"chat": "/remote/chat", "summary": "/remote/summary"},
            )
            first = stream.next_batch()
            state = stream.state_dict()

            resumed = CxnfsTrainerPipelineStream(
                dataset,
                2,
                tensor_factory=lambda _source, _source_index, _sample_index, nbytes: FakeTensor(nbytes),
                include_paths=True,
                source_path_prefixes={"chat": "/remote/chat", "summary": "/remote/summary"},
            )
            loaded_cursor = resumed.load_state_dict(state)
            second = resumed.next_batch()

            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state["format"])
            self.assertEqual("direct-tensors", state["read_mode"])
            self.assertEqual("pipeline-descriptor-stream", state["trainer_batch_iterator"])
            self.assertEqual(2, state["cursor"]["emitted"])
            self.assertEqual(2, loaded_cursor.emitted)
            self.assertEqual(["chat", "chat"], [sample.source_name for sample in first.samples])
            self.assertEqual(["summary", "chat"], [sample.source_name for sample in second.samples])
            self.assertEqual(["/remote/chat/chat-000000.mds", "/remote/chat/chat-000000.mds"], [
                sample.read.shard_path for sample in first.samples
            ])
            self.assertEqual(["/remote/summary/summary-000000.mds", "/remote/chat/chat-000000.mds"], [
                sample.read.shard_path for sample in second.samples
            ])

    def test_trainer_pipeline_stream_saves_and_loads_state_file_atomically(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "reports/pipeline-state.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False, samples_per_epoch=6)
            stream = CxnfsTrainerPipelineStream(
                dataset,
                2,
                tensor_factory=lambda _source, _source_index, _sample_index, nbytes: FakeTensor(nbytes),
                include_paths=True,
                source_path_prefixes={"chat": Path("/remote/chat"), "summary": "/remote/summary"},
            )
            first = stream.next_batch()

            saved_path = stream.save_state_file(state_path)
            saved_state = json.loads(state_path.read_text(encoding="utf-8"))
            resumed = CxnfsTrainerPipelineStream(
                dataset,
                2,
                tensor_factory=lambda _source, _source_index, _sample_index, nbytes: FakeTensor(nbytes),
                include_paths=True,
                source_path_prefixes={"chat": "/remote/chat", "summary": "/remote/summary"},
            )
            loaded_cursor = resumed.load_state_file(saved_path)
            second = resumed.next_batch()

            self.assertEqual(state_path, saved_path)
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, saved_state["format"])
            self.assertEqual("pipeline-descriptor-stream", saved_state["trainer_batch_iterator"])
            self.assertEqual("/remote/chat", saved_state["source_path_prefixes"]["chat"])
            self.assertEqual(2, saved_state["cursor"]["emitted"])
            self.assertEqual(6, saved_state["samples_per_epoch"])
            self.assertEqual(2, loaded_cursor.emitted)
            self.assertEqual(["chat", "chat"], [sample.source_name for sample in first.samples])
            self.assertEqual(["summary", "chat"], [sample.source_name for sample in second.samples])
            self.assertFalse(tuple((root / "reports").glob(".pipeline-state.json.tmp-*")))

    def test_trainer_pipeline_stream_rejects_mismatched_state_contract(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)
            stream = CxnfsTrainerPipelineStream(
                dataset,
                1,
                tensor_factory=lambda _source, _source_index, _sample_index, nbytes: FakeTensor(nbytes),
                source_path_prefixes={"chat": "/remote/chat"},
            )
            stream.next_batch()
            state = stream.state_dict()

            wrong_iterator = dict(state)
            wrong_iterator["trainer_batch_iterator"] = "native"
            with self.assertRaisesRegex(ValueError, "trainer_batch_iterator"):
                stream.load_state_dict(wrong_iterator)

            wrong_prefix = dict(state)
            wrong_prefix["source_path_prefixes"] = {"chat": "/other/chat"}
            with self.assertRaisesRegex(ValueError, "source_path_prefixes"):
                stream.load_state_dict(wrong_prefix)

    def test_trainer_pipeline_smoke_cli_reports_pipeline_tensor_path(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            with patch.object(torch_dataset_module, "_require_torch", lambda: object()), \
                patch.object(torch_dataset_module, "_torch_dtype", lambda _torch, _dtype: "uint8"), \
                patch.object(torch_dataset_module, "_dtype_nbytes", lambda _dtype: 1), \
                patch.object(torch_dataset_module, "_torch_empty", lambda _torch, shape, **_kwargs: FakeTensor(shape[0])):
                exit_code = run_trainer_pipeline_smoke_cli(
                    [
                        str(root / "mds"),
                        "--weights",
                        "chat=2,summary=1",
                        "--batch-size",
                        "2",
                        "--batches",
                        "2",
                        "--samples-per-epoch",
                        "3",
                        "--pipeline-workers",
                        "2",
                        "--pipeline-prefetch-batches",
                        "2",
                        "--max-cords",
                        "4",
                        "--write-probe-bytes",
                        "33",
                        "--write-probe-frame-bytes",
                        "8",
                    ],
                    stdout=stdout,
                    stderr=stderr,
                )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_PIPELINE_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual("direct-tensors", payload["read_mode"])
            self.assertEqual("CxnfsTrainingPipeline", payload["training_pipeline_object"])
            self.assertEqual("CxnfsTrainingPipeline.prefetch_trainer_stream", payload["training_pipeline_context"])
            self.assertEqual("CxnfsTrainerPipelineStream", payload["pipeline_descriptor_stream"])
            self.assertTrue(payload["pipeline_prefetch_stream_used"])
            self.assertEqual(2, payload["pipeline_prefetch_batches"])
            self.assertEqual("CxnfsPipelinePrefetchedTensorBatch", payload["pipeline_prefetch_result_object"])
            self.assertEqual(3, payload["sample_count"])
            self.assertEqual(21, payload["byte_size"])
            self.assertTrue(payload["read_path_evidence"]["training_pipeline_used"])
            self.assertTrue(payload["read_path_evidence"]["pipeline_prefetch_stream_used"])
            self.assertEqual(2, payload["read_path_evidence"]["pipeline_prefetch_batches"])
            self.assertTrue(payload["read_path_evidence"]["pipeline_descriptor_planning_used"])
            self.assertTrue(payload["read_path_evidence"]["pipeline_python_byte_batch_avoided"])
            self.assertTrue(payload["tensor_memory_path"]["complete"])
            self.assertTrue(payload["pipeline_network_path"]["complete"])
            self.assertEqual("cxnfs.trainer-pipeline-network-path.v1", payload["pipeline_network_path_format"])
            self.assertEqual(2, payload["pipeline_network_path"]["network_read_workers"])
            self.assertEqual(4, payload["pipeline_network_path"]["max_cords"])
            self.assertTrue(payload["pipeline_network_path"]["network_fanout_configured"])
            self.assertTrue(payload["pipeline_network_path"]["python_byte_batch_avoided"])
            self.assertTrue(payload["read_path_reliability"]["complete"])
            self.assertEqual("/chat/chat-000000.mds", payload["batches"][0]["samples"][0]["read"]["shard_path"])
            self.assertTrue(payload["write_probe"]["enabled"])
            self.assertTrue(payload["write_probe"]["verified_read_back"])
            self.assertEqual(33, payload["write_probe"]["byte_size"])
            self.assertEqual(8, payload["write_probe"]["frame_bytes"])
            self.assertEqual(1, payload["write_probe"]["file_count"])
            self.assertEqual(1, payload["write_probe"]["tensor_file_count"])
            self.assertEqual(33, payload["write_probe"]["bytes_sent"])
            self.assertTrue(payload["write_probe"]["path"].startswith("__cxnfs-pipeline-write-probe/"))
            self.assertTrue((root / "mds" / payload["write_probe"]["path"]).is_file())

    def test_trainer_pipeline_smoke_cli_reports_remote_pipeline_path_with_prompted_credentials(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()
            controls = []
            bulks = []
            prompted_credentials = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                prompted_credentials.append(credentials)
                return credentials

            with patch.object(torch_dataset_module, "_require_torch", lambda: object()), \
                patch.object(torch_dataset_module, "_torch_dtype", lambda _torch, _dtype: "uint8"), \
                patch.object(torch_dataset_module, "_dtype_nbytes", lambda _dtype: 1), \
                patch.object(torch_dataset_module, "_torch_empty", lambda _torch, shape, **_kwargs: FakeTensor(shape[0])):
                exit_code = run_trainer_pipeline_smoke_cli(
                    [
                        str(root / "mds"),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "cxnfs.example.internal",
                        "--control-port",
                        "1443",
                        "--transport-port",
                        "1444",
                        "--batch-size",
                        "2",
                        "--batches",
                        "2",
                        "--samples-per-epoch",
                        "3",
                        "--pipeline-workers",
                        "2",
                        "--pipeline-prefetch-batches",
                        "2",
                        "--max-cords",
                        "4",
                        "--write-probe-bytes",
                        "33",
                        "--write-probe-frame-bytes",
                        "8",
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=credential_prompt,
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=no_tls_context,
                )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_PIPELINE_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual("/stage0-mds", payload["remote_root"])
            self.assertEqual("cxnfs-length-prefixed", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertEqual(1, len(prompted_credentials))
            self.assertTrue(prompted_credentials[0].closed)
            self.assertGreaterEqual(len(controls), 3)
            self.assertEqual(len(controls), len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertTrue(payload["remote_client_warmup"]["complete"])
            self.assertEqual(["chat", "summary"], payload["remote_client_warmup"]["warmed_source_names"])
            self.assertTrue(payload["remote_client_topology"]["per_source_client_observed"])
            self.assertEqual(["chat", "summary"], payload["remote_client_topology"]["unique_source_request_names"])
            self.assertTrue(payload["remote_client_topology_after_close"]["all_open_clients_closed"])
            self.assertTrue(payload["remote_client_lifecycle_closed"])
            self.assertTrue(payload["read_path_evidence"]["training_pipeline_used"])
            self.assertTrue(payload["read_path_evidence"]["pipeline_python_byte_batch_avoided"])
            self.assertGreaterEqual(payload["read_path_evidence"]["direct_tensor_read_operations"], 3)
            self.assertGreaterEqual(payload["read_path_evidence"]["pipeline_payload_transport_count"], 1)
            self.assertTrue(payload["tensor_memory_path"]["complete"])
            self.assertTrue(payload["pipeline_network_path"]["complete"])
            self.assertEqual(2, payload["pipeline_network_path"]["network_read_workers"])
            self.assertTrue(payload["pipeline_network_path"]["network_fanout_configured"])
            self.assertTrue(payload["pipeline_network_path"]["python_byte_batch_avoided"])
            self.assertTrue(payload["write_probe"]["enabled"])
            self.assertTrue(payload["write_probe"]["verified_read_back"])
            self.assertEqual(33, payload["write_probe"]["bytes_sent"])
            payload_bulks = [bulk for bulk in bulks if bulk.direct_tensor_read_operations > 0]
            write_bulks = [bulk for bulk in bulks if bulk.write_paths]
            self.assertEqual(1, len(payload_bulks))
            self.assertGreaterEqual(payload_bulks[0].direct_tensor_read_operations, 3)
            self.assertEqual(1, len(write_bulks))
            self.assertTrue(write_bulks[0].write_paths[0].startswith("/stage0-mds/__cxnfs-pipeline-write-probe/"))

    def test_trainer_pipeline_source_path_prefixes_follow_remote_root(self):
        sources = (
            CxnfsWeightedSource("chat", "unused-chat", weight=2),
            CxnfsWeightedSource("summary", "unused-summary", weight=1),
        )

        prefixes = torch_dataset_module._trainer_pipeline_source_path_prefixes(
            sources,
            {"remote_root": "/stage0-mds"},
        )

        self.assertEqual({
            "chat": "/stage0-mds/chat",
            "summary": "/stage0-mds/summary",
        }, prefixes)
        self.assertIsNone(torch_dataset_module._trainer_pipeline_source_path_prefixes(sources, {}))

    def test_trainer_stream_keeps_state_across_weighted_direct_batches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = (
                CxnfsWeightedSource(
                    "chat",
                    MdsDataset(
                        root / "mds" / "chat",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "chat")),
                    ),
                    weight=2,
                ),
                CxnfsWeightedSource(
                    "summary",
                    MdsDataset(
                        root / "mds" / "summary",
                        client=CxnfsPythonClient(LocalFileTransport(root / "mds" / "summary")),
                    ),
                    weight=1,
                ),
            )
            allocations = []

            def destination_factory(source_name, source_index, sample_index, data_length):
                buffer = bytearray(data_length)
                allocations.append((source_name, source_index, sample_index, data_length))
                return buffer

            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )
            stream = CxnfsTrainerStream(
                dataset,
                2,
                read_mode="direct-buffers",
                destination_factory=destination_factory,
                include_paths=True,
            )

            first = stream.next_batch()
            second = stream.next_batch()
            state = stream.state_dict()

            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state["format"])
            self.assertEqual(4, state["cursor"]["emitted"])
            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [bytes(sample.data).decode("utf-8") for sample in second.samples])
            self.assertEqual(["0000.txt", "0001.txt"], [sample.path for sample in first.samples])
            self.assertEqual(["0000.txt", "0002.txt"], [sample.path for sample in second.samples])
            self.assertEqual(("chat", "summary"), stream.cursor.source_names)
            self.assertGreaterEqual(len(allocations), 4)

    def test_trainer_stream_loads_state_for_later_trainer_resume(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)
            stream = CxnfsTrainerStream(dataset, 2, read_mode="samples")
            first = stream.next_batch()
            state = stream.state_dict()

            resumed = CxnfsTrainerStream(dataset, 2, read_mode="samples")
            loaded_cursor = resumed.load_state_dict(state)
            second = resumed.next_batch()

            self.assertEqual(["chat-0", "chat-1"], [sample.data for sample in first.samples])
            self.assertEqual(2, loaded_cursor.emitted)
            self.assertEqual(["summary-0", "chat-2"], [sample.data for sample in second.samples])
            self.assertEqual(4, resumed.cursor.emitted)

    def test_trainer_stream_saves_and_loads_state_file_atomically(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "reports/trainer-state.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False, samples_per_epoch=6)
            stream = CxnfsTrainerStream(dataset, 2, read_mode="samples")
            first = stream.next_batch()

            saved_path = stream.save_state_file(state_path)
            saved_state = json.loads(state_path.read_text(encoding="utf-8"))
            resumed = CxnfsTrainerStream(dataset, 2, read_mode="samples", cursor=saved_state["cursor"])
            loaded_cursor = resumed.load_state_file(saved_path)
            second = resumed.next_batch()

            self.assertEqual(state_path, saved_path)
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, saved_state["format"])
            self.assertEqual(2, saved_state["cursor"]["emitted"])
            self.assertEqual(6, saved_state["samples_per_epoch"])
            self.assertEqual(2, loaded_cursor.emitted)
            self.assertEqual(["chat-0", "chat-1"], [sample.data for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [sample.data for sample in second.samples])
            self.assertFalse(tuple((root / "reports").glob(".trainer-state.json.tmp-*")))

    def test_trainer_stream_accepts_resume_cursor_at_construction(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)
            first_stream = CxnfsTrainerStream(dataset, 2, read_mode="samples")
            first = first_stream.next_batch()

            resumed = CxnfsTrainerStream(
                dataset,
                2,
                read_mode="samples",
                cursor=first_stream.cursor,
            )
            self.assertEqual(2, resumed.cursor.emitted)
            second = resumed.next_batch()

            self.assertEqual(["chat-0", "chat-1"], [sample.data for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [sample.data for sample in second.samples])
            self.assertEqual(4, resumed.cursor.emitted)

    def test_open_weighted_mds_trainer_stream_builds_stateful_direct_stream(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=2,summary=1",
                batch_size=2,
                read_mode="direct-buffers",
                rounds_per_read=2,
                source_read_workers=2,
                prefetch_chunks=2,
                samples_per_epoch=4,
                include_paths=True,
            ) as stream:
                first = stream.next_batch()
                state = stream.state_dict()

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=2,summary=1",
                batch_size=2,
                read_mode="direct-buffers",
                cursor=state["cursor"],
                include_paths=True,
            ) as resumed:
                second = resumed.next_batch()

            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state["format"])
            self.assertEqual(2, state["cursor"]["emitted"])
            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [bytes(sample.data).decode("utf-8") for sample in second.samples])
            self.assertEqual(["0000.txt", "0001.txt"], [sample.path for sample in first.samples])
            self.assertEqual(["0000.txt", "0002.txt"], [sample.path for sample in second.samples])

    def test_trainer_stream_is_plain_iterable_and_updates_cursor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=2,summary=1",
                batch_size=2,
                read_mode="samples",
                samples_per_epoch=4,
            ) as stream:
                batches = list(stream)
                state = stream.state_dict()

            self.assertEqual(2, len(batches))
            self.assertEqual(["chat-0", "chat-1"], [sample.data.data.decode("utf-8") for sample in batches[0].samples])
            self.assertEqual(["summary-0", "chat-2"], [sample.data.data.decode("utf-8") for sample in batches[1].samples])
            self.assertEqual(4, stream.cursor.emitted)
            self.assertEqual(4, state["cursor"]["emitted"])

    def test_trainer_stream_resets_iterator_after_failed_batch_read(self):
        source = FailingOnceTorchDataset(("sample-0", "sample-1"))
        dataset = CxnfsWeightedTorchDataset(
            (CxnfsWeightedSource("chat", source),),
            repeat=False,
        )
        stream = CxnfsTrainerStream(dataset, 1, read_mode="samples")

        with self.assertRaisesRegex(CxnfsTrainerSurfaceError, "transient dataset read failure") as failure:
            stream.next_batch()

        self.assertIsInstance(failure.exception.__cause__, OSError)
        self.assertEqual("chat", failure.exception.context["source_name"])
        self.assertEqual(0, stream.cursor.emitted)
        batch = stream.next_batch()

        self.assertEqual(["sample-0"], [sample.data for sample in batch.samples])
        self.assertEqual(1, stream.cursor.emitted)
        self.assertEqual([(0,), (0,)], source.reads)

    def test_trainer_prefetch_stream_reads_ahead_without_advancing_cursor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=1",
                batch_size=1,
                read_mode="samples",
                samples_per_epoch=3,
                repeat=False,
            ) as stream:
                with stream.prefetch_batches(max_prefetch_batches=2) as prefetch:
                    self.assertIsInstance(prefetch, CxnfsTrainerPrefetchStream)
                    deadline = time.monotonic() + 2.0
                    while prefetch.queued_batches < 2 and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertGreaterEqual(prefetch.queued_batches, 2)
                    self.assertEqual(0, stream.cursor.emitted)

                    first = prefetch.next_batch()
                    self.assertEqual(["chat-0"], [sample.data.data.decode("utf-8") for sample in first.samples])
                    self.assertEqual(1, stream.cursor.emitted)

                    rest = list(prefetch.iter_batches())

            self.assertEqual(
                [["chat-1"], ["chat-2"]],
                [
                    [sample.data.data.decode("utf-8") for sample in batch.samples]
                    for batch in rest
                ],
            )
            self.assertEqual(3, stream.cursor.emitted)

    def test_trainer_prefetch_stream_close_rewinds_unacknowledged_batches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=1",
                batch_size=1,
                read_mode="samples",
                samples_per_epoch=4,
                repeat=False,
            ) as stream:
                prefetch = stream.prefetch_batches(max_prefetch_batches=3)
                try:
                    deadline = time.monotonic() + 2.0
                    while prefetch.queued_batches < 2 and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertGreaterEqual(prefetch.queued_batches, 2)

                    first = prefetch.next_batch()
                    self.assertEqual(["chat-0"], [sample.data.data.decode("utf-8") for sample in first.samples])
                    self.assertEqual(1, stream.cursor.emitted)
                finally:
                    prefetch.close()

                second = stream.next_batch()
                self.assertEqual(["chat-1"], [sample.data.data.decode("utf-8") for sample in second.samples])
                self.assertEqual(2, stream.cursor.emitted)

                rest = list(stream.iter_batches())

            self.assertEqual(
                [["chat-2"], ["chat-3"]],
                [
                    [sample.data.data.decode("utf-8") for sample in batch.samples]
                    for batch in rest
                ],
            )

    def test_trainer_prefetch_stream_rejects_non_positive_prefetch_depth(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=1",
                batch_size=1,
                read_mode="samples",
                repeat=False,
            ) as stream:
                with self.assertRaisesRegex(ValueError, "max_prefetch_batches"):
                    stream.prefetch_batches(max_prefetch_batches=0)

    def test_open_weighted_mds_trainer_stream_uses_remote_source_clients(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            transports = {}

            def client_factory(name):
                transport = RemoteShardTransport(root / "mds" / name)
                transports[name] = transport
                return ClosableDatasetClient(transport)

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=1,summary=1",
                batch_size=2,
                remote_root="/remote/datasets",
                client_factory=client_factory,
                read_mode="direct-buffers",
                repeat=False,
                include_paths=True,
            ) as stream:
                batch = stream.next_batch()

            self.assertEqual(["chat-0", "summary-0"], [bytes(sample.data).decode("utf-8") for sample in batch.samples])
            self.assertEqual(
                ["/remote/datasets/chat/chat-000000.mds"],
                sorted(set(transports["chat"].read_paths + transports["chat"].buffer_read_paths)),
            )
            self.assertEqual(
                ["/remote/datasets/summary/summary-000000.mds"],
                sorted(set(transports["summary"].read_paths + transports["summary"].buffer_read_paths)),
            )

    def test_remote_trainer_client_factory_feeds_weighted_stream(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            client_factory = create_remote_trainer_client_factory(
                ClientCredentials("trainer", "secret"),
                "cxnfs.example.internal",
                4509,
                4510,
                server_hostname="cxnfs.example.internal",
                read_config=CxnfsBulkReadConfig(initial_cords=8, max_cords=64),
                write_connections=4,
                bulk_transport="stream",
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
            )

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=1,summary=1",
                batch_size=2,
                remote_root="/remote/datasets",
                client_factory=client_factory,
                read_mode="direct-buffers",
                repeat=False,
                include_paths=True,
            ) as stream:
                batch = stream.next_batch()

            self.assertEqual(["chat-0", "summary-0"], [bytes(sample.data).decode("utf-8") for sample in batch.samples])
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertEqual(["trainer", "trainer"], [control.username for control in controls])
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual([8, 8], [bulk.options["read_config"].initial_cords for bulk in bulks])
            self.assertEqual([64, 64], [bulk.options["read_config"].max_cords for bulk in bulks])
            self.assertEqual([4, 4], [bulk.options["write_connections"] for bulk in bulks])
            topology = client_factory.connection_topology()
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(["chat", "summary"], topology["unique_client_open_source_names"])
            self.assertEqual(2, topology["client_close_count"])
            self.assertEqual(["chat", "summary"], topology["unique_client_close_source_names"])
            self.assertEqual(0, topology["active_client_count"])
            self.assertTrue(topology["all_open_clients_closed"])
            self.assertEqual(0, topology["factory_close_count"])

    def test_remote_trainer_client_factory_close_closes_active_per_source_clients(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            client_factory = create_remote_trainer_client_factory(
                ClientCredentials("trainer", "secret"),
                "cxnfs.example.internal",
                4509,
                4510,
                server_hostname="cxnfs.example.internal",
                read_config=CxnfsBulkReadConfig(initial_cords=8, max_cords=64),
                write_connections=4,
                bulk_transport="stream",
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
            )

            chat_client = client_factory("chat")
            summary_client = client_factory("summary")

            self.assertIs(chat_client.delegate, bulks[0])
            self.assertIs(summary_client.delegate, bulks[1])
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            topology = client_factory.connection_topology()
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(0, topology["client_close_count"])
            self.assertEqual(2, topology["active_client_count"])
            self.assertEqual(["chat", "summary"], topology["unique_active_client_source_names"])
            self.assertFalse(topology["all_open_clients_closed"])

            client_factory.close()

            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual([1, 1], [control.close_count for control in controls])
            self.assertEqual([1, 1], [bulk.close_count for bulk in bulks])
            topology = client_factory.connection_topology()
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(2, topology["client_close_count"])
            self.assertEqual(0, topology["active_client_count"])
            self.assertTrue(topology["all_open_clients_closed"])
            self.assertTrue(topology["connection_source_set_matches_requests"])
            self.assertTrue(topology["client_open_source_set_matches_requests"])
            self.assertTrue(topology["client_close_source_set_matches_opens"])
            self.assertTrue(topology["client_lifecycle_source_set_complete"])
            self.assertEqual(["chat", "summary"], topology["unique_connection_source_names"])
            self.assertEqual(["chat", "summary"], topology["unique_client_open_source_names"])
            self.assertEqual(["chat", "summary"], topology["unique_client_close_source_names"])
            self.assertEqual(1, topology["factory_close_count"])

            client_factory.close()

            self.assertEqual([1, 1], [control.close_count for control in controls])
            self.assertEqual([1, 1], [bulk.close_count for bulk in bulks])
            self.assertEqual(2, client_factory.connection_topology()["factory_close_count"])

    def test_remote_trainer_client_factory_shared_client_reopens_after_stream_close(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            client_factory = create_remote_trainer_client_factory(
                ClientCredentials("trainer", "secret"),
                "cxnfs.example.internal",
                4509,
                4510,
                server_hostname="cxnfs.example.internal",
                read_config=CxnfsBulkReadConfig(initial_cords=8, max_cords=64),
                shared_client=True,
                bulk_transport="stream",
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
            )

            chat_client = client_factory("chat")
            summary_client = client_factory("summary")

            self.assertIs(chat_client, summary_client)
            self.assertEqual(1, len(controls))
            self.assertEqual(1, len(bulks))
            topology = client_factory.connection_topology()
            self.assertTrue(topology["shared_client"])
            self.assertEqual(2, topology["source_request_count"])
            self.assertEqual(1, topology["connection_count"])
            self.assertEqual(1, topology["client_open_count"])
            self.assertEqual(1, topology["active_client_count"])
            self.assertEqual(1, topology["shared_client_reuse_count"])
            self.assertTrue(topology["shared_client_observed"])

            chat_client.close()
            summary_client.close()

            self.assertTrue(controls[0].closed)
            self.assertTrue(bulks[0].closed)
            self.assertEqual(1, controls[0].close_count)
            self.assertEqual(1, bulks[0].close_count)
            topology = client_factory.connection_topology()
            self.assertEqual(1, topology["client_close_count"])
            self.assertEqual(0, topology["active_client_count"])
            self.assertTrue(topology["all_open_clients_closed"])

            reopened_client = client_factory("summary")

            self.assertIsNot(chat_client, reopened_client)
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            topology = client_factory.connection_topology()
            self.assertEqual(2, topology["connection_count"])
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(1, topology["active_client_count"])
            self.assertTrue(topology["shared_client_observed"])

            client_factory.close()

            self.assertEqual([1, 1], [control.close_count for control in controls])
            self.assertEqual([1, 1], [bulk.close_count for bulk in bulks])
            topology = client_factory.connection_topology()
            self.assertEqual(2, topology["client_close_count"])
            self.assertEqual(0, topology["active_client_count"])
            self.assertTrue(topology["all_open_clients_closed"])

    def test_remote_weighted_sources_defer_client_factory_until_read(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            created = []

            def client_factory(name):
                created.append(name)
                return ClosableDatasetClient(RemoteShardTransport(root / "mds" / name))

            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=1",
                remote_root="/remote/datasets",
                client_factory=client_factory,
            )
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)

            self.assertEqual([], created)
            batch = next(dataset.iter_trainer_direct_batches(1, include_paths=True))
            dataset.close()

            self.assertEqual(["chat"], created)
            self.assertEqual(b"chat-0", bytes(batch.samples[0].data))
            self.assertEqual("0000.txt", batch.samples[0].path)

    def test_lazy_remote_source_client_is_created_once_under_concurrent_reads(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            created = []
            created_clients = []

            def client_factory(name):
                created.append(name)
                time.sleep(0.05)
                client = ClosableDatasetClient(RemoteShardTransport(root / "mds" / name))
                created_clients.append(client)
                return client

            lazy = torch_dataset_module.CxnfsLazyDatasetClient(
                "chat",
                client_factory,
                path_prefix="/remote/datasets/chat",
            )
            start = threading.Barrier(8)

            def open_client():
                start.wait()
                return lazy.client()

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = tuple(executor.submit(open_client) for _ in range(8))
                clients = tuple(future.result() for future in futures)

            self.assertEqual(1, len(set(id(client) for client in clients)))
            self.assertEqual(["chat"], created)
            self.assertEqual(1, len(created_clients))
            lazy.close()
            self.assertEqual(1, created_clients[0].close_count)

    def test_remote_trainer_client_factory_is_pickleable_for_worker_processes(self):
        credentials = ClientCredentials("trainer", "secret")
        try:
            factory = create_remote_trainer_client_factory(
                credentials,
                "cxnfs.example.internal",
                4509,
                4510,
                server_hostname="cxnfs.example.internal",
                read_config=CxnfsBulkReadConfig(initial_cords=8, max_cords=64),
                write_connections=4,
                bulk_transport="stream",
            )
            restored = pickle.loads(pickle.dumps(factory))

            self.assertIsInstance(restored, CxnfsRemoteTrainerClientFactory)
            self.assertIsNone(restored._shared_client)
            self.assertEqual("cxnfs.example.internal", restored.host)
            self.assertEqual("stream", restored.bulk_transport)
            self.assertEqual(64, restored.read_config.max_cords)
            topology = restored.connection_topology()
            self.assertEqual(0, topology["connection_count"])
            self.assertEqual(0, topology["client_open_count"])
            self.assertEqual(0, topology["client_close_count"])
            self.assertEqual(0, topology["active_client_count"])
            self.assertTrue(topology["all_open_clients_closed"])
            restored.credentials.close()
        finally:
            credentials.close()

    def test_remote_trainer_client_factory_drops_live_tls_context_for_worker_pickle(self):
        credentials = ClientCredentials("trainer", "secret")
        try:
            factory = create_remote_trainer_client_factory(
                credentials,
                "cxnfs.example.internal",
                4509,
                4510,
                tls_context=ssl.create_default_context(),
                server_hostname="cxnfs.example.internal",
                read_config=CxnfsBulkReadConfig(initial_cords=8, max_cords=64),
                bulk_transport="stream",
                cafile="/tmp/cxnfs-ca.pem",
            )
            restored = pickle.loads(pickle.dumps(factory))

            self.assertIsInstance(restored, CxnfsRemoteTrainerClientFactory)
            self.assertIsNone(restored.tls_context)
            self.assertEqual("/tmp/cxnfs-ca.pem", restored.cafile)
            restored.credentials.close()
        finally:
            credentials.close()

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for remote trainer DataLoader worker smoke test")
    def test_remote_trainer_dataloader_workers_open_clients_inside_workers(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            log_path = root / "worker-clients.log"
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(6)))
            write_file_dataset(root, "summary", tuple(f"summary-{index}" for index in range(4)))
            factory = PickleableRemoteDatasetClientFactory(root / "mds", log_path)
            pickle.loads(pickle.dumps(factory))

            with open_weighted_mds_trainer_stream(
                root / "mds",
                "chat=2,summary=1",
                batch_size=2,
                remote_root="/remote/datasets",
                client_factory=factory,
                read_mode="direct-buffers",
                trainer_batch_iterator="trainer-dataloader",
                data_loader_workers=2,
                rounds_per_read=2,
                samples_per_epoch=8,
                repeat=False,
                include_paths=True,
            ) as stream:
                batches = list(stream.iter_batches(limit=4))
                state = stream.state_dict()

            entries = [
                line.split()
                for line in log_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            worker_pids = {int(pid) for pid, _ in entries}
            source_names = {name for _, name in entries}
            sample_keys = [
                (sample.source_name, sample.sample_index)
                for batch in batches
                for sample in batch.samples
            ]

            self.assertEqual(4, len(batches))
            self.assertEqual(8, len(sample_keys))
            self.assertEqual(len(sample_keys), len(set(sample_keys)))
            self.assertNotIn(os.getpid(), worker_pids)
            self.assertGreaterEqual(len(worker_pids), 2)
            self.assertEqual({"chat", "summary"}, source_names)
            worker_read_evidence = [
                batch.worker_read_path_evidence
                for batch in batches
                if isinstance(batch.worker_read_path_evidence, dict)
            ]
            self.assertEqual(4, len(worker_read_evidence))
            self.assertTrue(all(report["produced_inside_torch_worker"] for report in worker_read_evidence))
            self.assertEqual({2}, {report["worker_count"] for report in worker_read_evidence})
            self.assertGreaterEqual(
                sum(report["direct_buffer_read_operations"] for report in worker_read_evidence),
                8,
            )
            self.assertTrue(state["worker_partitioning_enabled"])
            self.assertEqual(2, state["worker_cursor_count"])

    def test_trainer_remote_factory_passes_quic_write_retry_config(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = ClientCredentials("trainer", "secret")
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root, control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            factory = torch_dataset_module._trainer_stream_remote_client_factory(
                credentials,
                "cxnfs.example.internal",
                4509,
                4510,
                5.0,
                None,
                "cxnfs.example.internal",
                CxnfsBulkReadConfig(initial_cords=4, max_cords=8),
                2,
                1,
                3,
                False,
                "quic",
                "/tmp/cxnfs-ca.pem",
                control_factory,
                bulk_factory,
                no_tls_context,
            )

            client = factory("chat")

            self.assertEqual(1, len(controls))
            self.assertEqual(1, len(bulks))
            self.assertTrue(controls[0].authenticated)
            self.assertEqual(
                CxnfsBulkRetryConfig(read_retries=1, write_retries=3),
                bulks[0].options["retry_config"],
            )
            self.assertEqual("/tmp/cxnfs-ca.pem", bulks[0].options["cafile"])
            self.assertEqual(2, bulks[0].options["write_connections"])
            client.close()
            credentials.close()

    def test_trainer_stream_rejects_mismatched_state_contract(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)
            stream = CxnfsTrainerStream(dataset, 1, read_mode="samples")
            state = stream.state_dict()
            state["read_mode"] = "direct-buffers"

            with self.assertRaisesRegex(ValueError, "read_mode"):
                stream.load_state_dict(state)

    def test_weighted_mds_trainer_batches_prefetch_future_chunks(self):
        source = PrefetchBlockingTorchDataset(("sample-0", "sample-1"))
        dataset = CxnfsWeightedTorchDataset(
            (CxnfsWeightedSource("chat", source),),
            repeat=False,
            prefetch_chunks=2,
        )

        batches = list(dataset.iter_trainer_batches(1))

        self.assertEqual(["sample-0", "sample-1"], [batch.samples[0].data for batch in batches])
        self.assertEqual([(0,), (1,)], source.reads)

    def test_weighted_mds_worker_shards_do_not_overlap_before_repeat(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(6)))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                {"chat": 1},
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=False,
                include_source_metadata=True,
            )

            worker_zero = list(dataset.iter_samples(CxnfsTrainingShard(0, 2)))
            worker_one = list(dataset.iter_samples(CxnfsTrainingShard(1, 2)))

            self.assertEqual([0, 2, 4], [item.sample_index for item in worker_zero])
            self.assertEqual([1, 3, 5], [item.sample_index for item in worker_one])
            self.assertTrue(set(item.data for item in worker_zero).isdisjoint(item.data for item in worker_one))

    def test_parse_weighted_mds_spec_matches_liam_style_weights(self):
        self.assertEqual({"ultrachat": 4, "tldr": 3}, parse_weighted_mds_spec("ultrachat=4,tldr=3"))
        with self.assertRaisesRegex(ValueError, "name=weight"):
            parse_weighted_mds_spec("ultrachat")

    def test_weighted_mds_plan_reports_counts_and_remote_prefixes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))

            plan = plan_weighted_mds_sources(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets/",
            )
            payload = plan.to_dict()

            self.assertEqual("cxnfs.weighted-mds-plan.v1", payload["format"])
            self.assertEqual(2, payload["source_count"])
            self.assertEqual(3, payload["total_weight"])
            self.assertEqual(5, payload["total_samples"])
            self.assertEqual(["chat", "chat", "summary"], payload["source_schedule"])
            self.assertEqual("/remote/datasets/chat", payload["sources"][0]["remote_prefix"])
            self.assertEqual(3, payload["sources"][0]["sample_count"])
            self.assertEqual(1, payload["sources"][0]["shard_count"])
            self.assertEqual(["chat-000000.mds"], payload["sources"][0]["shards"])
            self.assertGreater(payload["total_bytes"], 0)
            self.assertEqual(WEIGHTED_MDS_WORKLOAD_FINGERPRINT_FORMAT, payload["workload_fingerprint"]["format"])
            self.assertEqual(weighted_mds_plan_fingerprint(plan)["sha256"], payload["workload_fingerprint"]["sha256"])

    def test_weighted_mds_plan_cli_writes_stable_json_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/weighted.json"
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_weighted_mds_plan_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1,summary=1",
                    "--remote-root",
                    "/remote/datasets",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual("cxnfs.weighted-mds-plan.v1", payload["format"])
            self.assertEqual("/remote/datasets/summary", payload["sources"][1]["remote_prefix"])
            self.assertRegex(payload["workload_fingerprint"]["sha256"], r"^[0-9a-f]{64}$")

    def test_trainer_contract_payload_validates_direct_buffer_surface(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))

            payload = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                rounds_per_read=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=2,
                samples_per_epoch=3,
                epoch=1,
                min_workload_runs=5,
                min_workload_samples=30,
                min_workload_bytes=210,
                min_cord_sweep=5,
            )

            self.assertEqual(TRAINER_CONTRACT_FORMAT, payload["format"])
            self.assertEqual(str(root / "mds"), payload["root"])
            self.assertEqual("/remote/datasets", payload["remote_root"])
            self.assertEqual({"chat": 2, "summary": 1}, payload["weights"])
            self.assertEqual("cxnfs.weighted-mds-plan.v1", payload["weighted_mds_plan"]["format"])
            self.assertEqual(
                payload["weighted_mds_plan"]["workload_fingerprint"]["sha256"],
                payload["workload_fingerprint"]["sha256"],
            )
            self.assertTrue(payload["workload_requirements"]["configured"])
            self.assertEqual(5, payload["workload_requirements"]["min_workload_runs"])
            self.assertEqual(30, payload["workload_requirements"]["min_workload_samples"])
            self.assertEqual(210, payload["workload_requirements"]["min_workload_bytes"])
            self.assertEqual(5, payload["workload_requirements"]["min_cord_sweep"])
            self.assertFalse(payload["python_surface"]["requires_mount"])
            self.assertFalse(payload["python_surface"]["touches_trainer_code"])
            self.assertTrue(payload["python_surface"]["supports_weighted_sources"])
            self.assertTrue(payload["python_surface"]["supports_per_source_clients"])
            self.assertTrue(payload["python_surface"]["supports_direct_buffers"])
            self.assertTrue(payload["python_surface"]["supports_direct_tensors"])
            self.assertTrue(payload["python_surface"]["supports_tensor_allocation_policy"])
            self.assertTrue(payload["python_surface"]["supports_trainer_dataloader"])
            self.assertTrue(payload["python_surface"]["supports_dataloader_worker_partitioning"])
            self.assertTrue(payload["python_surface"]["supports_trainer_stream"])
            self.assertTrue(payload["python_surface"]["supports_trainer_prefetch_stream"])
            self.assertTrue(payload["python_surface"]["supports_trainer_cursor"])
            self.assertTrue(payload["python_surface"]["supports_training_pipeline"])
            self.assertEqual("CxnfsTrainingPipeline", payload["python_surface"]["training_pipeline_object"])
            self.assertTrue(payload["python_surface"]["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(payload["python_surface"]["supports_pipeline_gpu_transforms"])
            self.assertTrue(payload["python_surface"]["supports_pipeline_torch_compile"])
            self.assertEqual(
                "disabled-with-explicit-opt-in",
                payload["python_surface"]["pipeline_cpu_augmentation_default"],
            )
            self.assertEqual("pytorch-pinned-cpu", payload["python_surface"]["default_tensor_allocation"]["target"])
            self.assertEqual("CxnfsTrainerStream", payload["python_surface"]["stream_object"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["python_surface"]["stream_state_format"])
            self.assertEqual("CxnfsTrainerBatch", payload["python_surface"]["dataloader_batch_object"])
            self.assertIsNone(payload["python_surface"]["dataloader_batch_size"])
            self.assertEqual("cxnfs-quic", payload["network_contract"]["transport"])
            self.assertEqual("quic", payload["network_contract"]["bulk_transport"])
            self.assertTrue(payload["network_contract"]["remote_reads_require_credentials"])
            self.assertFalse(payload["network_contract"]["raw_password_sent_over_control"])
            self.assertEqual(torch_dataset_module.ABSOLUTE_MAX_CORDS, payload["network_contract"]["max_cords"])
            self.assertEqual(0, payload["network_contract"]["bulk_read_retries"])
            self.assertEqual(0, payload["network_contract"]["bulk_write_retries"])
            self.assertEqual("direct-buffers", payload["read_contract"]["read_mode"])
            self.assertEqual("writable-byte-buffer", payload["read_contract"]["destination_type"])
            self.assertIsNone(payload["read_contract"]["tensor_allocation"])
            self.assertEqual("native", payload["read_contract"]["trainer_batch_iterator"])
            self.assertEqual(0, payload["read_contract"]["data_loader_workers"])
            self.assertEqual(TRAINER_WORKER_PARTITION_FORMAT, payload["read_contract"]["worker_partitioning"]["format"])
            self.assertFalse(payload["read_contract"]["worker_partitioning"]["enabled"])
            self.assertEqual(1, payload["read_contract"]["worker_partitioning"]["effective_training_shard_count"])
            self.assertEqual(2, payload["read_contract"]["prefetch_chunks"])
            self.assertEqual(2, payload["read_contract"]["trainer_prefetch_batches"])
            self.assertEqual(1, payload["read_contract"]["epoch"])
            self.assertTrue(payload["validation"]["validated"])
            self.assertEqual(2, payload["validation"]["batch_count"])
            self.assertEqual(3, payload["validation"]["sample_count"])
            self.assertEqual(21, payload["validation"]["byte_size"])
            self.assertEqual({"chat": 2, "summary": 1}, payload["validation"]["source_counts"])
            self.assertEqual({"chat": 2}, payload["validation"]["batches"][0]["source_counts"])
            self.assertEqual({"summary": 1}, payload["validation"]["batches"][1]["source_counts"])

    def test_trainer_tensor_allocation_policy_reports_target_and_copy_contract(self):
        allocation = CxnfsTrainerTensorAllocation(
            target=TensorMemoryTarget.PYTORCH_CPU,
            dtype="uint8",
            device="cpu",
            non_blocking=False,
        )

        self.assertEqual({
            "format": "cxnfs.trainer-tensor-allocation.v1",
            "target": "pytorch-cpu",
            "dtype": "uint8",
            "device": "cpu",
            "non_blocking": False,
            "uses_pinned_memory": False,
            "caller_owned_destination": True,
            "memory_path": {
                "format": TRAINER_TENSOR_MEMORY_PATH_FORMAT,
                "target": "pytorch-cpu",
                "dtype": "uint8",
                "device": "cpu",
                "caller_owned_destination": True,
                "python_byte_batch_avoided": True,
                "network_fills_tensor_destination": True,
                "uses_pinned_host_memory": False,
                "requires_pinned_host_staging": False,
                "device_copy_required": False,
                "device_copy_non_blocking": False,
                "gpu_target": False,
                "gpu_direct_network_dma": False,
            },
        }, allocation.to_dict())

    def test_trainer_cuda_tensor_allocation_reports_pinned_staging_contract(self):
        allocation = CxnfsTrainerTensorAllocation(
            target=TensorMemoryTarget.PYTORCH_CUDA,
            dtype="uint8",
            device="cuda:0",
            non_blocking=True,
        )

        path = allocation.memory_path()

        self.assertEqual(TRAINER_TENSOR_MEMORY_PATH_FORMAT, path["format"])
        self.assertEqual("pytorch-cuda", path["target"])
        self.assertTrue(path["gpu_target"])
        self.assertTrue(path["uses_pinned_host_memory"])
        self.assertTrue(path["requires_pinned_host_staging"])
        self.assertTrue(path["device_copy_required"])
        self.assertTrue(path["device_copy_non_blocking"])
        self.assertFalse(path["network_fills_tensor_destination"])
        self.assertFalse(path["gpu_direct_network_dma"])

    def test_trainer_surface_check_requires_tensor_allocation_for_direct_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            contract["read_contract"]["read_mode"] = "direct-tensors"
            contract["read_contract"]["destination_type"] = "torch-uint8-tensor"
            contract["read_contract"]["tensor_allocation"] = CxnfsTrainerTensorAllocation(
                target="pytorch-cpu",
                dtype="uint8",
                device="cpu",
            ).to_dict()

            payload = build_trainer_surface_check_payload(contract)

            self.assertTrue(payload["passed"])
            self.assertTrue(next(
                check["passed"]
                for check in payload["checks"]
                if check["name"] == "tensor_allocation_declared"
            ))

            contract["read_contract"]["tensor_allocation"] = None
            payload = build_trainer_surface_check_payload(contract)

            self.assertFalse(payload["passed"])
            self.assertIn("tensor_allocation_declared", {
                check["name"] for check in payload["failed_checks"]
            })

    def test_trainer_contract_cli_writes_stable_json_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/trainer-contract.json"
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_contract_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1,summary=1",
                    "--bulk-transport",
                    "stream",
                    "--bulk-read-retries",
                    "1",
                    "--bulk-write-retries",
                    "2",
                    "--read-mode",
                    "direct-buffers",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--min-workload-runs",
                    "2",
                    "--min-workload-samples",
                    "4",
                    "--min-workload-bytes",
                    "16",
                    "--min-cord-sweep",
                    "2",
                    "--output",
                    str(output),
                    "--compact",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_CONTRACT_FORMAT, payload["format"])
            self.assertEqual("local-mds", payload["network_contract"]["transport"])
            self.assertEqual("stream", payload["network_contract"]["bulk_transport"])
            self.assertTrue(payload["network_contract"]["bulk_transport_available"])
            self.assertFalse(payload["network_contract"]["remote_reads_require_credentials"])
            self.assertEqual(1, payload["network_contract"]["bulk_read_retries"])
            self.assertEqual(2, payload["network_contract"]["bulk_write_retries"])
            self.assertTrue(payload["workload_requirements"]["configured"])
            self.assertEqual(2, payload["workload_requirements"]["min_workload_runs"])
            self.assertEqual(4, payload["workload_requirements"]["min_workload_samples"])
            self.assertEqual(16, payload["workload_requirements"]["min_workload_bytes"])
            self.assertEqual(2, payload["workload_requirements"]["min_cord_sweep"])
            self.assertEqual("native", payload["read_contract"]["trainer_batch_iterator"])
            self.assertEqual(0, payload["read_contract"]["data_loader_workers"])
            self.assertEqual(1, payload["validation"]["batch_count"])
            self.assertEqual(2, payload["validation"]["sample_count"])
            self.assertEqual({"chat": 1, "summary": 1}, payload["validation"]["source_counts"])

    def test_trainer_surface_check_accepts_remote_quic_contract_and_workload(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            matrix = proven_surface_matrix_payload(contract)

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_workload=True,
                require_matrix=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
                min_workload_runs=2,
                min_workload_samples=8,
                min_workload_bytes=128,
                min_cord_sweep=2,
            )

            self.assertEqual(TRAINER_SURFACE_CHECK_FORMAT, payload["format"])
            self.assertTrue(payload["passed"], payload["failed_checks"])
            self.assertEqual("cxnfs-quic", payload["contract"]["transport"])
            self.assertEqual("native", payload["contract"]["trainer_batch_iterator"])
            self.assertEqual(0, payload["contract"]["data_loader_workers"])
            self.assertEqual(0, payload["contract"]["bulk_write_retries"])
            self.assertEqual(2, payload["contract"]["weighted_source_count"])
            self.assertEqual("cxnfs-quic", payload["workload"]["transport"])
            self.assertEqual("native", payload["workload"]["trainer_batch_iterator"])
            self.assertEqual(0, payload["workload"]["data_loader_workers"])
            self.assertEqual(0, payload["workload"]["bulk_write_retries"])
            self.assertEqual(2, payload["workload"]["weighted_source_count"])
            self.assertEqual(2, payload["workload"]["cord_sweep_count"])
            self.assertTrue(payload["workload"]["read_path_evidence_complete"])
            self.assertTrue(payload["workload"]["read_path_reliability_complete"])
            self.assertEqual(0, payload["workload"]["unrecovered_read_failures"])
            self.assertTrue(payload["workload"]["write_path_evidence_complete"])
            self.assertTrue(payload["workload"]["remote_client_topology_complete"])
            self.assertTrue(payload["workload"]["remote_client_per_source_complete"])
            self.assertTrue(payload["workload"]["workload_scale_complete"])
            self.assertEqual("profile-storage", payload["workload"]["blockfs_decision"])
            self.assertEqual("cxnfs.trainer-workload-provenance.v1", payload["workload"]["benchmark_provenance"]["format"])
            self.assertEqual(
                payload["contract"]["workload_fingerprint"]["sha256"],
                payload["workload"]["workload_fingerprint"]["sha256"],
            )
            self.assertEqual("cxnfs.trainer-surface-matrix.v1", payload["matrix"]["format"])
            self.assertEqual("cxnfs-quic", payload["matrix"]["transport"])
            self.assertEqual(4, payload["matrix"]["case_count"])
            self.assertTrue(payload["matrix"]["memory_paths_complete"])
            self.assertTrue(payload["matrix"]["quic_read_paths_complete"])
            self.assertTrue(payload["matrix"]["read_path_reliability_complete"])
            self.assertTrue(payload["matrix"]["remote_client_topology_complete"])
            self.assertTrue(payload["matrix"]["remote_client_per_source_complete"])
            self.assertEqual(0, payload["matrix"]["unrecovered_read_failures"])
            self.assertEqual(
                {
                    "direct-buffers/native",
                    "direct-buffers/trainer-dataloader",
                    "direct-tensors/native",
                    "direct-tensors/trainer-dataloader",
                },
                set(payload["matrix"]["case_names"]),
            )

    def test_trainer_handoff_payload_packages_ready_trainer_surface(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            matrix = proven_surface_matrix_payload(contract)
            decision = {
                "format": "cxnfs.workload-decision.v1",
                "benchmark_completed": True,
                "trainer_surface_check_passed": True,
                "trainer_surface_matrix_passed": True,
                "pressure_profile_complete": False,
                "kernel_module_allowed": False,
                "required_next_evidence": ["full current-workload benchmark"],
            }
            blockfs_handoff = {
                "format": "cxnfs.blockfs-handoff.v1",
                "status": "deferred",
                "decision": "profile-storage",
                "kernel_module_allowed": False,
                "required_next_evidence": ["timestamped server evidence"],
                "missing_readiness_checks": ["pressure_profile_complete"],
            }

            payload = build_trainer_handoff_payload(
                contract,
                contract_path=root / "trainer-contract.json",
                workload_payload=workload,
                workload_path=root / "trainer-workload.json",
                matrix_payload=matrix,
                matrix_path=root / "trainer-matrix.json",
                workload_decision_payload=decision,
                workload_decision_path=root / "workload-decision.json",
                blockfs_handoff_payload=blockfs_handoff,
                blockfs_handoff_path=root / "blockfs-handoff.json",
                batch_direct_read_payload=proven_batch_direct_read_report(),
                batch_direct_read_path=root / "python-quic-batch-direct-read.json",
                batch_direct_tensor_read_payload=proven_batch_direct_tensor_read_report(),
                batch_direct_tensor_read_path=root / "python-quic-batch-direct-tensor-read.json",
                require_low_level_direct_read_proofs=True,
                require_low_level_direct_write_proofs=True,
                require_remote=True,
                require_quic=True,
                require_workload=True,
                require_matrix=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
                min_workload_runs=2,
                min_workload_samples=8,
                min_workload_bytes=128,
                min_cord_sweep=2,
            )

            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["format"])
            self.assertEqual("ready", payload["status"])
            self.assertTrue(payload["ready_for_trainer_wiring"])
            self.assertTrue(payload["ready_for_remote_quic"])
            self.assertFalse(payload["ready_for_blockfs_candidate"])
            self.assertTrue(payload["low_level_direct_read_proofs"]["required"])
            self.assertTrue(payload["low_level_direct_read_proofs"]["complete"])
            self.assertEqual(
                "cxnfs.quic-batch-direct-read.v1",
                payload["low_level_direct_read_proofs"]["batch_direct_read"]["format"],
            )
            self.assertEqual(
                str(root / "python-quic-batch-direct-read.json"),
                payload["low_level_direct_read_proofs"]["batch_direct_read"]["path"],
            )
            self.assertEqual(
                "TLS_AES_128_GCM_SHA256",
                payload["low_level_direct_read_proofs"]["batch_direct_read"]["cipher_suite"],
            )
            self.assertEqual(
                "cxnfs.quic-batch-direct-tensor-read.v1",
                payload["low_level_direct_read_proofs"]["batch_direct_tensor_read"]["format"],
            )
            self.assertEqual(
                str(root / "python-quic-batch-direct-tensor-read.json"),
                payload["low_level_direct_read_proofs"]["batch_direct_tensor_read"]["path"],
            )
            self.assertEqual(
                "pytorch-cpu",
                payload["low_level_direct_read_proofs"]["batch_direct_tensor_read"]["tensor_target"],
            )
            self.assertEqual(
                2,
                payload["low_level_direct_read_proofs"]["batch_direct_read"]["direct_buffer_read_operations"],
            )
            self.assertEqual(
                2,
                payload["low_level_direct_read_proofs"]["batch_direct_tensor_read"]["direct_tensor_read_operations"],
            )
            self.assertTrue(payload["low_level_direct_write_proofs"]["required"])
            self.assertTrue(payload["low_level_direct_write_proofs"]["complete"])
            self.assertEqual(
                "cxnfs.trainer-low-level-direct-write-proofs.v1",
                payload["low_level_direct_write_proofs"]["format"],
            )
            self.assertEqual(
                str(root / "trainer-workload.json"),
                payload["low_level_direct_write_proofs"]["workload_path"],
            )
            self.assertEqual(
                "TLS_AES_128_GCM_SHA256",
                payload["low_level_direct_write_proofs"]["quic_write_transport_cipher_suite"],
            )
            self.assertEqual(
                2,
                payload["low_level_direct_write_proofs"]["quic_write_transport_expected_cipher_suite_count"],
            )
            self.assertTrue(payload["low_level_direct_write_proofs"]["metadata_pending_observed"])
            self.assertTrue(payload["low_level_direct_write_proofs"]["all_write_probes_verified"])
            self.assertTrue(payload["low_level_direct_write_proofs"]["quic_write_transport_complete"])
            self.assertTrue(
                payload["low_level_direct_write_proofs"][
                    "quic_write_transport_all_reports_write_ack_stream_completion_verified"
                ]
            )
            self.assertTrue(
                payload["low_level_direct_write_proofs"][
                    "quic_write_transport_all_reports_write_ack_stream_eof_verified"
                ]
            )
            self.assertEqual(4, payload["low_level_direct_write_proofs"]["max_observed_write_cords"])
            self.assertFalse(payload["requires_mount"])
            self.assertFalse(payload["touches_trainer_code"])
            self.assertEqual("cxnfs-quic", payload["contract"]["transport"])
            self.assertEqual("quic", payload["contract"]["bulk_transport"])
            self.assertEqual("direct-buffers", payload["integration_surface"]["read_mode"])
            self.assertEqual("CxnfsTrainerBatch", payload["integration_surface"]["batch_object"])
            self.assertEqual("CxnfsTrainerStream", payload["integration_surface"]["stream_object"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["integration_surface"]["stream_state_format"])
            self.assertEqual(TRAINER_CURSOR_FORMAT, payload["integration_surface"]["resume_cursor_format"])
            self.assertTrue(payload["integration_surface"]["supports_trainer_stream"])
            self.assertTrue(payload["integration_surface"]["supports_trainer_prefetch_stream"])
            self.assertTrue(payload["integration_surface"]["supports_training_pipeline"])
            self.assertEqual("CxnfsTrainingPipeline", payload["integration_surface"]["training_pipeline_object"])
            self.assertTrue(payload["integration_surface"]["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(payload["integration_surface"]["supports_pipeline_gpu_transforms"])
            self.assertTrue(payload["integration_surface"]["supports_pipeline_torch_compile"])
            self.assertEqual(
                "disabled-with-explicit-opt-in",
                payload["integration_surface"]["pipeline_cpu_augmentation_default"],
            )
            self.assertEqual(TRAINER_INTEGRATION_PLAN_FORMAT, payload["integration_plan"]["format"])
            self.assertEqual("open_weighted_mds_trainer_stream", payload["integration_plan"]["stream_factory"])
            self.assertTrue(payload["integration_plan"]["remote_client_required"])
            self.assertEqual("CxnfsTrainerIntegrationPlan", payload["integration_plan"]["plan_object"])
            self.assertEqual(
                "CxnfsTrainerIntegrationPlan.open_training_loop",
                payload["integration_plan"]["training_loop_context"],
            )
            self.assertTrue(payload["integration_plan"]["supports_training_loop_prefetch"])
            self.assertEqual("CxnfsTrainingPipeline", payload["integration_plan"]["training_pipeline_object"])
            self.assertEqual(
                "CxnfsTrainingPipeline.read_tensor_batch",
                payload["integration_plan"]["training_pipeline_context"],
            )
            self.assertTrue(payload["integration_plan"]["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(payload["integration_plan"]["supports_pipeline_gpu_transforms"])
            self.assertTrue(payload["integration_plan"]["supports_pipeline_torch_compile"])
            self.assertEqual(str(root / "mds"), payload["integration_plan"]["stream_kwargs"]["root"])
            self.assertEqual({"chat": 2, "summary": 1}, payload["integration_plan"]["stream_kwargs"]["weights"])
            self.assertEqual(2, payload["integration_plan"]["stream_kwargs"]["batch_size"])
            self.assertEqual("/stage0-mds", payload["integration_plan"]["stream_kwargs"]["remote_root"])
            self.assertEqual("direct-buffers", payload["integration_plan"]["stream_kwargs"]["read_mode"])
            self.assertNotIn("trainer_prefetch_batches", payload["integration_plan"]["stream_kwargs"])
            self.assertEqual(0, payload["integration_plan"]["trainer_loop_options"]["trainer_prefetch_batches"])
            self.assertEqual("quic", payload["integration_plan"]["remote_client_kwargs"]["bulk_transport"])
            self.assertEqual(1, payload["integration_plan"]["remote_client_kwargs"]["initial_cords"])
            self.assertEqual(
                torch_dataset_module.ABSOLUTE_MAX_CORDS,
                payload["integration_plan"]["remote_client_kwargs"]["max_cords"],
            )
            self.assertGreater(payload["integration_plan"]["remote_client_kwargs"]["provisional_container_size_bytes"], 0)
            self.assertEqual(0, payload["integration_plan"]["remote_client_kwargs"]["expected_size_limit_bytes"])
            self.assertIsNone(payload["integration_plan"]["remote_client_kwargs"]["write_connections"])
            self.assertFalse(payload["integration_plan"]["remote_client_kwargs"]["shared_client"])
            self.assertFalse(payload["integration_plan"]["remote_client_kwargs"]["raw_password_sent_over_control"])
            self.assertEqual(
                "CxnfsTrainerIntegrationPlan.create_remote_client_factory",
                payload["integration_plan"]["remote_client_factory"],
            )
            self.assertEqual(
                "CxnfsTrainerIntegrationPlan.open_remote_stream",
                payload["integration_plan"]["remote_stream_context"],
            )
            self.assertIn("CxnfsTrainerIntegrationPlan", payload["integration_plan"]["required_imports"])
            self.assertIn("CxnfsTrainingPipeline", payload["integration_plan"]["required_imports"])
            self.assertIn("CxnfsPipelineConfig", payload["integration_plan"]["required_imports"])
            self.assertTrue(payload["surface_check"]["passed"])
            self.assertEqual(0, payload["surface_check"]["failed_check_count"])
            self.assertEqual(2, payload["workload"]["cord_sweep_count"])
            self.assertTrue(payload["workload"]["read_path_evidence_complete"])
            self.assertTrue(payload["workload"]["read_path_reliability_complete"])
            self.assertEqual(0, payload["workload"]["unrecovered_read_failures"])
            self.assertEqual(4, payload["matrix"]["case_count"])
            self.assertTrue(payload["matrix"]["read_path_reliability_complete"])
            self.assertEqual(0, payload["matrix"]["unrecovered_read_failures"])
            self.assertEqual("deferred", payload["blockfs_handoff"]["status"])
            self.assertIn("timestamped server evidence", payload["next_actions"])

    def test_trainer_integration_plan_validates_handoff_and_exposes_stream_kwargs(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            allocation = CxnfsTrainerTensorAllocation(target="pytorch-cpu", dtype="uint8", device="cpu")
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-tensors",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
                trainer_batch_iterator="native",
                data_loader_workers=0,
                tensor_allocation=allocation,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["tensor_allocation"] = contract["read_contract"]["tensor_allocation"]
            workload["benchmark_provenance"]["invocation"]["tensor_allocation"] = contract["read_contract"]["tensor_allocation"]
            matrix = proven_surface_matrix_payload(contract)
            matrix["trainer_prefetch_batches"] = contract["read_contract"]["trainer_prefetch_batches"]
            payload = build_proven_remote_quic_handoff(
                contract,
                workload_payload=workload,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_workload=True,
                require_matrix=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
                min_workload_runs=2,
                min_workload_samples=8,
                min_workload_bytes=128,
                min_cord_sweep=2,
            )

            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(payload, require_remote_quic=True)
            self.assertTrue(plan.ready_for_remote_quic)
            self.assertEqual(TRAINER_INTEGRATION_PLAN_FORMAT, plan.to_dict()["format"])
            self.assertEqual("quic", plan.remote_client_kwargs_json["bulk_transport"])
            self.assertEqual(1, plan.trainer_loop_options()["trainer_prefetch_batches"])
            kwargs = plan.stream_kwargs()
            self.assertEqual(str(root / "mds"), kwargs["root"])
            self.assertEqual({"chat": 2, "summary": 1}, kwargs["weights"])
            self.assertEqual("/stage0-mds", kwargs["remote_root"])
            self.assertEqual("direct-tensors", kwargs["read_mode"])
            self.assertEqual(2, kwargs["source_read_workers"])
            self.assertEqual(2, kwargs["prefetch_chunks"])
            self.assertEqual(3, kwargs["samples_per_epoch"])
            self.assertEqual("native", kwargs["trainer_batch_iterator"])
            self.assertEqual(0, kwargs["data_loader_workers"])
            self.assertIsInstance(kwargs["tensor_allocation"], CxnfsTrainerTensorAllocation)
            self.assertEqual(TensorMemoryTarget.PYTORCH_CPU, kwargs["tensor_allocation"].target)

            payload["ready_for_trainer_wiring"] = False
            with self.assertRaisesRegex(ValueError, "not ready"):
                CxnfsTrainerIntegrationPlan.from_handoff_payload(payload)

    def test_trainer_integration_plan_preflight_report_summarizes_local_stream_without_reading(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
                trainer_batch_iterator="native",
            )
            handoff = build_trainer_handoff_payload(contract)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)

            report = plan.preflight_report()

            self.assertEqual(TRAINER_INTEGRATION_PREFLIGHT_FORMAT, report["format"])
            self.assertTrue(report["passed"])
            self.assertEqual(TRAINER_HANDOFF_FORMAT, report["handoff_format"])
            self.assertEqual(TRAINER_INTEGRATION_PLAN_FORMAT, report["plan_format"])
            self.assertTrue(report["ready_for_trainer_wiring"])
            self.assertFalse(report["remote_client_required"])
            self.assertFalse(report["requires_credentials"])
            self.assertFalse(report["requires_mount"])
            self.assertFalse(report["touches_trainer_code"])
            self.assertTrue(report["safe_to_run_before_trainer_workers"])
            self.assertFalse(report["opens_network_connections"])
            self.assertFalse(report["consumes_batches"])
            self.assertEqual("open_weighted_mds_trainer_stream", report["stream_factory"])
            self.assertEqual("CxnfsTrainerStream", report["stream_object"])
            self.assertEqual("CxnfsTrainerBatch", report["batch_object"])
            self.assertEqual("CxnfsTrainingPipeline", report["training_pipeline_object"])
            self.assertEqual("CxnfsTrainingPipeline.read_tensor_batch", report["training_pipeline_context"])
            self.assertTrue(report["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(report["supports_pipeline_gpu_transforms"])
            self.assertTrue(report["supports_pipeline_torch_compile"])
            self.assertEqual("disabled-with-explicit-opt-in", report["pipeline_cpu_augmentation_default"])
            self.assertEqual(str(root / "mds"), report["root"])
            self.assertIsNone(report["remote_root"])
            self.assertEqual({"chat": 2, "summary": 1}, report["weights"])
            self.assertEqual(["chat", "summary"], report["weighted_sources"])
            self.assertEqual(2, report["source_count"])
            self.assertEqual("direct-buffers", report["read_mode"])
            self.assertEqual("native", report["trainer_batch_iterator"])
            self.assertEqual(2, report["source_read_workers"])
            self.assertEqual(2, report["prefetch_chunks"])
            self.assertEqual(1, report["trainer_prefetch_batches"])
            self.assertEqual(1, report["trainer_loop_options"]["trainer_prefetch_batches"])
            self.assertIn("read_mode", report["stream_kwargs_keys"])
            self.assertIsNone(report["bulk_transport"])
            self.assertFalse(report["raw_password_sent_over_control"])

    def test_trainer_integration_plan_preflight_report_summarizes_remote_quic_policy(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            allocation = CxnfsTrainerTensorAllocation(target="pytorch-cpu", dtype="uint8", device="cpu")
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                bulk_read_retries=1,
                bulk_write_retries=1,
                read_mode="direct-tensors",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
                tensor_allocation=allocation,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_proven_remote_quic_handoff(contract, require_remote=True, require_quic=True)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff, require_remote_quic=True)

            report = plan.preflight_report()

            self.assertEqual(TRAINER_INTEGRATION_PREFLIGHT_FORMAT, report["format"])
            self.assertTrue(report["passed"])
            self.assertTrue(report["ready_for_remote_quic"])
            self.assertTrue(report["low_level_direct_read_proofs_complete"])
            self.assertTrue(report["batch_direct_read_proof_complete"])
            self.assertTrue(report["batch_direct_tensor_read_proof_complete"])
            self.assertEqual("TLS_AES_128_GCM_SHA256", report["batch_direct_read_cipher_suite"])
            self.assertEqual("TLS_AES_128_GCM_SHA256", report["batch_direct_tensor_read_cipher_suite"])
            self.assertTrue(report["remote_client_required"])
            self.assertTrue(report["requires_credentials"])
            self.assertEqual("/stage0-mds", report["remote_root"])
            self.assertEqual("quic", report["bulk_transport"])
            self.assertEqual(1, report["bulk_read_retries"])
            self.assertEqual(1, report["bulk_write_retries"])
            self.assertEqual(torch_dataset_module.ABSOLUTE_MAX_CORDS, report["max_cords"])
            self.assertEqual("direct-tensors", report["read_mode"])
            self.assertEqual("pytorch-cpu", report["stream_kwargs"]["tensor_allocation"]["target"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.create_remote_client_factory", report["remote_client_factory"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_remote_stream", report["remote_stream_context"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", report["training_loop_context"])
            self.assertEqual("CxnfsTrainingPipeline", report["training_pipeline_object"])
            self.assertEqual("CxnfsTrainingPipeline.read_tensor_batch", report["training_pipeline_context"])
            self.assertTrue(report["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(report["supports_pipeline_gpu_transforms"])
            self.assertTrue(report["supports_pipeline_torch_compile"])
            self.assertTrue(report["supports_training_loop_prefetch"])
            self.assertTrue(report["supports_resume_cursor"])
            self.assertTrue(report["supports_resume_state_file"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, report["resume_state_format"])
            self.assertFalse(report["raw_password_sent_over_control"])
            self.assertTrue(report["per_source_client_default"])

    def test_trainer_preflight_cli_writes_local_report_without_prompt_or_network(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            handoff = build_trainer_handoff_payload(contract)
            handoff_path = root / "trainer-handoff.json"
            report_path = root / "trainer-preflight.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_preflight_cli(
                [str(handoff_path), "--output", str(report_path)],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(TRAINER_INTEGRATION_PREFLIGHT_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertFalse(payload["remote_client_required"])
            self.assertFalse(payload["requires_credentials"])
            self.assertFalse(payload["opens_network_connections"])
            self.assertFalse(payload["consumes_batches"])
            self.assertEqual("open_weighted_mds_trainer_stream", payload["stream_factory"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", payload["training_loop_context"])
            self.assertEqual("CxnfsTrainingPipeline", payload["training_pipeline_object"])
            self.assertEqual("CxnfsTrainingPipeline.read_tensor_batch", payload["training_pipeline_context"])
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertEqual({"chat": 2, "summary": 1}, payload["weights"])
            self.assertIn('"cxnfs.trainer-integration-preflight.v1"', stdout.getvalue())

    def test_trainer_preflight_cli_requires_remote_quic_when_requested(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            handoff_path = root / "trainer-handoff.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_preflight_cli(
                [str(handoff_path), "--require-remote-quic"],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stdout.getvalue())
            self.assertIn("remote QUIC readiness", stderr.getvalue())

    def test_trainer_preflight_cli_requires_embedded_direct_read_proofs_for_remote_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_trainer_handoff_payload(contract, require_remote=True, require_quic=True)
            self.assertFalse(handoff["ready_for_remote_quic"])
            handoff_path = root / "trainer-handoff.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with self.assertRaisesRegex(ValueError, "low-level direct read QUIC proofs"):
                CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff, require_remote_quic=True)

            exit_code = run_trainer_preflight_cli(
                [str(handoff_path), "--require-remote-quic"],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stdout.getvalue())
            self.assertIn("low-level direct read QUIC proofs", stderr.getvalue())
            self.assertIn("batch_direct_read_proof_complete", stderr.getvalue())

    def test_trainer_preflight_cli_reports_remote_quic_policy_without_password_argument(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                bulk_read_retries=1,
                bulk_write_retries=1,
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_proven_remote_quic_handoff(contract, require_remote=True, require_quic=True)
            handoff_path = root / "trainer-handoff.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            help_stdout = io.StringIO()
            help_stderr = io.StringIO()

            help_exit = run_trainer_preflight_cli(["--help"], stdout=help_stdout, stderr=help_stderr)

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            exit_code = run_trainer_preflight_cli(
                [str(handoff_path), "--require-remote-quic", "--compact"],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_INTEGRATION_PREFLIGHT_FORMAT, payload["format"])
            self.assertTrue(payload["ready_for_remote_quic"])
            self.assertTrue(payload["low_level_direct_read_proofs_complete"])
            self.assertTrue(payload["batch_direct_read_proof_complete"])
            self.assertTrue(payload["batch_direct_tensor_read_proof_complete"])
            self.assertTrue(payload["remote_client_required"])
            self.assertTrue(payload["requires_credentials"])
            self.assertEqual("quic", payload["bulk_transport"])
            self.assertEqual(torch_dataset_module.ABSOLUTE_MAX_CORDS, payload["max_cords"])
            self.assertEqual(1, payload["bulk_read_retries"])
            self.assertEqual(1, payload["bulk_write_retries"])
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertFalse(payload["raw_password_sent_over_control"])

    def test_trainer_network_preflight_cli_reads_handoff_probe_over_quic_without_password_argument(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                bulk_read_retries=1,
                bulk_write_retries=1,
                read_mode="direct-tensors",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_proven_remote_quic_handoff(contract, require_remote=True, require_quic=True)
            handoff_path = root / "trainer-handoff.json"
            output_path = root / "trainer-network-preflight.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            prompt = TrainerNetworkPreflightPrompt()
            control_factory = TrainerNetworkPreflightControlFactory()
            quic_bulk_factory = TrainerNetworkPreflightBulkFactory(b"x")
            help_stdout = io.StringIO()
            help_stderr = io.StringIO()

            help_exit = run_trainer_network_preflight_cli(["--help"], stdout=help_stdout, stderr=help_stderr)

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--bulk-transport", help_stdout.getvalue())
            self.assertIn("--read-path", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch("cxnfs.bulk.importlib.util.find_spec", lambda _name: object()), \
                patch("cxnfs.health.CxnfsQuicBulkClient.connect", quic_bulk_factory):
                exit_code = run_trainer_network_preflight_cli(
                    [
                        str(handoff_path),
                        "--require-remote-quic",
                        "--host",
                        "cxnfs.example.internal",
                        "--control-port",
                        "1443",
                        "--transport-port",
                        "1444",
                        "--server-hostname",
                        "trainer.cxnfs.internal",
                        "--cafile",
                        "/etc/cxnfs/ca.pem",
                        "--output",
                        str(output_path),
                        "--compact",
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=prompt,
                    control_client_factory=control_factory,
                    tls_context_factory=RecordingTrainerNetworkTlsContextFactory(),
                )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual(1, prompt.calls)
            self.assertTrue(prompt.credentials.closed)
            self.assertTrue(control_factory.client.closed)
            self.assertTrue(quic_bulk_factory.client.closed)
            self.assertEqual("cxnfs.example.internal", control_factory.calls[0]["host"])
            self.assertEqual(1443, control_factory.calls[0]["port"])
            self.assertEqual("trainer.cxnfs.internal", control_factory.calls[0]["server_hostname"])
            self.assertEqual("/etc/cxnfs/ca.pem", control_factory.calls[0]["tls_context"].cafile)
            self.assertEqual("cxnfs.example.internal", quic_bulk_factory.calls[0]["host"])
            self.assertEqual(1444, quic_bulk_factory.calls[0]["port"])
            self.assertEqual([("/stage0-mds/chat/chat-000000.mds", 0, 1)], quic_bulk_factory.client.reads)
            payload = json.loads(stdout.getvalue())
            file_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload, file_payload)
            self.assertEqual(TRAINER_NETWORK_PREFLIGHT_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertTrue(payload["ready_for_remote_quic"])
            self.assertTrue(payload["opens_network_connections"])
            self.assertFalse(payload["consumes_batches"])
            self.assertTrue(payload["safe_to_run_before_trainer_workers"])
            self.assertTrue(payload["requires_credentials"])
            self.assertEqual("cxnfs-quic", payload["transport"])
            self.assertEqual("quic", payload["bulk_transport"])
            self.assertEqual("/stage0-mds", payload["probe_path"])
            self.assertEqual("/stage0-mds/chat/chat-000000.mds", payload["bulk_probe_path"])
            self.assertEqual("ok", payload["bulk_probe"])
            self.assertEqual(1, payload["bulk_probe_bytes"])
            self.assertEqual(1, payload["read_length"])
            self.assertEqual("direct-tensors", payload["read_mode"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.create_remote_client_factory", payload["remote_client_factory"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertFalse(payload["trainer_code_touched"])
            self.assertNotIn("secret", stdout.getvalue())
            self.assertNotIn("secret", output_path.read_text(encoding="utf-8"))

    def test_trainer_network_preflight_payload_can_use_explicit_stream_probe(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            control_factory = TrainerNetworkPreflightControlFactory()
            bulk_factory = TrainerNetworkPreflightBulkFactory(b"xy")

            with ClientCredentials("trainer", "secret") as credentials:
                payload = build_trainer_network_preflight_payload(
                    handoff,
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=4509,
                    transport_port=4510,
                    probe_path="/stage0-mds/chat",
                    read_path="/stage0-mds/chat/custom-000000.mds",
                    read_length=2,
                    bulk_transport="stream",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                )

            self.assertEqual(TRAINER_NETWORK_PREFLIGHT_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual("cxnfs-length-prefixed", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertEqual("/stage0-mds/chat", payload["probe_path"])
            self.assertEqual("/stage0-mds/chat/custom-000000.mds", payload["bulk_probe_path"])
            self.assertEqual(2, payload["bulk_probe_bytes"])
            self.assertEqual([("/stage0-mds/chat/custom-000000.mds", 0, 2)], bulk_factory.client.reads)

    def test_trainer_network_preflight_cli_rejects_missing_quic_dependency_before_prompting(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-tensors",
                batch_size=1,
                batches=1,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_proven_remote_quic_handoff(contract, require_remote=True, require_quic=True)
            handoff_path = root / "trainer-handoff.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            prompt = TrainerNetworkPreflightPrompt()
            stderr = io.StringIO()

            with patch("cxnfs.bulk.importlib.util.find_spec", lambda _name: None):
                exit_code = run_trainer_network_preflight_cli(
                    [str(handoff_path), "--require-remote-quic"],
                    stderr=stderr,
                    credential_prompt=prompt,
                )

            self.assertEqual(1, exit_code)
            self.assertEqual(0, prompt.calls)
            self.assertIn("Python QUIC bulk transport requires", stderr.getvalue())

    def test_trainer_integration_plan_builds_per_rank_distributed_plan(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                bulk_read_retries=1,
                bulk_write_retries=1,
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_proven_remote_quic_handoff(contract, require_remote=True, require_quic=True)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff, require_remote_quic=True)

            rank_plan = plan.rank_plan(2, 4)
            rank_preflight = rank_plan.preflight_report()
            report = plan.distributed_plan_report(4, selected_rank=2)

            self.assertEqual(TRAINER_INTEGRATION_PREFLIGHT_FORMAT, rank_preflight["format"])
            self.assertEqual({"index": 2, "count": 4}, rank_preflight["training_shard"])
            self.assertEqual(2, rank_preflight["rank"])
            self.assertEqual(4, rank_preflight["world_size"])
            self.assertEqual({"index": 2, "count": 4}, rank_preflight["stream_kwargs"]["shard"])
            self.assertEqual(TRAINER_DISTRIBUTED_PLAN_FORMAT, report["format"])
            self.assertTrue(report["passed"])
            self.assertTrue(report["ready_for_remote_quic"])
            self.assertTrue(report["low_level_direct_read_proofs_complete"])
            self.assertTrue(report["batch_direct_read_proof_complete"])
            self.assertTrue(report["batch_direct_tensor_read_proof_complete"])
            self.assertTrue(report["remote_client_required"])
            self.assertTrue(report["requires_credentials"])
            self.assertFalse(report["opens_network_connections"])
            self.assertFalse(report["consumes_batches"])
            self.assertFalse(report["raw_password_sent_over_control"])
            self.assertEqual(4, report["world_size"])
            self.assertEqual(4, report["rank_count"])
            self.assertEqual([0, 1, 2, 3], report["ranks"])
            self.assertEqual(2, report["selected_rank"])
            self.assertEqual({"index": 2, "count": 4}, report["selected_rank_plan"]["training_shard"])
            self.assertEqual(
                [{"index": rank, "count": 4} for rank in range(4)],
                [rank_report["training_shard"] for rank_report in report["rank_plans"]],
            )
            self.assertTrue(all("shard" in rank_report["stream_kwargs"] for rank_report in report["rank_plans"]))
            self.assertEqual("CxnfsTrainerIntegrationPlan.rank_plan", report["per_rank_plan_method"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.distributed_rank_plans", report["distributed_plan_method"])
            self.assertEqual("CxnfsTrainerRuntimeBinding", report["runtime_binding_object"])
            self.assertEqual(TRAINER_RUNTIME_BINDING_FORMAT, report["runtime_binding_format"])
            self.assertTrue(
                all(rank_report["runtime_binding_format"] == TRAINER_RUNTIME_BINDING_FORMAT for rank_report in report["rank_plans"])
            )
            self.assertEqual("quic", report["bulk_transport"])
            self.assertEqual(torch_dataset_module.ABSOLUTE_MAX_CORDS, report["max_cords"])

    def test_trainer_distributed_plan_cli_writes_report_without_prompt_or_network(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            handoff = build_proven_remote_quic_handoff(contract, require_remote=True, require_quic=True)
            handoff_path = root / "trainer-handoff.json"
            report_path = root / "trainer-distributed-plan.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            help_stdout = io.StringIO()
            help_stderr = io.StringIO()

            help_exit = run_trainer_distributed_plan_cli(["--help"], stdout=help_stdout, stderr=help_stderr)

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--world-size", help_stdout.getvalue())
            self.assertIn("--rank", help_stdout.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            exit_code = run_trainer_distributed_plan_cli(
                [
                    str(handoff_path),
                    "--require-remote-quic",
                    "--world-size",
                    "3",
                    "--rank",
                    "1",
                    "--output",
                    str(report_path),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(TRAINER_DISTRIBUTED_PLAN_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(3, payload["world_size"])
            self.assertEqual(3, payload["rank_count"])
            self.assertEqual(1, payload["selected_rank"])
            self.assertEqual({"index": 1, "count": 3}, payload["selected_rank_plan"]["training_shard"])
            self.assertEqual({"index": 1, "count": 3}, payload["selected_rank_plan"]["stream_kwargs"]["shard"])
            self.assertTrue(payload["low_level_direct_read_proofs_complete"])
            self.assertTrue(payload["batch_direct_read_proof_complete"])
            self.assertTrue(payload["batch_direct_tensor_read_proof_complete"])
            self.assertTrue(payload["selected_rank_plan"]["low_level_direct_read_proofs_complete"])
            self.assertTrue(payload["selected_rank_plan"]["batch_direct_read_proof_complete"])
            self.assertTrue(payload["selected_rank_plan"]["batch_direct_tensor_read_proof_complete"])
            self.assertTrue(payload["remote_client_required"])
            self.assertTrue(payload["requires_credentials"])
            self.assertFalse(payload["opens_network_connections"])
            self.assertFalse(payload["consumes_batches"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertIn('"cxnfs.trainer-distributed-plan.v1"', stdout.getvalue())

            env_stdout = io.StringIO()
            env_stderr = io.StringIO()
            with patch.dict(os.environ, {"RANK": "2", "WORLD_SIZE": "3"}):
                env_exit = run_trainer_distributed_plan_cli(
                    [str(handoff_path), "--require-remote-quic", "--compact"],
                    stdout=env_stdout,
                    stderr=env_stderr,
                )
            env_payload = json.loads(env_stdout.getvalue())
            self.assertEqual(0, env_exit, env_stderr.getvalue())
            self.assertEqual(3, env_payload["world_size"])
            self.assertEqual(2, env_payload["selected_rank"])
            self.assertEqual({"index": 2, "count": 3}, env_payload["selected_rank_plan"]["training_shard"])

    def test_trainer_runtime_binding_opens_rank_local_training_loop(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)

            with patch.dict(os.environ, {"RANK": "1", "WORLD_SIZE": "2"}):
                binding = plan.bind_rank()
            report = binding.to_dict()

            self.assertIsInstance(binding, CxnfsTrainerRuntimeBinding)
            self.assertEqual(1, binding.rank)
            self.assertEqual(2, binding.world_size)
            self.assertEqual(CxnfsTrainingShard(1, 2), binding.training_shard)
            self.assertEqual(TRAINER_RUNTIME_BINDING_FORMAT, report["format"])
            self.assertEqual({"index": 1, "count": 2}, report["training_shard"])
            self.assertEqual({"index": 1, "count": 2}, report["stream_kwargs"]["shard"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.open_training_loop", report["open_training_loop_context"])
            self.assertFalse(report["remote_client_required"])
            self.assertFalse(report["requires_credentials"])
            self.assertFalse(report["opens_network_connections"])
            self.assertFalse(report["consumes_batches"])
            self.assertFalse(report["raw_password_sent_over_control"])

            with binding.open_training_loop(trainer_prefetch_batches=0) as stream:
                self.assertIsInstance(stream, CxnfsTrainerStream)
                self.assertEqual(CxnfsTrainingShard(1, 2), stream.dataset.shard)
                batch = stream.next_batch()

            self.assertEqual(2, len(batch.samples))
            self.assertEqual(1, batch.resume_cursor.shard_index)
            self.assertEqual(2, batch.resume_cursor.shard_count)

    def test_trainer_runtime_binding_opens_rank_local_pipeline_training_loop(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-tensors",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract)
            binding = CxnfsTrainerRuntimeBinding.from_handoff_payload(handoff, rank=1, world_size=2)
            report = binding.to_dict()

            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                report["open_pipeline_training_loop_context"],
            )
            self.assertEqual(
                "CxnfsTrainerIntegrationPlan.open_pipeline_training_loop",
                report["pipeline_training_loop_context"],
            )
            self.assertEqual(CxnfsTrainingShard(1, 2), binding.training_shard)

            with binding.open_pipeline_training_loop(pipeline_prefetch_batches=1, pipeline_workers=2) as loop:
                self.assertIsInstance(loop, CxnfsTrainerPipelineLoop)
                self.assertEqual(CxnfsTrainingShard(1, 2), loop.stream.dataset.shard)
                batch = loop.next_batch()
                state = loop.state_dict()

            self.assertEqual(2, batch.sample_count)
            self.assertEqual(1, batch.resume_cursor.shard_index)
            self.assertEqual(2, batch.resume_cursor.shard_count)
            self.assertEqual(2, state["cursor"]["emitted"])
            self.assertEqual({"index": 1, "count": 2}, state["cursor"]["shard"])

    def test_trainer_runtime_binding_prompts_for_remote_credentials_when_requested(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            binding = CxnfsTrainerRuntimeBinding.from_handoff_payload(handoff, rank=1, world_size=2)
            controls = []
            bulks = []
            prompted_credentials = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                prompted_credentials.append(credentials)
                return credentials

            report = binding.to_dict()
            self.assertTrue(report["remote_client_required"])
            self.assertTrue(report["requires_credentials"])
            self.assertTrue(report["credential_prompt_supported"])
            self.assertEqual({"index": 1, "count": 2}, report["training_shard"])
            self.assertEqual("stream", report["bulk_transport"])
            with self.assertRaisesRegex(ValueError, "credentials or credential_prompt"):
                with binding.open_training_loop(trainer_prefetch_batches=0):
                    pass

            with binding.open_training_loop(
                credential_prompt=credential_prompt,
                host="cxnfs.example.internal",
                control_port=1443,
                transport_port=1444,
                server_hostname="cxnfs.example.internal",
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
                trainer_prefetch_batches=0,
            ) as stream:
                self.assertIsInstance(stream, CxnfsTrainerStream)
                self.assertEqual(CxnfsTrainingShard(1, 2), stream.dataset.shard)
                batch = stream.next_batch()

            self.assertEqual(1, len(batch.samples))
            self.assertEqual(1, len(prompted_credentials))
            self.assertTrue(prompted_credentials[0].closed)
            self.assertEqual(1, batch.resume_cursor.shard_index)
            self.assertEqual(2, batch.resume_cursor.shard_count)
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))

    def test_trainer_runtime_smoke_payload_verifies_pipeline_loop_for_direct_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-tensors",
                batch_size=1,
                batches=2,
                trainer_prefetch_batches=0,
                samples_per_epoch=3,
            )
            handoff = build_trainer_handoff_payload(contract)

            with patch.object(torch_dataset_module, "_require_torch", lambda: object()), \
                patch.object(torch_dataset_module, "_torch_dtype", lambda _torch, _dtype: "uint8"), \
                patch.object(torch_dataset_module, "_dtype_nbytes", lambda _dtype: 1), \
                patch.object(torch_dataset_module, "_torch_empty", lambda _torch, shape, **_kwargs: FakeTensor(shape[0])):
                payload = build_trainer_runtime_smoke_payload(
                    handoff,
                    batches=2,
                    trainer_prefetch_batches=0,
                )

            self.assertEqual(TRAINER_RUNTIME_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual("direct-tensors", payload["read_mode"])
            self.assertTrue(payload["pipeline_training_loop_required"])
            self.assertTrue(payload["pipeline_training_loop_verified"])
            self.assertTrue(payload["pipeline_training_loop_context_used"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                payload["pipeline_training_loop_context"],
            )
            self.assertEqual("CxnfsTrainerPipelineLoop", payload["pipeline_training_loop_object"])
            pipeline_report = payload["pipeline_training_loop_report"]
            self.assertEqual(2, pipeline_report["batch_count"])
            self.assertEqual(2, pipeline_report["sample_count"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, pipeline_report["stream_state_format"])
            self.assertTrue(payload["pipeline_network_path_verified"])
            self.assertTrue(pipeline_report["pipeline_network_path_complete"])
            self.assertEqual(
                "cxnfs.trainer-pipeline-network-path.v1",
                pipeline_report["pipeline_network_path_format"],
            )

    def test_trainer_runtime_smoke_cli_streams_remote_binding_with_prompted_credentials(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            handoff_path = root / "trainer-handoff.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            controls = []
            bulks = []
            prompted_credentials = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                prompted_credentials.append(credentials)
                return credentials

            help_stdout = io.StringIO()
            help_stderr = io.StringIO()
            help_exit = run_trainer_runtime_smoke_cli(["--help"], stdout=help_stdout, stderr=help_stderr)

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--rank", help_stdout.getvalue())
            self.assertIn("--world-size", help_stdout.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            exit_code = run_trainer_runtime_smoke_cli(
                [
                    str(handoff_path),
                    "--rank",
                    "1",
                    "--world-size",
                    "2",
                    "--batches",
                    "1",
                    "--trainer-prefetch-batches",
                    "0",
                    "--host",
                    "cxnfs.example.internal",
                    "--control-port",
                    "1443",
                    "--transport-port",
                    "1444",
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_RUNTIME_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(TRAINER_RUNTIME_BINDING_FORMAT, payload["runtime_binding_format"])
            self.assertEqual("CxnfsTrainerRuntimeBinding", payload["runtime_binding_object"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.open_training_loop", payload["training_loop_context"])
            self.assertEqual({"index": 1, "count": 2}, payload["training_shard"])
            self.assertEqual(1, payload["rank"])
            self.assertEqual(2, payload["world_size"])
            self.assertTrue(payload["remote_client_required"])
            self.assertTrue(payload["requires_credentials"])
            self.assertTrue(payload["credential_prompt_used"])
            self.assertFalse(payload["client_factory_injected"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertTrue(payload["stream_opened"])
            self.assertEqual("native", payload["trainer_batch_iterator"])
            self.assertEqual(0, payload["data_loader_workers"])
            self.assertFalse(payload["dataloader_worker_partitioning_enabled"])
            self.assertTrue(payload["worker_partition_complete"])
            self.assertFalse(payload["worker_batch_read_evidence_observed"])
            self.assertEqual(0, payload["worker_batch_read_evidence_count"])
            self.assertTrue(payload["remote_client_warmup_complete"])
            self.assertEqual(2, payload["remote_client_warmup_source_count"])
            self.assertEqual(2, payload["remote_client_warmup_warmed_source_count"])
            self.assertTrue(payload["remote_client_topology_complete"])
            self.assertTrue(payload["remote_client_per_source_complete"])
            self.assertTrue(payload["remote_client_lifecycle_closed"])
            self.assertEqual(1, payload["batch_count"])
            self.assertEqual(1, payload["sample_count"])
            self.assertEqual("cxnfs.trainer-stream-state.v1", payload["stream_state_format"])
            self.assertEqual("cxnfs-length-prefixed", payload["stream_report"]["transport"])
            self.assertEqual("stream", payload["stream_report"]["bulk_transport"])
            self.assertEqual(1, len(prompted_credentials))
            self.assertTrue(prompted_credentials[0].closed)
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))

    def test_trainer_distributed_runtime_smoke_payload_reads_rank_local_batches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1", "summary-2", "summary-3"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=2,
                trainer_prefetch_batches=0,
                samples_per_epoch=4,
            )
            handoff = build_trainer_handoff_payload(contract)

            payload = build_trainer_distributed_runtime_smoke_payload(
                handoff,
                world_size=2,
                batches_per_rank=2,
                trainer_prefetch_batches=0,
            )

            self.assertEqual(TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(TRAINER_RUNTIME_SMOKE_FORMAT, payload["runtime_smoke_format"])
            self.assertEqual(TRAINER_RUNTIME_BINDING_FORMAT, payload["runtime_binding_format"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.open_training_loop", payload["training_loop_context"])
            self.assertEqual(2, payload["world_size"])
            self.assertEqual([0, 1], payload["ranks"])
            self.assertEqual(2, payload["rank_count"])
            self.assertEqual(4, payload["total_batch_count"])
            self.assertEqual(4, payload["total_sample_count"])
            self.assertFalse(payload["remote_client_required"])
            self.assertFalse(payload["credential_prompt_used"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertTrue(payload["all_rank_streams_opened"])
            self.assertTrue(payload["all_rank_stream_kwargs_accepted"])
            self.assertTrue(payload["all_rank_state_matches_final_cursor"])
            self.assertTrue(payload["all_rank_training_loop_context_used"])
            self.assertTrue(payload["all_rank_training_shards_correct"])
            self.assertEqual("local-mds", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertTrue(payload["read_path_reliability"]["complete"])
            self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
            self.assertTrue(payload["rank_sample_overlap"]["no_cross_rank_overlap"])
            self.assertEqual(4, payload["rank_sample_overlap"]["unique_observed_samples"])
            self.assertEqual({"index": 0, "count": 2}, payload["rank_smokes"][0]["training_shard"])
            self.assertEqual({"index": 1, "count": 2}, payload["rank_smokes"][1]["training_shard"])

    def test_trainer_distributed_runtime_smoke_cli_prompts_once_for_remote_ranks(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=3,
                trainer_prefetch_batches=0,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            handoff_path = root / "trainer-handoff.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            controls = []
            bulks = []
            prompted_credentials = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                prompted_credentials.append(credentials)
                return credentials

            help_stdout = io.StringIO()
            help_stderr = io.StringIO()
            help_exit = run_trainer_distributed_runtime_smoke_cli(
                ["--help"],
                stdout=help_stdout,
                stderr=help_stderr,
            )

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--world-size", help_stdout.getvalue())
            self.assertIn("--batches-per-rank", help_stdout.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertIn("--write-rank-states-dir", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            rank_states_dir = root / "rank-states"
            exit_code = run_trainer_distributed_runtime_smoke_cli(
                [
                    str(handoff_path),
                    "--world-size",
                    "2",
                    "--batches-per-rank",
                    "3",
                    "--host",
                    "cxnfs.example.internal",
                    "--control-port",
                    "1443",
                    "--transport-port",
                    "1444",
                    "--write-rank-states-dir",
                    str(rank_states_dir),
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(2, payload["world_size"])
            self.assertEqual(2, payload["rank_count"])
            self.assertEqual(6, payload["total_batch_count"])
            self.assertEqual(6, payload["total_sample_count"])
            self.assertTrue(payload["remote_client_required"])
            self.assertTrue(payload["requires_credentials"])
            self.assertTrue(payload["credential_prompt_used"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertTrue(payload["all_rank_streams_opened"])
            self.assertTrue(payload["all_rank_state_matches_final_cursor"])
            self.assertTrue(payload["all_rank_training_shards_correct"])
            self.assertEqual("cxnfs-length-prefixed", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertTrue(payload["rank_sample_overlap"]["no_cross_rank_overlap"])
            self.assertTrue(payload["remote_client_topology_after_close"]["all_open_clients_closed"])
            self.assertTrue(payload["remote_client_topology_after_close"]["complete"])
            self.assertEqual(2, payload["rank_state_file_count"])
            self.assertTrue(payload["all_rank_state_files_written"])
            self.assertEqual("cxnfs.trainer-distributed-runtime-rank-states.v1", payload["rank_state_artifacts"]["format"])
            self.assertEqual(2, len(payload["rank_state_artifacts"]["artifacts"]))
            for rank in (0, 1):
                state_path = rank_states_dir / f"rank-{rank:05d}.trainer-stream-state.json"
                saved_state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(TRAINER_STREAM_STATE_FORMAT, saved_state["format"])
                self.assertEqual({"index": rank, "count": 2}, saved_state["cursor"]["shard"])
            self.assertEqual(1, len(prompted_credentials))
            self.assertTrue(prompted_credentials[0].closed)
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))

    def test_trainer_readiness_bundle_payload_writes_component_reports_and_rank_states(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1", "summary-2", "summary-3"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=4,
            )
            handoff = build_trainer_handoff_payload(contract)
            output_dir = root / "trainer-readiness"

            payload = build_trainer_readiness_bundle_payload(
                handoff,
                output_dir=output_dir,
                integration_batches=1,
                runtime_batches=1,
                world_size=2,
                batches_per_rank=1,
                loop_steps=2,
            )

            self.assertEqual(TRAINER_READINESS_BUNDLE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertTrue(payload["ready_for_trainer_wiring"])
            self.assertFalse(payload["remote_client_required"])
            self.assertFalse(payload["requires_low_level_direct_read_proofs"])
            self.assertFalse(payload["low_level_direct_read_proofs_complete"])
            self.assertFalse(payload["direct_quic_memory_path_verified"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertTrue(payload["direct_python_streaming_verified"])
            self.assertTrue(payload["runtime_training_loop_verified"])
            self.assertTrue(payload["distributed_runtime_verified"])
            self.assertTrue(payload["distributed_rank_states_written"])
            self.assertTrue(payload["loop_resume_verified"])
            self.assertEqual(2, payload["world_size"])
            self.assertEqual(0, payload["runtime_rank"])
            self.assertEqual(5, payload["report_artifacts"]["artifact_count"])
            self.assertEqual(
                {
                    "trainer-preflight.json",
                    "trainer-integration-smoke.json",
                    "trainer-runtime-smoke.json",
                    "trainer-distributed-runtime-smoke.json",
                    "trainer-loop-replay.json",
                },
                {artifact["name"] for artifact in payload["report_artifacts"]["artifacts"]},
            )
            self.assertEqual(TRAINER_INTEGRATION_PREFLIGHT_FORMAT, payload["reports"]["preflight"]["format"])
            self.assertEqual(TRAINER_INTEGRATION_SMOKE_FORMAT, payload["reports"]["integration_smoke"]["format"])
            self.assertEqual(TRAINER_RUNTIME_SMOKE_FORMAT, payload["reports"]["runtime_smoke"]["format"])
            self.assertEqual(
                TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT,
                payload["reports"]["distributed_runtime_smoke"]["format"],
            )
            self.assertEqual(TRAINER_LOOP_REPLAY_FORMAT, payload["reports"]["loop_replay"]["format"])
            self.assertEqual("cxnfs.trainer-distributed-runtime-rank-states.v1", payload["rank_state_artifacts"]["format"])
            self.assertTrue(payload["rank_state_artifacts"]["all_rank_state_files_written"])
            self.assertEqual(2, payload["rank_state_artifacts"]["rank_state_file_count"])
            for rank in (0, 1):
                state_path = output_dir / "rank-states" / f"rank-{rank:05d}.trainer-stream-state.json"
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state["format"])
                self.assertEqual({"index": rank, "count": 2}, state["cursor"]["shard"])
            self.assertEqual(
                TRAINER_LOOP_REPLAY_FORMAT,
                json.loads((output_dir / "trainer-loop-replay.json").read_text(encoding="utf-8"))["format"],
            )

    def test_trainer_readiness_bundle_cli_prompts_once_and_writes_remote_bundle(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1", "summary-2", "summary-3"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=4,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            handoff_path = root / "trainer-handoff.json"
            output_dir = root / "trainer-readiness"
            output = root / "trainer-readiness-bundle.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            controls = []
            bulks = []
            prompted_credentials = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                prompted_credentials.append(credentials)
                return credentials

            help_stdout = io.StringIO()
            help_stderr = io.StringIO()
            help_exit = run_trainer_readiness_bundle_cli(["--help"], stdout=help_stdout, stderr=help_stderr)

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--output-dir", help_stdout.getvalue())
            self.assertIn("--world-size", help_stdout.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            exit_code = run_trainer_readiness_bundle_cli(
                [
                    str(handoff_path),
                    "--output-dir",
                    str(output_dir),
                    "--output",
                    str(output),
                    "--world-size",
                    "2",
                    "--runtime-rank",
                    "1",
                    "--integration-batches",
                    "1",
                    "--runtime-batches",
                    "1",
                    "--batches-per-rank",
                    "1",
                    "--loop-steps",
                    "2",
                    "--host",
                    "cxnfs.example.internal",
                    "--control-port",
                    "1443",
                    "--transport-port",
                    "1444",
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_READINESS_BUNDLE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertTrue(payload["remote_client_required"])
            self.assertTrue(payload["requires_credentials"])
            self.assertFalse(payload["requires_low_level_direct_read_proofs"])
            self.assertFalse(payload["direct_quic_memory_path_verified"])
            self.assertTrue(payload["credential_prompt_used"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertTrue(payload["direct_python_streaming_verified"])
            self.assertTrue(payload["distributed_rank_states_written"])
            self.assertEqual(2, payload["world_size"])
            self.assertEqual(1, payload["runtime_rank"])
            self.assertEqual("stream", payload["reports"]["runtime_smoke"]["stream_report"]["bulk_transport"])
            self.assertEqual("cxnfs-length-prefixed", payload["reports"]["runtime_smoke"]["stream_report"]["transport"])
            self.assertEqual(1, len(prompted_credentials))
            self.assertTrue(prompted_credentials[0].closed)
            self.assertGreaterEqual(len(controls), 2)
            self.assertEqual(len(controls), len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual("cxnfs.trainer-distributed-runtime-rank-states.v1", payload["rank_state_artifacts"]["format"])
            self.assertTrue(payload["rank_state_artifacts"]["all_rank_state_files_written"])
            for rank in (0, 1):
                state_path = output_dir / "rank-states" / f"rank-{rank:05d}.trainer-stream-state.json"
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state["format"])
                self.assertEqual({"index": rank, "count": 2}, state["cursor"]["shard"])
            self.assertEqual(
                TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT,
                json.loads((output_dir / "trainer-distributed-runtime-smoke.json").read_text(encoding="utf-8"))["format"],
            )

    def test_trainer_handoff_acceptance_payload_accepts_existing_local_handoff(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1", "summary-2", "summary-3"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=4,
            )
            handoff = build_trainer_handoff_payload(contract)
            output_dir = root / "trainer-handoff-acceptance"

            payload = build_trainer_handoff_acceptance_payload(
                handoff,
                output_dir=output_dir,
                integration_batches=1,
                runtime_batches=1,
                world_size=2,
                batches_per_rank=1,
                loop_steps=2,
            )

            self.assertEqual(TRAINER_HANDOFF_ACCEPTANCE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual("passed", payload["status"])
            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["handoff_format"])
            self.assertEqual(TRAINER_READINESS_BUNDLE_FORMAT, payload["readiness_bundle_format"])
            self.assertFalse(payload["remote_client_required"])
            self.assertFalse(payload["requires_credentials"])
            self.assertFalse(payload["credential_prompt_used"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertFalse(payload["requires_mount"])
            self.assertFalse(payload["trainer_code_touched"])
            self.assertTrue(payload["ready_for_trainer_wiring"])
            self.assertTrue(payload["direct_python_streaming_verified"])
            self.assertTrue(payload["runtime_training_loop_verified"])
            self.assertTrue(payload["distributed_runtime_verified"])
            self.assertTrue(payload["loop_resume_verified"])
            self.assertTrue(payload["cross_rank_overlap_free"])
            self.assertTrue(payload["rank_states_written"])
            self.assertEqual("CxnfsTrainerRuntimeBinding", payload["runtime_binding_object"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_training_loop",
                payload["training_loop_context"],
            )
            self.assertFalse(payload["pipeline_training_loop_required"])
            self.assertTrue(payload["pipeline_training_loop_verified"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                payload["pipeline_training_loop_context"],
            )
            self.assertEqual("CxnfsTrainerPipelineLoop", payload["pipeline_training_loop_object"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.from_file",
                payload["public_python_surface"]["runtime_binding_loader"],
            )
            contract_surface = payload["trainer_loop_contract"]
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, payload["trainer_loop_contract_format"])
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, contract_surface["format"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.from_file", contract_surface["entrypoint"]["runtime_binding_loader"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_training_loop",
                contract_surface["entrypoint"]["training_loop_context"],
            )
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                contract_surface["entrypoint"]["pipeline_training_loop_context"],
            )
            self.assertEqual("CxnfsTrainerPipelineLoop", contract_surface["entrypoint"]["pipeline_loop_object"])
            self.assertFalse(contract_surface["mount_free_contract"]["requires_mount"])
            self.assertFalse(contract_surface["mount_free_contract"]["touches_trainer_code"])
            self.assertFalse(contract_surface["authentication"]["password_cli_argument"])
            self.assertFalse(contract_surface["authentication"]["raw_password_sent_over_control"])
            pipeline_contract = contract_surface["pipeline_contract"]
            self.assertEqual("CxnfsTrainingPipeline", pipeline_contract["training_pipeline_object"])
            self.assertEqual(
                "CxnfsTrainingPipeline.read_tensor_batch",
                pipeline_contract["training_pipeline_context"],
            )
            self.assertEqual(
                "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors",
                pipeline_contract["pipeline_descriptor_iterator"],
            )
            self.assertEqual("CxnfsTrainerPipelineStream", pipeline_contract["pipeline_descriptor_stream"])
            self.assertEqual("CxnfsTrainerPipelineLoop", pipeline_contract["pipeline_loop_object"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, pipeline_contract["pipeline_stream_state_format"])
            self.assertEqual(
                "CxnfsTrainerPipelineStream.save_state_file",
                pipeline_contract["pipeline_stream_save_method"],
            )
            self.assertEqual(
                "CxnfsTrainerPipelineStream.load_state_file",
                pipeline_contract["pipeline_stream_load_method"],
            )
            self.assertTrue(pipeline_contract["pipeline_stream_validates_resume_contract"])
            self.assertTrue(pipeline_contract["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(pipeline_contract["supports_pipeline_torch_compile"])
            self.assertTrue(pipeline_contract["python_byte_batch_avoidance_supported"])
            self.assertFalse(pipeline_contract["direct_tensor_read_mode_selected"])
            self.assertIn("sample_count", contract_surface["batch_contract"]["fields"])
            self.assertIn("resume_cursor", contract_surface["batch_contract"]["fields"])
            self.assertIn("data", contract_surface["batch_contract"]["sample_fields"])
            step_trace_contract = contract_surface["step_trace_contract"]
            self.assertEqual("cxnfs.trainer-step-trace.v1", step_trace_contract["report_format"])
            self.assertIn("sample_identities", step_trace_contract["fields"])
            self.assertTrue(step_trace_contract["requires_cursor_delta_verified"])
            self.assertTrue(step_trace_contract["requires_resume_split_continuity"])
            self.assertTrue(step_trace_contract["requires_unique_sample_identities_by_default"])
            self.assertEqual(
                "source_name:source_index:sample_index",
                step_trace_contract["sample_identity_format"],
            )
            self.assertEqual(2, contract_surface["distributed_contract"]["world_size"])
            self.assertTrue(contract_surface["distributed_contract"]["cross_rank_overlap_free"])
            self.assertTrue(contract_surface["resume_contract"]["loop_resume_verified"])
            self.assertIn("for batch in stream:", "\n".join(contract_surface["minimal_python"]["lines"]))
            contract_path = output_dir / "trainer-loop-contract.json"
            self.assertTrue(contract_path.exists())
            written_contract = json.loads(contract_path.read_text(encoding="utf-8"))
            self.assertEqual(contract_surface, written_contract)
            self.assertFalse(payload["trainer_handoff_surface"]["password_cli_argument"])
            self.assertGreaterEqual(payload["artifacts"]["artifact_count"], 7)
            for name in (
                "trainer-handoff.json",
                "trainer-readiness-bundle.json",
                "trainer-loop-contract.json",
                "trainer-preflight.json",
                "trainer-integration-smoke.json",
                "trainer-runtime-smoke.json",
                "trainer-distributed-runtime-smoke.json",
                "trainer-loop-replay.json",
            ):
                self.assertTrue((output_dir / name).exists(), name)
            self.assertTrue((output_dir / "rank-states" / "rank-00000.trainer-stream-state.json").exists())
            self.assertTrue((output_dir / "rank-states" / "rank-00001.trainer-stream-state.json").exists())

    def test_trainer_handoff_acceptance_cli_prompts_once_for_remote_handoff(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1", "summary-2", "summary-3"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=4,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            handoff_path = root / "trainer-handoff.json"
            output_dir = root / "trainer-handoff-acceptance"
            output = root / "trainer-handoff-acceptance.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            controls = []
            bulks = []
            prompted_credentials = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                prompted_credentials.append(credentials)
                return credentials

            help_stdout = io.StringIO()
            help_stderr = io.StringIO()
            help_exit = run_trainer_handoff_acceptance_cli(["--help"], stdout=help_stdout, stderr=help_stderr)

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--output-dir", help_stdout.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            exit_code = run_trainer_handoff_acceptance_cli(
                [
                    str(handoff_path),
                    "--output-dir",
                    str(output_dir),
                    "--output",
                    str(output),
                    "--world-size",
                    "2",
                    "--runtime-rank",
                    "1",
                    "--integration-batches",
                    "1",
                    "--runtime-batches",
                    "1",
                    "--batches-per-rank",
                    "1",
                    "--loop-steps",
                    "2",
                    "--host",
                    "cxnfs.example.internal",
                    "--control-port",
                    "1443",
                    "--transport-port",
                    "1444",
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_HANDOFF_ACCEPTANCE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertTrue(payload["remote_client_required"])
            self.assertTrue(payload["requires_credentials"])
            self.assertTrue(payload["credential_prompt_used"])
            self.assertFalse(payload["raw_password_sent_over_control"])
            self.assertFalse(payload["trainer_handoff_surface"]["password_cli_argument"])
            contract_surface = payload["trainer_loop_contract"]
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, contract_surface["format"])
            self.assertTrue(contract_surface["authentication"]["credential_prompt_required_for_remote"])
            self.assertFalse(contract_surface["authentication"]["password_cli_argument"])
            self.assertFalse(contract_surface["authentication"]["raw_password_sent_over_control"])
            self.assertTrue(contract_surface["remote_transport_contract"]["per_source_client_default"])
            self.assertTrue(contract_surface["pipeline_contract"]["supports_pipeline_direct_tensor_batches"])
            self.assertTrue(contract_surface["pipeline_contract"]["python_byte_batch_avoidance_supported"])
            self.assertTrue(payload["direct_python_streaming_verified"])
            self.assertTrue(payload["distributed_runtime_verified"])
            self.assertEqual(1, len(prompted_credentials))
            self.assertTrue(prompted_credentials[0].closed)
            self.assertGreaterEqual(len(controls), 2)
            self.assertEqual(len(controls), len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual(
                TRAINER_READINESS_BUNDLE_FORMAT,
                json.loads((output_dir / "trainer-readiness-bundle.json").read_text(encoding="utf-8"))["format"],
            )
            loop_contract = json.loads((output_dir / "trainer-loop-contract.json").read_text(encoding="utf-8"))
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, loop_contract["format"])
            self.assertTrue(loop_contract["authentication"]["credential_prompt_required_for_remote"])
            self.assertFalse(loop_contract["authentication"]["password_cli_argument"])

    def test_trainer_loop_contract_check_cli_validates_standalone_contracts(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "trainer-fixture"
            output = root / "contract-check.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            fixture = build_trainer_acceptance_fixture_payload(
                root / "mds",
                "chat=2,summary=1",
                output_dir=output_dir,
                batch_size=2,
                contract_batches=2,
                samples_per_epoch=6,
                trainer_batch_iterator="trainer-dataloader",
                data_loader_workers=0,
                world_size=2,
                loop_steps=2,
            )
            contract_path = output_dir / "trainer-loop-contract.json"
            stdout = io.StringIO()
            stderr = io.StringIO()
            help_stdout = io.StringIO()
            help_stderr = io.StringIO()

            help_exit = run_trainer_loop_contract_check_cli(
                ["--help"],
                stdout=help_stdout,
                stderr=help_stderr,
            )
            exit_code = run_trainer_loop_contract_check_cli(
                [
                    str(contract_path),
                    "--min-world-size",
                    "2",
                    "--require-rank-states",
                    "--require-prefetch",
                    "--require-pipeline",
                    "--require-torch-compile",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, help_exit, help_stderr.getvalue())
            self.assertIn("--require-remote-quic", help_stdout.getvalue())
            self.assertIn("--require-pipeline", help_stdout.getvalue())
            self.assertIn("--require-direct-tensors", help_stdout.getvalue())
            self.assertIn("--require-torch-compile", help_stdout.getvalue())
            self.assertNotIn("--password", help_stdout.getvalue())
            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_LOOP_CONTRACT_CHECK_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual([], payload["failed_checks"])
            self.assertEqual(str(contract_path), payload["contract_path"])
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, payload["contract_format"])
            self.assertTrue(payload["checks"]["trainer_prefetch_enabled"])
            self.assertTrue(payload["checks"]["rank_states_written"])
            self.assertTrue(payload["checks"]["pipeline_contract_present"])
            self.assertTrue(payload["checks"]["training_pipeline_object_valid"])
            self.assertTrue(payload["checks"]["pipeline_stream_state_format_valid"])
            self.assertTrue(payload["checks"]["pipeline_stream_save_method_valid"])
            self.assertTrue(payload["checks"]["pipeline_stream_load_method_valid"])
            self.assertTrue(payload["checks"]["pipeline_stream_validates_resume_contract"])
            self.assertTrue(payload["checks"]["pipeline_direct_tensor_batches_supported"])
            self.assertTrue(payload["checks"]["pipeline_torch_compile_supported"])
            self.assertTrue(payload["checks"]["pipeline_training_loop_context_valid"])
            self.assertTrue(payload["checks"]["pipeline_loop_object_valid"])
            self.assertTrue(payload["checks"]["step_trace_contract_present"])
            self.assertTrue(payload["checks"]["step_trace_format_valid"])
            self.assertTrue(payload["checks"]["step_trace_fields_complete"])
            self.assertTrue(payload["checks"]["step_trace_cursor_delta_required"])
            self.assertTrue(payload["checks"]["step_trace_resume_continuity_required"])
            self.assertTrue(payload["checks"]["step_trace_identity_uniqueness_default"])
            self.assertEqual(
                fixture["trainer_loop_contract"]["pipeline_contract"],
                payload["pipeline_contract"],
            )
            self.assertEqual(
                fixture["trainer_loop_contract"]["stream_contract"],
                payload["stream_contract"],
            )
            remote_contract_path = root / "remote-trainer-loop-contract.json"
            remote_contract = json.loads(json.dumps(fixture["trainer_loop_contract"]))
            remote_contract["authentication"]["remote_client_required"] = True
            remote_contract["authentication"]["requires_credentials"] = True
            remote_contract["authentication"]["credential_prompt_required_for_remote"] = True
            remote_contract["remote_transport_contract"]["bulk_transport"] = "quic"
            remote_contract["remote_transport_contract"]["max_cords"] = torch_dataset_module.ABSOLUTE_MAX_CORDS
            remote_contract["remote_transport_contract"]["per_source_client_default"] = True
            remote_contract["remote_transport_contract"]["ready_for_remote_quic"] = True
            remote_contract["remote_transport_contract"]["direct_quic_memory_path_verified"] = True
            remote_contract["stream_contract"]["read_mode"] = "direct-tensors"
            remote_contract["pipeline_contract"]["selected_read_mode"] = "direct-tensors"
            remote_contract["pipeline_contract"]["direct_tensor_read_mode_selected"] = True
            remote_contract["pipeline_contract"]["tensor_allocation"] = CxnfsTrainerTensorAllocation().to_dict()
            remote_contract["pipeline_contract"]["tensor_memory_path"] = (
                CxnfsTrainerTensorAllocation().memory_path()
            )
            remote_contract["pipeline_contract"]["network_fills_tensor_destination_supported"] = True
            remote_contract["pipeline_contract"]["pipeline_network_path"] = {
                "format": "cxnfs.trainer-pipeline-network-path.v1",
                "complete": True,
                "network_read_workers": 16,
                "max_cords": torch_dataset_module.ABSOLUTE_MAX_CORDS,
                "active_pipeline_prefetch_batches": 2,
                "direct_destination_read_operations": 2,
            }
            remote_contract["pipeline_contract"]["pipeline_network_path_verified"] = True
            remote_contract["pipeline_contract"]["direct_tensor_transport_proof_complete"] = True
            remote_contract["pipeline_contract"]["direct_tensor_transport_cipher_suite"] = (
                "TLS_AES_128_GCM_SHA256"
            )
            remote_contract_path.write_text(
                json.dumps(remote_contract, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            remote_stdout = io.StringIO()
            remote_stderr = io.StringIO()

            remote_exit = run_trainer_loop_contract_check_cli(
                [
                    str(remote_contract_path),
                    "--require-remote-quic",
                    "--min-world-size",
                    "2",
                    "--require-rank-states",
                    "--require-prefetch",
                    "--require-pipeline",
                    "--require-direct-tensors",
                    "--require-torch-compile",
                ],
                stdout=remote_stdout,
                stderr=remote_stderr,
            )

            self.assertEqual(0, remote_exit, remote_stderr.getvalue())
            remote_payload = json.loads(remote_stdout.getvalue())
            self.assertTrue(remote_payload["checks"]["remote_quic_bulk_transport"])
            self.assertTrue(remote_payload["checks"]["remote_direct_quic_memory_verified"])
            self.assertTrue(remote_payload["checks"]["pipeline_direct_tensor_read_mode_selected"])
            self.assertTrue(remote_payload["checks"]["pipeline_python_byte_batch_avoided"])
            self.assertTrue(remote_payload["checks"]["pipeline_network_path_complete"])

    def test_trainer_loop_contract_check_cli_rejects_missing_direct_tensor_pipeline(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "trainer-fixture"
            bad_contract_path = root / "bad-trainer-loop-contract.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            fixture = build_trainer_acceptance_fixture_payload(
                root / "mds",
                "chat=1",
                output_dir=output_dir,
                batch_size=1,
                contract_batches=1,
                samples_per_epoch=2,
                world_size=1,
                loop_steps=2,
            )
            bad_contract = json.loads(json.dumps(fixture["trainer_loop_contract"]))
            bad_contract["pipeline_contract"]["direct_tensor_read_mode_selected"] = False
            bad_contract["pipeline_contract"]["tensor_allocation"] = None
            bad_contract["pipeline_contract"]["tensor_memory_path"] = None
            bad_contract_path.write_text(json.dumps(bad_contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_loop_contract_check_cli(
                [str(bad_contract_path), "--require-direct-tensors"],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["passed"])
            self.assertIn("pipeline_direct_tensor_read_mode_selected", payload["failed_checks"])
            self.assertIn("pipeline_tensor_allocation_declared", payload["failed_checks"])

    def test_trainer_loop_contract_check_cli_rejects_password_cli_argument(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "trainer-fixture"
            bad_contract_path = root / "bad-trainer-loop-contract.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            fixture = build_trainer_acceptance_fixture_payload(
                root / "mds",
                "chat=1",
                output_dir=output_dir,
                batch_size=1,
                contract_batches=1,
                samples_per_epoch=2,
                world_size=1,
                loop_steps=2,
            )
            bad_contract = dict(fixture["trainer_loop_contract"])
            bad_contract["authentication"] = dict(bad_contract["authentication"])
            bad_contract["authentication"]["password_cli_argument"] = True
            bad_contract_path.write_text(json.dumps(bad_contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_loop_contract_check_cli(
                [str(bad_contract_path)],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["passed"])
            self.assertIn("password_cli_argument_absent", payload["failed_checks"])

    def test_trainer_contract_handoff_preflight_preserves_explicit_training_shard(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract_path = root / "trainer-contract.json"
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_contract_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "1",
                    "--batches",
                    "1",
                    "--samples-per-epoch",
                    "3",
                    "--rank",
                    "1",
                    "--world-size",
                    "2",
                    "--output",
                    str(contract_path),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            contract = json.loads(contract_path.read_text(encoding="utf-8"))
            self.assertEqual({"index": 1, "count": 2}, contract["read_contract"]["training_shard"])
            self.assertEqual(
                {"index": 1, "count": 2},
                contract["read_contract"]["worker_partitioning"]["base_training_shard"],
            )
            handoff = build_trainer_handoff_payload(contract)
            stream_kwargs = handoff["integration_plan"]["stream_kwargs"]
            self.assertEqual({"index": 1, "count": 2}, stream_kwargs["shard"])
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)
            runtime_kwargs = plan.stream_kwargs()
            self.assertIsInstance(runtime_kwargs["shard"], CxnfsTrainingShard)
            self.assertEqual(1, runtime_kwargs["shard"].index)
            self.assertEqual(2, runtime_kwargs["shard"].count)
            preflight = plan.preflight_report()
            self.assertEqual({"index": 1, "count": 2}, preflight["training_shard"])
            self.assertEqual(1, preflight["rank"])
            self.assertEqual(2, preflight["world_size"])

    def test_trainer_integration_smoke_opens_stream_from_handoff_plan(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=3,
            )
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["transport"] = "local-mds"
            workload["bulk_transport"] = "stream"
            workload["benchmark_provenance"]["invocation"]["bulk_transport"] = "stream"
            workload["trainer_prefetch_batches"] = contract["read_contract"]["trainer_prefetch_batches"]
            workload["benchmark_provenance"]["invocation"]["trainer_prefetch_batches"] = contract["read_contract"]["trainer_prefetch_batches"]
            matrix = proven_surface_matrix_payload(contract)
            matrix["trainer_prefetch_batches"] = contract["read_contract"]["trainer_prefetch_batches"]
            handoff = build_trainer_handoff_payload(
                contract,
                workload_payload=workload,
                matrix_payload=matrix,
                require_workload=True,
                require_matrix=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
                min_workload_runs=2,
                min_workload_samples=8,
                min_workload_bytes=128,
                min_cord_sweep=2,
            )

            payload = build_trainer_integration_smoke_payload(handoff, batches=2)

            self.assertEqual(TRAINER_INTEGRATION_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["handoff_format"])
            self.assertEqual(TRAINER_INTEGRATION_PLAN_FORMAT, payload["plan_format"])
            self.assertEqual("open_weighted_mds_trainer_stream", payload["stream_factory"])
            self.assertFalse(payload["remote_client_required"])
            self.assertFalse(payload["requires_mount"])
            self.assertFalse(payload["touches_trainer_code"])
            self.assertTrue(payload["stream_opened"])
            self.assertTrue(payload["stream_kwargs_accepted"])
            self.assertNotIn("trainer_prefetch_batches", payload["stream_kwargs_keys"])
            self.assertEqual(1, payload["trainer_prefetch_batches"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", payload["training_loop_context"])
            self.assertTrue(payload["training_loop_context_used"])
            self.assertEqual("CxnfsTrainerPrefetchStream", payload["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", payload["training_loop_base_stream_object"])
            self.assertEqual(1, payload["training_loop_prefetch_batches"])
            self.assertTrue(payload["training_loop_prefetch_applied"])
            self.assertEqual("native", payload["trainer_batch_iterator"])
            self.assertEqual(0, payload["data_loader_workers"])
            self.assertFalse(payload["dataloader_worker_partitioning_enabled"])
            self.assertTrue(payload["worker_partition_complete"])
            self.assertFalse(payload["worker_batch_read_evidence_observed"])
            self.assertEqual(0, payload["worker_batch_read_evidence_count"])
            self.assertTrue(payload["remote_client_warmup_complete"])
            self.assertEqual(2, payload["remote_client_warmup_source_count"])
            self.assertEqual(0, payload["remote_client_warmup_warmed_source_count"])
            self.assertTrue(payload["remote_client_topology_complete"])
            self.assertTrue(payload["remote_client_per_source_complete"])
            self.assertTrue(payload["remote_client_lifecycle_closed"])
            self.assertEqual(2, payload["batch_count"])
            self.assertEqual(3, payload["sample_count"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["stream_state_format"])
            self.assertTrue(payload["stream_state_matches_final_cursor"])
            self.assertEqual("CxnfsTrainerStream", payload["stream_object"])
            self.assertEqual("CxnfsTrainerBatch", payload["batch_object"])

    def test_trainer_acceptance_fixture_writes_local_trainer_surface_bundle(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "trainer-fixture"
            output = root / "trainer-fixture.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))

            payload = build_trainer_acceptance_fixture_payload(
                root / "mds",
                "chat=2,summary=1",
                output_dir=output_dir,
                batch_size=2,
                contract_batches=2,
                samples_per_epoch=6,
                trainer_batch_iterator="trainer-dataloader",
                data_loader_workers=0,
                world_size=2,
                loop_steps=2,
            )

            self.assertEqual(TRAINER_ACCEPTANCE_FIXTURE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertFalse(payload["trainer_code_touched"])
            self.assertFalse(payload["requires_mount"])
            self.assertFalse(payload["remote_client_required"])
            self.assertTrue(payload["ready_for_trainer_wiring"])
            self.assertTrue(payload["direct_python_streaming_verified"])
            self.assertTrue(payload["runtime_training_loop_verified"])
            self.assertTrue(payload["distributed_runtime_verified"])
            self.assertTrue(payload["loop_resume_verified"])
            self.assertTrue(payload["cross_rank_overlap_free"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.open_training_loop", payload["training_loop_context"])
            self.assertEqual("CxnfsTrainerStream", payload["stream_object"])
            self.assertEqual("CxnfsTrainerBatch", payload["batch_object"])
            self.assertEqual("trainer-dataloader", payload["trainer_batch_iterator"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.from_file", payload["public_python_surface"]["runtime_binding_loader"])
            contract_surface = payload["trainer_loop_contract"]
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, payload["trainer_loop_contract_format"])
            self.assertEqual(TRAINER_LOOP_CONTRACT_FORMAT, contract_surface["format"])
            self.assertEqual(str(output_dir / "trainer-handoff.json"), contract_surface["artifact_paths"]["handoff"])
            self.assertEqual(str(output_dir / "rank-states"), contract_surface["artifact_paths"]["rank_states_dir"])
            self.assertEqual("direct-buffers", contract_surface["stream_contract"]["read_mode"])
            self.assertEqual("trainer-dataloader", contract_surface["stream_contract"]["trainer_batch_iterator"])
            self.assertEqual(1, contract_surface["stream_contract"]["trainer_prefetch_batches"])
            self.assertEqual(2, contract_surface["distributed_contract"]["world_size"])
            self.assertTrue(contract_surface["distributed_contract"]["rank_states_written"])
            self.assertIn("source_name", contract_surface["batch_contract"]["sample_fields"])
            self.assertFalse(contract_surface["authentication"]["password_cli_argument"])
            contract_path = output_dir / "trainer-loop-contract.json"
            self.assertTrue(contract_path.exists())
            written_contract = json.loads(contract_path.read_text(encoding="utf-8"))
            self.assertEqual(contract_surface, written_contract)
            self.assertGreaterEqual(payload["artifacts"]["artifact_count"], 8)
            for name in (
                "trainer-contract.json",
                "trainer-handoff.json",
                "trainer-readiness-bundle.json",
                "trainer-loop-contract.json",
                "trainer-preflight.json",
                "trainer-integration-smoke.json",
                "trainer-runtime-smoke.json",
                "trainer-distributed-runtime-smoke.json",
                "trainer-loop-replay.json",
            ):
                self.assertTrue((output_dir / name).exists(), name)
            self.assertTrue((output_dir / "rank-states" / "rank-00000.trainer-stream-state.json").exists())
            self.assertTrue((output_dir / "rank-states" / "rank-00001.trainer-stream-state.json").exists())

            stdout = io.StringIO()
            stderr = io.StringIO()
            exit_code = run_trainer_acceptance_fixture_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--output-dir",
                    str(output_dir / "cli"),
                    "--output",
                    str(output),
                    "--batch-size",
                    "2",
                    "--contract-batches",
                    "2",
                    "--samples-per-epoch",
                    "6",
                    "--trainer-batch-iterator",
                    "trainer-dataloader",
                    "--data-loader-workers",
                    "0",
                    "--world-size",
                    "2",
                    "--loop-steps",
                    "2",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            cli_payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_ACCEPTANCE_FIXTURE_FORMAT, cli_payload["format"])
            self.assertTrue(cli_payload["passed"])
            self.assertEqual(str(output_dir / "cli"), cli_payload["output_dir"])

    def test_trainer_acceptance_fixture_verifies_pipeline_loop_for_direct_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))

            with patch.object(torch_dataset_module, "_require_torch", lambda: object()), \
                patch.object(torch_dataset_module, "_torch_dtype", lambda _torch, _dtype: "uint8"), \
                patch.object(torch_dataset_module, "_dtype_nbytes", lambda _dtype: 1), \
                patch.object(torch_dataset_module, "_torch_empty", lambda _torch, shape, **_kwargs: FakeTensor(shape[0])):
                payload = build_trainer_acceptance_fixture_payload(
                    root / "mds",
                    "chat=2,summary=1",
                    read_mode="direct-tensors",
                    batch_size=1,
                    contract_batches=2,
                    samples_per_epoch=3,
                    trainer_batch_iterator="native",
                    world_size=2,
                    loop_steps=2,
                    allow_partial_source_coverage=True,
                )

            self.assertEqual(TRAINER_ACCEPTANCE_FIXTURE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual("direct-tensors", payload["read_mode"])
            self.assertTrue(payload["pipeline_training_loop_required"])
            self.assertTrue(payload["pipeline_training_loop_verified"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                payload["pipeline_training_loop_context"],
            )
            self.assertEqual("CxnfsTrainerPipelineLoop", payload["pipeline_training_loop_object"])
            readiness = payload["reports"]["readiness_bundle"]
            self.assertTrue(readiness["pipeline_training_loop_required"])
            self.assertTrue(readiness["pipeline_training_loop_verified"])
            runtime_smoke = readiness["reports"]["runtime_smoke"]
            self.assertTrue(runtime_smoke["pipeline_training_loop_verified"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                runtime_smoke["pipeline_training_loop_context"],
            )
            self.assertEqual(
                "CxnfsTrainerPipelineLoop",
                runtime_smoke["pipeline_training_loop_object"],
            )

    def test_trainer_integration_plan_opens_stream_from_resume_state_file(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "reports/trainer-state.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)

            with plan.open_stream() as stream:
                first = stream.next_batch()
                stream.save_state_file(state_path)
                state = stream.state_dict()

            with plan.open_stream(resume_state_path=state_path) as resumed:
                self.assertEqual(2, resumed.cursor.emitted)
                second = resumed.next_batch()

            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [bytes(sample.data).decode("utf-8") for sample in second.samples])
            self.assertEqual(4, second.resume_cursor.emitted)
            with self.assertRaisesRegex(ValueError, "use resume_state or resume_state_path"):
                plan.open_stream(resume_state=state, resume_state_path=state_path)
            with self.assertRaisesRegex(ValueError, "resume_state cannot be combined with cursor"):
                plan.open_stream(resume_state=state, cursor=state["cursor"])

    def test_trainer_integration_plan_open_training_loop_applies_prefetch_policy(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)

            with plan.open_training_loop() as stream:
                self.assertIsInstance(stream, CxnfsTrainerPrefetchStream)
                first = stream.next_batch()
                second = stream.next_batch()
                state = stream.stream.state_dict()

            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [bytes(sample.data).decode("utf-8") for sample in second.samples])
            self.assertEqual(4, state["cursor"]["emitted"])

            with plan.open_training_loop(trainer_prefetch_batches=0) as stream:
                self.assertIsInstance(stream, CxnfsTrainerStream)
                batch = stream.next_batch()
            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in batch.samples])

    def test_trainer_integration_plan_open_pipeline_training_loop_prefetches_tensor_batches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "reports/pipeline-loop-state.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-tensors",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=2,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)

            with plan.open_pipeline_training_loop(pipeline_prefetch_batches=2, pipeline_workers=2) as loop:
                self.assertIsInstance(loop, CxnfsTrainerPipelineLoop)
                first = loop.next_batch()
                saved_path = loop.save_state_file(state_path)
                state = loop.state_dict()

            with plan.open_pipeline_training_loop(
                resume_state_path=saved_path,
                pipeline_prefetch_batches=1,
                pipeline_workers=2,
            ) as resumed:
                self.assertEqual(2, resumed.cursor.emitted)
                second = resumed.next_batch()

            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state["format"])
            self.assertEqual(2, state["cursor"]["emitted"])
            self.assertEqual(["chat", "chat"], [sample.source_name for sample in first.batch.samples])
            self.assertEqual(["summary", "chat"], [sample.source_name for sample in second.batch.samples])
            self.assertEqual([b"chat-0", b"chat-1"], [
                bytes(sample.data)
                for sample in first.batch.samples
            ])
            self.assertEqual([b"summary-0", b"chat-2"], [
                bytes(sample.data)
                for sample in second.batch.samples
            ])
            self.assertEqual(state, json.loads(state_path.read_text(encoding="utf-8")))

    def test_trainer_integration_plan_pipeline_training_loop_requires_direct_tensors(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
                samples_per_epoch=1,
            )
            handoff = build_trainer_handoff_payload(contract)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)

            with self.assertRaisesRegex(ValueError, "requires read_mode=direct-tensors"):
                with plan.open_pipeline_training_loop():
                    pass

    def test_trainer_integration_plan_opens_remote_stream_with_injected_client_factory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=1,
                trainer_prefetch_batches=1,
                samples_per_epoch=2,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)
            created_sources = []

            def client_factory(name):
                created_sources.append(name)
                return ClosableDatasetClient(RemoteShardTransport(root / "mds" / name))

            self.assertTrue(plan.remote_client_required)
            with self.assertRaisesRegex(ValueError, "requires client"):
                plan.open_stream()

            with plan.open_stream(client_factory=client_factory) as stream:
                batch = stream.next_batch()

            self.assertEqual(["chat-0", "summary-0"], [bytes(sample.data).decode("utf-8") for sample in batch.samples])
            self.assertEqual({"chat", "summary"}, set(created_sources))

    def test_trainer_integration_plan_open_remote_stream_accepts_resume_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            with ClientCredentials("trainer", "secret") as credentials:
                with plan.open_remote_stream(
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=1443,
                    transport_port=1444,
                    server_hostname="cxnfs.example.internal",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=lambda cafile: None,
                ) as stream:
                    first = stream.next_batch()
                    state = stream.state_dict()

                with plan.open_remote_stream(
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=1443,
                    transport_port=1444,
                    server_hostname="cxnfs.example.internal",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=lambda cafile: None,
                    resume_state=state,
                ) as resumed:
                    self.assertEqual(2, resumed.cursor.emitted)
                    second = resumed.next_batch()

            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [bytes(sample.data).decode("utf-8") for sample in second.samples])
            self.assertEqual(4, second.resume_cursor.emitted)
            self.assertEqual(4, len(controls))
            self.assertEqual(4, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))

    def test_trainer_integration_plan_open_training_loop_uses_remote_resume_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            with self.assertRaisesRegex(ValueError, "requires credentials"):
                with plan.open_training_loop():
                    pass

            with ClientCredentials("trainer", "secret") as credentials:
                with plan.open_training_loop(
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=1443,
                    transport_port=1444,
                    server_hostname="cxnfs.example.internal",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=lambda cafile: None,
                    trainer_prefetch_batches=0,
                ) as stream:
                    first = stream.next_batch()
                    state = stream.state_dict()

                with plan.open_training_loop(
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=1443,
                    transport_port=1444,
                    server_hostname="cxnfs.example.internal",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=lambda cafile: None,
                    resume_state=state,
                ) as resumed:
                    self.assertIsInstance(resumed, CxnfsTrainerPrefetchStream)
                    self.assertEqual(2, resumed.cursor.emitted)
                    second = resumed.next_batch()

            self.assertEqual(["chat-0", "chat-1"], [bytes(sample.data).decode("utf-8") for sample in first.samples])
            self.assertEqual(["summary-0", "chat-2"], [bytes(sample.data).decode("utf-8") for sample in second.samples])
            self.assertEqual(4, second.resume_cursor.emitted)
            self.assertEqual(4, len(controls))
            self.assertEqual(4, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))

    def test_trainer_integration_plan_creates_remote_client_factory_from_handoff(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                bulk_read_retries=1,
                bulk_write_retries=2,
                read_mode="direct-buffers",
                batch_size=2,
                batches=1,
                samples_per_epoch=2,
            )
            handoff = build_trainer_handoff_payload(
                contract,
                require_remote=True,
            )
            handoff["integration_plan"]["remote_client_kwargs"].update({
                "initial_cords": 8,
                "max_cords": 32,
                "provisional_container_size_bytes": 1024 * 1024,
                "expected_size_limit_bytes": 2 * 1024 * 1024,
                "write_connections": 4,
                "shared_client": False,
            })
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            with ClientCredentials("trainer", "secret") as credentials:
                client_factory = plan.create_remote_client_factory(
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=1443,
                    transport_port=1444,
                    server_hostname="cxnfs.example.internal",
                    cafile="/tmp/cxnfs-ca.pem",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=lambda cafile: None,
                )
                try:
                    with plan.open_stream(client_factory=client_factory) as stream:
                        batch = stream.next_batch()
                finally:
                    client_factory.close()

            self.assertEqual(["chat-0", "summary-0"], [bytes(sample.data).decode("utf-8") for sample in batch.samples])
            self.assertEqual(["cxnfs.example.internal", "cxnfs.example.internal"], [control.host for control in controls])
            self.assertEqual([1443, 1443], [control.port for control in controls])
            self.assertEqual([1444, 1444], [bulk.port for bulk in bulks])
            self.assertEqual(1, client_factory.bulk_read_retries)
            self.assertEqual(2, client_factory.bulk_write_retries)
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertEqual(["trainer", "trainer"], [control.username for control in controls])
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual([8, 8], [bulk.options["read_config"].initial_cords for bulk in bulks])
            self.assertEqual([32, 32], [bulk.options["read_config"].max_cords for bulk in bulks])
            self.assertEqual(
                [1024 * 1024, 1024 * 1024],
                [bulk.options["read_config"].provisional_container_size_bytes for bulk in bulks],
            )
            self.assertEqual(
                [2 * 1024 * 1024, 2 * 1024 * 1024],
                [bulk.options["read_config"].expected_size_limit_bytes for bulk in bulks],
            )
            self.assertEqual([4, 4], [bulk.options["write_connections"] for bulk in bulks])
            self.assertEqual(["cxnfs.example.internal", "cxnfs.example.internal"], [bulk.options["server_hostname"] for bulk in bulks])
            topology = client_factory.connection_topology()
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(2, topology["client_close_count"])
            self.assertEqual(0, topology["active_client_count"])
            self.assertTrue(topology["all_open_clients_closed"])

    def test_trainer_integration_plan_opens_remote_stream_and_closes_factory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=1,
                samples_per_epoch=2,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(handoff)
            controls = []
            bulks = []

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            with ClientCredentials("trainer", "secret") as credentials:
                with plan.open_remote_stream(
                    credentials,
                    host="cxnfs.example.internal",
                    control_port=1443,
                    transport_port=1444,
                    server_hostname="cxnfs.example.internal",
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                    tls_context_factory=lambda cafile: None,
                ) as stream:
                    batch = stream.next_batch()

            self.assertEqual(["chat-0", "summary-0"], [bytes(sample.data).decode("utf-8") for sample in batch.samples])
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))

    def test_trainer_integration_smoke_cli_writes_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
                samples_per_epoch=1,
            )
            handoff = build_trainer_handoff_payload(contract)
            handoff_path = root / "trainer-handoff.json"
            output = root / "trainer-integration-smoke.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_integration_smoke_cli(
                [
                    str(handoff_path),
                    "--batches",
                    "1",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_INTEGRATION_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(1, payload["batch_count"])
            self.assertEqual(1, payload["sample_count"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", payload["training_loop_context"])
            self.assertTrue(payload["training_loop_context_used"])
            self.assertEqual("CxnfsTrainerStream", payload["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", payload["training_loop_base_stream_object"])
            self.assertFalse(payload["training_loop_prefetch_applied"])

    def test_trainer_integration_smoke_cli_streams_remote_handoff_with_prompted_credentials(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=1,
                samples_per_epoch=2,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            self.assertTrue(handoff["integration_plan"]["remote_client_required"])
            handoff_path = root / "trainer-handoff.json"
            output = root / "trainer-integration-smoke.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            controls = []
            bulks = []
            credentials_seen = []

            def credential_prompt():
                credentials = ClientCredentials("trainer", "secret")
                credentials_seen.append(credentials)
                return credentials

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_integration_smoke_cli(
                [
                    str(handoff_path),
                    "--batches",
                    "1",
                    "--host",
                    "cxnfs.example.internal",
                    "--control-port",
                    "1443",
                    "--transport-port",
                    "1444",
                    "--bulk-transport",
                    "stream",
                    "--initial-cords",
                    "2",
                    "--max-cords",
                    "4",
                    "--bulk-read-retries",
                    "1",
                    "--bulk-write-retries",
                    "1",
                    "--trainer-prefetch-batches",
                    "1",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_INTEGRATION_SMOKE_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertTrue(payload["remote_client_required"])
            self.assertEqual("stream", payload["stream_report"]["bulk_transport"])
            self.assertEqual("/stage0-mds", payload["stream_report"]["remote_root"])
            self.assertEqual(1, payload["trainer_prefetch_batches"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", payload["training_loop_context"])
            self.assertTrue(payload["training_loop_context_used"])
            self.assertEqual("CxnfsTrainerPrefetchStream", payload["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", payload["training_loop_base_stream_object"])
            self.assertEqual(1, payload["training_loop_prefetch_batches"])
            self.assertTrue(payload["training_loop_prefetch_applied"])
            self.assertEqual(1, payload["batch_count"])
            self.assertEqual(2, payload["sample_count"])
            self.assertEqual(1, len(credentials_seen))
            self.assertTrue(credentials_seen[0].closed)
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertEqual(["trainer", "trainer"], [control.username for control in controls])
            self.assertEqual(["cxnfs.example.internal", "cxnfs.example.internal"], [control.host for control in controls])
            self.assertEqual([1443, 1443], [control.port for control in controls])
            self.assertTrue(all(control.closed for control in controls))
            self.assertEqual([1444, 1444], [bulk.port for bulk in bulks])
            self.assertEqual([2, 2], [bulk.options["read_config"].initial_cords for bulk in bulks])
            self.assertEqual([4, 4], [bulk.options["read_config"].max_cords for bulk in bulks])
            topology = payload["stream_report"]["remote_client_topology"]
            self.assertEqual(2, topology["unique_source_request_count"])
            self.assertEqual(2, topology["connection_count"])
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(2, topology["active_client_count"])
            self.assertEqual(0, topology["client_close_count"])
            self.assertFalse(topology["all_open_clients_closed"])
            self.assertTrue(topology["per_source_client_observed"])
            self.assertTrue(topology["complete"])
            closed_topology = payload["stream_report"]["remote_client_topology_after_close"]
            self.assertEqual(2, closed_topology["client_close_count"])
            self.assertEqual(0, closed_topology["active_client_count"])
            self.assertTrue(closed_topology["all_open_clients_closed"])
            self.assertTrue(payload["stream_report"]["remote_client_lifecycle_closed"])

    def test_trainer_integration_smoke_cli_has_no_password_argument(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = run_trainer_integration_smoke_cli(["--help"], stderr=stderr)

        self.assertEqual(0, exit_code)
        self.assertEqual("", stderr.getvalue())
        self.assertIn("--host", stdout.getvalue())
        self.assertIn("--bulk-transport", stdout.getvalue())
        self.assertNotIn("--password", stdout.getvalue())

    def test_trainer_loop_replay_payload_runs_resume_split_and_source_coverage(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract)

            payload = build_trainer_loop_replay_payload(handoff, steps=2)

            self.assertEqual(TRAINER_LOOP_REPLAY_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(2, payload["step_count"])
            self.assertEqual(2, payload["batch_count"])
            self.assertEqual(4, payload["sample_count"])
            self.assertEqual({"chat": 3, "summary": 1}, payload["source_counts"])
            self.assertEqual(["chat", "summary"], payload["expected_sources"])
            self.assertTrue(payload["weighted_source_coverage_complete"])
            self.assertTrue(payload["resume_replayed"])
            self.assertTrue(payload["resume_continuity_verified"])
            self.assertEqual(0, payload["first_initial_emitted"])
            self.assertEqual(2, payload["first_final_emitted"])
            self.assertEqual(2, payload["second_initial_emitted"])
            self.assertEqual(4, payload["final_emitted"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["resume_state"]["format"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["final_state"]["format"])
            self.assertEqual("CxnfsTrainerPrefetchStream", payload["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", payload["training_loop_base_stream_object"])
            self.assertTrue(payload["training_loop_prefetch_applied"])
            self.assertEqual("cxnfs.trainer-step-trace.v1", payload["step_trace_format"])
            self.assertTrue(payload["step_trace_complete"])
            self.assertTrue(payload["cursor_trace_complete"])
            self.assertTrue(payload["cursor_delta_verified"])
            self.assertTrue(payload["cursor_monotonic"])
            self.assertTrue(payload["no_duplicate_sample_identities"])
            self.assertEqual(4, payload["sample_identity_count"])
            self.assertEqual(4, payload["unique_sample_identity_count"])
            self.assertEqual(0, payload["duplicate_sample_identity_count"])
            self.assertEqual(["chat:0:0", "chat:0:1"], payload["step_trace"][0]["sample_identities"])
            self.assertEqual(["summary:1:0", "chat:0:2"], payload["step_trace"][1]["sample_identities"])
            self.assertEqual(0, payload["step_trace"][0]["resume_start_emitted"])
            self.assertEqual(2, payload["step_trace"][0]["resume_final_emitted"])
            self.assertEqual("batch-resume-cursor", payload["step_trace"][0]["cursor_trace_source"])
            self.assertTrue(payload["step_trace"][0]["cursor_delta_verified"])
            self.assertEqual(2, payload["steps"][0]["batches"][0]["resume_delta"])
            self.assertEqual("batch-resume-cursor", payload["steps"][0]["batches"][0]["cursor_trace_source"])
            self.assertEqual(["chat:0:0", "chat:0:1"], payload["steps"][0]["batches"][0]["sample_identities"])
            self.assertEqual("cxnfs.trainer-stream-smoke.v1", payload["stream_report"]["format"])
            self.assertTrue(payload["stream_report"]["read_path_reliability"]["complete"])

    def test_trainer_loop_replay_payload_rejects_duplicate_sample_identities_by_default(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                read_mode="direct-buffers",
                batch_size=1,
                batches=2,
                source_read_workers=1,
                prefetch_chunks=1,
                trainer_prefetch_batches=0,
                samples_per_epoch=2,
            )
            handoff = build_trainer_handoff_payload(contract)

            strict_payload = build_trainer_loop_replay_payload(handoff, steps=2)
            allowed_payload = build_trainer_loop_replay_payload(
                handoff,
                steps=2,
                require_unique_sample_identities=False,
            )

            self.assertFalse(strict_payload["passed"])
            self.assertTrue(strict_payload["step_trace_complete"])
            self.assertFalse(strict_payload["no_duplicate_sample_identities"])
            self.assertEqual(1, strict_payload["duplicate_sample_identity_count"])
            self.assertEqual(["chat:0:0"], strict_payload["duplicate_sample_identities"])
            self.assertTrue(strict_payload["require_unique_sample_identities"])
            self.assertTrue(allowed_payload["passed"])
            self.assertFalse(allowed_payload["require_unique_sample_identities"])
            self.assertEqual(strict_payload["step_trace"], allowed_payload["step_trace"])

    def test_trainer_loop_replay_payload_accepts_remote_handoff_with_injected_factory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=6,
            )
            handoff = build_trainer_handoff_payload(contract, require_remote=True)
            created_sources = []

            def client_factory(name):
                created_sources.append(name)
                return ClosableDatasetClient(RemoteShardTransport(root / "mds" / name))

            payload = build_trainer_loop_replay_payload(
                handoff,
                steps=2,
                client_factory=client_factory,
            )

            self.assertTrue(payload["passed"])
            self.assertTrue(payload["remote_client_required"])
            self.assertEqual("/stage0-mds", payload["stream_report"]["remote_root"])
            self.assertEqual({"chat", "summary"}, set(created_sources))
            self.assertTrue(payload["weighted_source_coverage_complete"])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer tensor loop replay")
    def test_trainer_loop_replay_payload_reports_direct_tensor_memory_path(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            allocation = CxnfsTrainerTensorAllocation(
                target=TensorMemoryTarget.PYTORCH_CPU,
                dtype="uint8",
                device="cpu",
            )
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-tensors",
                batch_size=2,
                batches=2,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=4,
                tensor_allocation=allocation,
            )
            handoff = build_trainer_handoff_payload(contract)

            payload = build_trainer_loop_replay_payload(handoff, steps=2)

            self.assertTrue(payload["passed"])
            self.assertTrue(payload["resume_replayed"])
            self.assertTrue(payload["resume_continuity_verified"])
            self.assertEqual("direct-tensors", payload["stream_report"]["read_mode"])
            self.assertEqual("pytorch-cpu", payload["stream_report"]["tensor_allocation"]["target"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["final_state"]["format"])
            tensor_path = payload["stream_report"]["tensor_memory_path"]
            self.assertEqual(TRAINER_TENSOR_MEMORY_PATH_FORMAT, tensor_path["format"])
            self.assertTrue(tensor_path["complete"], tensor_path)
            self.assertEqual("pytorch-cpu", tensor_path["target"])
            self.assertTrue(tensor_path["network_fills_tensor_destination"])
            self.assertTrue(tensor_path["python_byte_batch_avoided"])
            self.assertFalse(tensor_path["device_copy_required"])
            evidence = payload["stream_report"]["read_path_evidence"]
            self.assertGreater(evidence["direct_tensor_read_operations"], 0)
            self.assertGreater(evidence["direct_destination_read_operations"], 0)
            self.assertTrue(payload["stream_report"]["read_path_reliability"]["complete"])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer worker loop replay")
    def test_trainer_loop_replay_payload_preserves_dataloader_worker_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(6)))
            write_file_dataset(root, "summary", tuple(f"summary-{index}" for index in range(4)))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=4,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=0,
                trainer_batch_iterator="trainer-dataloader",
                data_loader_workers=2,
                samples_per_epoch=8,
                repeat=False,
            )
            handoff = build_trainer_handoff_payload(contract)

            payload = build_trainer_loop_replay_payload(handoff, steps=4, resume_after_steps=2)

            self.assertTrue(payload["passed"])
            self.assertTrue(payload["resume_replayed"])
            self.assertTrue(payload["resume_continuity_verified"])
            self.assertTrue(payload["step_trace_complete"])
            self.assertTrue(payload["cursor_delta_verified"])
            self.assertEqual(
                {"logical-worker-step"},
                {step["cursor_trace_source"] for step in payload["step_trace"]},
            )
            self.assertEqual(4, payload["batch_count"])
            self.assertEqual(8, payload["sample_count"])
            self.assertFalse(contract["read_contract"]["repeat"])
            self.assertFalse(handoff["integration_plan"]["stream_kwargs"]["repeat"])
            self.assertFalse(payload["stream_report"]["repeat"])
            self.assertEqual("direct-buffers", payload["stream_report"]["read_mode"])
            self.assertEqual("trainer-dataloader", payload["stream_report"]["trainer_batch_iterator"])
            self.assertEqual(2, payload["stream_report"]["data_loader_workers"])
            self.assertIsNone(payload["stream_report"]["tensor_memory_path"])
            self.assertFalse(payload["training_loop_prefetch_applied"])
            resume_state = payload["resume_state"]
            final_state = payload["final_state"]
            self.assertTrue(resume_state["worker_partitioning_enabled"])
            self.assertEqual(2, resume_state["worker_cursor_count"])
            self.assertTrue(final_state["worker_partitioning_enabled"])
            self.assertEqual(2, final_state["worker_cursor_count"])
            worker_partition = payload["stream_report"]["worker_partition_evidence"]
            self.assertEqual(TRAINER_WORKER_PARTITION_FORMAT, worker_partition["format"])
            self.assertTrue(worker_partition["complete"], worker_partition)
            self.assertEqual(2, worker_partition["data_loader_workers"])
            self.assertEqual(0, worker_partition["duplicate_source_sample_count"])
            read_evidence = payload["stream_report"]["read_path_evidence"]
            self.assertGreater(read_evidence["direct_buffer_read_operations"], 0)
            self.assertTrue(read_evidence["worker_batch_read_evidence_observed"])
            self.assertEqual(4, read_evidence["worker_batch_read_evidence_count"])
            self.assertEqual(
                {True},
                {
                    report["produced_inside_torch_worker"]
                    for report in read_evidence["worker_process_read_path_evidence"]
                },
            )

    def test_trainer_loop_replay_cli_writes_report_and_resume_state_files(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3", "chat-4"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=1,
                batches=4,
                source_read_workers=2,
                prefetch_chunks=2,
                trainer_prefetch_batches=1,
                samples_per_epoch=7,
            )
            handoff = build_trainer_handoff_payload(contract)
            handoff_path = root / "trainer-handoff.json"
            output = root / "reports/trainer-loop-replay.json"
            resume_state = root / "reports/replay-resume-state.json"
            final_state = root / "reports/replay-final-state.json"
            handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_loop_replay_cli(
                [
                    str(handoff_path),
                    "--steps",
                    "2",
                    "--gradient-accumulation-steps",
                    "2",
                    "--write-resume-state",
                    str(resume_state),
                    "--write-final-state",
                    str(final_state),
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_LOOP_REPLAY_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertEqual(2, payload["step_count"])
            self.assertEqual(4, payload["batch_count"])
            self.assertEqual(4, payload["sample_count"])
            self.assertEqual(2, payload["gradient_accumulation_steps"])
            self.assertTrue(payload["resume_replayed"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, json.loads(resume_state.read_text(encoding="utf-8"))["format"])
            self.assertEqual(4, json.loads(final_state.read_text(encoding="utf-8"))["cursor"]["emitted"])

    def test_trainer_loop_replay_cli_has_no_password_argument(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = run_trainer_loop_replay_cli(["--help"], stderr=stderr)

        self.assertEqual(0, exit_code)
        self.assertEqual("", stderr.getvalue())
        self.assertIn("--gradient-accumulation-steps", stdout.getvalue())
        self.assertIn("--write-resume-state", stdout.getvalue())
        self.assertIn("--allow-duplicate-sample-identities", stdout.getvalue())
        self.assertNotIn("--password", stdout.getvalue())

    def test_trainer_handoff_cli_writes_bundle_and_surface_check(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            matrix = proven_surface_matrix_payload(contract)
            contract_path = root / "trainer-contract.json"
            workload_path = root / "trainer-workload.json"
            matrix_path = root / "trainer-matrix.json"
            batch_direct_read_path = root / "python-quic-batch-direct-read.json"
            batch_direct_tensor_read_path = root / "python-quic-batch-direct-tensor-read.json"
            output = root / "reports/trainer-handoff.json"
            surface_check = root / "reports/trainer-surface-check.json"
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            workload_path.write_text(json.dumps(workload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            batch_direct_read_path.write_text(
                json.dumps(proven_batch_direct_read_report(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            batch_direct_tensor_read_path.write_text(
                json.dumps(proven_batch_direct_tensor_read_report(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_handoff_cli(
                [
                    str(contract_path),
                    "--workload-report",
                    str(workload_path),
                    "--matrix-report",
                    str(matrix_path),
                    "--batch-direct-read-report",
                    str(batch_direct_read_path),
                    "--batch-direct-tensor-read-report",
                    str(batch_direct_tensor_read_path),
                    "--require-low-level-direct-read-proofs",
                    "--require-low-level-direct-write-proofs",
                    "--require-remote",
                    "--require-quic",
                    "--require-workload",
                    "--require-matrix",
                    "--min-sources",
                    "2",
                    "--min-validation-samples",
                    "3",
                    "--min-validation-bytes",
                    "21",
                    "--min-workload-runs",
                    "2",
                    "--min-workload-samples",
                    "8",
                    "--min-workload-bytes",
                    "128",
                    "--min-cord-sweep",
                    "2",
                    "--surface-check-json",
                    str(surface_check),
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["format"])
            self.assertTrue(payload["ready_for_trainer_wiring"])
            self.assertEqual(str(contract_path), payload["contract"]["path"])
            self.assertEqual(str(workload_path), payload["workload"]["path"])
            self.assertEqual(str(matrix_path), payload["matrix"]["path"])
            self.assertTrue(payload["low_level_direct_read_proofs"]["complete"])
            self.assertEqual(str(batch_direct_read_path), payload["low_level_direct_read_proofs"]["batch_direct_read"]["path"])
            self.assertEqual(str(batch_direct_tensor_read_path), payload["low_level_direct_read_proofs"]["batch_direct_tensor_read"]["path"])
            self.assertTrue(payload["low_level_direct_write_proofs"]["complete"])
            self.assertEqual(str(workload_path), payload["low_level_direct_write_proofs"]["workload_path"])
            self.assertEqual(
                "TLS_AES_128_GCM_SHA256",
                payload["low_level_direct_write_proofs"]["quic_write_transport_cipher_suite"],
            )
            surface_payload = json.loads(surface_check.read_text(encoding="utf-8"))
            self.assertEqual(TRAINER_SURFACE_CHECK_FORMAT, surface_payload["format"])
            self.assertTrue(surface_payload["passed"])

    def test_trainer_handoff_cli_fails_closed_when_low_level_proofs_required(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            matrix = proven_surface_matrix_payload(contract)
            contract_path = root / "trainer-contract.json"
            workload_path = root / "trainer-workload.json"
            matrix_path = root / "trainer-matrix.json"
            output = root / "reports/trainer-handoff.json"
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            workload_path.write_text(json.dumps(workload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_handoff_cli(
                [
                    str(contract_path),
                    "--workload-report",
                    str(workload_path),
                    "--matrix-report",
                    str(matrix_path),
                    "--require-remote",
                    "--require-quic",
                    "--require-workload",
                    "--require-matrix",
                    "--require-low-level-direct-read-proofs",
                    "--min-sources",
                    "2",
                    "--min-validation-samples",
                    "3",
                    "--min-validation-bytes",
                    "21",
                    "--min-workload-runs",
                    "2",
                    "--min-workload-samples",
                    "8",
                    "--min-workload-bytes",
                    "128",
                    "--min-cord-sweep",
                    "2",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["format"])
            self.assertFalse(payload["ready_for_trainer_wiring"])
            self.assertFalse(payload["ready_for_remote_quic"])
            self.assertFalse(payload["low_level_direct_read_proofs"]["complete"])
            self.assertIn("batch_direct_read_proof_complete", payload["next_actions"])
            self.assertIn("batch_direct_tensor_read_proof_complete", payload["next_actions"])

    def test_trainer_handoff_payload_requires_direct_read_proofs_for_remote_quic_by_default(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True

            payload = build_trainer_handoff_payload(
                contract,
                require_remote=True,
                require_quic=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
            )

            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["format"])
            self.assertEqual("deferred", payload["status"])
            self.assertFalse(payload["ready_for_trainer_wiring"])
            self.assertFalse(payload["ready_for_remote_quic"])
            self.assertTrue(payload["low_level_direct_read_proofs"]["required"])
            self.assertFalse(payload["low_level_direct_read_proofs"]["complete"])
            self.assertIn("batch_direct_read_proof_complete", payload["next_actions"])
            self.assertIn("batch_direct_tensor_read_proof_complete", payload["next_actions"])

            proven_payload = build_trainer_handoff_payload(
                contract,
                batch_direct_read_payload=proven_batch_direct_read_report(),
                batch_direct_tensor_read_payload=proven_batch_direct_tensor_read_report(),
                require_remote=True,
                require_quic=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
            )

            self.assertTrue(proven_payload["ready_for_trainer_wiring"])
            self.assertTrue(proven_payload["ready_for_remote_quic"])
            self.assertTrue(proven_payload["low_level_direct_read_proofs"]["required"])
            self.assertTrue(proven_payload["low_level_direct_read_proofs"]["complete"])

    def test_trainer_handoff_payload_fails_closed_when_write_proof_cipher_is_missing(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/stage0-mds",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["write_path_evidence"]["quic_write_transport_cipher_suite_counts"] = {}
            workload["write_path_evidence"]["quic_write_transport_expected_cipher_suite_count"] = 0
            workload["write_path_evidence"]["quic_write_transport_all_reports_expected_cipher_suite"] = False
            workload["write_path_evidence"]["quic_write_transport_complete"] = False

            payload = build_trainer_handoff_payload(
                contract,
                workload_payload=workload,
                workload_path=root / "trainer-workload.json",
                matrix_payload=proven_surface_matrix_payload(contract),
                batch_direct_read_payload=proven_batch_direct_read_report(),
                batch_direct_tensor_read_payload=proven_batch_direct_tensor_read_report(),
                require_low_level_direct_read_proofs=True,
                require_low_level_direct_write_proofs=True,
                require_remote=True,
                require_quic=True,
                require_workload=True,
                require_matrix=True,
                min_sources=2,
                min_validation_samples=3,
                min_validation_bytes=21,
                min_workload_runs=2,
                min_workload_samples=8,
                min_workload_bytes=128,
                min_cord_sweep=2,
            )

            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["format"])
            self.assertFalse(payload["ready_for_trainer_wiring"])
            self.assertFalse(payload["ready_for_remote_quic"])
            self.assertFalse(payload["low_level_direct_write_proofs"]["complete"])
            self.assertIn("quic_write_transport_cipher_suite", payload["low_level_direct_write_proofs"]["missing"])
            self.assertIn("quic_write_transport_complete", payload["low_level_direct_write_proofs"]["missing"])
            self.assertIn("quic_write_transport_cipher_suite", payload["next_actions"])

    def test_trainer_handoff_cli_fails_closed_when_required_workload_missing(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                batch_size=1,
                batches=1,
            )
            contract_path = root / "trainer-contract.json"
            output = root / "trainer-handoff.json"
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_handoff_cli(
                [
                    str(contract_path),
                    "--require-workload",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_HANDOFF_FORMAT, payload["format"])
            self.assertFalse(payload["ready_for_trainer_wiring"])
            self.assertEqual("deferred", payload["status"])
            self.assertIn("workload_report_present", payload["next_actions"])
            self.assertIn("workload_report_present", {
                check["name"] for check in payload["failed_checks"]
            })

    def test_trainer_surface_check_fails_workload_without_benchmark_provenance(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload.pop("benchmark_provenance")

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            failed = [check["name"] for check in payload["failed_checks"]]
            self.assertIn("benchmark_provenance_present", failed)
            self.assertIn("benchmark_provenance_format", failed)

    def test_trainer_surface_check_fails_remote_workload_without_client_topology(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload.pop("remote_client_topology")

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "workload_remote_client_topology",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_remote_workload_with_mismatched_client_source_sets(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            topology = dict(workload["remote_client_topology"])
            topology["connection_source_set_matches_requests_count"] = 0
            topology["client_open_source_set_matches_requests_count"] = 0
            topology["all_connection_source_sets_match_requests"] = False
            topology["all_client_open_source_sets_match_requests"] = False
            workload["remote_client_topology"] = topology

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            failed_checks = {check["name"]: check for check in payload["failed_checks"]}
            self.assertIn("workload_remote_client_topology", failed_checks)
            self.assertIn("connection sources", failed_checks["workload_remote_client_topology"]["detail"])
            self.assertIn("open sources", failed_checks["workload_remote_client_topology"]["detail"])

    def test_trainer_surface_check_fails_remote_matrix_without_client_topology(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            matrix = proven_surface_matrix_payload(contract)
            matrix.pop("remote_client_topology")
            for case in matrix["cases"]:
                case.pop("remote_client_topology")

            payload = build_trainer_surface_check_payload(
                contract,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_matrix=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "matrix_remote_client_topology",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_workload_fingerprint_mismatch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["workload_fingerprint"] = {
                **contract["workload_fingerprint"],
                "sha256": "0" * 64,
            }

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "workload_fingerprint_matches_contract",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_matrix_fingerprint_mismatch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            matrix = proven_surface_matrix_payload(contract)
            matrix["workload_fingerprint"] = {
                **contract["workload_fingerprint"],
                "sha256": "0" * 64,
            }

            payload = build_trainer_surface_check_payload(
                contract,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_matrix=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "matrix_fingerprint_matches_contract",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_matrix_missing_direct_tensor_case(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            matrix = proven_surface_matrix_payload(contract)
            matrix["cases"] = [
                case for case in matrix["cases"]
                if case["read_mode"] != "direct-tensors"
            ]
            matrix["case_count"] = len(matrix["cases"])
            matrix["passed_case_count"] = len(matrix["cases"])

            payload = build_trainer_surface_check_payload(
                contract,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_matrix=True,
            )

            failed = [check["name"] for check in payload["failed_checks"]]
            self.assertFalse(payload["passed"])
            self.assertIn("matrix_direct_tensors_native_present", failed)
            self.assertIn("matrix_direct_tensors_dataloader_present", failed)
            self.assertIn("matrix_tensor_targets_supported", failed)

    def test_trainer_surface_check_fails_matrix_without_read_reliability(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            matrix = proven_surface_matrix_payload(contract)
            for case in matrix["cases"]:
                case.pop("read_path_reliability", None)
            matrix["read_path_reliability_complete"] = False

            payload = build_trainer_surface_check_payload(
                contract,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_matrix=True,
            )

            failed = [check["name"] for check in payload["failed_checks"]]
            self.assertFalse(payload["passed"])
            self.assertIn("matrix_quic_read_reliability", failed)
            self.assertFalse(payload["matrix"]["read_path_reliability_complete"])

    def test_trainer_surface_check_requires_clean_worker_partition_when_matrix_uses_workers(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            contract["read_contract"]["trainer_batch_iterator"] = "trainer-dataloader"
            contract["read_contract"]["data_loader_workers"] = 2
            contract["read_contract"]["worker_partitioning"] = {
                "format": TRAINER_WORKER_PARTITION_FORMAT,
                "enabled": True,
                "base_training_shard": {"index": 0, "count": 1},
                "effective_training_shard_count": 2,
            }
            matrix = proven_surface_matrix_payload(contract)
            for case in matrix["cases"]:
                if case["trainer_batch_iterator"] != "trainer-dataloader":
                    continue
                case["data_loader_workers"] = 2
                case["worker_partition_evidence"] = {
                    "format": TRAINER_WORKER_PARTITION_FORMAT,
                    "complete": True,
                    "worker_sharding_enabled": True,
                    "data_loader_workers": 2,
                    "expected_effective_training_shards": 2,
                    "observed_training_shard_count": 2,
                    "duplicate_source_sample_count": 0,
                    "no_duplicate_source_samples": True,
                }

            passing = build_trainer_surface_check_payload(
                contract,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_matrix=True,
            )
            self.assertTrue(passing["passed"], passing["failed_checks"])
            self.assertTrue(passing["matrix"]["worker_partition_required"])
            self.assertTrue(passing["matrix"]["worker_partition_complete"])

            for case in matrix["cases"]:
                if case["trainer_batch_iterator"] == "trainer-dataloader":
                    case["worker_partition_evidence"]["complete"] = False
                    case["worker_partition_evidence"]["duplicate_source_sample_count"] = 1
                    break

            failing = build_trainer_surface_check_payload(
                contract,
                matrix_payload=matrix,
                require_remote=True,
                require_quic=True,
                require_matrix=True,
            )
            self.assertFalse(failing["passed"])
            self.assertIn(
                "matrix_worker_partition_complete",
                [check["name"] for check in failing["failed_checks"]],
            )

    def test_trainer_surface_check_fails_workload_iterator_mismatch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["trainer_batch_iterator"] = "trainer-dataloader"
            workload["benchmark_provenance"]["invocation"]["trainer_batch_iterator"] = "trainer-dataloader"

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "workload_trainer_batch_iterator_matches_contract",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_workload_data_loader_workers_mismatch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["data_loader_workers"] = 2
            workload["benchmark_provenance"]["invocation"]["data_loader_workers"] = 2

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "workload_data_loader_workers_match_contract",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_workload_bulk_write_retries_mismatch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                bulk_read_retries=1,
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["bulk_write_retries"] = 2
            workload["benchmark_provenance"]["invocation"]["bulk_write_retries"] = 2

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            failed = [check["name"] for check in payload["failed_checks"]]
            self.assertFalse(payload["passed"])
            self.assertIn("workload_bulk_write_retries_match_contract", failed)

    def test_trainer_surface_check_fails_workload_without_blockfs_scale_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["blockfs_gate"].pop("workload_scale_complete")
            workload["blockfs_gate"].pop("workload_scale_gate")

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "workload_blockfs_scale_gate_present",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_workload_with_stale_blockfs_scale_observed_counts(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)
            workload["blockfs_gate"]["workload_scale_gate"]["observed"]["sample_count"] = 999

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            self.assertFalse(payload["passed"])
            self.assertIn(
                "workload_blockfs_scale_observed_matches_report",
                [check["name"] for check in payload["failed_checks"]],
            )

    def test_trainer_surface_check_fails_tiny_workload_thresholds(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = {"format": "cxnfs.weighted-mds-plan.v1", "sources": [{"name": "chat"}]}
            workload["sample_count"] = 1
            workload["byte_size"] = 6

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
                min_sources=2,
                min_validation_samples=2,
                min_validation_bytes=12,
                min_workload_runs=4,
                min_workload_samples=8,
                min_workload_bytes=128,
                min_cord_sweep=3,
            )

            failed = [check["name"] for check in payload["failed_checks"]]
            self.assertFalse(payload["passed"])
            self.assertIn("source_count_meets_minimum", failed)
            self.assertIn("validation_samples_meet_minimum", failed)
            self.assertIn("workload_source_count_meets_minimum", failed)
            self.assertIn("workload_fingerprint_matches_contract", failed)
            self.assertIn("workload_samples_meet_minimum", failed)
            self.assertIn("workload_cord_sweep_meets_minimum", failed)

    def test_trainer_surface_check_uses_contract_workload_requirements(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                remote_root="/remote/datasets",
                bulk_transport="quic",
                read_mode="direct-buffers",
                batch_size=2,
                batches=2,
                samples_per_epoch=3,
                min_workload_runs=4,
                min_workload_samples=16,
                min_workload_bytes=256,
                min_cord_sweep=3,
            )
            contract["network_contract"]["bulk_transport_available"] = True
            workload = proven_quic_workload_payload()
            workload["weighted_mds_plan"] = contract["weighted_mds_plan"]
            workload["workload_fingerprint"] = contract["workload_fingerprint"]
            align_workload_with_contract(workload, contract)

            payload = build_trainer_surface_check_payload(
                contract,
                workload_payload=workload,
                require_remote=True,
                require_quic=True,
                require_workload=True,
            )

            failed = [check["name"] for check in payload["failed_checks"]]
            self.assertFalse(payload["passed"])
            self.assertEqual(4, payload["requirements"]["min_workload_runs"])
            self.assertEqual(16, payload["requirements"]["min_workload_samples"])
            self.assertEqual(256, payload["requirements"]["min_workload_bytes"])
            self.assertEqual(3, payload["requirements"]["min_cord_sweep"])
            self.assertIn("workload_runs_meet_minimum", failed)
            self.assertIn("workload_samples_meet_minimum", failed)
            self.assertIn("workload_bytes_meet_minimum", failed)
            self.assertIn("workload_cord_sweep_meets_minimum", failed)

    def test_trainer_surface_check_cli_fails_local_contract_when_remote_required(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "reports/trainer-contract.json"
            output = root / "reports/surface-check.json"
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            contract_path.parent.mkdir(parents=True)
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_check_cli(
                [
                    str(contract_path),
                    "--require-remote",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["passed"])
            self.assertIn("remote_root_present", [check["name"] for check in payload["failed_checks"]])

    def test_trainer_surface_check_cli_requires_workload_report_when_requested(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "trainer-contract.json"
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_check_cli(
                [
                    str(contract_path),
                    "--require-workload",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["passed"])
            self.assertIn("workload_report_present", [check["name"] for check in payload["failed_checks"]])

    def test_trainer_surface_check_cli_requires_matrix_report_when_requested(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "trainer-contract.json"
            write_file_dataset(root, "chat", ("chat-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=1",
                bulk_transport="stream",
                read_mode="direct-buffers",
                batch_size=1,
                batches=1,
            )
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_check_cli(
                [
                    str(contract_path),
                    "--require-matrix",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["passed"])
            self.assertIn("matrix_report_present", [check["name"] for check in payload["failed_checks"]])

    def test_trainer_contract_benchmark_cli_replays_contract_as_workload(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "reports/trainer-contract.json"
            output = root / "reports/contract-workload.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract_stdout = io.StringIO()
            contract_stderr = io.StringIO()
            contract_exit = run_trainer_contract_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--read-mode",
                    "direct-buffers",
                    "--batch-size",
                    "2",
                    "--batches",
                    "2",
                    "--rounds-per-read",
                    "2",
                    "--source-read-workers",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--trainer-prefetch-batches",
                    "2",
                    "--samples-per-epoch",
                    "4",
                    "--min-workload-runs",
                    "4",
                    "--min-workload-samples",
                    "16",
                    "--min-workload-bytes",
                    "84",
                    "--min-cord-sweep",
                    "2",
                    "--output",
                    str(contract_path),
                ],
                stdout=contract_stdout,
                stderr=contract_stderr,
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_contract_benchmark_cli(
                [
                    str(contract_path),
                    "--runs",
                    "2",
                    "--cord-sweep",
                    "1,4",
                    "--warmup-batches",
                    "1",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, contract_exit, contract_stderr.getvalue())
            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual("cxnfs.trainer-workload-benchmark.v1", payload["format"])
            self.assertTrue(payload["contract_applied"])
            self.assertEqual(TRAINER_CONTRACT_FORMAT, payload["trainer_contract"]["format"])
            self.assertEqual(str(contract_path), payload["trainer_contract"]["path"])
            self.assertTrue(payload["trainer_contract"]["validated"])
            self.assertEqual(4, payload["trainer_contract"]["workload_requirements"]["min_workload_runs"])
            self.assertEqual(4, payload["blockfs_workload_requirements"]["min_runs"])
            self.assertEqual(16, payload["blockfs_workload_requirements"]["min_samples"])
            self.assertEqual(84, payload["blockfs_workload_requirements"]["min_bytes"])
            self.assertEqual(2, payload["blockfs_workload_requirements"]["min_cord_sweep"])
            self.assertEqual(4, payload["run_count"])
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertEqual("native", payload["trainer_batch_iterator"])
            self.assertEqual(0, payload["data_loader_workers"])
            self.assertEqual(2, payload["trainer_prefetch_batches"])
            self.assertEqual("native", payload["benchmark_provenance"]["invocation"]["trainer_batch_iterator"])
            self.assertEqual(0, payload["benchmark_provenance"]["invocation"]["data_loader_workers"])
            self.assertEqual(2, payload["benchmark_provenance"]["invocation"]["trainer_prefetch_batches"])
            self.assertEqual({"chat": 2, "summary": 1}, payload["weights"])
            self.assertEqual(["1", "4", "1", "4"], [run["cord_label"] for run in payload["runs"]])
            self.assertEqual(16, payload["sample_count"])
            self.assertEqual("local-mds", payload["transport"])

    def test_trainer_contract_benchmark_argv_inherits_batch_iterator_contract(self):
        args = Namespace(
            root=None,
            weights=None,
            workload_name=None,
            remote_root=None,
            host=None,
            control_port=None,
            transport_port=None,
            bulk_transport=None,
            server_hostname=None,
            cafile=None,
            timeout=None,
            initial_cords=None,
            max_cords=None,
            cord_sweep=None,
            provisional_container_size=None,
            expected_size_limit=None,
            write_connections=None,
            bulk_read_retries=None,
            bulk_write_retries=None,
            write_probe_bytes=4096,
            write_probe_frame_bytes=1024,
            write_probe_root="/trainer-write-probes",
            shared_client=False,
            batch_size=None,
            batches=None,
            warmup_batches=None,
            runs=None,
            rounds_per_read=None,
            max_workers=None,
            source_read_workers=None,
            prefetch_chunks=None,
            shuffle=False,
            no_shuffle=False,
            seed=None,
            epoch=None,
            samples_per_epoch=None,
            decoder=None,
            read_mode=None,
            trainer_batch_iterator=None,
            data_loader_workers=None,
            objective=None,
            baseline_mb_s=None,
            fail_on_miss=False,
            include_run_batches=False,
            blockfs_min_runs=None,
            blockfs_min_samples=None,
            blockfs_min_bytes=None,
            blockfs_min_cord_sweep=None,
            server_disk_evidence=None,
            server_cache_evidence=None,
            server_cpu_evidence=None,
        )
        contract = {
            "root": "/stage0-mds",
            "weights": {"chat": 2, "summary": 1},
            "network_contract": {
                "bulk_transport": "quic",
                "max_cords": 64,
                "bulk_read_retries": 2,
                "bulk_write_retries": 3,
            },
            "read_contract": {
                "read_mode": "direct-buffers",
                "batch_size": 32,
                "requested_batches": 8,
                "rounds_per_read": 4,
                "shuffle": True,
                "trainer_batch_iterator": "trainer-dataloader",
                "data_loader_workers": 4,
                "trainer_prefetch_batches": 3,
            },
            "workload_requirements": {
                "format": "cxnfs.trainer-workload-requirements.v1",
                "configured": True,
                "min_workload_runs": 5,
                "min_workload_samples": 1024,
                "min_workload_bytes": 4096,
                "min_cord_sweep": 5,
            },
        }

        argv = torch_dataset_module._trainer_contract_benchmark_argv(args, contract)

        def option_value(option):
            self.assertIn(option, argv)
            return argv[argv.index(option) + 1]

        self.assertEqual("direct-buffers", option_value("--read-mode"))
        self.assertEqual("32", option_value("--batch-size"))
        self.assertEqual("8", option_value("--batches"))
        self.assertEqual("4", option_value("--rounds-per-read"))
        self.assertEqual("trainer-dataloader", option_value("--trainer-batch-iterator"))
        self.assertEqual("4", option_value("--data-loader-workers"))
        self.assertEqual("3", option_value("--trainer-prefetch-batches"))
        self.assertEqual("2", option_value("--bulk-read-retries"))
        self.assertEqual("3", option_value("--bulk-write-retries"))
        self.assertEqual("5", option_value("--blockfs-min-runs"))
        self.assertEqual("1024", option_value("--blockfs-min-samples"))
        self.assertEqual("4096", option_value("--blockfs-min-bytes"))
        self.assertEqual("5", option_value("--blockfs-min-cord-sweep"))
        self.assertEqual("4096", option_value("--write-probe-bytes"))
        self.assertEqual("1024", option_value("--write-probe-frame-bytes"))
        self.assertEqual("/trainer-write-probes", option_value("--write-probe-root"))
        self.assertIn("--shuffle", argv)

    def test_trainer_stream_smoke_cli_writes_stable_json_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/trainer-stream.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "2",
                    "--warmup-batches",
                    "1",
                    "--bulk-transport",
                    "stream",
                    "--rounds-per-read",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--trainer-prefetch-batches",
                    "2",
                    "--samples-per-epoch",
                    "3",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual("cxnfs.trainer-stream-smoke.v1", payload["format"])
            self.assertEqual(str(root / "mds"), payload["root"])
            self.assertEqual(["chat", "chat", "summary"], payload["source_schedule"])
            self.assertEqual(2, payload["batch_size"])
            self.assertEqual(2, payload["requested_batches"])
            self.assertEqual(1, payload["warmup_batches"])
            self.assertEqual("native", payload["trainer_batch_iterator"])
            self.assertEqual(0, payload["data_loader_workers"])
            self.assertEqual("CxnfsTrainerStream", payload["stream_object"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["stream_state_format"])
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, payload["stream_state"]["format"])
            self.assertTrue(payload["stream_state_matches_final_cursor"])
            self.assertEqual(payload["final_cursor"], payload["stream_state"]["cursor"])
            self.assertEqual(2, payload["prefetch_chunks"])
            self.assertEqual(2, payload["trainer_prefetch_batches"])
            self.assertTrue(payload["trainer_prefetch_enabled"])
            self.assertEqual("CxnfsTrainerPrefetchStream", payload["trainer_prefetch_stream_object"])
            self.assertEqual("local-mds", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertIsNone(payload["remote_root"])
            self.assertEqual(2, payload["batch_count"])
            self.assertEqual(3, payload["sample_count"])
            self.assertGreater(payload["byte_size"], 0)
            self.assertGreater(payload["elapsed_seconds"], 0.0)
            self.assertGreater(payload["samples_per_second"], 0.0)
            self.assertGreater(payload["bytes_per_second"], 0.0)
            self.assertEqual({"chat": 2}, payload["batches"][0]["source_counts"])
            self.assertEqual({"summary": 1}, payload["batches"][1]["source_counts"])

    def test_trainer_stream_smoke_cli_preserves_explicit_training_shard(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/trainer-stream-shard.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--batch-size",
                    "1",
                    "--batches",
                    "2",
                    "--samples-per-epoch",
                    "4",
                    "--read-mode",
                    "direct-buffers",
                    "--rank",
                    "1",
                    "--world-size",
                    "2",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual({"index": 1, "count": 2}, payload["training_shard"])
            self.assertEqual(1, payload["rank"])
            self.assertEqual(2, payload["world_size"])
            self.assertEqual({"index": 1, "count": 2}, payload["initial_cursor"]["shard"])
            self.assertEqual({"index": 1, "count": 2}, payload["final_cursor"]["shard"])
            self.assertEqual({"index": 1, "count": 2}, payload["stream_state"]["cursor"]["shard"])

    def test_trainer_stream_smoke_cli_writes_and_resumes_cursor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            first_output = root / "reports/first.json"
            second_output = root / "reports/second.json"
            cursor_path = root / "reports/resume-cursor.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            first_stdout = io.StringIO()
            first_stderr = io.StringIO()
            second_stdout = io.StringIO()
            second_stderr = io.StringIO()

            first_exit = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--samples-per-epoch",
                    "6",
                    "--write-resume-cursor",
                    str(cursor_path),
                    "--output",
                    str(first_output),
                ],
                stdout=first_stdout,
                stderr=first_stderr,
            )
            second_exit = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--resume-cursor",
                    str(cursor_path),
                    "--output",
                    str(second_output),
                ],
                stdout=second_stdout,
                stderr=second_stderr,
            )

            self.assertEqual(0, first_exit, first_stderr.getvalue())
            self.assertEqual(0, second_exit, second_stderr.getvalue())
            self.assertEqual(first_stdout.getvalue(), first_output.read_text(encoding="utf-8"))
            self.assertEqual(second_stdout.getvalue(), second_output.read_text(encoding="utf-8"))
            first_payload = json.loads(first_stdout.getvalue())
            second_payload = json.loads(second_stdout.getvalue())
            saved_cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
            self.assertEqual(TRAINER_CURSOR_FORMAT, saved_cursor["format"])
            self.assertEqual(first_payload["final_cursor"], saved_cursor)
            self.assertEqual("CxnfsTrainerStream", first_payload["stream_object"])
            self.assertTrue(first_payload["stream_state_matches_final_cursor"])
            self.assertTrue(second_payload["stream_state_matches_final_cursor"])
            self.assertEqual(2, first_payload["final_cursor"]["emitted"])
            self.assertEqual(2, second_payload["initial_cursor"]["emitted"])
            self.assertEqual(4, second_payload["final_cursor"]["emitted"])
            self.assertEqual(
                [("summary", 0), ("chat", 2)],
                [
                    (sample["source_name"], sample["sample_index"])
                    for sample in second_payload["batches"][0]["samples"]
                ],
            )

    def test_trainer_stream_smoke_cli_writes_and_resumes_stream_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            first_output = root / "reports/first.json"
            second_output = root / "reports/second.json"
            state_path = root / "reports/resume-state.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            first_stdout = io.StringIO()
            second_stdout = io.StringIO()

            first_exit = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--samples-per-epoch",
                    "6",
                    "--write-resume-state",
                    str(state_path),
                    "--output",
                    str(first_output),
                ],
                stdout=first_stdout,
                stderr=io.StringIO(),
            )
            second_exit = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--resume-state",
                    str(state_path),
                    "--output",
                    str(second_output),
                ],
                stdout=second_stdout,
                stderr=io.StringIO(),
            )

            self.assertEqual(0, first_exit)
            self.assertEqual(0, second_exit)
            self.assertEqual(first_stdout.getvalue(), first_output.read_text(encoding="utf-8"))
            self.assertEqual(second_stdout.getvalue(), second_output.read_text(encoding="utf-8"))
            first_payload = json.loads(first_stdout.getvalue())
            second_payload = json.loads(second_stdout.getvalue())
            saved_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(TRAINER_STREAM_STATE_FORMAT, saved_state["format"])
            self.assertEqual(first_payload["stream_state"], saved_state)
            self.assertEqual(first_payload["final_cursor"], saved_state["cursor"])
            self.assertEqual(first_payload["final_cursor"], second_payload["initial_cursor"])
            self.assertEqual({"chat": 1, "summary": 1}, second_payload["batches"][0]["source_counts"])
            self.assertTrue(second_payload["stream_state_matches_final_cursor"])
            self.assertFalse(tuple((root / "reports").glob(".resume-state.json.tmp-*")))

    def test_trainer_stream_smoke_cli_rejects_resume_state_contract_mismatch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "reports/resume-state.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            first_stdout = io.StringIO()
            second_stderr = io.StringIO()

            first_exit = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--write-resume-state",
                    str(state_path),
                ],
                stdout=first_stdout,
                stderr=io.StringIO(),
            )
            second_exit = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--batch-size",
                    "3",
                    "--batches",
                    "1",
                    "--resume-state",
                    str(state_path),
                ],
                stdout=io.StringIO(),
                stderr=second_stderr,
            )

            self.assertEqual(0, first_exit)
            self.assertEqual(1, second_exit)
            self.assertIn("trainer stream state batch_size does not match stream", second_stderr.getvalue())

    def test_trainer_stream_smoke_cli_can_use_direct_buffer_read_mode(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "2",
                    "--read-mode",
                    "direct-buffers",
                    "--source-read-workers",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--trainer-prefetch-batches",
                    "2",
                    "--samples-per-epoch",
                    "3",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertEqual("native", payload["trainer_batch_iterator"])
            self.assertEqual(3, payload["sample_count"])
            self.assertEqual(21, payload["byte_size"])
            self.assertEqual(TRAINER_READ_PATH_RELIABILITY_FORMAT, payload["read_path_reliability"]["format"])
            self.assertTrue(payload["read_path_reliability"]["complete"])
            self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
            self.assertTrue(payload["read_path_evidence"]["uses_caller_owned_destinations"])
            self.assertEqual(0, payload["read_path_evidence"]["bulk_payload_transport_count"])
            self.assertFalse(payload["read_path_evidence"]["transport_frame_sink_observed"])
            self.assertEqual({"chat": 2}, payload["batches"][0]["source_counts"])
            self.assertEqual({"summary": 1}, payload["batches"][1]["source_counts"])

    def test_trainer_surface_matrix_cli_runs_direct_buffer_native_case(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/trainer-surface-matrix.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_matrix_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--read-modes",
                    "direct-buffers",
                    "--trainer-batch-iterators",
                    "native",
                    "--batch-size",
                    "2",
                    "--batches",
                    "2",
                    "--rounds-per-read",
                    "2",
                    "--source-read-workers",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--trainer-prefetch-batches",
                    "2",
                    "--samples-per-epoch",
                    "3",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(TRAINER_SURFACE_MATRIX_FORMAT, payload["format"])
            self.assertTrue(payload["passed"])
            self.assertFalse(payload["requires_mount"])
            self.assertFalse(payload["touches_trainer_code"])
            self.assertEqual(1, payload["case_count"])
            self.assertEqual(1, payload["passed_case_count"])
            self.assertEqual(2, payload["trainer_prefetch_batches"])
            self.assertTrue(payload["read_path_reliability_complete"])
            self.assertEqual(0, payload["unrecovered_read_failures"])
            case = payload["cases"][0]
            self.assertEqual("direct-buffers/native", case["case_name"])
            self.assertTrue(case["passed"])
            self.assertEqual(3, case["sample_count"])
            self.assertEqual(2, case["trainer_prefetch_batches"])
            self.assertTrue(case["trainer_prefetch_enabled"])
            self.assertEqual(TRAINER_READ_PATH_RELIABILITY_FORMAT, case["read_path_reliability"]["format"])
            self.assertTrue(case["read_path_reliability"]["complete"])
            self.assertEqual(0, case["read_path_reliability"]["unrecovered_read_failures"])
            self.assertEqual(3, case["memory_path_gate"]["direct_buffer_read_operations"])
            self.assertTrue(case["memory_path_gate"]["complete"])

    def test_trainer_surface_matrix_cli_reports_explicit_training_shard(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/trainer-surface-matrix-shard.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_matrix_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--read-modes",
                    "direct-buffers",
                    "--trainer-batch-iterators",
                    "native",
                    "--batch-size",
                    "1",
                    "--batches",
                    "2",
                    "--samples-per-epoch",
                    "4",
                    "--rank",
                    "1",
                    "--world-size",
                    "2",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual({"index": 1, "count": 2}, payload["training_shard"])
            self.assertEqual(1, payload["rank"])
            self.assertEqual(2, payload["world_size"])
            case = payload["cases"][0]
            self.assertEqual({"index": 1, "count": 2}, case["training_shard"])
            self.assertEqual(1, case["rank"])
            self.assertEqual(2, case["world_size"])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for direct tensor matrix smoke test")
    def test_trainer_surface_matrix_cli_runs_direct_tensor_and_dataloader_cases(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_matrix_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--read-modes",
                    "direct-buffers,direct-tensors",
                    "--trainer-batch-iterators",
                    "native,trainer-dataloader",
                    "--data-loader-workers",
                    "0",
                    "--tensor-target",
                    "pytorch-cpu",
                    "--batch-size",
                    "3",
                    "--batches",
                    "1",
                    "--rounds-per-read",
                    "2",
                    "--source-read-workers",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--samples-per-epoch",
                    "3",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["passed"])
            self.assertEqual(4, payload["case_count"])
            self.assertEqual(4, payload["passed_case_count"])
            self.assertEqual(
                "cxnfs.trainer-remote-client-topology-aggregate.v1",
                payload["remote_client_topology"]["format"],
            )
            self.assertFalse(payload["remote_client_topology"]["remote_client_required"])
            self.assertTrue(payload["remote_client_topology"]["complete"])
            self.assertEqual(4, payload["remote_client_topology"]["topology_report_count"])
            self.assertTrue(all(case["remote_client_topology"]["complete"] for case in payload["cases"]))
            self.assertEqual(
                {
                    "direct-buffers/native",
                    "direct-buffers/trainer-dataloader",
                    "direct-tensors/native",
                    "direct-tensors/trainer-dataloader",
                },
                {case["case_name"] for case in payload["cases"]},
            )
            tensor_cases = [
                case for case in payload["cases"]
                if case["read_mode"] == "direct-tensors"
            ]
            self.assertEqual(2, len(tensor_cases))
            self.assertTrue(all(case["memory_path_gate"]["complete"] for case in tensor_cases))
            self.assertTrue(payload["read_path_reliability_complete"])
            self.assertEqual(0, payload["unrecovered_read_failures"])
            self.assertTrue(all(case["read_path_reliability"]["complete"] for case in payload["cases"]))
            self.assertTrue(all(case["memory_path_gate"]["direct_tensor_read_operations"] == 3 for case in tensor_cases))
            self.assertTrue(all(case["tensor_allocation"]["target"] == "pytorch-cpu" for case in tensor_cases))
            self.assertTrue(all(case["memory_path_gate"]["tensor_memory_path_complete"] for case in tensor_cases))
            self.assertTrue(all(case["memory_path_gate"]["network_fills_tensor_destination"] for case in tensor_cases))
            self.assertTrue(all(case["tensor_memory_path"]["format"] == TRAINER_TENSOR_MEMORY_PATH_FORMAT for case in tensor_cases))
            self.assertTrue(all(case["tensor_memory_path"]["complete"] for case in tensor_cases))
            self.assertTrue(all(case["tensor_memory_path"]["python_byte_batch_avoided"] for case in tensor_cases))
            self.assertTrue(all(case["tensor_memory_path"]["network_fills_tensor_destination"] for case in tensor_cases))
            self.assertTrue(all(not case["tensor_memory_path"]["device_copy_required"] for case in tensor_cases))

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer DataLoader worker matrix smoke test")
    def test_trainer_surface_matrix_cli_proves_dataloader_worker_partitioning(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(6)))
            write_file_dataset(root, "summary", tuple(f"summary-{index}" for index in range(4)))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_surface_matrix_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--read-modes",
                    "direct-buffers",
                    "--trainer-batch-iterators",
                    "trainer-dataloader",
                    "--data-loader-workers",
                    "2",
                    "--batch-size",
                    "2",
                    "--batches",
                    "4",
                    "--rounds-per-read",
                    "2",
                    "--samples-per-epoch",
                    "8",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["passed"])
            self.assertTrue(payload["worker_partition_required"])
            self.assertTrue(payload["worker_partition_complete"])
            self.assertEqual(0, payload["worker_partition_duplicate_source_sample_count"])
            case = payload["cases"][0]
            self.assertTrue(case["passed"])
            self.assertEqual("direct-buffers/trainer-dataloader", case["case_name"])
            self.assertTrue(case["worker_partition_evidence"]["complete"])
            self.assertEqual(2, case["worker_partition_evidence"]["observed_training_shard_count"])

    def test_trainer_stream_smoke_cli_rejects_data_loader_workers_for_native_iterator(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--data-loader-workers",
                    "1",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stdout.getvalue())
            self.assertIn("trainer-dataloader", stderr.getvalue())

    def test_trainer_stream_smoke_cli_can_fail_on_missed_objective_after_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/trainer-objective.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--objective",
                    "ai-training-25gbe",
                    "--baseline-mb-s",
                    "1000000000000",
                    "--fail-on-miss",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual("ai-training-25gbe", payload["objective"])
            self.assertEqual("legacy-mounted-share", payload["objective_baseline"])
            self.assertFalse(payload["objective_passed"])
            self.assertFalse(payload["objective_improvement_passed"])
            self.assertGreater(payload["objective_baseline_mb_per_second"], 0.0)
            self.assertIn("objective_candidate_link_utilization", payload)

    def test_trainer_workload_benchmark_cli_sweeps_cords_and_writes_aggregate_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/workload.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_workload_benchmark_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--workload-name",
                    "liam-shape-smoke",
                    "--batch-size",
                    "2",
                    "--batches",
                    "2",
                    "--warmup-batches",
                    "1",
                    "--runs",
                    "2",
                    "--cord-sweep",
                    "1,4",
                    "--rounds-per-read",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--trainer-prefetch-batches",
                    "2",
                    "--samples-per-epoch",
                    "4",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual("cxnfs.trainer-workload-benchmark.v1", payload["format"])
            self.assertEqual("liam-shape-smoke", payload["workload_name"])
            self.assertEqual("local-mds", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertEqual("native", payload["trainer_batch_iterator"])
            self.assertEqual(0, payload["data_loader_workers"])
            self.assertEqual(2, payload["trainer_prefetch_batches"])
            self.assertTrue(payload["trainer_prefetch_enabled"])
            self.assertEqual(2, payload["runs_requested"])
            self.assertEqual(4, payload["run_count"])
            self.assertEqual(16, payload["sample_count"])
            self.assertEqual(
                [
                    {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                    {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                ],
                payload["cord_sweep"],
            )
            self.assertEqual("cxnfs.weighted-mds-plan.v1", payload["weighted_mds_plan"]["format"])
            self.assertEqual(
                payload["weighted_mds_plan"]["workload_fingerprint"]["sha256"],
                payload["workload_fingerprint"]["sha256"],
            )
            self.assertEqual("cxnfs.trainer-workload-provenance.v1", payload["benchmark_provenance"]["format"])
            self.assertEqual("liam-shape-smoke", payload["benchmark_provenance"]["invocation"]["workload_name"])
            self.assertEqual("native", payload["benchmark_provenance"]["invocation"]["trainer_batch_iterator"])
            self.assertEqual(0, payload["benchmark_provenance"]["invocation"]["data_loader_workers"])
            self.assertEqual(2, payload["benchmark_provenance"]["invocation"]["trainer_prefetch_batches"])
            self.assertEqual(
                [{"initial_cords": 1, "max_cords": 1}, {"initial_cords": 4, "max_cords": 4}],
                payload["benchmark_provenance"]["invocation"]["cord_sweep"],
            )
            self.assertEqual("sha256", payload["benchmark_provenance"]["invocation_fingerprint"]["algorithm"])
            self.assertEqual(
                "cxnfs.trainer-workload-cursor-evidence.v1",
                payload["resume_cursor_evidence"]["format"],
            )
            self.assertEqual(TRAINER_CURSOR_FORMAT, payload["resume_cursor_evidence"]["cursor_format"])
            self.assertEqual(4, payload["resume_cursor_evidence"]["run_count"])
            self.assertTrue(payload["resume_cursor_evidence"]["complete"])
            self.assertTrue(payload["resume_cursor_evidence"]["all_runs_have_resume_cursors"])
            self.assertTrue(payload["resume_cursor_evidence"]["all_final_cursors_advance_by_sample_count"])
            self.assertEqual([4, 4, 4, 4], payload["resume_cursor_evidence"]["emitted_deltas"])
            self.assertEqual(0, payload["resume_cursor_evidence"]["first_initial_cursor"]["emitted"])
            self.assertEqual(4, payload["resume_cursor_evidence"]["last_final_cursor"]["emitted"])
            self.assertGreater(payload["mebibytes_per_second"], 0.0)
            self.assertGreater(payload["max_run_mebibytes_per_second"], 0.0)
            self.assertTrue(payload["read_path_evidence"]["all_runs_have_evidence"])
            self.assertEqual(4, payload["read_path_evidence"]["run_count"])
            self.assertEqual(0, payload["read_path_evidence"]["bulk_payload_transport_count"])
            self.assertEqual(
                "cxnfs.trainer-remote-client-topology-aggregate.v1",
                payload["remote_client_topology"]["format"],
            )
            self.assertFalse(payload["remote_client_topology"]["remote_client_required"])
            self.assertTrue(payload["remote_client_topology"]["complete"])
            self.assertEqual(4, payload["remote_client_topology"]["topology_report_count"])
            self.assertTrue(payload["network_path_summary"]["remote_client_topology_complete"])
            self.assertTrue(payload["network_path_summary"]["remote_client_per_source_complete"])
            self.assertEqual(TRAINER_READ_PATH_RELIABILITY_FORMAT, payload["read_path_reliability"]["format"])
            self.assertTrue(payload["read_path_reliability"]["complete"])
            self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
            self.assertEqual(
                TRAINER_SUSTAINED_READ_PROFILE_FORMAT,
                payload["sustained_read_profile"]["format"],
            )
            self.assertFalse(payload["sustained_read_profile"]["requires_quic_transport_evidence"])
            self.assertEqual(4, payload["sustained_read_profile"]["run_count"])
            self.assertEqual(8, payload["sustained_read_profile"]["total_batches"])
            self.assertFalse(payload["sustained_read_profile"]["complete"])
            self.assertTrue(payload["blockfs_gate"]["read_path_reliability_complete"])
            self.assertEqual(0, payload["blockfs_gate"]["read_path_reliability"]["unrecovered_read_failures"])
            self.assertFalse(payload["blockfs_gate"]["sustained_read_profile_complete"])
            self.assertFalse(payload["network_path_summary"]["java_python_network_path_exercised"])
            self.assertEqual(2, payload["network_path_summary"]["cord_sweep_count"])
            self.assertEqual("cxnfs.trainer-workload-pressure.v1", payload["pressure_profile"]["format"])
            self.assertFalse(payload["pressure_profile"]["objective_present"])
            self.assertFalse(payload["pressure_profile"]["java_python_network_path_exercised"])
            self.assertFalse(payload["pressure_profile"]["sustained_read_profile_complete"])
            self.assertTrue(payload["pressure_profile"]["read_path_reliability_complete"])
            self.assertEqual(0, payload["pressure_profile"]["unrecovered_read_failures"])
            self.assertFalse(payload["pressure_profile"]["decision_ready"])
            self.assertEqual("defer", payload["blockfs_gate"]["decision"])
            self.assertFalse(payload["blockfs_gate"]["kernel_module_allowed"])
            self.assertEqual(["disk", "cache", "cpu"], payload["server_evidence"]["missing"])
            self.assertEqual([0, 0, 1, 1], [run["run_index"] for run in payload["runs"]])
            self.assertEqual(["1", "4", "1", "4"], [run["cord_label"] for run in payload["runs"]])
            self.assertTrue(all(run["trainer_prefetch_enabled"] for run in payload["runs"]))
            self.assertTrue(all("batches" not in run for run in payload["runs"]))

    def test_trainer_workload_benchmark_cli_reports_explicit_training_shard(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/workload-shard.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_workload_benchmark_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--workload-name",
                    "rank-shard-smoke",
                    "--batch-size",
                    "1",
                    "--batches",
                    "2",
                    "--runs",
                    "1",
                    "--cord-sweep",
                    "1",
                    "--samples-per-epoch",
                    "4",
                    "--rank",
                    "1",
                    "--world-size",
                    "2",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual({"index": 1, "count": 2}, payload["training_shard"])
            self.assertEqual(1, payload["rank"])
            self.assertEqual(2, payload["world_size"])
            self.assertEqual(
                {"index": 1, "count": 2},
                payload["benchmark_provenance"]["invocation"]["training_shard"],
            )
            self.assertEqual(1, payload["run_count"])
            self.assertEqual({"index": 1, "count": 2}, payload["runs"][0]["training_shard"])
            self.assertEqual(1, payload["runs"][0]["rank"])
            self.assertEqual(2, payload["runs"][0]["world_size"])
            self.assertEqual({"index": 1, "count": 2}, payload["runs"][0]["final_cursor"]["shard"])

    def test_parse_cord_sweep_auto_uses_capped_standard_counts(self):
        self.assertEqual(
            ((1, 1), (8, 8), (16, 16), (32, 32), (64, 64)),
            torch_dataset_module._parse_cord_sweep("auto", 1, 64),
        )
        self.assertEqual(
            ((1, 1), (8, 8), (10, 10)),
            torch_dataset_module._parse_cord_sweep("auto", 1, 10),
        )
        self.assertEqual(
            ((1, 1), (4, 4)),
            torch_dataset_module._parse_cord_sweep("auto", 1, 4),
        )
        self.assertEqual(
            ((2, 8),),
            torch_dataset_module._parse_cord_sweep(None, 2, 8),
        )

    def test_trainer_workload_benchmark_cli_auto_cord_sweep_writes_provenance(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/workload-auto.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_workload_benchmark_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--workload-name",
                    "auto-cord-smoke",
                    "--batch-size",
                    "1",
                    "--batches",
                    "1",
                    "--runs",
                    "1",
                    "--max-cords",
                    "10",
                    "--cord-sweep",
                    "auto",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            expected_sweep = [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 2, "initial_cords": 10, "max_cords": 10},
            ]
            self.assertEqual(expected_sweep, payload["cord_sweep"])
            self.assertEqual(3, payload["run_count"])
            self.assertEqual(["1", "8", "10"], [run["cord_label"] for run in payload["runs"]])
            self.assertEqual(
                [
                    {"initial_cords": 1, "max_cords": 1},
                    {"initial_cords": 8, "max_cords": 8},
                    {"initial_cords": 10, "max_cords": 10},
                ],
                payload["benchmark_provenance"]["invocation"]["cord_sweep"],
            )
            self.assertEqual(3, payload["network_path_summary"]["cord_sweep_count"])

    def test_trainer_workload_benchmark_cli_runs_remote_write_probe(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            stdout = io.StringIO()
            stderr = io.StringIO()
            credentials = []
            controls = []
            bulks = []

            def credential_prompt():
                value = ClientCredentials("trainer", "secret")
                credentials.append(value)
                return value

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            exit_code = run_trainer_workload_benchmark_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--workload-name",
                    "trainer-write-probe",
                    "--remote-root",
                    "/remote/datasets",
                    "--host",
                    "cxnfs.example.internal",
                    "--server-hostname",
                    "cxnfs.example.internal",
                    "--bulk-transport",
                    "stream",
                    "--bulk-read-retries",
                    "1",
                    "--bulk-write-retries",
                    "2",
                    "--initial-cords",
                    "2",
                    "--max-cords",
                    "4",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--read-mode",
                    "direct-buffers",
                    "--write-probe-bytes",
                    "33",
                    "--write-probe-root",
                    "/write-probes",
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(1, len(credentials))
            self.assertTrue(credentials[0].closed)
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual([], bulks[0].write_paths)
            self.assertEqual(1, len(bulks[1].write_paths))
            self.assertTrue(bulks[1].write_paths[0].startswith("/write-probes/trainer-write-probe-run-0000-cord-00-2x4"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual(1, payload["bulk_read_retries"])
            self.assertEqual(2, payload["bulk_write_retries"])
            self.assertEqual(2, payload["benchmark_provenance"]["invocation"]["bulk_write_retries"])
            self.assertTrue(payload["write_probe"]["enabled"])
            self.assertEqual(33, payload["write_probe"]["byte_size"])
            self.assertEqual("/write-probes", payload["write_probe"]["remote_root"])
            self.assertTrue(payload["runs"][0]["write_probe"]["verified_read_back"])
            self.assertEqual(2, payload["runs"][0]["bulk_write_retries"])
            self.assertEqual(2, payload["runs"][0]["write_probe"]["bulk_write_retries"])
            self.assertEqual(0, payload["runs"][0]["write_probe"]["run_index"])
            self.assertEqual(0, payload["runs"][0]["write_probe"]["cord_index"])
            self.assertEqual(2, payload["runs"][0]["write_probe"]["initial_cords"])
            self.assertEqual(4, payload["runs"][0]["write_probe"]["max_cords"])
            self.assertEqual("2-4", payload["runs"][0]["write_probe"]["cord_label"])
            self.assertEqual(33, payload["runs"][0]["write_probe"]["bytes_sent"])
            self.assertTrue(payload["write_path_evidence"]["complete"])
            self.assertEqual(1, payload["write_path_evidence"]["write_probe_count"])
            self.assertEqual(1, payload["write_path_evidence"]["write_probe_cord_sweep_count"])
            self.assertEqual(["2-4"], payload["write_path_evidence"]["write_probe_cord_labels"])
            self.assertEqual(1, payload["write_path_evidence"]["run_count"])
            self.assertEqual(1, payload["write_path_evidence"]["verified_read_back_count"])
            self.assertTrue(payload["write_path_evidence"]["all_write_probes_verified"])
            self.assertEqual(33, payload["write_path_evidence"]["payload_bytes_written"])
            self.assertEqual(0, payload["write_path_evidence"]["write_retry_limit"])
            self.assertEqual(0, payload["write_path_evidence"]["write_retry_attempts"])
            self.assertEqual(0, payload["write_path_evidence"]["write_retry_successes"])
            self.assertEqual(0, payload["write_path_evidence"]["bulk_transport_write_reconnects"])
            self.assertTrue(payload["write_path_evidence"]["write_retry_attempts_recovered"])
            self.assertTrue(payload["write_path_evidence"]["write_retry_reconnects_observed"])
            self.assertTrue(payload["write_path_evidence"]["metadata_pending_observed"])
            self.assertEqual(2, payload["write_path_evidence"]["max_observed_write_cords"])
            self.assertTrue(payload["network_path_summary"]["write_probe_enabled"])
            self.assertTrue(payload["network_path_summary"]["write_probe_completed"])

    def test_trainer_workload_benchmark_cli_attaches_server_evidence_reports(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/workload-evidence.json"
            disk = root / "disk.report"
            cache = root / "cache.json"
            cpu = root / "cpu.report"
            disk.write_text("cxnfs.server.disk.v1\ndisk_busy_percent=91.5\n", encoding="utf-8")
            cache.write_text(json.dumps({"cache_effective": False, "page_cache_hit_ratio": 0.20}), encoding="utf-8")
            cpu.write_text("cpu_idle_percent=42\ncpu_saturated=false\n", encoding="utf-8")
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_workload_benchmark_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--server-disk-evidence",
                    str(disk),
                    "--server-cache-evidence",
                    str(cache),
                    "--server-cpu-evidence",
                    str(cpu),
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["server_evidence"]["complete"])
            self.assertEqual([], payload["server_evidence"]["missing"])
            self.assertEqual(91.5, payload["server_evidence"]["reports"]["disk"]["fields"]["disk_busy_percent"])
            self.assertFalse(payload["server_evidence"]["reports"]["cache"]["fields"]["cache_effective"])
            self.assertEqual("defer", payload["blockfs_gate"]["decision"])
            self.assertFalse(payload["blockfs_gate"]["kernel_module_allowed"])

    def test_read_path_aggregation_promotes_quic_stream_transport_evidence(self):
        aggregate = torch_dataset_module._aggregate_read_path_evidence([
            {
                "transport_frame_sink_supported": True,
                "read_operations": 1,
                "direct_buffer_read_operations": 1,
                "direct_destination_read_operations": 1,
                "direct_destination_frame_sink_read_operations": 1,
                "frame_sink_read_operations": 1,
                "payload_bytes_read": 4096,
                "max_observed_cords": 4,
                "bulk_payload_transport_count": 1,
                "bulk_transport_read_evidence": {
                    "format": "cxnfs.quic-bulk-transport-evidence.v1",
                    "tls_version": "TLSv1.3",
                    "cipher_suite": "TLS_AES_128_GCM_SHA256",
                    "connection_opened_count": 1,
                    "connection_attempt_count": 1,
                    "connection_retry_count": 0,
                    "successful_read_operations": 1,
                    "failed_read_operations": 0,
                    "last_read_stream_count": 4,
                    "last_attempted_read_stream_count": 4,
                    "attempted_read_streams": 4,
                    "read_streams_created": 4,
                    "read_streams_eof_verified": 4,
                    "read_stream_completion_verified": True,
                    "read_stream_eof_verified": True,
                    "total_attempted_streams": 4,
                    "total_successful_streams": 4,
                    "connection_reused_for_streams": True,
                    "read_partition_count": 4,
                    "read_partition_chunk_count": 4,
                    "read_partition_requested_bytes": 4096,
                    "read_partition_payload_bytes": 4096,
                    "read_partition_covers_plan": True,
                },
            },
            {
                "transport_frame_sink_supported": True,
                "read_operations": 1,
                "direct_buffer_read_operations": 1,
                "direct_destination_read_operations": 1,
                "direct_destination_frame_sink_read_operations": 1,
                "frame_sink_read_operations": 1,
                "payload_bytes_read": 4096,
                "max_observed_cords": 8,
                "bulk_payload_transport_count": 1,
                "bulk_transport_read_evidence": {
                    "format": "cxnfs.quic-bulk-transport-evidence.v1",
                    "tls_version": "TLSv1.3",
                    "cipher_suite": "TLS_AES_128_GCM_SHA256",
                    "connection_opened_count": 1,
                    "connection_attempt_count": 2,
                    "connection_retry_count": 1,
                    "successful_read_operations": 2,
                    "failed_read_operations": 0,
                    "last_read_stream_count": 8,
                    "last_attempted_read_stream_count": 8,
                    "attempted_read_streams": 8,
                    "read_streams_created": 8,
                    "read_streams_eof_verified": 8,
                    "read_stream_completion_verified": True,
                    "read_stream_eof_verified": True,
                    "total_attempted_streams": 8,
                    "total_successful_streams": 8,
                    "connection_reused_for_streams": True,
                    "read_partition_count": 8,
                    "read_partition_chunk_count": 8,
                    "read_partition_requested_bytes": 4096,
                    "read_partition_payload_bytes": 4096,
                    "read_partition_covers_plan": True,
                },
            },
        ])

        self.assertTrue(aggregate["quic_read_transport_observed"])
        self.assertTrue(aggregate["quic_read_transport_complete"])
        self.assertEqual(2, aggregate["quic_read_transport_evidence_count"])
        self.assertEqual(2, aggregate["quic_read_transport_connection_opened_count"])
        self.assertEqual(3, aggregate["quic_read_transport_connection_attempt_count"])
        self.assertEqual(1, aggregate["quic_read_transport_connection_retry_count"])
        self.assertEqual(3, aggregate["quic_read_transport_successful_read_operations"])
        self.assertEqual(0, aggregate["quic_read_transport_failed_read_operations"])
        self.assertEqual(12, aggregate["quic_read_transport_read_streams_created"])
        self.assertEqual(12, aggregate["quic_read_transport_read_streams_eof_verified"])
        self.assertEqual(2, aggregate["quic_read_transport_read_stream_completion_report_count"])
        self.assertEqual(2, aggregate["quic_read_transport_read_stream_eof_report_count"])
        self.assertTrue(aggregate["quic_read_transport_stream_completion_verified"])
        self.assertTrue(aggregate["quic_read_transport_stream_eof_verified"])
        self.assertEqual(12, aggregate["quic_read_transport_total_successful_streams"])
        self.assertEqual(2, aggregate["quic_read_transport_connection_reuse_count"])
        self.assertEqual(8, aggregate["quic_read_transport_max_read_stream_count"])
        self.assertTrue(aggregate["quic_read_transport_connection_reuse_observed"])
        self.assertEqual(12, aggregate["quic_read_transport_read_partition_count"])
        self.assertEqual(12, aggregate["quic_read_transport_read_partition_chunk_count"])
        self.assertEqual(8192, aggregate["quic_read_transport_read_partition_requested_bytes"])
        self.assertEqual(8192, aggregate["quic_read_transport_read_partition_payload_bytes"])
        self.assertEqual(2, aggregate["quic_read_transport_read_partition_coverage_report_count"])
        self.assertTrue(aggregate["quic_read_transport_read_partition_coverage_verified"])
        self.assertEqual(2, aggregate["quic_read_transport_expected_cipher_suite_count"])
        self.assertTrue(aggregate["quic_read_transport_all_reports_expected_cipher_suite"])
        self.assertEqual({"TLS_AES_128_GCM_SHA256": 2}, aggregate["quic_read_transport_cipher_suite_counts"])

        aggregate["all_runs_have_evidence"] = True
        gate = torch_dataset_module._trainer_workload_read_path_gate({
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "read_path_evidence": aggregate,
        })

        self.assertTrue(gate["complete"])
        self.assertTrue(gate["requires_quic_transport_evidence"])
        self.assertTrue(gate["quic_read_transport_complete"])
        self.assertTrue(gate["quic_read_transport_stream_eof_verified"])
        self.assertEqual(2, gate["quic_read_transport_evidence_count"])
        self.assertEqual(12, gate["quic_read_transport_read_streams_created"])
        self.assertEqual(12, gate["quic_read_transport_read_streams_eof_verified"])
        self.assertEqual(12, gate["quic_read_transport_read_partition_count"])
        self.assertEqual(12, gate["quic_read_transport_read_partition_chunk_count"])
        self.assertEqual(8192, gate["quic_read_transport_read_partition_payload_bytes"])
        self.assertTrue(gate["quic_read_transport_read_partition_coverage_verified"])
        self.assertEqual(2, gate["quic_read_transport_read_stream_eof_report_count"])
        self.assertEqual(2, gate["quic_read_transport_connection_reuse_count"])
        self.assertEqual(2, gate["quic_read_transport_expected_cipher_suite_count"])
        self.assertTrue(gate["quic_read_transport_all_reports_expected_cipher_suite"])
        self.assertEqual({"TLS_AES_128_GCM_SHA256": 2}, gate["quic_read_transport_cipher_suite_counts"])

    def test_read_path_aggregation_requires_quic_stream_eof_for_completion(self):
        aggregate = torch_dataset_module._aggregate_read_path_evidence([
            {
                "transport_frame_sink_supported": True,
                "read_operations": 1,
                "direct_buffer_read_operations": 1,
                "direct_destination_read_operations": 1,
                "direct_destination_frame_sink_read_operations": 1,
                "frame_sink_read_operations": 1,
                "payload_bytes_read": 4096,
                "max_observed_cords": 4,
                "bulk_payload_transport_count": 1,
                "bulk_transport_read_evidence": {
                    "format": "cxnfs.quic-bulk-transport-evidence.v1",
                    "tls_version": "TLSv1.3",
                    "cipher_suite": "TLS_AES_128_GCM_SHA256",
                    "connection_opened_count": 1,
                    "connection_attempt_count": 1,
                    "connection_retry_count": 0,
                    "successful_read_operations": 1,
                    "failed_read_operations": 0,
                    "last_read_stream_count": 4,
                    "last_attempted_read_stream_count": 4,
                    "attempted_read_streams": 4,
                    "read_streams_created": 4,
                    "read_streams_eof_verified": 0,
                    "read_stream_completion_verified": True,
                    "read_stream_eof_verified": False,
                    "total_attempted_streams": 4,
                    "total_successful_streams": 4,
                    "connection_reused_for_streams": True,
                },
            },
        ])

        self.assertTrue(aggregate["quic_read_transport_stream_completion_verified"])
        self.assertFalse(aggregate["quic_read_transport_stream_eof_verified"])
        self.assertFalse(aggregate["quic_read_transport_complete"])

        aggregate["all_runs_have_evidence"] = True
        gate = torch_dataset_module._trainer_workload_read_path_gate({
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "read_path_evidence": aggregate,
        })

        self.assertFalse(gate["complete"])
        self.assertIn("QUIC read streams must reach EOF after payload delivery", gate["missing"])

    def test_read_path_gate_fails_closed_when_quic_cipher_proof_is_missing(self):
        evidence = proven_quic_read_path_evidence()
        evidence["quic_read_transport_expected_cipher_suite_count"] = 0
        evidence["quic_read_transport_all_reports_expected_cipher_suite"] = False
        evidence["quic_read_transport_cipher_suite_counts"] = {}
        evidence["quic_read_transport_complete"] = False

        gate = torch_dataset_module._trainer_workload_read_path_gate({
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "read_path_evidence": evidence,
        })

        self.assertFalse(gate["complete"])
        self.assertFalse(gate["quic_read_transport_all_reports_expected_cipher_suite"])
        self.assertEqual(0, gate["quic_read_transport_expected_cipher_suite_count"])
        self.assertIn("QUIC read transport must use the required cipher suite", gate["missing"])

    def test_sustained_read_profile_requires_multi_run_quic_delivery(self):
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "run_count": 1,
            "batches_per_run": 1,
            "sample_count": 3,
            "byte_size": 4096,
            "cord_sweep": [
                {"initial_cords": 1, "max_cords": 1},
                {"initial_cords": 4, "max_cords": 4},
            ],
            "read_path_evidence": proven_quic_read_path_evidence(),
            "read_path_reliability": proven_quic_read_path_reliability(),
        }
        short_profile = torch_dataset_module._trainer_workload_sustained_read_profile(
            payload,
            {"java_python_network_path_exercised": True, "cord_sweep_count": 1},
        )

        self.assertEqual(TRAINER_SUSTAINED_READ_PROFILE_FORMAT, short_profile["format"])
        self.assertFalse(short_profile["complete"])
        self.assertIn("at least two workload runs are required", short_profile["missing"])
        self.assertIn("a multi-cord workload sweep is required", short_profile["missing"])

        sustained = dict(payload)
        sustained.update({
            "run_count": 2,
            "batches_per_run": 2,
            "sample_count": 12,
            "byte_size": 16384,
            "cord_sweep": [
                {"initial_cords": 1, "max_cords": 1},
                {"initial_cords": 4, "max_cords": 4},
            ],
        })
        full_profile = torch_dataset_module._trainer_workload_sustained_read_profile(
            sustained,
            {"java_python_network_path_exercised": True, "cord_sweep_count": 2},
        )

        self.assertTrue(full_profile["complete"])
        self.assertEqual([], full_profile["missing"])
        self.assertEqual(4, full_profile["total_batches"])

    def test_blockfs_gate_requires_sustained_quic_read_profile(self):
        evidence = proven_server_evidence()
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "run_count": 1,
            "batches_per_run": 1,
            "sample_count": 3,
            "byte_size": 4096,
            "cord_sweep": [{"initial_cords": 4, "max_cords": 4}],
            "read_path_evidence": proven_quic_read_path_evidence(),
            "read_path_reliability": proven_quic_read_path_reliability(),
            "write_path_evidence": proven_quic_write_path_evidence(run_count=1, payload_bytes=4096),
            "blockfs_workload_requirements": {
            "configured": True,
                "min_runs": 1,
                "min_samples": 3,
                "min_bytes": 4096,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-sustained-quic-read-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["sustained_read_profile_complete"])
        self.assertIn("at least two workload runs are required", gate["required_next_evidence"])

    def test_blockfs_gate_allows_prototype_only_after_remote_quic_storage_evidence(self):
        evidence = proven_server_evidence()
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("allow-blockfs-prototype", gate["decision"])
        self.assertTrue(gate["kernel_module_allowed"])
        self.assertTrue(gate["server_evidence_complete"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertTrue(gate["write_path_evidence_complete"])
        self.assertTrue(gate["memory_path_evidence_complete"])
        self.assertTrue(gate["workload_scale_complete"])

    def test_blockfs_gate_requires_java_python_network_path_evidence(self):
        evidence = proven_server_evidence()
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": False,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-java-python-network-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["java_python_network_path_exercised"])
        self.assertTrue(gate["server_evidence_complete"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertIn(
            "authenticated Java/Python remote QUIC workload proof",
            gate["required_next_evidence"],
        )

    def test_blockfs_gate_requires_direct_buffer_memory_path_evidence(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence["direct_buffer_read_operations"] = 0
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": read_path_evidence,
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-trainer-memory-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertFalse(gate["memory_path_evidence_complete"])
        self.assertIn("direct buffer reads must be observed", gate["required_next_evidence"])

    def test_blockfs_gate_requires_direct_tensor_memory_path_evidence(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence["direct_buffer_read_operations"] = 0
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-tensors",
            "tensor_allocation": CxnfsTrainerTensorAllocation().to_dict(),
            "read_path_evidence": read_path_evidence,
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-trainer-memory-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertFalse(gate["memory_path_evidence_complete"])
        self.assertIn("direct tensor reads must be observed", gate["required_next_evidence"])

    def test_blockfs_gate_requires_production_read_cord_watermark_for_full_sweep(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence["max_observed_cords"] = 4
        read_path_evidence["direct_destination_max_observed_cords"] = 4
        read_path_evidence["direct_destination_frame_sink_cord_fanout_verified"] = False
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": read_path_evidence,
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 2, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 3, "initial_cords": 32, "max_cords": 32},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-read-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["read_path_evidence_complete"])
        self.assertEqual(64, gate["read_path_gate"]["minimum_observed_cords"])
        self.assertEqual(4, gate["read_path_gate"]["max_observed_cords"])

    def test_blockfs_gate_requires_production_write_cord_watermark_for_full_sweep(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=5, payload_bytes=20480)
        write_path_evidence["max_observed_write_cords"] = 4
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 2, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 3, "initial_cords": 32, "max_cords": 32},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertEqual(64, gate["write_path_gate"]["minimum_observed_write_cords"])
        self.assertEqual(4, gate["write_path_gate"]["max_observed_write_cords"])

    def test_blockfs_gate_requires_timestamped_server_evidence_window(self):
        evidence = proven_server_evidence()
        evidence["sample_window_complete"] = False
        evidence["missing_sample_windows"] = ["disk"]
        evidence["sample_window"] = {
            "complete": False,
            "missing": ["disk"],
            "sample_start_unix_seconds": 1000.0,
            "sample_end_unix_seconds": 1005.0,
            "reports": {
                "cache": {"sample_start_unix_seconds": 1000.0, "sample_end_unix_seconds": 1005.0},
                "cpu": {"sample_start_unix_seconds": 1000.0, "sample_end_unix_seconds": 1005.0},
            },
        }
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("profile-storage", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["server_evidence_sample_window_complete"])
        self.assertIn("server disk evidence sample window", gate["required_next_evidence"])

    def test_blockfs_gate_keeps_write_and_scale_state_when_quic_tuning_continues(self):
        evidence = proven_server_evidence()
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.25,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("continue-quic-tuning", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertTrue(gate["write_path_evidence_complete"])
        self.assertTrue(gate["workload_scale_complete"])
        self.assertEqual(64, gate["write_path_gate"]["max_observed_write_cords"])

    def test_blockfs_gate_requires_recovered_quic_read_failures(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence["failed_read_operations"] = 1
        read_path_evidence["failed_direct_destination_read_operations"] = 1
        read_path_evidence["read_retry_attempts"] = 1
        read_path_evidence["read_retry_successes"] = 0
        read_path_evidence["bulk_transport_reconnects"] = 0
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": read_path_evidence,
            "write_path_evidence": proven_quic_write_path_evidence(run_count=5, payload_bytes=20480),
            "run_count": 5,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 5,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 5,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-read-reliability", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertTrue(gate["read_path_evidence_complete"])
        self.assertFalse(gate["read_path_reliability_complete"])
        self.assertEqual(1, gate["read_path_reliability"]["unrecovered_read_failures"])
        self.assertIn(
            "all read failures must be recovered by bounded retry",
            gate["required_next_evidence"],
        )

    def test_blockfs_gate_defers_until_direct_quic_read_path_is_proven(self):
        evidence = proven_server_evidence()
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-read-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["read_path_evidence_complete"])
        self.assertIn("read_path_evidence", gate["required_next_evidence"])

    def test_blockfs_gate_defers_until_multi_cord_frame_sink_reads_are_observed(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence["max_observed_cords"] = 1
        read_path_evidence["direct_destination_max_observed_cords"] = 1
        read_path_evidence["direct_destination_frame_sink_cord_fanout_verified"] = False
        read_path_evidence["frame_sink_used_for_all_direct_destination_reads"] = False
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": read_path_evidence,
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-read-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertIn(
            "all direct destination reads must use the QUIC frame sink",
            gate["required_next_evidence"],
        )
        self.assertIn("multi-cord QUIC delivery must be observed", gate["required_next_evidence"])

    def test_blockfs_gate_defers_when_explicit_direct_destination_fanout_is_false(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence["direct_destination_frame_sink_cord_fanout_verified"] = False
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": read_path_evidence,
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-read-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["read_path_evidence_complete"])
        self.assertFalse(gate["read_path_gate"]["direct_destination_frame_sink_cord_fanout_verified"])
        self.assertEqual(64, gate["read_path_gate"]["direct_destination_max_observed_cords"])
        self.assertIn("multi-cord QUIC delivery must be observed", gate["required_next_evidence"])

    def test_blockfs_gate_defers_until_quic_transport_stream_evidence_is_present(self):
        evidence = proven_server_evidence()
        read_path_evidence = proven_quic_read_path_evidence()
        for key in tuple(read_path_evidence):
            if key.startswith("quic_read_transport_"):
                read_path_evidence.pop(key)
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": read_path_evidence,
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-read-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["read_path_evidence_complete"])
        self.assertTrue(gate["read_path_gate"]["requires_quic_transport_evidence"])
        self.assertFalse(gate["read_path_gate"]["quic_read_transport_complete"])
        self.assertIn(
            "QUIC read transport stream evidence must be complete",
            gate["required_next_evidence"],
        )

    def test_blockfs_gate_defers_until_direct_quic_write_path_is_proven(self):
        evidence = proven_server_evidence()
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["kernel_module_allowed"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertIn("write_path_evidence", gate["required_next_evidence"])

    def test_blockfs_gate_defers_until_write_probe_read_back_is_verified(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        write_path_evidence["all_write_probes_verified"] = False
        write_path_evidence["verified_read_back_count"] = 1
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertIn("all write probes must verify read-back bytes", gate["required_next_evidence"])

    def test_blockfs_gate_requires_multi_cord_write_probe_evidence(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        write_path_evidence["max_observed_write_cords"] = 1
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertIn("multi-cord QUIC writes must be observed", gate["required_next_evidence"])

    def test_blockfs_gate_requires_write_probe_coverage_for_cord_sweep(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        write_path_evidence["write_probe_cord_sweep_count"] = 1
        write_path_evidence["write_probe_cord_labels"] = ["1"]
        payload = {
            "transport": "cxnfs-quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertIn("write probes must cover the workload cord sweep", gate["required_next_evidence"])

    def test_blockfs_gate_requires_quic_write_transport_stream_evidence(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        for key in (
            "quic_write_transport_evidence_count",
            "quic_write_transport_connection_reuse_count",
            "quic_write_transport_successful_write_operations",
            "quic_write_transport_failed_write_operations",
            "quic_write_transport_attempted_write_streams",
            "quic_write_transport_write_streams_created",
            "quic_write_transport_write_ack_streams_eof_verified",
            "quic_write_transport_max_active_write_streams",
            "quic_write_transport_concurrent_stream_report_count",
            "quic_write_transport_concurrent_streams_observed",
            "quic_write_transport_write_stream_success_report_count",
            "quic_write_transport_all_reports_write_streams_succeeded",
            "quic_write_transport_write_acks_received",
            "quic_write_transport_write_acks_verified",
            "quic_write_transport_write_ack_verification_failures",
            "quic_write_transport_write_ack_identity_report_count",
            "quic_write_transport_all_reports_write_ack_frame_identity_verified",
            "quic_write_transport_connection_opened_count",
            "quic_write_transport_all_reports_present",
            "quic_write_transport_all_reports_reused_connection",
            "quic_write_transport_complete",
        ):
            write_path_evidence.pop(key, None)
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertTrue(gate["write_path_gate"]["requires_quic_transport_evidence"])
        self.assertIn(
            "all QUIC write probes must include transport stream evidence",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write probes must reuse one connection across write streams",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must prove all attempted write streams succeeded",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must verify an ACK for every frame",
            gate["required_next_evidence"],
        )

    def test_blockfs_gate_requires_quic_write_ack_identity_evidence(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        write_path_evidence["write_ack_frame_verified"] = 1
        write_path_evidence["write_ack_frame_identity_verified"] = False
        write_path_evidence["quic_write_transport_write_acks_verified"] = 1
        write_path_evidence["quic_write_transport_write_ack_verification_failures"] = 1
        write_path_evidence["quic_write_transport_write_ack_identity_report_count"] = 1
        write_path_evidence["quic_write_transport_all_reports_write_ack_frame_identity_verified"] = False
        write_path_evidence["quic_write_transport_complete"] = False
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertFalse(gate["write_path_gate"]["write_ack_frame_identity_verified"])
        self.assertEqual(1, gate["write_path_gate"]["quic_write_transport_write_ack_verification_failures"])
        self.assertIn(
            "write acknowledgements must match sent frame identity",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must have no ACK verification failures",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must prove ACK frame identity for every report",
            gate["required_next_evidence"],
        )

    def test_blockfs_gate_requires_quic_write_ack_stream_eof_evidence(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        write_path_evidence["quic_write_transport_write_ack_stream_completion_report_count"] = 1
        write_path_evidence["quic_write_transport_write_ack_stream_eof_report_count"] = 1
        write_path_evidence["quic_write_transport_write_ack_streams_eof_verified"] = 1
        write_path_evidence["quic_write_transport_all_reports_write_ack_stream_completion_verified"] = False
        write_path_evidence["quic_write_transport_all_reports_write_ack_stream_eof_verified"] = False
        write_path_evidence["quic_write_transport_complete"] = False
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertFalse(gate["write_path_gate"]["quic_write_transport_all_reports_write_ack_stream_completion_verified"])
        self.assertFalse(gate["write_path_gate"]["quic_write_transport_all_reports_write_ack_stream_eof_verified"])
        self.assertEqual(1, gate["write_path_gate"]["quic_write_transport_write_ack_streams_eof_verified"])
        self.assertIn(
            "QUIC write transport must prove every ACK stream completed",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must prove every ACK stream reached EOF",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must EOF-verify every attempted ACK stream",
            gate["required_next_evidence"],
        )

    def test_blockfs_gate_requires_quic_write_transport_concurrency_evidence(self):
        evidence = proven_server_evidence()
        write_path_evidence = proven_quic_write_path_evidence(run_count=2, payload_bytes=8192)
        write_path_evidence["quic_write_transport_max_active_write_streams"] = 1
        write_path_evidence["quic_write_transport_concurrent_stream_report_count"] = 0
        write_path_evidence["quic_write_transport_concurrent_streams_observed"] = False
        write_path_evidence["quic_write_transport_complete"] = False
        payload = {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "objective": "ai-training-25gbe",
            "objective_passed_any": False,
            "read_mode": "direct-buffers",
            "read_path_evidence": proven_quic_read_path_evidence(),
            "write_path_evidence": write_path_evidence,
            "run_count": 2,
            "sample_count": 30,
            "byte_size": 210,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "blockfs_workload_requirements": {
                "configured": True,
                "min_runs": 2,
                "min_samples": 30,
                "min_bytes": 210,
                "min_cord_sweep": 2,
            },
        }
        network_summary = {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 2,
            "cord_scaling_improvement_ratio": 0.0,
        }

        gate = torch_dataset_module._trainer_workload_blockfs_gate(payload, network_summary, evidence)

        self.assertEqual("prove-quic-write-path", gate["decision"])
        self.assertFalse(gate["write_path_evidence_complete"])
        self.assertEqual(1, gate["write_path_gate"]["quic_write_transport_max_active_write_streams"])
        self.assertIn(
            "QUIC write transport must show concurrent active write streams",
            gate["required_next_evidence"],
        )
        self.assertIn(
            "QUIC write transport must include a concurrent write-stream report",
            gate["required_next_evidence"],
        )

    def test_trainer_workload_benchmark_cli_can_fail_when_no_run_hits_objective(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports/workload-objective.json"
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_workload_benchmark_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--runs",
                    "2",
                    "--objective",
                    "ai-training-25gbe",
                    "--baseline-mb-s",
                    "1000000000000",
                    "--fail-on-miss",
                    "--output",
                    str(output),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(stdout.getvalue(), output.read_text(encoding="utf-8"))
            payload = json.loads(stdout.getvalue())
            self.assertEqual("cxnfs.trainer-workload-benchmark.v1", payload["format"])
            self.assertFalse(payload["objective_passed_any"])
            self.assertFalse(payload["objective_passed_all"])
            self.assertEqual("cxnfs.trainer-workload-pressure.v1", payload["pressure_profile"]["format"])
            self.assertEqual("ai-training-25gbe", payload["pressure_profile"]["objective"])
            self.assertTrue(payload["pressure_profile"]["objective_present"])
            self.assertGreater(payload["pressure_profile"]["target_mebibytes_per_second"], 0.0)
            self.assertGreater(payload["pressure_profile"]["target_gap_mebibytes_per_second"], 0.0)
            self.assertTrue(payload["pressure_profile"]["read_path_reliability_complete"])
            self.assertEqual(0, payload["pressure_profile"]["unrecovered_read_failures"])
            self.assertFalse(payload["pressure_profile"]["decision_ready"])
            self.assertEqual("defer", payload["blockfs_gate"]["decision"])
            self.assertFalse(payload["blockfs_gate"]["kernel_module_allowed"])
            self.assertEqual([False, False], [run["objective_passed"] for run in payload["runs"]])

    def test_trainer_stream_smoke_cli_can_read_remote_payloads_with_per_source_clients(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()
            credentials = []
            controls = []
            bulks = []

            def credential_prompt():
                value = ClientCredentials("trainer", "secret")
                credentials.append(value)
                return value

            def control_factory(host, port, **kwargs):
                control = TrainerRemoteControl(host, port, kwargs)
                controls.append(control)
                return control

            def bulk_factory(control, host, port, **kwargs):
                bulk = TrainerRemoteBulkClient(root / "mds", control, host, port, kwargs)
                bulks.append(bulk)
                return bulk

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1,summary=1",
                    "--remote-root",
                    "/remote/datasets",
                    "--host",
                    "cxnfs.example.internal",
                    "--server-hostname",
                    "cxnfs.example.internal",
                    "--control-port",
                    "4509",
                    "--transport-port",
                    "4510",
                    "--bulk-transport",
                    "stream",
                    "--initial-cords",
                    "8",
                    "--max-cords",
                    "64",
                    "--write-connections",
                    "4",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--read-mode",
                    "direct-buffers",
                ],
                stdout=stdout,
                stderr=stderr,
                credential_prompt=credential_prompt,
                control_client_factory=control_factory,
                bulk_client_factory=bulk_factory,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(1, len(credentials))
            self.assertTrue(credentials[0].closed)
            self.assertEqual(2, len(controls))
            self.assertEqual(2, len(bulks))
            self.assertTrue(all(control.authenticated for control in controls))
            self.assertTrue(all(control.closed for control in controls))
            self.assertTrue(all(bulk.closed for bulk in bulks))
            self.assertEqual({"/remote/datasets/chat/chat-000000.mds"}, set(bulks[0].read_paths))
            self.assertEqual({"/remote/datasets/summary/summary-000000.mds"}, set(bulks[1].read_paths))
            read_config = bulks[0].options["read_config"]
            self.assertEqual(8, read_config.initial_cords)
            self.assertEqual(64, read_config.max_cords)
            self.assertEqual(4, bulks[0].options["write_connections"])
            self.assertTrue(bulks[0].options["close_control_client"])
            self.assertTrue(bulks[0].buffer_read_paths)
            self.assertTrue(bulks[1].buffer_read_paths)
            payload = json.loads(stdout.getvalue())
            self.assertEqual("cxnfs.trainer-stream-smoke.v1", payload["format"])
            self.assertEqual("cxnfs-length-prefixed", payload["transport"])
            self.assertEqual("stream", payload["bulk_transport"])
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertEqual("/remote/datasets", payload["remote_root"])
            self.assertEqual("cxnfs.example.internal", payload["host"])
            self.assertEqual(8, payload["initial_cords"])
            self.assertEqual(64, payload["max_cords"])
            self.assertFalse(payload["shared_client"])
            self.assertEqual(2, payload["sample_count"])
            evidence = payload["read_path_evidence"]
            self.assertTrue(evidence["uses_caller_owned_destinations"])
            self.assertEqual(2, evidence["bulk_payload_transport_count"])
            self.assertEqual(2, evidence["direct_destination_read_operations"])
            self.assertEqual(0, evidence["direct_destination_frame_sink_read_operations"])
            self.assertFalse(evidence["transport_frame_sink_supported"])
            self.assertFalse(evidence["frame_sink_used_for_all_direct_destination_reads"])
            topology = payload["remote_client_topology"]
            self.assertEqual("cxnfs.trainer-remote-client-topology.v1", topology["format"])
            self.assertFalse(topology["shared_client"])
            self.assertEqual(2, topology["unique_source_request_count"])
            self.assertEqual(2, topology["connection_count"])
            self.assertEqual(2, topology["client_open_count"])
            self.assertEqual(2, topology["active_client_count"])
            self.assertEqual(0, topology["client_close_count"])
            self.assertFalse(topology["all_open_clients_closed"])
            self.assertTrue(topology["per_source_client_observed"])
            self.assertTrue(topology["complete"])
            closed_topology = payload["remote_client_topology_after_close"]
            self.assertEqual(2, closed_topology["client_close_count"])
            self.assertEqual(0, closed_topology["active_client_count"])
            self.assertTrue(closed_topology["all_open_clients_closed"])
            self.assertTrue(payload["remote_client_lifecycle_closed"])

    @unittest.skipIf(importlib.util.find_spec("aioquic"), "aioquic is installed")
    def test_trainer_stream_smoke_cli_rejects_python_quic_before_prompt(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        prompted = []

        exit_code = run_trainer_stream_smoke_cli(
            [
                "/datasets/stage0-mds",
                "--weights",
                "chat=1",
                "--remote-root",
                "/remote/datasets",
                "--bulk-transport",
                "quic",
            ],
            stdout=stdout,
            stderr=stderr,
            credential_prompt=lambda: prompted.append(True) or ClientCredentials("trainer", "secret"),
        )

        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout.getvalue())
        self.assertIn("Python QUIC bulk transport requires the optional aioquic dependency", stderr.getvalue())
        self.assertEqual([], prompted)

    def test_weighted_mds_plan_cli_reports_missing_source(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "mds").mkdir()
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_weighted_mds_plan_cli(
                [str(root / "mds"), "--weights", "missing=1"],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stdout.getvalue())
            self.assertIn("index.json", stderr.getvalue())

    def test_weighted_mds_sources_can_use_dedicated_remote_clients_per_source(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            transports = {}
            clients = {}

            def client_factory(name):
                transport = RemoteShardTransport(root / "mds" / name)
                transports[name] = transport
                client = ClosableDatasetClient(transport)
                clients[name] = client
                return client

            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=1,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
                client_factory=client_factory,
                remote_root="/remote/datasets",
            )
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False, include_source_metadata=True)

            items = list(dataset)

            self.assertEqual(["chat-0", "summary-0", "chat-1"], [item.data for item in items])
            self.assertEqual(
                ["/remote/datasets/chat/chat-000000.mds"],
                sorted(set(transports["chat"].read_paths)),
            )
            self.assertGreater(len(transports["chat"].read_paths), 1)
            self.assertEqual(
                ["/remote/datasets/summary/summary-000000.mds"],
                sorted(set(transports["summary"].read_paths)),
            )
            self.assertIsNot(sources[0].client, sources[1].client)
            dataset.close()
            self.assertTrue(clients["chat"].closed)
            self.assertTrue(clients["summary"].closed)
            self.assertEqual(1, clients["chat"].close_count)
            self.assertEqual(1, clients["summary"].close_count)

    def test_weighted_mds_sources_reject_remote_root_traversal(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))

            with self.assertRaisesRegex(ValueError, "parent"):
                weighted_mds_sources_from_root(
                    root / "mds",
                    "chat=1",
                    client=ClosableDatasetClient(RemoteShardTransport(root / "mds" / "chat")),
                    remote_root="/remote/../datasets",
                )

    def test_weighted_mds_close_releases_shared_remote_client_once(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            write_file_dataset(root, "summary", ("summary-0",))
            shared = ClosableDatasetClient(RemoteShardTransport(root / "mds" / "chat"))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=1,summary=1",
                client=shared,
            )
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)

            dataset.close()

            self.assertTrue(shared.closed)
            self.assertEqual(1, shared.close_count)

    def test_weighted_mds_source_wraps_existing_mds_dataset_with_transform(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            mds = MdsDataset(root / "mds" / "chat")
            dataset = CxnfsWeightedTorchDataset(
                (
                    CxnfsWeightedSource(
                        "chat",
                        mds,
                        transform=lambda sample: sample.data.decode("utf-8").upper(),
                    ),
                ),
                repeat=False,
            )

            self.assertEqual(("CHAT-0", "CHAT-1"), tuple(dataset))

    def test_weighted_mds_source_rejects_ignored_transform_for_existing_torch_dataset(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            torch_dataset = CxnfsTorchDataset(root / "mds" / "chat")

            with self.assertRaisesRegex(ValueError, "transform"):
                CxnfsWeightedSource(
                    "chat",
                    torch_dataset,
                    transform=lambda sample: sample,
                ).dataset()

            with self.assertRaisesRegex(ValueError, "decoder"):
                CxnfsWeightedSource(
                    "chat",
                    torch_dataset,
                    decoder=MdsSampleFormat.PARQUET_JSON,
                ).dataset()

    def test_weighted_mds_close_releases_existing_torch_dataset_clients(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            client = ClosableDatasetClient(RemoteShardTransport(root / "mds" / "chat"))
            torch_dataset = CxnfsTorchDataset(root / "mds" / "chat", client=client)
            weighted = CxnfsWeightedTorchDataset((CxnfsWeightedSource("chat", torch_dataset),), repeat=False)

            weighted.close()

            self.assertTrue(client.closed)

    def test_weighted_mds_active_sources_read_concurrently(self):
        barrier = threading.Barrier(2)
        left = BlockingTorchDataset("chat", ("chat-0",), barrier)
        right = BlockingTorchDataset("summary", ("summary-0",), barrier)
        dataset = CxnfsWeightedTorchDataset(
            (
                CxnfsWeightedSource("chat", left),
                CxnfsWeightedSource("summary", right),
            ),
            repeat=False,
            include_source_metadata=True,
            max_workers=1,
        )

        items = list(dataset)

        self.assertEqual(["chat-0", "summary-0"], [item.data for item in items])
        self.assertEqual([(0,)], left.reads)
        self.assertEqual([(0,)], right.reads)

    def test_weighted_mds_prefetches_future_chunks_while_current_chunk_is_reading(self):
        source = PrefetchBlockingTorchDataset(("sample-0", "sample-1"))
        dataset = CxnfsWeightedTorchDataset(
            (CxnfsWeightedSource("chat", source),),
            repeat=False,
            prefetch_chunks=2,
        )

        items = list(dataset)

        self.assertEqual(["sample-0", "sample-1"], items)
        self.assertEqual([(0,), (1,)], source.reads)

    def test_weighted_mds_prefetch_byte_window_records_backpressure(self):
        source = SimpleTorchDataset(("sample-0", "sample-1", "sample-2"))
        dataset = CxnfsWeightedTorchDataset(
            (CxnfsWeightedSource("chat", source),),
            repeat=False,
            prefetch_chunks=2,
            max_prefetch_bytes=1,
        )

        items = list(dataset)
        evidence = dataset.prefetch_admission_evidence()

        self.assertEqual(["sample-0", "sample-1", "sample-2"], items)
        self.assertTrue(evidence["bounded"])
        self.assertEqual(1, evidence["max_prefetch_bytes"])
        self.assertGreater(evidence["delayed_plan_count"], 0)
        self.assertTrue(evidence["byte_window_engaged"])
        self.assertTrue(evidence["byte_window_respected"])
        self.assertLessEqual(evidence["max_observed_inflight_bytes"], 1)

    def test_weighted_mds_source_failures_raise_trainer_surface_error(self):
        source = FailingOnceTorchDataset(("sample-0",))
        dataset = CxnfsWeightedTorchDataset(
            (CxnfsWeightedSource("chat", source),),
            repeat=False,
        )

        with self.assertRaises(CxnfsTrainerSurfaceError) as failure:
            list(dataset)

        error = failure.exception
        self.assertEqual("weighted source batch read", error.operation)
        self.assertIsInstance(error.__cause__, OSError)
        self.assertEqual("chat", error.context["source_name"])
        self.assertEqual(0, error.context["source_index"])
        self.assertEqual([0], error.context["sample_indices"])
        self.assertEqual("samples", error.context["read_mode"])
        payload = error.to_dict()
        self.assertEqual("cxnfs.trainer-surface-error.v1", payload["format"])
        self.assertEqual("OSError", payload["cause_type"])

    def test_weighted_mds_shuffle_is_epoch_deterministic_without_materializing_indices(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(8)))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                {"chat": 1},
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=False,
                shuffle=True,
                seed=2026,
                include_source_metadata=True,
            )

            first_epoch = list(dataset.iter_epoch(0))
            first_epoch_again = list(dataset.iter_epoch(0))
            second_epoch = list(dataset.iter_epoch(1))

            self.assertEqual([item.sample_index for item in first_epoch], [item.sample_index for item in first_epoch_again])
            self.assertEqual([item.data for item in first_epoch], [item.data for item in first_epoch_again])
            self.assertNotEqual([item.sample_index for item in first_epoch], list(range(8)))
            self.assertNotEqual([item.sample_index for item in first_epoch], [item.sample_index for item in second_epoch])
            self.assertEqual(set(range(8)), {item.sample_index for item in first_epoch})
            self.assertEqual(set(range(8)), {item.sample_index for item in second_epoch})

    def test_weighted_mds_shuffle_preserves_rank_worker_shard_disjointness(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(8)))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                {"chat": 1},
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=False,
                shuffle=True,
                seed=17,
                include_source_metadata=True,
            )

            worker_zero = list(dataset.iter_epoch(3, CxnfsTrainingShard(0, 2)))
            worker_one = list(dataset.iter_epoch(3, CxnfsTrainingShard(1, 2)))

            zero_indices = {item.sample_index for item in worker_zero}
            one_indices = {item.sample_index for item in worker_one}
            self.assertEqual(4, len(zero_indices))
            self.assertEqual(4, len(one_indices))
            self.assertTrue(zero_indices.isdisjoint(one_indices))
            self.assertEqual(set(range(8)), zero_indices | one_indices)

    def test_weighted_mds_samples_per_epoch_caps_local_stream_without_extra_reads(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(8)))
            write_file_dataset(root, "summary", tuple(f"summary-{index}" for index in range(8)))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=True,
                samples_per_epoch=5,
                include_source_metadata=True,
            )

            items = list(dataset.iter_epoch(0))

            self.assertEqual(5, len(items))
            self.assertEqual(["chat", "chat", "summary", "chat", "chat"], [item.source_name for item in items])
            self.assertEqual(["chat-0", "chat-1", "summary-0", "chat-2", "chat-3"], [item.data for item in items])

    def test_weighted_mds_set_epoch_controls_default_iterator(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(8)))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                {"chat": 1},
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=False,
                shuffle=True,
                seed=9,
            )

            dataset.set_epoch(0)
            first = list(dataset)
            dataset.set_epoch(1)
            second = list(dataset)

            self.assertNotEqual(first, second)

    def test_weighted_mds_rejects_invalid_source_read_workers(self):
        with self.assertRaisesRegex(ValueError, "source_read_workers"):
            CxnfsWeightedTorchDataset(
                (CxnfsWeightedSource("chat", BlockingTorchDataset("chat", ("chat-0",), threading.Barrier(1))),),
                source_read_workers=0,
            )

        with self.assertRaisesRegex(ValueError, "prefetch_chunks"):
            CxnfsWeightedTorchDataset(
                (CxnfsWeightedSource("chat", BlockingTorchDataset("chat", ("chat-0",), threading.Barrier(1))),),
                prefetch_chunks=0,
            )

        with self.assertRaisesRegex(ValueError, "samples_per_epoch"):
            CxnfsWeightedTorchDataset(
                (CxnfsWeightedSource("chat", BlockingTorchDataset("chat", ("chat-0",), threading.Barrier(1))),),
                samples_per_epoch=0,
            )

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for weighted DataLoader smoke test")
    def test_weighted_mds_dataset_works_with_regular_torch_dataloader(self):
        from torch.utils.data import DataLoader

        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0", "summary-1"))
            sources = weighted_mds_sources_from_root(
                root / "mds",
                "chat=2,summary=1",
                transform=lambda sample: sample.data.decode("utf-8"),
            )
            dataset = CxnfsWeightedTorchDataset(sources, rounds_per_read=1).to_torch_dataset()

            loader = DataLoader(dataset, batch_size=3, collate_fn=lambda items: items)
            first_batch = next(iter(loader))

            self.assertEqual(["chat-0", "chat-1", "summary-0"], first_batch)

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer batch DataLoader smoke test")
    def test_weighted_mds_trainer_dataloader_yields_prebuilt_direct_batches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1", "chat-2"))
            write_file_dataset(root, "summary", ("summary-0",))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")
            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
                source_read_workers=2,
                prefetch_chunks=2,
            )

            loader = dataset.to_trainer_dataloader(
                3,
                read_mode="direct-buffers",
                include_paths=True,
                data_loader_kwargs={"num_workers": 0},
            )
            first_batch = next(iter(loader))

            self.assertIsInstance(first_batch, CxnfsTrainerBatch)
            self.assertEqual(3, first_batch.sample_count)
            self.assertEqual(["chat", "chat", "summary"], [sample.source_name for sample in first_batch.samples])
            self.assertEqual([b"chat-0", b"chat-1", b"summary-0"], [bytes(sample.data) for sample in first_batch.samples])
            self.assertEqual(["0000.txt", "0001.txt", "0000.txt"], [sample.path for sample in first_batch.samples])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer batch DataLoader worker smoke test")
    def test_weighted_mds_trainer_dataloader_workers_partition_samples(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(6)))
            write_file_dataset(root, "summary", tuple(f"summary-{index}" for index in range(4)))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=2,summary=1")
            dataset = CxnfsWeightedTorchDataset(
                sources,
                rounds_per_read=2,
                repeat=False,
            )

            loader = dataset.to_trainer_dataloader(
                2,
                read_mode="direct-buffers",
                include_paths=True,
                samples_per_epoch=8,
                data_loader_kwargs={"num_workers": 2},
            )
            batches = list(itertools.islice(loader, 4))

            sample_keys = [
                (sample.source_name, sample.sample_index)
                for batch in batches
                for sample in batch.samples
            ]
            shard_keys = {
                (batch.resume_cursor.shard_index, batch.resume_cursor.shard_count)
                for batch in batches
            }

            self.assertEqual(4, len(batches))
            self.assertEqual(8, len(sample_keys))
            self.assertEqual(len(sample_keys), len(set(sample_keys)))
            self.assertEqual({(0, 2), (1, 2)}, shard_keys)

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer batch DataLoader smoke test")
    def test_weighted_mds_trainer_dataloader_rejects_rebatching_options(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0",))
            sources = weighted_mds_sources_from_root(root / "mds", "chat=1")
            dataset = CxnfsWeightedTorchDataset(sources, repeat=False)

            with self.assertRaisesRegex(ValueError, "batch_size"):
                dataset.to_trainer_dataloader(1, data_loader_kwargs={"batch_size": 2})

            with self.assertRaisesRegex(ValueError, "drop_last"):
                dataset.to_trainer_dataloader(1, data_loader_kwargs={"drop_last": True})

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer DataLoader CLI smoke test")
    def test_trainer_stream_smoke_cli_can_use_trainer_dataloader_batches(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "3",
                    "--batches",
                    "1",
                    "--read-mode",
                    "direct-buffers",
                    "--trainer-batch-iterator",
                    "trainer-dataloader",
                    "--data-loader-workers",
                    "0",
                    "--rounds-per-read",
                    "2",
                    "--source-read-workers",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--samples-per-epoch",
                    "3",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            self.assertEqual("trainer-dataloader", payload["trainer_batch_iterator"])
            self.assertEqual(0, payload["data_loader_workers"])
            self.assertEqual("direct-buffers", payload["read_mode"])
            self.assertEqual(1, payload["batch_count"])
            self.assertEqual(3, payload["sample_count"])
            self.assertEqual({"chat": 2, "summary": 1}, payload["batches"][0]["source_counts"])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer DataLoader worker CLI smoke test")
    def test_trainer_stream_smoke_cli_reports_dataloader_worker_partitioning(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", tuple(f"chat-{index}" for index in range(6)))
            write_file_dataset(root, "summary", tuple(f"summary-{index}" for index in range(4)))
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_trainer_stream_smoke_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--batch-size",
                    "2",
                    "--batches",
                    "4",
                    "--read-mode",
                    "direct-buffers",
                    "--trainer-batch-iterator",
                    "trainer-dataloader",
                    "--data-loader-workers",
                    "2",
                    "--rounds-per-read",
                    "2",
                    "--samples-per-epoch",
                    "8",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            payload = json.loads(stdout.getvalue())
            evidence = payload["worker_partition_evidence"]
            self.assertEqual(TRAINER_WORKER_PARTITION_FORMAT, evidence["format"])
            self.assertTrue(evidence["worker_sharding_enabled"])
            self.assertTrue(evidence["complete"])
            self.assertEqual(2, evidence["data_loader_workers"])
            self.assertEqual(2, evidence["expected_effective_training_shards"])
            self.assertEqual(2, evidence["observed_training_shard_count"])
            self.assertEqual(0, evidence["duplicate_source_sample_count"])
            self.assertTrue(evidence["no_duplicate_source_samples"])
            self.assertTrue(payload["stream_state"]["worker_partitioning_enabled"])
            self.assertEqual(2, payload["stream_state"]["worker_cursor_count"])
            self.assertEqual(2, len(payload["stream_state"]["worker_cursors"]))
            self.assertEqual(1, payload["stream_state"]["cursor"]["shard"]["count"])
            read_evidence = payload["read_path_evidence"]
            self.assertTrue(read_evidence["worker_batch_read_evidence_observed"])
            self.assertEqual(4, read_evidence["worker_batch_read_evidence_count"])
            self.assertEqual(
                {True},
                {
                    report["produced_inside_torch_worker"]
                    for report in read_evidence["worker_process_read_path_evidence"]
                },
            )

    def test_rejects_unknown_decoder(self):
        with self.assertRaises(ValueError):
            CxnfsTorchDataset(".", decoder="unknown")


class TrainerNetworkPreflightControl:
    def __init__(self):
        self.session_id = UUID("00000000-0000-0000-0000-000000000202")
        self.authenticated_username = None
        self.list_paths = []
        self.closed = False

    def authenticate(self, credentials):
        self.authenticated_username = credentials.username
        return AuthAccepted(self.session_id, (AuthMechanism.USERNAME_PASSWORD,))

    def list_directory(self, path):
        self.list_paths.append(path)
        return DirectoryListResponse(DirectoryListing(path, (
            FileMetadata(
                f"{path.rstrip('/')}/chat/chat-000000.mds",
                "chat-000000.mds",
                FileType.REGULAR_FILE,
                1,
            ),
        )))

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class TrainerNetworkPreflightBulk:
    def __init__(self, payload):
        self.payload = payload
        self.reads = []
        self.closed = False

    def read_mds_shard(self, path, offset=0, length=None):
        self.reads.append((path, offset, length))
        end = len(self.payload) if length is None else offset + length
        return self.payload[offset:end]

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class TrainerNetworkPreflightPrompt:
    def __init__(self):
        self.calls = 0
        self.credentials = None

    def __call__(self):
        self.calls += 1
        self.credentials = ClientCredentials("trainer", "secret")
        return self.credentials


class TrainerNetworkPreflightControlFactory:
    def __init__(self):
        self.client = TrainerNetworkPreflightControl()
        self.calls = []

    def __call__(self, host, port, **kwargs):
        self.calls.append({"host": host, "port": port, **kwargs})
        return self.client


class TrainerNetworkPreflightBulkFactory:
    def __init__(self, payload):
        self.client = TrainerNetworkPreflightBulk(payload)
        self.calls = []

    def __call__(self, control_client, host, port, **kwargs):
        self.control_client = control_client
        self.calls.append({"host": host, "port": port, **kwargs})
        return self.client


class RecordingTrainerNetworkTlsContext:
    def __init__(self, cafile):
        self.cafile = cafile


class RecordingTrainerNetworkTlsContextFactory:
    def __call__(self, cafile):
        return RecordingTrainerNetworkTlsContext(cafile)


def make_tar(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))


def write_file_dataset(root: Path, name: str, values: tuple[str, ...]) -> None:
    raw = root / "raw" / name
    raw.mkdir(parents=True)
    for index, value in enumerate(values):
        (raw / f"{index:04d}.txt").write_text(value, encoding="utf-8")
    plan = DatasetPreparer(target_shard_size_bytes=4096).plan(raw, name, "files")
    DatasetConverter().execute(plan, root / "mds" / name)


class FakeTensor:
    def __init__(self, nbytes: int) -> None:
        self.data = bytearray(nbytes)
        self.device = "cpu"
        self.is_cuda = False


class PipelineRecordingClient(CxnfsPythonClient):
    def __init__(self, root: Path):
        super().__init__(LocalFileTransport(root))
        self.tensor_reads = []

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        self.tensor_reads.append((str(shard_path), offset, nbytes, target, device, non_blocking))
        return super().read_into_tensor(shard_path, offset, tensor, nbytes, target, device, non_blocking)


class RemoteShardTransport:
    def __init__(self, local_root: Path):
        self.local_root = local_root
        self.read_paths = []
        self.buffer_read_paths = []
        self.payload_bytes_read = 0

    def read_mds_shard(self, operation):
        self.read_paths.append(operation.shard_path)
        payload = self._read_payload(operation.shard_path, operation.offset, operation.length)
        self.payload_bytes_read += len(payload)
        return payload

    def read_tensor(self, operation):
        payload = self._read_payload(operation.shard_path, operation.offset, operation.nbytes)
        self.payload_bytes_read += len(payload)
        return payload

    def read_into_tensor(self, operation):
        payload = self._read_payload(operation.shard_path, operation.offset, operation.nbytes)
        self.payload_bytes_read += len(payload)
        target = getattr(operation.destination.tensor, "data", operation.destination.tensor)
        target[:len(payload)] = payload
        return len(payload)

    def read_into_buffer(self, operation):
        self.buffer_read_paths.append(operation.shard_path)
        chunk = self._read_payload(operation.shard_path, operation.offset, operation.nbytes)
        self.payload_bytes_read += len(chunk)
        operation.destination[:len(chunk)] = chunk
        return len(chunk)

    def write_from_tensor(self, operation):
        return SendResult(1, 1, operation.nbytes)

    def _read_payload(self, shard_path, offset=0, length=None):
        payload = (self.local_root / Path(shard_path).name).read_bytes()
        end = len(payload) if length is None else offset + length
        return payload[offset:end]

    def send_safetensors_output(self, operation):
        return SendResult(0, 0, 0)

    def read_path_evidence(self):
        read_operations = len(self.read_paths) + len(self.buffer_read_paths)
        return {
            "bulk_transport_class": "RemoteShardTransport",
            "transport_frame_sink_supported": False,
            "read_operations": read_operations,
            "byte_return_read_operations": len(self.read_paths),
            "direct_buffer_read_operations": len(self.buffer_read_paths),
            "direct_tensor_read_operations": 0,
            "direct_destination_read_operations": len(self.buffer_read_paths),
            "frame_sink_read_operations": 0,
            "direct_destination_frame_sink_read_operations": 0,
            "fallback_frame_iter_read_operations": read_operations,
            "failed_read_operations": 0,
            "failed_byte_return_read_operations": 0,
            "failed_direct_buffer_read_operations": 0,
            "failed_direct_tensor_read_operations": 0,
            "failed_direct_destination_read_operations": 0,
            "direct_destination_zeroed_bytes_on_failure": 0,
            "read_retry_limit": 0,
            "read_retry_attempts": 0,
            "read_retry_successes": 0,
            "bulk_transport_reconnects": 0,
            "frame_sink_frames": 0,
            "fallback_frame_iter_frames": read_operations,
            "payload_bytes_read": self.payload_bytes_read,
            "max_observed_cords": 1,
            "last_observed_cords": 1,
            "frame_sink_used_for_all_direct_destination_reads": False,
        }


class PickleableRemoteDatasetClientFactory:
    def __init__(self, local_root: Path, log_path: Path):
        self.local_root = str(local_root)
        self.log_path = str(log_path)

    def __call__(self, name):
        with Path(self.log_path).open("a", encoding="utf-8") as log:
            log.write(f"{os.getpid()} {name}\n")
        return ClosableDatasetClient(RemoteShardTransport(Path(self.local_root) / name))


def no_tls_context(_cafile):
    return None


class ClosableDatasetClient(CxnfsDatasetClient):
    def __init__(self, transport):
        super().__init__(transport)
        self.closed = False
        self.close_count = 0

    def read_tensor(self, *args, **kwargs):
        return CxnfsPythonClient.read_tensor(self, *args, **kwargs)

    def read_into_tensor(self, *args, **kwargs):
        return CxnfsPythonClient.read_into_tensor(self, *args, **kwargs)

    def read_into_buffer(self, *args, **kwargs):
        return CxnfsPythonClient.read_into_buffer(self, *args, **kwargs)

    def write_tensor(self, *args, **kwargs):
        return CxnfsPythonClient.write_tensor(self, *args, **kwargs)

    def close(self):
        self.closed = True
        self.close_count += 1


class TrainerRemoteControl:
    def __init__(self, host, port, options):
        self.host = host
        self.port = port
        self.options = options
        self.authenticated = False
        self.closed = False
        self.close_count = 0

    def authenticate(self, credentials):
        self.authenticated = True
        self.username = credentials.username
        return None

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.close_count += 1


class TrainerRemoteBulkClient:
    def __init__(self, local_root, control, host, port, options):
        self.local_root = local_root
        self.control = control
        self.host = host
        self.port = port
        self.options = options
        self.read_paths = []
        self.buffer_read_paths = []
        self.byte_return_read_operations = 0
        self.direct_buffer_read_operations = 0
        self.direct_tensor_read_operations = 0
        self.payload_bytes_read = 0
        self.write_paths = []
        self.write_bytes = 0
        self.writes_by_path = {}
        self.closed = False
        self.close_count = 0

    def read_mds_shard(self, shard_path, offset=0, length=None):
        self.read_paths.append(shard_path)
        self.byte_return_read_operations += 1
        payload = self._read_payload(shard_path, offset, length)
        self.payload_bytes_read += len(payload)
        return payload

    def _read_payload(self, shard_path, offset=0, length=None):
        staged = self.writes_by_path.get(str(shard_path))
        if staged is not None:
            end = len(staged) if length is None else offset + length
            return staged[offset:end]
        path = Path(str(shard_path))
        payload = (self.local_root / path.parent.name / path.name).read_bytes()
        end = len(payload) if length is None else offset + length
        return payload[offset:end]

    def read_into_buffer(self, shard_path, offset, buffer, nbytes=None):
        self.buffer_read_paths.append(shard_path)
        self.direct_buffer_read_operations += 1
        payload = self._read_payload(shard_path, offset, nbytes)
        self.payload_bytes_read += len(payload)
        view = memoryview(buffer)
        writable = view if view.format == "B" and view.ndim == 1 else view.cast("B")
        writable[:len(payload)] = payload
        return len(payload)

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        self.buffer_read_paths.append(shard_path)
        self.direct_tensor_read_operations += 1
        payload = self._read_payload(shard_path, offset, nbytes)
        self.payload_bytes_read += len(payload)
        destination = getattr(tensor, "data", tensor)
        view = memoryview(destination)
        writable = view if view.format == "B" and view.ndim == 1 else view.cast("B")
        writable[:len(payload)] = payload
        return len(payload)

    def send_safetensors_output(self, operation):
        return SendResult(0, 0, 0)

    def write_tensor(self, path, tensor, nbytes):
        payload = bytes(memoryview(tensor)[:nbytes])
        self.write_paths.append(str(path))
        self.write_bytes += len(payload)
        self.writes_by_path[str(path)] = payload
        return SendResult(1, 1, len(payload))

    def read_path_evidence(self):
        read_operations = (
            self.byte_return_read_operations
            + self.direct_buffer_read_operations
            + self.direct_tensor_read_operations
        )
        return {
            "bulk_transport_class": "TrainerRemoteBulkClient",
            "transport_frame_sink_supported": False,
            "read_operations": read_operations,
            "byte_return_read_operations": self.byte_return_read_operations,
            "direct_buffer_read_operations": self.direct_buffer_read_operations,
            "direct_tensor_read_operations": self.direct_tensor_read_operations,
            "direct_destination_read_operations": self.direct_buffer_read_operations + self.direct_tensor_read_operations,
            "frame_sink_read_operations": 0,
            "direct_destination_frame_sink_read_operations": 0,
            "fallback_frame_iter_read_operations": read_operations,
            "frame_sink_frames": 0,
            "fallback_frame_iter_frames": read_operations,
            "payload_bytes_read": self.payload_bytes_read,
            "max_observed_cords": self.options["read_config"].initial_cords,
            "last_observed_cords": self.options["read_config"].initial_cords,
            "frame_sink_used_for_all_direct_destination_reads": False,
        }

    def write_path_evidence(self):
        retry_config = self.options.get("retry_config") or CxnfsBulkRetryConfig()
        return {
            "format": "cxnfs.bulk-write-path-evidence.v1",
            "write_operations": len(self.write_paths),
            "direct_tensor_write_operations": len(self.write_paths),
            "directory_output_write_operations": 0,
            "payload_bytes_written": self.write_bytes,
            "write_frames": len(self.write_paths),
            "write_acks": len(self.write_paths),
            "write_ack_frame_verified": len(self.write_paths),
            "write_ack_frame_identity_verified": bool(self.write_paths),
            "write_ack_metadata_verified": 0,
            "write_ack_metadata_pending": len(self.write_paths),
            "write_ack_memory": len(self.write_paths),
            "write_ack_spill": 0,
            "metadata_after_payload_entries": len(self.write_paths),
            "entries_committed": len(self.write_paths),
            "transfers_committed": len(self.write_paths),
            "last_accepted_initial_cords": self.options["read_config"].initial_cords,
            "last_accepted_max_cords": self.options["read_config"].max_cords,
            "max_observed_write_cords": self.options["read_config"].initial_cords,
            "write_retry_limit": retry_config.write_retries,
            "write_retry_attempts": 0,
            "write_retry_successes": 0,
            "bulk_transport_write_reconnects": 0,
        }

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.close_count += 1
        if self.options.get("close_control_client"):
            self.control.close()


class BlockingTorchDataset(CxnfsTorchDataset):
    def __init__(self, name, values, barrier):
        self.name = name
        self.values = tuple(values)
        self.barrier = barrier
        self.reads = []

    def __len__(self):
        return len(self.values)

    def read_batch(self, indices, max_workers=16):
        self.reads.append(tuple(indices))
        self.barrier.wait(timeout=2.0)
        return tuple(self.values[index] for index in indices)


class SimpleTorchDataset(CxnfsTorchDataset):
    def __init__(self, values):
        self.values = tuple(values)
        self.reads = []

    def __len__(self):
        return len(self.values)

    def read_batch(self, indices, max_workers=16):
        self.reads.append(tuple(indices))
        return tuple(self.values[index] for index in indices)


class PrefetchBlockingTorchDataset(CxnfsTorchDataset):
    def __init__(self, values):
        self.values = tuple(values)
        self.reads = []
        self._second_read_started = threading.Event()

    def __len__(self):
        return len(self.values)

    def read_batch(self, indices, max_workers=16):
        self.reads.append(tuple(indices))
        if tuple(indices) == (1,):
            self._second_read_started.set()
        elif tuple(indices) == (0,) and not self._second_read_started.wait(timeout=2.0):
            raise AssertionError("second prefetched chunk did not start while first chunk was in flight")
        return tuple(self.values[index] for index in indices)


class FailingOnceTorchDataset(CxnfsTorchDataset):
    def __init__(self, values):
        self.values = tuple(values)
        self.reads = []
        self.failures_remaining = 1

    def __len__(self):
        return len(self.values)

    def read_batch(self, indices, max_workers=16):
        self.reads.append(tuple(indices))
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise OSError("transient dataset read failure")
        return tuple(self.values[index] for index in indices)


def proven_quic_read_path_evidence():
    return {
        "all_runs_have_evidence": True,
        "read_retry_limit": 1,
        "read_retry_attempts": 0,
        "read_retry_successes": 0,
        "bulk_transport_reconnects": 0,
        "failed_read_operations": 0,
        "failed_byte_return_read_operations": 0,
        "failed_direct_buffer_read_operations": 0,
        "failed_direct_tensor_read_operations": 0,
        "failed_direct_destination_read_operations": 0,
        "direct_destination_zeroed_bytes_on_failure": 0,
        "bulk_payload_transport_count": 5,
        "direct_destination_reads_observed": True,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_buffer_read_operations": 8,
        "direct_tensor_read_operations": 0,
        "direct_destination_read_operations": 8,
        "direct_destination_frame_sink_read_operations": 8,
        "direct_destination_parallel_frame_sink_read_operations": 8,
        "direct_destination_parallel_frame_sink_frames": 32,
        "direct_destination_max_observed_cords": 64,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "frame_sink_read_operations": 8,
        "max_observed_cords": 64,
        "quic_read_transport_observed": True,
        "quic_read_transport_complete": True,
        "quic_read_transport_evidence_count": 2,
        "quic_read_transport_connection_opened_count": 2,
        "quic_read_transport_connection_attempt_count": 2,
        "quic_read_transport_connection_retry_count": 0,
        "quic_read_transport_successful_read_operations": 8,
        "quic_read_transport_failed_read_operations": 0,
        "quic_read_transport_last_read_stream_count": 64,
        "quic_read_transport_last_attempted_read_stream_count": 64,
        "quic_read_transport_attempted_read_streams": 128,
        "quic_read_transport_read_streams_created": 128,
        "quic_read_transport_read_streams_eof_verified": 128,
        "quic_read_transport_read_stream_completion_report_count": 2,
        "quic_read_transport_read_stream_eof_report_count": 2,
        "quic_read_transport_stream_completion_verified": True,
        "quic_read_transport_stream_eof_verified": True,
        "quic_read_transport_total_attempted_streams": 128,
        "quic_read_transport_total_successful_streams": 128,
        "quic_read_transport_connection_reuse_count": 2,
        "quic_read_transport_max_read_stream_count": 64,
        "quic_read_transport_connection_reuse_observed": True,
        "quic_read_transport_expected_cipher_suite_count": 2,
        "quic_read_transport_all_reports_expected_cipher_suite": True,
        "quic_read_transport_cipher_suite_counts": {"TLS_AES_128_GCM_SHA256": 2},
        "quic_read_transport_tls_version_counts": {"TLSv1.3": 2},
    }


def proven_quic_read_path_reliability():
    return {
        "format": TRAINER_READ_PATH_RELIABILITY_FORMAT,
        "complete": True,
        "missing": [],
        "retry_enabled": True,
        "read_retry_limit": 1,
        "read_retry_attempts": 0,
        "read_retry_successes": 0,
        "bulk_transport_reconnects": 0,
        "failed_read_operations": 0,
        "failed_byte_return_read_operations": 0,
        "failed_direct_buffer_read_operations": 0,
        "failed_direct_tensor_read_operations": 0,
        "failed_direct_destination_read_operations": 0,
        "direct_destination_zeroed_bytes_on_failure": 0,
        "recovered_read_failures": 0,
        "unrecovered_read_failures": 0,
        "retry_attempts_recovered": True,
        "retry_reconnects_observed": True,
        "all_runs_have_evidence": True,
    }


def proven_quic_write_path_evidence(run_count=2, payload_bytes=8192):
    return {
        "format": "cxnfs.workload-write-path-evidence.v1",
        "enabled": True,
        "write_probe_count": run_count,
        "write_probe_cord_sweep_count": 5 if run_count >= 5 else 2,
        "write_probe_cord_labels": ["1", "4", "8", "16", "64"] if run_count >= 5 else ["1", "4"],
        "run_count": run_count,
        "all_runs_have_evidence": True,
        "verified_read_back_count": run_count,
        "all_write_probes_verified": True,
        "write_operations": run_count,
        "direct_tensor_write_operations": run_count,
        "directory_output_write_operations": 0,
        "payload_bytes_written": payload_bytes,
        "write_frames": run_count,
        "write_acks": run_count,
        "write_ack_frame_verified": run_count,
        "write_ack_frame_identity_verified": True,
        "write_ack_metadata_verified": 0,
        "write_ack_metadata_pending": run_count,
        "write_ack_memory": run_count,
        "write_ack_spill": 0,
        "metadata_after_payload_entries": run_count,
        "entries_committed": run_count,
        "transfers_committed": run_count,
        "max_observed_write_cords": 64 if run_count >= 5 else 4,
        "write_retry_limit": 1,
        "write_retry_attempts": 0,
        "write_retry_successes": 0,
        "bulk_transport_write_reconnects": 0,
        "write_retry_attempts_recovered": True,
        "write_retry_reconnects_observed": True,
        "metadata_pending_observed": True,
        "direct_tensor_writes_observed": True,
        "quic_write_transport_evidence_count": run_count,
        "quic_write_transport_connection_reuse_count": run_count,
        "quic_write_transport_successful_write_operations": run_count,
        "quic_write_transport_failed_write_operations": 0,
        "quic_write_transport_attempted_write_streams": run_count,
        "quic_write_transport_write_streams_created": run_count,
        "quic_write_transport_write_ack_streams_eof_verified": run_count,
        "quic_write_transport_max_active_write_streams": 2,
        "quic_write_transport_concurrent_stream_report_count": max(1, run_count - 1),
        "quic_write_transport_concurrent_streams_observed": True,
        "quic_write_transport_write_stream_success_report_count": run_count,
        "quic_write_transport_all_reports_write_streams_succeeded": True,
        "quic_write_transport_write_ack_stream_completion_report_count": run_count,
        "quic_write_transport_write_ack_stream_eof_report_count": run_count,
        "quic_write_transport_all_reports_write_ack_stream_completion_verified": True,
        "quic_write_transport_all_reports_write_ack_stream_eof_verified": True,
        "quic_write_transport_write_acks_received": run_count,
        "quic_write_transport_write_acks_verified": run_count,
        "quic_write_transport_write_ack_verification_failures": 0,
        "quic_write_transport_write_ack_identity_report_count": run_count,
        "quic_write_transport_all_reports_write_ack_frame_identity_verified": True,
        "quic_write_transport_write_ack_metadata_pending": run_count,
        "quic_write_transport_write_ack_metadata_pending_observed": True,
        "quic_write_transport_connection_opened_count": run_count,
        "quic_write_transport_all_reports_present": True,
        "quic_write_transport_all_reports_reused_connection": True,
        "quic_write_transport_expected_cipher_suite_count": run_count,
        "quic_write_transport_all_reports_expected_cipher_suite": True,
        "quic_write_transport_cipher_suite_counts": {"TLS_AES_128_GCM_SHA256": run_count},
        "quic_write_transport_tls_version_counts": {"TLSv1.3": run_count},
        "quic_write_transport_complete": True,
        "complete": True,
    }


def proven_server_evidence(start=1000.0, end=1005.0):
    window = {
        "sample_start_unix_seconds": start,
        "sample_end_unix_seconds": end,
    }
    return {
        "complete": True,
        "missing": [],
        "missing_sample_windows": [],
        "sample_window_complete": True,
        "sample_window": {
            "complete": True,
            "missing": [],
            "sample_start_unix_seconds": start,
            "sample_end_unix_seconds": end,
            "reports": {
                "disk": dict(window),
                "cache": dict(window),
                "cpu": dict(window),
            },
        },
        "reports": {
            "disk": {"fields": {"disk_busy_percent": 96, **window}},
            "cache": {"fields": {"cache_effective": False, **window}},
            "cpu": {"fields": {"cpu_idle_percent": 35, **window}},
        },
    }


def proven_remote_client_topology(expected_source_count=2, shared_client=False):
    names = ["chat", "summary"][:expected_source_count]
    if shared_client:
        connection_names = names[:1]
        open_names = names[:1]
        active_names = names[:1]
        reuse_count = max(0, len(names) - 1)
    else:
        connection_names = list(names)
        open_names = list(names)
        active_names = list(names)
        reuse_count = 0
    connection_source_set_matches_requests = set(connection_names) == set(names)
    client_open_source_set_matches_requests = set(open_names) == set(names)
    client_close_source_set_matches_opens = False
    return {
        "format": "cxnfs.trainer-remote-client-topology.v1",
        "remote_client_required": True,
        "expected_source_count": expected_source_count,
        "shared_client": shared_client,
        "source_request_count": len(names),
        "unique_source_request_count": len(names),
        "source_request_names": list(names),
        "unique_source_request_names": list(names),
        "connection_count": len(connection_names),
        "connection_source_names": list(connection_names),
        "unique_connection_source_names": list(connection_names),
        "client_open_count": len(open_names),
        "client_open_source_names": list(open_names),
        "unique_client_open_source_names": list(open_names),
        "client_close_count": 0,
        "client_close_source_names": [],
        "unique_client_close_source_names": [],
        "active_client_count": len(active_names),
        "active_client_source_names": list(active_names),
        "unique_active_client_source_names": list(active_names),
        "shared_client_reuse_count": reuse_count,
        "factory_close_count": 0,
        "all_open_clients_closed": False,
        "connection_source_set_matches_requests": connection_source_set_matches_requests,
        "client_open_source_set_matches_requests": client_open_source_set_matches_requests,
        "client_close_source_set_matches_opens": client_close_source_set_matches_opens,
        "client_lifecycle_source_set_complete": client_close_source_set_matches_opens,
        "per_source_client_observed": not shared_client,
        "shared_client_observed": shared_client,
        "complete": True,
    }


def proven_remote_client_topology_aggregate(report_count=2, expected_source_count=2):
    source_names = ["chat", "summary"][:expected_source_count]
    return {
        "format": "cxnfs.trainer-remote-client-topology-aggregate.v1",
        "remote_client_required": True,
        "expected_source_count": expected_source_count,
        "report_count": report_count,
        "topology_report_count": report_count,
        "all_reports_have_topology": True,
        "all_topologies_complete": True,
        "per_source_client_observed_count": report_count,
        "shared_client_observed_count": 0,
        "connection_source_set_matches_requests_count": report_count,
        "client_open_source_set_matches_requests_count": report_count,
        "client_lifecycle_source_set_complete_count": 0,
        "all_connection_source_sets_match_requests": True,
        "all_client_open_source_sets_match_requests": True,
        "all_client_lifecycle_source_sets_complete": False,
        "minimum_unique_source_request_count": expected_source_count,
        "minimum_connection_count": expected_source_count,
        "source_request_count": report_count * expected_source_count,
        "connection_count": report_count * expected_source_count,
        "client_open_count": report_count * expected_source_count,
        "client_close_count": 0,
        "active_client_count": report_count * expected_source_count,
        "shared_client_reuse_count": 0,
        "unique_source_request_names": source_names,
        "unique_connection_source_names": source_names,
        "unique_client_open_source_names": source_names,
        "unique_client_close_source_names": [],
        "unique_active_client_source_names": source_names,
        "client_lifecycle_observed_count": report_count,
        "all_open_clients_closed_count": 0,
        "all_open_clients_closed": False,
        "per_source_client_complete": True,
        "shared_client_complete": False,
        "complete": True,
    }


def proven_batch_direct_read_report():
    return {
        "format": "cxnfs.quic-batch-direct-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "batch_read_count": 2,
        "batch_worker_count": 2,
        "ordered_batch_read_verified": True,
        "direct_buffer_read_operations": 2,
        "direct_destination_read_operations": 2,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "max_observed_cords": 2,
        "direct_destination_max_observed_cords": 2,
    }


def proven_batch_direct_tensor_read_report():
    return {
        "format": "cxnfs.quic-batch-direct-tensor-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "tensor_target": "pytorch-cpu",
        "tensor_read_count": 2,
        "tensor_worker_count": 2,
        "ordered_tensor_read_verified": True,
        "direct_tensor_read_operations": 2,
        "network_fills_tensor_destination": True,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "max_observed_cords": 2,
        "direct_destination_max_observed_cords": 2,
    }


def build_proven_remote_quic_handoff(contract, **kwargs):
    return build_trainer_handoff_payload(
        contract,
        batch_direct_read_payload=proven_batch_direct_read_report(),
        batch_direct_tensor_read_payload=proven_batch_direct_tensor_read_report(),
        require_low_level_direct_read_proofs=True,
        **kwargs,
    )


def proven_quic_workload_payload():
    plan = {
        "format": "cxnfs.weighted-mds-plan.v1",
        "source_count": 2,
        "total_weight": 3,
        "total_samples": 3,
        "total_bytes": 21,
        "source_schedule": ["chat", "chat", "summary"],
        "sources": [
            {"name": "chat", "weight": 2, "sample_count": 2, "shard_count": 1, "byte_size": 14, "shards": ["chat-000000.mds"]},
            {"name": "summary", "weight": 1, "sample_count": 1, "shard_count": 1, "byte_size": 7, "shards": ["summary-000000.mds"]},
        ],
    }
    plan["workload_fingerprint"] = weighted_mds_plan_fingerprint(plan)
    return {
        "format": "cxnfs.trainer-workload-benchmark.v1",
        "workload_name": "quic-contract",
        "root": "/var/cache/cxnfs/indexes/stage0-mds",
        "weights": {"chat": 2, "summary": 1},
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "read_mode": "direct-buffers",
        "trainer_batch_iterator": "native",
        "data_loader_workers": 0,
        "bulk_read_retries": 1,
        "bulk_write_retries": 1,
        "remote_root": "/stage0-mds",
        "weighted_mds_plan": plan,
        "workload_fingerprint": plan["workload_fingerprint"],
        "network_path_summary": {"cord_sweep_count": 2},
        "cord_sweep": [
            {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
            {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
        ],
        "runs_requested": 1,
        "run_count": 2,
        "sample_count": 8,
        "byte_size": 128,
        "contract_applied": True,
        "benchmark_provenance": {
            "format": "cxnfs.trainer-workload-provenance.v1",
            "invocation": {
                "root": "/var/cache/cxnfs/indexes/stage0-mds",
                "weights": {"chat": 2, "summary": 1},
                "workload_name": "quic-contract",
                "remote_root": "/stage0-mds",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_batch_iterator": "native",
                "data_loader_workers": 0,
                "bulk_read_retries": 1,
                "bulk_write_retries": 1,
                "runs": 1,
                "cord_sweep": [
                    {"initial_cords": 1, "max_cords": 1},
                    {"initial_cords": 4, "max_cords": 4},
                ],
            },
            "invocation_fingerprint": {"algorithm": "sha256", "sha256": "1" * 64},
        },
        "read_path_evidence": proven_quic_read_path_evidence(),
        "read_path_reliability": proven_quic_read_path_reliability(),
        "remote_client_topology": proven_remote_client_topology_aggregate(report_count=2),
        "write_probe": {
            "enabled": True,
            "byte_size": 4096,
            "remote_root": "/trainer-write-probes",
        },
        "write_path_evidence": proven_quic_write_path_evidence(),
        "blockfs_gate": {
            "decision": "profile-storage",
            "kernel_module_allowed": False,
            "required_next_evidence": ["server disk evidence", "server cache evidence", "server cpu evidence"],
            "read_path_evidence_complete": True,
            "read_path_gate": {"complete": True, "missing": []},
            "read_path_reliability_complete": True,
            "read_path_reliability": proven_quic_read_path_reliability(),
            "memory_path_evidence_complete": True,
            "memory_path_gate": {
                "complete": True,
                "missing": [],
                "read_mode": "direct-buffers",
                "tensor_allocation_declared": False,
                "tensor_allocation_target": None,
                "tensor_allocation_dtype": None,
                "tensor_allocation_device": None,
                "direct_buffer_read_operations": 8,
                "direct_tensor_read_operations": 0,
                "direct_destination_read_operations": 8,
                "direct_destination_reads_observed": True,
            },
            "write_path_evidence_complete": True,
            "write_path_gate": {"complete": True, "missing": []},
            "workload_scale_complete": True,
            "workload_scale_gate": {
                "complete": True,
                "missing": [],
                "requirements": {"min_runs": 2, "min_samples": 8, "min_bytes": 128, "min_cord_sweep": 2},
                "observed": {"run_count": 2, "sample_count": 8, "byte_size": 128, "cord_sweep_count": 2},
            },
            "server_evidence_complete": False,
        },
    }


def proven_surface_matrix_payload(contract):
    plan = contract["weighted_mds_plan"]
    fingerprint = contract["workload_fingerprint"]
    cases = []
    for read_mode, iterator in itertools.product(
        ("direct-buffers", "direct-tensors"),
        ("native", "trainer-dataloader"),
    ):
        is_tensor = read_mode == "direct-tensors"
        read_path_evidence = proven_quic_read_path_evidence()
        read_path_evidence.update({
            "bulk_payload_transport_count": 2,
            "direct_buffer_read_operations": 0 if is_tensor else 3,
            "direct_tensor_read_operations": 3 if is_tensor else 0,
            "direct_destination_read_operations": 3,
            "direct_destination_frame_sink_read_operations": 3,
            "frame_sink_read_operations": 3,
            "max_observed_cords": 2,
            "uses_caller_owned_destinations": True,
            "quic_read_transport_evidence_count": 2,
            "quic_read_transport_successful_read_operations": 3,
            "quic_read_transport_attempted_read_streams": 6,
            "quic_read_transport_read_streams_created": 6,
            "quic_read_transport_read_streams_eof_verified": 6,
            "quic_read_transport_total_attempted_streams": 6,
            "quic_read_transport_total_successful_streams": 6,
            "quic_read_transport_connection_reuse_count": 2,
            "quic_read_transport_max_read_stream_count": 2,
        })
        cases.append({
            "case_name": f"{read_mode}/{iterator}",
            "status": "passed",
            "passed": True,
            "read_mode": read_mode,
            "destination_type": "torch-uint8-tensor" if is_tensor else "writable-byte-buffer",
            "trainer_batch_iterator": iterator,
            "data_loader_workers": 0,
            "tensor_allocation": (
                CxnfsTrainerTensorAllocation(target="pytorch-cpu").to_dict()
                if is_tensor
                else None
            ),
            "batch_count": 1,
            "sample_count": 3,
            "byte_size": 21,
            "source_counts": {"chat": 2, "summary": 1},
            "read_path_evidence": read_path_evidence,
            "read_path_reliability": proven_quic_read_path_reliability(),
            "remote_client_topology": proven_remote_client_topology(),
            "memory_path_gate": {
                "complete": True,
                "missing": [],
                "read_mode": read_mode,
                "direct_buffer_read_operations": 0 if is_tensor else 3,
                "direct_tensor_read_operations": 3 if is_tensor else 0,
                "direct_destination_read_operations": 3,
                "direct_destination_reads_observed": True,
                "tensor_allocation_declared": is_tensor,
                "tensor_allocation_target": "pytorch-cpu" if is_tensor else None,
                "tensor_allocation_dtype": "uint8" if is_tensor else None,
                "tensor_allocation_device": "cpu" if is_tensor else None,
            },
            "objective_passed": None,
            "error_type": None,
            "error": None,
        })
    return {
        "format": TRAINER_SURFACE_MATRIX_FORMAT,
        "root": contract["root"],
        "remote_root": contract.get("remote_root"),
        "weights": contract["weights"],
        "weighted_mds_plan": plan,
        "workload_fingerprint": fingerprint,
        "transport": contract["network_contract"]["transport"],
        "bulk_transport": contract["network_contract"]["bulk_transport"],
        "requires_mount": False,
        "touches_trainer_code": False,
        "case_count": len(cases),
        "passed_case_count": len(cases),
        "failed_case_count": 0,
        "passed": True,
        "status": "passed",
        "read_modes": [case["read_mode"] for case in cases],
        "trainer_batch_iterators": [case["trainer_batch_iterator"] for case in cases],
        "batch_size": 3,
        "requested_batches": 1,
        "warmup_batches": 0,
        "rounds_per_read": 2,
        "source_read_workers": 2,
        "prefetch_chunks": 2,
        "data_loader_workers": 0,
        "tensor_allocation": CxnfsTrainerTensorAllocation(target="pytorch-cpu").to_dict(),
        "objective": None,
        "objective_passed_all": None,
        "read_path_reliability_complete": True,
        "remote_client_topology": proven_remote_client_topology_aggregate(report_count=len(cases)),
        "remote_client_topology_complete": True,
        "remote_client_per_source_complete": True,
        "unrecovered_read_failures": 0,
        "read_retry_successes": 0,
        "bulk_transport_reconnects": 0,
        "cases": cases,
    }


def align_workload_with_contract(workload, contract):
    workload["root"] = contract["root"]
    workload["remote_root"] = contract.get("remote_root")
    workload["weights"] = contract["weights"]
    invocation = workload["benchmark_provenance"]["invocation"]
    invocation["root"] = contract["root"]
    invocation["remote_root"] = contract.get("remote_root")
    invocation["weights"] = contract["weights"]
    network = contract.get("network_contract")
    if isinstance(network, dict):
        if "bulk_read_retries" in network:
            workload["bulk_read_retries"] = network["bulk_read_retries"]
            invocation["bulk_read_retries"] = network["bulk_read_retries"]
        if "bulk_write_retries" in network:
            workload["bulk_write_retries"] = network["bulk_write_retries"]
            invocation["bulk_write_retries"] = network["bulk_write_retries"]


if __name__ == "__main__":
    unittest.main()
