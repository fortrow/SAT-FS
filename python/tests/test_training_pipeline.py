from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import threading
import unittest
from unittest.mock import patch

import cxnfs.pipeline as pipeline_module
from cxnfs import (
    AugmentationPlacement,
    CxnfsPipelineConfig,
    CxnfsPipelinePrefetchedTensorBatch,
    CxnfsPipelineTensorPrefetchStream,
    CxnfsPythonClient,
    CxnfsTrainerPipelineBatchSample,
    CxnfsTrainingPipeline,
    LocalFileTransport,
    SampleRead,
    TorchCompilePolicy,
)


class CxnfsTrainingPipelineTest(unittest.TestCase):
    def test_reads_samples_with_threaded_pipeline_in_request_order(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"aaabbbccc")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=2))

            samples = pipeline.read_samples((
                SampleRead("dataset/shard.00000.mds", 3, 3, "b"),
                SampleRead("dataset/shard.00000.mds", 0, 3, "a"),
            ))

            self.assertEqual((b"bbb", b"aaa"), samples)

    def test_readIntoBuffersFillsCallerOwnedDestinationsInRequestOrder(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"aaabbbccc")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=2))
            first = bytearray(3)
            second = bytearray(3)

            counts = pipeline.read_into_buffers(
                (
                    SampleRead("dataset/shard.00000.mds", 3, 3, "b"),
                    SampleRead("dataset/shard.00000.mds", 0, 3, "a"),
                ),
                (first, second),
            )

            self.assertEqual((3, 3), counts)
            self.assertEqual(b"bbb", bytes(first))
            self.assertEqual(b"aaa", bytes(second))

    def test_readIntoTensorsUsesCallerAllocatedCpuDestinations(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"aaabbb")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=2))
            first = bytearray(3)
            second = bytearray(3)

            counts = pipeline.read_into_tensors(
                (
                    ("dataset/shard.00000.mds", 0, 3),
                    ("dataset/shard.00000.mds", 3, 3),
                ),
                (first, second),
            )

            self.assertEqual((3, 3), counts)
            self.assertEqual(b"aaa", bytes(first))
            self.assertEqual(b"bbb", bytes(second))

    def test_readTensorBatchFillsCallerAllocatedDestinationsBeforeGpuTransform(self):
        client = DirectOnlyTensorClient({
            ("dataset/shard.00000.mds", 0, 3): b"aaa",
            ("dataset/shard.00000.mds", 3, 3): b"bbb",
        })
        pipeline = CxnfsTrainingPipeline(
            client,
            CxnfsPipelineConfig(network_read_workers=2),
            transform_compiler=lambda transform, policy: transform,
        )

        def tensor_factory(request):
            return bytearray(request.nbytes)

        def gpu_transform(tensors, device):
            return {
                "device": device,
                "payloads": tuple(bytes(tensor) for tensor in tensors),
            }

        result = pipeline.read_tensor_batch(
            (
                SampleRead("dataset/shard.00000.mds", 0, 3, "a"),
                SampleRead("dataset/shard.00000.mds", 3, 3, "b"),
            ),
            tensor_factory,
            device="cuda:0",
            tensor_transform=gpu_transform,
        )

        self.assertEqual("cuda:0", result["device"])
        self.assertEqual((b"aaa", b"bbb"), result["payloads"])
        self.assertEqual(0, client.byte_read_count)
        self.assertEqual(2, client.tensor_read_count)

    def test_readTensorBatchReturnsFilledDestinationsWithoutTransform(self):
        client = DirectOnlyTensorClient({
            ("dataset/shard.00000.mds", 0, 3): b"aaa",
            ("dataset/shard.00000.mds", 3, 3): b"bbb",
        })
        pipeline = CxnfsTrainingPipeline(
            client,
            CxnfsPipelineConfig(network_read_workers=2),
            transform_compiler=lambda transform, policy: transform,
        )

        tensors = pipeline.read_tensor_batch(
            (
                ("dataset/shard.00000.mds", 0, 3),
                ("dataset/shard.00000.mds", 3, 3),
            ),
            lambda request: bytearray(request.nbytes),
        )

        self.assertEqual((b"aaa", b"bbb"), tuple(bytes(tensor) for tensor in tensors))
        self.assertEqual(0, client.byte_read_count)
        self.assertEqual(2, client.tensor_read_count)

    def test_readTrainerTensorBatchFillsPlannedTrainerDestinations(self):
        client = DirectOnlyTensorClient({
            ("dataset/shard.00000.mds", 0, 3): b"aaa",
            ("dataset/shard.00000.mds", 3, 3): b"bbb",
        })
        pipeline = CxnfsTrainingPipeline(
            client,
            CxnfsPipelineConfig(network_read_workers=2),
            transform_compiler=lambda transform, policy: transform,
        )
        first = bytearray(3)
        second = bytearray(3)
        batch = TrainerBatchStub((
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                0,
                first,
                SampleRead("dataset/shard.00000.mds", 0, 3, "chat:0"),
            ),
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                1,
                second,
                SampleRead("dataset/shard.00000.mds", 3, 3, "chat:1"),
            ),
        ))

        tensors = pipeline.read_trainer_tensor_batch(batch, non_blocking=False)

        self.assertEqual((first, second), tensors)
        self.assertEqual(b"aaa", bytes(first))
        self.assertEqual(b"bbb", bytes(second))
        self.assertEqual(2, client.tensor_read_count)
        self.assertEqual([False, False], [read[-1] for read in client.tensor_reads])

    def test_prefetchTrainerStreamFillsNextTensorBatchBeforeFirstIsConsumed(self):
        client = BlockingPrefetchTensorClient({
            ("dataset/shard.00000.mds", 0, 3): b"aaa",
            ("dataset/shard.00000.mds", 3, 3): b"bbb",
        })
        pipeline = CxnfsTrainingPipeline(
            client,
            CxnfsPipelineConfig(network_read_workers=1, prefetch_batches=2),
            transform_compiler=lambda transform, policy: transform,
        )
        first_destination = bytearray(3)
        second_destination = bytearray(3)
        first_batch = TrainerBatchStub((
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                0,
                first_destination,
                SampleRead("dataset/shard.00000.mds", 0, 3, "chat:0"),
            ),
        ))
        second_batch = TrainerBatchStub((
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                1,
                second_destination,
                SampleRead("dataset/shard.00000.mds", 3, 3, "chat:1"),
            ),
        ))
        stream = AcknowledgeableTrainerStreamStub((first_batch, second_batch))

        with pipeline.prefetch_trainer_stream(stream, max_prefetch_batches=2, non_blocking=False) as prefetched:
            self.assertIsInstance(prefetched, CxnfsPipelineTensorPrefetchStream)
            self.assertTrue(client.second_started.wait(timeout=2.0))
            self.assertEqual([], stream.acknowledged)

            first = next(prefetched)
            self.assertIsInstance(first, CxnfsPipelinePrefetchedTensorBatch)
            self.assertIs(first.batch, first_batch)
            self.assertEqual((first_destination,), first.tensors)
            self.assertEqual(b"aaa", bytes(first_destination))
            self.assertEqual([first_batch], stream.acknowledged)

            client.release_second.set()
            second = next(prefetched)
            self.assertIs(second.batch, second_batch)
            self.assertEqual((second_destination,), second.tensors)
            self.assertEqual(b"bbb", bytes(second_destination))
            self.assertEqual([first_batch, second_batch], stream.acknowledged)

            with self.assertRaises(StopIteration):
                next(prefetched)

        self.assertEqual([False, False], [read[-1] for read in client.tensor_reads])
        self.assertEqual([False, False, False], stream.update_cursor_args)
        self.assertFalse(stream.discarded)

    def test_prefetchTrainerStreamDiscardsUnacknowledgedBatchOnEarlyClose(self):
        client = DirectOnlyTensorClient({
            ("dataset/shard.00000.mds", 0, 3): b"aaa",
            ("dataset/shard.00000.mds", 3, 3): b"bbb",
        })
        pipeline = CxnfsTrainingPipeline(
            client,
            CxnfsPipelineConfig(network_read_workers=1, prefetch_batches=2),
            transform_compiler=lambda transform, policy: transform,
        )
        first_destination = bytearray(3)
        second_destination = bytearray(3)
        first_batch = TrainerBatchStub((
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                0,
                first_destination,
                SampleRead("dataset/shard.00000.mds", 0, 3, "chat:0"),
            ),
        ), batch_index=0)
        second_batch = TrainerBatchStub((
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                1,
                second_destination,
                SampleRead("dataset/shard.00000.mds", 3, 3, "chat:1"),
            ),
        ), batch_index=1)
        stream = AcknowledgeableTrainerStreamStub((first_batch, second_batch))

        prefetched = pipeline.prefetch_trainer_stream(stream, max_prefetch_batches=2, non_blocking=False)
        first = next(prefetched)
        self.assertEqual(0, first.batch_index)
        self.assertEqual(1, first.sample_count)
        self.assertEqual(3, first.byte_size)
        self.assertEqual([first_batch], stream.acknowledged)

        prefetched.close()

        self.assertTrue(prefetched.closed)
        self.assertTrue(stream.discarded)
        self.assertEqual([first_batch], stream.acknowledged)

    def test_prefetchTrainerTensorBatchesRaisesProducerFailureWithoutAcknowledgement(self):
        pipeline = CxnfsTrainingPipeline(
            ShortReadClient(),
            CxnfsPipelineConfig(network_read_workers=1, prefetch_batches=1),
        )
        destination = bytearray(4)
        batch = TrainerBatchStub((
            CxnfsTrainerPipelineBatchSample(
                "chat",
                0,
                0,
                destination,
                SampleRead("dataset/shard.00000.mds", 0, 4, "chat:0"),
            ),
        ))
        acknowledged = []

        with self.assertRaisesRegex(ValueError, "short CXNFS destination read"):
            with pipeline.prefetch_trainer_tensor_batches(
                (batch,),
                max_prefetch_batches=1,
                acknowledge=acknowledged.append,
            ) as prefetched:
                next(prefetched)

        self.assertEqual([], acknowledged)
        self.assertEqual(bytearray(b"\x00" * 4), destination)

    def test_readTensorBatchUsesDefaultTorchCompileAndCachesCompiledTransform(self):
        client = DirectOnlyTensorClient({
            ("dataset/shard.00000.mds", 0, 3): b"aaa",
            ("dataset/shard.00000.mds", 3, 3): b"bbb",
        })
        compile_calls = []

        class FakeTorch:
            @staticmethod
            def compile(transform, *, mode, fullgraph, dynamic):
                compile_calls.append({
                    "mode": mode,
                    "fullgraph": fullgraph,
                    "dynamic": dynamic,
                })

                def compiled(tensors, device):
                    return {
                        "compiled": True,
                        "payloads": transform(tensors, device),
                    }

                return compiled

        def gpu_transform(tensors, device):
            return tuple(bytes(tensor).upper() for tensor in tensors)

        with patch.dict(sys.modules, {"torch": FakeTorch()}):
            pipeline = CxnfsTrainingPipeline(
                client,
                CxnfsPipelineConfig(
                    network_read_workers=1,
                    torch_compile=TorchCompilePolicy(
                        enabled=True,
                        mode="reduce-overhead",
                        fullgraph=True,
                        dynamic=False,
                    ),
                ),
            )
            first = pipeline.read_tensor_batch(
                (("dataset/shard.00000.mds", 0, 3),),
                lambda request: bytearray(request.nbytes),
                tensor_transform=gpu_transform,
            )
            second = pipeline.read_tensor_batch(
                (("dataset/shard.00000.mds", 3, 3),),
                lambda request: bytearray(request.nbytes),
                tensor_transform=gpu_transform,
            )

        self.assertTrue(first["compiled"])
        self.assertEqual((b"AAA",), first["payloads"])
        self.assertEqual((b"BBB",), second["payloads"])
        self.assertEqual([{
            "mode": "reduce-overhead",
            "fullgraph": True,
            "dynamic": False,
        }], compile_calls)

    def test_readTensorBatchRequiresGpuPlacementForTransform(self):
        client = DirectOnlyTensorClient({("dataset/shard.00000.mds", 0, 3): b"aaa"})
        pipeline = CxnfsTrainingPipeline(
            client,
            CxnfsPipelineConfig(
                network_read_workers=1,
                augmentation_placement=AugmentationPlacement.DISABLED,
            ),
        )

        with self.assertRaisesRegex(ValueError, "tensor_transform requires GPU"):
            pipeline.read_tensor_batch(
                (("dataset/shard.00000.mds", 0, 3),),
                lambda request: bytearray(request.nbytes),
                device="cpu",
                tensor_transform=lambda tensors, device: tensors,
            )

    def test_reusesNetworkExecutorAndClosesItExplicitly(self):
        RecordingThreadPoolExecutor.instances.clear()
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"aaabbb")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=2))

            with patch.object(pipeline_module, "ThreadPoolExecutor", RecordingThreadPoolExecutor):
                self.assertEqual((b"aaa",), pipeline.read_samples((("dataset/shard.00000.mds", 0, 3),)))
                self.assertEqual((b"bbb",), pipeline.read_samples((("dataset/shard.00000.mds", 3, 3),)))
                self.assertEqual(1, len(RecordingThreadPoolExecutor.instances))
                self.assertEqual(2, RecordingThreadPoolExecutor.instances[0].max_workers)
                self.assertFalse(RecordingThreadPoolExecutor.instances[0].shutdown_called)

                pipeline.close()

                self.assertTrue(pipeline.closed)
                self.assertTrue(RecordingThreadPoolExecutor.instances[0].shutdown_called)
                with self.assertRaisesRegex(ValueError, "closed"):
                    pipeline.read_samples((("dataset/shard.00000.mds", 0, 3),))

    def test_contextManagerCanCloseOwnedClient(self):
        client = ClosableReadClient({("dataset/shard.00000.mds", 0, 3): b"abc"})

        with CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=1), close_client=True) as pipeline:
            self.assertEqual((b"abc",), pipeline.read_samples((("dataset/shard.00000.mds", 0, 3),)))

        self.assertTrue(pipeline.closed)
        self.assertTrue(client.closed)
        self.assertEqual(1, client.close_count)

    def test_gpu_transform_is_compiled_once_and_reused(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abcdef")
            client = CxnfsPythonClient(LocalFileTransport(root))
            compile_calls = []

            def compiler(transform, policy):
                compile_calls.append(policy)

                def compiled(samples, device):
                    return {"samples": transform(samples, device), "device": device, "compiled": True}

                return compiled

            def gpu_transform(samples, device):
                return tuple(sample.upper() for sample in samples)

            pipeline = CxnfsTrainingPipeline(
                client,
                CxnfsPipelineConfig(network_read_workers=2),
                transform_compiler=compiler,
            )

            first = pipeline.read_batch((("dataset/shard.00000.mds", 0, 3),), gpu_transform=gpu_transform)
            second = pipeline.read_batch((("dataset/shard.00000.mds", 3, 3),), gpu_transform=gpu_transform)

            self.assertTrue(first["compiled"])
            self.assertEqual((b"ABC",), first["samples"])
            self.assertEqual((b"DEF",), second["samples"])
            self.assertEqual(1, len(compile_calls))
            self.assertEqual("reduce-overhead", compile_calls[0].mode)

    def test_gpu_transform_runsUncompiledWhenTorchCompileDisabled(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abc")
            client = CxnfsPythonClient(LocalFileTransport(root))
            compile_calls = []

            def compiler(transform, policy):
                compile_calls.append(policy)
                return transform

            pipeline = CxnfsTrainingPipeline(
                client,
                CxnfsPipelineConfig(torch_compile=TorchCompilePolicy(enabled=False)),
                transform_compiler=compiler,
            )

            result = pipeline.read_batch(
                (("dataset/shard.00000.mds", 0, 3),),
                gpu_transform=lambda samples, device: tuple(sample.upper() for sample in samples),
            )

            self.assertEqual((b"ABC",), result)
            self.assertEqual([], compile_calls)

    def test_cpuAugmentationOnCudaRequiresExplicitOptIn(self):
        with self.assertRaisesRegex(ValueError, "CPU augmentations"):
            CxnfsPipelineConfig(augmentation_placement=AugmentationPlacement.CPU_THREAD_POOL)

    def test_cpuTransformCanBeExplicitlyAllowedForNonHotPath(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abc")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(
                client,
                CxnfsPipelineConfig(
                    augmentation_placement=AugmentationPlacement.CPU_THREAD_POOL,
                    allow_cpu_augmentations=True,
                ),
            )

            result = pipeline.read_batch(
                (("dataset/shard.00000.mds", 0, 3),),
                device="cpu",
                cpu_transform=lambda sample: sample.upper(),
            )

            self.assertEqual((b"ABC",), result)

    def test_cpuThreadPoolAugmentationUsesReusableExplicitWorkerCount(self):
        RecordingThreadPoolExecutor.instances.clear()
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abcdef")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(
                client,
                CxnfsPipelineConfig(
                    network_read_workers=3,
                    max_cords=3,
                    augmentation_placement=AugmentationPlacement.CPU_THREAD_POOL,
                    allow_cpu_augmentations=True,
                    cpu_augmentation_workers=2,
                ),
            )

            with patch.object(pipeline_module, "ThreadPoolExecutor", RecordingThreadPoolExecutor):
                first = pipeline.read_batch(
                    (("dataset/shard.00000.mds", 0, 3),),
                    device="cpu",
                    cpu_transform=lambda sample: sample.upper(),
                )
                second = pipeline.read_batch(
                    (("dataset/shard.00000.mds", 3, 3),),
                    device="cpu",
                    cpu_transform=lambda sample: sample.upper(),
                )
                pipeline.close()

            self.assertEqual((b"ABC",), first)
            self.assertEqual((b"DEF",), second)
            self.assertEqual([3, 2], [executor.max_workers for executor in RecordingThreadPoolExecutor.instances])
            self.assertTrue(all(executor.shutdown_called for executor in RecordingThreadPoolExecutor.instances))

    def test_cpuProcessPoolTransformCanBeExplicitlyAllowed(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "dataset/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abcdef")
            client = CxnfsPythonClient(LocalFileTransport(root))
            pipeline = CxnfsTrainingPipeline(
                client,
                CxnfsPipelineConfig(
                    network_read_workers=2,
                    augmentation_placement=AugmentationPlacement.CPU_PROCESS_POOL,
                    allow_cpu_augmentations=True,
                    cpu_augmentation_workers=2,
                ),
            )
            FakeProcessPoolExecutor.max_workers_seen.clear()

            with patch.object(pipeline_module, "ProcessPoolExecutor", FakeProcessPoolExecutor):
                result = pipeline.read_batch(
                    (
                        ("dataset/shard.00000.mds", 0, 3),
                        ("dataset/shard.00000.mds", 3, 3),
                    ),
                    device="cpu",
                    cpu_transform=uppercase_sample,
                )

            self.assertEqual((b"ABC", b"DEF"), result)
            self.assertEqual([2], FakeProcessPoolExecutor.max_workers_seen)

    def test_rejectsInvalidCpuAugmentationWorkers(self):
        with self.assertRaisesRegex(ValueError, "cpu_augmentation_workers"):
            CxnfsPipelineConfig(cpu_augmentation_workers=0)

    def test_rejectsBatchesThatExceedInflightBudget(self):
        with TemporaryDirectory() as directory:
            client = CxnfsPythonClient(LocalFileTransport(directory))
            pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(max_inflight_bytes=4))

            with self.assertRaisesRegex(ValueError, "max_inflight_bytes"):
                pipeline.read_samples((("missing.mds", 0, 5),))

    def test_rejectsShortNetworkReadBeforeTransform(self):
        client = ShortReadClient()
        pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=1))

        with self.assertRaisesRegex(ValueError, "short CXNFS sample read"):
            pipeline.read_batch(
                (SampleRead("dataset/shard.00000.mds", 0, 4, "sample-0"),),
                gpu_transform=lambda samples, device: samples,
            )

    def test_rejectsShortDestinationReadBeforePublishToTrainer(self):
        client = ShortReadClient()
        pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=1))

        with self.assertRaisesRegex(ValueError, "short CXNFS destination read"):
            pipeline.read_into_buffers(
                (SampleRead("dataset/shard.00000.mds", 0, 4, "sample-0"),),
                (bytearray(4),),
            )

    def test_readIntoBuffersWaitsForRunningSiblingBeforeClearingFailedBatch(self):
        self._assert_failed_direct_batch_waits_and_clears("read_into_buffers")

    def test_readIntoTensorsWaitsForRunningSiblingBeforeClearingFailedBatch(self):
        self._assert_failed_direct_batch_waits_and_clears("read_into_tensors")

    def _assert_failed_direct_batch_waits_and_clears(self, method_name):
        client = CoordinatedFailingDestinationClient()
        pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=2))
        first = bytearray(b"aaaa")
        second = bytearray(b"bbbb")
        errors = []

        def run_read():
            try:
                getattr(pipeline, method_name)(
                    (
                        SampleRead("dataset/shard.00000.mds", 0, 4, "failing"),
                        SampleRead("dataset/shard.00000.mds", 4, 4, "blocked"),
                    ),
                    (first, second),
                )
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run_read)
        thread.start()
        try:
            self.assertTrue(client.second_started.wait(timeout=2.0))
            self.assertTrue(client.failure_raised.wait(timeout=2.0))
            thread.join(timeout=0.1)
            self.assertTrue(thread.is_alive())
            self.assertFalse(client.second_finished.is_set())

            client.release_second.set()
            thread.join(timeout=2.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(1, len(errors))
            self.assertIsInstance(errors[0], OSError)
            self.assertIn("network failure", str(errors[0]))
            self.assertTrue(client.second_finished.is_set())
            self.assertEqual(bytearray(b"\x00" * 4), first)
            self.assertEqual(bytearray(b"\x00" * 4), second)
        finally:
            client.release_second.set()
            thread.join(timeout=2.0)

    def test_destinationCountMustMatchReadCount(self):
        client = CxnfsPythonClient(LocalFileTransport("."))
        pipeline = CxnfsTrainingPipeline(client, CxnfsPipelineConfig(network_read_workers=1))

        with self.assertRaisesRegex(ValueError, "destination count"):
            pipeline.read_into_buffers((("dataset/shard.00000.mds", 0, 4),), ())


def uppercase_sample(sample):
    return sample.upper()


class FakeProcessPoolExecutor:
    max_workers_seen = []

    def __init__(self, max_workers):
        self.max_workers_seen.append(max_workers)
        self.shutdown_called = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return None

    def submit(self, transform, sample):
        return ImmediateFuture(transform(sample))

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_called = True


class RecordingThreadPoolExecutor:
    instances = []

    def __init__(self, max_workers, **kwargs):
        self.max_workers = max_workers
        self.kwargs = kwargs
        self.shutdown_called = False
        self.submitted = []
        self.instances.append(self)

    def submit(self, fn, *args, **kwargs):
        self.submitted.append((fn, args, kwargs))
        return ImmediateFuture(fn(*args, **kwargs))

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_called = True
        self.shutdown_wait = wait
        self.shutdown_cancel_futures = cancel_futures


class ImmediateFuture:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error
        self.cancelled = False

    def result(self):
        if self.error is not None:
            raise self.error
        return self.value

    def cancel(self):
        self.cancelled = True


class ClosableReadClient:
    def __init__(self, payloads):
        self.payloads = dict(payloads)
        self.closed = False
        self.close_count = 0

    def read_mds_shard(self, shard_path, offset=0, length=None):
        return self.payloads[(shard_path, offset, length)]

    def close(self):
        self.closed = True
        self.close_count += 1


class ShortReadClient:
    def read_mds_shard(self, shard_path, offset=0, length=None):
        return b"x" * ((length or 1) - 1)

    def read_into_buffer(self, shard_path, offset, buffer, nbytes=None):
        count = (nbytes or len(buffer)) - 1
        memoryview(buffer)[:count] = b"x" * count
        return count

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        return self.read_into_buffer(shard_path, offset, tensor, nbytes)


class DirectOnlyTensorClient:
    def __init__(self, payloads):
        self.payloads = dict(payloads)
        self.byte_read_count = 0
        self.tensor_read_count = 0
        self.tensor_reads = []

    def read_mds_shard(self, shard_path, offset=0, length=None):
        self.byte_read_count += 1
        raise AssertionError("read_tensor_batch must use direct tensor destinations")

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        self.tensor_read_count += 1
        self.tensor_reads.append((shard_path, offset, nbytes, target, device, non_blocking))
        payload = self.payloads[(shard_path, offset, nbytes)]
        memoryview(tensor)[:nbytes] = payload
        return nbytes


class TrainerBatchStub:
    def __init__(self, samples, *, batch_index=0, resume_cursor=None):
        self.samples = tuple(samples)
        self.batch_index = batch_index
        self.resume_cursor = resume_cursor

    @property
    def sample_count(self):
        return len(self.samples)

    @property
    def byte_size(self):
        return sum(sample.read.nbytes for sample in self.samples)


class AcknowledgeableTrainerStreamStub:
    def __init__(self, batches):
        self.batches = tuple(batches)
        self.cursor = object()
        self.index = 0
        self.acknowledged = []
        self.update_cursor_args = []
        self.discarded = False

    def next_batch(self, *, update_cursor=True):
        self.update_cursor_args.append(update_cursor)
        if update_cursor:
            raise AssertionError("pipeline stream prefetch must defer cursor acknowledgement")
        if self.index >= len(self.batches):
            raise StopIteration
        batch = self.batches[self.index]
        self.index += 1
        return batch

    def acknowledge_batch(self, batch):
        self.acknowledged.append(batch)
        return self.cursor

    def _discard_unacknowledged_prefetch(self):
        self.discarded = True


class CoordinatedFailingDestinationClient:
    def __init__(self):
        self.second_started = threading.Event()
        self.failure_raised = threading.Event()
        self.release_second = threading.Event()
        self.second_finished = threading.Event()

    def read_into_buffer(self, shard_path, offset, buffer, nbytes=None):
        active = nbytes or len(buffer)
        if offset == 0:
            if not self.second_started.wait(timeout=2.0):
                raise AssertionError("sibling read did not start")
            self.failure_raised.set()
            raise OSError("network failure before direct batch publish")
        if offset == 4:
            self.second_started.set()
            if not self.release_second.wait(timeout=2.0):
                raise AssertionError("pipeline did not drain sibling read")
            memoryview(buffer)[:active] = b"z" * active
            self.second_finished.set()
            return active
        raise AssertionError(f"unexpected read offset: {offset}")

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        return self.read_into_buffer(shard_path, offset, tensor, nbytes)


class BlockingPrefetchTensorClient(DirectOnlyTensorClient):
    def __init__(self, payloads):
        super().__init__(payloads)
        self.second_started = threading.Event()
        self.release_second = threading.Event()

    def read_into_tensor(self, shard_path, offset, tensor, nbytes, target=None, device=None, non_blocking=True):
        if offset == 3:
            self.second_started.set()
            if not self.release_second.wait(timeout=2.0):
                raise AssertionError("second tensor prefetch was not released")
        return super().read_into_tensor(shard_path, offset, tensor, nbytes, target, device, non_blocking)


if __name__ == "__main__":
    unittest.main()
