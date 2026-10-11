from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import queue
import threading
from typing import Any, Callable, Iterable, Sequence

from .dataset import CxnfsPythonClient
from .bulk import ABSOLUTE_MAX_CORDS
from .tensor_memory import tensor_writable_memoryview


ZERO_FILL_BLOCK = b"\x00" * (1024 * 1024)


class AugmentationPlacement(str, Enum):
    DISABLED = "disabled"
    GPU = "gpu"
    CPU_THREAD_POOL = "cpu-thread-pool"
    CPU_PROCESS_POOL = "cpu-process-pool"


@dataclass(frozen=True)
class SampleRead:
    shard_path: str
    offset: int
    nbytes: int
    sample_id: str = ""

    def __post_init__(self) -> None:
        if not self.shard_path:
            raise ValueError("shard_path is required")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.nbytes <= 0:
            raise ValueError("nbytes must be positive")


@dataclass(frozen=True)
class TorchCompilePolicy:
    enabled: bool = True
    mode: str = "reduce-overhead"
    fullgraph: bool = False
    dynamic: bool = True

    def __post_init__(self) -> None:
        if self.enabled and not self.mode:
            raise ValueError("torch compile mode is required when compile is enabled")


@dataclass(frozen=True)
class CxnfsPipelineConfig:
    network_read_workers: int = 16
    max_cords: int = ABSOLUTE_MAX_CORDS
    prefetch_batches: int = 4
    max_inflight_bytes: int = 1024 * 1024 * 1024
    pin_memory: bool = True
    augmentation_placement: AugmentationPlacement = AugmentationPlacement.GPU
    allow_cpu_augmentations: bool = False
    cpu_augmentation_workers: int | None = None
    torch_compile: TorchCompilePolicy = field(default_factory=TorchCompilePolicy)

    def __post_init__(self) -> None:
        if self.network_read_workers <= 0:
            raise ValueError("network_read_workers must be positive")
        if self.max_cords <= 0 or self.max_cords > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"max_cords must be between 1 and {ABSOLUTE_MAX_CORDS}")
        if self.network_read_workers > self.max_cords:
            raise ValueError("network_read_workers must not exceed max_cords")
        if self.prefetch_batches <= 0:
            raise ValueError("prefetch_batches must be positive")
        if self.max_inflight_bytes <= 0:
            raise ValueError("max_inflight_bytes must be positive")
        if self.augmentation_placement is None:
            raise ValueError("augmentation_placement is required")
        if self.augmentation_placement in {
            AugmentationPlacement.CPU_THREAD_POOL,
            AugmentationPlacement.CPU_PROCESS_POOL,
        } and not self.allow_cpu_augmentations:
            raise ValueError("CPU augmentations must be explicitly allowed")
        if self.cpu_augmentation_workers is not None and self.cpu_augmentation_workers <= 0:
            raise ValueError("cpu_augmentation_workers must be positive")

    def validate_for_device(self, device: str, augmentation_enabled: bool) -> None:
        if not augmentation_enabled:
            return
        if device.startswith("cuda") and self.augmentation_placement != AugmentationPlacement.GPU and not self.allow_cpu_augmentations:
            raise ValueError("CUDA training requires GPU-side augmentation placement unless CPU augmentations are explicitly allowed")


BatchTransform = Callable[[tuple[bytes, ...], str], Any]
TensorBatchTransform = Callable[[tuple[Any, ...], str], Any]
SampleTransform = Callable[[bytes], Any]
TransformCompiler = Callable[[Callable[..., Any], TorchCompilePolicy], Callable[..., Any]]
TensorDestinationFactory = Callable[[SampleRead], Any]
TrainerBatchAcknowledger = Callable[[Any], Any]


@dataclass(frozen=True)
class CxnfsPipelinePrefetchedTensorBatch:
    batch: Any
    tensors: Any

    @property
    def batch_index(self) -> int | None:
        value = getattr(self.batch, "batch_index", None)
        return value if isinstance(value, int) else None

    @property
    def sample_count(self) -> int:
        value = getattr(self.batch, "sample_count", None)
        if isinstance(value, int):
            return value
        samples = getattr(self.batch, "samples", ())
        try:
            return len(samples)
        except TypeError:
            return 0

    @property
    def byte_size(self) -> int:
        value = getattr(self.batch, "byte_size", None)
        return value if isinstance(value, int) else 0

    @property
    def resume_cursor(self) -> Any:
        return getattr(self.batch, "resume_cursor", None)


@dataclass(frozen=True)
class _PipelinePrefetchFailure:
    error: BaseException


class CxnfsTrainingPipeline:
    def __init__(
        self,
        client: CxnfsPythonClient,
        config: CxnfsPipelineConfig | None = None,
        transform_compiler: TransformCompiler | None = None,
        *,
        close_client: bool = False,
    ) -> None:
        if client is None:
            raise ValueError("client is required")
        self.client = client
        self.config = config or CxnfsPipelineConfig()
        self.transform_compiler = transform_compiler or default_torch_transform_compiler
        self._compiled_gpu_transforms: dict[int, BatchTransform] = {}
        self._compiled_gpu_tensor_transforms: dict[int, TensorBatchTransform] = {}
        self._close_client = close_client
        self._closed = False
        self._executor_lock = threading.Lock()
        self._network_executor: ThreadPoolExecutor | None = None
        self._cpu_thread_executor: ThreadPoolExecutor | None = None
        self._cpu_process_executor: ProcessPoolExecutor | None = None

    def read_samples(self, reads: Sequence[SampleRead | tuple[str | Path, int, int]]) -> tuple[bytes, ...]:
        self._ensure_open()
        requests = tuple(normalize_reads(reads))
        self._validate_inflight_budget(requests)

        executor = self._network_pool()
        futures = [
            executor.submit(self.client.read_mds_shard, request.shard_path, request.offset, request.nbytes)
            for request in requests
        ]
        try:
            results = tuple(future.result() for future in futures)
            for request, payload in zip(requests, results):
                if len(payload) != request.nbytes:
                    raise ValueError(f"short CXNFS sample read for {request.sample_id or request.shard_path}")
            return results
        except BaseException:
            _cancel_and_drain_futures(futures)
            raise

    def read_into_buffers(
        self,
        reads: Sequence[SampleRead | tuple[str | Path, int, int]],
        destinations: Sequence[Any],
    ) -> tuple[int, ...]:
        self._ensure_open()
        requests = tuple(normalize_reads(reads))
        self._validate_destination_count(requests, destinations)
        self._validate_inflight_budget(requests)

        executor = self._network_pool()
        futures = [
            executor.submit(
                self.client.read_into_buffer,
                request.shard_path,
                request.offset,
                destination,
                request.nbytes,
            )
            for request, destination in zip(requests, destinations)
        ]
        try:
            counts = tuple(future.result() for future in futures)
            self._validate_counts(requests, counts)
            return counts
        except BaseException:
            _cancel_and_drain_futures(futures)
            _clear_destinations(destinations)
            raise

    def read_into_tensors(
        self,
        reads: Sequence[SampleRead | tuple[str | Path, int, int]],
        tensors: Sequence[Any],
        *,
        non_blocking: bool = True,
    ) -> tuple[int, ...]:
        self._ensure_open()
        requests = tuple(normalize_reads(reads))
        self._validate_destination_count(requests, tensors)
        self._validate_inflight_budget(requests)

        executor = self._network_pool()
        futures = [
            executor.submit(
                self.client.read_into_tensor,
                request.shard_path,
                request.offset,
                tensor,
                request.nbytes,
                None,
                None,
                non_blocking,
            )
            for request, tensor in zip(requests, tensors)
        ]
        try:
            counts = tuple(future.result() for future in futures)
            self._validate_counts(requests, counts)
            return counts
        except BaseException:
            _cancel_and_drain_futures(futures)
            _clear_destinations(tensors)
            raise

    def read_tensor_batch(
        self,
        reads: Sequence[SampleRead | tuple[str | Path, int, int]],
        tensor_factory: TensorDestinationFactory,
        *,
        device: str = "cuda:0",
        tensor_transform: TensorBatchTransform | None = None,
        non_blocking: bool = True,
    ) -> tuple[Any, ...] | Any:
        self._ensure_open()
        if tensor_factory is None:
            raise ValueError("tensor_factory is required")
        requests = tuple(normalize_reads(reads))
        self._validate_inflight_budget(requests)
        tensors = tuple(tensor_factory(request) for request in requests)
        self._validate_destination_count(requests, tensors)
        self.read_into_tensors(requests, tensors, non_blocking=non_blocking)
        if tensor_transform is None:
            return tensors
        self.config.validate_for_device(device, augmentation_enabled=True)
        if self.config.augmentation_placement != AugmentationPlacement.GPU:
            raise ValueError("tensor_transform requires GPU augmentation placement")
        return self._gpu_tensor_transform(tensor_transform)(tensors, device)

    def read_trainer_tensor_batch(
        self,
        batch: Any,
        *,
        device: str = "cuda:0",
        tensor_transform: TensorBatchTransform | None = None,
        non_blocking: bool = True,
    ) -> tuple[Any, ...] | Any:
        self._ensure_open()
        samples = tuple(getattr(batch, "samples", ()))
        if not samples:
            raise ValueError("trainer batch samples are required")
        reads = []
        destinations = []
        for sample in samples:
            read = getattr(sample, "read", None)
            destination = getattr(sample, "destination", None)
            if not isinstance(read, SampleRead):
                raise ValueError("trainer batch sample is missing a SampleRead")
            if destination is None:
                raise ValueError("trainer batch sample is missing a destination")
            reads.append(read)
            destinations.append(destination)
        self.read_into_tensors(tuple(reads), tuple(destinations), non_blocking=non_blocking)
        tensors = tuple(destinations)
        if tensor_transform is None:
            return tensors
        self.config.validate_for_device(device, augmentation_enabled=True)
        if self.config.augmentation_placement != AugmentationPlacement.GPU:
            raise ValueError("tensor_transform requires GPU augmentation placement")
        return self._gpu_tensor_transform(tensor_transform)(tensors, device)

    def prefetch_trainer_tensor_batches(
        self,
        batches: Iterable[Any],
        *,
        max_prefetch_batches: int | None = None,
        device: str = "cuda:0",
        tensor_transform: TensorBatchTransform | None = None,
        non_blocking: bool = True,
        acknowledge: TrainerBatchAcknowledger | None = None,
        discard_unacknowledged: Callable[[], Any] | None = None,
    ) -> CxnfsPipelineTensorPrefetchStream:
        self._ensure_open()
        if batches is None:
            raise ValueError("batches are required")
        return CxnfsPipelineTensorPrefetchStream(
            self,
            batches,
            max_prefetch_batches=(
                self.config.prefetch_batches
                if max_prefetch_batches is None
                else max_prefetch_batches
            ),
            device=device,
            tensor_transform=tensor_transform,
            non_blocking=non_blocking,
            acknowledge=acknowledge,
            discard_unacknowledged=discard_unacknowledged,
        )

    def prefetch_trainer_stream(
        self,
        stream: Any,
        *,
        max_prefetch_batches: int | None = None,
        device: str = "cuda:0",
        tensor_transform: TensorBatchTransform | None = None,
        non_blocking: bool = True,
    ) -> CxnfsPipelineTensorPrefetchStream:
        self._ensure_open()
        next_batch = getattr(stream, "next_batch", None)
        acknowledge = getattr(stream, "acknowledge_batch", None)
        if not callable(next_batch) or not callable(acknowledge):
            raise ValueError("trainer stream must expose next_batch and acknowledge_batch")

        def batches():
            while True:
                try:
                    yield next_batch(update_cursor=False)
                except StopIteration:
                    return

        discard = getattr(stream, "_discard_unacknowledged_prefetch", None)
        return self.prefetch_trainer_tensor_batches(
            batches(),
            max_prefetch_batches=max_prefetch_batches,
            device=device,
            tensor_transform=tensor_transform,
            non_blocking=non_blocking,
            acknowledge=acknowledge,
            discard_unacknowledged=discard if callable(discard) else None,
        )

    def read_batch(
        self,
        reads: Sequence[SampleRead | tuple[str | Path, int, int]],
        *,
        device: str = "cuda:0",
        gpu_transform: BatchTransform | None = None,
        cpu_transform: SampleTransform | None = None,
    ) -> Any:
        self._ensure_open()
        if gpu_transform is not None and cpu_transform is not None:
            raise ValueError("choose gpu_transform or cpu_transform, not both")
        augmentation_enabled = gpu_transform is not None or cpu_transform is not None
        self.config.validate_for_device(device, augmentation_enabled)

        samples = self.read_samples(reads)
        if gpu_transform is not None:
            if self.config.augmentation_placement != AugmentationPlacement.GPU:
                raise ValueError("gpu_transform requires GPU augmentation placement")
            return self._gpu_transform(gpu_transform)(samples, device)
        if cpu_transform is not None:
            if self.config.augmentation_placement == AugmentationPlacement.GPU:
                raise ValueError("cpu_transform is disabled while augmentation placement is GPU")
            return self._apply_cpu_transform(samples, cpu_transform)
        return samples

    def _apply_cpu_transform(self, samples: tuple[bytes, ...], transform: SampleTransform) -> tuple[Any, ...]:
        if self.config.augmentation_placement == AugmentationPlacement.DISABLED:
            raise ValueError("CPU transform provided while augmentations are disabled")
        if self.config.augmentation_placement == AugmentationPlacement.CPU_PROCESS_POOL:
            return self._map_transform(self._cpu_process_pool(), transform, samples)
        return self._map_transform(self._cpu_thread_pool(), transform, samples)

    def _gpu_transform(self, transform: BatchTransform) -> BatchTransform:
        self._ensure_open()
        if not self.config.torch_compile.enabled:
            return transform
        if self.transform_compiler is None:
            return transform

        key = id(transform)
        compiled = self._compiled_gpu_transforms.get(key)
        if compiled is None:
            compiled = self.transform_compiler(transform, self.config.torch_compile)
            self._compiled_gpu_transforms[key] = compiled
        return compiled

    def _gpu_tensor_transform(self, transform: TensorBatchTransform) -> TensorBatchTransform:
        self._ensure_open()
        if not self.config.torch_compile.enabled:
            return transform

        key = id(transform)
        compiled = self._compiled_gpu_tensor_transforms.get(key)
        if compiled is None:
            compiled = self.transform_compiler(transform, self.config.torch_compile)
            self._compiled_gpu_tensor_transforms[key] = compiled
        return compiled

    def close(self) -> None:
        with self._executor_lock:
            if self._closed:
                return
            self._closed = True
            executors = (
                self._network_executor,
                self._cpu_thread_executor,
                self._cpu_process_executor,
            )
            self._network_executor = None
            self._cpu_thread_executor = None
            self._cpu_process_executor = None

        for executor in executors:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
        if self._close_client:
            close = getattr(self.client, "close", None)
            if callable(close):
                close()

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> CxnfsTrainingPipeline:
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("training pipeline is closed")

    def _validate_inflight_budget(self, requests: Sequence[SampleRead]) -> None:
        total = sum(request.nbytes for request in requests)
        if total > self.config.max_inflight_bytes:
            raise ValueError("batch exceeds max_inflight_bytes")

    @staticmethod
    def _validate_destination_count(requests: Sequence[SampleRead], destinations: Sequence[Any]) -> None:
        if len(requests) != len(destinations):
            raise ValueError("destination count must match read count")

    @staticmethod
    def _validate_counts(requests: Sequence[SampleRead], counts: Sequence[int]) -> None:
        for request, count in zip(requests, counts):
            if count != request.nbytes:
                raise ValueError(f"short CXNFS destination read for {request.sample_id or request.shard_path}")

    def _network_pool(self) -> ThreadPoolExecutor:
        with self._executor_lock:
            self._ensure_open()
            if self._network_executor is None:
                self._network_executor = ThreadPoolExecutor(
                    max_workers=self.config.network_read_workers,
                    thread_name_prefix="cxnfs-network-read",
                )
            return self._network_executor

    def _cpu_thread_pool(self) -> ThreadPoolExecutor:
        with self._executor_lock:
            self._ensure_open()
            if self._cpu_thread_executor is None:
                self._cpu_thread_executor = ThreadPoolExecutor(
                    max_workers=self.config.cpu_augmentation_workers or self.config.network_read_workers,
                    thread_name_prefix="cxnfs-cpu-augment",
                )
            return self._cpu_thread_executor

    def _cpu_process_pool(self) -> ProcessPoolExecutor:
        with self._executor_lock:
            self._ensure_open()
            if self._cpu_process_executor is None:
                self._cpu_process_executor = ProcessPoolExecutor(
                    max_workers=self.config.cpu_augmentation_workers or self.config.network_read_workers,
                )
            return self._cpu_process_executor

    @staticmethod
    def _map_transform(executor, transform: SampleTransform, samples: tuple[bytes, ...]) -> tuple[Any, ...]:
        futures = [executor.submit(transform, sample) for sample in samples]
        try:
            return tuple(future.result() for future in futures)
        except BaseException:
            _cancel_and_drain_futures(futures)
            raise


class CxnfsPipelineTensorPrefetchStream:
    def __init__(
        self,
        pipeline: CxnfsTrainingPipeline,
        batches: Iterable[Any],
        *,
        max_prefetch_batches: int,
        device: str,
        tensor_transform: TensorBatchTransform | None,
        non_blocking: bool,
        acknowledge: TrainerBatchAcknowledger | None,
        discard_unacknowledged: Callable[[], Any] | None,
    ) -> None:
        if pipeline is None:
            raise ValueError("pipeline is required")
        if batches is None:
            raise ValueError("batches are required")
        if max_prefetch_batches <= 0:
            raise ValueError("max_prefetch_batches must be positive")
        self.pipeline = pipeline
        self.max_prefetch_batches = int(max_prefetch_batches)
        self.device = device
        self.tensor_transform = tensor_transform
        self.non_blocking = non_blocking
        self.acknowledge = acknowledge
        self.discard_unacknowledged = discard_unacknowledged
        self._batches = iter(batches)
        self._queue: queue.Queue[object] = queue.Queue(maxsize=self.max_prefetch_batches)
        self._sentinel = object()
        self._closed = False
        self._consumed_all = False
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="cxnfs-pipeline-prefetch",
        )
        self._producer = self._executor.submit(self._produce)

    @property
    def queued_batches(self) -> int:
        return self._queue.qsize()

    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self) -> CxnfsPipelineTensorPrefetchStream:
        return self

    def __next__(self) -> CxnfsPipelinePrefetchedTensorBatch:
        while True:
            if self._closed:
                raise StopIteration
            try:
                item = self._queue.get(timeout=0.05)
                break
            except queue.Empty:
                continue
        if item is self._sentinel:
            self._consumed_all = True
            self._close(discard=False)
            raise StopIteration
        if isinstance(item, _PipelinePrefetchFailure):
            self.close()
            error = item.error
            if isinstance(error, Exception):
                raise error
            raise RuntimeError(str(error)) from error
        if not isinstance(item, CxnfsPipelinePrefetchedTensorBatch):
            self.close()
            raise RuntimeError("pipeline prefetch worker returned an invalid item")
        if self.acknowledge is not None:
            try:
                self.acknowledge(item.batch)
            except BaseException:
                self.close()
                raise
        return item

    def close(self) -> None:
        self._close(discard=not self._consumed_all)

    def __enter__(self) -> CxnfsPipelineTensorPrefetchStream:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _close(self, *, discard: bool) -> None:
        if self._closed:
            return
        self._closed = True
        self._drain_queue()
        if discard and self.discard_unacknowledged is not None:
            self.discard_unacknowledged()
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._drain_queue()

    def _produce(self) -> None:
        try:
            for batch in self._batches:
                if self._closed:
                    return
                tensors = self.pipeline.read_trainer_tensor_batch(
                    batch,
                    device=self.device,
                    tensor_transform=self.tensor_transform,
                    non_blocking=self.non_blocking,
                )
                self._put(CxnfsPipelinePrefetchedTensorBatch(batch, tensors))
            self._put(self._sentinel)
        except BaseException as exc:
            self._put(_PipelinePrefetchFailure(exc))

    def _put(self, item: object) -> None:
        while not self._closed:
            try:
                self._queue.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    def _drain_queue(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return


def _cancel_and_drain_futures(futures: Iterable[Any]) -> None:
    pending = tuple(futures)
    for future in pending:
        future.cancel()
    for future in pending:
        try:
            future.result()
        except BaseException:
            pass


def _clear_destinations(destinations: Iterable[Any]) -> None:
    for destination in destinations:
        try:
            _clear_destination(destination)
        except BaseException:
            pass


def _clear_destination(destination: Any) -> int:
    view = tensor_writable_memoryview(destination)
    length = view.nbytes
    cursor = 0
    while cursor < length:
        end = min(length, cursor + len(ZERO_FILL_BLOCK))
        view[cursor:end] = ZERO_FILL_BLOCK[:end - cursor]
        cursor = end
    return length


def normalize_reads(reads: Iterable[SampleRead | tuple[str | Path, int, int]]) -> list[SampleRead]:
    normalized: list[SampleRead] = []
    for item in reads:
        if isinstance(item, SampleRead):
            normalized.append(item)
        else:
            path, offset, nbytes = item
            normalized.append(SampleRead(str(path), offset, nbytes))
    return normalized


def default_torch_transform_compiler(
    transform: Callable[..., Any],
    policy: TorchCompilePolicy,
) -> Callable[..., Any]:
    if not policy.enabled:
        return transform
    try:
        import torch  # type: ignore
    except Exception:
        return transform
    compile_fn = getattr(torch, "compile", None)
    if not callable(compile_fn):
        return transform
    return compile_fn(
        transform,
        mode=policy.mode,
        fullgraph=policy.fullgraph,
        dynamic=policy.dynamic,
    )
