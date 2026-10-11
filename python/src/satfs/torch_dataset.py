from __future__ import annotations

import copy
import hashlib
import io
import json
import math
import os
import platform
import queue
import random
import sys
import tarfile
import threading
import time
from argparse import ArgumentParser, Namespace
from collections.abc import Callable
from collections import deque
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping, MutableMapping, Sequence

from .auth import ClientCredentials, prompt_client_credentials
from .benchmark import (
    AI_TRAINING_25GBE_OBJECTIVE,
    AI_TRAINING_MINIMUM_BYTES_PER_SECOND,
    NativeReadBenchmarkResult,
    evaluate_native_read_objective,
)
from .bulk import (
    ABSOLUTE_MAX_CORDS,
    CxnfsBulkReadConfig,
    CxnfsBulkRetryConfig,
    CxnfsLengthPrefixedBulkClient,
    CxnfsQuicBulkClient,
    PYTHON_BULK_TRANSPORT_QUIC,
    PYTHON_BULK_TRANSPORT_STREAM,
    normalize_python_bulk_transport,
    require_supported_python_bulk_transport,
)
from .control import CxnfsControlClient
from .dataset import CxnfsDatasetClient, CxnfsPathPrefixClient, CxnfsPythonClient, TensorMemoryTarget
from .health import connect_and_probe_cxnfs_health
from .local import LocalFileTransport
from .mds import (
    MdsBatch,
    MdsDataReadPlan,
    MdsDataset,
    MdsDirectBufferSample,
    MdsDirectSample,
    MdsDirectTensorSample,
    MdsPrefetchResult,
    MdsSample,
)
from .namespace import join_namespace_path, normalize_namespace_root, normalize_relative_namespace_path
from .pipeline import (
    CxnfsPipelineConfig,
    CxnfsPipelineTensorPrefetchStream,
    CxnfsTrainingPipeline,
    SampleRead,
)
from .tls import CXNFS_TLS_CIPHER_SUITE, create_client_tls_context
from .tensor_memory import dtype_nbytes as _dtype_nbytes
from .tensor_memory import torch_dtype as _torch_dtype
from .tensor_memory import torch_empty as _torch_empty

try:
    from torch.utils.data import IterableDataset as _TorchIterableDatasetBase  # type: ignore
except Exception:  # pragma: no cover - exercised only when torch is unavailable
    _TorchIterableDatasetBase = object  # type: ignore


class MdsSampleFormat(str, Enum):
    RAW = "raw"
    PARQUET_JSON = "parquet-json"
    WEBDATASET_TAR = "webdataset-tar"


class TrainerReadMode(str, Enum):
    SAMPLES = "samples"
    DIRECT_BUFFERS = "direct-buffers"
    DIRECT_TENSORS = "direct-tensors"


TRAINER_CONTRACT_FORMAT = "cxnfs.trainer-contract.v1"
TRAINER_CURSOR_FORMAT = "cxnfs.trainer-cursor.v1"
TRAINER_DISTRIBUTED_PLAN_FORMAT = "cxnfs.trainer-distributed-plan.v1"
TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT = "cxnfs.trainer-distributed-runtime-smoke.v1"
TRAINER_HANDOFF_FORMAT = "cxnfs.trainer-handoff.v1"
TRAINER_INTEGRATION_PLAN_FORMAT = "cxnfs.trainer-integration-plan.v1"
TRAINER_NETWORK_PREFLIGHT_FORMAT = "cxnfs.trainer-network-preflight.v1"
TRAINER_INTEGRATION_PREFLIGHT_FORMAT = "cxnfs.trainer-integration-preflight.v1"
TRAINER_INTEGRATION_SMOKE_FORMAT = "cxnfs.trainer-integration-smoke.v1"
TRAINER_ACCEPTANCE_FIXTURE_FORMAT = "cxnfs.trainer-acceptance-fixture.v1"
TRAINER_HANDOFF_ACCEPTANCE_FORMAT = "cxnfs.trainer-handoff-acceptance.v1"
TRAINER_LOOP_CONTRACT_FORMAT = "cxnfs.trainer-loop-contract.v1"
TRAINER_LOOP_CONTRACT_CHECK_FORMAT = "cxnfs.trainer-loop-contract-check.v1"
TRAINER_LOW_LEVEL_DIRECT_READ_PROOFS_FORMAT = "cxnfs.trainer-low-level-direct-read-proofs.v1"
TRAINER_LOW_LEVEL_DIRECT_WRITE_PROOFS_FORMAT = "cxnfs.trainer-low-level-direct-write-proofs.v1"
TRAINER_READINESS_BUNDLE_FORMAT = "cxnfs.trainer-readiness-bundle.v1"
TRAINER_LOOP_REPLAY_FORMAT = "cxnfs.trainer-loop-replay.v1"
TRAINER_RUNTIME_BINDING_FORMAT = "cxnfs.trainer-runtime-binding.v1"
TRAINER_RUNTIME_SMOKE_FORMAT = "cxnfs.trainer-runtime-smoke.v1"
TRAINER_SUSTAINED_READ_PROFILE_FORMAT = "cxnfs.trainer-sustained-read-profile.v1"
TRAINER_WORKLOAD_PRESSURE_FORMAT = "cxnfs.trainer-workload-pressure.v1"
TRAINER_READ_PATH_RELIABILITY_FORMAT = "cxnfs.trainer-read-path-reliability.v1"
TRAINER_SURFACE_CHECK_FORMAT = "cxnfs.trainer-surface-check.v1"
TRAINER_SURFACE_MATRIX_FORMAT = "cxnfs.trainer-surface-matrix.v1"
TRAINER_WORKLOAD_PROVENANCE_FORMAT = "cxnfs.trainer-workload-provenance.v1"
TRAINER_STREAM_STATE_FORMAT = "cxnfs.trainer-stream-state.v1"
TRAINER_WORKER_PARTITION_FORMAT = "cxnfs.trainer-worker-partition.v1"
TRAINER_TENSOR_MEMORY_PATH_FORMAT = "cxnfs.trainer-tensor-memory-path.v1"
TRAINER_PIPELINE_SMOKE_FORMAT = "cxnfs.trainer-pipeline-smoke.v1"
TRAINER_PIPELINE_NETWORK_PATH_FORMAT = "cxnfs.trainer-pipeline-network-path.v1"
WEIGHTED_MDS_WORKLOAD_FINGERPRINT_FORMAT = "cxnfs.weighted-mds-workload-fingerprint.v1"
TRAINER_BATCH_ITERATOR_NATIVE = "native"
TRAINER_BATCH_ITERATOR_DATALOADER = "trainer-dataloader"
TRAINER_BATCH_ITERATORS = (TRAINER_BATCH_ITERATOR_NATIVE, TRAINER_BATCH_ITERATOR_DATALOADER)
TRAINER_TENSOR_TARGETS = tuple(
    target.value
    for target in TensorMemoryTarget
    if target != TensorMemoryTarget.BYTES
)


SampleDecoder = Callable[[MdsSample], Any]
SampleTransform = Callable[[Any], Any]
DatasetClientFactory = Callable[[str], CxnfsDatasetClient]
DirectDestinationFactory = Callable[[str, int, int, int], object]
DirectTensorFactory = Callable[[str, int, int, int], object]


class CxnfsTrainerSurfaceError(RuntimeError):
    """Trainer-facing failure with source/read context preserved."""

    def __init__(
        self,
        operation: str,
        *,
        context: Mapping[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        if not operation:
            raise ValueError("operation is required")
        self.operation = operation
        self.context = dict(context or {})
        self.cause = cause
        detail = _trainer_surface_error_detail(self.context)
        cause_text = "" if cause is None else f": {cause}"
        message = f"{operation} failed"
        if detail:
            message += f" ({detail})"
        super().__init__(message + cause_text)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": "cxnfs.trainer-surface-error.v1",
            "operation": self.operation,
            "context": dict(self.context),
            "cause_type": None if self.cause is None else type(self.cause).__name__,
            "cause": None if self.cause is None else str(self.cause),
        }


def _trainer_surface_error_detail(context: Mapping[str, Any]) -> str:
    fields = []
    for key in (
        "read_mode",
        "source_name",
        "source_index",
        "sample_indices",
        "chunk_sample_count",
        "prefetch_chunks",
        "max_prefetch_bytes",
    ):
        value = context.get(key)
        if value is not None:
            fields.append(f"{key}={value}")
    return ", ".join(fields)


def _raise_trainer_surface_error(
    cause: BaseException,
    operation: str,
    **context: Any,
) -> None:
    if isinstance(cause, CxnfsTrainerSurfaceError):
        raise cause
    raise CxnfsTrainerSurfaceError(
        operation,
        context={key: value for key, value in context.items() if value is not None},
        cause=cause,
    ) from cause


def _trainer_read_mode_value(read_mode: TrainerReadMode | str | None) -> str | None:
    if read_mode is None:
        return None
    try:
        return TrainerReadMode(read_mode).value
    except Exception:
        return str(read_mode)


def _source_failure_context(
    sources: Sequence["CxnfsWeightedSource"],
    source_index: int,
    indices: Sequence[int],
    read_mode: TrainerReadMode | str,
) -> dict[str, Any]:
    source_name = sources[source_index].name if 0 <= source_index < len(sources) else None
    return {
        "read_mode": _trainer_read_mode_value(read_mode),
        "source_name": source_name,
        "source_index": source_index,
        "sample_indices": list(indices),
        "source_batch_size": len(indices),
    }


def _chunk_failure_context(
    sources: Sequence["CxnfsWeightedSource"],
    plan: "_WeightedChunkPlan",
    read_mode: TrainerReadMode | str,
    *,
    prefetch_chunks: int | None = None,
    max_prefetch_bytes: int | None = None,
) -> dict[str, Any]:
    source_names = [
        sources[source_index].name
        for source_index, indices in enumerate(plan.indices_by_source)
        if indices
    ]
    return {
        "read_mode": _trainer_read_mode_value(read_mode),
        "source_names": source_names,
        "chunk_sample_count": plan.sample_count,
        "chunk_estimated_payload_bytes": plan.estimated_payload_bytes,
        "prefetch_chunks": prefetch_chunks,
        "max_prefetch_bytes": max_prefetch_bytes,
    }


class CxnfsLazyDatasetClient(CxnfsDatasetClient):
    def __init__(
        self,
        source_name: str,
        client_factory: DatasetClientFactory,
        *,
        path_prefix: str | Path | None = None,
    ) -> None:
        if not source_name:
            raise ValueError("source_name is required")
        if client_factory is None:
            raise ValueError("client_factory is required")
        self.source_name = source_name
        self.client_factory = client_factory
        self.path_prefix = None if path_prefix is None else str(path_prefix)
        self._client: CxnfsDatasetClient | None = None
        self._lock = threading.RLock()

    @property
    def delegate(self) -> CxnfsDatasetClient | None:
        with self._lock:
            return self._client

    def client(self) -> CxnfsDatasetClient:
        with self._lock:
            if self._client is not None:
                return self._client
            client = self.client_factory(self.source_name)
            if self.path_prefix is not None:
                client = CxnfsPathPrefixClient(client, self.path_prefix)
            self._client = client
            return self._client

    def read_mds_shard(self, shard_path: str | Path, offset: int = 0, length: int | None = None) -> bytes:
        return self.client().read_mds_shard(shard_path, offset, length)

    def send_safetensors_output(self, output_dir: str | Path, remote_root: str | Path | None = None) -> SendResult:
        return self.client().send_safetensors_output(output_dir, remote_root=remote_root)

    def read_tensor(
        self,
        shard_path: str | Path,
        offset: int,
        nbytes: int,
        dtype: str,
        shape: tuple[int, ...],
        target: TensorMemoryTarget = TensorMemoryTarget.PYTORCH_PINNED_CPU,
        device: str = "cpu",
        non_blocking: bool = True,
    ) -> Any:
        return _require_client_method(self.client(), "read_tensor")(
            shard_path,
            offset,
            nbytes,
            dtype,
            shape,
            target,
            device,
            non_blocking,
        )

    def read_into_tensor(
        self,
        shard_path: str | Path,
        offset: int,
        tensor: Any,
        nbytes: int,
        target: TensorMemoryTarget | None = None,
        device: str | None = None,
        non_blocking: bool = True,
    ) -> int:
        return _require_client_method(self.client(), "read_into_tensor")(
            shard_path,
            offset,
            tensor,
            nbytes,
            target,
            device,
            non_blocking,
        )

    def read_into_tensors(self, reads, *, max_workers: int | None = None) -> tuple[int, ...]:
        return _require_client_method(self.client(), "read_into_tensors")(
            reads,
            max_workers=max_workers,
        )

    def read_into_buffer(
        self,
        shard_path: str | Path,
        offset: int,
        buffer: Any,
        nbytes: int | None = None,
    ) -> int:
        return _require_client_method(self.client(), "read_into_buffer")(
            shard_path,
            offset,
            buffer,
            nbytes,
        )

    def read_into_buffers(self, reads, *, max_workers: int | None = None) -> tuple[int, ...]:
        return _require_client_method(self.client(), "read_into_buffers")(
            reads,
            max_workers=max_workers,
        )

    def write_tensor(self, path: str | Path, tensor: Any, nbytes: int) -> SendResult:
        return _require_client_method(self.client(), "write_tensor")(path, tensor, nbytes)

    def close(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
        close = getattr(client, "close", None)
        if callable(close):
            close()

    def __getstate__(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "client_factory": self.client_factory,
            "path_prefix": self.path_prefix,
        }

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self.source_name = str(state["source_name"])
        self.client_factory = state["client_factory"]
        self.path_prefix = state.get("path_prefix")
        self._client = None
        self._lock = threading.RLock()


@dataclass(frozen=True)
class CxnfsTrainerTensorAllocation:
    target: TensorMemoryTarget | str = TensorMemoryTarget.PYTORCH_PINNED_CPU
    dtype: str = "uint8"
    device: str = "cpu"
    non_blocking: bool = True

    def __post_init__(self) -> None:
        target = TensorMemoryTarget(self.target)
        if target == TensorMemoryTarget.BYTES:
            raise ValueError("trainer tensor allocation requires a PyTorch tensor target")
        if not self.dtype:
            raise ValueError("dtype is required")
        if not self.device:
            raise ValueError("device is required")
        if target == TensorMemoryTarget.PYTORCH_CUDA and not str(self.device).startswith("cuda"):
            raise ValueError("cuda tensor allocation requires a cuda device")
        if target != TensorMemoryTarget.PYTORCH_CUDA and str(self.device) != "cpu":
            raise ValueError("CPU tensor allocation requires device=cpu")
        object.__setattr__(self, "target", target)

    @property
    def uses_pinned_memory(self) -> bool:
        return self.target == TensorMemoryTarget.PYTORCH_PINNED_CPU

    @property
    def requires_pinned_host_staging(self) -> bool:
        return self.target == TensorMemoryTarget.PYTORCH_CUDA

    @property
    def fills_tensor_destination_directly(self) -> bool:
        return self.target in {TensorMemoryTarget.PYTORCH_CPU, TensorMemoryTarget.PYTORCH_PINNED_CPU}

    def memory_path(self) -> dict[str, Any]:
        return {
            "format": TRAINER_TENSOR_MEMORY_PATH_FORMAT,
            "target": self.target.value,
            "dtype": self.dtype,
            "device": self.device,
            "caller_owned_destination": True,
            "python_byte_batch_avoided": True,
            "network_fills_tensor_destination": self.fills_tensor_destination_directly,
            "uses_pinned_host_memory": self.uses_pinned_memory or self.requires_pinned_host_staging,
            "requires_pinned_host_staging": self.requires_pinned_host_staging,
            "device_copy_required": self.target == TensorMemoryTarget.PYTORCH_CUDA,
            "device_copy_non_blocking": self.target == TensorMemoryTarget.PYTORCH_CUDA and self.non_blocking,
            "gpu_target": self.target == TensorMemoryTarget.PYTORCH_CUDA,
            "gpu_direct_network_dma": False,
        }

    def factory(self) -> DirectTensorFactory:
        return trainer_tensor_factory(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": "cxnfs.trainer-tensor-allocation.v1",
            "target": self.target.value,
            "dtype": self.dtype,
            "device": self.device,
            "non_blocking": self.non_blocking,
            "uses_pinned_memory": self.uses_pinned_memory,
            "caller_owned_destination": True,
            "memory_path": self.memory_path(),
        }


class CxnfsTorchDataset:
    """Map-style dataset facade for PyTorch DataLoader compatible reads."""

    def __init__(
        self,
        root: str | Path | MdsDataset,
        *,
        client: CxnfsDatasetClient | None = None,
        decoder: MdsSampleFormat | str | SampleDecoder = MdsSampleFormat.RAW,
        transform: SampleTransform | None = None,
        include_metadata: bool = False,
        cache_bytes: int | None = None,
    ) -> None:
        self.decoder = normalize_decoder(decoder)
        if isinstance(root, MdsDataset):
            if cache_bytes is not None:
                raise ValueError("cache_bytes cannot be used when root is already an MdsDataset")
            self.mds = root
        else:
            self.mds = MdsDataset(root, client=client, cache_bytes=cache_bytes)
        self.transform = transform
        self.include_metadata = include_metadata

    def __len__(self) -> int:
        return len(self.mds)

    def __getitem__(self, index: int) -> Any:
        sample = self.mds[index]
        return self._item_from_sample(sample)

    def read_batch(self, indices: Sequence[int] | MdsBatch, max_workers: int = 16) -> tuple[Any, ...]:
        if isinstance(indices, MdsBatch):
            samples = self.mds.read_batch(indices, max_workers=max_workers)
        else:
            samples = self.mds.read_samples(indices, max_workers=max_workers)
        return tuple(
            self._item_from_sample(sample)
            for sample in samples
        )

    def read_data_into_buffers(
        self,
        indices: Sequence[int] | MdsBatch,
        destinations: Sequence[object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ):
        if isinstance(indices, MdsBatch):
            return self.mds.read_batch_data_into_buffers(
                indices,
                destinations,
                max_workers=max_workers,
                include_paths=include_paths,
            )
        return self.mds.read_data_into_buffers(
            indices,
            destinations,
            max_workers=max_workers,
            include_paths=include_paths,
        )

    def read_data_into_tensors(
        self,
        indices: Sequence[int] | MdsBatch,
        tensors: Sequence[object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
        non_blocking: bool = True,
    ):
        if isinstance(indices, MdsBatch):
            return self.mds.read_batch_data_into_tensors(
                indices,
                tensors,
                max_workers=max_workers,
                include_paths=include_paths,
                non_blocking=non_blocking,
            )
        return self.mds.read_data_into_tensors(
            indices,
            tensors,
            max_workers=max_workers,
            include_paths=include_paths,
            non_blocking=non_blocking,
        )

    def read_data_with_buffer_factory(
        self,
        indices: Sequence[int] | MdsBatch,
        destination_factory,
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDirectBufferSample, ...]:
        if isinstance(indices, MdsBatch):
            return self.mds.read_batch_data_with_buffer_factory(
                indices,
                destination_factory,
                max_workers=max_workers,
                include_paths=include_paths,
            )
        return self.mds.read_data_with_buffer_factory(
            indices,
            destination_factory,
            max_workers=max_workers,
            include_paths=include_paths,
        )

    def read_data_with_tensor_factory(
        self,
        indices: Sequence[int] | MdsBatch,
        tensor_factory,
        *,
        max_workers: int = 16,
        include_paths: bool = False,
        non_blocking: bool = True,
    ) -> tuple[MdsDirectTensorSample, ...]:
        if isinstance(indices, MdsBatch):
            return self.mds.read_batch_data_with_tensor_factory(
                indices,
                tensor_factory,
                max_workers=max_workers,
                include_paths=include_paths,
                non_blocking=non_blocking,
            )
        return self.mds.read_data_with_tensor_factory(
            indices,
            tensor_factory,
            max_workers=max_workers,
            include_paths=include_paths,
            non_blocking=non_blocking,
        )

    def plan_data_reads(
        self,
        indices: Sequence[int] | MdsBatch,
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDataReadPlan, ...]:
        if isinstance(indices, MdsBatch):
            return self.mds.plan_batch_data_reads(
                indices,
                max_workers=max_workers,
                include_paths=include_paths,
            )
        return self.mds.plan_data_reads(
            indices,
            max_workers=max_workers,
            include_paths=include_paths,
        )

    def plan_batches(
        self,
        batch_size: int,
        *,
        indices: Sequence[int] | None = None,
        shuffle: bool = False,
        seed: int | None = None,
        drop_last: bool = False,
    ) -> tuple[MdsBatch, ...]:
        return self.mds.plan_batches(
            batch_size,
            indices=indices,
            shuffle=shuffle,
            seed=seed,
            drop_last=drop_last,
        )

    def prefetch(
        self,
        indices: Sequence[int],
        max_workers: int = 16,
        coalesce: bool = True,
    ) -> MdsPrefetchResult:
        return self.mds.prefetch(indices, max_workers=max_workers, coalesce=coalesce)

    def prefetch_batch(self, batch: MdsBatch, max_workers: int = 16) -> MdsPrefetchResult:
        return self.mds.prefetch_batch(batch, max_workers=max_workers)

    def close(self) -> None:
        close = getattr(self.mds, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> CxnfsTorchDataset:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _item_from_sample(self, sample: MdsSample) -> Any:
        decoded = self.decoder(sample)
        if self.transform is not None:
            decoded = self.transform(decoded)
        if not self.include_metadata:
            return decoded
        return {
            "data": decoded,
            "path": sample.path,
            "global_index": sample.global_index,
            "shard_name": sample.shard_name,
        }


@dataclass(frozen=True)
class CxnfsWeightedSource:
    name: str
    root: str | Path | MdsDataset | CxnfsTorchDataset
    weight: int = 1
    decoder: MdsSampleFormat | str | SampleDecoder = MdsSampleFormat.RAW
    transform: SampleTransform | None = None
    cache_bytes: int | None = None
    client: CxnfsDatasetClient | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("source name is required")
        if self.weight <= 0:
            raise ValueError("source weight must be positive")

    def dataset(self) -> CxnfsTorchDataset:
        if isinstance(self.root, CxnfsTorchDataset):
            if self.cache_bytes is not None:
                raise ValueError("cache_bytes cannot be used when source root is already a CxnfsTorchDataset")
            if self.client is not None:
                raise ValueError("client cannot be used when source root is already a CxnfsTorchDataset")
            if self.transform is not None:
                raise ValueError("transform cannot be used when source root is already a CxnfsTorchDataset")
            if self.decoder != MdsSampleFormat.RAW:
                raise ValueError("decoder cannot be used when source root is already a CxnfsTorchDataset")
            return self.root
        if isinstance(self.root, MdsDataset):
            if self.cache_bytes is not None:
                raise ValueError("cache_bytes cannot be used when source root is already an MdsDataset")
            if self.client is not None:
                raise ValueError("client cannot be used when source root is already an MdsDataset")
            return CxnfsTorchDataset(
                self.root,
                decoder=self.decoder,
                transform=self.transform,
            )
        return CxnfsTorchDataset(
            self.root,
            client=self.client,
            decoder=self.decoder,
            transform=self.transform,
            cache_bytes=self.cache_bytes,
        )


@dataclass(frozen=True)
class CxnfsTrainingShard:
    index: int = 0
    count: int = 1

    def __post_init__(self) -> None:
        if self.count <= 0:
            raise ValueError("training shard count must be positive")
        if self.index < 0 or self.index >= self.count:
            raise ValueError("training shard index must be inside shard count")


@dataclass(frozen=True)
class CxnfsTrainerCursor:
    epoch: int
    cursors: tuple[int, ...]
    emitted: int = 0
    chunk_index: int = 0
    schedule_position: int = 0
    sample_limit: int | None = None
    shard_index: int = 0
    shard_count: int = 1
    source_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        cursors = tuple(int(value) for value in self.cursors)
        source_names = tuple(str(value) for value in self.source_names)
        if self.epoch < 0:
            raise ValueError("cursor epoch must be non-negative")
        if any(value < 0 for value in cursors):
            raise ValueError("cursor source positions must be non-negative")
        if self.emitted < 0:
            raise ValueError("cursor emitted count must be non-negative")
        if self.chunk_index < 0:
            raise ValueError("cursor chunk index must be non-negative")
        if self.schedule_position < 0:
            raise ValueError("cursor schedule position must be non-negative")
        if self.sample_limit is not None and self.sample_limit <= 0:
            raise ValueError("cursor sample limit must be positive when set")
        if self.shard_count <= 0:
            raise ValueError("cursor shard count must be positive")
        if self.shard_index < 0 or self.shard_index >= self.shard_count:
            raise ValueError("cursor shard index must be inside shard count")
        if source_names and len(source_names) != len(cursors):
            raise ValueError("cursor source_names must match source cursor count")
        object.__setattr__(self, "cursors", cursors)
        object.__setattr__(self, "source_names", source_names)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> CxnfsTrainerCursor:
        if payload.get("format") != TRAINER_CURSOR_FORMAT:
            raise ValueError(f"unsupported trainer cursor format: {payload.get('format')}")
        shard = _mapping(payload.get("shard"))
        return cls(
            epoch=_int_value(payload.get("epoch"), 0),
            cursors=tuple(_int_value(value, 0) for value in payload.get("cursors", ())),
            emitted=_int_value(payload.get("emitted"), 0),
            chunk_index=_int_value(payload.get("chunk_index"), 0),
            schedule_position=_int_value(payload.get("schedule_position"), 0),
            sample_limit=(
                None
                if payload.get("sample_limit") is None
                else _int_value(payload.get("sample_limit"), 0)
            ),
            shard_index=_int_value(shard.get("index"), 0),
            shard_count=_int_value(shard.get("count"), 1),
            source_names=tuple(str(value) for value in payload.get("source_names", ())),
        )

    @property
    def source_count(self) -> int:
        return len(self.cursors)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": TRAINER_CURSOR_FORMAT,
            "epoch": self.epoch,
            "emitted": self.emitted,
            "chunk_index": self.chunk_index,
            "schedule_position": self.schedule_position,
            "sample_limit": self.sample_limit,
            "source_count": self.source_count,
            "source_names": list(self.source_names),
            "cursors": list(self.cursors),
            "shard": {
                "index": self.shard_index,
                "count": self.shard_count,
            },
        }


@dataclass(frozen=True)
class CxnfsWeightedSample:
    source_name: str
    source_index: int
    sample_index: int
    data: Any


@dataclass(frozen=True)
class CxnfsTrainerBatchSample:
    source_name: str
    source_index: int
    sample_index: int
    data: Any


@dataclass(frozen=True)
class CxnfsTrainerDirectBatchSample:
    source_name: str
    source_index: int
    sample_index: int
    destination: Any
    metadata: MdsDirectSample

    def __post_init__(self) -> None:
        if not self.source_name:
            raise ValueError("source_name is required")
        if self.source_index < 0:
            raise ValueError("source_index must be non-negative")
        if self.sample_index < 0:
            raise ValueError("sample_index must be non-negative")
        if self.destination is None:
            raise ValueError("destination is required")
        if self.metadata is None:
            raise ValueError("metadata is required")

    @property
    def data(self) -> Any:
        try:
            return memoryview(self.destination)[:self.metadata.data_length]
        except TypeError:
            data = getattr(self.destination, "data", None)
            try:
                return memoryview(data).cast("B")[:self.metadata.data_length]
            except TypeError:
                return self.destination

    @property
    def byte_size(self) -> int:
        return self.metadata.data_length

    @property
    def path(self) -> str | None:
        return self.metadata.path


@dataclass(frozen=True)
class CxnfsTrainerPipelineBatchSample:
    source_name: str
    source_index: int
    sample_index: int
    destination: Any
    read: SampleRead
    path: str | None = None

    def __post_init__(self) -> None:
        if not self.source_name:
            raise ValueError("source_name is required")
        if self.source_index < 0:
            raise ValueError("source_index must be non-negative")
        if self.sample_index < 0:
            raise ValueError("sample_index must be non-negative")
        if self.destination is None:
            raise ValueError("destination is required")
        if self.read is None:
            raise ValueError("read is required")

    @property
    def data(self) -> Any:
        try:
            return memoryview(self.destination)[:self.read.nbytes]
        except TypeError:
            data = getattr(self.destination, "data", None)
            try:
                return memoryview(data).cast("B")[:self.read.nbytes]
            except TypeError:
                return self.destination

    @property
    def byte_size(self) -> int:
        return self.read.nbytes


@dataclass(frozen=True)
class CxnfsTrainerBatch:
    batch_index: int
    epoch: int
    samples: tuple[CxnfsTrainerBatchSample, ...]
    resume_cursor: CxnfsTrainerCursor | None = None
    worker_read_path_evidence: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.batch_index < 0:
            raise ValueError("batch_index must be non-negative")
        if self.epoch < 0:
            raise ValueError("epoch must be non-negative")
        if not self.samples:
            raise ValueError("samples are required")
        object.__setattr__(self, "samples", tuple(self.samples))
        if self.worker_read_path_evidence is not None:
            object.__setattr__(self, "worker_read_path_evidence", dict(self.worker_read_path_evidence))

    @property
    def sample_count(self) -> int:
        return len(self.samples)

    @property
    def byte_size(self) -> int:
        return sum(_trainer_sample_nbytes(sample) for sample in self.samples)

    @property
    def source_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for sample in self.samples:
            counts[sample.source_name] = counts.get(sample.source_name, 0) + 1
        return counts

    def to_dict(self, *, include_sample_data: bool = False) -> dict[str, Any]:
        payload = {
            "batch_index": self.batch_index,
            "epoch": self.epoch,
            "sample_count": self.sample_count,
            "byte_size": self.byte_size,
            "source_counts": self.source_counts,
            "resume_cursor": self.resume_cursor.to_dict() if self.resume_cursor is not None else None,
            "samples": [
                _trainer_batch_sample_to_dict(sample, include_sample_data=include_sample_data)
                for sample in self.samples
            ],
        }
        if self.worker_read_path_evidence is not None:
            payload["worker_read_path_evidence"] = dict(self.worker_read_path_evidence)
        return payload


@dataclass(frozen=True)
class CxnfsWeightedSourcePlan:
    name: str
    weight: int
    local_root: str
    remote_prefix: str | None
    sample_count: int
    shard_count: int
    byte_size: int
    shards: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("source name is required")
        if self.weight <= 0:
            raise ValueError("source weight must be positive")
        if not self.local_root:
            raise ValueError("local_root is required")
        if self.sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        if self.shard_count != len(self.shards):
            raise ValueError("shard_count must match shards")
        if self.byte_size < 0:
            raise ValueError("byte_size must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "weight": self.weight,
            "local_root": self.local_root,
            "remote_prefix": self.remote_prefix,
            "sample_count": self.sample_count,
            "shard_count": self.shard_count,
            "byte_size": self.byte_size,
            "shards": list(self.shards),
        }


@dataclass(frozen=True)
class CxnfsWeightedMdsPlan:
    root: str
    remote_root: str | None
    total_weight: int
    source_count: int
    total_samples: int
    total_bytes: int
    source_schedule: tuple[str, ...]
    sources: tuple[CxnfsWeightedSourcePlan, ...]

    def __post_init__(self) -> None:
        if not self.root:
            raise ValueError("root is required")
        if self.total_weight <= 0:
            raise ValueError("total_weight must be positive")
        if self.source_count != len(self.sources):
            raise ValueError("source_count must match sources")
        if self.total_samples < 0:
            raise ValueError("total_samples must be non-negative")
        if self.total_bytes < 0:
            raise ValueError("total_bytes must be non-negative")
        if len(self.source_schedule) != self.total_weight:
            raise ValueError("source_schedule length must match total_weight")

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "format": "cxnfs.weighted-mds-plan.v1",
            "root": self.root,
            "remote_root": self.remote_root,
            "source_count": self.source_count,
            "total_weight": self.total_weight,
            "total_samples": self.total_samples,
            "total_bytes": self.total_bytes,
            "source_schedule": list(self.source_schedule),
            "sources": [source.to_dict() for source in self.sources],
        }
        payload["workload_fingerprint"] = weighted_mds_plan_fingerprint(payload)
        return payload

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True) + "\n"


@dataclass(frozen=True)
class _WeightedChunkPlan:
    planned: tuple[tuple[int, int, int], ...]
    indices_by_source: tuple[tuple[int, ...], ...]
    estimated_payload_bytes: int = 0

    @property
    def sample_count(self) -> int:
        return len(self.planned)


@dataclass
class _WeightedIterationState:
    cursors: list[int]
    emitted: int
    chunk_index: int
    epoch: int
    sample_limit: int | None
    schedule_position: int = 0


class CxnfsWeightedTorchDataset:
    """Weighted, worker-sharded iterable facade over multiple MDS dataset roots."""

    def __init__(
        self,
        sources: Sequence[CxnfsWeightedSource],
        *,
        repeat: bool = True,
        rounds_per_read: int = 1,
        max_workers: int = 16,
        source_read_workers: int | None = None,
        prefetch_chunks: int = 1,
        max_prefetch_bytes: int | None = None,
        include_source_metadata: bool = False,
        shard: CxnfsTrainingShard | None = None,
        shuffle: bool = False,
        seed: int | None = None,
        samples_per_epoch: int | None = None,
    ) -> None:
        if not sources:
            raise ValueError("at least one weighted source is required")
        if rounds_per_read <= 0:
            raise ValueError("rounds_per_read must be positive")
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if source_read_workers is not None and source_read_workers <= 0:
            raise ValueError("source_read_workers must be positive")
        if prefetch_chunks <= 0:
            raise ValueError("prefetch_chunks must be positive")
        if max_prefetch_bytes is not None and max_prefetch_bytes <= 0:
            raise ValueError("max_prefetch_bytes must be positive")
        if samples_per_epoch is not None and samples_per_epoch <= 0:
            raise ValueError("samples_per_epoch must be positive")
        self.sources = tuple(sources)
        self.datasets = tuple(source.dataset() for source in self.sources)
        for source, dataset in zip(self.sources, self.datasets):
            if len(dataset) <= 0:
                raise ValueError(f"weighted source has no samples: {source.name}")
        self.repeat = repeat
        self.rounds_per_read = rounds_per_read
        self.max_workers = max_workers
        self.source_read_workers = source_read_workers
        self.prefetch_chunks = prefetch_chunks
        self.max_prefetch_bytes = max_prefetch_bytes
        self.include_source_metadata = include_source_metadata
        self.shard = shard or current_training_shard()
        self.shuffle = shuffle
        self.seed = 0 if seed is None else int(seed)
        self.samples_per_epoch = samples_per_epoch
        self.epoch = 0
        self._permutation_cache: dict[tuple[int, int, int], tuple[int, int]] = {}
        self._worker_read_path_evidence_snapshots: dict[str, dict[str, Any]] = {}
        self._prefetch_evidence_lock = threading.RLock()
        self._prefetch_plan_count = 0
        self._prefetch_delayed_plan_count = 0
        self._prefetch_oversized_plan_count = 0
        self._prefetch_estimated_payload_bytes = 0
        self._prefetch_max_plan_bytes = 0
        self._prefetch_max_observed_inflight_bytes = 0
        self._source_schedule = tuple(
            source_index
            for source_index, source in enumerate(self.sources)
            for _ in range(source.weight)
        )

    def __iter__(self):
        return self.iter_samples()

    def close(self) -> None:
        seen: set[int] = set()
        for dataset in self.datasets:
            _close_weighted_dataset(dataset, seen)

    def __enter__(self) -> CxnfsWeightedTorchDataset:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        state.pop("_prefetch_evidence_lock", None)
        return state

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self.__dict__.update(dict(state))
        self._prefetch_evidence_lock = threading.RLock()

    def set_epoch(self, epoch: int) -> None:
        if epoch < 0:
            raise ValueError("epoch must be non-negative")
        self.epoch = epoch

    def initial_trainer_cursor(
        self,
        *,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        shard: CxnfsTrainingShard | None = None,
    ) -> CxnfsTrainerCursor:
        active_shard = shard or self.shard
        active_epoch = self.epoch if epoch is None else epoch
        if active_epoch < 0:
            raise ValueError("epoch must be non-negative")
        sample_limit = self.samples_per_epoch if samples_per_epoch is None else samples_per_epoch
        if sample_limit is not None and sample_limit <= 0:
            raise ValueError("samples_per_epoch must be positive")
        return CxnfsTrainerCursor(
            epoch=active_epoch,
            cursors=tuple(0 for _ in self.sources),
            emitted=0,
            chunk_index=0,
            schedule_position=0,
            sample_limit=sample_limit,
            shard_index=active_shard.index,
            shard_count=active_shard.count,
            source_names=tuple(source.name for source in self.sources),
        )

    def advance_trainer_cursor(
        self,
        cursor: CxnfsTrainerCursor | Mapping[str, Any],
        sample_count: int,
        *,
        shard: CxnfsTrainingShard | None = None,
    ) -> CxnfsTrainerCursor:
        if sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        active_cursor = self._coerce_trainer_cursor(cursor)
        active_shard = shard or CxnfsTrainingShard(active_cursor.shard_index, active_cursor.shard_count)
        self._validate_trainer_cursor(active_cursor, active_shard)
        state = self._state_from_trainer_cursor(active_cursor)
        remaining = sample_count
        while remaining > 0:
            before = state.emitted
            plan = self._plan_chunk(state, active_shard, max_samples=remaining)
            if plan is None:
                break
            remaining -= state.emitted - before
        return self._cursor_from_state(state, active_shard)

    def iter_epoch(self, epoch: int, shard: CxnfsTrainingShard | None = None):
        return self.iter_samples(shard=shard, epoch=epoch)

    def iter_trainer_batches(
        self,
        batch_size: int,
        *,
        shard: CxnfsTrainingShard | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        active_shard, active_epoch, state, resume_cursor = self._trainer_iteration_context(
            cursor=cursor,
            shard=shard,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
        )
        batch: list[CxnfsTrainerBatchSample] = []
        batch_index = 0
        for sample in self._iter_trainer_samples(state, active_shard):
            batch.append(sample)
            if len(batch) == batch_size:
                resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
                yield CxnfsTrainerBatch(
                    batch_index,
                    active_epoch,
                    tuple(batch),
                    resume_cursor,
                    self._worker_batch_read_path_evidence(TrainerReadMode.SAMPLES),
                )
                batch_index += 1
                batch = []
        if batch and not drop_last:
            resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
            yield CxnfsTrainerBatch(
                batch_index,
                active_epoch,
                tuple(batch),
                resume_cursor,
                self._worker_batch_read_path_evidence(TrainerReadMode.SAMPLES),
            )

    def iter_trainer_direct_batches(
        self,
        batch_size: int,
        *,
        destination_factory: DirectDestinationFactory | None = None,
        shard: CxnfsTrainingShard | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        factory = destination_factory or _default_direct_destination_factory
        active_shard, active_epoch, state, resume_cursor = self._trainer_iteration_context(
            cursor=cursor,
            shard=shard,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
        )
        batch: list[CxnfsTrainerDirectBatchSample] = []
        batch_index = 0
        for sample in self._iter_trainer_direct_samples(state, active_shard, factory, include_paths):
            batch.append(sample)
            if len(batch) == batch_size:
                resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
                yield CxnfsTrainerBatch(
                    batch_index,
                    active_epoch,
                    tuple(batch),
                    resume_cursor,
                    self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_BUFFERS),
                )
                batch_index += 1
                batch = []
        if batch and not drop_last:
            resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
            yield CxnfsTrainerBatch(
                batch_index,
                active_epoch,
                tuple(batch),
                resume_cursor,
                self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_BUFFERS),
            )

    def iter_trainer_direct_tensor_batches(
        self,
        batch_size: int,
        *,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        shard: CxnfsTrainingShard | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        non_blocking: bool = True,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
        worker_cursors: Mapping[tuple[int, int], CxnfsTrainerCursor] | None = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        factory = _resolve_direct_tensor_factory(tensor_factory, tensor_allocation)
        active_shard, active_epoch, state, resume_cursor = self._trainer_iteration_context(
            cursor=cursor,
            shard=shard,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
        )
        batch: list[CxnfsTrainerDirectBatchSample] = []
        batch_index = 0
        for sample in self._iter_trainer_direct_tensor_samples(
            state,
            active_shard,
            factory,
            include_paths,
            non_blocking,
        ):
            batch.append(sample)
            if len(batch) == batch_size:
                resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
                yield CxnfsTrainerBatch(
                    batch_index,
                    active_epoch,
                    tuple(batch),
                    resume_cursor,
                    self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_TENSORS),
                )
                batch_index += 1
                batch = []
        if batch and not drop_last:
            resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
            yield CxnfsTrainerBatch(
                batch_index,
                active_epoch,
                tuple(batch),
                resume_cursor,
                self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_TENSORS),
            )

    def iter_trainer_pipeline_tensor_batches(
        self,
        batch_size: int,
        pipeline: CxnfsTrainingPipeline,
        *,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        shard: CxnfsTrainingShard | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        non_blocking: bool = True,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
        source_path_prefixes: Mapping[str, str | Path | None] | None = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if pipeline is None:
            raise ValueError("training pipeline is required")
        factory = _resolve_direct_tensor_factory(tensor_factory, tensor_allocation)
        active_shard, active_epoch, state, resume_cursor = self._trainer_iteration_context(
            cursor=cursor,
            shard=shard,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
        )
        batch: list[CxnfsTrainerPipelineBatchSample] = []
        batch_index = 0
        for sample in self._iter_trainer_pipeline_tensor_samples(
            state,
            active_shard,
            pipeline,
            factory,
            include_paths,
            non_blocking,
            source_path_prefixes,
        ):
            batch.append(sample)
            if len(batch) == batch_size:
                resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
                yield CxnfsTrainerBatch(
                    batch_index,
                    active_epoch,
                    tuple(batch),
                    resume_cursor,
                    self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_TENSORS),
                )
                batch_index += 1
                batch = []
        if batch and not drop_last:
            resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
            yield CxnfsTrainerBatch(
                batch_index,
                active_epoch,
                tuple(batch),
                resume_cursor,
                self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_TENSORS),
            )

    def iter_trainer_pipeline_tensor_batch_descriptors(
        self,
        batch_size: int,
        *,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        shard: CxnfsTrainingShard | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
        source_path_prefixes: Mapping[str, str | Path | None] | None = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        factory = _resolve_direct_tensor_factory(tensor_factory, tensor_allocation)
        active_shard, active_epoch, state, resume_cursor = self._trainer_iteration_context(
            cursor=cursor,
            shard=shard,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
        )
        batch: list[CxnfsTrainerPipelineBatchSample] = []
        batch_index = 0
        for sample in self._iter_trainer_pipeline_tensor_descriptor_samples(
            state,
            active_shard,
            factory,
            include_paths,
            source_path_prefixes,
        ):
            batch.append(sample)
            if len(batch) == batch_size:
                resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
                yield CxnfsTrainerBatch(
                    batch_index,
                    active_epoch,
                    tuple(batch),
                    resume_cursor,
                    self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_TENSORS),
                )
                batch_index += 1
                batch = []
        if batch and not drop_last:
            resume_cursor = self.advance_trainer_cursor(resume_cursor, len(batch), shard=active_shard)
            yield CxnfsTrainerBatch(
                batch_index,
                active_epoch,
                tuple(batch),
                resume_cursor,
                self._worker_batch_read_path_evidence(TrainerReadMode.DIRECT_TENSORS),
            )

    def iter_trainer_read_batches(
        self,
        batch_size: int,
        *,
        read_mode: TrainerReadMode | str = TrainerReadMode.DIRECT_BUFFERS,
        destination_factory: DirectDestinationFactory | None = None,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        shard: CxnfsTrainingShard | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        non_blocking: bool = True,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
    ):
        mode = TrainerReadMode(read_mode)
        if mode == TrainerReadMode.SAMPLES:
            return self.iter_trainer_batches(
                batch_size,
                shard=shard,
                epoch=epoch,
                samples_per_epoch=samples_per_epoch,
                drop_last=drop_last,
                cursor=cursor,
            )
        if mode == TrainerReadMode.DIRECT_BUFFERS:
            return self.iter_trainer_direct_batches(
                batch_size,
                destination_factory=destination_factory,
                shard=shard,
                epoch=epoch,
                samples_per_epoch=samples_per_epoch,
                drop_last=drop_last,
                include_paths=include_paths,
                cursor=cursor,
            )
        return self.iter_trainer_direct_tensor_batches(
            batch_size,
            tensor_factory=tensor_factory,
            tensor_allocation=tensor_allocation,
            shard=shard,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
            drop_last=drop_last,
            include_paths=include_paths,
            non_blocking=non_blocking,
            cursor=cursor,
        )

    def to_trainer_batch_dataset(
        self,
        batch_size: int,
        *,
        read_mode: TrainerReadMode | str = TrainerReadMode.DIRECT_BUFFERS,
        destination_factory: DirectDestinationFactory | None = None,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        non_blocking: bool = True,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
        worker_cursors: Mapping[tuple[int, int], CxnfsTrainerCursor] | None = None,
    ):
        _require_torch()
        return _CxnfsTrainerBatchIterableDataset(
            self,
            batch_size,
            read_mode=read_mode,
            destination_factory=destination_factory,
            tensor_factory=tensor_factory,
            tensor_allocation=tensor_allocation,
            samples_per_epoch=samples_per_epoch,
            drop_last=drop_last,
            include_paths=include_paths,
            non_blocking=non_blocking,
            cursor=cursor,
            worker_cursors=worker_cursors,
        )

    def to_trainer_dataloader(
        self,
        batch_size: int,
        *,
        read_mode: TrainerReadMode | str = TrainerReadMode.DIRECT_BUFFERS,
        destination_factory: DirectDestinationFactory | None = None,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        non_blocking: bool = True,
        data_loader_kwargs: Mapping[str, Any] | None = None,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
        worker_cursors: Mapping[tuple[int, int], CxnfsTrainerCursor] | None = None,
    ):
        torch = _require_torch()
        options = dict(data_loader_kwargs or {})
        if "batch_size" in options:
            raise ValueError("trainer DataLoader batch_size is controlled by CXNFS")
        if "drop_last" in options:
            raise ValueError("trainer DataLoader drop_last is controlled by CXNFS")
        collate_fn = options.pop("collate_fn", _identity_collate)
        return torch.utils.data.DataLoader(
            self.to_trainer_batch_dataset(
                batch_size,
                read_mode=read_mode,
                destination_factory=destination_factory,
                tensor_factory=tensor_factory,
                tensor_allocation=tensor_allocation,
                samples_per_epoch=samples_per_epoch,
                drop_last=drop_last,
                include_paths=include_paths,
                non_blocking=non_blocking,
                cursor=cursor,
                worker_cursors=worker_cursors,
            ),
            batch_size=None,
            collate_fn=collate_fn,
            **options,
        )

    def iter_samples(
        self,
        shard: CxnfsTrainingShard | None = None,
        *,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
    ):
        active_shard = shard or self.shard
        active_epoch = self.epoch if epoch is None else epoch
        if active_epoch < 0:
            raise ValueError("epoch must be non-negative")
        sample_limit = self.samples_per_epoch if samples_per_epoch is None else samples_per_epoch
        if sample_limit is not None and sample_limit <= 0:
            raise ValueError("samples_per_epoch must be positive")
        state = _WeightedIterationState(
            cursors=[0] * len(self.sources),
            emitted=0,
            chunk_index=0,
            epoch=active_epoch,
            sample_limit=sample_limit,
        )
        if self.prefetch_chunks > 1:
            yield from self._iter_prefetched_samples(state, active_shard)
            return
        while True:
            chunk = self._read_chunk(state, active_shard)
            if not chunk:
                return
            yield from chunk

    def _trainer_iteration_context(
        self,
        *,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None,
        shard: CxnfsTrainingShard | None,
        epoch: int | None,
        samples_per_epoch: int | None,
    ) -> tuple[CxnfsTrainingShard, int, _WeightedIterationState, CxnfsTrainerCursor]:
        active_shard = shard or self.shard
        sample_limit = self.samples_per_epoch if samples_per_epoch is None else samples_per_epoch
        if sample_limit is not None and sample_limit <= 0:
            raise ValueError("samples_per_epoch must be positive")
        if cursor is not None:
            active_cursor = self._coerce_trainer_cursor(cursor)
            self._validate_trainer_cursor(active_cursor, active_shard)
            if epoch is not None and active_cursor.epoch != epoch:
                raise ValueError("cursor epoch does not match requested epoch")
            if (samples_per_epoch is not None or self.samples_per_epoch is not None) and sample_limit != active_cursor.sample_limit:
                raise ValueError("cursor sample limit does not match requested samples_per_epoch")
            return (
                active_shard,
                active_cursor.epoch,
                self._state_from_trainer_cursor(active_cursor),
                active_cursor,
            )
        active_epoch = self.epoch if epoch is None else epoch
        if active_epoch < 0:
            raise ValueError("epoch must be non-negative")
        active_cursor = self.initial_trainer_cursor(
            epoch=active_epoch,
            samples_per_epoch=sample_limit,
            shard=active_shard,
        )
        return (
            active_shard,
            active_epoch,
            self._state_from_trainer_cursor(active_cursor),
            active_cursor,
        )

    def _coerce_trainer_cursor(self, cursor: CxnfsTrainerCursor | Mapping[str, Any]) -> CxnfsTrainerCursor:
        if isinstance(cursor, CxnfsTrainerCursor):
            return cursor
        if isinstance(cursor, Mapping):
            return CxnfsTrainerCursor.from_mapping(cursor)
        raise TypeError("cursor must be a CxnfsTrainerCursor or mapping")

    def _validate_trainer_cursor(self, cursor: CxnfsTrainerCursor, shard: CxnfsTrainingShard) -> None:
        if cursor.source_count != len(self.sources):
            raise ValueError("cursor source count does not match dataset")
        expected_names = tuple(source.name for source in self.sources)
        if cursor.source_names and cursor.source_names != expected_names:
            raise ValueError("cursor source names do not match dataset")
        if cursor.shard_index != shard.index or cursor.shard_count != shard.count:
            raise ValueError("cursor training shard does not match requested shard")
        if cursor.schedule_position > len(self._source_schedule):
            raise ValueError("cursor schedule position is outside the weighted schedule")

    def _state_from_trainer_cursor(self, cursor: CxnfsTrainerCursor) -> _WeightedIterationState:
        return _WeightedIterationState(
            cursors=list(cursor.cursors),
            emitted=cursor.emitted,
            chunk_index=cursor.chunk_index,
            epoch=cursor.epoch,
            sample_limit=cursor.sample_limit,
            schedule_position=cursor.schedule_position,
        )

    def _cursor_from_state(self, state: _WeightedIterationState, shard: CxnfsTrainingShard) -> CxnfsTrainerCursor:
        return CxnfsTrainerCursor(
            epoch=state.epoch,
            cursors=tuple(state.cursors),
            emitted=state.emitted,
            chunk_index=state.chunk_index,
            schedule_position=state.schedule_position,
            sample_limit=state.sample_limit,
            shard_index=shard.index,
            shard_count=shard.count,
            source_names=tuple(source.name for source in self.sources),
        )

    def to_torch_dataset(self):
        _require_torch()
        return _CxnfsWeightedIterableDataset(self)

    def _read_chunk(self, state: _WeightedIterationState, shard: CxnfsTrainingShard) -> tuple[Any, ...]:
        plan = self._plan_chunk(state, shard)
        if plan is None:
            return tuple()
        return self._read_planned_chunk(plan)

    def _iter_prefetched_samples(self, state: _WeightedIterationState, shard: CxnfsTrainingShard):
        yield from self._iter_prefetched_chunks(
            state,
            shard,
            self._read_planned_chunk,
        )

    def _plan_chunk(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        *,
        max_samples: int | None = None,
    ) -> _WeightedChunkPlan | None:
        if max_samples is not None and max_samples <= 0:
            return None
        planned: list[tuple[int, int, int]] = []
        indices_by_source: list[list[int]] = [[] for _ in self.sources]
        rounds_started = 0
        while rounds_started < self.rounds_per_read:
            schedule = self._source_schedule_for_chunk(state.epoch, state.chunk_index)
            while state.schedule_position < len(schedule):
                if state.sample_limit is not None and state.emitted >= state.sample_limit:
                    break
                if max_samples is not None and len(planned) >= max_samples:
                    break
                source_index = schedule[state.schedule_position]
                state.schedule_position += 1
                sample_index = self._next_sample_index(source_index, state, shard)
                if sample_index is None:
                    continue
                position_in_source_batch = len(indices_by_source[source_index])
                indices_by_source[source_index].append(sample_index)
                planned.append((source_index, position_in_source_batch, sample_index))
                state.emitted += 1
                if state.schedule_position >= len(schedule) and (
                    (max_samples is not None and len(planned) >= max_samples)
                    or (state.sample_limit is not None and state.emitted >= state.sample_limit)
                ):
                    state.schedule_position = 0
                    state.chunk_index += 1
            if state.sample_limit is not None and state.emitted >= state.sample_limit:
                break
            if max_samples is not None and len(planned) >= max_samples:
                break
            if state.schedule_position >= len(schedule):
                state.schedule_position = 0
                state.chunk_index += 1
                rounds_started += 1
                continue
            state.chunk_index += 1
            state.schedule_position = 0
            rounds_started += 1
        if not planned:
            return None
        indices_tuple = tuple(tuple(indices) for indices in indices_by_source)

        return _WeightedChunkPlan(
            tuple(planned),
            indices_tuple,
            self._estimate_chunk_payload_bytes(indices_tuple)
            if self.max_prefetch_bytes is not None
            else 0,
        )

    def _read_planned_chunk(self, plan: _WeightedChunkPlan) -> tuple[Any, ...]:
        batches = self._read_source_batches([list(indices) for indices in plan.indices_by_source])
        results = []
        for source_index, position_in_source_batch, sample_index in plan.planned:
            value = batches[source_index][position_in_source_batch]
            if self.include_source_metadata:
                results.append(CxnfsWeightedSample(
                    self.sources[source_index].name,
                    source_index,
                    sample_index,
                    value,
                ))
            else:
                results.append(value)
        return tuple(results)

    def _read_trainer_chunk(self, plan: _WeightedChunkPlan) -> tuple[CxnfsTrainerBatchSample, ...]:
        batches = self._read_source_batches([list(indices) for indices in plan.indices_by_source])
        results = []
        for source_index, position_in_source_batch, sample_index in plan.planned:
            results.append(CxnfsTrainerBatchSample(
                self.sources[source_index].name,
                source_index,
                sample_index,
                batches[source_index][position_in_source_batch],
            ))
        return tuple(results)

    def _iter_trainer_samples(self, state: _WeightedIterationState, shard: CxnfsTrainingShard):
        if self.prefetch_chunks > 1:
            yield from self._iter_prefetched_trainer_samples(state, shard)
            return
        while True:
            plan = self._plan_chunk(state, shard)
            if plan is None:
                return
            yield from self._read_trainer_chunk(plan)

    def _iter_trainer_direct_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        destination_factory: DirectDestinationFactory,
        include_paths: bool,
    ):
        if self.prefetch_chunks > 1:
            yield from self._iter_prefetched_trainer_direct_samples(
                state,
                shard,
                destination_factory,
                include_paths,
            )
            return
        while True:
            plan = self._plan_chunk(state, shard)
            if plan is None:
                return
            yield from self._read_trainer_direct_chunk(plan, destination_factory, include_paths)

    def _iter_trainer_direct_tensor_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
    ):
        if self.prefetch_chunks > 1:
            yield from self._iter_prefetched_trainer_direct_tensor_samples(
                state,
                shard,
                tensor_factory,
                include_paths,
                non_blocking,
            )
            return
        while True:
            plan = self._plan_chunk(state, shard)
            if plan is None:
                return
            yield from self._read_trainer_direct_tensor_chunk(
                plan,
                tensor_factory,
                include_paths,
                non_blocking,
            )

    def _iter_trainer_pipeline_tensor_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        pipeline: CxnfsTrainingPipeline,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
        source_path_prefixes: Mapping[str, str | Path | None] | None,
    ):
        if self.prefetch_chunks > 1:
            yield from self._iter_prefetched_trainer_pipeline_tensor_samples(
                state,
                shard,
                pipeline,
                tensor_factory,
                include_paths,
                non_blocking,
                source_path_prefixes,
            )
            return
        while True:
            plan = self._plan_chunk(state, shard)
            if plan is None:
                return
            yield from self._read_trainer_pipeline_tensor_chunk(
                plan,
                pipeline,
                tensor_factory,
                include_paths,
                non_blocking,
                source_path_prefixes,
            )

    def _iter_trainer_pipeline_tensor_descriptor_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        source_path_prefixes: Mapping[str, str | Path | None] | None,
    ):
        while True:
            plan = self._plan_chunk(state, shard)
            if plan is None:
                return
            yield from self._plan_trainer_pipeline_tensor_chunk(
                plan,
                tensor_factory,
                include_paths,
                source_path_prefixes,
            )

    def _iter_prefetched_trainer_samples(self, state: _WeightedIterationState, shard: CxnfsTrainingShard):
        yield from self._iter_prefetched_chunks(
            state,
            shard,
            self._read_trainer_chunk,
        )

    def _iter_prefetched_trainer_direct_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        destination_factory: DirectDestinationFactory,
        include_paths: bool,
    ):
        yield from self._iter_prefetched_chunks(
            state,
            shard,
            lambda plan: self._read_trainer_direct_chunk(plan, destination_factory, include_paths),
        )

    def _iter_prefetched_trainer_direct_tensor_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
    ):
        yield from self._iter_prefetched_chunks(
            state,
            shard,
            lambda plan: self._read_trainer_direct_tensor_chunk(
                plan,
                tensor_factory,
                include_paths,
                non_blocking,
            ),
        )

    def _iter_prefetched_trainer_pipeline_tensor_samples(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        pipeline: CxnfsTrainingPipeline,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
        source_path_prefixes: Mapping[str, str | Path | None] | None,
    ):
        yield from self._iter_prefetched_chunks(
            state,
            shard,
            lambda plan: self._read_trainer_pipeline_tensor_chunk(
                plan,
                pipeline,
                tensor_factory,
                include_paths,
                non_blocking,
                source_path_prefixes,
            ),
        )

    def _iter_prefetched_chunks(
        self,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
        reader: Callable[[_WeightedChunkPlan], tuple[Any, ...]],
    ):
        pending = deque()
        next_plan: _WeightedChunkPlan | None = None
        in_flight_bytes = 0
        with ThreadPoolExecutor(max_workers=self.prefetch_chunks) as executor:
            def load_next_plan() -> _WeightedChunkPlan | None:
                nonlocal next_plan
                if next_plan is None:
                    next_plan = self._plan_chunk(state, shard)
                return next_plan

            def schedule_next() -> bool:
                nonlocal next_plan, in_flight_bytes
                plan = load_next_plan()
                if plan is None:
                    return False
                if not self._can_admit_prefetch_plan(plan, in_flight_bytes, bool(pending)):
                    self._record_prefetch_delay(plan)
                    return False
                future = executor.submit(reader, plan)
                pending.append((future, plan.estimated_payload_bytes))
                in_flight_bytes += plan.estimated_payload_bytes
                self._record_prefetch_admission(plan, in_flight_bytes)
                next_plan = None
                return True

            while len(pending) < self.prefetch_chunks and schedule_next():
                pass

            while pending:
                future, estimated_bytes = pending.popleft()
                try:
                    chunk = future.result()
                finally:
                    in_flight_bytes = max(0, in_flight_bytes - estimated_bytes)
                while len(pending) < self.prefetch_chunks and schedule_next():
                    pass
                yield from chunk

    def _can_admit_prefetch_plan(
        self,
        plan: _WeightedChunkPlan,
        in_flight_bytes: int,
        has_pending: bool,
    ) -> bool:
        if self.max_prefetch_bytes is None:
            return True
        if not has_pending:
            return True
        return in_flight_bytes + plan.estimated_payload_bytes <= self.max_prefetch_bytes

    def _estimate_chunk_payload_bytes(self, indices_by_source: tuple[tuple[int, ...], ...]) -> int:
        total = 0
        for source_index, indices in enumerate(indices_by_source):
            if not indices:
                continue
            dataset = self.datasets[source_index]
            read_spans = getattr(getattr(dataset, "mds", None), "read_spans", None)
            if callable(read_spans):
                try:
                    total += sum(int(span.length) for span in read_spans(indices))
                    continue
                except Exception:
                    pass
            total += len(indices)
        return max(0, total)

    def _record_prefetch_admission(self, plan: _WeightedChunkPlan, in_flight_bytes: int) -> None:
        with self._prefetch_evidence_lock:
            self._prefetch_plan_count += 1
            self._prefetch_estimated_payload_bytes += plan.estimated_payload_bytes
            self._prefetch_max_plan_bytes = max(
                self._prefetch_max_plan_bytes,
                plan.estimated_payload_bytes,
            )
            self._prefetch_max_observed_inflight_bytes = max(
                self._prefetch_max_observed_inflight_bytes,
                in_flight_bytes,
            )
            if (
                self.max_prefetch_bytes is not None
                and plan.estimated_payload_bytes > self.max_prefetch_bytes
            ):
                self._prefetch_oversized_plan_count += 1

    def _record_prefetch_delay(self, plan: _WeightedChunkPlan) -> None:
        del plan
        with self._prefetch_evidence_lock:
            self._prefetch_delayed_plan_count += 1

    def prefetch_admission_evidence(self) -> dict[str, Any]:
        with self._prefetch_evidence_lock:
            plan_count = self._prefetch_plan_count
            delayed_plan_count = self._prefetch_delayed_plan_count
            oversized_plan_count = self._prefetch_oversized_plan_count
            estimated_payload_bytes = self._prefetch_estimated_payload_bytes
            max_plan_bytes = self._prefetch_max_plan_bytes
            max_observed_inflight_bytes = self._prefetch_max_observed_inflight_bytes
        bounded = self.max_prefetch_bytes is not None
        return {
            "format": "cxnfs.weighted-prefetch-admission.v1",
            "chunk_window": self.prefetch_chunks,
            "max_prefetch_bytes": self.max_prefetch_bytes,
            "bounded": bounded,
            "plan_count": plan_count,
            "delayed_plan_count": delayed_plan_count,
            "oversized_plan_count": oversized_plan_count,
            "estimated_payload_bytes": estimated_payload_bytes,
            "max_plan_bytes": max_plan_bytes,
            "max_observed_inflight_bytes": max_observed_inflight_bytes,
            "observed": plan_count > 0,
            "byte_window_engaged": delayed_plan_count > 0,
            "byte_window_respected": (
                not bounded
                or max_observed_inflight_bytes <= int(self.max_prefetch_bytes or 0)
                or oversized_plan_count > 0
            ),
        }

    def _read_trainer_direct_chunk(
        self,
        plan: _WeightedChunkPlan,
        destination_factory: DirectDestinationFactory,
        include_paths: bool,
    ) -> tuple[CxnfsTrainerDirectBatchSample, ...]:
        batches = self._read_source_direct_batches(
            [list(indices) for indices in plan.indices_by_source],
            destination_factory,
            include_paths,
        )
        results = []
        for source_index, position_in_source_batch, sample_index in plan.planned:
            direct = batches[source_index][position_in_source_batch]
            results.append(CxnfsTrainerDirectBatchSample(
                self.sources[source_index].name,
                source_index,
                sample_index,
                direct.destination,
                direct.metadata,
            ))
        return tuple(results)

    def _read_trainer_direct_tensor_chunk(
        self,
        plan: _WeightedChunkPlan,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
    ) -> tuple[CxnfsTrainerDirectBatchSample, ...]:
        batches = self._read_source_direct_tensor_batches(
            [list(indices) for indices in plan.indices_by_source],
            tensor_factory,
            include_paths,
            non_blocking,
        )
        results = []
        for source_index, position_in_source_batch, sample_index in plan.planned:
            direct = batches[source_index][position_in_source_batch]
            results.append(CxnfsTrainerDirectBatchSample(
                self.sources[source_index].name,
                source_index,
                sample_index,
                direct.destination,
                direct.metadata,
            ))
        return tuple(results)

    def _read_trainer_pipeline_tensor_chunk(
        self,
        plan: _WeightedChunkPlan,
        pipeline: CxnfsTrainingPipeline,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
        source_path_prefixes: Mapping[str, str | Path | None] | None,
    ) -> tuple[CxnfsTrainerPipelineBatchSample, ...]:
        results = self._plan_trainer_pipeline_tensor_chunk(
            plan,
            tensor_factory,
            include_paths,
            source_path_prefixes,
        )
        if results:
            try:
                pipeline.read_into_tensors(
                    tuple(sample.read for sample in results),
                    tuple(sample.destination for sample in results),
                    non_blocking=non_blocking,
                )
            except Exception as exc:
                _raise_trainer_surface_error(
                    exc,
                    "trainer pipeline tensor read",
                    **_chunk_failure_context(
                        self.sources,
                        plan,
                        TrainerReadMode.DIRECT_TENSORS,
                        prefetch_chunks=self.prefetch_chunks,
                        max_prefetch_bytes=self.max_prefetch_bytes,
                    ),
                )
        return results

    def _plan_trainer_pipeline_tensor_chunk(
        self,
        plan: _WeightedChunkPlan,
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        source_path_prefixes: Mapping[str, str | Path | None] | None,
    ) -> tuple[CxnfsTrainerPipelineBatchSample, ...]:
        batches = self._plan_source_pipeline_tensor_batches(
            [list(indices) for indices in plan.indices_by_source],
            tensor_factory,
            include_paths,
            source_path_prefixes,
        )
        results = []
        for source_index, position_in_source_batch, _sample_index in plan.planned:
            results.append(batches[source_index][position_in_source_batch])
        return tuple(results)

    def _read_source_batches(
        self,
        indices_by_source: list[list[int]],
        read_mode: TrainerReadMode | str = TrainerReadMode.SAMPLES,
    ) -> list[tuple[Any, ...]]:
        batches: list[tuple[Any, ...]] = [tuple() for _ in self.sources]
        active = tuple(
            (source_index, tuple(indices))
            for source_index, indices in enumerate(indices_by_source)
            if indices
        )
        if not active:
            return batches
        if len(active) == 1:
            source_index, indices = active[0]
            try:
                batches[source_index] = self.datasets[source_index].read_batch(
                    indices,
                    max_workers=self.max_workers,
                )
            except Exception as exc:
                _raise_trainer_surface_error(
                    exc,
                    "weighted source batch read",
                    **_source_failure_context(self.sources, source_index, indices, read_mode),
                )
            return batches

        worker_count = min(len(active), self.source_read_workers or len(active))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            submitted = tuple(
                (
                    source_index,
                    executor.submit(
                        self.datasets[source_index].read_batch,
                        indices,
                        max_workers=self.max_workers,
                    ),
                )
                for source_index, indices in active
            )
            for source_index, future in submitted:
                indices = next(indices for active_source, indices in active if active_source == source_index)
                try:
                    batches[source_index] = future.result()
                except Exception as exc:
                    _raise_trainer_surface_error(
                        exc,
                        "weighted source batch read",
                        **_source_failure_context(self.sources, source_index, indices, read_mode),
                    )
        return batches

    def _worker_batch_read_path_evidence(self, read_mode: TrainerReadMode) -> dict[str, Any] | None:
        worker_info = _torch_worker_info()
        if worker_info is None:
            return None
        current = _weighted_dataset_read_path_evidence(self, read_mode)
        key = read_mode.value
        previous = self._worker_read_path_evidence_snapshots.get(key)
        self._worker_read_path_evidence_snapshots[key] = dict(current)
        evidence = _read_path_evidence_delta(current, previous)
        evidence.update({
            "produced_inside_torch_worker": True,
            "worker_id": int(worker_info.id),
            "worker_count": int(worker_info.num_workers),
        })
        return evidence

    def _read_source_direct_batches(
        self,
        indices_by_source: list[list[int]],
        destination_factory: DirectDestinationFactory,
        include_paths: bool,
    ) -> list[tuple[MdsDirectBufferSample, ...]]:
        batches: list[tuple[MdsDirectBufferSample, ...]] = [tuple() for _ in self.sources]
        active = tuple(
            (source_index, tuple(indices))
            for source_index, indices in enumerate(indices_by_source)
            if indices
        )
        if not active:
            return batches

        def read_source(source_index: int, indices: tuple[int, ...]) -> tuple[MdsDirectBufferSample, ...]:
            def make_destination(location, data_length: int):
                return destination_factory(
                    self.sources[source_index].name,
                    source_index,
                    location.global_index,
                    data_length,
                )

            return self.datasets[source_index].read_data_with_buffer_factory(
                indices,
                make_destination,
                max_workers=self.max_workers,
                include_paths=include_paths,
            )

        if len(active) == 1:
            source_index, indices = active[0]
            try:
                batches[source_index] = read_source(source_index, indices)
            except Exception as exc:
                _raise_trainer_surface_error(
                    exc,
                    "weighted direct buffer read",
                    **_source_failure_context(
                        self.sources,
                        source_index,
                        indices,
                        TrainerReadMode.DIRECT_BUFFERS,
                    ),
                )
            return batches

        worker_count = min(len(active), self.source_read_workers or len(active))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            submitted = tuple(
                (
                    source_index,
                    executor.submit(read_source, source_index, indices),
                )
                for source_index, indices in active
            )
            for source_index, future in submitted:
                indices = next(indices for active_source, indices in active if active_source == source_index)
                try:
                    batches[source_index] = future.result()
                except Exception as exc:
                    _raise_trainer_surface_error(
                        exc,
                        "weighted direct buffer read",
                        **_source_failure_context(
                            self.sources,
                            source_index,
                            indices,
                            TrainerReadMode.DIRECT_BUFFERS,
                        ),
                    )
        return batches

    def _read_source_direct_tensor_batches(
        self,
        indices_by_source: list[list[int]],
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        non_blocking: bool,
    ) -> list[tuple[MdsDirectTensorSample, ...]]:
        batches: list[tuple[MdsDirectTensorSample, ...]] = [tuple() for _ in self.sources]
        active = tuple(
            (source_index, tuple(indices))
            for source_index, indices in enumerate(indices_by_source)
            if indices
        )
        if not active:
            return batches

        def read_source(source_index: int, indices: tuple[int, ...]) -> tuple[MdsDirectTensorSample, ...]:
            def make_tensor(location, data_length: int):
                return tensor_factory(
                    self.sources[source_index].name,
                    source_index,
                    location.global_index,
                    data_length,
                )

            return self.datasets[source_index].read_data_with_tensor_factory(
                indices,
                make_tensor,
                max_workers=self.max_workers,
                include_paths=include_paths,
                non_blocking=non_blocking,
            )

        if len(active) == 1:
            source_index, indices = active[0]
            try:
                batches[source_index] = read_source(source_index, indices)
            except Exception as exc:
                _raise_trainer_surface_error(
                    exc,
                    "weighted direct tensor read",
                    **_source_failure_context(
                        self.sources,
                        source_index,
                        indices,
                        TrainerReadMode.DIRECT_TENSORS,
                    ),
                )
            return batches

        worker_count = min(len(active), self.source_read_workers or len(active))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            submitted = tuple(
                (
                    source_index,
                    executor.submit(read_source, source_index, indices),
                )
                for source_index, indices in active
            )
            for source_index, future in submitted:
                indices = next(indices for active_source, indices in active if active_source == source_index)
                try:
                    batches[source_index] = future.result()
                except Exception as exc:
                    _raise_trainer_surface_error(
                        exc,
                        "weighted direct tensor read",
                        **_source_failure_context(
                            self.sources,
                            source_index,
                            indices,
                            TrainerReadMode.DIRECT_TENSORS,
                        ),
                    )
        return batches

    def _plan_source_pipeline_tensor_batches(
        self,
        indices_by_source: list[list[int]],
        tensor_factory: DirectTensorFactory,
        include_paths: bool,
        source_path_prefixes: Mapping[str, str | Path | None] | None,
    ) -> list[tuple[CxnfsTrainerPipelineBatchSample, ...]]:
        batches: list[tuple[CxnfsTrainerPipelineBatchSample, ...]] = [tuple() for _ in self.sources]
        active = tuple(
            (source_index, tuple(indices))
            for source_index, indices in enumerate(indices_by_source)
            if indices
        )
        if not active:
            return batches

        def plan_source_reads(source_index: int, indices: tuple[int, ...]):
            return self.datasets[source_index].plan_data_reads(
                indices,
                max_workers=self.max_workers,
                include_paths=include_paths,
            )

        def materialize_source(source_index: int, read_plans) -> tuple[CxnfsTrainerPipelineBatchSample, ...]:
            source = self.sources[source_index]
            planned = []
            for read_plan in read_plans:
                destination = tensor_factory(
                    source.name,
                    source_index,
                    read_plan.global_index,
                    read_plan.data_length,
                )
                shard_path = _pipeline_shard_path(source.name, read_plan.shard_name, source_path_prefixes)
                planned.append(CxnfsTrainerPipelineBatchSample(
                    source.name,
                    source_index,
                    read_plan.global_index,
                    destination,
                    SampleRead(
                        shard_path,
                        read_plan.data_offset,
                        read_plan.data_length,
                        f"{source.name}:{read_plan.global_index}",
                    ),
                    read_plan.path,
                ))
            return tuple(planned)

        if len(active) == 1:
            source_index, indices = active[0]
            try:
                batches[source_index] = materialize_source(source_index, plan_source_reads(source_index, indices))
            except Exception as exc:
                _raise_trainer_surface_error(
                    exc,
                    "weighted pipeline tensor plan",
                    **_source_failure_context(
                        self.sources,
                        source_index,
                        indices,
                        TrainerReadMode.DIRECT_TENSORS,
                    ),
                )
            return batches

        worker_count = min(len(active), self.source_read_workers or len(active))
        read_plans_by_source: dict[int, Any] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            submitted = tuple(
                (
                    source_index,
                    executor.submit(plan_source_reads, source_index, indices),
                )
                for source_index, indices in active
            )
            for source_index, future in submitted:
                indices = next(indices for active_source, indices in active if active_source == source_index)
                try:
                    read_plans_by_source[source_index] = future.result()
                except Exception as exc:
                    _raise_trainer_surface_error(
                        exc,
                        "weighted pipeline tensor plan",
                        **_source_failure_context(
                            self.sources,
                            source_index,
                            indices,
                            TrainerReadMode.DIRECT_TENSORS,
                        ),
                    )
        for source_index, _indices in active:
            try:
                batches[source_index] = materialize_source(source_index, read_plans_by_source[source_index])
            except Exception as exc:
                _raise_trainer_surface_error(
                    exc,
                    "weighted pipeline tensor materialize",
                    **_source_failure_context(
                        self.sources,
                        source_index,
                        _indices,
                        TrainerReadMode.DIRECT_TENSORS,
                    ),
                )
        return batches

    def _next_sample_index(
        self,
        source_index: int,
        state: _WeightedIterationState,
        shard: CxnfsTrainingShard,
    ) -> int | None:
        dataset_size = len(self.datasets[source_index])
        raw_index = shard.index + (state.cursors[source_index] * shard.count)
        state.cursors[source_index] += 1
        if self.repeat:
            return self._map_source_index(source_index, raw_index % dataset_size, dataset_size, state.epoch)
        if raw_index >= dataset_size:
            return None
        return self._map_source_index(source_index, raw_index, dataset_size, state.epoch)

    def _source_schedule_for_chunk(self, epoch: int, chunk_index: int) -> tuple[int, ...]:
        if not self.shuffle or len(self._source_schedule) <= 1:
            return self._source_schedule
        schedule = list(self._source_schedule)
        random.Random(f"{self.seed}:source-schedule:{epoch}:{chunk_index}").shuffle(schedule)
        return tuple(schedule)

    def _map_source_index(self, source_index: int, position: int, dataset_size: int, epoch: int) -> int:
        if not self.shuffle or dataset_size <= 1:
            return position
        offset, stride = self._source_permutation(source_index, dataset_size, epoch)
        return (offset + (position * stride)) % dataset_size

    def _source_permutation(self, source_index: int, dataset_size: int, epoch: int) -> tuple[int, int]:
        key = (source_index, dataset_size, epoch)
        cached = self._permutation_cache.get(key)
        if cached is not None:
            return cached

        rng = random.Random(f"{self.seed}:source:{self.sources[source_index].name}:{source_index}:{dataset_size}:{epoch}")
        offset = rng.randrange(dataset_size)
        stride = 1 if dataset_size == 1 else rng.randrange(1, dataset_size)
        while math.gcd(stride, dataset_size) != 1:
            stride += 1
            if stride >= dataset_size:
                stride = 1
        if dataset_size > 1 and offset == 0 and stride == 1:
            offset = 1
        value = (offset, stride)
        self._permutation_cache[key] = value
        return value


class _CxnfsWeightedIterableDataset(_TorchIterableDatasetBase):
    def __init__(self, parent: CxnfsWeightedTorchDataset) -> None:
        self.parent = parent

    def __iter__(self):
        return self.parent.iter_samples(_torch_training_shard(self.parent.shard))

    def set_epoch(self, epoch: int) -> None:
        self.parent.set_epoch(epoch)


class _CxnfsTrainerBatchIterableDataset(_TorchIterableDatasetBase):
    def __init__(
        self,
        parent: CxnfsWeightedTorchDataset,
        batch_size: int,
        *,
        read_mode: TrainerReadMode | str,
        destination_factory: DirectDestinationFactory | None,
        tensor_factory: DirectTensorFactory | None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None,
        samples_per_epoch: int | None,
        drop_last: bool,
        include_paths: bool,
        non_blocking: bool,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None,
        worker_cursors: Mapping[tuple[int, int], CxnfsTrainerCursor] | None = None,
    ) -> None:
        self.parent = parent
        self.batch_size = batch_size
        self.read_mode = read_mode
        self.destination_factory = destination_factory
        self.tensor_factory = tensor_factory
        self.tensor_allocation = tensor_allocation
        self.samples_per_epoch = samples_per_epoch
        self.drop_last = drop_last
        self.include_paths = include_paths
        self.non_blocking = non_blocking
        self.cursor = cursor
        self.worker_cursors = dict(worker_cursors or {})

    def __iter__(self):
        worker_shard = _torch_training_shard(self.parent.shard)
        return self.parent.iter_trainer_read_batches(
            self.batch_size,
            read_mode=self.read_mode,
            destination_factory=self.destination_factory,
            tensor_factory=self.tensor_factory,
            tensor_allocation=self.tensor_allocation,
            shard=worker_shard,
            samples_per_epoch=self.samples_per_epoch,
            drop_last=self.drop_last,
            include_paths=self.include_paths,
            non_blocking=self.non_blocking,
            cursor=_cursor_for_training_shard(self.cursor, self.worker_cursors, worker_shard),
        )

    def set_epoch(self, epoch: int) -> None:
        self.parent.set_epoch(epoch)


class CxnfsTrainerStream:
    """Stateful trainer-facing stream over weighted MDS sources."""

    def __init__(
        self,
        dataset: CxnfsWeightedTorchDataset,
        batch_size: int,
        *,
        read_mode: TrainerReadMode | str = TrainerReadMode.DIRECT_BUFFERS,
        destination_factory: DirectDestinationFactory | None = None,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        non_blocking: bool = True,
        trainer_batch_iterator: str = TRAINER_BATCH_ITERATOR_NATIVE,
        data_loader_workers: int = 0,
        data_loader_kwargs: Mapping[str, Any] | None = None,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if samples_per_epoch is not None and samples_per_epoch <= 0:
            raise ValueError("samples_per_epoch must be positive")
        self.dataset = dataset
        self.batch_size = batch_size
        self.read_mode = TrainerReadMode(read_mode)
        self.destination_factory = destination_factory
        self.tensor_factory = tensor_factory
        self.tensor_allocation = tensor_allocation
        self.drop_last = drop_last
        self.include_paths = include_paths
        self.non_blocking = non_blocking
        self.trainer_batch_iterator = trainer_batch_iterator
        self.data_loader_workers = int(data_loader_workers)
        self.data_loader_kwargs = dict(data_loader_kwargs or {})
        _validate_trainer_batch_iterator_args(self)
        if self.trainer_batch_iterator not in TRAINER_BATCH_ITERATORS:
            raise ValueError(f"unsupported trainer batch iterator: {self.trainer_batch_iterator}")
        if self.trainer_batch_iterator != TRAINER_BATCH_ITERATOR_DATALOADER and self.data_loader_kwargs:
            raise ValueError("data_loader_kwargs require trainer_batch_iterator=trainer-dataloader")
        if cursor is None:
            active_epoch = dataset.epoch if epoch is None else epoch
            if active_epoch < 0:
                raise ValueError("epoch must be non-negative")
            self.dataset.set_epoch(active_epoch)
            self._cursor = self.dataset.initial_trainer_cursor(
                epoch=active_epoch,
                samples_per_epoch=samples_per_epoch,
            )
        else:
            active_cursor = self.dataset._coerce_trainer_cursor(cursor)
            self.dataset._validate_trainer_cursor(active_cursor, self.dataset.shard)
            if epoch is not None and active_cursor.epoch != epoch:
                raise ValueError("cursor epoch does not match stream epoch")
            if samples_per_epoch is not None and active_cursor.sample_limit != samples_per_epoch:
                raise ValueError("cursor sample limit does not match stream samples_per_epoch")
            self.dataset.set_epoch(active_cursor.epoch)
            self._cursor = active_cursor
        self._worker_cursors: dict[tuple[int, int], CxnfsTrainerCursor] = {}
        self._active_iterator = None

    @property
    def cursor(self) -> CxnfsTrainerCursor:
        return self._cursor

    @property
    def epoch(self) -> int:
        return self._cursor.epoch

    def set_epoch(self, epoch: int, *, reset_cursor: bool = True) -> None:
        if epoch < 0:
            raise ValueError("epoch must be non-negative")
        self.dataset.set_epoch(epoch)
        self._active_iterator = None
        if reset_cursor:
            self._cursor = self.dataset.initial_trainer_cursor(
                epoch=epoch,
                samples_per_epoch=self._cursor.sample_limit,
            )

    def reset_cursor(
        self,
        *,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
    ) -> CxnfsTrainerCursor:
        active_epoch = self.epoch if epoch is None else epoch
        if active_epoch < 0:
            raise ValueError("epoch must be non-negative")
        sample_limit = self._cursor.sample_limit if samples_per_epoch is None else samples_per_epoch
        if sample_limit is not None and sample_limit <= 0:
            raise ValueError("samples_per_epoch must be positive")
        self.dataset.set_epoch(active_epoch)
        self._cursor = self.dataset.initial_trainer_cursor(
            epoch=active_epoch,
            samples_per_epoch=sample_limit,
        )
        self._active_iterator = None
        return self._cursor

    def __iter__(self):
        return self.iter_batches()

    def prefetch_batches(self, max_prefetch_batches: int = 2) -> CxnfsTrainerPrefetchStream:
        return CxnfsTrainerPrefetchStream(self, max_prefetch_batches=max_prefetch_batches)

    def iter_batches(
        self,
        *,
        limit: int | None = None,
        update_cursor: bool = True,
    ):
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        emitted = 0
        while True:
            try:
                batch = self.next_batch(update_cursor=update_cursor)
            except StopIteration:
                return
            yield batch
            emitted += 1
            if limit is not None and emitted >= limit:
                return

    def next_batch(self, *, update_cursor: bool = True) -> CxnfsTrainerBatch:
        if self._active_iterator is None:
            self._active_iterator = self._batch_iterator()
        try:
            batch = next(self._active_iterator)
        except StopIteration:
            self._active_iterator = None
            raise
        except BaseException:
            self._active_iterator = None
            raise
        if update_cursor and batch.resume_cursor is not None:
            self.acknowledge_batch(batch)
        return batch

    def acknowledge_batch(self, batch: CxnfsTrainerBatch) -> CxnfsTrainerCursor:
        if batch.resume_cursor is not None:
            if self._uses_worker_partitioning():
                self._worker_cursors[
                    (batch.resume_cursor.shard_index, batch.resume_cursor.shard_count)
                ] = batch.resume_cursor
                self._cursor = self._aggregate_worker_cursor(batch)
            else:
                self._cursor = batch.resume_cursor
        return self._cursor

    def _uses_worker_partitioning(self) -> bool:
        return (
            self.trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER
            and self.data_loader_workers > 0
        )

    def _aggregate_worker_cursor(self, batch: CxnfsTrainerBatch) -> CxnfsTrainerCursor:
        return CxnfsTrainerCursor(
            epoch=batch.epoch,
            cursors=self._cursor.cursors,
            emitted=self._cursor.emitted + batch.sample_count,
            chunk_index=self._cursor.chunk_index,
            schedule_position=self._cursor.schedule_position,
            sample_limit=self._cursor.sample_limit,
            shard_index=self.dataset.shard.index,
            shard_count=self.dataset.shard.count,
            source_names=tuple(source.name for source in self.dataset.sources),
        )

    def state_dict(self) -> dict[str, Any]:
        tensor_allocation = self.tensor_allocation.to_dict() if self.tensor_allocation is not None else None
        return {
            "format": TRAINER_STREAM_STATE_FORMAT,
            "epoch": self.epoch,
            "batch_size": self.batch_size,
            "read_mode": self.read_mode.value,
            "trainer_batch_iterator": self.trainer_batch_iterator,
            "data_loader_workers": self.data_loader_workers,
            "samples_per_epoch": self._cursor.sample_limit,
            "drop_last": self.drop_last,
            "include_paths": self.include_paths,
            "non_blocking": self.non_blocking,
            "source_count": len(self.dataset.sources),
            "source_names": [source.name for source in self.dataset.sources],
            "cursor": self._cursor.to_dict(),
            "worker_partitioning_enabled": self._uses_worker_partitioning(),
            "worker_cursor_count": len(self._worker_cursors),
            "worker_cursors": [
                cursor.to_dict()
                for _, cursor in sorted(self._worker_cursors.items())
            ],
            "tensor_allocation": tensor_allocation,
        }

    def load_state_dict(self, payload: Mapping[str, Any], *, strict: bool = True) -> CxnfsTrainerCursor:
        if payload.get("format") != TRAINER_STREAM_STATE_FORMAT:
            raise ValueError(f"unsupported trainer stream state format: {payload.get('format')}")
        if strict:
            self._validate_state_contract(payload)
        cursor = CxnfsTrainerCursor.from_mapping(_mapping(payload.get("cursor")))
        self._cursor = self.dataset.advance_trainer_cursor(cursor, 0)
        self._worker_cursors = {
            (worker_cursor.shard_index, worker_cursor.shard_count): worker_cursor
            for worker_cursor in (
                CxnfsTrainerCursor.from_mapping(_mapping(value))
                for value in payload.get("worker_cursors", ())
                if isinstance(value, Mapping)
            )
        }
        self.dataset.set_epoch(self._cursor.epoch)
        self._active_iterator = None
        return self._cursor

    def save_state_file(self, path: str | Path, *, compact: bool = False) -> Path:
        target = Path(path)
        _write_json_mapping_atomic(target, self.state_dict(), compact=compact)
        return target

    def load_state_file(self, path: str | Path, *, strict: bool = True) -> CxnfsTrainerCursor:
        return self.load_state_dict(_read_json_mapping(Path(path), "trainer stream state"), strict=strict)

    def close(self) -> None:
        self.dataset.close()

    def _discard_unacknowledged_prefetch(self) -> None:
        self._active_iterator = None

    def __enter__(self) -> CxnfsTrainerStream:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _batch_iterator(self):
        if self.trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER:
            options = dict(self.data_loader_kwargs)
            options.setdefault("num_workers", self.data_loader_workers)
            loader = self.dataset.to_trainer_dataloader(
                self.batch_size,
                read_mode=self.read_mode,
                destination_factory=self.destination_factory,
                tensor_factory=self.tensor_factory,
                tensor_allocation=self.tensor_allocation,
                drop_last=self.drop_last,
                include_paths=self.include_paths,
                non_blocking=self.non_blocking,
                data_loader_kwargs=options,
                cursor=self._cursor,
                worker_cursors=self._worker_cursors,
            )
            return iter(loader)
        return self.dataset.iter_trainer_read_batches(
            self.batch_size,
            read_mode=self.read_mode,
            destination_factory=self.destination_factory,
            tensor_factory=self.tensor_factory,
            tensor_allocation=self.tensor_allocation,
            drop_last=self.drop_last,
            include_paths=self.include_paths,
            non_blocking=self.non_blocking,
            cursor=self._cursor,
        )

    def _validate_state_contract(self, payload: Mapping[str, Any]) -> None:
        if _int_value(payload.get("batch_size"), self.batch_size) != self.batch_size:
            raise ValueError("trainer stream state batch_size does not match stream")
        if str(payload.get("read_mode") or self.read_mode.value) != self.read_mode.value:
            raise ValueError("trainer stream state read_mode does not match stream")
        if str(payload.get("trainer_batch_iterator") or self.trainer_batch_iterator) != self.trainer_batch_iterator:
            raise ValueError("trainer stream state trainer_batch_iterator does not match stream")
        if _int_value(payload.get("data_loader_workers"), self.data_loader_workers) != self.data_loader_workers:
            raise ValueError("trainer stream state data_loader_workers does not match stream")
        if "samples_per_epoch" in payload:
            state_sample_limit = _optional_int_value(payload.get("samples_per_epoch"))
            cursor_sample_limit = _optional_int_value(_mapping(payload.get("cursor")).get("sample_limit"))
            if state_sample_limit != cursor_sample_limit:
                raise ValueError("trainer stream state samples_per_epoch does not match cursor")
            if state_sample_limit != self._cursor.sample_limit:
                raise ValueError("trainer stream state samples_per_epoch does not match stream")
        source_names = tuple(str(value) for value in payload.get("source_names", ()))
        expected_names = tuple(source.name for source in self.dataset.sources)
        if source_names and source_names != expected_names:
            raise ValueError("trainer stream state source_names do not match stream")


class CxnfsTrainerPipelineStream:
    """Stateful descriptor stream for CxnfsTrainingPipeline tensor prefetch."""

    def __init__(
        self,
        dataset: CxnfsWeightedTorchDataset,
        batch_size: int,
        *,
        tensor_factory: DirectTensorFactory | None = None,
        tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
        epoch: int | None = None,
        samples_per_epoch: int | None = None,
        drop_last: bool = False,
        include_paths: bool = False,
        cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
        source_path_prefixes: Mapping[str, str | Path | None] | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if samples_per_epoch is not None and samples_per_epoch <= 0:
            raise ValueError("samples_per_epoch must be positive")
        self.dataset = dataset
        self.batch_size = batch_size
        self.tensor_factory = tensor_factory
        self.tensor_allocation = tensor_allocation
        self.drop_last = drop_last
        self.include_paths = include_paths
        self.source_path_prefixes = dict(source_path_prefixes or {})
        if cursor is None:
            active_epoch = dataset.epoch if epoch is None else epoch
            if active_epoch < 0:
                raise ValueError("epoch must be non-negative")
            self.dataset.set_epoch(active_epoch)
            self._cursor = self.dataset.initial_trainer_cursor(
                epoch=active_epoch,
                samples_per_epoch=samples_per_epoch,
            )
        else:
            active_cursor = self.dataset._coerce_trainer_cursor(cursor)
            self.dataset._validate_trainer_cursor(active_cursor, self.dataset.shard)
            if epoch is not None and active_cursor.epoch != epoch:
                raise ValueError("cursor epoch does not match stream epoch")
            if samples_per_epoch is not None and active_cursor.sample_limit != samples_per_epoch:
                raise ValueError("cursor sample limit does not match stream samples_per_epoch")
            self.dataset.set_epoch(active_cursor.epoch)
            self._cursor = active_cursor
        self._active_iterator = None

    @property
    def cursor(self) -> CxnfsTrainerCursor:
        return self._cursor

    @property
    def epoch(self) -> int:
        return self._cursor.epoch

    def __iter__(self):
        return self.iter_batches()

    def iter_batches(
        self,
        *,
        limit: int | None = None,
        update_cursor: bool = True,
    ):
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        emitted = 0
        while True:
            try:
                batch = self.next_batch(update_cursor=update_cursor)
            except StopIteration:
                return
            yield batch
            emitted += 1
            if limit is not None and emitted >= limit:
                return

    def next_batch(self, *, update_cursor: bool = True) -> CxnfsTrainerBatch:
        if self._active_iterator is None:
            self._active_iterator = self._batch_iterator()
        try:
            batch = next(self._active_iterator)
        except StopIteration:
            self._active_iterator = None
            raise
        except BaseException:
            self._active_iterator = None
            raise
        if update_cursor and batch.resume_cursor is not None:
            self.acknowledge_batch(batch)
        return batch

    def acknowledge_batch(self, batch: CxnfsTrainerBatch) -> CxnfsTrainerCursor:
        if batch.resume_cursor is not None:
            self._cursor = batch.resume_cursor
        return self._cursor

    def state_dict(self) -> dict[str, Any]:
        tensor_allocation = self.tensor_allocation.to_dict() if self.tensor_allocation is not None else None
        return {
            "format": TRAINER_STREAM_STATE_FORMAT,
            "epoch": self.epoch,
            "batch_size": self.batch_size,
            "read_mode": TrainerReadMode.DIRECT_TENSORS.value,
            "trainer_batch_iterator": "pipeline-descriptor-stream",
            "samples_per_epoch": self._cursor.sample_limit,
            "drop_last": self.drop_last,
            "include_paths": self.include_paths,
            "source_count": len(self.dataset.sources),
            "source_names": [source.name for source in self.dataset.sources],
            "cursor": self._cursor.to_dict(),
            "tensor_allocation": tensor_allocation,
            "source_path_prefixes": {
                name: None if prefix is None else str(prefix)
                for name, prefix in self.source_path_prefixes.items()
            },
        }

    def load_state_dict(self, payload: Mapping[str, Any], *, strict: bool = True) -> CxnfsTrainerCursor:
        if payload.get("format") != TRAINER_STREAM_STATE_FORMAT:
            raise ValueError(f"unsupported trainer stream state format: {payload.get('format')}")
        if strict:
            self._validate_state_contract(payload)
        cursor = CxnfsTrainerCursor.from_mapping(_mapping(payload.get("cursor")))
        self._cursor = self.dataset.advance_trainer_cursor(cursor, 0)
        self.dataset.set_epoch(self._cursor.epoch)
        self._active_iterator = None
        return self._cursor

    def save_state_file(self, path: str | Path, *, compact: bool = False) -> Path:
        target = Path(path)
        _write_json_mapping_atomic(target, self.state_dict(), compact=compact)
        return target

    def load_state_file(self, path: str | Path, *, strict: bool = True) -> CxnfsTrainerCursor:
        return self.load_state_dict(_read_json_mapping(Path(path), "trainer pipeline stream state"), strict=strict)

    def close(self) -> None:
        self._active_iterator = None

    def _discard_unacknowledged_prefetch(self) -> None:
        self._active_iterator = None

    def __enter__(self) -> CxnfsTrainerPipelineStream:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _batch_iterator(self):
        return self.dataset.iter_trainer_pipeline_tensor_batch_descriptors(
            self.batch_size,
            tensor_factory=self.tensor_factory,
            tensor_allocation=self.tensor_allocation,
            epoch=None,
            samples_per_epoch=None,
            drop_last=self.drop_last,
            include_paths=self.include_paths,
            cursor=self._cursor,
            source_path_prefixes=self.source_path_prefixes,
        )

    def _validate_state_contract(self, payload: Mapping[str, Any]) -> None:
        if _int_value(payload.get("batch_size"), self.batch_size) != self.batch_size:
            raise ValueError("trainer pipeline stream state batch_size does not match stream")
        if str(payload.get("read_mode") or TrainerReadMode.DIRECT_TENSORS.value) != TrainerReadMode.DIRECT_TENSORS.value:
            raise ValueError("trainer pipeline stream state read_mode does not match stream")
        if str(payload.get("trainer_batch_iterator") or "pipeline-descriptor-stream") != "pipeline-descriptor-stream":
            raise ValueError("trainer pipeline stream state trainer_batch_iterator does not match stream")
        if "samples_per_epoch" in payload:
            state_sample_limit = _optional_int_value(payload.get("samples_per_epoch"))
            cursor_sample_limit = _optional_int_value(_mapping(payload.get("cursor")).get("sample_limit"))
            if state_sample_limit != cursor_sample_limit:
                raise ValueError("trainer pipeline stream state samples_per_epoch does not match cursor")
            if state_sample_limit != self._cursor.sample_limit:
                raise ValueError("trainer pipeline stream state samples_per_epoch does not match stream")
        source_names = tuple(str(value) for value in payload.get("source_names", ()))
        expected_names = tuple(source.name for source in self.dataset.sources)
        if source_names and source_names != expected_names:
            raise ValueError("trainer pipeline stream state source_names do not match stream")
        state_prefixes = {
            str(name): None if prefix is None else str(prefix)
            for name, prefix in _mapping(payload.get("source_path_prefixes")).items()
        }
        current_prefixes = {
            str(name): None if prefix is None else str(prefix)
            for name, prefix in self.source_path_prefixes.items()
        }
        if state_prefixes and current_prefixes and state_prefixes != current_prefixes:
            raise ValueError("trainer pipeline stream state source_path_prefixes do not match stream")


class CxnfsTrainerPipelineLoop:
    """Trainer-facing iterator for tensor descriptor batches filled by CxnfsTrainingPipeline."""

    def __init__(
        self,
        stream: CxnfsTrainerPipelineStream,
        pipeline: CxnfsTrainingPipeline,
        prefetch_stream: CxnfsPipelineTensorPrefetchStream,
    ) -> None:
        if stream is None:
            raise ValueError("stream is required")
        if pipeline is None:
            raise ValueError("pipeline is required")
        if prefetch_stream is None:
            raise ValueError("prefetch_stream is required")
        self.stream = stream
        self.pipeline = pipeline
        self.prefetch_stream = prefetch_stream

    @property
    def cursor(self) -> CxnfsTrainerCursor:
        return self.stream.cursor

    @property
    def queued_batches(self) -> int:
        return self.prefetch_stream.queued_batches

    @property
    def closed(self) -> bool:
        return self.prefetch_stream.closed

    def __iter__(self) -> CxnfsTrainerPipelineLoop:
        return self

    def __next__(self):
        return next(self.prefetch_stream)

    def next_batch(self):
        return next(self)

    def iter_batches(self, *, limit: int | None = None):
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        emitted = 0
        while True:
            if limit is not None and emitted >= limit:
                return
            try:
                batch = next(self)
            except StopIteration:
                return
            emitted += 1
            yield batch

    def state_dict(self) -> dict[str, Any]:
        return self.stream.state_dict()

    def save_state_file(self, path: str | Path, *, compact: bool = False) -> Path:
        return self.stream.save_state_file(path, compact=compact)

    def load_state_dict(self, payload: Mapping[str, Any], *, strict: bool = True) -> CxnfsTrainerCursor:
        return self.stream.load_state_dict(payload, strict=strict)

    def load_state_file(self, path: str | Path, *, strict: bool = True) -> CxnfsTrainerCursor:
        return self.stream.load_state_file(path, strict=strict)

    def close(self) -> None:
        self.prefetch_stream.close()

    def __enter__(self) -> CxnfsTrainerPipelineLoop:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


@dataclass(frozen=True)
class _TrainerPrefetchFailure:
    error: BaseException


class CxnfsTrainerPrefetchStream:
    """Background batch prefetch adapter for trainer loops."""

    def __init__(
        self,
        stream: CxnfsTrainerStream,
        *,
        max_prefetch_batches: int = 2,
    ) -> None:
        if max_prefetch_batches <= 0:
            raise ValueError("max_prefetch_batches must be positive")
        self.stream = stream
        self.max_prefetch_batches = int(max_prefetch_batches)
        self._queue: queue.Queue[object] = queue.Queue(maxsize=self.max_prefetch_batches)
        self._sentinel = object()
        self._closed = False
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="cxnfs-trainer-prefetch",
        )
        self._producer = self._executor.submit(self._produce)

    @property
    def cursor(self) -> CxnfsTrainerCursor:
        return self.stream.cursor

    @property
    def queued_batches(self) -> int:
        return self._queue.qsize()

    def __iter__(self):
        return self

    def __next__(self) -> CxnfsTrainerBatch:
        while True:
            if self._closed:
                raise StopIteration
            try:
                item = self._queue.get(timeout=0.05)
                break
            except queue.Empty:
                continue
        if item is self._sentinel:
            self.close()
            raise StopIteration
        if isinstance(item, _TrainerPrefetchFailure):
            self.close()
            error = item.error
            if isinstance(error, Exception):
                raise error
            raise RuntimeError(str(error)) from error
        if not isinstance(item, CxnfsTrainerBatch):
            self.close()
            raise RuntimeError("trainer prefetch worker returned an invalid item")
        self.stream.acknowledge_batch(item)
        return item

    def next_batch(self) -> CxnfsTrainerBatch:
        return next(self)

    def iter_batches(self, *, limit: int | None = None):
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        emitted = 0
        while True:
            if limit is not None and emitted >= limit:
                return
            try:
                batch = next(self)
            except StopIteration:
                return
            emitted += 1
            yield batch

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._drain_queue()
        self.stream._discard_unacknowledged_prefetch()
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._drain_queue()

    def __enter__(self) -> CxnfsTrainerPrefetchStream:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _produce(self) -> None:
        try:
            while not self._closed:
                try:
                    batch = self.stream.next_batch(update_cursor=False)
                except StopIteration:
                    self._put(self._sentinel)
                    return
                self._put(batch)
        except BaseException as exc:
            self._put(_TrainerPrefetchFailure(exc))

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


def normalize_decoder(decoder: MdsSampleFormat | str | SampleDecoder) -> SampleDecoder:
    if callable(decoder):
        return decoder
    sample_format = MdsSampleFormat(decoder)
    if sample_format == MdsSampleFormat.RAW:
        return decode_raw_sample
    if sample_format == MdsSampleFormat.PARQUET_JSON:
        return decode_parquet_json_sample
    if sample_format == MdsSampleFormat.WEBDATASET_TAR:
        return decode_webdataset_tar_sample
    raise ValueError(f"unsupported MDS sample decoder: {decoder}")


def decode_raw_sample(sample: MdsSample) -> MdsSample:
    return sample


def decode_parquet_json_sample(sample: MdsSample) -> dict[str, Any]:
    decoded = json.loads(sample.data.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("parquet-json MDS sample did not decode to an object")
    return decoded


def decode_webdataset_tar_sample(sample: MdsSample) -> dict[str, bytes]:
    members: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(sample.data), mode="r:*") as archive:
        for member in archive:
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            if extracted is not None:
                members[member.name] = extracted.read()
    return members


def parse_weighted_mds_spec(value: str | None) -> dict[str, int]:
    if value is None or not value.strip():
        raise ValueError("weighted MDS spec is required")
    weights: dict[str, int] = {}
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError("weighted MDS entries must use name=weight: " + part)
        name, raw_weight = part.split("=", 1)
        name = name.strip()
        if not name:
            raise ValueError("weighted MDS source name is required")
        weight = int(raw_weight.strip())
        if weight <= 0:
            raise ValueError("weighted MDS source weight must be positive: " + name)
        weights[name] = weight
    if not weights:
        raise ValueError("at least one weighted MDS entry is required")
    return weights


def _add_trainer_tensor_allocation_args(parser: ArgumentParser, *, default_values: bool) -> None:
    parser.add_argument(
        "--tensor-target",
        choices=TRAINER_TENSOR_TARGETS,
        default=TensorMemoryTarget.PYTORCH_PINNED_CPU.value if default_values else None,
        help="Tensor destination target for direct-tensor trainer reads.",
    )
    parser.add_argument(
        "--tensor-dtype",
        default="uint8" if default_values else None,
        help="PyTorch dtype for direct-tensor trainer reads.",
    )
    parser.add_argument(
        "--tensor-device",
        default="cpu" if default_values else None,
        help="PyTorch device for direct-tensor trainer reads.",
    )
    parser.add_argument(
        "--tensor-non-blocking",
        choices=("true", "false"),
        default="true" if default_values else None,
        help="Use non-blocking tensor copy semantics when supported.",
    )


def _add_trainer_repeat_args(parser: ArgumentParser, *, default: bool | None = True) -> None:
    parser.add_argument(
        "--repeat",
        dest="repeat",
        action="store_true",
        default=default,
        help="Wrap each weighted source when a trainer epoch reads past the source sample count.",
    )
    parser.add_argument(
        "--no-repeat",
        dest="repeat",
        action="store_false",
        help="Stop each weighted source after its finite sample set is exhausted.",
    )


def _trainer_tensor_allocation_from_args(
    args: Any,
    fallback: Mapping[str, Any] | None = None,
) -> CxnfsTrainerTensorAllocation:
    source = fallback or {}
    target = getattr(args, "tensor_target", None) or source.get("target") or TensorMemoryTarget.PYTORCH_PINNED_CPU.value
    dtype = getattr(args, "tensor_dtype", None) or source.get("dtype") or "uint8"
    device = getattr(args, "tensor_device", None) or source.get("device") or "cpu"
    raw_non_blocking = getattr(args, "tensor_non_blocking", None)
    if raw_non_blocking is None:
        raw_non_blocking = source.get("non_blocking")
    return CxnfsTrainerTensorAllocation(
        target=target,
        dtype=str(dtype),
        device=str(device),
        non_blocking=_bool_value(raw_non_blocking, True),
    )


def _bool_value(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"expected boolean value: {value}")


def weighted_mds_sources_from_root(
    root: str | Path,
    weights: Mapping[str, int] | str,
    *,
    decoder: MdsSampleFormat | str | SampleDecoder = MdsSampleFormat.RAW,
    transform: SampleTransform | None = None,
    cache_bytes: int | None = None,
    client: CxnfsDatasetClient | None = None,
    client_factory: DatasetClientFactory | None = None,
    remote_root: str | Path | None = None,
) -> tuple[CxnfsWeightedSource, ...]:
    parsed = parse_weighted_mds_spec(weights) if isinstance(weights, str) else dict(weights)
    base = Path(root)
    if client is not None and client_factory is not None:
        raise ValueError("use client or client_factory, not both")
    if remote_root is not None and client is None and client_factory is None:
        raise ValueError("remote_root requires client or client_factory")
    sources = []
    for name, weight in parsed.items():
        dataset_root = base / name
        if not (dataset_root / "index.json").is_file():
            raise FileNotFoundError(f"missing MDS index for weighted source {name}: {dataset_root / 'index.json'}")
        source_client = _source_client(name, client, client_factory, remote_root)
        sources.append(CxnfsWeightedSource(
            name,
            dataset_root,
            weight,
            decoder=decoder,
            transform=transform,
            cache_bytes=cache_bytes,
            client=source_client,
        ))
    return tuple(sources)


def open_weighted_mds_trainer_stream(
    root: str | Path,
    weights: Mapping[str, int] | str,
    *,
    batch_size: int,
    remote_root: str | Path | None = None,
    decoder: MdsSampleFormat | str | SampleDecoder = MdsSampleFormat.RAW,
    transform: SampleTransform | None = None,
    cache_bytes: int | None = None,
    client: CxnfsDatasetClient | None = None,
    client_factory: DatasetClientFactory | None = None,
    read_mode: TrainerReadMode | str = TrainerReadMode.DIRECT_BUFFERS,
    destination_factory: DirectDestinationFactory | None = None,
    tensor_factory: DirectTensorFactory | None = None,
    tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
    epoch: int | None = None,
    samples_per_epoch: int | None = None,
    repeat: bool = True,
    rounds_per_read: int = 1,
    max_workers: int = 16,
    source_read_workers: int | None = None,
    prefetch_chunks: int = 1,
    include_source_metadata: bool = False,
    shard: CxnfsTrainingShard | None = None,
    shuffle: bool = False,
    seed: int | None = None,
    drop_last: bool = False,
    include_paths: bool = False,
    non_blocking: bool = True,
    trainer_batch_iterator: str = TRAINER_BATCH_ITERATOR_NATIVE,
    data_loader_workers: int = 0,
    data_loader_kwargs: Mapping[str, Any] | None = None,
    cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
) -> CxnfsTrainerStream:
    sources = weighted_mds_sources_from_root(
        root,
        weights,
        decoder=decoder,
        transform=transform,
        cache_bytes=cache_bytes,
        client=client,
        client_factory=client_factory,
        remote_root=remote_root,
    )
    dataset = CxnfsWeightedTorchDataset(
        sources,
        repeat=repeat,
        rounds_per_read=rounds_per_read,
        max_workers=max_workers,
        source_read_workers=source_read_workers,
        prefetch_chunks=prefetch_chunks,
        include_source_metadata=include_source_metadata,
        shard=shard,
        shuffle=shuffle,
        seed=seed,
        samples_per_epoch=samples_per_epoch if cursor is None else None,
    )
    try:
        return CxnfsTrainerStream(
            dataset,
            batch_size,
            read_mode=read_mode,
            destination_factory=destination_factory,
            tensor_factory=tensor_factory,
            tensor_allocation=tensor_allocation,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
            drop_last=drop_last,
            include_paths=include_paths,
            non_blocking=non_blocking,
            trainer_batch_iterator=trainer_batch_iterator,
            data_loader_workers=data_loader_workers,
            data_loader_kwargs=data_loader_kwargs,
            cursor=cursor,
        )
    except Exception:
        dataset.close()
        raise


def create_remote_trainer_client_factory(
    credentials: ClientCredentials,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    *,
    timeout: float | None = 10.0,
    tls_context: Any = None,
    server_hostname: str | None = None,
    read_config: CxnfsBulkReadConfig | None = None,
    write_connections: int | None = None,
    bulk_read_retries: int = 0,
    bulk_write_retries: int | None = None,
    shared_client: bool = False,
    bulk_transport: str = PYTHON_BULK_TRANSPORT_STREAM,
    cafile: str | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> DatasetClientFactory:
    selected_transport = require_supported_python_bulk_transport(bulk_transport)
    return _trainer_stream_remote_client_factory(
        credentials,
        host,
        control_port,
        transport_port,
        timeout,
        tls_context,
        server_hostname or host,
        read_config or CxnfsBulkReadConfig(),
        write_connections,
        bulk_read_retries,
        bulk_write_retries,
        shared_client,
        selected_transport,
        cafile,
        control_client_factory,
        bulk_client_factory,
        tls_context_factory,
    )


class CxnfsRemoteTrainerClientFactory:
    def __init__(
        self,
        credentials: ClientCredentials,
        host: str,
        control_port: int,
        transport_port: int,
        timeout: float | None,
        tls_context: Any,
        server_hostname: str,
        read_config: CxnfsBulkReadConfig,
        write_connections: int | None,
        bulk_read_retries: int,
        bulk_write_retries: int | None,
        shared_client: bool,
        bulk_transport: str,
        cafile: str | None,
        control_client_factory: Callable[..., Any],
        bulk_client_factory: Callable[..., Any],
        tls_context_factory: Callable[[str | None], Any],
    ) -> None:
        self.credentials = credentials
        self.host = host
        self.control_port = control_port
        self.transport_port = transport_port
        self.timeout = timeout
        self.tls_context = tls_context
        self.server_hostname = server_hostname
        self.read_config = read_config
        self.write_connections = write_connections
        self.bulk_read_retries = bulk_read_retries
        self.bulk_write_retries = bulk_write_retries
        self.shared_client = shared_client
        self.bulk_transport = bulk_transport
        self.cafile = cafile
        self.control_client_factory = control_client_factory
        self.bulk_client_factory = bulk_client_factory
        self.tls_context_factory = tls_context_factory
        self._shared_client: Any = None
        self._connection_lock = threading.RLock()
        self._source_request_names: list[str] = []
        self._connection_source_names: list[str] = []
        self._client_open_source_names: list[str] = []
        self._client_close_source_names: list[str] = []
        self._active_clients: dict[int, "_TrackedRemoteTrainerClient"] = {}
        self._closed_client_ids: set[int] = set()
        self._next_client_id = 0
        self._shared_client_reuse_count = 0
        self._factory_close_count = 0

    def __call__(self, source_name: str):
        if not source_name:
            raise ValueError("source_name is required")
        with self._connection_lock:
            self._source_request_names.append(source_name)
            if not self.shared_client:
                self._connection_source_names.append(source_name)
                return self._track_client(source_name, self._connect_one())
            if self._shared_client is None:
                self._connection_source_names.append(source_name)
                self._shared_client = self._track_client(source_name, self._connect_one())
            else:
                self._shared_client_reuse_count += 1
            return self._shared_client

    def _connect_one(self):
        tls_context = self._tls_context_for_connection()
        return _connect_trainer_remote_bulk_client(
            self.credentials,
            self.host,
            self.control_port,
            self.transport_port,
            self.timeout,
            tls_context,
            self.server_hostname,
            self.read_config,
            self.write_connections,
            self.bulk_read_retries,
            self.bulk_write_retries,
            self.bulk_transport,
            self.cafile,
            self.control_client_factory,
            self.bulk_client_factory,
        )

    def _track_client(self, source_name: str, client: Any):
        self._next_client_id += 1
        client_id = self._next_client_id
        tracked = _TrackedRemoteTrainerClient(self, client_id, source_name, client)
        self._active_clients[client_id] = tracked
        self._client_open_source_names.append(source_name)
        return tracked

    def _mark_client_closed(self, client_id: int, source_name: str) -> None:
        with self._connection_lock:
            self._active_clients.pop(client_id, None)
            shared_client = self._shared_client
            if (
                shared_client is not None
                and getattr(shared_client, "client_id", None) == client_id
            ):
                self._shared_client = None
            if client_id not in self._closed_client_ids:
                self._closed_client_ids.add(client_id)
                self._client_close_source_names.append(source_name)

    def close(self) -> None:
        with self._connection_lock:
            self._factory_close_count += 1
            clients = list(self._active_clients.values())
            self._shared_client = None
        errors: list[BaseException] = []
        for client in clients:
            try:
                client.close()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        state["_shared_client"] = None
        state["tls_context"] = None
        state["_connection_lock"] = None
        state["_active_clients"] = {}
        state["_closed_client_ids"] = set()
        return state

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self.__dict__.update(state)
        self._shared_client = None
        self._connection_lock = threading.RLock()
        self._client_open_source_names = list(getattr(self, "_client_open_source_names", ()))
        self._client_close_source_names = list(getattr(self, "_client_close_source_names", ()))
        self._next_client_id = int(getattr(self, "_next_client_id", 0) or 0)
        self._shared_client_reuse_count = int(getattr(self, "_shared_client_reuse_count", 0) or 0)
        self._factory_close_count = int(getattr(self, "_factory_close_count", 0) or 0)
        self._active_clients = {}
        self._closed_client_ids = set()

    def connection_topology(self) -> dict[str, Any]:
        with self._connection_lock:
            source_requests = list(self._source_request_names)
            connection_sources = list(self._connection_source_names)
            open_sources = list(self._client_open_source_names)
            close_sources = list(self._client_close_source_names)
            active_sources = [client.source_name for client in self._active_clients.values()]
            active_count = len(self._active_clients)
            closed_count = len(self._closed_client_ids)
            shared_client_reuse_count = self._shared_client_reuse_count
            factory_close_count = self._factory_close_count
        unique_source_requests = _dedupe_strings(source_requests)
        unique_connection_sources = _dedupe_strings(connection_sources)
        unique_open_sources = _dedupe_strings(open_sources)
        unique_close_sources = _dedupe_strings(close_sources)
        source_request_set = set(unique_source_requests)
        connection_source_set = set(unique_connection_sources)
        open_source_set = set(unique_open_sources)
        close_source_set = set(unique_close_sources)
        unique_request_count = len(unique_source_requests)
        connection_count = len(connection_sources)
        connection_source_set_matches_requests = (
            unique_request_count == 0
            or connection_source_set == source_request_set
        )
        client_open_source_set_matches_requests = (
            unique_request_count == 0
            or open_source_set == source_request_set
        )
        client_close_source_set_matches_opens = (
            len(open_sources) == closed_count
            and active_count == 0
            and close_source_set == open_source_set
        )
        per_source_observed = (
            not self.shared_client
            and unique_request_count > 0
            and connection_count >= unique_request_count
            and connection_source_set_matches_requests
            and client_open_source_set_matches_requests
        )
        shared_observed = (
            self.shared_client
            and unique_request_count > 0
            and (shared_client_reuse_count > 0 or connection_count == 1)
        )
        return {
            "format": "cxnfs.trainer-remote-client-topology.v1",
            "shared_client": self.shared_client,
            "source_request_count": len(source_requests),
            "unique_source_request_count": unique_request_count,
            "source_request_names": source_requests,
            "unique_source_request_names": unique_source_requests,
            "client_open_count": len(open_sources),
            "client_open_source_names": open_sources,
            "unique_client_open_source_names": unique_open_sources,
            "client_close_count": closed_count,
            "client_close_source_names": close_sources,
            "unique_client_close_source_names": unique_close_sources,
            "active_client_count": active_count,
            "active_client_source_names": active_sources,
            "unique_active_client_source_names": _dedupe_strings(active_sources),
            "shared_client_reuse_count": shared_client_reuse_count,
            "factory_close_count": factory_close_count,
            "all_open_clients_closed": len(open_sources) == closed_count and active_count == 0,
            "connection_source_set_matches_requests": connection_source_set_matches_requests,
            "client_open_source_set_matches_requests": client_open_source_set_matches_requests,
            "client_close_source_set_matches_opens": client_close_source_set_matches_opens,
            "client_lifecycle_source_set_complete": client_close_source_set_matches_opens,
            "connection_count": connection_count,
            "connection_source_names": connection_sources,
            "unique_connection_source_names": unique_connection_sources,
            "per_source_client_observed": per_source_observed,
            "shared_client_observed": shared_observed,
            "complete": per_source_observed or shared_observed or unique_request_count == 0,
        }

    def _tls_context_for_connection(self):
        if self.tls_context is not None:
            return self.tls_context
        if self.cafile:
            return self.tls_context_factory(self.cafile)
        return None


class _TrackedRemoteTrainerClient:
    def __init__(
        self,
        owner: CxnfsRemoteTrainerClientFactory,
        client_id: int,
        source_name: str,
        delegate: Any,
    ) -> None:
        self._owner = owner
        self._client_id = client_id
        self._source_name = source_name
        self._delegate = delegate
        self._closed = False
        self._lock = threading.Lock()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    @property
    def delegate(self) -> Any:
        return self._delegate

    @property
    def client_id(self) -> int:
        return self._client_id

    @property
    def source_name(self) -> str:
        return self._source_name

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            close = getattr(self._delegate, "close", None)
            if callable(close):
                close()
        finally:
            self._owner._mark_client_closed(self._client_id, self._source_name)


def plan_weighted_mds_sources(
    root: str | Path,
    weights: Mapping[str, int] | str,
    *,
    remote_root: str | Path | None = None,
) -> CxnfsWeightedMdsPlan:
    parsed = parse_weighted_mds_spec(weights) if isinstance(weights, str) else dict(weights)
    base = Path(root)
    if not base.is_dir():
        raise FileNotFoundError(f"weighted MDS root does not exist: {base}")
    remote_root_value = _normalize_remote_root(remote_root) if remote_root is not None else None
    sources: list[CxnfsWeightedSourcePlan] = []
    schedule: list[str] = []
    for name, weight in parsed.items():
        dataset_root = base / name
        index = MdsDataset(dataset_root).index
        shard_names = tuple(shard.shard_name for shard in index.shards)
        sources.append(CxnfsWeightedSourcePlan(
            name=name,
            weight=weight,
            local_root=str(dataset_root),
            remote_prefix=_join_remote_source_path(remote_root_value, name) if remote_root_value is not None else None,
            sample_count=index.sample_count,
            shard_count=len(index.shards),
            byte_size=sum(shard.byte_size for shard in index.shards),
            shards=shard_names,
        ))
        schedule.extend([name] * weight)

    return CxnfsWeightedMdsPlan(
        root=str(base),
        remote_root=remote_root_value,
        total_weight=sum(parsed.values()),
        source_count=len(sources),
        total_samples=sum(source.sample_count for source in sources),
        total_bytes=sum(source.byte_size for source in sources),
        source_schedule=tuple(schedule),
        sources=tuple(sources),
    )


def weighted_mds_plan_fingerprint(plan: Mapping[str, Any] | CxnfsWeightedMdsPlan) -> dict[str, Any]:
    if isinstance(plan, CxnfsWeightedMdsPlan):
        payload = {
            "source_count": plan.source_count,
            "total_weight": plan.total_weight,
            "total_samples": plan.total_samples,
            "total_bytes": plan.total_bytes,
            "source_schedule": list(plan.source_schedule),
            "sources": [source.to_dict() for source in plan.sources],
        }
    else:
        payload = dict(plan)
    normalized = _weighted_mds_fingerprint_payload(payload)
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "format": WEIGHTED_MDS_WORKLOAD_FINGERPRINT_FORMAT,
        "algorithm": "sha256",
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "source_count": normalized["source_count"],
        "total_weight": normalized["total_weight"],
        "total_samples": normalized["total_samples"],
        "total_bytes": normalized["total_bytes"],
    }


def _weighted_mds_fingerprint_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    sources = []
    for source in plan.get("sources", ()):
        if not isinstance(source, Mapping):
            continue
        sources.append({
            "name": str(source.get("name") or ""),
            "weight": _int_value(source.get("weight"), 0),
            "sample_count": _int_value(source.get("sample_count"), 0),
            "shard_count": _int_value(source.get("shard_count"), 0),
            "byte_size": _int_value(source.get("byte_size"), 0),
            "shards": [str(value) for value in source.get("shards", ()) if isinstance(value, str)],
        })
    source_schedule = [
        str(value)
        for value in plan.get("source_schedule", ())
        if isinstance(value, str)
    ]
    return {
        "format": WEIGHTED_MDS_WORKLOAD_FINGERPRINT_FORMAT,
        "source_count": _int_value(plan.get("source_count"), len(sources)),
        "total_weight": _int_value(plan.get("total_weight"), 0),
        "total_samples": _int_value(plan.get("total_samples"), 0),
        "total_bytes": _int_value(plan.get("total_bytes"), 0),
        "source_schedule": source_schedule,
        "sources": sources,
    }


def run_weighted_mds_plan_cli(argv: Sequence[str] | None = None, stdout=None, stderr=None) -> int:
    parser = ArgumentParser(description="Validate and describe weighted CXNFS MDS-Shard datasets.")
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--remote-root", help="Remote CXNFS namespace root containing the same source directories.")
    parser.add_argument("--output", help="Optional path to write the JSON plan.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        plan = plan_weighted_mds_sources(args.root, args.weights, remote_root=args.remote_root)
        text = plan.to_json(indent=None if args.compact else 2)
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_weighted_mds_plan() -> None:
    raise SystemExit(run_weighted_mds_plan_cli())


def _add_training_shard_args(parser: ArgumentParser) -> None:
    parser.add_argument("--rank", type=int, help="Distributed trainer rank for the CXNFS stream shard.")
    parser.add_argument("--world-size", type=int, help="Distributed trainer world size for the CXNFS stream shard.")


def _training_shard_from_values(rank: int | None, world_size: int | None) -> CxnfsTrainingShard | None:
    if rank is None and world_size is None:
        return None
    return CxnfsTrainingShard(0 if rank is None else rank, 1 if world_size is None else world_size)


def _training_shard_from_args(args: Any) -> CxnfsTrainingShard | None:
    return _training_shard_from_values(
        _optional_int_value(getattr(args, "rank", None)),
        _optional_int_value(getattr(args, "world_size", None)),
    )


def _training_shard_from_mapping(payload: Mapping[str, Any]) -> CxnfsTrainingShard:
    return CxnfsTrainingShard(
        _int_value(payload.get("index"), 0),
        _int_value(payload.get("count"), 1),
    )


def _training_shard_payload(shard: CxnfsTrainingShard) -> dict[str, int]:
    return {
        "index": shard.index,
        "count": shard.count,
    }


def _serialize_trainer_stream_kwargs(stream_kwargs: Mapping[str, Any]) -> dict[str, Any]:
    serialized_stream_kwargs = dict(stream_kwargs)
    tensor_allocation = serialized_stream_kwargs.get("tensor_allocation")
    if isinstance(tensor_allocation, CxnfsTrainerTensorAllocation):
        serialized_stream_kwargs["tensor_allocation"] = tensor_allocation.to_dict()
    training_shard = serialized_stream_kwargs.get("shard")
    if isinstance(training_shard, CxnfsTrainingShard):
        serialized_stream_kwargs["shard"] = _training_shard_payload(training_shard)
    return serialized_stream_kwargs


def build_trainer_contract_payload(
    root: str | Path,
    weights: Mapping[str, int] | str,
    *,
    remote_root: str | Path | None = None,
    bulk_transport: str = PYTHON_BULK_TRANSPORT_STREAM,
    bulk_read_retries: int = 0,
    bulk_write_retries: int | None = None,
    read_mode: TrainerReadMode | str = TrainerReadMode.DIRECT_BUFFERS,
    batch_size: int = 8,
    batches: int = 1,
    rounds_per_read: int = 1,
    max_workers: int = 16,
    source_read_workers: int | None = None,
    prefetch_chunks: int = 1,
    trainer_prefetch_batches: int = 0,
    shuffle: bool = False,
    seed: int = 0,
    epoch: int = 0,
    samples_per_epoch: int | None = None,
    repeat: bool = True,
    decoder: MdsSampleFormat | str | SampleDecoder = MdsSampleFormat.RAW,
    trainer_batch_iterator: str = TRAINER_BATCH_ITERATOR_NATIVE,
    data_loader_workers: int = 0,
    tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
    shard: CxnfsTrainingShard | None = None,
    min_workload_runs: int = 0,
    min_workload_samples: int = 0,
    min_workload_bytes: int = 0,
    min_cord_sweep: int = 0,
) -> dict[str, Any]:
    if batch_size <= 0:
        raise ValueError("batch-size must be positive")
    if batches <= 0:
        raise ValueError("batches must be positive")
    if rounds_per_read <= 0:
        raise ValueError("rounds-per-read must be positive")
    if max_workers <= 0:
        raise ValueError("max-workers must be positive")
    if source_read_workers is not None and source_read_workers <= 0:
        raise ValueError("source-read-workers must be positive")
    if prefetch_chunks <= 0:
        raise ValueError("prefetch-chunks must be positive")
    trainer_prefetch_batches = _validate_trainer_prefetch_batches(trainer_prefetch_batches)
    if epoch < 0:
        raise ValueError("epoch must be non-negative")
    if samples_per_epoch is not None and samples_per_epoch <= 0:
        raise ValueError("samples-per-epoch must be positive")
    _validate_non_negative_thresholds(
        min_workload_runs=min_workload_runs,
        min_workload_samples=min_workload_samples,
        min_workload_bytes=min_workload_bytes,
        min_cord_sweep=min_cord_sweep,
    )
    iterator_args = Namespace(
        trainer_batch_iterator=trainer_batch_iterator,
        data_loader_workers=data_loader_workers,
    )
    _validate_trainer_batch_iterator_args(iterator_args)

    normalized_transport = normalize_python_bulk_transport(bulk_transport)
    bulk_read_retries = _validate_bulk_read_retries(bulk_read_retries)
    bulk_write_retries = _validate_bulk_write_retries(
        bulk_read_retries if bulk_write_retries is None else bulk_write_retries
    )
    active_read_mode = TrainerReadMode(read_mode)
    active_tensor_allocation = tensor_allocation or CxnfsTrainerTensorAllocation()
    weighted_plan = plan_weighted_mds_sources(root, weights, remote_root=remote_root)
    sources = weighted_mds_sources_from_root(
        root,
        weights,
        decoder=decoder,
    )
    dataset = CxnfsWeightedTorchDataset(
        sources,
        repeat=repeat,
        rounds_per_read=rounds_per_read,
        max_workers=max_workers,
        source_read_workers=source_read_workers,
        prefetch_chunks=prefetch_chunks,
        include_source_metadata=False,
        shard=shard,
        shuffle=shuffle,
        seed=seed,
        samples_per_epoch=samples_per_epoch,
    )
    args = Namespace(
        batch_size=batch_size,
        epoch=epoch,
        samples_per_epoch=samples_per_epoch,
        trainer_batch_iterator=trainer_batch_iterator,
        data_loader_workers=data_loader_workers,
        trainer_prefetch_batches=trainer_prefetch_batches,
        tensor_target=active_tensor_allocation.target.value,
        tensor_dtype=active_tensor_allocation.dtype,
        tensor_device=active_tensor_allocation.device,
        tensor_non_blocking=str(active_tensor_allocation.non_blocking).lower(),
    )
    try:
        stream = _trainer_stream_from_args(
            dataset,
            args,
            active_read_mode,
            active_tensor_allocation,
            None,
        )
        validated_batches = _collect_trainer_stream_batches(
            stream,
            batches,
            trainer_prefetch_batches,
        )
        return _trainer_contract_report(
            weighted_plan,
            dataset,
            validated_batches,
            root=root,
            remote_root=remote_root,
            bulk_transport=normalized_transport,
            bulk_read_retries=bulk_read_retries,
            bulk_write_retries=bulk_write_retries,
            read_mode=active_read_mode,
            batch_size=batch_size,
            requested_batches=batches,
            rounds_per_read=rounds_per_read,
            max_workers=max_workers,
            source_read_workers=source_read_workers,
            prefetch_chunks=prefetch_chunks,
            trainer_prefetch_batches=trainer_prefetch_batches,
            shuffle=shuffle,
            seed=seed,
            epoch=epoch,
            samples_per_epoch=samples_per_epoch,
            repeat=repeat,
            trainer_batch_iterator=trainer_batch_iterator,
            data_loader_workers=data_loader_workers,
            tensor_allocation=active_tensor_allocation,
            min_workload_runs=min_workload_runs,
            min_workload_samples=min_workload_samples,
            min_workload_bytes=min_workload_bytes,
            min_cord_sweep=min_cord_sweep,
        )
    finally:
        dataset.close()


def run_trainer_contract_cli(argv: Sequence[str] | None = None, stdout=None, stderr=None) -> int:
    parser = ArgumentParser(description="Emit the CXNFS weighted trainer integration contract.")
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--remote-root", help="Remote CXNFS namespace root containing the same source directories.")
    parser.add_argument("--bulk-transport", default=PYTHON_BULK_TRANSPORT_STREAM)
    parser.add_argument("--bulk-read-retries", type=int, default=0)
    parser.add_argument("--bulk-write-retries", type=int)
    parser.add_argument("--read-mode", default=TrainerReadMode.DIRECT_BUFFERS.value, choices=tuple(mode.value for mode in TrainerReadMode))
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--rounds-per-read", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--source-read-workers", type=int)
    parser.add_argument("--prefetch-chunks", type=int, default=1)
    parser.add_argument("--trainer-prefetch-batches", type=int, default=0)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--epoch", type=int, default=0)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_training_shard_args(parser)
    _add_trainer_repeat_args(parser, default=True)
    parser.add_argument("--decoder", default=MdsSampleFormat.RAW.value)
    parser.add_argument("--trainer-batch-iterator", default=TRAINER_BATCH_ITERATOR_NATIVE, choices=TRAINER_BATCH_ITERATORS)
    parser.add_argument("--data-loader-workers", type=int, default=0)
    _add_trainer_tensor_allocation_args(parser, default_values=True)
    parser.add_argument("--min-workload-runs", type=int, default=0, help="Minimum workload benchmark runs expected for this contract.")
    parser.add_argument("--min-workload-samples", type=int, default=0, help="Minimum workload benchmark samples expected for this contract.")
    parser.add_argument("--min-workload-bytes", type=int, default=0, help="Minimum workload benchmark bytes expected for this contract.")
    parser.add_argument("--min-cord-sweep", type=int, default=0, help="Minimum cord sweep entries expected for this contract.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        payload = build_trainer_contract_payload(
            args.root,
            args.weights,
            remote_root=args.remote_root,
            bulk_transport=args.bulk_transport,
            bulk_read_retries=args.bulk_read_retries,
            bulk_write_retries=args.bulk_write_retries,
            read_mode=args.read_mode,
            batch_size=args.batch_size,
            batches=args.batches,
            rounds_per_read=args.rounds_per_read,
            max_workers=args.max_workers,
            source_read_workers=args.source_read_workers,
            prefetch_chunks=args.prefetch_chunks,
            trainer_prefetch_batches=args.trainer_prefetch_batches,
            shuffle=args.shuffle,
            seed=args.seed,
            epoch=args.epoch,
            samples_per_epoch=args.samples_per_epoch,
            repeat=args.repeat,
            decoder=args.decoder,
            trainer_batch_iterator=args.trainer_batch_iterator,
            data_loader_workers=args.data_loader_workers,
            tensor_allocation=_trainer_tensor_allocation_from_args(args),
            shard=_training_shard_from_args(args),
            min_workload_runs=args.min_workload_runs,
            min_workload_samples=args.min_workload_samples,
            min_workload_bytes=args.min_workload_bytes,
            min_cord_sweep=args.min_cord_sweep,
        )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_contract() -> None:
    raise SystemExit(run_trainer_contract_cli())


def build_trainer_surface_check_payload(
    contract_payload: Mapping[str, Any],
    *,
    workload_payload: Mapping[str, Any] | None = None,
    matrix_payload: Mapping[str, Any] | None = None,
    require_remote: bool = False,
    require_quic: bool = False,
    require_workload: bool = False,
    require_matrix: bool = False,
    min_sources: int = 1,
    min_validation_samples: int = 1,
    min_validation_bytes: int = 1,
    min_workload_runs: int = 1,
    min_workload_samples: int = 1,
    min_workload_bytes: int = 1,
    min_cord_sweep: int = 1,
) -> dict[str, Any]:
    contract_requirements = _mapping(contract_payload.get("workload_requirements"))
    min_workload_runs = max(min_workload_runs, _int_value(contract_requirements.get("min_workload_runs"), 0))
    min_workload_samples = max(min_workload_samples, _int_value(contract_requirements.get("min_workload_samples"), 0))
    min_workload_bytes = max(min_workload_bytes, _int_value(contract_requirements.get("min_workload_bytes"), 0))
    min_cord_sweep = max(min_cord_sweep, _int_value(contract_requirements.get("min_cord_sweep"), 0))
    _validate_non_negative_thresholds(
        min_sources=min_sources,
        min_validation_samples=min_validation_samples,
        min_validation_bytes=min_validation_bytes,
        min_workload_runs=min_workload_runs,
        min_workload_samples=min_workload_samples,
        min_workload_bytes=min_workload_bytes,
        min_cord_sweep=min_cord_sweep,
    )
    contract = _mapping(contract_payload)
    python_surface = _mapping(contract.get("python_surface"))
    network_contract = _mapping(contract.get("network_contract"))
    read_contract = _mapping(contract.get("read_contract"))
    validation = _mapping(contract.get("validation"))
    weighted_plan = _mapping(contract.get("weighted_mds_plan"))
    contract_fingerprint = _workload_fingerprint_from_payload(contract, weighted_plan)
    contract_source_count = _weighted_plan_source_count(weighted_plan)
    validation_sample_count = _int_value(validation.get("sample_count"), 0)
    validation_byte_size = _int_value(validation.get("byte_size"), 0)
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    add("contract_format", contract.get("format") == TRAINER_CONTRACT_FORMAT, str(contract.get("format")))
    add("no_mount_required", python_surface.get("requires_mount") is False, str(python_surface.get("requires_mount")))
    add("trainer_code_untouched", python_surface.get("touches_trainer_code") is False, str(python_surface.get("touches_trainer_code")))
    add("weighted_sources_supported", bool(python_surface.get("supports_weighted_sources")), str(python_surface.get("supports_weighted_sources")))
    add("per_source_clients_supported", bool(python_surface.get("supports_per_source_clients")), str(python_surface.get("supports_per_source_clients")))
    add("direct_buffers_supported", bool(python_surface.get("supports_direct_buffers")), str(python_surface.get("supports_direct_buffers")))
    add("direct_tensors_supported", bool(python_surface.get("supports_direct_tensors")), str(python_surface.get("supports_direct_tensors")))
    add("tensor_allocation_policy_supported", bool(python_surface.get("supports_tensor_allocation_policy")), str(python_surface.get("supports_tensor_allocation_policy")))
    add("trainer_dataloader_supported", bool(python_surface.get("supports_trainer_dataloader")), str(python_surface.get("supports_trainer_dataloader")))
    add("dataloader_worker_partitioning_supported", bool(python_surface.get("supports_dataloader_worker_partitioning")), str(python_surface.get("supports_dataloader_worker_partitioning")))
    add("trainer_stream_supported", bool(python_surface.get("supports_trainer_stream")), str(python_surface.get("supports_trainer_stream")))
    add("trainer_cursor_supported", bool(python_surface.get("supports_trainer_cursor")), str(python_surface.get("supports_trainer_cursor")))
    add("password_not_sent_raw", network_contract.get("raw_password_sent_over_control") is False, str(network_contract.get("raw_password_sent_over_control")))
    add(
        "max_cords_ready",
        _int_value(network_contract.get("max_cords"), 0) >= ABSOLUTE_MAX_CORDS,
        str(network_contract.get("max_cords")),
    )
    read_mode = str(read_contract.get("read_mode") or "")
    add("direct_read_mode", read_mode in {TrainerReadMode.DIRECT_BUFFERS.value, TrainerReadMode.DIRECT_TENSORS.value}, read_mode)
    tensor_allocation = _mapping(read_contract.get("tensor_allocation"))
    if read_mode == TrainerReadMode.DIRECT_TENSORS.value:
        add("tensor_allocation_declared", bool(tensor_allocation), str(bool(tensor_allocation)))
        add("tensor_allocation_target_supported", str(tensor_allocation.get("target") or "") in TRAINER_TENSOR_TARGETS, str(tensor_allocation.get("target") or ""))
        add("tensor_allocation_dtype_declared", bool(tensor_allocation.get("dtype")), str(tensor_allocation.get("dtype") or ""))
        add("tensor_allocation_device_declared", bool(tensor_allocation.get("device")), str(tensor_allocation.get("device") or ""))
    add("source_count_meets_minimum", contract_source_count >= min_sources, f"{contract_source_count}>={min_sources}")
    add("validation_batches_present", _int_value(validation.get("batch_count"), 0) > 0, str(validation.get("batch_count")))
    add("validation_samples_meet_minimum", validation_sample_count >= min_validation_samples, f"{validation_sample_count}>={min_validation_samples}")
    add("validation_bytes_meet_minimum", validation_byte_size >= min_validation_bytes, f"{validation_byte_size}>={min_validation_bytes}")
    add("validated_contract", validation.get("validated") is True, str(validation.get("validated")))
    if require_remote:
        add("remote_root_present", bool(contract.get("remote_root")), str(contract.get("remote_root") or ""))
        add("remote_credentials_expected", network_contract.get("remote_reads_require_credentials") is True, str(network_contract.get("remote_reads_require_credentials")))
        add("remote_transport_selected", network_contract.get("transport") != "local-mds", str(network_contract.get("transport")))
    if require_quic:
        add("quic_contract_selected", network_contract.get("transport") == "cxnfs-quic", str(network_contract.get("transport")))
        add("quic_bulk_selected", network_contract.get("bulk_transport") == PYTHON_BULK_TRANSPORT_QUIC, str(network_contract.get("bulk_transport")))
        add("quic_dependency_available", network_contract.get("bulk_transport_available") is True, str(network_contract.get("bulk_transport_available")))

    workload_summary: dict[str, Any] | None = None
    if workload_payload is None:
        if require_workload:
            add("workload_report_present", False, "not provided")
    else:
        workload = _mapping(workload_payload)
        blockfs_gate = _mapping(workload.get("blockfs_gate"))
        workload_plan = _mapping(workload.get("weighted_mds_plan"))
        workload_fingerprint = _workload_fingerprint_from_payload(workload, workload_plan)
        read_path_gate = _trainer_workload_read_path_gate(workload)
        read_path_reliability = _mapping(workload.get("read_path_reliability")) or _trainer_read_path_reliability(workload)
        memory_path_gate = _trainer_workload_memory_path_gate(workload)
        write_path_gate = _trainer_workload_write_path_gate(workload)
        workload_source_count = _weighted_plan_source_count(workload_plan)
        workload_remote_topology = _mapping(workload.get("remote_client_topology"))
        if not workload_remote_topology and isinstance(workload.get("runs"), Sequence):
            workload_remote_topology = _aggregate_trainer_remote_client_topology(
                [
                    _mapping(run)
                    for run in workload.get("runs", ())
                    if isinstance(run, Mapping)
                ],
                workload_source_count,
                bool(require_remote or workload.get("remote_root")),
            )
        workload_remote_topology_gate = _trainer_remote_client_topology_gate(
            workload_remote_topology,
            expected_source_count=workload_source_count,
            require_remote=bool(require_remote or workload.get("remote_root")),
            require_per_source=bool(network_contract.get("per_source_client_default", True)),
        )
        workload_run_count = _int_value(workload.get("run_count"), 0)
        workload_runs_requested = _int_value(workload.get("runs_requested"), workload_run_count)
        workload_sample_count = _int_value(workload.get("sample_count"), 0)
        workload_byte_size = _int_value(workload.get("byte_size"), 0)
        workload_cord_sweep_count = _workload_cord_sweep_count(workload)
        workload_scale_gate = _mapping(blockfs_gate.get("workload_scale_gate"))
        workload_scale_observed = _mapping(workload_scale_gate.get("observed"))
        workload_scale_observed_has_counts = all(
            key in workload_scale_observed
            for key in ("run_count", "sample_count", "byte_size", "cord_sweep_count")
        )
        provenance = _mapping(workload.get("benchmark_provenance"))
        invocation = _mapping(provenance.get("invocation"))
        invocation_fingerprint = _mapping(provenance.get("invocation_fingerprint"))
        workload_summary = {
            "format": workload.get("format"),
            "root": workload.get("root"),
            "transport": workload.get("transport"),
            "bulk_transport": workload.get("bulk_transport"),
            "read_mode": workload.get("read_mode"),
            "tensor_allocation": workload.get("tensor_allocation"),
            "trainer_batch_iterator": workload.get("trainer_batch_iterator"),
            "data_loader_workers": workload.get("data_loader_workers"),
            "trainer_prefetch_batches": workload.get("trainer_prefetch_batches"),
            "bulk_read_retries": workload.get("bulk_read_retries"),
            "bulk_write_retries": workload.get("bulk_write_retries"),
            "weighted_source_count": workload_source_count,
            "workload_fingerprint": workload_fingerprint,
            "benchmark_provenance": {
                "format": provenance.get("format"),
                "invocation_fingerprint": invocation_fingerprint or None,
            },
            "run_count": workload_run_count,
            "runs_requested": workload_runs_requested,
            "sample_count": workload_sample_count,
            "byte_size": workload_byte_size,
            "cord_sweep_count": workload_cord_sweep_count,
            "contract_applied": bool(workload.get("contract_applied")),
            "read_path_evidence_complete": bool(read_path_gate.get("complete")),
            "read_path_reliability_complete": bool(read_path_reliability.get("complete")),
            "unrecovered_read_failures": _int_value(read_path_reliability.get("unrecovered_read_failures"), 0),
            "memory_path_evidence_complete": bool(memory_path_gate.get("complete")),
            "memory_path_gate": memory_path_gate,
            "write_path_evidence_complete": bool(write_path_gate.get("complete")),
            "remote_client_topology": dict(workload_remote_topology) if workload_remote_topology else None,
            "remote_client_topology_complete": bool(workload_remote_topology_gate.get("complete")),
            "remote_client_per_source_complete": bool(workload_remote_topology_gate.get("per_source_client_complete")),
            "remote_client_topology_gate": workload_remote_topology_gate,
            "blockfs_decision": blockfs_gate.get("decision"),
            "workload_scale_complete": bool(blockfs_gate.get("workload_scale_complete")),
            "server_evidence_complete": bool(blockfs_gate.get("server_evidence_complete")),
            "kernel_module_allowed": bool(blockfs_gate.get("kernel_module_allowed")),
            "required_next_evidence": list(blockfs_gate.get("required_next_evidence", ())),
        }
        add("workload_format", workload.get("format") == "cxnfs.trainer-workload-benchmark.v1", str(workload.get("format")))
        add("benchmark_provenance_present", bool(provenance), str(bool(provenance)))
        add("benchmark_provenance_format", provenance.get("format") == TRAINER_WORKLOAD_PROVENANCE_FORMAT, str(provenance.get("format")))
        add("benchmark_invocation_present", bool(invocation), str(bool(invocation)))
        add("benchmark_invocation_fingerprint_present", bool(invocation_fingerprint), str(bool(invocation_fingerprint)))
        add("benchmark_invocation_fingerprint_sha256", _is_sha256_fingerprint(invocation_fingerprint), str(invocation_fingerprint.get("sha256") or ""))
        add("benchmark_root_matches_report", str(invocation.get("root") or "") == str(workload.get("root") or ""), f"{invocation.get('root')}=={workload.get('root')}")
        add("benchmark_weights_match_report", _mapping(invocation.get("weights")) == _mapping(workload.get("weights")), f"{invocation.get('weights')}=={workload.get('weights')}")
        add("benchmark_remote_root_matches_report", invocation.get("remote_root") == workload.get("remote_root"), f"{invocation.get('remote_root')}=={workload.get('remote_root')}")
        add("benchmark_bulk_transport_matches_report", invocation.get("bulk_transport") == workload.get("bulk_transport"), f"{invocation.get('bulk_transport')}=={workload.get('bulk_transport')}")
        add("benchmark_read_mode_matches_report", invocation.get("read_mode") == workload.get("read_mode"), f"{invocation.get('read_mode')}=={workload.get('read_mode')}")
        if workload.get("read_mode") == TrainerReadMode.DIRECT_TENSORS.value:
            add("benchmark_tensor_allocation_matches_report", _mapping(invocation.get("tensor_allocation")) == _mapping(workload.get("tensor_allocation")), "tensor_allocation")
        add("benchmark_trainer_batch_iterator_matches_report", invocation.get("trainer_batch_iterator") == workload.get("trainer_batch_iterator"), f"{invocation.get('trainer_batch_iterator')}=={workload.get('trainer_batch_iterator')}")
        add("benchmark_data_loader_workers_matches_report", _int_value(invocation.get("data_loader_workers"), -1) == _int_value(workload.get("data_loader_workers"), -2), f"{invocation.get('data_loader_workers')}=={workload.get('data_loader_workers')}")
        if invocation.get("trainer_prefetch_batches") is not None or workload.get("trainer_prefetch_batches") is not None:
            add(
                "benchmark_trainer_prefetch_batches_matches_report",
                _int_value(invocation.get("trainer_prefetch_batches"), -1)
                == _int_value(workload.get("trainer_prefetch_batches"), -2),
                f"{invocation.get('trainer_prefetch_batches')}=={workload.get('trainer_prefetch_batches')}",
            )
        if invocation.get("bulk_read_retries") is not None or workload.get("bulk_read_retries") is not None:
            add(
                "benchmark_bulk_read_retries_matches_report",
                _int_value(invocation.get("bulk_read_retries"), -1)
                == _int_value(workload.get("bulk_read_retries"), -2),
                f"{invocation.get('bulk_read_retries')}=={workload.get('bulk_read_retries')}",
            )
        if invocation.get("bulk_write_retries") is not None or workload.get("bulk_write_retries") is not None:
            add(
                "benchmark_bulk_write_retries_matches_report",
                _int_value(invocation.get("bulk_write_retries"), -1)
                == _int_value(workload.get("bulk_write_retries"), -2),
                f"{invocation.get('bulk_write_retries')}=={workload.get('bulk_write_retries')}",
            )
        add("benchmark_runs_match_report", _int_value(invocation.get("runs"), -1) == workload_runs_requested, f"{invocation.get('runs')}=={workload_runs_requested}")
        add("benchmark_cord_sweep_matches_report", _cord_sweep_pairs(invocation.get("cord_sweep")) == _cord_sweep_pairs(workload.get("cord_sweep")), "cord_sweep")
        if contract.get("root") is not None and workload.get("root") is not None:
            add("workload_root_matches_contract", str(workload.get("root")) == str(contract.get("root")), f"{workload.get('root')}=={contract.get('root')}")
        if contract.get("remote_root") is not None or workload.get("remote_root") is not None:
            add("workload_remote_root_matches_contract", workload.get("remote_root") == contract.get("remote_root"), f"{workload.get('remote_root')}=={contract.get('remote_root')}")
        if contract.get("weights") is not None and workload.get("weights") is not None:
            add("workload_weights_match_contract", _mapping(workload.get("weights")) == _mapping(contract.get("weights")), f"{workload.get('weights')}=={contract.get('weights')}")
        if read_contract.get("trainer_batch_iterator") is not None and workload.get("trainer_batch_iterator") is not None:
            add("workload_trainer_batch_iterator_matches_contract", workload.get("trainer_batch_iterator") == read_contract.get("trainer_batch_iterator"), f"{workload.get('trainer_batch_iterator')}=={read_contract.get('trainer_batch_iterator')}")
        if read_contract.get("data_loader_workers") is not None and workload.get("data_loader_workers") is not None:
            add("workload_data_loader_workers_match_contract", _int_value(workload.get("data_loader_workers"), -1) == _int_value(read_contract.get("data_loader_workers"), -2), f"{workload.get('data_loader_workers')}=={read_contract.get('data_loader_workers')}")
        if read_contract.get("trainer_prefetch_batches") is not None and workload.get("trainer_prefetch_batches") is not None:
            add("workload_trainer_prefetch_batches_match_contract", _int_value(workload.get("trainer_prefetch_batches"), -1) == _int_value(read_contract.get("trainer_prefetch_batches"), -2), f"{workload.get('trainer_prefetch_batches')}=={read_contract.get('trainer_prefetch_batches')}")
        if network_contract.get("bulk_read_retries") is not None and workload.get("bulk_read_retries") is not None:
            add("workload_bulk_read_retries_match_contract", _int_value(workload.get("bulk_read_retries"), -1) == _int_value(network_contract.get("bulk_read_retries"), -2), f"{workload.get('bulk_read_retries')}=={network_contract.get('bulk_read_retries')}")
        if network_contract.get("bulk_write_retries") is not None and workload.get("bulk_write_retries") is not None:
            add("workload_bulk_write_retries_match_contract", _int_value(workload.get("bulk_write_retries"), -1) == _int_value(network_contract.get("bulk_write_retries"), -2), f"{workload.get('bulk_write_retries')}=={network_contract.get('bulk_write_retries')}")
        if read_contract.get("read_mode") == TrainerReadMode.DIRECT_TENSORS.value or workload.get("read_mode") == TrainerReadMode.DIRECT_TENSORS.value:
            add("workload_tensor_allocation_matches_contract", _mapping(workload.get("tensor_allocation")) == _mapping(read_contract.get("tensor_allocation")), "tensor_allocation")
        add("workload_direct_read_mode", str(workload.get("read_mode") or "").startswith("direct-"), str(workload.get("read_mode") or ""))
        add(
            "workload_memory_path",
            bool(memory_path_gate.get("complete")),
            ", ".join(str(value) for value in memory_path_gate.get("missing", ())) or "complete",
        )
        add("workload_source_count_meets_minimum", workload_source_count >= min_sources, f"{workload_source_count}>={min_sources}")
        add(
            "workload_fingerprint_matches_contract",
            _fingerprint_sha256(contract_fingerprint) == _fingerprint_sha256(workload_fingerprint),
            f"{_fingerprint_sha256(contract_fingerprint) or 'missing'}=={_fingerprint_sha256(workload_fingerprint) or 'missing'}",
        )
        add("workload_runs_meet_minimum", workload_run_count >= min_workload_runs, f"{workload_run_count}>={min_workload_runs}")
        add("workload_samples_meet_minimum", workload_sample_count >= min_workload_samples, f"{workload_sample_count}>={min_workload_samples}")
        add("workload_bytes_meet_minimum", workload_byte_size >= min_workload_bytes, f"{workload_byte_size}>={min_workload_bytes}")
        add("workload_cord_sweep_meets_minimum", workload_cord_sweep_count >= min_cord_sweep, f"{workload_cord_sweep_count}>={min_cord_sweep}")
        add("workload_blockfs_gate_present", bool(blockfs_gate), str(bool(blockfs_gate)))
        add(
            "workload_blockfs_read_gate_present",
            "read_path_evidence_complete" in blockfs_gate and isinstance(blockfs_gate.get("read_path_gate"), Mapping),
            str("read_path_evidence_complete" in blockfs_gate and isinstance(blockfs_gate.get("read_path_gate"), Mapping)),
        )
        add(
            "workload_blockfs_memory_gate_present",
            "memory_path_evidence_complete" in blockfs_gate and isinstance(blockfs_gate.get("memory_path_gate"), Mapping),
            str("memory_path_evidence_complete" in blockfs_gate and isinstance(blockfs_gate.get("memory_path_gate"), Mapping)),
        )
        add(
            "workload_blockfs_write_gate_present",
            "write_path_evidence_complete" in blockfs_gate and isinstance(blockfs_gate.get("write_path_gate"), Mapping),
            str("write_path_evidence_complete" in blockfs_gate and isinstance(blockfs_gate.get("write_path_gate"), Mapping)),
        )
        add(
            "workload_blockfs_scale_gate_present",
            "workload_scale_complete" in blockfs_gate and isinstance(blockfs_gate.get("workload_scale_gate"), Mapping),
            str("workload_scale_complete" in blockfs_gate and isinstance(blockfs_gate.get("workload_scale_gate"), Mapping)),
        )
        add(
            "workload_blockfs_scale_observed_matches_report",
            (
                not workload_scale_observed_has_counts
                or (
                    _int_value(workload_scale_observed.get("run_count"), -1) == workload_run_count
                    and _int_value(workload_scale_observed.get("sample_count"), -1) == workload_sample_count
                    and _int_value(workload_scale_observed.get("byte_size"), -1) == workload_byte_size
                    and _int_value(workload_scale_observed.get("cord_sweep_count"), -1) == workload_cord_sweep_count
                )
            ),
            "scale observed counts",
        )
        if require_remote:
            add("workload_remote_transport", workload.get("transport") != "local-mds", str(workload.get("transport")))
            add(
                "workload_remote_client_topology",
                bool(workload_remote_topology_gate.get("complete")),
                ", ".join(str(value) for value in workload_remote_topology_gate.get("missing", ())) or "complete",
            )
        if require_quic:
            add("workload_quic_transport", workload.get("transport") == "cxnfs-quic", str(workload.get("transport")))
            add("workload_quic_bulk", workload.get("bulk_transport") == PYTHON_BULK_TRANSPORT_QUIC, str(workload.get("bulk_transport")))
            add(
                "workload_quic_read_path",
                bool(read_path_gate.get("complete")),
                ", ".join(str(value) for value in read_path_gate.get("missing", ())) or "complete",
            )
            add(
                "workload_quic_read_reliability",
                bool(read_path_reliability.get("complete")),
                ", ".join(str(value) for value in read_path_reliability.get("missing", ())) or "complete",
            )
            add(
                "workload_quic_memory_path",
                bool(memory_path_gate.get("complete")),
                ", ".join(str(value) for value in memory_path_gate.get("missing", ())) or "complete",
            )
            add(
                "workload_quic_write_path",
                bool(write_path_gate.get("complete")),
                ", ".join(str(value) for value in write_path_gate.get("missing", ())) or "complete",
            )

    matrix_summary: dict[str, Any] | None = None
    if matrix_payload is None:
        if require_matrix:
            add("matrix_report_present", False, "not provided")
    else:
        matrix = _mapping(matrix_payload)
        matrix_plan = _mapping(matrix.get("weighted_mds_plan"))
        matrix_fingerprint = _workload_fingerprint_from_payload(matrix, matrix_plan)
        matrix_source_count = _weighted_plan_source_count(matrix_plan)
        matrix_cases = [
            _mapping(case)
            for case in matrix.get("cases", ())
            if isinstance(case, Mapping)
        ]
        matrix_case_names = {str(case.get("case_name") or "") for case in matrix_cases}
        matrix_read_modes = {str(case.get("read_mode") or "") for case in matrix_cases}
        matrix_iterators = {str(case.get("trainer_batch_iterator") or "") for case in matrix_cases}
        matrix_memory_paths_complete = bool(matrix_cases) and all(
            _mapping(case.get("memory_path_gate")).get("complete") is True
            for case in matrix_cases
        )
        matrix_case_samples_present = bool(matrix_cases) and all(
            _int_value(case.get("sample_count"), 0) > 0
            for case in matrix_cases
        )
        matrix_quic_read_paths_complete = bool(matrix_cases) and all(
            _matrix_case_quic_read_path_complete(case)
            for case in matrix_cases
        )
        matrix_remote_topology = _mapping(matrix.get("remote_client_topology"))
        if not matrix_remote_topology:
            matrix_remote_topology = _aggregate_trainer_remote_client_topology(
                matrix_cases,
                matrix_source_count,
                bool(require_remote or matrix.get("remote_root")),
            )
        matrix_remote_topology_gate = _trainer_remote_client_topology_gate(
            matrix_remote_topology,
            expected_source_count=matrix_source_count,
            require_remote=bool(require_remote or matrix.get("remote_root")),
            require_per_source=bool(network_contract.get("per_source_client_default", True)),
        )
        matrix_read_path_reliability_complete = bool(matrix_cases) and all(
            _mapping(case.get("read_path_reliability")).get("complete") is True
            for case in matrix_cases
        )
        matrix_worker_partition_required = any(
            str(case.get("trainer_batch_iterator") or "") == TRAINER_BATCH_ITERATOR_DATALOADER
            and _int_value(case.get("data_loader_workers"), 0) > 0
            for case in matrix_cases
        )
        matrix_worker_partition_complete = (
            not matrix_worker_partition_required
            or (
                bool(matrix_cases)
                and all(
                    _mapping(case.get("worker_partition_evidence")).get("complete") is True
                    for case in matrix_cases
                    if str(case.get("trainer_batch_iterator") or "") == TRAINER_BATCH_ITERATOR_DATALOADER
                    and _int_value(case.get("data_loader_workers"), 0) > 0
                )
            )
        )
        matrix_worker_partition_duplicates = sum(
            _int_value(_mapping(case.get("worker_partition_evidence")).get("duplicate_source_sample_count"), 0)
            for case in matrix_cases
        )
        matrix_unrecovered_read_failures = sum(
            _int_value(_mapping(case.get("read_path_reliability")).get("unrecovered_read_failures"), 0)
            for case in matrix_cases
        )
        tensor_targets = [
            str(_mapping(case.get("tensor_allocation")).get("target") or "")
            for case in matrix_cases
            if case.get("read_mode") == TrainerReadMode.DIRECT_TENSORS.value
        ]
        unsupported_tensor_targets = [
            target for target in tensor_targets if target not in TRAINER_TENSOR_TARGETS
        ]
        matrix_contract_iterator = str(read_contract.get("trainer_batch_iterator") or TRAINER_BATCH_ITERATOR_NATIVE)
        matrix_contract_prefetch = _int_value(read_contract.get("trainer_prefetch_batches"), 0)
        matrix_contract_bulk_read_retries = _int_value(network_contract.get("bulk_read_retries"), 0)
        matrix_summary = {
            "format": matrix.get("format"),
            "root": matrix.get("root"),
            "remote_root": matrix.get("remote_root"),
            "transport": matrix.get("transport"),
            "bulk_transport": matrix.get("bulk_transport"),
            "requires_mount": matrix.get("requires_mount"),
            "touches_trainer_code": matrix.get("touches_trainer_code"),
            "case_count": _int_value(matrix.get("case_count"), len(matrix_cases)),
            "passed_case_count": _int_value(matrix.get("passed_case_count"), 0),
            "failed_case_count": _int_value(matrix.get("failed_case_count"), 0),
            "case_names": sorted(matrix_case_names),
            "read_modes": sorted(matrix_read_modes),
            "trainer_batch_iterators": sorted(matrix_iterators),
            "trainer_prefetch_batches": matrix.get("trainer_prefetch_batches"),
            "bulk_read_retries": matrix.get("bulk_read_retries"),
            "weighted_source_count": matrix_source_count,
            "workload_fingerprint": matrix_fingerprint,
            "memory_paths_complete": matrix_memory_paths_complete,
            "case_samples_present": matrix_case_samples_present,
            "quic_read_paths_complete": matrix_quic_read_paths_complete,
            "read_path_reliability_complete": matrix_read_path_reliability_complete,
            "remote_client_topology": dict(matrix_remote_topology) if matrix_remote_topology else None,
            "remote_client_topology_complete": bool(matrix_remote_topology_gate.get("complete")),
            "remote_client_per_source_complete": bool(matrix_remote_topology_gate.get("per_source_client_complete")),
            "remote_client_topology_gate": matrix_remote_topology_gate,
            "worker_partition_required": matrix_worker_partition_required,
            "worker_partition_complete": matrix_worker_partition_complete,
            "worker_partition_duplicate_source_sample_count": matrix_worker_partition_duplicates,
            "unrecovered_read_failures": matrix_unrecovered_read_failures,
            "tensor_targets": sorted(set(tensor_targets)),
        }
        add("matrix_format", matrix.get("format") == TRAINER_SURFACE_MATRIX_FORMAT, str(matrix.get("format")))
        add("matrix_passed", matrix.get("passed") is True, str(matrix.get("passed")))
        add("matrix_no_mount_required", matrix.get("requires_mount") is False, str(matrix.get("requires_mount")))
        add("matrix_trainer_code_untouched", matrix.get("touches_trainer_code") is False, str(matrix.get("touches_trainer_code")))
        add("matrix_cases_present", bool(matrix_cases), str(len(matrix_cases)))
        add("matrix_all_cases_passed", bool(matrix_cases) and all(case.get("passed") is True for case in matrix_cases), str(len(matrix_cases)))
        add("matrix_case_samples_present", matrix_case_samples_present, str(len(matrix_cases)))
        add("matrix_memory_paths_complete", matrix_memory_paths_complete, "complete" if matrix_memory_paths_complete else "incomplete")
        add("matrix_source_count_meets_minimum", matrix_source_count >= min_sources, f"{matrix_source_count}>={min_sources}")
        add(
            "matrix_fingerprint_matches_contract",
            _fingerprint_sha256(contract_fingerprint) == _fingerprint_sha256(matrix_fingerprint),
            f"{_fingerprint_sha256(contract_fingerprint) or 'missing'}=={_fingerprint_sha256(matrix_fingerprint) or 'missing'}",
        )
        add("matrix_contract_read_mode_covered", read_mode in matrix_read_modes, read_mode)
        add("matrix_contract_iterator_covered", matrix_contract_iterator in matrix_iterators, matrix_contract_iterator)
        if matrix_contract_prefetch > 0 or matrix.get("trainer_prefetch_batches") is not None:
            add(
                "matrix_trainer_prefetch_batches_matches_contract",
                _int_value(matrix.get("trainer_prefetch_batches"), -1) == matrix_contract_prefetch,
                f"{matrix.get('trainer_prefetch_batches')}=={matrix_contract_prefetch}",
            )
        if matrix_contract_bulk_read_retries > 0 or matrix.get("bulk_read_retries") is not None:
            add(
                "matrix_bulk_read_retries_matches_contract",
                _int_value(matrix.get("bulk_read_retries"), -1) == matrix_contract_bulk_read_retries,
                f"{matrix.get('bulk_read_retries')}=={matrix_contract_bulk_read_retries}",
            )
        add("matrix_direct_buffers_native_present", "direct-buffers/native" in matrix_case_names, str("direct-buffers/native" in matrix_case_names))
        add("matrix_direct_buffers_dataloader_present", "direct-buffers/trainer-dataloader" in matrix_case_names, str("direct-buffers/trainer-dataloader" in matrix_case_names))
        add("matrix_direct_tensors_native_present", "direct-tensors/native" in matrix_case_names, str("direct-tensors/native" in matrix_case_names))
        add("matrix_direct_tensors_dataloader_present", "direct-tensors/trainer-dataloader" in matrix_case_names, str("direct-tensors/trainer-dataloader" in matrix_case_names))
        add("matrix_tensor_targets_supported", bool(tensor_targets) and not unsupported_tensor_targets, ",".join(unsupported_tensor_targets) or "supported")
        if contract.get("remote_root") is not None or matrix.get("remote_root") is not None:
            add("matrix_remote_root_matches_contract", matrix.get("remote_root") == contract.get("remote_root"), f"{matrix.get('remote_root')}=={contract.get('remote_root')}")
        if require_remote:
            add("matrix_remote_transport", matrix.get("transport") != "local-mds", str(matrix.get("transport")))
            add(
                "matrix_remote_client_topology",
                bool(matrix_remote_topology_gate.get("complete")),
                ", ".join(str(value) for value in matrix_remote_topology_gate.get("missing", ())) or "complete",
            )
        if require_quic:
            add("matrix_quic_transport", matrix.get("transport") == "cxnfs-quic", str(matrix.get("transport")))
            add("matrix_quic_bulk", matrix.get("bulk_transport") == PYTHON_BULK_TRANSPORT_QUIC, str(matrix.get("bulk_transport")))
            add("matrix_quic_read_paths", matrix_quic_read_paths_complete, "complete" if matrix_quic_read_paths_complete else "incomplete")
            add(
                "matrix_quic_read_reliability",
                matrix_read_path_reliability_complete and matrix_unrecovered_read_failures == 0,
                "complete" if matrix_read_path_reliability_complete else "incomplete",
            )
        if matrix_worker_partition_required:
            add(
                "matrix_worker_partition_complete",
                matrix_worker_partition_complete and matrix_worker_partition_duplicates == 0,
                f"duplicates={matrix_worker_partition_duplicates}",
            )

    passed = all(check["passed"] for check in checks)
    return {
        "format": TRAINER_SURFACE_CHECK_FORMAT,
        "passed": passed,
        "status": "passed" if passed else "failed",
        "requirements": {
            "require_remote": require_remote,
            "require_quic": require_quic,
            "require_workload": require_workload,
            "require_matrix": require_matrix,
            "min_sources": min_sources,
            "min_validation_samples": min_validation_samples,
            "min_validation_bytes": min_validation_bytes,
            "min_workload_runs": min_workload_runs,
            "min_workload_samples": min_workload_samples,
            "min_workload_bytes": min_workload_bytes,
            "min_cord_sweep": min_cord_sweep,
            "contract_workload_requirements": dict(contract_requirements) if contract_requirements else None,
        },
        "contract": {
            "format": contract.get("format"),
            "remote_root": contract.get("remote_root"),
            "transport": network_contract.get("transport"),
            "bulk_transport": network_contract.get("bulk_transport"),
            "bulk_read_retries": network_contract.get("bulk_read_retries"),
            "bulk_write_retries": network_contract.get("bulk_write_retries"),
            "read_mode": read_contract.get("read_mode"),
            "trainer_batch_iterator": read_contract.get("trainer_batch_iterator"),
            "data_loader_workers": read_contract.get("data_loader_workers"),
            "tensor_allocation": read_contract.get("tensor_allocation"),
            "weighted_source_count": contract_source_count,
            "workload_fingerprint": contract_fingerprint,
            "validation_batch_count": _int_value(validation.get("batch_count"), 0),
            "validation_sample_count": validation_sample_count,
            "validation_byte_size": validation_byte_size,
        },
        "workload": workload_summary,
        "matrix": matrix_summary,
        "checks": checks,
        "failed_checks": [check for check in checks if not check["passed"]],
    }


def _matrix_case_quic_read_path_complete(case: Mapping[str, Any]) -> bool:
    evidence = _mapping(case.get("read_path_evidence"))
    return (
        bool(evidence.get("transport_frame_sink_observed"))
        and bool(evidence.get("frame_sink_used_for_all_direct_destination_reads"))
        and _int_value(evidence.get("direct_destination_read_operations"), 0) > 0
        and _direct_destination_frame_sink_cord_fanout_verified(evidence)
        and bool(evidence.get("quic_read_transport_complete"))
        and _quic_read_transport_stream_eof_verified(evidence)
    )


def _quic_read_transport_stream_eof_verified(evidence: Mapping[str, Any]) -> bool:
    attempted_read_streams = _int_value(evidence.get("quic_read_transport_attempted_read_streams"), 0)
    eof_verified_streams = _int_value(evidence.get("quic_read_transport_read_streams_eof_verified"), 0)
    if attempted_read_streams > 0 or eof_verified_streams > 0:
        return (
            attempted_read_streams > 0
            and eof_verified_streams == attempted_read_streams
            and _int_value(evidence.get("quic_read_transport_failed_read_operations"), 0) == 0
        )
    if bool(evidence.get("quic_read_transport_stream_eof_verified")):
        return True
    if bool(evidence.get("read_stream_eof_verified")):
        return True
    bulk_transport = _mapping(evidence.get("bulk_transport_read_evidence"))
    attempted_read_streams = _int_value(bulk_transport.get("attempted_read_streams"), 0)
    eof_verified_streams = _int_value(bulk_transport.get("read_streams_eof_verified"), 0)
    if attempted_read_streams > 0 or eof_verified_streams > 0:
        return (
            attempted_read_streams > 0
            and eof_verified_streams == attempted_read_streams
            and _int_value(bulk_transport.get("failed_read_operations"), 0) == 0
        )
    return bool(bulk_transport.get("read_stream_eof_verified"))


def _direct_destination_max_observed_cords(evidence: Mapping[str, Any]) -> int:
    direct_max = _int_value(evidence.get("direct_destination_max_observed_cords"), 0)
    if direct_max > 0:
        return direct_max
    if (
        bool(evidence.get("direct_destination_reads_observed"))
        or _int_value(evidence.get("direct_destination_read_operations"), 0) > 0
    ):
        return _int_value(evidence.get("max_observed_cords"), 0)
    return 0


def _direct_destination_frame_sink_cord_fanout_verified(evidence: Mapping[str, Any]) -> bool:
    if "direct_destination_frame_sink_cord_fanout_verified" in evidence:
        return bool(evidence.get("direct_destination_frame_sink_cord_fanout_verified"))
    return (
        bool(evidence.get("frame_sink_used_for_all_direct_destination_reads"))
        and _direct_destination_max_observed_cords(evidence) > 1
    )


def run_trainer_surface_check_cli(argv: Sequence[str] | None = None, stdout=None, stderr=None) -> int:
    parser = ArgumentParser(description="Validate a CXNFS trainer contract and optional workload report.")
    parser.add_argument("contract", help="Path to a cxnfs.trainer-contract.v1 JSON report.")
    parser.add_argument("--workload-report", help="Optional cxnfs.trainer-workload-benchmark.v1 JSON report.")
    parser.add_argument("--matrix-report", help="Optional cxnfs.trainer-surface-matrix.v1 JSON report.")
    parser.add_argument("--require-workload", action="store_true", help="Fail unless --workload-report is provided.")
    parser.add_argument("--require-matrix", action="store_true", help="Fail unless --matrix-report is provided.")
    parser.add_argument("--require-remote", action="store_true", help="Require remote CXNFS reads instead of local-only reads.")
    parser.add_argument("--require-quic", action="store_true", help="Require QUIC transport and read-path proof.")
    parser.add_argument("--min-sources", type=int, default=1, help="Minimum weighted source count required in contract and workload reports.")
    parser.add_argument("--min-validation-samples", type=int, default=1, help="Minimum validation sample count required in the contract.")
    parser.add_argument("--min-validation-bytes", type=int, default=1, help="Minimum validation byte count required in the contract.")
    parser.add_argument("--min-workload-runs", type=int, default=1, help="Minimum run count required when a workload report is checked.")
    parser.add_argument("--min-workload-samples", type=int, default=1, help="Minimum sample count required when a workload report is checked.")
    parser.add_argument("--min-workload-bytes", type=int, default=1, help="Minimum byte count required when a workload report is checked.")
    parser.add_argument("--min-cord-sweep", type=int, default=1, help="Minimum cord sweep entry count required when a workload report is checked.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        contract = _read_json_mapping(Path(args.contract), "trainer contract")
        workload = _read_json_mapping(Path(args.workload_report), "trainer workload") if args.workload_report else None
        matrix = _read_json_mapping(Path(args.matrix_report), "trainer surface matrix") if args.matrix_report else None
        payload = build_trainer_surface_check_payload(
            contract,
            workload_payload=workload,
            matrix_payload=matrix,
            require_remote=args.require_remote,
            require_quic=args.require_quic,
            require_workload=args.require_workload,
            require_matrix=args.require_matrix,
            min_sources=args.min_sources,
            min_validation_samples=args.min_validation_samples,
            min_validation_bytes=args.min_validation_bytes,
            min_workload_runs=args.min_workload_runs,
            min_workload_samples=args.min_workload_samples,
            min_workload_bytes=args.min_workload_bytes,
            min_cord_sweep=args.min_cord_sweep,
        )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_surface_check() -> None:
    raise SystemExit(run_trainer_surface_check_cli())


def build_trainer_handoff_payload(
    contract_payload: Mapping[str, Any],
    *,
    contract_path: str | Path | None = None,
    workload_payload: Mapping[str, Any] | None = None,
    workload_path: str | Path | None = None,
    matrix_payload: Mapping[str, Any] | None = None,
    matrix_path: str | Path | None = None,
    workload_decision_payload: Mapping[str, Any] | None = None,
    workload_decision_path: str | Path | None = None,
    blockfs_handoff_payload: Mapping[str, Any] | None = None,
    blockfs_handoff_path: str | Path | None = None,
    batch_direct_read_payload: Mapping[str, Any] | None = None,
    batch_direct_read_path: str | Path | None = None,
    batch_direct_tensor_read_payload: Mapping[str, Any] | None = None,
    batch_direct_tensor_read_path: str | Path | None = None,
    require_low_level_direct_read_proofs: bool = False,
    require_low_level_direct_write_proofs: bool = False,
    require_remote: bool = False,
    require_quic: bool = False,
    require_workload: bool = False,
    require_matrix: bool = False,
    min_sources: int = 1,
    min_validation_samples: int = 1,
    min_validation_bytes: int = 1,
    min_workload_runs: int = 1,
    min_workload_samples: int = 1,
    min_workload_bytes: int = 1,
    min_cord_sweep: int = 1,
) -> dict[str, Any]:
    surface_check = build_trainer_surface_check_payload(
        contract_payload,
        workload_payload=workload_payload,
        matrix_payload=matrix_payload,
        require_remote=require_remote,
        require_quic=require_quic,
        require_workload=require_workload,
        require_matrix=require_matrix,
        min_sources=min_sources,
        min_validation_samples=min_validation_samples,
        min_validation_bytes=min_validation_bytes,
        min_workload_runs=min_workload_runs,
        min_workload_samples=min_workload_samples,
        min_workload_bytes=min_workload_bytes,
        min_cord_sweep=min_cord_sweep,
    )
    failed_checks = [
        str(_mapping(check).get("name") or "")
        for check in surface_check.get("failed_checks", ())
        if isinstance(check, Mapping)
    ]
    decision = _mapping(workload_decision_payload)
    blockfs_handoff = _mapping(blockfs_handoff_payload)
    remote_quic_requested = bool(require_remote and require_quic)
    read_proofs_required = bool(require_low_level_direct_read_proofs or remote_quic_requested)
    write_proofs_required = bool(require_low_level_direct_write_proofs)
    direct_read_proofs = _trainer_handoff_low_level_direct_read_proofs_summary(
        batch_direct_read_payload,
        batch_direct_read_path,
        batch_direct_tensor_read_payload,
        batch_direct_tensor_read_path,
        required=read_proofs_required,
    )
    direct_write_proofs = _trainer_handoff_low_level_direct_write_proofs_summary(
        workload_payload,
        workload_path,
        required=write_proofs_required,
    )
    proofs_ready = (
        (not read_proofs_required or bool(direct_read_proofs.get("complete")))
        and (not write_proofs_required or bool(direct_write_proofs.get("complete")))
    )
    ready_for_trainer = bool(surface_check.get("passed")) and proofs_ready
    ready_for_remote_quic = bool(
        ready_for_trainer
        and remote_quic_requested
        and bool(direct_read_proofs.get("complete"))
    )
    ready_for_blockfs = bool(
        blockfs_handoff.get("kernel_module_allowed")
        or decision.get("kernel_module_allowed")
    )
    next_actions = _dedupe_strings(
        [
            *failed_checks,
            *[str(value) for value in _mapping(surface_check.get("workload")).get("required_next_evidence", ())],
            *[str(value) for value in decision.get("required_next_evidence", ())],
            *[str(value) for value in blockfs_handoff.get("required_next_evidence", ())],
            *(
                [str(value) for value in direct_read_proofs.get("missing", ())]
                if read_proofs_required
                else []
            ),
            *(
                [str(value) for value in direct_write_proofs.get("missing", ())]
                if write_proofs_required
                else []
            ),
        ]
    )
    return {
        "format": TRAINER_HANDOFF_FORMAT,
        "status": "ready" if ready_for_trainer else "deferred",
        "ready_for_trainer_wiring": ready_for_trainer,
        "ready_for_remote_quic": ready_for_remote_quic,
        "ready_for_blockfs_candidate": ready_for_blockfs,
        "requires_mount": bool(_mapping(contract_payload.get("python_surface")).get("requires_mount")),
        "touches_trainer_code": bool(_mapping(contract_payload.get("python_surface")).get("touches_trainer_code")),
        "contract": _trainer_handoff_contract_summary(contract_payload, contract_path),
        "surface_check": _trainer_handoff_surface_summary(surface_check),
        "workload": _trainer_handoff_workload_summary(workload_payload, workload_path),
        "matrix": _trainer_handoff_matrix_summary(matrix_payload, matrix_path),
        "workload_decision": _trainer_handoff_decision_summary(
            workload_decision_payload,
            workload_decision_path,
        ),
        "blockfs_handoff": _trainer_handoff_blockfs_summary(
            blockfs_handoff_payload,
            blockfs_handoff_path,
        ),
        "low_level_direct_read_proofs": direct_read_proofs,
        "low_level_direct_write_proofs": direct_write_proofs,
        "integration_surface": _trainer_handoff_integration_surface(contract_payload),
        "integration_plan": _trainer_handoff_integration_plan(contract_payload),
        "requirements": dict(_mapping(surface_check.get("requirements"))),
        "failed_checks": list(surface_check.get("failed_checks", ())),
        "next_actions": next_actions,
    }


def run_trainer_handoff_cli(argv: Sequence[str] | None = None, stdout=None, stderr=None) -> int:
    parser = ArgumentParser(description="Build one CXNFS trainer handoff report from contract and benchmark evidence.")
    parser.add_argument("contract", help="Path to a cxnfs.trainer-contract.v1 JSON report.")
    parser.add_argument("--workload-report", help="Optional cxnfs.trainer-workload-benchmark.v1 JSON report.")
    parser.add_argument("--matrix-report", help="Optional cxnfs.trainer-surface-matrix.v1 JSON report.")
    parser.add_argument("--workload-decision", help="Optional cxnfs.workload-decision.v1 JSON report.")
    parser.add_argument("--blockfs-handoff", help="Optional cxnfs.blockfs-handoff.v1 JSON report.")
    parser.add_argument("--batch-direct-read-report", help="Optional cxnfs.quic-batch-direct-read.v1 JSON report.")
    parser.add_argument("--batch-direct-tensor-read-report", help="Optional cxnfs.quic-batch-direct-tensor-read.v1 JSON report.")
    parser.add_argument("--surface-check-json", help="Optional path to write the generated surface-check report.")
    parser.add_argument("--require-low-level-direct-read-proofs", action="store_true", help="Fail unless batch direct buffer and tensor QUIC proofs are provided and complete.")
    parser.add_argument("--require-low-level-direct-write-proofs", action="store_true", help="Fail unless workload QUIC write probes prove direct checkpoint-style writes.")
    parser.add_argument("--require-workload", action="store_true", help="Fail unless --workload-report is provided.")
    parser.add_argument("--require-matrix", action="store_true", help="Fail unless --matrix-report is provided.")
    parser.add_argument("--require-remote", action="store_true", help="Require remote CXNFS reads instead of local-only reads.")
    parser.add_argument("--require-quic", action="store_true", help="Require QUIC transport and read-path proof.")
    parser.add_argument("--min-sources", type=int, default=1, help="Minimum weighted source count required in contract and workload reports.")
    parser.add_argument("--min-validation-samples", type=int, default=1, help="Minimum validation sample count required in the contract.")
    parser.add_argument("--min-validation-bytes", type=int, default=1, help="Minimum validation byte count required in the contract.")
    parser.add_argument("--min-workload-runs", type=int, default=1, help="Minimum run count required when a workload report is checked.")
    parser.add_argument("--min-workload-samples", type=int, default=1, help="Minimum sample count required when a workload report is checked.")
    parser.add_argument("--min-workload-bytes", type=int, default=1, help="Minimum byte count required when a workload report is checked.")
    parser.add_argument("--min-cord-sweep", type=int, default=1, help="Minimum cord sweep entry count required when a workload report is checked.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        contract_path = Path(args.contract)
        workload_path = Path(args.workload_report) if args.workload_report else None
        matrix_path = Path(args.matrix_report) if args.matrix_report else None
        decision_path = Path(args.workload_decision) if args.workload_decision else None
        blockfs_path = Path(args.blockfs_handoff) if args.blockfs_handoff else None
        batch_direct_read_path = Path(args.batch_direct_read_report) if args.batch_direct_read_report else None
        batch_direct_tensor_read_path = Path(args.batch_direct_tensor_read_report) if args.batch_direct_tensor_read_report else None
        contract = _read_json_mapping(contract_path, "trainer contract")
        workload = _read_json_mapping(workload_path, "trainer workload") if workload_path else None
        matrix = _read_json_mapping(matrix_path, "trainer surface matrix") if matrix_path else None
        decision = _read_json_mapping(decision_path, "workload decision") if decision_path else None
        blockfs = _read_json_mapping(blockfs_path, "blockfs handoff") if blockfs_path else None
        batch_direct_read = (
            _read_json_mapping(batch_direct_read_path, "batch direct read proof")
            if batch_direct_read_path
            else None
        )
        batch_direct_tensor_read = (
            _read_json_mapping(batch_direct_tensor_read_path, "batch direct tensor read proof")
            if batch_direct_tensor_read_path
            else None
        )
        payload = build_trainer_handoff_payload(
            contract,
            contract_path=contract_path,
            workload_payload=workload,
            workload_path=workload_path,
            matrix_payload=matrix,
            matrix_path=matrix_path,
            workload_decision_payload=decision,
            workload_decision_path=decision_path,
            blockfs_handoff_payload=blockfs,
            blockfs_handoff_path=blockfs_path,
            batch_direct_read_payload=batch_direct_read,
            batch_direct_read_path=batch_direct_read_path,
            batch_direct_tensor_read_payload=batch_direct_tensor_read,
            batch_direct_tensor_read_path=batch_direct_tensor_read_path,
            require_low_level_direct_read_proofs=args.require_low_level_direct_read_proofs,
            require_low_level_direct_write_proofs=args.require_low_level_direct_write_proofs,
            require_remote=args.require_remote,
            require_quic=args.require_quic,
            require_workload=args.require_workload,
            require_matrix=args.require_matrix,
            min_sources=args.min_sources,
            min_validation_samples=args.min_validation_samples,
            min_validation_bytes=args.min_validation_bytes,
            min_workload_runs=args.min_workload_runs,
            min_workload_samples=args.min_workload_samples,
            min_workload_bytes=args.min_workload_bytes,
            min_cord_sweep=args.min_cord_sweep,
        )
        indent = None if args.compact else 2
        if args.surface_check_json:
            surface_path = Path(args.surface_check_json)
            surface_path.parent.mkdir(parents=True, exist_ok=True)
            surface_path.write_text(
                json.dumps(payload["surface_check"]["payload"], indent=indent, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["ready_for_trainer_wiring"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_handoff() -> None:
    raise SystemExit(run_trainer_handoff_cli())


@dataclass(frozen=True)
class CxnfsTrainerIntegrationPlan:
    payload: Mapping[str, Any]

    @classmethod
    def from_handoff_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        require_ready: bool = True,
        require_remote_quic: bool = False,
    ) -> CxnfsTrainerIntegrationPlan:
        if not isinstance(payload, Mapping):
            raise TypeError("trainer handoff payload must be a mapping")
        if payload.get("format") != TRAINER_HANDOFF_FORMAT:
            raise ValueError("trainer handoff format is not cxnfs.trainer-handoff.v1")
        if require_remote_quic:
            proof_gate = _trainer_handoff_low_level_direct_read_proofs_gate(payload)
            read_proofs = _mapping(payload.get("low_level_direct_read_proofs"))
            if bool(read_proofs.get("required")) and not proof_gate["complete"]:
                missing = ", ".join(proof_gate["missing"])
                raise ValueError(
                    "trainer handoff does not include complete low-level direct read QUIC proofs"
                    + (f": {missing}" if missing else "")
                )
        if require_ready and not bool(payload.get("ready_for_trainer_wiring")):
            raise ValueError("trainer handoff is not ready for trainer wiring")
        if require_remote_quic and not bool(payload.get("ready_for_remote_quic")):
            raise ValueError("trainer handoff does not prove remote QUIC readiness")
        if require_remote_quic:
            if not proof_gate["complete"]:
                missing = ", ".join(proof_gate["missing"])
                raise ValueError(
                    "trainer handoff does not include complete low-level direct read QUIC proofs"
                    + (f": {missing}" if missing else "")
                )
            write_proofs = _mapping(payload.get("low_level_direct_write_proofs"))
            if bool(write_proofs.get("required")):
                write_proof_gate = _trainer_handoff_low_level_direct_write_proofs_gate(payload)
                if not write_proof_gate["complete"]:
                    missing = ", ".join(write_proof_gate["missing"])
                    raise ValueError(
                        "trainer handoff does not include complete low-level direct write QUIC proofs"
                        + (f": {missing}" if missing else "")
                    )
        if bool(payload.get("requires_mount")):
            raise ValueError("trainer handoff requires a mount; expected direct Python streaming")
        if bool(payload.get("touches_trainer_code")):
            raise ValueError("trainer handoff reports trainer-code edits; expected CXNFS-only proof")

        integration = _mapping(payload.get("integration_surface"))
        if integration.get("stream_object") != "CxnfsTrainerStream":
            raise ValueError("trainer handoff does not expose CxnfsTrainerStream")
        if integration.get("batch_object") != "CxnfsTrainerBatch":
            raise ValueError("trainer handoff does not expose CxnfsTrainerBatch")
        plan = _mapping(payload.get("integration_plan"))
        if plan.get("format") != TRAINER_INTEGRATION_PLAN_FORMAT:
            raise ValueError("trainer handoff does not include a trainer integration plan")
        stream_kwargs = _mapping(plan.get("stream_kwargs"))
        weights = _mapping(stream_kwargs.get("weights"))
        if not stream_kwargs.get("root"):
            raise ValueError("trainer integration plan is missing root")
        if not weights:
            raise ValueError("trainer integration plan is missing weighted sources")
        if _int_value(stream_kwargs.get("batch_size"), 0) <= 0:
            raise ValueError("trainer integration plan batch_size must be positive")
        return cls(dict(payload))

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        require_ready: bool = True,
        require_remote_quic: bool = False,
    ) -> CxnfsTrainerIntegrationPlan:
        return cls.from_handoff_payload(
            _read_json_mapping(Path(path), "trainer handoff"),
            require_ready=require_ready,
            require_remote_quic=require_remote_quic,
        )

    @property
    def ready_for_remote_quic(self) -> bool:
        return bool(self.payload.get("ready_for_remote_quic"))

    @property
    def remote_client_required(self) -> bool:
        return bool(self.to_dict().get("remote_client_required"))

    @property
    def stream_kwargs_json(self) -> Mapping[str, Any]:
        return _mapping(_mapping(self.payload.get("integration_plan")).get("stream_kwargs"))

    @property
    def remote_client_kwargs_json(self) -> Mapping[str, Any]:
        return _mapping(_mapping(self.payload.get("integration_plan")).get("remote_client_kwargs"))

    @property
    def trainer_loop_options_json(self) -> Mapping[str, Any]:
        return _mapping(_mapping(self.payload.get("integration_plan")).get("trainer_loop_options"))

    def stream_kwargs(self) -> dict[str, Any]:
        kwargs = dict(self.stream_kwargs_json)
        kwargs.pop("trainer_prefetch_batches", None)
        tensor_allocation = _mapping(kwargs.get("tensor_allocation"))
        if tensor_allocation:
            kwargs["tensor_allocation"] = CxnfsTrainerTensorAllocation(
                target=tensor_allocation.get("target", TensorMemoryTarget.PYTORCH_PINNED_CPU.value),
                dtype=str(tensor_allocation.get("dtype") or "uint8"),
                device=str(tensor_allocation.get("device") or "cpu"),
                non_blocking=_bool_value(tensor_allocation.get("non_blocking"), True),
            )
        else:
            kwargs.pop("tensor_allocation", None)
        training_shard = _mapping(kwargs.get("shard"))
        if training_shard:
            kwargs["shard"] = _training_shard_from_mapping(training_shard)
        else:
            kwargs.pop("shard", None)
        return kwargs

    def trainer_loop_options(self) -> dict[str, Any]:
        options = dict(self.trainer_loop_options_json)
        if "trainer_prefetch_batches" not in options:
            legacy_prefetch = self.stream_kwargs_json.get("trainer_prefetch_batches")
            options["trainer_prefetch_batches"] = _int_value(legacy_prefetch, 0)
        else:
            options["trainer_prefetch_batches"] = _int_value(options.get("trainer_prefetch_batches"), 0)
        return options

    def with_training_shard(self, shard: CxnfsTrainingShard | Mapping[str, Any]) -> CxnfsTrainerIntegrationPlan:
        active_shard = shard if isinstance(shard, CxnfsTrainingShard) else _training_shard_from_mapping(shard)
        payload = copy.deepcopy(dict(self.payload))
        integration_plan = payload.get("integration_plan")
        if not isinstance(integration_plan, dict):
            raise ValueError("trainer handoff does not include a mutable integration plan")
        stream_kwargs = integration_plan.get("stream_kwargs")
        if not isinstance(stream_kwargs, dict):
            raise ValueError("trainer integration plan does not include stream kwargs")
        stream_kwargs["shard"] = _training_shard_payload(active_shard)

        integration_surface = payload.get("integration_surface")
        if isinstance(integration_surface, dict):
            surface_kwargs = integration_surface.get("stream_kwargs")
            if isinstance(surface_kwargs, dict):
                surface_kwargs["shard"] = _training_shard_payload(active_shard)
        return CxnfsTrainerIntegrationPlan.from_handoff_payload(payload, require_ready=False)

    def rank_plan(self, rank: int, world_size: int) -> CxnfsTrainerIntegrationPlan:
        return self.with_training_shard(CxnfsTrainingShard(rank, world_size))

    def distributed_rank_plans(self, world_size: int) -> tuple[CxnfsTrainerIntegrationPlan, ...]:
        if world_size <= 0:
            raise ValueError("world_size must be positive")
        return tuple(self.rank_plan(rank, world_size) for rank in range(world_size))

    def bind_rank(self, rank: int | None = None, world_size: int | None = None) -> CxnfsTrainerRuntimeBinding:
        env_shard = current_training_shard() if rank is None or world_size is None else None
        active_rank = rank if rank is not None else (env_shard.index if env_shard is not None else 0)
        active_world_size = (
            world_size if world_size is not None else (env_shard.count if env_shard is not None else 1)
        )
        shard = CxnfsTrainingShard(active_rank, active_world_size)
        return CxnfsTrainerRuntimeBinding(
            plan=self.with_training_shard(shard),
            rank=shard.index,
            world_size=shard.count,
        )

    def distributed_plan_report(self, world_size: int, *, selected_rank: int | None = None) -> dict[str, Any]:
        if world_size <= 0:
            raise ValueError("world_size must be positive")
        if selected_rank is not None and (selected_rank < 0 or selected_rank >= world_size):
            raise ValueError("selected_rank must be inside world_size")
        base_preflight = self.preflight_report()
        rank_reports: list[dict[str, Any]] = []
        selected_rank_plan: dict[str, Any] | None = None
        for rank, rank_plan in enumerate(self.distributed_rank_plans(world_size)):
            rank_preflight = rank_plan.preflight_report()
            rank_payload = {
                "rank": rank,
                "world_size": world_size,
                "training_shard": rank_preflight["training_shard"],
                "stream_kwargs": rank_preflight["stream_kwargs"],
                "stream_kwargs_keys": rank_preflight["stream_kwargs_keys"],
                "trainer_loop_options": rank_preflight["trainer_loop_options"],
                "trainer_prefetch_batches": rank_preflight["trainer_prefetch_batches"],
                "remote_client_required": rank_preflight["remote_client_required"],
                "requires_credentials": rank_preflight["requires_credentials"],
                "low_level_direct_read_proofs_complete": rank_preflight["low_level_direct_read_proofs_complete"],
                "batch_direct_read_proof_complete": rank_preflight["batch_direct_read_proof_complete"],
                "batch_direct_tensor_read_proof_complete": rank_preflight["batch_direct_tensor_read_proof_complete"],
                "low_level_direct_write_proofs_complete": rank_preflight["low_level_direct_write_proofs_complete"],
                "write_probe_proof_complete": rank_preflight["write_probe_proof_complete"],
                "read_mode": rank_preflight["read_mode"],
                "trainer_batch_iterator": rank_preflight["trainer_batch_iterator"],
                "data_loader_workers": rank_preflight["data_loader_workers"],
                "stream_factory": rank_preflight["stream_factory"],
                "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
                "runtime_binding_format": TRAINER_RUNTIME_BINDING_FORMAT,
                "remote_client_factory": rank_preflight["remote_client_factory"],
                "remote_stream_context": rank_preflight["remote_stream_context"],
                "training_loop_context": rank_preflight["training_loop_context"],
                "pipeline_training_loop_context": rank_preflight["pipeline_training_loop_context"],
                "training_pipeline_object": rank_preflight["training_pipeline_object"],
                "training_pipeline_context": rank_preflight["training_pipeline_context"],
            }
            rank_reports.append(rank_payload)
            if selected_rank == rank:
                selected_rank_plan = rank_payload

        return {
            "format": TRAINER_DISTRIBUTED_PLAN_FORMAT,
            "passed": True,
            "handoff_format": self.payload.get("format"),
            "plan_format": base_preflight["plan_format"],
            "preflight_format": TRAINER_INTEGRATION_PREFLIGHT_FORMAT,
            "runtime_binding_format": TRAINER_RUNTIME_BINDING_FORMAT,
            "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
            "ready_for_trainer_wiring": base_preflight["ready_for_trainer_wiring"],
            "ready_for_remote_quic": base_preflight["ready_for_remote_quic"],
            "ready_for_blockfs_candidate": base_preflight["ready_for_blockfs_candidate"],
            "requires_mount": base_preflight["requires_mount"],
            "touches_trainer_code": base_preflight["touches_trainer_code"],
            "safe_to_run_before_trainer_workers": True,
            "opens_network_connections": False,
            "consumes_batches": False,
            "remote_client_required": base_preflight["remote_client_required"],
            "requires_credentials": base_preflight["requires_credentials"],
            "raw_password_sent_over_control": base_preflight["raw_password_sent_over_control"],
            "low_level_direct_read_proofs": base_preflight["low_level_direct_read_proofs"],
            "low_level_direct_read_proofs_complete": base_preflight["low_level_direct_read_proofs_complete"],
            "low_level_direct_read_proof_missing": base_preflight["low_level_direct_read_proof_missing"],
            "batch_direct_read_proof_complete": base_preflight["batch_direct_read_proof_complete"],
            "batch_direct_tensor_read_proof_complete": base_preflight["batch_direct_tensor_read_proof_complete"],
            "low_level_direct_write_proofs": base_preflight["low_level_direct_write_proofs"],
            "low_level_direct_write_proofs_complete": base_preflight["low_level_direct_write_proofs_complete"],
            "low_level_direct_write_proof_missing": base_preflight["low_level_direct_write_proof_missing"],
            "write_probe_proof_complete": base_preflight["write_probe_proof_complete"],
            "write_probe_cipher_suite": base_preflight["write_probe_cipher_suite"],
            "world_size": world_size,
            "rank_count": len(rank_reports),
            "ranks": [rank for rank in range(world_size)],
            "selected_rank": selected_rank,
            "selected_rank_plan": selected_rank_plan,
            "stream_factory": base_preflight["stream_factory"],
            "plan_object": base_preflight["plan_object"],
            "per_rank_plan_method": "CxnfsTrainerIntegrationPlan.rank_plan",
            "distributed_plan_method": "CxnfsTrainerIntegrationPlan.distributed_rank_plans",
            "remote_client_factory": base_preflight["remote_client_factory"],
            "remote_stream_context": base_preflight["remote_stream_context"],
            "training_loop_context": base_preflight["training_loop_context"],
            "pipeline_training_loop_context": base_preflight["pipeline_training_loop_context"],
            "training_pipeline_object": base_preflight["training_pipeline_object"],
            "training_pipeline_context": base_preflight["training_pipeline_context"],
            "supports_pipeline_direct_tensor_batches": base_preflight["supports_pipeline_direct_tensor_batches"],
            "supports_pipeline_gpu_transforms": base_preflight["supports_pipeline_gpu_transforms"],
            "supports_pipeline_torch_compile": base_preflight["supports_pipeline_torch_compile"],
            "supports_training_loop_prefetch": base_preflight["supports_training_loop_prefetch"],
            "supports_resume_cursor": base_preflight["supports_resume_cursor"],
            "supports_resume_state_file": base_preflight["supports_resume_state_file"],
            "resume_state_format": base_preflight["resume_state_format"],
            "root": base_preflight["root"],
            "remote_root": base_preflight["remote_root"],
            "weights": base_preflight["weights"],
            "source_count": base_preflight["source_count"],
            "weighted_sources": base_preflight["weighted_sources"],
            "read_mode": base_preflight["read_mode"],
            "trainer_batch_iterator": base_preflight["trainer_batch_iterator"],
            "data_loader_workers": base_preflight["data_loader_workers"],
            "trainer_loop_options": base_preflight["trainer_loop_options"],
            "trainer_prefetch_batches": base_preflight["trainer_prefetch_batches"],
            "bulk_transport": base_preflight["bulk_transport"],
            "initial_cords": base_preflight["initial_cords"],
            "max_cords": base_preflight["max_cords"],
            "bulk_read_retries": base_preflight["bulk_read_retries"],
            "bulk_write_retries": base_preflight["bulk_write_retries"],
            "rank_plans": rank_reports,
        }

    def preflight_report(self) -> dict[str, Any]:
        plan_payload = self.to_dict()
        stream_kwargs = self.stream_kwargs()
        serialized_stream_kwargs = _serialize_trainer_stream_kwargs(stream_kwargs)
        remote_kwargs = dict(self.remote_client_kwargs_json)
        low_level_proofs = _mapping(self.payload.get("low_level_direct_read_proofs"))
        low_level_proof_gate = _trainer_handoff_low_level_direct_read_proofs_gate(self.payload)
        batch_direct_read_proof = _mapping(low_level_proofs.get("batch_direct_read"))
        batch_direct_tensor_read_proof = _mapping(low_level_proofs.get("batch_direct_tensor_read"))
        low_level_write_proofs = _mapping(self.payload.get("low_level_direct_write_proofs"))
        low_level_write_proof_gate = _trainer_handoff_low_level_direct_write_proofs_gate(self.payload)
        weights = dict(_mapping(stream_kwargs.get("weights")))
        active_training_shard = stream_kwargs.get("shard")
        training_shard_payload = (
            _training_shard_payload(active_training_shard)
            if isinstance(active_training_shard, CxnfsTrainingShard)
            else None
        )
        trainer_loop_options = self.trainer_loop_options()
        return {
            "format": TRAINER_INTEGRATION_PREFLIGHT_FORMAT,
            "passed": True,
            "handoff_format": self.payload.get("format"),
            "plan_format": plan_payload.get("format"),
            "ready_for_trainer_wiring": bool(self.payload.get("ready_for_trainer_wiring")),
            "ready_for_remote_quic": bool(self.payload.get("ready_for_remote_quic")),
            "ready_for_blockfs_candidate": bool(self.payload.get("ready_for_blockfs_candidate")),
            "low_level_direct_read_proofs": dict(low_level_proofs) if low_level_proofs else None,
            "low_level_direct_read_proofs_complete": bool(low_level_proof_gate["complete"]),
            "low_level_direct_read_proof_missing": list(low_level_proof_gate["missing"]),
            "batch_direct_read_proof_complete": bool(batch_direct_read_proof.get("complete")),
            "batch_direct_tensor_read_proof_complete": bool(batch_direct_tensor_read_proof.get("complete")),
            "batch_direct_read_cipher_suite": batch_direct_read_proof.get("cipher_suite"),
            "batch_direct_tensor_read_cipher_suite": batch_direct_tensor_read_proof.get("cipher_suite"),
            "low_level_direct_write_proofs": dict(low_level_write_proofs) if low_level_write_proofs else None,
            "low_level_direct_write_proofs_required": bool(low_level_write_proofs.get("required")),
            "low_level_direct_write_proofs_complete": bool(low_level_write_proof_gate["complete"]),
            "low_level_direct_write_proof_missing": list(low_level_write_proof_gate["missing"]),
            "write_probe_proof_complete": bool(low_level_write_proofs.get("complete")),
            "write_probe_cipher_suite": low_level_write_proofs.get("quic_write_transport_cipher_suite"),
            "write_probe_quic_transport_complete": bool(low_level_write_proofs.get("quic_write_transport_complete")),
            "write_probe_max_observed_write_cords": _int_value(
                low_level_write_proofs.get("max_observed_write_cords"), 0
            ),
            "requires_mount": bool(self.payload.get("requires_mount")),
            "touches_trainer_code": bool(self.payload.get("touches_trainer_code")),
            "safe_to_run_before_trainer_workers": True,
            "opens_network_connections": False,
            "consumes_batches": False,
            "stream_factory": plan_payload.get("stream_factory"),
            "plan_object": plan_payload.get("plan_object"),
            "stream_object": plan_payload.get("stream_object"),
            "batch_object": plan_payload.get("batch_object"),
            "remote_client_factory": plan_payload.get("remote_client_factory"),
            "remote_stream_context": plan_payload.get("remote_stream_context"),
            "training_loop_context": plan_payload.get("training_loop_context")
            or "CxnfsTrainerIntegrationPlan.open_training_loop",
            "pipeline_training_loop_context": plan_payload.get("pipeline_training_loop_context")
            or "CxnfsTrainerIntegrationPlan.open_pipeline_training_loop",
            "training_pipeline_object": plan_payload.get("training_pipeline_object") or "CxnfsTrainingPipeline",
            "training_pipeline_context": plan_payload.get("training_pipeline_context")
            or "CxnfsTrainingPipeline.read_tensor_batch",
            "supports_pipeline_direct_tensor_batches": bool(
                plan_payload.get("supports_pipeline_direct_tensor_batches", True)
            ),
            "supports_pipeline_gpu_transforms": bool(plan_payload.get("supports_pipeline_gpu_transforms", True)),
            "supports_pipeline_torch_compile": bool(plan_payload.get("supports_pipeline_torch_compile", True)),
            "pipeline_cpu_augmentation_default": (
                plan_payload.get("pipeline_cpu_augmentation_default")
                or "disabled-with-explicit-opt-in"
            ),
            "supports_training_loop_prefetch": bool(plan_payload.get("supports_training_loop_prefetch", True)),
            "supports_resume_cursor": bool(plan_payload.get("supports_resume_cursor", True)),
            "supports_resume_state_file": bool(plan_payload.get("supports_resume_state_file", True)),
            "resume_state_format": plan_payload.get("resume_state_format") or TRAINER_STREAM_STATE_FORMAT,
            "remote_client_required": self.remote_client_required,
            "requires_credentials": self.remote_client_required,
            "remote_root": stream_kwargs.get("remote_root"),
            "root": stream_kwargs.get("root"),
            "weights": weights,
            "source_count": len(weights),
            "weighted_sources": list(weights),
            "read_mode": stream_kwargs.get("read_mode"),
            "trainer_batch_iterator": stream_kwargs.get("trainer_batch_iterator"),
            "data_loader_workers": _int_value(stream_kwargs.get("data_loader_workers"), 0),
            "training_shard": training_shard_payload,
            "rank": None if training_shard_payload is None else training_shard_payload["index"],
            "world_size": None if training_shard_payload is None else training_shard_payload["count"],
            "max_workers": _int_value(stream_kwargs.get("max_workers"), 0),
            "source_read_workers": _optional_int_value(stream_kwargs.get("source_read_workers")),
            "prefetch_chunks": _int_value(stream_kwargs.get("prefetch_chunks"), 0),
            "rounds_per_read": _int_value(stream_kwargs.get("rounds_per_read"), 0),
            "samples_per_epoch": _optional_int_value(stream_kwargs.get("samples_per_epoch")),
            "repeat": _bool_value(stream_kwargs.get("repeat"), True),
            "trainer_loop_options": trainer_loop_options,
            "trainer_prefetch_batches": _int_value(trainer_loop_options.get("trainer_prefetch_batches"), 0),
            "stream_kwargs": serialized_stream_kwargs,
            "stream_kwargs_keys": sorted(serialized_stream_kwargs),
            "remote_client_kwargs": remote_kwargs,
            "bulk_transport": remote_kwargs.get("bulk_transport") if self.remote_client_required else None,
            "initial_cords": remote_kwargs.get("initial_cords") if self.remote_client_required else None,
            "max_cords": remote_kwargs.get("max_cords") if self.remote_client_required else None,
            "bulk_read_retries": remote_kwargs.get("bulk_read_retries") if self.remote_client_required else None,
            "bulk_write_retries": remote_kwargs.get("bulk_write_retries") if self.remote_client_required else None,
            "raw_password_sent_over_control": bool(remote_kwargs.get("raw_password_sent_over_control")),
            "per_source_client_default": bool(remote_kwargs.get("per_source_client_default", True)),
        }

    def create_remote_client_factory(
        self,
        credentials: ClientCredentials,
        *,
        host: str = "127.0.0.1",
        control_port: int = 4509,
        transport_port: int = 4510,
        timeout: float | None = 10.0,
        tls_context: Any = None,
        server_hostname: str | None = None,
        cafile: str | None = None,
        initial_cords: int | None = None,
        max_cords: int | None = None,
        provisional_container_size: int | None = None,
        expected_size_limit: int | None = None,
        write_connections: int | None = None,
        bulk_transport: str | None = None,
        bulk_read_retries: int | None = None,
        bulk_write_retries: int | None = None,
        shared_client: bool | None = None,
        control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
        bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
        tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
    ) -> DatasetClientFactory:
        if not self.remote_client_required:
            raise ValueError("trainer handoff does not require a remote client factory")
        remote_kwargs = self.remote_client_kwargs_json
        selected_bulk_transport = require_supported_python_bulk_transport(
            bulk_transport or remote_kwargs.get("bulk_transport") or PYTHON_BULK_TRANSPORT_STREAM
        )
        selected_initial_cords = _int_value(
            initial_cords if initial_cords is not None else remote_kwargs.get("initial_cords"),
            1,
        )
        selected_max_cords = _int_value(
            max_cords if max_cords is not None else remote_kwargs.get("max_cords"),
            ABSOLUTE_MAX_CORDS,
        )
        selected_bulk_read_retries = _validate_bulk_read_retries(
            bulk_read_retries if bulk_read_retries is not None else remote_kwargs.get("bulk_read_retries")
        )
        bulk_write_value = (
            bulk_write_retries if bulk_write_retries is not None else remote_kwargs.get("bulk_write_retries")
        )
        selected_bulk_write_retries = (
            None if bulk_write_value is None else _validate_bulk_write_retries(bulk_write_value)
        )
        selected_provisional_container_size = _int_value(
            provisional_container_size
            if provisional_container_size is not None
            else (
                remote_kwargs.get("provisional_container_size")
                if remote_kwargs.get("provisional_container_size") is not None
                else remote_kwargs.get("provisional_container_size_bytes")
            ),
            CxnfsBulkReadConfig.provisional_container_size_bytes,
        )
        selected_expected_size_limit = _int_value(
            expected_size_limit
            if expected_size_limit is not None
            else (
                remote_kwargs.get("expected_size_limit")
                if remote_kwargs.get("expected_size_limit") is not None
                else remote_kwargs.get("expected_size_limit_bytes")
            ),
            0,
        )
        selected_write_connections = (
            write_connections
            if write_connections is not None
            else _optional_int_value(remote_kwargs.get("write_connections"))
        )
        selected_shared_client = (
            bool(shared_client)
            if shared_client is not None
            else _bool_value(remote_kwargs.get("shared_client"), False)
        )
        selected_tls_context = tls_context
        if selected_tls_context is None and cafile:
            selected_tls_context = tls_context_factory(cafile)
        return create_remote_trainer_client_factory(
            credentials,
            host,
            control_port,
            transport_port,
            timeout=timeout,
            tls_context=selected_tls_context,
            server_hostname=server_hostname or host,
            read_config=CxnfsBulkReadConfig(
                initial_cords=selected_initial_cords,
                max_cords=selected_max_cords,
                provisional_container_size_bytes=selected_provisional_container_size,
                expected_size_limit_bytes=selected_expected_size_limit,
            ),
            write_connections=selected_write_connections,
            bulk_read_retries=selected_bulk_read_retries,
            bulk_write_retries=selected_bulk_write_retries,
            shared_client=selected_shared_client,
            bulk_transport=selected_bulk_transport,
            cafile=cafile,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )

    def open_stream(
        self,
        *,
        client_factory: DatasetClientFactory | None = None,
        client: CxnfsDatasetClient | None = None,
        resume_state: Mapping[str, Any] | None = None,
        resume_state_path: str | Path | None = None,
        **overrides: Any,
    ) -> CxnfsTrainerStream:
        if client is not None and client_factory is not None:
            raise ValueError("use client or client_factory, not both")
        state_payload = _trainer_stream_state_from_value(resume_state, resume_state_path)
        if state_payload is not None:
            if overrides.get("cursor") is not None:
                raise ValueError("resume_state cannot be combined with cursor override")
            overrides["cursor"] = _mapping(state_payload.get("cursor"))
        kwargs = self.stream_kwargs()
        kwargs.update(overrides)
        if client_factory is not None:
            kwargs["client_factory"] = client_factory
        if client is not None:
            kwargs["client"] = client
        if self.remote_client_required and "client" not in kwargs and "client_factory" not in kwargs:
            raise ValueError("remote trainer integration plan requires client or client_factory")
        stream = open_weighted_mds_trainer_stream(**kwargs)
        if state_payload is not None:
            try:
                stream.load_state_dict(state_payload)
            except Exception:
                stream.close()
                raise
        return stream

    @contextmanager
    def open_remote_stream(
        self,
        credentials: ClientCredentials,
        *,
        host: str = "127.0.0.1",
        control_port: int = 4509,
        transport_port: int = 4510,
        timeout: float | None = 10.0,
        tls_context: Any = None,
        server_hostname: str | None = None,
        cafile: str | None = None,
        initial_cords: int | None = None,
        max_cords: int | None = None,
        provisional_container_size: int | None = None,
        expected_size_limit: int | None = None,
        write_connections: int | None = None,
        bulk_transport: str | None = None,
        bulk_read_retries: int | None = None,
        bulk_write_retries: int | None = None,
        shared_client: bool | None = None,
        control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
        bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
        tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
        resume_state: Mapping[str, Any] | None = None,
        resume_state_path: str | Path | None = None,
        **stream_overrides: Any,
    ) -> Iterator[CxnfsTrainerStream]:
        client_factory = self.create_remote_client_factory(
            credentials,
            host=host,
            control_port=control_port,
            transport_port=transport_port,
            timeout=timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
            cafile=cafile,
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size=provisional_container_size,
            expected_size_limit=expected_size_limit,
            write_connections=write_connections,
            bulk_transport=bulk_transport,
            bulk_read_retries=bulk_read_retries,
            bulk_write_retries=bulk_write_retries,
            shared_client=shared_client,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        try:
            with self.open_stream(
                client_factory=client_factory,
                resume_state=resume_state,
                resume_state_path=resume_state_path,
                **stream_overrides,
            ) as stream:
                yield stream
        finally:
            close = getattr(client_factory, "close", None)
            if callable(close):
                close()

    @contextmanager
    def open_pipeline_training_loop(
        self,
        credentials: ClientCredentials | None = None,
        *,
        host: str = "127.0.0.1",
        control_port: int = 4509,
        transport_port: int = 4510,
        timeout: float | None = 10.0,
        tls_context: Any = None,
        server_hostname: str | None = None,
        cafile: str | None = None,
        initial_cords: int | None = None,
        max_cords: int | None = None,
        provisional_container_size: int | None = None,
        expected_size_limit: int | None = None,
        write_connections: int | None = None,
        bulk_transport: str | None = None,
        bulk_read_retries: int | None = None,
        bulk_write_retries: int | None = None,
        shared_client: bool | None = None,
        control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
        bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
        tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
        resume_state: Mapping[str, Any] | None = None,
        resume_state_path: str | Path | None = None,
        pipeline_config: CxnfsPipelineConfig | None = None,
        pipeline_client: CxnfsDatasetClient | None = None,
        client_factory: DatasetClientFactory | None = None,
        pipeline_prefetch_batches: int | None = None,
        pipeline_workers: int | None = None,
        pipeline_max_inflight_bytes: int | None = None,
        **stream_overrides: Any,
    ) -> Iterator[CxnfsTrainerPipelineLoop]:
        state_payload = _trainer_stream_state_from_value(resume_state, resume_state_path)
        if state_payload is not None and stream_overrides.get("cursor") is not None:
            raise ValueError("resume_state cannot be combined with cursor override")
        kwargs = self.stream_kwargs()
        kwargs.update(stream_overrides)
        read_mode = TrainerReadMode(kwargs.get("read_mode", TrainerReadMode.DIRECT_TENSORS.value))
        if read_mode != TrainerReadMode.DIRECT_TENSORS:
            raise ValueError("pipeline training loop requires read_mode=direct-tensors")
        root = kwargs.get("root")
        weights = _mapping(kwargs.get("weights"))
        if not root:
            raise ValueError("trainer pipeline loop requires root")
        if not weights:
            raise ValueError("trainer pipeline loop requires weighted sources")

        remote_kwargs = self.remote_client_kwargs_json
        selected_bulk_transport = require_supported_python_bulk_transport(
            bulk_transport or remote_kwargs.get("bulk_transport") or PYTHON_BULK_TRANSPORT_STREAM
        )
        selected_initial_cords = _int_value(
            initial_cords if initial_cords is not None else remote_kwargs.get("initial_cords"),
            1,
        )
        selected_max_cords = _int_value(
            max_cords if max_cords is not None else remote_kwargs.get("max_cords"),
            ABSOLUTE_MAX_CORDS,
        )
        selected_bulk_read_retries = _validate_bulk_read_retries(
            bulk_read_retries if bulk_read_retries is not None else remote_kwargs.get("bulk_read_retries")
        )
        bulk_write_value = (
            bulk_write_retries if bulk_write_retries is not None else remote_kwargs.get("bulk_write_retries")
        )
        selected_bulk_write_retries = (
            None if bulk_write_value is None else _validate_bulk_write_retries(bulk_write_value)
        )
        selected_provisional_container_size = _int_value(
            provisional_container_size
            if provisional_container_size is not None
            else (
                remote_kwargs.get("provisional_container_size")
                if remote_kwargs.get("provisional_container_size") is not None
                else remote_kwargs.get("provisional_container_size_bytes")
            ),
            CxnfsBulkReadConfig.provisional_container_size_bytes,
        )
        selected_expected_size_limit = _int_value(
            expected_size_limit
            if expected_size_limit is not None
            else (
                remote_kwargs.get("expected_size_limit")
                if remote_kwargs.get("expected_size_limit") is not None
                else remote_kwargs.get("expected_size_limit_bytes")
            ),
            0,
        )
        selected_write_connections = (
            write_connections
            if write_connections is not None
            else _optional_int_value(remote_kwargs.get("write_connections"))
        )
        selected_shared_client = (
            bool(shared_client)
            if shared_client is not None
            else _bool_value(remote_kwargs.get("shared_client"), False)
        )
        read_config = CxnfsBulkReadConfig(
            initial_cords=selected_initial_cords,
            max_cords=selected_max_cords,
            provisional_container_size_bytes=selected_provisional_container_size,
            expected_size_limit_bytes=selected_expected_size_limit,
        )

        created_client_factory = None
        active_client_factory = client_factory
        if self.remote_client_required and active_client_factory is None:
            if credentials is None:
                raise ValueError("remote trainer pipeline loop requires credentials or client_factory")
            active_client_factory = self.create_remote_client_factory(
                credentials,
                host=host,
                control_port=control_port,
                transport_port=transport_port,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                cafile=cafile,
                initial_cords=selected_initial_cords,
                max_cords=selected_max_cords,
                provisional_container_size=selected_provisional_container_size,
                expected_size_limit=selected_expected_size_limit,
                write_connections=selected_write_connections,
                bulk_transport=selected_bulk_transport,
                bulk_read_retries=selected_bulk_read_retries,
                bulk_write_retries=selected_bulk_write_retries,
                shared_client=selected_shared_client,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
                tls_context_factory=tls_context_factory,
            )
            created_client_factory = active_client_factory

        remote_root = kwargs.get("remote_root") if self.remote_client_required else None
        dataset = None
        descriptor_stream = None
        pipeline = None
        active_pipeline_client = pipeline_client
        pipeline_client_owned = False
        try:
            sources = weighted_mds_sources_from_root(
                root,
                weights,
                client_factory=active_client_factory if self.remote_client_required else None,
                remote_root=remote_root if self.remote_client_required else None,
            )
            dataset = CxnfsWeightedTorchDataset(
                sources,
                repeat=_bool_value(kwargs.get("repeat"), True),
                rounds_per_read=_int_value(kwargs.get("rounds_per_read"), 1),
                max_workers=_int_value(kwargs.get("max_workers"), 16),
                source_read_workers=_optional_int_value(kwargs.get("source_read_workers")),
                prefetch_chunks=_int_value(kwargs.get("prefetch_chunks"), 1),
                include_source_metadata=False,
                shard=kwargs.get("shard") if isinstance(kwargs.get("shard"), CxnfsTrainingShard) else None,
                shuffle=_bool_value(kwargs.get("shuffle"), False),
                seed=_optional_int_value(kwargs.get("seed")),
                samples_per_epoch=(
                    None if state_payload is not None else _optional_int_value(kwargs.get("samples_per_epoch"))
                ),
            )
            if state_payload is None and kwargs.get("epoch") is not None:
                dataset.set_epoch(_int_value(kwargs.get("epoch"), 0))

            tensor_allocation = kwargs.get("tensor_allocation")
            if not isinstance(tensor_allocation, CxnfsTrainerTensorAllocation):
                tensor_allocation = CxnfsTrainerTensorAllocation()
            source_path_prefixes = _trainer_pipeline_source_path_prefixes(
                sources,
                {"remote_root": remote_root} if remote_root else {},
            )
            descriptor_stream = CxnfsTrainerPipelineStream(
                dataset,
                _int_value(kwargs.get("batch_size"), 0),
                tensor_factory=kwargs.get("tensor_factory"),
                tensor_allocation=tensor_allocation,
                epoch=None if state_payload is not None else _optional_int_value(kwargs.get("epoch")),
                samples_per_epoch=(
                    None if state_payload is not None else _optional_int_value(kwargs.get("samples_per_epoch"))
                ),
                drop_last=_bool_value(kwargs.get("drop_last"), False),
                include_paths=_bool_value(kwargs.get("include_paths"), False),
                cursor=(
                    _mapping(state_payload.get("cursor"))
                    if state_payload is not None
                    else kwargs.get("cursor")
                ),
                source_path_prefixes=source_path_prefixes,
            )
            if state_payload is not None:
                descriptor_stream.load_state_dict(state_payload)

            if active_pipeline_client is None:
                if self.remote_client_required:
                    if credentials is None:
                        raise ValueError("remote trainer pipeline loop requires credentials or pipeline_client")
                    selected_tls_context = tls_context
                    if selected_tls_context is None and cafile:
                        selected_tls_context = tls_context_factory(cafile)
                    root_client = _connect_trainer_remote_bulk_client(
                        credentials,
                        host,
                        control_port,
                        transport_port,
                        timeout,
                        selected_tls_context,
                        server_hostname or host,
                        read_config,
                        selected_write_connections,
                        selected_bulk_read_retries,
                        selected_bulk_write_retries,
                        selected_bulk_transport,
                        cafile,
                        control_client_factory,
                        bulk_client_factory,
                    )
                    active_pipeline_client = CxnfsPathPrefixClient(root_client, str(remote_root))
                else:
                    active_pipeline_client = CxnfsPythonClient(LocalFileTransport(root))
                pipeline_client_owned = True

            active_pipeline_prefetch = _validate_pipeline_prefetch_batches(
                pipeline_prefetch_batches
                if pipeline_prefetch_batches is not None
                else self.trainer_loop_options().get("trainer_prefetch_batches")
            )
            if active_pipeline_prefetch <= 0:
                active_pipeline_prefetch = CxnfsPipelineConfig().prefetch_batches
            active_pipeline_config = pipeline_config or CxnfsPipelineConfig(
                network_read_workers=min(
                    max(1, _int_value(pipeline_workers, _int_value(kwargs.get("max_workers"), 16))),
                    max(1, selected_max_cords),
                ),
                max_cords=max(1, selected_max_cords),
                prefetch_batches=active_pipeline_prefetch,
                max_inflight_bytes=(
                    pipeline_max_inflight_bytes
                    if pipeline_max_inflight_bytes is not None
                    else CxnfsPipelineConfig().max_inflight_bytes
                ),
            )
            pipeline = CxnfsTrainingPipeline(
                active_pipeline_client,
                active_pipeline_config,
                close_client=pipeline_client_owned,
            )
            with pipeline:
                with pipeline.prefetch_trainer_stream(
                    descriptor_stream,
                    max_prefetch_batches=active_pipeline_prefetch,
                    non_blocking=tensor_allocation.non_blocking,
                ) as prefetched:
                    yield CxnfsTrainerPipelineLoop(descriptor_stream, pipeline, prefetched)
        finally:
            if descriptor_stream is not None:
                descriptor_stream.close()
            elif dataset is not None:
                dataset.close()
            if created_client_factory is not None:
                close = getattr(created_client_factory, "close", None)
                if callable(close):
                    close()

    @contextmanager
    def open_training_loop(
        self,
        credentials: ClientCredentials | None = None,
        *,
        host: str = "127.0.0.1",
        control_port: int = 4509,
        transport_port: int = 4510,
        timeout: float | None = 10.0,
        tls_context: Any = None,
        server_hostname: str | None = None,
        cafile: str | None = None,
        initial_cords: int | None = None,
        max_cords: int | None = None,
        provisional_container_size: int | None = None,
        expected_size_limit: int | None = None,
        write_connections: int | None = None,
        bulk_transport: str | None = None,
        bulk_read_retries: int | None = None,
        bulk_write_retries: int | None = None,
        shared_client: bool | None = None,
        control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
        bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
        tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
        resume_state: Mapping[str, Any] | None = None,
        resume_state_path: str | Path | None = None,
        trainer_prefetch_batches: int | None = None,
        client: CxnfsDatasetClient | None = None,
        client_factory: DatasetClientFactory | None = None,
        **stream_overrides: Any,
    ) -> Iterator[CxnfsTrainerStream | CxnfsTrainerPrefetchStream]:
        if self.remote_client_required and credentials is None and client is None and client_factory is None:
            raise ValueError("remote trainer integration plan requires credentials")
        loop_options = self.trainer_loop_options()
        active_prefetch = _validate_trainer_prefetch_batches(
            _int_value(
                trainer_prefetch_batches
                if trainer_prefetch_batches is not None
                else loop_options.get("trainer_prefetch_batches"),
                0,
            )
        )
        if self.remote_client_required and client is None and client_factory is None:
            assert credentials is not None
            stream_context = self.open_remote_stream(
                credentials,
                host=host,
                control_port=control_port,
                transport_port=transport_port,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                cafile=cafile,
                initial_cords=initial_cords,
                max_cords=max_cords,
                provisional_container_size=provisional_container_size,
                expected_size_limit=expected_size_limit,
                write_connections=write_connections,
                bulk_transport=bulk_transport,
                bulk_read_retries=bulk_read_retries,
                bulk_write_retries=bulk_write_retries,
                shared_client=shared_client,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
                tls_context_factory=tls_context_factory,
                resume_state=resume_state,
                resume_state_path=resume_state_path,
                **stream_overrides,
            )
        else:
            stream_context = self.open_stream(
                client=client,
                client_factory=client_factory,
                resume_state=resume_state,
                resume_state_path=resume_state_path,
                **stream_overrides,
            )
        with stream_context as stream:
            if active_prefetch <= 0:
                yield stream
                return
            with stream.prefetch_batches(max_prefetch_batches=active_prefetch) as prefetched:
                yield prefetched

    def to_dict(self) -> dict[str, Any]:
        return dict(_mapping(self.payload.get("integration_plan")))


@dataclass(frozen=True)
class CxnfsTrainerRuntimeBinding:
    plan: CxnfsTrainerIntegrationPlan
    rank: int
    world_size: int

    @classmethod
    def from_handoff_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        rank: int | None = None,
        world_size: int | None = None,
        require_ready: bool = True,
        require_remote_quic: bool = False,
    ) -> CxnfsTrainerRuntimeBinding:
        plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
            payload,
            require_ready=require_ready,
            require_remote_quic=require_remote_quic,
        )
        return plan.bind_rank(rank, world_size)

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        rank: int | None = None,
        world_size: int | None = None,
        require_ready: bool = True,
        require_remote_quic: bool = False,
    ) -> CxnfsTrainerRuntimeBinding:
        return cls.from_handoff_payload(
            _read_json_mapping(Path(path), "trainer handoff"),
            rank=rank,
            world_size=world_size,
            require_ready=require_ready,
            require_remote_quic=require_remote_quic,
        )

    @property
    def training_shard(self) -> CxnfsTrainingShard:
        return CxnfsTrainingShard(self.rank, self.world_size)

    @property
    def remote_client_required(self) -> bool:
        return self.plan.remote_client_required

    def stream_kwargs(self) -> dict[str, Any]:
        return self.plan.stream_kwargs()

    def trainer_loop_options(self) -> dict[str, Any]:
        return self.plan.trainer_loop_options()

    def to_dict(self) -> dict[str, Any]:
        preflight = self.plan.preflight_report()
        return {
            "format": TRAINER_RUNTIME_BINDING_FORMAT,
            "passed": True,
            "handoff_format": preflight["handoff_format"],
            "plan_format": preflight["plan_format"],
            "preflight_format": TRAINER_INTEGRATION_PREFLIGHT_FORMAT,
            "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
            "open_training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "open_pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
            "rank": self.rank,
            "world_size": self.world_size,
            "training_shard": _training_shard_payload(self.training_shard),
            "ready_for_trainer_wiring": preflight["ready_for_trainer_wiring"],
            "ready_for_remote_quic": preflight["ready_for_remote_quic"],
            "ready_for_blockfs_candidate": preflight["ready_for_blockfs_candidate"],
            "requires_mount": preflight["requires_mount"],
            "touches_trainer_code": preflight["touches_trainer_code"],
            "safe_to_run_before_trainer_workers": True,
            "opens_network_connections": False,
            "consumes_batches": False,
            "remote_client_required": preflight["remote_client_required"],
            "requires_credentials": preflight["requires_credentials"],
            "credential_prompt_supported": True,
            "raw_password_sent_over_control": preflight["raw_password_sent_over_control"],
            "low_level_direct_read_proofs": preflight["low_level_direct_read_proofs"],
            "low_level_direct_read_proofs_complete": preflight["low_level_direct_read_proofs_complete"],
            "low_level_direct_read_proof_missing": preflight["low_level_direct_read_proof_missing"],
            "batch_direct_read_proof_complete": preflight["batch_direct_read_proof_complete"],
            "batch_direct_tensor_read_proof_complete": preflight["batch_direct_tensor_read_proof_complete"],
            "stream_factory": preflight["stream_factory"],
            "stream_object": preflight["stream_object"],
            "batch_object": preflight["batch_object"],
            "remote_client_factory": preflight["remote_client_factory"],
            "remote_stream_context": preflight["remote_stream_context"],
            "training_loop_context": preflight["training_loop_context"],
            "pipeline_training_loop_context": preflight["pipeline_training_loop_context"],
            "training_pipeline_object": preflight["training_pipeline_object"],
            "training_pipeline_context": preflight["training_pipeline_context"],
            "supports_pipeline_direct_tensor_batches": preflight["supports_pipeline_direct_tensor_batches"],
            "supports_pipeline_gpu_transforms": preflight["supports_pipeline_gpu_transforms"],
            "supports_pipeline_torch_compile": preflight["supports_pipeline_torch_compile"],
            "supports_training_loop_prefetch": preflight["supports_training_loop_prefetch"],
            "supports_resume_cursor": preflight["supports_resume_cursor"],
            "supports_resume_state_file": preflight["supports_resume_state_file"],
            "resume_state_format": preflight["resume_state_format"],
            "root": preflight["root"],
            "remote_root": preflight["remote_root"],
            "weights": preflight["weights"],
            "source_count": preflight["source_count"],
            "weighted_sources": preflight["weighted_sources"],
            "read_mode": preflight["read_mode"],
            "trainer_batch_iterator": preflight["trainer_batch_iterator"],
            "data_loader_workers": preflight["data_loader_workers"],
            "trainer_loop_options": preflight["trainer_loop_options"],
            "trainer_prefetch_batches": preflight["trainer_prefetch_batches"],
            "stream_kwargs": preflight["stream_kwargs"],
            "stream_kwargs_keys": preflight["stream_kwargs_keys"],
            "remote_client_kwargs": preflight["remote_client_kwargs"],
            "bulk_transport": preflight["bulk_transport"],
            "initial_cords": preflight["initial_cords"],
            "max_cords": preflight["max_cords"],
            "bulk_read_retries": preflight["bulk_read_retries"],
            "bulk_write_retries": preflight["bulk_write_retries"],
        }

    @contextmanager
    def open_training_loop(
        self,
        credentials: ClientCredentials | None = None,
        *,
        credential_prompt: Callable[[], ClientCredentials] | None = None,
        **kwargs: Any,
    ) -> Iterator[CxnfsTrainerStream | CxnfsTrainerPrefetchStream]:
        prompted_credentials: ClientCredentials | None = None
        active_credentials = credentials
        client = kwargs.get("client")
        client_factory = kwargs.get("client_factory")
        if (
            self.remote_client_required
            and active_credentials is None
            and client is None
            and client_factory is None
        ):
            if credential_prompt is None:
                raise ValueError("remote trainer runtime binding requires credentials or credential_prompt")
            prompted_credentials = credential_prompt()
            active_credentials = prompted_credentials
        try:
            with self.plan.open_training_loop(active_credentials, **kwargs) as stream:
                yield stream
        finally:
            if prompted_credentials is not None:
                prompted_credentials.close()

    @contextmanager
    def open_pipeline_training_loop(
        self,
        credentials: ClientCredentials | None = None,
        *,
        credential_prompt: Callable[[], ClientCredentials] | None = None,
        **kwargs: Any,
    ) -> Iterator[CxnfsTrainerPipelineLoop]:
        prompted_credentials: ClientCredentials | None = None
        active_credentials = credentials
        client_factory = kwargs.get("client_factory")
        if (
            self.remote_client_required
            and active_credentials is None
            and client_factory is None
        ):
            if credential_prompt is None:
                raise ValueError("remote trainer runtime binding requires credentials or credential_prompt")
            prompted_credentials = credential_prompt()
            active_credentials = prompted_credentials
        try:
            with self.plan.open_pipeline_training_loop(active_credentials, **kwargs) as stream:
                yield stream
        finally:
            if prompted_credentials is not None:
                prompted_credentials.close()


def _trainer_loop_base_stream(
    trainer_loop: CxnfsTrainerStream | CxnfsTrainerPrefetchStream,
) -> CxnfsTrainerStream:
    if isinstance(trainer_loop, CxnfsTrainerPrefetchStream):
        return trainer_loop.stream
    if isinstance(trainer_loop, CxnfsTrainerStream):
        return trainer_loop
    raise TypeError("trainer loop context returned an unsupported stream object")


def build_trainer_loop_contract_check_payload(
    contract_payload: Mapping[str, Any],
    *,
    contract_path: str | Path | None = None,
    require_remote: bool = False,
    require_remote_quic: bool = False,
    min_world_size: int = 1,
    require_rank_states: bool = False,
    require_prefetch: bool = False,
    require_pipeline: bool = False,
    require_direct_tensors: bool = False,
    require_torch_compile: bool = False,
) -> dict[str, Any]:
    contract = _mapping(contract_payload)
    entrypoint = _mapping(contract.get("entrypoint"))
    authentication = _mapping(contract.get("authentication"))
    mount_free = _mapping(contract.get("mount_free_contract"))
    stream = _mapping(contract.get("stream_contract"))
    pipeline = _mapping(contract.get("pipeline_contract"))
    batch = _mapping(contract.get("batch_contract"))
    step_trace = _mapping(contract.get("step_trace_contract"))
    distributed = _mapping(contract.get("distributed_contract"))
    resume = _mapping(contract.get("resume_contract"))
    transport = _mapping(contract.get("remote_transport_contract"))
    artifact_paths = _mapping(contract.get("artifact_paths"))
    pipeline_tensor_allocation = _mapping(pipeline.get("tensor_allocation"))
    pipeline_tensor_memory_path = _mapping(pipeline.get("tensor_memory_path"))
    pipeline_network_path = _mapping(pipeline.get("pipeline_network_path"))
    batch_fields = {
        str(value)
        for value in batch.get("fields", ())
        if isinstance(value, str)
    }
    sample_fields = {
        str(value)
        for value in batch.get("sample_fields", ())
        if isinstance(value, str)
    }
    step_trace_fields = {
        str(value)
        for value in step_trace.get("fields", ())
        if isinstance(value, str)
    }
    active_min_world_size = max(1, int(min_world_size))
    remote_required = bool(authentication.get("remote_client_required"))
    remote_quic_required = bool(require_remote_quic)
    checks = {
        "format_valid": contract.get("format") == TRAINER_LOOP_CONTRACT_FORMAT,
        "contract_passed": bool(contract.get("passed")),
        "runtime_binding_format_valid": contract.get("runtime_binding_format") == TRAINER_RUNTIME_BINDING_FORMAT,
        "stream_state_format_valid": contract.get("stream_state_format") == TRAINER_STREAM_STATE_FORMAT,
        "runtime_binding_loader_valid": entrypoint.get("runtime_binding_loader") == "CxnfsTrainerRuntimeBinding.from_file",
        "training_loop_context_valid": entrypoint.get("training_loop_context") == "CxnfsTrainerRuntimeBinding.open_training_loop",
        "batch_object_valid": entrypoint.get("batch_object") == "CxnfsTrainerBatch",
        "password_cli_argument_absent": authentication.get("password_cli_argument") is False,
        "raw_password_not_sent": authentication.get("raw_password_sent_over_control") is False,
        "credential_prompt_supported": bool(authentication.get("credential_prompt_supported")),
        "mount_not_required": mount_free.get("requires_mount") is False,
        "trainer_code_not_touched": mount_free.get("touches_trainer_code") is False,
        "preflight_opens_no_network": mount_free.get("opens_network_connections_during_preflight") is False,
        "preflight_consumes_no_batches": mount_free.get("consumes_batches_during_preflight") is False,
        "safe_before_workers": mount_free.get("safe_to_run_before_trainer_workers") is True,
        "stream_factory_valid": stream.get("stream_factory") == "open_weighted_mds_trainer_stream",
        "read_mode_supported": stream.get("read_mode") in {mode.value for mode in TrainerReadMode},
        "trainer_batch_iterator_supported": stream.get("trainer_batch_iterator") in TRAINER_BATCH_ITERATORS,
        "direct_python_streaming_verified": stream.get("direct_python_streaming_verified") is True,
        "runtime_training_loop_verified": stream.get("runtime_training_loop_verified") is True,
        "pipeline_contract_present": bool(pipeline),
        "training_pipeline_object_valid": pipeline.get("training_pipeline_object") == "CxnfsTrainingPipeline",
        "training_pipeline_context_valid": pipeline.get("training_pipeline_context") in {
            "CxnfsTrainingPipeline.read_tensor_batch",
            "CxnfsTrainingPipeline.read_into_tensors",
            "CxnfsTrainingPipeline.prefetch_trainer_stream",
        },
        "pipeline_descriptor_iterator_valid": (
            pipeline.get("pipeline_descriptor_iterator")
            == "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors"
        ),
        "pipeline_direct_tensor_batches_supported": (
            pipeline.get("supports_pipeline_direct_tensor_batches") is True
        ),
        "pipeline_python_byte_batch_avoidance_supported": (
            pipeline.get("python_byte_batch_avoidance_supported") is True
        ),
        "pipeline_cpu_augmentation_default_safe": (
            pipeline.get("pipeline_cpu_augmentation_default") == "disabled-with-explicit-opt-in"
        ),
        "batch_fields_complete": {"sample_count", "byte_size", "source_counts", "resume_cursor"}.issubset(batch_fields),
        "sample_fields_complete": {"source_name", "source_index", "sample_index", "data"}.issubset(sample_fields),
        "step_trace_contract_present": bool(step_trace),
        "step_trace_format_valid": step_trace.get("report_format") == "cxnfs.trainer-step-trace.v1",
        "step_trace_fields_complete": {
            "step_index",
            "phase",
            "microbatch_count",
            "sample_count",
            "byte_size",
            "source_counts",
            "sample_identities",
            "resume_start_emitted",
            "resume_final_emitted",
            "cursor_trace_source",
            "cursor_delta_verified",
        }.issubset(step_trace_fields),
        "step_trace_cursor_delta_required": step_trace.get("requires_cursor_delta_verified") is True,
        "step_trace_resume_continuity_required": step_trace.get("requires_resume_split_continuity") is True,
        "step_trace_identity_uniqueness_default": (
            step_trace.get("requires_unique_sample_identities_by_default") is True
        ),
        "cursor_format_valid": resume.get("cursor_format") == TRAINER_CURSOR_FORMAT,
        "state_format_valid": resume.get("state_format") == TRAINER_STREAM_STATE_FORMAT,
        "loop_resume_verified": resume.get("loop_resume_verified") is True,
        "world_size_meets_minimum": _int_value(distributed.get("world_size"), 0) >= active_min_world_size,
        "cross_rank_overlap_free": distributed.get("cross_rank_overlap_free") is True,
    }
    if require_rank_states:
        checks["rank_states_written"] = distributed.get("rank_states_written") is True
    if require_prefetch:
        checks["trainer_prefetch_enabled"] = _int_value(stream.get("trainer_prefetch_batches"), 0) > 0
    if require_pipeline:
        checks["pipeline_stream_context_valid"] = (
            pipeline.get("pipeline_stream_context") == "CxnfsTrainingPipeline.prefetch_trainer_stream"
        )
        checks["pipeline_training_loop_context_valid"] = (
            entrypoint.get("pipeline_training_loop_context")
            == "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"
        )
        checks["pipeline_loop_object_valid"] = (
            entrypoint.get("pipeline_loop_object") == "CxnfsTrainerPipelineLoop"
            and pipeline.get("pipeline_loop_object") == "CxnfsTrainerPipelineLoop"
        )
        checks["pipeline_descriptor_stream_valid"] = (
            pipeline.get("pipeline_descriptor_stream") == "CxnfsTrainerPipelineStream"
        )
        checks["pipeline_stream_state_format_valid"] = (
            pipeline.get("pipeline_stream_state_format") == TRAINER_STREAM_STATE_FORMAT
        )
        checks["pipeline_stream_save_method_valid"] = (
            pipeline.get("pipeline_stream_save_method") == "CxnfsTrainerPipelineStream.save_state_file"
        )
        checks["pipeline_stream_load_method_valid"] = (
            pipeline.get("pipeline_stream_load_method") == "CxnfsTrainerPipelineStream.load_state_file"
        )
        checks["pipeline_stream_validates_resume_contract"] = (
            pipeline.get("pipeline_stream_validates_resume_contract") is True
        )
        checks["pipeline_prefetch_result_object_valid"] = (
            pipeline.get("pipeline_prefetch_result_object") == "CxnfsPipelinePrefetchedTensorBatch"
        )
    if require_direct_tensors:
        checks["pipeline_direct_tensor_read_mode_selected"] = (
            pipeline.get("direct_tensor_read_mode_selected") is True
            and stream.get("read_mode") == TrainerReadMode.DIRECT_TENSORS.value
        )
        checks["pipeline_tensor_allocation_declared"] = bool(pipeline_tensor_allocation)
        checks["pipeline_tensor_allocation_target_supported"] = (
            str(pipeline_tensor_allocation.get("target") or "") in TRAINER_TENSOR_TARGETS
        )
        checks["pipeline_caller_owned_tensor_destination"] = (
            pipeline_tensor_memory_path.get("caller_owned_destination") is True
        )
        checks["pipeline_python_byte_batch_avoided"] = (
            pipeline_tensor_memory_path.get("python_byte_batch_avoided") is True
        )
        checks["pipeline_network_fills_or_stages_tensor_destination"] = (
            pipeline_tensor_memory_path.get("network_fills_tensor_destination") is True
            or pipeline_tensor_memory_path.get("requires_pinned_host_staging") is True
        )
        checks["pipeline_network_path_complete"] = (
            pipeline_network_path.get("format") == TRAINER_PIPELINE_NETWORK_PATH_FORMAT
            and pipeline.get("pipeline_network_path_verified") is True
            and pipeline_network_path.get("complete") is True
            and _int_value(pipeline_network_path.get("network_read_workers"), 0) > 1
            and _int_value(pipeline_network_path.get("max_cords"), 0) > 1
            and _int_value(pipeline_network_path.get("direct_destination_read_operations"), 0) > 0
        )
    if require_torch_compile:
        checks["pipeline_torch_compile_supported"] = pipeline.get("supports_pipeline_torch_compile") is True
    if require_remote or remote_quic_required:
        checks["remote_client_required"] = remote_required
        checks["remote_requires_credentials"] = authentication.get("requires_credentials") is True
        checks["remote_credential_prompt_required"] = (
            authentication.get("credential_prompt_required_for_remote") is True
        )
    if remote_quic_required:
        checks["remote_quic_bulk_transport"] = transport.get("bulk_transport") == PYTHON_BULK_TRANSPORT_QUIC
        checks["remote_quic_ready"] = transport.get("ready_for_remote_quic") is True
        checks["remote_direct_quic_memory_verified"] = (
            transport.get("direct_quic_memory_path_verified") is True
        )
        checks["remote_quic_cord_limit"] = _int_value(transport.get("max_cords"), 0) >= ABSOLUTE_MAX_CORDS
        checks["remote_per_source_client_default"] = transport.get("per_source_client_default") is True

    failed_checks = [name for name, passed in checks.items() if not passed]
    passed = not failed_checks
    return {
        "format": TRAINER_LOOP_CONTRACT_CHECK_FORMAT,
        "passed": passed,
        "status": "passed" if passed else "failed",
        "contract_path": None if contract_path is None else str(contract_path),
        "contract_format": contract.get("format"),
        "contract_passed": bool(contract.get("passed")),
        "requirements": {
            "require_remote": bool(require_remote),
            "require_remote_quic": bool(require_remote_quic),
            "min_world_size": active_min_world_size,
            "require_rank_states": bool(require_rank_states),
            "require_prefetch": bool(require_prefetch),
            "require_pipeline": bool(require_pipeline),
            "require_direct_tensors": bool(require_direct_tensors),
            "require_torch_compile": bool(require_torch_compile),
        },
        "failed_checks": failed_checks,
        "checks": checks,
        "artifact_paths": dict(artifact_paths),
        "entrypoint": dict(entrypoint),
        "authentication": dict(authentication),
        "mount_free_contract": dict(mount_free),
        "stream_contract": dict(stream),
        "pipeline_contract": dict(pipeline),
        "batch_contract": dict(batch),
        "distributed_contract": dict(distributed),
        "resume_contract": dict(resume),
        "remote_transport_contract": dict(transport),
    }


def run_trainer_loop_contract_check_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
) -> int:
    parser = ArgumentParser(
        description=(
            "Validate a standalone CXNFS trainer loop contract without prompting, "
            "opening sockets, or consuming batches."
        )
    )
    parser.add_argument("contract", help="Path to a cxnfs.trainer-loop-contract.v1 JSON report.")
    parser.add_argument("--require-remote", action="store_true", help="Require a remote client and credential prompt policy.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require remote QUIC direct-memory evidence.")
    parser.add_argument("--min-world-size", type=int, default=1, help="Minimum distributed world size required by the contract.")
    parser.add_argument("--require-rank-states", action="store_true", help="Require rank-state files to have been written.")
    parser.add_argument("--require-prefetch", action="store_true", help="Require trainer-loop prefetch to be enabled.")
    parser.add_argument("--require-pipeline", action="store_true", help="Require the direct tensor training pipeline contract.")
    parser.add_argument("--require-direct-tensors", action="store_true", help="Require the selected trainer loop to use direct tensor reads.")
    parser.add_argument("--require-torch-compile", action="store_true", help="Require torch.compile-capable pipeline transforms.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        contract_path = Path(args.contract)
        payload = build_trainer_loop_contract_check_payload(
            _read_json_mapping(contract_path, "trainer loop contract"),
            contract_path=contract_path,
            require_remote=args.require_remote,
            require_remote_quic=args.require_remote_quic,
            min_world_size=args.min_world_size,
            require_rank_states=args.require_rank_states,
            require_prefetch=args.require_prefetch,
            require_pipeline=args.require_pipeline,
            require_direct_tensors=args.require_direct_tensors,
            require_torch_compile=args.require_torch_compile,
        )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_loop_contract_check() -> None:
    raise SystemExit(run_trainer_loop_contract_check_cli())


def run_trainer_preflight_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
) -> int:
    parser = ArgumentParser(description="Validate a CXNFS trainer handoff without prompting, opening sockets, or consuming batches.")
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--require-ready", action="store_true", default=True, help="Require ready_for_trainer_wiring to be true.")
    parser.add_argument("--allow-not-ready", dest="require_ready", action="store_false", help="Emit a preflight report even when the handoff is not ready.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        plan = CxnfsTrainerIntegrationPlan.from_file(
            args.handoff,
            require_ready=args.require_ready,
            require_remote_quic=args.require_remote_quic,
        )
        payload = plan.preflight_report()
        indent = None if args.compact else 2
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_preflight() -> None:
    raise SystemExit(run_trainer_preflight_cli())


def build_trainer_network_preflight_payload(
    handoff_payload: Mapping[str, Any],
    credentials: ClientCredentials,
    *,
    require_ready: bool = True,
    require_remote_quic: bool = False,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    timeout: float | None = 10.0,
    tls_context: Any = None,
    server_hostname: str | None = None,
    probe_path: str | None = None,
    read_path: str | None = None,
    read_length: int = 1,
    bulk_transport: str | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
) -> dict[str, Any]:
    if credentials is None:
        raise ValueError("credentials are required")
    if read_length <= 0:
        raise ValueError("read_length must be positive")
    plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
        handoff_payload,
        require_ready=require_ready,
        require_remote_quic=require_remote_quic,
    )
    preflight = plan.preflight_report()
    if not preflight["remote_client_required"]:
        raise ValueError("trainer network preflight requires a remote trainer handoff")
    remote_root = preflight.get("remote_root")
    if not remote_root:
        raise ValueError("trainer network preflight requires remote_root")
    selected_probe_path = normalize_namespace_root(
        probe_path or str(remote_root),
        label="trainer network probe path",
    )
    selected_read_path = normalize_namespace_root(
        read_path or _default_trainer_network_preflight_read_path(plan),
        label="trainer network read path",
    )
    selected_bulk_transport = require_supported_python_bulk_transport(
        bulk_transport or preflight.get("bulk_transport") or PYTHON_BULK_TRANSPORT_STREAM
    )
    result = connect_and_probe_cxnfs_health(
        credentials,
        host=host,
        control_port=control_port,
        transport_port=transport_port,
        probe_path=selected_probe_path,
        bulk_probe_path=selected_read_path,
        bulk_probe_length=read_length,
        bulk_transport=selected_bulk_transport,
        timeout=timeout,
        tls_context=tls_context,
        server_hostname=server_hostname or host,
        control_client_factory=control_client_factory,
        bulk_client_factory=bulk_client_factory,
    )
    return {
        "format": TRAINER_NETWORK_PREFLIGHT_FORMAT,
        "passed": result.ready,
        "handoff_format": handoff_payload.get("format"),
        "preflight_format": preflight["format"],
        "ready_for_trainer_wiring": preflight["ready_for_trainer_wiring"],
        "ready_for_remote_quic": preflight["ready_for_remote_quic"],
        "ready_for_blockfs_candidate": preflight["ready_for_blockfs_candidate"],
        "requires_mount": preflight["requires_mount"],
        "touches_trainer_code": preflight["touches_trainer_code"],
        "safe_to_run_before_trainer_workers": True,
        "opens_network_connections": True,
        "consumes_batches": False,
        "remote_client_required": True,
        "requires_credentials": True,
        "raw_password_sent_over_control": preflight["raw_password_sent_over_control"],
        "host": host,
        "control_port": control_port,
        "transport_port": transport_port,
        "server_hostname": server_hostname or host,
        "transport": "cxnfs-quic" if result.bulk_transport == PYTHON_BULK_TRANSPORT_QUIC else "cxnfs-length-prefixed",
        "session_id": str(result.session_id),
        "authenticated": True,
        "probe_path": result.probe_path,
        "probe_entries": result.probe_entries,
        "bulk_probe": result.bulk_probe,
        "bulk_probe_path": result.bulk_probe_path,
        "bulk_probe_bytes": result.bulk_probe_bytes,
        "bulk_transport": result.bulk_transport,
        "read_length": read_length,
        "remote_root": preflight["remote_root"],
        "root": preflight["root"],
        "weights": preflight["weights"],
        "weighted_sources": preflight["weighted_sources"],
        "source_count": preflight["source_count"],
        "read_mode": preflight["read_mode"],
        "trainer_batch_iterator": preflight["trainer_batch_iterator"],
        "data_loader_workers": preflight["data_loader_workers"],
        "stream_factory": preflight["stream_factory"],
        "stream_object": preflight["stream_object"],
        "batch_object": preflight["batch_object"],
        "remote_client_factory": preflight["remote_client_factory"],
        "remote_stream_context": preflight["remote_stream_context"],
        "training_loop_context": preflight["training_loop_context"],
        "pipeline_training_loop_context": preflight["pipeline_training_loop_context"],
        "training_pipeline_object": preflight["training_pipeline_object"],
        "training_pipeline_context": preflight["training_pipeline_context"],
        "supports_pipeline_direct_tensor_batches": preflight["supports_pipeline_direct_tensor_batches"],
        "supports_pipeline_gpu_transforms": preflight["supports_pipeline_gpu_transforms"],
        "supports_pipeline_torch_compile": preflight["supports_pipeline_torch_compile"],
        "initial_cords": preflight["initial_cords"],
        "max_cords": preflight["max_cords"],
        "bulk_read_retries": preflight["bulk_read_retries"],
        "bulk_write_retries": preflight["bulk_write_retries"],
        "network_probe_is_handoff_aware": True,
        "trainer_code_touched": False,
    }


def run_trainer_network_preflight_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(
        description=(
            "Run a CXNFS trainer handoff network preflight with hidden credentials "
            "and a tiny bulk read probe."
        )
    )
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--require-ready", action="store_true", default=True, help="Require ready_for_trainer_wiring.")
    parser.add_argument("--allow-not-ready", dest="require_ready", action="store_false", help="Allow a deferred handoff.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require remote QUIC readiness proofs.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--path", help="Remote directory to list before the bulk probe; defaults to remote_root.")
    parser.add_argument("--read-path", help="Remote shard path to read; defaults to the first weighted source shard.")
    parser.add_argument("--read-length", type=int, default=1, help="Probe byte count; defaults to 1.")
    parser.add_argument("--bulk-transport", help="Override stream or QUIC bulk transport from the handoff.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        handoff = _read_json_mapping(Path(args.handoff), "trainer handoff")
        plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
            handoff,
            require_ready=args.require_ready,
            require_remote_quic=args.require_remote_quic,
        )
        preflight = plan.preflight_report()
        if not preflight["remote_client_required"]:
            raise ValueError("trainer network preflight requires a remote trainer handoff")
        read_path = args.read_path or _default_trainer_network_preflight_read_path(plan)
        bulk_transport = require_supported_python_bulk_transport(
            args.bulk_transport or preflight.get("bulk_transport") or PYTHON_BULK_TRANSPORT_STREAM
        )
        tls_context = tls_context_factory(args.cafile) if args.cafile else None
        with credential_prompt() as credentials:
            payload = build_trainer_network_preflight_payload(
                handoff,
                credentials,
                require_ready=args.require_ready,
                require_remote_quic=args.require_remote_quic,
                host=args.host,
                control_port=args.control_port,
                transport_port=args.transport_port,
                timeout=args.timeout,
                tls_context=tls_context,
                server_hostname=args.server_hostname or args.host,
                probe_path=args.path,
                read_path=read_path,
                read_length=args.read_length,
                bulk_transport=bulk_transport,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
            )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except ValueError as exc:
        err.write(str(exc) + "\n")
        return 2
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_network_preflight() -> None:
    raise SystemExit(run_trainer_network_preflight_cli())


def _default_trainer_network_preflight_read_path(plan: CxnfsTrainerIntegrationPlan) -> str:
    stream_kwargs = plan.stream_kwargs()
    remote_root = stream_kwargs.get("remote_root")
    weights = _mapping(stream_kwargs.get("weights"))
    if not remote_root:
        raise ValueError("trainer network preflight requires remote_root")
    if not weights:
        raise ValueError("trainer network preflight requires weighted sources")
    first_source = normalize_relative_namespace_path(next(iter(weights)), label="weighted source")
    source_leaf = first_source.rsplit("/", 1)[-1]
    return join_namespace_path(remote_root, f"{first_source}/{source_leaf}-000000.mds")


def run_trainer_distributed_plan_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
) -> int:
    parser = ArgumentParser(
        description=(
            "Build per-rank CXNFS trainer stream plans from a trainer handoff without prompting, "
            "opening sockets, or consuming batches."
        )
    )
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--world-size", type=int, help="Distributed trainer world size; defaults to WORLD_SIZE.")
    parser.add_argument("--rank", type=int, help="Optional rank to mirror into selected_rank_plan.")
    parser.add_argument("--require-ready", action="store_true", default=True, help="Require ready_for_trainer_wiring to be true.")
    parser.add_argument("--allow-not-ready", dest="require_ready", action="store_false", help="Emit a report even when the handoff is not ready.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        plan = CxnfsTrainerIntegrationPlan.from_file(
            args.handoff,
            require_ready=args.require_ready,
            require_remote_quic=args.require_remote_quic,
        )
        env_shard = current_training_shard() if args.world_size is None else None
        world_size = args.world_size
        if world_size is None:
            if env_shard is None:
                env_shard = current_training_shard()
            world_size = env_shard.count
        selected_rank = args.rank
        if selected_rank is None and env_shard is not None:
            selected_rank = env_shard.index
        payload = plan.distributed_plan_report(world_size, selected_rank=selected_rank)
        indent = None if args.compact else 2
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_distributed_plan() -> None:
    raise SystemExit(run_trainer_distributed_plan_cli())


def build_trainer_integration_smoke_payload(
    handoff_payload: Mapping[str, Any],
    *,
    batches: int = 1,
    trainer_prefetch_batches: int | None = None,
    require_remote_quic: bool = False,
    client_factory: DatasetClientFactory | None = None,
) -> dict[str, Any]:
    if batches <= 0:
        raise ValueError("batches must be positive")
    plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
        handoff_payload,
        require_remote_quic=require_remote_quic,
    )
    plan_payload = plan.to_dict()
    if bool(plan_payload.get("remote_client_required")) and client_factory is None:
        raise ValueError("remote trainer handoff smoke requires a caller-provided client_factory")
    stream_kwargs = plan.stream_kwargs()
    if client_factory is not None:
        stream_kwargs["client_factory"] = client_factory
    loop_options = plan.trainer_loop_options()
    active_prefetch = _validate_trainer_prefetch_batches(
        _int_value(
            trainer_prefetch_batches
            if trainer_prefetch_batches is not None
            else loop_options.get("trainer_prefetch_batches"),
            0,
        )
    )
    read_mode = TrainerReadMode(stream_kwargs.get("read_mode") or TrainerReadMode.DIRECT_BUFFERS.value)
    tensor_allocation = stream_kwargs.get("tensor_allocation")
    if not isinstance(tensor_allocation, CxnfsTrainerTensorAllocation):
        tensor_allocation = CxnfsTrainerTensorAllocation()
    remote_options = _trainer_integration_smoke_remote_options(handoff_payload, stream_kwargs)
    start = time.perf_counter()
    stream_overrides = {"client_factory": client_factory} if client_factory is not None else {}
    with plan.open_training_loop(
        trainer_prefetch_batches=active_prefetch,
        **stream_overrides,
    ) as trainer_loop:
        stream = _trainer_loop_base_stream(trainer_loop)
        training_loop_object = type(trainer_loop).__name__
        training_loop_base_stream_object = type(stream).__name__
        training_loop_prefetch_applied = isinstance(trainer_loop, CxnfsTrainerPrefetchStream)
        collected = list(trainer_loop.iter_batches(limit=batches))
        elapsed_seconds = max(time.perf_counter() - start, 1e-9)
        stream_report = _trainer_stream_smoke_report(
            stream.dataset,
            collected,
            root=str(stream_kwargs.get("root") or ""),
            batch_size=_int_value(stream_kwargs.get("batch_size"), 0),
            requested_batches=batches,
            warmup_batches=0,
            elapsed_seconds=elapsed_seconds,
            remote_options=remote_options,
            read_mode=read_mode,
            trainer_batch_iterator=str(stream_kwargs.get("trainer_batch_iterator") or TRAINER_BATCH_ITERATOR_NATIVE),
            data_loader_workers=_int_value(stream_kwargs.get("data_loader_workers"), 0),
            trainer_prefetch_batches=active_prefetch,
            tensor_allocation=tensor_allocation,
            start_cursor=None,
            stream_state=stream.state_dict(),
            client_factory=client_factory,
        )
    stream_report["remote_client_topology_after_close"] = _trainer_remote_client_topology(
        client_factory,
        remote_options,
        _int_value(stream_report.get("source_count"), 0),
    )
    stream_report["remote_client_lifecycle_closed"] = bool(
        _mapping(stream_report["remote_client_topology_after_close"]).get("all_open_clients_closed")
    )
    read_path_evidence = _mapping(stream_report.get("read_path_evidence"))
    worker_partition = _mapping(stream_report.get("worker_partition_evidence"))
    remote_client_warmup = _mapping(stream_report.get("remote_client_warmup"))
    remote_client_topology = _mapping(stream_report.get("remote_client_topology"))
    remote_client_topology_gate = _trainer_remote_client_topology_gate(
        remote_client_topology,
        expected_source_count=_int_value(stream_report.get("source_count"), 0),
        require_remote=bool(remote_options.get("remote_root")),
        require_per_source=bool(remote_options.get("remote_root")) and not bool(remote_options.get("shared_client")),
    )
    return {
        "format": TRAINER_INTEGRATION_SMOKE_FORMAT,
        "passed": True,
        "handoff_format": handoff_payload.get("format"),
        "plan_format": plan_payload.get("format"),
        "stream_factory": plan_payload.get("stream_factory"),
        "remote_client_required": bool(plan_payload.get("remote_client_required")),
        "requires_mount": bool(handoff_payload.get("requires_mount")),
        "touches_trainer_code": bool(handoff_payload.get("touches_trainer_code")),
        "stream_opened": True,
        "stream_object": plan_payload.get("stream_object") or "CxnfsTrainerStream",
        "batch_object": plan_payload.get("batch_object") or "CxnfsTrainerBatch",
        "trainer_prefetch_batches": active_prefetch,
        "training_loop_context": plan_payload.get("training_loop_context")
        or "CxnfsTrainerIntegrationPlan.open_training_loop",
        "training_loop_context_used": True,
        "training_loop_object": training_loop_object,
        "training_loop_base_stream_object": training_loop_base_stream_object,
        "training_loop_prefetch_batches": active_prefetch,
        "training_loop_prefetch_applied": training_loop_prefetch_applied,
        "stream_kwargs_accepted": True,
        "stream_kwargs_keys": sorted(k for k in plan.stream_kwargs().keys()),
        "trainer_batch_iterator": stream_report.get("trainer_batch_iterator"),
        "data_loader_workers": _int_value(stream_report.get("data_loader_workers"), 0),
        "dataloader_worker_partitioning_enabled": bool(worker_partition.get("worker_sharding_enabled")),
        "worker_partition_complete": bool(worker_partition.get("complete")),
        "worker_batch_read_evidence_observed": bool(read_path_evidence.get("worker_batch_read_evidence_observed")),
        "worker_batch_read_evidence_count": _int_value(read_path_evidence.get("worker_batch_read_evidence_count"), 0),
        "remote_client_warmup_complete": bool(remote_client_warmup.get("complete")),
        "remote_client_warmup_source_count": _int_value(remote_client_warmup.get("source_count"), 0),
        "remote_client_warmup_warmed_source_count": _int_value(remote_client_warmup.get("warmed_source_count"), 0),
        "remote_client_topology_complete": bool(remote_client_topology_gate.get("complete")),
        "remote_client_per_source_complete": bool(remote_client_topology_gate.get("per_source_client_complete")),
        "remote_client_lifecycle_closed": bool(stream_report.get("remote_client_lifecycle_closed")),
        "batch_count": stream_report["batch_count"],
        "sample_count": stream_report["sample_count"],
        "byte_size": stream_report["byte_size"],
        "stream_state_format": stream_report["stream_state_format"],
        "stream_state_matches_final_cursor": stream_report["stream_state_matches_final_cursor"],
        "read_path_reliability": stream_report["read_path_reliability"],
        "worker_partition_evidence": stream_report["worker_partition_evidence"],
        "stream_report": stream_report,
    }


def build_trainer_runtime_smoke_payload(
    handoff_payload: Mapping[str, Any],
    *,
    batches: int = 1,
    rank: int | None = None,
    world_size: int | None = None,
    trainer_prefetch_batches: int | None = None,
    require_remote_quic: bool = False,
    credentials: ClientCredentials | None = None,
    credential_prompt: Callable[[], ClientCredentials] | None = None,
    client_factory: DatasetClientFactory | None = None,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    timeout: float | None = 10.0,
    tls_context: Any = None,
    server_hostname: str | None = None,
    cafile: str | None = None,
    initial_cords: int | None = None,
    max_cords: int | None = None,
    provisional_container_size: int | None = None,
    expected_size_limit: int | None = None,
    write_connections: int | None = None,
    bulk_transport: str | None = None,
    bulk_read_retries: int | None = None,
    bulk_write_retries: int | None = None,
    shared_client: bool | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> dict[str, Any]:
    if batches <= 0:
        raise ValueError("batches must be positive")
    binding = CxnfsTrainerRuntimeBinding.from_handoff_payload(
        handoff_payload,
        rank=rank,
        world_size=world_size,
        require_remote_quic=require_remote_quic,
    )
    binding_payload = binding.to_dict()
    if binding.remote_client_required and credentials is None and credential_prompt is None and client_factory is None:
        raise ValueError("remote trainer runtime smoke requires credentials, credential_prompt, or client_factory")
    stream_kwargs = binding.stream_kwargs()
    if client_factory is not None:
        stream_kwargs["client_factory"] = client_factory
    loop_options = binding.trainer_loop_options()
    active_prefetch = _validate_trainer_prefetch_batches(
        _int_value(
            trainer_prefetch_batches
            if trainer_prefetch_batches is not None
            else loop_options.get("trainer_prefetch_batches"),
            0,
        )
    )
    read_mode = TrainerReadMode(stream_kwargs.get("read_mode") or TrainerReadMode.DIRECT_BUFFERS.value)
    tensor_allocation = stream_kwargs.get("tensor_allocation")
    if not isinstance(tensor_allocation, CxnfsTrainerTensorAllocation):
        tensor_allocation = CxnfsTrainerTensorAllocation()
    remote_options = _trainer_integration_smoke_remote_options(handoff_payload, stream_kwargs)
    credential_prompt_called = False
    prompted_credentials: ClientCredentials | None = None
    active_credentials = credentials
    owned_client_factory: DatasetClientFactory | None = None
    active_client_factory = client_factory
    owned_client_factory_closed = False
    if binding.remote_client_required and active_client_factory is None:
        if active_credentials is None:
            if credential_prompt is None:
                raise ValueError("remote trainer runtime smoke requires credentials or credential_prompt")
            active_credentials = credential_prompt()
            prompted_credentials = active_credentials
            credential_prompt_called = True
        owned_client_factory = binding.plan.create_remote_client_factory(
            active_credentials,
            host=host,
            control_port=control_port,
            transport_port=transport_port,
            timeout=timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
            cafile=cafile,
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size=provisional_container_size,
            expected_size_limit=expected_size_limit,
            write_connections=write_connections,
            bulk_transport=bulk_transport,
            bulk_read_retries=bulk_read_retries,
            bulk_write_retries=bulk_write_retries,
            shared_client=shared_client,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        active_client_factory = owned_client_factory
    stream_overrides = {"client_factory": active_client_factory} if active_client_factory is not None else {}
    start = time.perf_counter()
    pipeline_loop_report: dict[str, Any] = {
        "required": read_mode == TrainerReadMode.DIRECT_TENSORS,
        "verified": False,
        "context_used": False,
    }
    try:
        with binding.open_training_loop(
            active_credentials,
            host=host,
            control_port=control_port,
            transport_port=transport_port,
            timeout=timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
            cafile=cafile,
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size=provisional_container_size,
            expected_size_limit=expected_size_limit,
            write_connections=write_connections,
            bulk_transport=bulk_transport,
            bulk_read_retries=bulk_read_retries,
            bulk_write_retries=bulk_write_retries,
            shared_client=shared_client,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
            trainer_prefetch_batches=active_prefetch,
            **stream_overrides,
        ) as trainer_loop:
            stream = _trainer_loop_base_stream(trainer_loop)
            training_loop_object = type(trainer_loop).__name__
            training_loop_base_stream_object = type(stream).__name__
            training_loop_prefetch_applied = isinstance(trainer_loop, CxnfsTrainerPrefetchStream)
            collected = list(trainer_loop.iter_batches(limit=batches))
            elapsed_seconds = max(time.perf_counter() - start, 1e-9)
            stream_report = _trainer_stream_smoke_report(
                stream.dataset,
                collected,
                root=str(stream_kwargs.get("root") or ""),
                batch_size=_int_value(stream_kwargs.get("batch_size"), 0),
                requested_batches=batches,
                warmup_batches=0,
                elapsed_seconds=elapsed_seconds,
                remote_options=remote_options,
                read_mode=read_mode,
                trainer_batch_iterator=str(stream_kwargs.get("trainer_batch_iterator") or TRAINER_BATCH_ITERATOR_NATIVE),
                data_loader_workers=_int_value(stream_kwargs.get("data_loader_workers"), 0),
                trainer_prefetch_batches=active_prefetch,
                tensor_allocation=tensor_allocation,
                start_cursor=None,
                stream_state=stream.state_dict(),
                client_factory=active_client_factory,
            )
        if read_mode == TrainerReadMode.DIRECT_TENSORS:
            pipeline_loop_batches = []
            with binding.open_pipeline_training_loop(
                active_credentials,
                host=host,
                control_port=control_port,
                transport_port=transport_port,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                cafile=cafile,
                initial_cords=initial_cords,
                max_cords=max_cords,
                provisional_container_size=provisional_container_size,
                expected_size_limit=expected_size_limit,
                write_connections=write_connections,
                bulk_transport=bulk_transport,
                bulk_read_retries=bulk_read_retries,
                bulk_write_retries=bulk_write_retries,
                shared_client=shared_client,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
                tls_context_factory=tls_context_factory,
                pipeline_prefetch_batches=max(1, active_prefetch),
                **stream_overrides,
            ) as pipeline_loop:
                pipeline_loop_batches = list(pipeline_loop.iter_batches(limit=batches))
                pipeline_loop_state = pipeline_loop.state_dict()
                pipeline_loop_sample_count = sum(batch.sample_count for batch in pipeline_loop_batches)
                pipeline_loop_byte_size = sum(batch.byte_size for batch in pipeline_loop_batches)
                pipeline_loop_read_path = _trainer_pipeline_read_path_evidence(
                    pipeline_loop.stream.dataset,
                    pipeline_loop.pipeline,
                    pipeline_loop_batches,
                    pipeline_prefetch_batches=max(1, active_prefetch),
                )
                pipeline_loop_read_reliability = _trainer_read_path_reliability({
                    "read_path_evidence": pipeline_loop_read_path,
                })
                pipeline_loop_tensor_memory = _trainer_tensor_memory_path_report(
                    TrainerReadMode.DIRECT_TENSORS,
                    tensor_allocation,
                    pipeline_loop_read_path,
                    pipeline_loop_sample_count,
                )
                pipeline_loop_network_path = _trainer_pipeline_network_path_report(
                    pipeline_loop.pipeline,
                    pipeline_loop_read_path,
                    pipeline_loop_tensor_memory,
                    batch_count=len(pipeline_loop_batches),
                    sample_count=pipeline_loop_sample_count,
                    byte_size=pipeline_loop_byte_size,
                    requested_batches=batches,
                    pipeline_prefetch_batches=max(1, active_prefetch),
                    remote_options=remote_options,
                )
                pipeline_loop_report = {
                    "required": True,
                    "verified": len(pipeline_loop_batches) == batches
                    and pipeline_loop_sample_count > 0
                    and bool(pipeline_loop_network_path.get("complete")),
                    "context_used": True,
                    "context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                    "object": type(pipeline_loop).__name__,
                    "batch_count": len(pipeline_loop_batches),
                    "sample_count": pipeline_loop_sample_count,
                    "byte_size": pipeline_loop_byte_size,
                    "stream_state_format": pipeline_loop_state.get("format"),
                    "stream_state": pipeline_loop_state,
                    "queued_batches": pipeline_loop.queued_batches,
                    "read_path_evidence": pipeline_loop_read_path,
                    "read_path_reliability": pipeline_loop_read_reliability,
                    "tensor_memory_path": pipeline_loop_tensor_memory,
                    "pipeline_network_path": pipeline_loop_network_path,
                    "pipeline_network_path_format": pipeline_loop_network_path.get("format"),
                    "pipeline_network_path_complete": bool(pipeline_loop_network_path.get("complete")),
                    "pipeline_network_read_workers": pipeline_loop.pipeline.config.network_read_workers,
                    "pipeline_max_inflight_bytes": pipeline_loop.pipeline.config.max_inflight_bytes,
                }
        if owned_client_factory is not None:
            owned_client_factory.close()
            owned_client_factory_closed = True
        stream_report["remote_client_topology_after_close"] = _trainer_remote_client_topology(
            active_client_factory,
            remote_options,
            _int_value(stream_report.get("source_count"), 0),
        )
    finally:
        if owned_client_factory is not None and not owned_client_factory_closed:
            owned_client_factory.close()
        if prompted_credentials is not None:
            prompted_credentials.close()
    stream_report["remote_client_lifecycle_closed"] = bool(
        _mapping(stream_report["remote_client_topology_after_close"]).get("all_open_clients_closed")
    )
    read_path_evidence = _mapping(stream_report.get("read_path_evidence"))
    worker_partition = _mapping(stream_report.get("worker_partition_evidence"))
    remote_client_warmup = _mapping(stream_report.get("remote_client_warmup"))
    remote_client_topology = _mapping(stream_report.get("remote_client_topology"))
    remote_client_topology_gate = _trainer_remote_client_topology_gate(
        remote_client_topology,
        expected_source_count=_int_value(stream_report.get("source_count"), 0),
        require_remote=bool(remote_options.get("remote_root")),
        require_per_source=bool(remote_options.get("remote_root")) and not bool(remote_options.get("shared_client")),
    )
    return {
        "format": TRAINER_RUNTIME_SMOKE_FORMAT,
        "passed": True,
        "handoff_format": handoff_payload.get("format"),
        "plan_format": binding_payload.get("plan_format"),
        "runtime_binding_format": TRAINER_RUNTIME_BINDING_FORMAT,
        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
        "runtime_binding": binding_payload,
        "rank": binding.rank,
        "world_size": binding.world_size,
        "training_shard": _training_shard_payload(binding.training_shard),
        "stream_factory": binding_payload.get("stream_factory"),
        "remote_client_required": binding.remote_client_required,
        "requires_credentials": binding.remote_client_required,
        "requires_mount": bool(handoff_payload.get("requires_mount")),
        "touches_trainer_code": bool(handoff_payload.get("touches_trainer_code")),
        "credential_prompt_supported": True,
        "credential_prompt_used": credential_prompt_called,
        "client_factory_injected": client_factory is not None,
        "raw_password_sent_over_control": bool(binding_payload.get("raw_password_sent_over_control")),
        "stream_opened": True,
        "stream_object": binding_payload.get("stream_object") or "CxnfsTrainerStream",
        "batch_object": binding_payload.get("batch_object") or "CxnfsTrainerBatch",
        "read_mode": read_mode.value,
        "trainer_prefetch_batches": active_prefetch,
        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
        "integration_plan_training_loop_context": binding_payload.get("training_loop_context"),
        "training_loop_context_used": True,
        "training_loop_object": training_loop_object,
        "training_loop_base_stream_object": training_loop_base_stream_object,
        "training_loop_prefetch_batches": active_prefetch,
        "training_loop_prefetch_applied": training_loop_prefetch_applied,
        "pipeline_training_loop_required": bool(pipeline_loop_report.get("required")),
        "pipeline_training_loop_verified": bool(pipeline_loop_report.get("verified")),
        "pipeline_training_loop_context": pipeline_loop_report.get(
            "context",
            "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
        ),
        "pipeline_training_loop_context_used": bool(pipeline_loop_report.get("context_used")),
        "pipeline_training_loop_object": pipeline_loop_report.get("object"),
        "pipeline_training_loop_report": pipeline_loop_report,
        "pipeline_network_path_verified": bool(pipeline_loop_report.get("pipeline_network_path_complete")),
        "pipeline_network_path_format": pipeline_loop_report.get("pipeline_network_path_format"),
        "stream_kwargs_accepted": True,
        "stream_kwargs_keys": sorted(k for k in binding.stream_kwargs().keys()),
        "trainer_batch_iterator": stream_report.get("trainer_batch_iterator"),
        "data_loader_workers": _int_value(stream_report.get("data_loader_workers"), 0),
        "dataloader_worker_partitioning_enabled": bool(worker_partition.get("worker_sharding_enabled")),
        "worker_partition_complete": bool(worker_partition.get("complete")),
        "worker_batch_read_evidence_observed": bool(read_path_evidence.get("worker_batch_read_evidence_observed")),
        "worker_batch_read_evidence_count": _int_value(read_path_evidence.get("worker_batch_read_evidence_count"), 0),
        "remote_client_warmup_complete": bool(remote_client_warmup.get("complete")),
        "remote_client_warmup_source_count": _int_value(remote_client_warmup.get("source_count"), 0),
        "remote_client_warmup_warmed_source_count": _int_value(remote_client_warmup.get("warmed_source_count"), 0),
        "remote_client_topology_complete": bool(remote_client_topology_gate.get("complete")),
        "remote_client_per_source_complete": bool(remote_client_topology_gate.get("per_source_client_complete")),
        "remote_client_lifecycle_closed": bool(stream_report.get("remote_client_lifecycle_closed")),
        "batch_count": stream_report["batch_count"],
        "sample_count": stream_report["sample_count"],
        "byte_size": stream_report["byte_size"],
        "stream_state_format": stream_report["stream_state_format"],
        "stream_state_matches_final_cursor": stream_report["stream_state_matches_final_cursor"],
        "read_path_reliability": stream_report["read_path_reliability"],
        "worker_partition_evidence": stream_report["worker_partition_evidence"],
        "stream_report": stream_report,
    }


def _runtime_smoke_sample_ids(payload: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    stream_report = _mapping(payload.get("stream_report"))
    for batch in stream_report.get("batches", ()):
        batch_payload = _mapping(batch)
        for sample in batch_payload.get("samples", ()):
            sample_payload = _mapping(sample)
            source_name = str(sample_payload.get("source_name") or "")
            sample_index = _int_value(sample_payload.get("sample_index"), -1)
            if source_name and sample_index >= 0:
                ids.append(f"{source_name}:{sample_index}")
    return ids


def _distributed_runtime_overlap_report(rank_payloads: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    owner_by_sample: dict[str, int] = {}
    duplicates: list[dict[str, Any]] = []
    rank_sample_counts: dict[int, int] = {}
    for payload in rank_payloads:
        rank = _int_value(payload.get("rank"), -1)
        ids = _runtime_smoke_sample_ids(payload)
        rank_sample_counts[rank] = len(ids)
        for sample_id in ids:
            prior_rank = owner_by_sample.get(sample_id)
            if prior_rank is None:
                owner_by_sample[sample_id] = rank
            elif prior_rank != rank:
                duplicates.append({
                    "sample_id": sample_id,
                    "first_rank": prior_rank,
                    "duplicate_rank": rank,
                })
    return {
        "format": "cxnfs.trainer-distributed-runtime-overlap.v1",
        "rank_count": len(rank_payloads),
        "rank_sample_counts": {str(rank): count for rank, count in sorted(rank_sample_counts.items())},
        "total_observed_samples": sum(rank_sample_counts.values()),
        "unique_observed_samples": len(owner_by_sample),
        "duplicate_observed_sample_count": len(duplicates),
        "no_cross_rank_overlap": not duplicates,
        "duplicates": duplicates[:64],
    }


def _aggregate_runtime_reliability(rank_payloads: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    reports = [_mapping(payload.get("read_path_reliability")) for payload in rank_payloads]
    return {
        "format": TRAINER_READ_PATH_RELIABILITY_FORMAT,
        "complete": bool(reports) and all(bool(report.get("complete")) for report in reports),
        "rank_report_count": len(reports),
        "all_ranks_have_evidence": bool(reports)
        and all(bool(report.get("all_runs_have_evidence")) for report in reports),
        "read_retry_attempts": sum(_int_value(report.get("read_retry_attempts"), 0) for report in reports),
        "read_retry_successes": sum(_int_value(report.get("read_retry_successes"), 0) for report in reports),
        "bulk_transport_reconnects": sum(_int_value(report.get("bulk_transport_reconnects"), 0) for report in reports),
        "failed_read_operations": sum(_int_value(report.get("failed_read_operations"), 0) for report in reports),
        "recovered_read_failures": sum(_int_value(report.get("recovered_read_failures"), 0) for report in reports),
        "unrecovered_read_failures": sum(_int_value(report.get("unrecovered_read_failures"), 0) for report in reports),
        "retry_attempts_recovered": all(bool(report.get("retry_attempts_recovered")) for report in reports),
        "retry_reconnects_observed": all(bool(report.get("retry_reconnects_observed")) for report in reports),
    }


def _aggregate_distributed_runtime_topology(
    rank_payloads: Sequence[Mapping[str, Any]],
    expected_source_count: int,
    remote_required: bool,
) -> dict[str, Any]:
    topologies: list[Mapping[str, Any]] = []
    source_request_names: list[str] = []
    connection_source_names: list[str] = []
    client_open_source_names: list[str] = []
    client_close_source_names: list[str] = []
    active_client_source_names: list[str] = []
    for payload in rank_payloads:
        stream_report = _mapping(payload.get("stream_report"))
        topology = _mapping(stream_report.get("remote_client_topology_after_close"))
        if topology:
            topologies.append(topology)
            source_request_names.extend(str(name) for name in topology.get("source_request_names", ()))
            connection_source_names.extend(str(name) for name in topology.get("connection_source_names", ()))
            client_open_source_names.extend(str(name) for name in topology.get("client_open_source_names", ()))
            client_close_source_names.extend(str(name) for name in topology.get("client_close_source_names", ()))
            active_client_source_names.extend(str(name) for name in topology.get("active_client_source_names", ()))

    report_count = len(rank_payloads)
    topology_count = len(topologies)
    all_reports_have_topology = topology_count == report_count
    all_open_clients_closed = (
        all(bool(topology.get("all_open_clients_closed")) for topology in topologies)
        if topologies
        else not remote_required
    )
    unique_source_request_names = _dedupe_strings(source_request_names)
    unique_connection_source_names = _dedupe_strings(connection_source_names)
    expected = max(0, int(expected_source_count or 0))
    observed_sources_complete = (
        not remote_required
        or len(unique_source_request_names) >= expected
        or len(unique_connection_source_names) >= expected
    )
    return {
        "format": "cxnfs.trainer-distributed-runtime-topology.v1",
        "remote_client_required": bool(remote_required),
        "expected_source_count": expected,
        "rank_count": report_count,
        "topology_report_count": topology_count,
        "all_reports_have_topology": all_reports_have_topology,
        "all_open_clients_closed": all_open_clients_closed,
        "observed_sources_complete": observed_sources_complete,
        "source_request_count": sum(_int_value(topology.get("source_request_count"), 0) for topology in topologies),
        "connection_count": sum(_int_value(topology.get("connection_count"), 0) for topology in topologies),
        "client_open_count": sum(_int_value(topology.get("client_open_count"), 0) for topology in topologies),
        "client_close_count": sum(_int_value(topology.get("client_close_count"), 0) for topology in topologies),
        "active_client_count": sum(_int_value(topology.get("active_client_count"), 0) for topology in topologies),
        "unique_source_request_names": unique_source_request_names,
        "unique_connection_source_names": unique_connection_source_names,
        "unique_client_open_source_names": _dedupe_strings(client_open_source_names),
        "unique_client_close_source_names": _dedupe_strings(client_close_source_names),
        "unique_active_client_source_names": _dedupe_strings(active_client_source_names),
        "complete": (
            not remote_required
            or (all_reports_have_topology and all_open_clients_closed and observed_sources_complete)
        ),
    }


def build_trainer_distributed_runtime_smoke_payload(
    handoff_payload: Mapping[str, Any],
    *,
    world_size: int,
    batches_per_rank: int = 1,
    trainer_prefetch_batches: int | None = None,
    require_remote_quic: bool = False,
    credentials: ClientCredentials | None = None,
    credential_prompt: Callable[[], ClientCredentials] | None = None,
    client_factory_for_rank: Callable[[int], DatasetClientFactory | None] | None = None,
    close_rank_client_factories: bool = True,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    timeout: float | None = 10.0,
    tls_context: Any = None,
    server_hostname: str | None = None,
    cafile: str | None = None,
    initial_cords: int | None = None,
    max_cords: int | None = None,
    provisional_container_size: int | None = None,
    expected_size_limit: int | None = None,
    write_connections: int | None = None,
    bulk_transport: str | None = None,
    bulk_read_retries: int | None = None,
    bulk_write_retries: int | None = None,
    shared_client: bool | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> dict[str, Any]:
    if world_size <= 0:
        raise ValueError("world_size must be positive")
    if batches_per_rank <= 0:
        raise ValueError("batches_per_rank must be positive")
    plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
        handoff_payload,
        require_remote_quic=require_remote_quic,
    )
    plan_payload = plan.to_dict()
    distributed_plan = plan.distributed_plan_report(world_size)
    remote_required = bool(plan_payload.get("remote_client_required"))
    if remote_required and credentials is None and credential_prompt is None and client_factory_for_rank is None:
        raise ValueError(
            "remote trainer distributed runtime smoke requires credentials, "
            "credential_prompt, or client_factory_for_rank"
        )

    prompted_credentials: ClientCredentials | None = None
    active_credentials = credentials
    credential_prompt_called = False
    if remote_required and active_credentials is None and client_factory_for_rank is None:
        assert credential_prompt is not None
        active_credentials = credential_prompt()
        prompted_credentials = active_credentials
        credential_prompt_called = True

    rank_payloads: list[dict[str, Any]] = []
    closed_rank_factories = 0
    try:
        for rank in range(world_size):
            rank_factory = client_factory_for_rank(rank) if client_factory_for_rank is not None else None
            try:
                rank_payloads.append(
                    build_trainer_runtime_smoke_payload(
                        handoff_payload,
                        batches=batches_per_rank,
                        rank=rank,
                        world_size=world_size,
                        trainer_prefetch_batches=trainer_prefetch_batches,
                        require_remote_quic=require_remote_quic,
                        credentials=active_credentials,
                        client_factory=rank_factory,
                        host=host,
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=timeout,
                        tls_context=tls_context,
                        server_hostname=server_hostname,
                        cafile=cafile,
                        initial_cords=initial_cords,
                        max_cords=max_cords,
                        provisional_container_size=provisional_container_size,
                        expected_size_limit=expected_size_limit,
                        write_connections=write_connections,
                        bulk_transport=bulk_transport,
                        bulk_read_retries=bulk_read_retries,
                        bulk_write_retries=bulk_write_retries,
                        shared_client=shared_client,
                        control_client_factory=control_client_factory,
                        bulk_client_factory=bulk_client_factory,
                        tls_context_factory=tls_context_factory,
                    )
                )
            finally:
                if close_rank_client_factories and rank_factory is not None:
                    close = getattr(rank_factory, "close", None)
                    if callable(close):
                        close()
                        closed_rank_factories += 1
    finally:
        if prompted_credentials is not None:
            prompted_credentials.close()

    stream_reports = [_mapping(payload.get("stream_report")) for payload in rank_payloads]
    source_count = max((_int_value(report.get("source_count"), 0) for report in stream_reports), default=0)
    topology = _aggregate_distributed_runtime_topology(rank_payloads, source_count, remote_required)
    overlap = _distributed_runtime_overlap_report(rank_payloads)
    read_reliability = _aggregate_runtime_reliability(rank_payloads)
    read_path_evidence = _aggregate_read_path_evidence([
        _mapping(report.get("read_path_evidence"))
        for report in stream_reports
    ])
    ranks = [_int_value(payload.get("rank"), -1) for payload in rank_payloads]
    training_shards = [_mapping(payload.get("training_shard")) for payload in rank_payloads]
    transports = _dedupe_strings(str(report.get("transport") or "") for report in stream_reports)
    bulk_transports = _dedupe_strings(str(report.get("bulk_transport") or "") for report in stream_reports)
    expected_shards = [{"index": rank, "count": world_size} for rank in range(world_size)]
    return {
        "format": TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT,
        "passed": True,
        "handoff_format": handoff_payload.get("format"),
        "plan_format": plan_payload.get("format"),
        "distributed_plan_format": TRAINER_DISTRIBUTED_PLAN_FORMAT,
        "runtime_smoke_format": TRAINER_RUNTIME_SMOKE_FORMAT,
        "runtime_binding_format": TRAINER_RUNTIME_BINDING_FORMAT,
        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
        "training_loop_context_used": all(bool(payload.get("training_loop_context_used")) for payload in rank_payloads),
        "world_size": world_size,
        "rank_count": len(rank_payloads),
        "ranks": ranks,
        "batches_per_rank": batches_per_rank,
        "total_batch_count": sum(_int_value(payload.get("batch_count"), 0) for payload in rank_payloads),
        "total_sample_count": sum(_int_value(payload.get("sample_count"), 0) for payload in rank_payloads),
        "total_byte_size": sum(_int_value(payload.get("byte_size"), 0) for payload in rank_payloads),
        "remote_client_required": remote_required,
        "requires_credentials": remote_required,
        "credential_prompt_supported": True,
        "credential_prompt_used": credential_prompt_called
        or any(bool(payload.get("credential_prompt_used")) for payload in rank_payloads),
        "client_factory_for_rank_injected": client_factory_for_rank is not None,
        "closed_rank_client_factory_count": closed_rank_factories,
        "raw_password_sent_over_control": bool(plan_payload.get("raw_password_sent_over_control"))
        or any(bool(payload.get("raw_password_sent_over_control")) for payload in rank_payloads),
        "all_rank_streams_opened": all(bool(payload.get("stream_opened")) for payload in rank_payloads),
        "all_rank_stream_kwargs_accepted": all(bool(payload.get("stream_kwargs_accepted")) for payload in rank_payloads),
        "all_rank_state_matches_final_cursor": all(
            bool(payload.get("stream_state_matches_final_cursor"))
            for payload in rank_payloads
        ),
        "all_rank_training_loop_context_used": all(
            bool(payload.get("training_loop_context_used"))
            and payload.get("training_loop_context") == "CxnfsTrainerRuntimeBinding.open_training_loop"
            for payload in rank_payloads
        ),
        "all_rank_training_shards_correct": training_shards == expected_shards,
        "no_cross_rank_overlap": bool(overlap.get("no_cross_rank_overlap")),
        "all_open_clients_closed": bool(topology.get("all_open_clients_closed")),
        "distributed_runtime_topology_complete": bool(topology.get("complete")),
        "observed_sources_complete": bool(topology.get("observed_sources_complete")),
        "transport": transports[0] if len(transports) == 1 else None,
        "bulk_transport": bulk_transports[0] if len(bulk_transports) == 1 else None,
        "transports": transports,
        "bulk_transports": bulk_transports,
        "read_path_evidence": read_path_evidence,
        "read_path_reliability": read_reliability,
        "rank_sample_overlap": overlap,
        "remote_client_topology_after_close": topology,
        "distributed_plan": distributed_plan,
        "rank_smokes": rank_payloads,
    }


def write_trainer_distributed_runtime_rank_states(
    payload: MutableMapping[str, Any],
    directory: str | Path,
    *,
    compact: bool = False,
) -> dict[str, Any]:
    target = Path(directory)
    artifacts: list[dict[str, Any]] = []
    for rank_payload in payload.get("rank_smokes", ()):
        rank_report = _mapping(rank_payload)
        rank = _int_value(rank_report.get("rank"), -1)
        if rank < 0:
            continue
        stream_report = _mapping(rank_report.get("stream_report"))
        state = _mapping(stream_report.get("stream_state"))
        if not state:
            continue
        state_path = target / f"rank-{rank:05d}.trainer-stream-state.json"
        _write_json_mapping_atomic(state_path, state, compact=compact)
        artifacts.append({
            "rank": rank,
            "path": str(state_path),
            "format": state.get("format"),
            "training_shard": rank_report.get("training_shard"),
            "state_matches_final_cursor": bool(rank_report.get("stream_state_matches_final_cursor")),
        })
    rank_count = _int_value(payload.get("rank_count"), 0)
    report = {
        "format": "cxnfs.trainer-distributed-runtime-rank-states.v1",
        "directory": str(target),
        "rank_count": rank_count,
        "rank_state_file_count": len(artifacts),
        "all_rank_state_files_written": len(artifacts) == rank_count,
        "artifacts": artifacts,
    }
    payload["rank_state_artifacts"] = report
    payload["rank_state_file_count"] = report["rank_state_file_count"]
    payload["all_rank_state_files_written"] = report["all_rank_state_files_written"]
    return report


def _trainer_bundle_report_artifact(
    name: str,
    payload: Mapping[str, Any],
    path: Path | None,
) -> dict[str, Any]:
    return {
        "name": name,
        "format": payload.get("format"),
        "passed": bool(payload.get("passed", True)),
        "path": None if path is None else str(path),
    }


def _write_trainer_bundle_report(
    output_dir: Path | None,
    name: str,
    payload: Mapping[str, Any],
    *,
    compact: bool,
) -> dict[str, Any]:
    path = None if output_dir is None else output_dir / name
    if path is not None:
        _write_json_mapping_atomic(path, payload, compact=compact)
    return _trainer_bundle_report_artifact(name, payload, path)


def _trainer_bundle_remote_client_factory(
    plan: CxnfsTrainerIntegrationPlan,
    credentials: ClientCredentials,
    *,
    host: str,
    control_port: int,
    transport_port: int,
    timeout: float | None,
    tls_context: Any,
    server_hostname: str | None,
    cafile: str | None,
    initial_cords: int | None,
    max_cords: int | None,
    provisional_container_size: int | None,
    expected_size_limit: int | None,
    write_connections: int | None,
    bulk_transport: str | None,
    bulk_read_retries: int | None,
    bulk_write_retries: int | None,
    shared_client: bool | None,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
    tls_context_factory: Callable[[str | None], Any],
) -> DatasetClientFactory:
    return plan.create_remote_client_factory(
        credentials,
        host=host,
        control_port=control_port,
        transport_port=transport_port,
        timeout=timeout,
        tls_context=tls_context,
        server_hostname=server_hostname,
        cafile=cafile,
        initial_cords=initial_cords,
        max_cords=max_cords,
        provisional_container_size=provisional_container_size,
        expected_size_limit=expected_size_limit,
        write_connections=write_connections,
        bulk_transport=bulk_transport,
        bulk_read_retries=bulk_read_retries,
        bulk_write_retries=bulk_write_retries,
        shared_client=shared_client,
        control_client_factory=control_client_factory,
        bulk_client_factory=bulk_client_factory,
        tls_context_factory=tls_context_factory,
    )


def build_trainer_readiness_bundle_payload(
    handoff_payload: Mapping[str, Any],
    *,
    output_dir: str | Path | None = None,
    integration_batches: int = 1,
    runtime_batches: int = 1,
    runtime_rank: int = 0,
    world_size: int = 2,
    batches_per_rank: int = 1,
    loop_steps: int = 2,
    gradient_accumulation_steps: int = 1,
    resume_after_steps: int | None = None,
    trainer_prefetch_batches: int | None = None,
    require_remote_quic: bool = False,
    allow_partial_source_coverage: bool = False,
    credentials: ClientCredentials | None = None,
    credential_prompt: Callable[[], ClientCredentials] | None = None,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    timeout: float | None = 10.0,
    tls_context: Any = None,
    server_hostname: str | None = None,
    cafile: str | None = None,
    initial_cords: int | None = None,
    max_cords: int | None = None,
    provisional_container_size: int | None = None,
    expected_size_limit: int | None = None,
    write_connections: int | None = None,
    bulk_transport: str | None = None,
    bulk_read_retries: int | None = None,
    bulk_write_retries: int | None = None,
    shared_client: bool | None = None,
    compact: bool = False,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> dict[str, Any]:
    if integration_batches <= 0:
        raise ValueError("integration_batches must be positive")
    if runtime_batches <= 0:
        raise ValueError("runtime_batches must be positive")
    if runtime_rank < 0:
        raise ValueError("runtime_rank must be non-negative")
    if world_size <= 0:
        raise ValueError("world_size must be positive")
    if runtime_rank >= world_size:
        raise ValueError("runtime_rank must be inside world_size")
    if batches_per_rank <= 0:
        raise ValueError("batches_per_rank must be positive")
    if loop_steps <= 0:
        raise ValueError("loop_steps must be positive")
    if gradient_accumulation_steps <= 0:
        raise ValueError("gradient_accumulation_steps must be positive")

    plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
        handoff_payload,
        require_remote_quic=require_remote_quic,
    )
    preflight = plan.preflight_report()
    remote_required = bool(preflight.get("remote_client_required"))
    if remote_required and credentials is None and credential_prompt is None:
        raise ValueError("remote trainer readiness bundle requires credentials or credential_prompt")

    target_dir = Path(output_dir) if output_dir is not None else None
    prompted_credentials: ClientCredentials | None = None
    active_credentials = credentials
    credential_prompt_used = False
    if remote_required and active_credentials is None:
        assert credential_prompt is not None
        active_credentials = credential_prompt()
        prompted_credentials = active_credentials
        credential_prompt_used = True

    artifacts: list[dict[str, Any]] = []
    try:
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-preflight.json",
                preflight,
                compact=compact,
            )
        )

        integration_client_factory: DatasetClientFactory | None = None
        try:
            if remote_required:
                assert active_credentials is not None
                integration_client_factory = _trainer_bundle_remote_client_factory(
                    plan,
                    active_credentials,
                    host=host,
                    control_port=control_port,
                    transport_port=transport_port,
                    timeout=timeout,
                    tls_context=tls_context,
                    server_hostname=server_hostname,
                    cafile=cafile,
                    initial_cords=initial_cords,
                    max_cords=max_cords,
                    provisional_container_size=provisional_container_size,
                    expected_size_limit=expected_size_limit,
                    write_connections=write_connections,
                    bulk_transport=bulk_transport,
                    bulk_read_retries=bulk_read_retries,
                    bulk_write_retries=bulk_write_retries,
                    shared_client=shared_client,
                    control_client_factory=control_client_factory,
                    bulk_client_factory=bulk_client_factory,
                    tls_context_factory=tls_context_factory,
                )
            integration_smoke = build_trainer_integration_smoke_payload(
                handoff_payload,
                batches=integration_batches,
                trainer_prefetch_batches=trainer_prefetch_batches,
                require_remote_quic=require_remote_quic,
                client_factory=integration_client_factory,
            )
        finally:
            if integration_client_factory is not None:
                close = getattr(integration_client_factory, "close", None)
                if callable(close):
                    close()
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-integration-smoke.json",
                integration_smoke,
                compact=compact,
            )
        )

        runtime_smoke = build_trainer_runtime_smoke_payload(
            handoff_payload,
            batches=runtime_batches,
            rank=runtime_rank,
            world_size=world_size,
            trainer_prefetch_batches=trainer_prefetch_batches,
            require_remote_quic=require_remote_quic,
            credentials=active_credentials,
            host=host,
            control_port=control_port,
            transport_port=transport_port,
            timeout=timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
            cafile=cafile,
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size=provisional_container_size,
            expected_size_limit=expected_size_limit,
            write_connections=write_connections,
            bulk_transport=bulk_transport,
            bulk_read_retries=bulk_read_retries,
            bulk_write_retries=bulk_write_retries,
            shared_client=shared_client,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-runtime-smoke.json",
                runtime_smoke,
                compact=compact,
            )
        )

        distributed_runtime_smoke = build_trainer_distributed_runtime_smoke_payload(
            handoff_payload,
            world_size=world_size,
            batches_per_rank=batches_per_rank,
            trainer_prefetch_batches=trainer_prefetch_batches,
            require_remote_quic=require_remote_quic,
            credentials=active_credentials,
            host=host,
            control_port=control_port,
            transport_port=transport_port,
            timeout=timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
            cafile=cafile,
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size=provisional_container_size,
            expected_size_limit=expected_size_limit,
            write_connections=write_connections,
            bulk_transport=bulk_transport,
            bulk_read_retries=bulk_read_retries,
            bulk_write_retries=bulk_write_retries,
            shared_client=shared_client,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        rank_state_artifacts = None
        if target_dir is not None:
            rank_state_artifacts = write_trainer_distributed_runtime_rank_states(
                distributed_runtime_smoke,
                target_dir / "rank-states",
                compact=compact,
            )
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-distributed-runtime-smoke.json",
                distributed_runtime_smoke,
                compact=compact,
            )
        )

        loop_client_factory: DatasetClientFactory | None = None
        try:
            if remote_required:
                assert active_credentials is not None
                loop_client_factory = _trainer_bundle_remote_client_factory(
                    plan,
                    active_credentials,
                    host=host,
                    control_port=control_port,
                    transport_port=transport_port,
                    timeout=timeout,
                    tls_context=tls_context,
                    server_hostname=server_hostname,
                    cafile=cafile,
                    initial_cords=initial_cords,
                    max_cords=max_cords,
                    provisional_container_size=provisional_container_size,
                    expected_size_limit=expected_size_limit,
                    write_connections=write_connections,
                    bulk_transport=bulk_transport,
                    bulk_read_retries=bulk_read_retries,
                    bulk_write_retries=bulk_write_retries,
                    shared_client=shared_client,
                    control_client_factory=control_client_factory,
                    bulk_client_factory=bulk_client_factory,
                    tls_context_factory=tls_context_factory,
                )
            loop_replay = build_trainer_loop_replay_payload(
                handoff_payload,
                steps=loop_steps,
                gradient_accumulation_steps=gradient_accumulation_steps,
                trainer_prefetch_batches=trainer_prefetch_batches,
                replay_resume=True,
                resume_after_steps=resume_after_steps,
                require_remote_quic=require_remote_quic,
                require_source_coverage=not allow_partial_source_coverage,
                client_factory=loop_client_factory,
            )
        finally:
            if loop_client_factory is not None:
                close = getattr(loop_client_factory, "close", None)
                if callable(close):
                    close()
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-loop-replay.json",
                loop_replay,
                compact=compact,
            )
        )
    finally:
        if prompted_credentials is not None:
            prompted_credentials.close()

    reports = {
        "preflight": preflight,
        "integration_smoke": integration_smoke,
        "runtime_smoke": runtime_smoke,
        "distributed_runtime_smoke": distributed_runtime_smoke,
        "loop_replay": loop_replay,
    }
    all_reports_passed = all(bool(report.get("passed", True)) for report in reports.values())
    raw_password_sent = any(bool(report.get("raw_password_sent_over_control")) for report in reports.values())
    low_level_direct_read_proofs_complete = bool(preflight.get("low_level_direct_read_proofs_complete"))
    batch_direct_read_proof_complete = bool(preflight.get("batch_direct_read_proof_complete"))
    batch_direct_tensor_read_proof_complete = bool(preflight.get("batch_direct_tensor_read_proof_complete"))
    low_level_direct_write_proofs_complete = bool(preflight.get("low_level_direct_write_proofs_complete"))
    write_probe_proof_complete = bool(preflight.get("write_probe_proof_complete"))
    direct_quic_memory_path_verified = (
        bool(preflight.get("ready_for_remote_quic"))
        and low_level_direct_read_proofs_complete
        and batch_direct_read_proof_complete
        and batch_direct_tensor_read_proof_complete
        and low_level_direct_write_proofs_complete
        and write_probe_proof_complete
        and preflight.get("batch_direct_read_cipher_suite") == CXNFS_TLS_CIPHER_SUITE
        and preflight.get("batch_direct_tensor_read_cipher_suite") == CXNFS_TLS_CIPHER_SUITE
        and preflight.get("write_probe_cipher_suite") == CXNFS_TLS_CIPHER_SUITE
    )
    rank_states_written = (
        target_dir is None
        or bool(_mapping(distributed_runtime_smoke.get("rank_state_artifacts")).get("all_rank_state_files_written"))
    )
    pipeline_training_loop_required = bool(runtime_smoke.get("pipeline_training_loop_required"))
    pipeline_training_loop_verified = (
        not pipeline_training_loop_required
        or bool(runtime_smoke.get("pipeline_training_loop_verified"))
    )
    pipeline_network_path_verified = (
        not pipeline_training_loop_required
        or bool(runtime_smoke.get("pipeline_network_path_verified"))
    )
    passed = (
        all_reports_passed
        and bool(preflight.get("ready_for_trainer_wiring"))
        and not bool(preflight.get("requires_mount"))
        and not bool(preflight.get("touches_trainer_code"))
        and not raw_password_sent
        and bool(integration_smoke.get("stream_kwargs_accepted"))
        and bool(runtime_smoke.get("training_loop_context_used"))
        and bool(distributed_runtime_smoke.get("all_rank_training_loop_context_used"))
        and bool(distributed_runtime_smoke.get("no_cross_rank_overlap"))
        and bool(distributed_runtime_smoke.get("all_open_clients_closed"))
        and bool(loop_replay.get("resume_continuity_verified"))
        and pipeline_training_loop_verified
        and pipeline_network_path_verified
        and rank_states_written
    )
    if require_remote_quic:
        passed = passed and direct_quic_memory_path_verified

    bundle = {
        "format": TRAINER_READINESS_BUNDLE_FORMAT,
        "passed": passed,
        "handoff_format": handoff_payload.get("format"),
        "preflight_format": TRAINER_INTEGRATION_PREFLIGHT_FORMAT,
        "integration_smoke_format": TRAINER_INTEGRATION_SMOKE_FORMAT,
        "runtime_smoke_format": TRAINER_RUNTIME_SMOKE_FORMAT,
        "distributed_runtime_smoke_format": TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT,
        "loop_replay_format": TRAINER_LOOP_REPLAY_FORMAT,
        "ready_for_trainer_wiring": bool(preflight.get("ready_for_trainer_wiring")),
        "ready_for_remote_quic": bool(preflight.get("ready_for_remote_quic")),
        "ready_for_blockfs_candidate": bool(preflight.get("ready_for_blockfs_candidate")),
        "requires_low_level_direct_read_proofs": bool(require_remote_quic),
        "low_level_direct_read_proofs_complete": low_level_direct_read_proofs_complete,
        "low_level_direct_read_proof_missing": list(preflight.get("low_level_direct_read_proof_missing", ())),
        "batch_direct_read_proof_complete": batch_direct_read_proof_complete,
        "batch_direct_tensor_read_proof_complete": batch_direct_tensor_read_proof_complete,
        "batch_direct_read_cipher_suite": preflight.get("batch_direct_read_cipher_suite"),
        "batch_direct_tensor_read_cipher_suite": preflight.get("batch_direct_tensor_read_cipher_suite"),
        "requires_low_level_direct_write_proofs": bool(require_remote_quic),
        "low_level_direct_write_proofs_complete": low_level_direct_write_proofs_complete,
        "low_level_direct_write_proof_missing": list(preflight.get("low_level_direct_write_proof_missing", ())),
        "write_probe_proof_complete": write_probe_proof_complete,
        "write_probe_cipher_suite": preflight.get("write_probe_cipher_suite"),
        "write_probe_quic_transport_complete": bool(preflight.get("write_probe_quic_transport_complete")),
        "write_probe_max_observed_write_cords": _int_value(
            preflight.get("write_probe_max_observed_write_cords"), 0
        ),
        "direct_quic_memory_path_verified": direct_quic_memory_path_verified,
        "requires_mount": bool(preflight.get("requires_mount")),
        "touches_trainer_code": bool(preflight.get("touches_trainer_code")),
        "remote_client_required": remote_required,
        "requires_credentials": remote_required,
        "credential_prompt_supported": True,
        "credential_prompt_used": credential_prompt_used,
        "raw_password_sent_over_control": raw_password_sent,
        "direct_python_streaming_verified": all_reports_passed
        and bool(integration_smoke.get("stream_opened"))
        and bool(runtime_smoke.get("stream_opened"))
        and bool(loop_replay.get("training_loop_context_used")),
        "runtime_training_loop_verified": bool(runtime_smoke.get("training_loop_context_used")),
        "pipeline_training_loop_required": pipeline_training_loop_required,
        "pipeline_training_loop_verified": pipeline_training_loop_verified,
        "pipeline_network_path_verified": pipeline_network_path_verified,
        "pipeline_network_path_format": runtime_smoke.get("pipeline_network_path_format"),
        "distributed_runtime_verified": bool(distributed_runtime_smoke.get("all_rank_training_loop_context_used")),
        "distributed_rank_states_written": rank_states_written,
        "loop_resume_verified": bool(loop_replay.get("resume_continuity_verified")),
        "world_size": world_size,
        "runtime_rank": runtime_rank,
        "batches": {
            "integration_batches": integration_batches,
            "runtime_batches": runtime_batches,
            "batches_per_rank": batches_per_rank,
            "loop_steps": loop_steps,
            "gradient_accumulation_steps": gradient_accumulation_steps,
        },
        "report_artifacts": {
            "format": "cxnfs.trainer-readiness-bundle-artifacts.v1",
            "directory": None if target_dir is None else str(target_dir),
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
        },
        "rank_state_artifacts": rank_state_artifacts,
        "reports": reports,
    }
    return bundle


def run_trainer_readiness_bundle_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Run a CXNFS trainer handoff through the trainer-host readiness bundle.")
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--output-dir", help="Optional directory to write component reports and rank stream states.")
    parser.add_argument("--output", help="Optional path to write the aggregate readiness bundle JSON.")
    parser.add_argument("--integration-batches", type=int, default=1, help="Batches consumed by the integration smoke.")
    parser.add_argument("--runtime-batches", type=int, default=1, help="Batches consumed by the rank runtime smoke.")
    parser.add_argument("--runtime-rank", type=int, default=0, help="Rank used by the single-rank runtime smoke.")
    parser.add_argument("--world-size", type=int, default=2, help="Distributed world size to probe.")
    parser.add_argument("--batches-per-rank", type=int, default=1, help="Batches consumed by each distributed rank.")
    parser.add_argument("--loop-steps", type=int, default=2, help="Trainer-shaped replay steps to run.")
    parser.add_argument("--gradient-accumulation-steps", type=int, default=1, help="Microbatches per replayed step.")
    parser.add_argument("--resume-after-steps", type=int, help="Replay resume boundary for the trainer loop.")
    parser.add_argument("--trainer-prefetch-batches", type=int, help="Override trainer-loop prefetch batches.")
    parser.add_argument("--allow-partial-source-coverage", action="store_true", help="Do not require the replay to touch every weighted source.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--host", default="127.0.0.1", help="CXNFS server host for remote trainer bundle probes.")
    parser.add_argument("--control-port", type=int, default=4509, help="CXNFS authenticated control TCP port.")
    parser.add_argument("--transport-port", type=int, default=4510, help="CXNFS bulk transport port.")
    parser.add_argument("--bulk-transport", choices=(PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC), help="Override the remote bulk transport from the handoff.")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, help="Initial cord count for remote bulk reads.")
    parser.add_argument("--max-cords", type=int, help="Maximum cord count for remote bulk reads.")
    parser.add_argument("--provisional-container-size", type=int, help="Provisional remote container size in bytes.")
    parser.add_argument("--expected-size-limit", type=int, help="Expected remote read size limit in bytes.")
    parser.add_argument("--write-connections", type=int, help="Length-prefixed write connection count.")
    parser.add_argument("--bulk-read-retries", type=int, help="Remote bulk read retry count.")
    parser.add_argument("--bulk-write-retries", type=int, help="Remote bulk write retry count.")
    parser.add_argument("--shared-client", action="store_true", help="Reuse one remote client across weighted sources.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        handoff = _read_json_mapping(Path(args.handoff), "trainer handoff")
        payload = build_trainer_readiness_bundle_payload(
            handoff,
            output_dir=args.output_dir,
            integration_batches=args.integration_batches,
            runtime_batches=args.runtime_batches,
            runtime_rank=args.runtime_rank,
            world_size=args.world_size,
            batches_per_rank=args.batches_per_rank,
            loop_steps=args.loop_steps,
            gradient_accumulation_steps=args.gradient_accumulation_steps,
            resume_after_steps=args.resume_after_steps,
            trainer_prefetch_batches=args.trainer_prefetch_batches,
            require_remote_quic=args.require_remote_quic,
            allow_partial_source_coverage=args.allow_partial_source_coverage,
            credential_prompt=credential_prompt,
            host=args.host,
            control_port=args.control_port,
            transport_port=args.transport_port,
            timeout=args.timeout,
            server_hostname=args.server_hostname or args.host,
            cafile=args.cafile,
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size=args.provisional_container_size,
            expected_size_limit=args.expected_size_limit,
            write_connections=args.write_connections,
            bulk_transport=args.bulk_transport,
            bulk_read_retries=args.bulk_read_retries,
            bulk_write_retries=args.bulk_write_retries,
            shared_client=True if args.shared_client else None,
            compact=args.compact,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        indent = None if args.compact else 2
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_readiness_bundle() -> None:
    raise SystemExit(run_trainer_readiness_bundle_cli())


def _trainer_handoff_acceptance_public_python_surface() -> dict[str, str]:
    return {
        "handoff_loader": "CxnfsTrainerIntegrationPlan.from_file",
        "runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file",
        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
        "stream_factory": "open_weighted_mds_trainer_stream",
        "stream_object": "CxnfsTrainerStream",
        "batch_object": "CxnfsTrainerBatch",
        "remote_client_factory": "CxnfsTrainerIntegrationPlan.create_remote_client_factory",
        "credential_prompt": "prompt_client_credentials",
    }


def _trainer_loop_contract_payload(
    *,
    handoff_path: str | Path | None,
    output_dir: Path | None,
    preflight: Mapping[str, Any],
    readiness: Mapping[str, Any],
    runtime_rank: int,
    world_size: int,
    trainer_prefetch_batches: int | None = None,
) -> dict[str, Any]:
    rank_state_artifacts = _mapping(readiness.get("rank_state_artifacts"))
    readiness_reports = _mapping(readiness.get("reports"))
    runtime_smoke = _mapping(readiness_reports.get("runtime_smoke"))
    pipeline_loop_report = _mapping(runtime_smoke.get("pipeline_training_loop_report"))
    pipeline_network_path = _mapping(pipeline_loop_report.get("pipeline_network_path"))
    loop_replay = _mapping(readiness_reports.get("loop_replay"))
    artifact_paths = {
        "handoff": str(output_dir / "trainer-handoff.json") if output_dir is not None else (
            None if handoff_path is None else str(handoff_path)
        ),
        "readiness_bundle": str(output_dir / "trainer-readiness-bundle.json") if output_dir is not None else None,
        "preflight": str(output_dir / "trainer-preflight.json") if output_dir is not None else None,
        "runtime_smoke": str(output_dir / "trainer-runtime-smoke.json") if output_dir is not None else None,
        "distributed_runtime_smoke": (
            str(output_dir / "trainer-distributed-runtime-smoke.json") if output_dir is not None else None
        ),
        "loop_replay": str(output_dir / "trainer-loop-replay.json") if output_dir is not None else None,
        "rank_states_dir": str(output_dir / "rank-states") if output_dir is not None else None,
    }
    active_prefetch = _int_value(
        trainer_prefetch_batches
        if trainer_prefetch_batches is not None
        else preflight.get("trainer_prefetch_batches"),
        0,
    )
    stream_kwargs = _mapping(preflight.get("stream_kwargs"))
    tensor_allocation = _mapping(stream_kwargs.get("tensor_allocation"))
    tensor_memory_path = _mapping(tensor_allocation.get("memory_path"))
    direct_tensor_mode_selected = preflight.get("read_mode") == TrainerReadMode.DIRECT_TENSORS.value
    pipeline_direct_tensor_supported = bool(preflight.get("supports_pipeline_direct_tensor_batches"))
    remote_required = bool(preflight.get("remote_client_required"))
    requires_credentials = bool(preflight.get("requires_credentials"))
    return {
        "format": TRAINER_LOOP_CONTRACT_FORMAT,
        "passed": bool(readiness.get("passed")),
        "runtime_binding_format": TRAINER_RUNTIME_BINDING_FORMAT,
        "stream_state_format": TRAINER_STREAM_STATE_FORMAT,
        "artifact_paths": artifact_paths,
        "entrypoint": {
            "import_from": "cxnfs",
            "runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file",
            "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
            "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
            "stream_object": "CxnfsTrainerStream",
            "prefetch_stream_object": "CxnfsTrainerPrefetchStream",
            "pipeline_loop_object": "CxnfsTrainerPipelineLoop",
            "batch_object": "CxnfsTrainerBatch",
            "credential_prompt": "prompt_client_credentials",
        },
        "minimal_python": {
            "format": "cxnfs.trainer-loop-contract-python.v1",
            "lines": [
                "from cxnfs import CxnfsTrainerRuntimeBinding, prompt_client_credentials",
                "binding = CxnfsTrainerRuntimeBinding.from_file(handoff_path, rank=rank, world_size=world_size)",
                "with binding.open_training_loop(credential_prompt=prompt_client_credentials, **network_options) as stream:",
                "    for batch in stream:",
                "        train_step(batch)",
            ],
        },
        "authentication": {
            "remote_client_required": remote_required,
            "requires_credentials": requires_credentials,
            "credential_prompt_supported": True,
            "credential_prompt_required_for_remote": remote_required and requires_credentials,
            "password_cli_argument": False,
            "raw_password_sent_over_control": bool(preflight.get("raw_password_sent_over_control"))
            or bool(readiness.get("raw_password_sent_over_control")),
            "credentials_may_be_supplied_by_caller": True,
        },
        "mount_free_contract": {
            "requires_mount": bool(preflight.get("requires_mount")),
            "touches_trainer_code": bool(preflight.get("touches_trainer_code")),
            "opens_network_connections_during_preflight": bool(preflight.get("opens_network_connections")),
            "consumes_batches_during_preflight": bool(preflight.get("consumes_batches")),
            "safe_to_run_before_trainer_workers": bool(preflight.get("safe_to_run_before_trainer_workers")),
        },
        "stream_contract": {
            "stream_factory": preflight.get("stream_factory"),
            "read_mode": preflight.get("read_mode"),
            "trainer_batch_iterator": preflight.get("trainer_batch_iterator"),
            "data_loader_workers": _int_value(preflight.get("data_loader_workers"), 0),
            "trainer_prefetch_batches": active_prefetch,
            "supports_training_loop_prefetch": bool(preflight.get("supports_training_loop_prefetch")),
            "supports_resume_cursor": bool(preflight.get("supports_resume_cursor")),
            "supports_resume_state_file": bool(preflight.get("supports_resume_state_file")),
            "direct_python_streaming_verified": bool(readiness.get("direct_python_streaming_verified")),
            "runtime_training_loop_verified": bool(readiness.get("runtime_training_loop_verified")),
        },
        "pipeline_contract": {
            "training_pipeline_object": preflight.get("training_pipeline_object") or "CxnfsTrainingPipeline",
            "training_pipeline_context": preflight.get("training_pipeline_context")
            or "CxnfsTrainingPipeline.read_tensor_batch",
            "pipeline_stream_context": "CxnfsTrainingPipeline.prefetch_trainer_stream",
            "pipeline_batch_iterator": "CxnfsTrainerPipelineStream.next_batch",
            "pipeline_descriptor_iterator": (
                "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors"
            ),
            "pipeline_descriptor_stream": "CxnfsTrainerPipelineStream",
            "pipeline_loop_object": "CxnfsTrainerPipelineLoop",
            "pipeline_stream_state_format": TRAINER_STREAM_STATE_FORMAT,
            "pipeline_stream_save_method": "CxnfsTrainerPipelineStream.save_state_file",
            "pipeline_stream_load_method": "CxnfsTrainerPipelineStream.load_state_file",
            "pipeline_stream_validates_resume_contract": True,
            "pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch",
            "supports_pipeline_direct_tensor_batches": pipeline_direct_tensor_supported,
            "supports_pipeline_gpu_transforms": bool(preflight.get("supports_pipeline_gpu_transforms")),
            "supports_pipeline_torch_compile": bool(preflight.get("supports_pipeline_torch_compile")),
            "pipeline_cpu_augmentation_default": (
                preflight.get("pipeline_cpu_augmentation_default")
                or "disabled-with-explicit-opt-in"
            ),
            "direct_tensor_read_mode": TrainerReadMode.DIRECT_TENSORS.value,
            "direct_tensor_read_mode_selected": direct_tensor_mode_selected,
            "selected_read_mode": preflight.get("read_mode"),
            "python_byte_batch_avoidance_required_for_direct_tensors": True,
            "python_byte_batch_avoidance_supported": pipeline_direct_tensor_supported,
            "network_fills_tensor_destination_supported": bool(
                tensor_memory_path.get("network_fills_tensor_destination")
            ),
            "pinned_host_staging_supported": bool(tensor_memory_path.get("requires_pinned_host_staging")),
            "tensor_allocation": dict(tensor_allocation) if direct_tensor_mode_selected else None,
            "tensor_memory_path": dict(tensor_memory_path) if direct_tensor_mode_selected else None,
            "pipeline_network_path": dict(pipeline_network_path) if pipeline_network_path else None,
            "pipeline_network_path_verified": bool(readiness.get("pipeline_network_path_verified")),
            "trainer_prefetch_batches": active_prefetch,
            "trainer_prefetch_enabled": active_prefetch > 0,
            "direct_tensor_transport_proof_complete": bool(
                preflight.get("batch_direct_tensor_read_proof_complete")
            ),
            "direct_tensor_transport_cipher_suite": preflight.get("batch_direct_tensor_read_cipher_suite"),
        },
        "batch_contract": {
            "batch_object": "CxnfsTrainerBatch",
            "fields": [
                "batch_index",
                "epoch",
                "samples",
                "sample_count",
                "byte_size",
                "source_counts",
                "resume_cursor",
            ],
            "sample_fields": [
                "source_name",
                "source_index",
                "sample_index",
                "data",
                "byte_size",
                "path",
            ],
            "data_memory": {
                "direct_buffers": "sample.data is a memoryview over caller-owned destination bytes",
                "direct_tensors": "sample.data is backed by caller-owned tensor storage when tensor reads are selected",
                "samples": "sample.data carries decoded sample data for diagnostics",
            },
        },
        "step_trace_contract": {
            "report_format": "cxnfs.trainer-step-trace.v1",
            "emitted_by": "cxnfs-trainer-loop-replay",
            "fields": [
                "step_index",
                "phase",
                "microbatch_count",
                "sample_count",
                "byte_size",
                "source_counts",
                "sample_identities",
                "resume_start_emitted",
                "resume_final_emitted",
                "resume_delta",
                "cursor_trace_source",
                "cursor_delta_verified",
            ],
            "sample_identity_format": "source_name:source_index:sample_index",
            "requires_cursor_delta_verified": True,
            "requires_resume_split_continuity": True,
            "requires_unique_sample_identities_by_default": True,
            "duplicate_sample_identity_override": "--allow-duplicate-sample-identities",
            "source_coverage_required_by_default": True,
        },
        "distributed_contract": {
            "rank": runtime_rank,
            "world_size": world_size,
            "rank_source": "explicit rank/world_size arguments or process RANK/WORLD_SIZE",
            "cross_rank_overlap_free": bool(_mapping(readiness_reports.get("distributed_runtime_smoke")).get("no_cross_rank_overlap")),
            "rank_states_written": bool(readiness.get("distributed_rank_states_written")),
            "rank_state_artifacts": dict(rank_state_artifacts),
        },
        "resume_contract": {
            "cursor_format": TRAINER_CURSOR_FORMAT,
            "state_format": TRAINER_STREAM_STATE_FORMAT,
            "save_method": "CxnfsTrainerStream.save_state_file",
            "resume_argument": "resume_state_path",
            "loop_resume_verified": bool(readiness.get("loop_resume_verified")),
            "final_emitted": _int_value(loop_replay.get("final_emitted"), 0),
            "sample_count": _int_value(loop_replay.get("sample_count"), 0),
            "byte_size": _int_value(loop_replay.get("byte_size"), 0),
        },
        "remote_transport_contract": {
            "bulk_transport": preflight.get("bulk_transport"),
            "initial_cords": preflight.get("initial_cords"),
            "max_cords": preflight.get("max_cords"),
            "bulk_read_retries": preflight.get("bulk_read_retries"),
            "bulk_write_retries": preflight.get("bulk_write_retries"),
            "per_source_client_default": bool(preflight.get("per_source_client_default", True)),
            "ready_for_remote_quic": bool(preflight.get("ready_for_remote_quic")),
            "direct_quic_memory_path_verified": bool(readiness.get("direct_quic_memory_path_verified")),
        },
    }


def build_trainer_handoff_acceptance_payload(
    handoff_payload: Mapping[str, Any],
    *,
    handoff_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    integration_batches: int = 1,
    runtime_batches: int = 1,
    runtime_rank: int = 0,
    world_size: int = 2,
    batches_per_rank: int = 1,
    loop_steps: int = 2,
    gradient_accumulation_steps: int = 1,
    resume_after_steps: int | None = None,
    trainer_prefetch_batches: int | None = None,
    require_remote_quic: bool = False,
    allow_partial_source_coverage: bool = False,
    credentials: ClientCredentials | None = None,
    credential_prompt: Callable[[], ClientCredentials] | None = None,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    timeout: float | None = 10.0,
    tls_context: Any = None,
    server_hostname: str | None = None,
    cafile: str | None = None,
    initial_cords: int | None = None,
    max_cords: int | None = None,
    provisional_container_size: int | None = None,
    expected_size_limit: int | None = None,
    write_connections: int | None = None,
    bulk_transport: str | None = None,
    bulk_read_retries: int | None = None,
    bulk_write_retries: int | None = None,
    shared_client: bool | None = None,
    compact: bool = False,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> dict[str, Any]:
    target_dir = Path(output_dir) if output_dir is not None else None
    artifacts: list[dict[str, Any]] = []
    if target_dir is not None:
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-handoff.json",
                handoff_payload,
                compact=compact,
            )
        )
    else:
        artifacts.append(
            _trainer_bundle_report_artifact(
                "trainer-handoff.json",
                handoff_payload,
                Path(handoff_path) if handoff_path is not None else None,
            )
        )

    readiness = build_trainer_readiness_bundle_payload(
        handoff_payload,
        output_dir=target_dir,
        integration_batches=integration_batches,
        runtime_batches=runtime_batches,
        runtime_rank=runtime_rank,
        world_size=world_size,
        batches_per_rank=batches_per_rank,
        loop_steps=loop_steps,
        gradient_accumulation_steps=gradient_accumulation_steps,
        resume_after_steps=resume_after_steps,
        trainer_prefetch_batches=trainer_prefetch_batches,
        require_remote_quic=require_remote_quic,
        allow_partial_source_coverage=allow_partial_source_coverage,
        credentials=credentials,
        credential_prompt=credential_prompt,
        host=host,
        control_port=control_port,
        transport_port=transport_port,
        timeout=timeout,
        tls_context=tls_context,
        server_hostname=server_hostname,
        cafile=cafile,
        initial_cords=initial_cords,
        max_cords=max_cords,
        provisional_container_size=provisional_container_size,
        expected_size_limit=expected_size_limit,
        write_connections=write_connections,
        bulk_transport=bulk_transport,
        bulk_read_retries=bulk_read_retries,
        bulk_write_retries=bulk_write_retries,
        shared_client=shared_client,
        compact=compact,
        control_client_factory=control_client_factory,
        bulk_client_factory=bulk_client_factory,
        tls_context_factory=tls_context_factory,
    )
    artifacts.append(
        _write_trainer_bundle_report(
            target_dir,
            "trainer-readiness-bundle.json",
            readiness,
            compact=compact,
        )
    )
    for artifact in _mapping(readiness.get("report_artifacts")).get("artifacts", ()):
        artifacts.append(dict(_mapping(artifact)))

    reports = _mapping(readiness.get("reports"))
    preflight = _mapping(reports.get("preflight"))
    runtime = _mapping(reports.get("runtime_smoke"))
    distributed = _mapping(reports.get("distributed_runtime_smoke"))
    loop_replay = _mapping(reports.get("loop_replay"))
    passed = (
        bool(readiness.get("passed"))
        and bool(preflight.get("ready_for_trainer_wiring"))
        and not bool(preflight.get("requires_mount"))
        and not bool(preflight.get("touches_trainer_code"))
        and not bool(readiness.get("raw_password_sent_over_control"))
        and bool(readiness.get("direct_python_streaming_verified"))
        and bool(readiness.get("runtime_training_loop_verified"))
        and bool(readiness.get("pipeline_network_path_verified"))
        and bool(readiness.get("distributed_runtime_verified"))
        and bool(readiness.get("loop_resume_verified"))
        and bool(distributed.get("no_cross_rank_overlap"))
        and bool(readiness.get("distributed_rank_states_written"))
    )
    if require_remote_quic:
        passed = passed and bool(readiness.get("direct_quic_memory_path_verified"))
    trainer_loop_contract = _trainer_loop_contract_payload(
        handoff_path=handoff_path,
        output_dir=target_dir,
        preflight=preflight,
        readiness=readiness,
        runtime_rank=runtime_rank,
        world_size=world_size,
        trainer_prefetch_batches=trainer_prefetch_batches,
    )
    artifacts.append(
        _write_trainer_bundle_report(
            target_dir,
            "trainer-loop-contract.json",
            trainer_loop_contract,
            compact=compact,
        )
    )

    return {
        "format": TRAINER_HANDOFF_ACCEPTANCE_FORMAT,
        "passed": passed,
        "status": "passed" if passed else "failed",
        "handoff_path": None if handoff_path is None else str(handoff_path),
        "output_dir": None if target_dir is None else str(target_dir),
        "handoff_format": TRAINER_HANDOFF_FORMAT,
        "readiness_bundle_format": TRAINER_READINESS_BUNDLE_FORMAT,
        "preflight_format": TRAINER_INTEGRATION_PREFLIGHT_FORMAT,
        "runtime_smoke_format": TRAINER_RUNTIME_SMOKE_FORMAT,
        "distributed_runtime_smoke_format": TRAINER_DISTRIBUTED_RUNTIME_SMOKE_FORMAT,
        "loop_replay_format": TRAINER_LOOP_REPLAY_FORMAT,
        "ready_for_trainer_wiring": bool(preflight.get("ready_for_trainer_wiring")),
        "ready_for_remote_quic": bool(preflight.get("ready_for_remote_quic")),
        "requires_remote_quic": bool(require_remote_quic),
        "direct_quic_memory_path_verified": bool(readiness.get("direct_quic_memory_path_verified")),
        "direct_python_streaming_verified": bool(readiness.get("direct_python_streaming_verified")),
        "runtime_training_loop_verified": bool(readiness.get("runtime_training_loop_verified")),
        "pipeline_training_loop_required": bool(readiness.get("pipeline_training_loop_required")),
        "pipeline_training_loop_verified": bool(readiness.get("pipeline_training_loop_verified")),
        "pipeline_network_path_verified": bool(readiness.get("pipeline_network_path_verified")),
        "pipeline_network_path_format": readiness.get("pipeline_network_path_format"),
        "pipeline_training_loop_context": runtime.get("pipeline_training_loop_context")
        or "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
        "pipeline_training_loop_object": runtime.get("pipeline_training_loop_object")
        or "CxnfsTrainerPipelineLoop",
        "distributed_runtime_verified": bool(readiness.get("distributed_runtime_verified")),
        "loop_resume_verified": bool(readiness.get("loop_resume_verified")),
        "cross_rank_overlap_free": bool(distributed.get("no_cross_rank_overlap")),
        "rank_states_written": bool(readiness.get("distributed_rank_states_written")),
        "remote_client_required": bool(preflight.get("remote_client_required")),
        "requires_credentials": bool(preflight.get("requires_credentials")),
        "credential_prompt_supported": True,
        "credential_prompt_used": bool(readiness.get("credential_prompt_used")),
        "raw_password_sent_over_control": bool(readiness.get("raw_password_sent_over_control")),
        "requires_mount": bool(preflight.get("requires_mount")),
        "trainer_code_touched": bool(preflight.get("touches_trainer_code")),
        "stream_factory": preflight.get("stream_factory"),
        "stream_object": preflight.get("stream_object"),
        "batch_object": preflight.get("batch_object"),
        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
        "trainer_loop_contract_format": TRAINER_LOOP_CONTRACT_FORMAT,
        "trainer_loop_contract": trainer_loop_contract,
        "runtime_batch_count": _int_value(runtime.get("batch_count"), 0),
        "runtime_sample_count": _int_value(runtime.get("sample_count"), 0),
        "runtime_byte_size": _int_value(runtime.get("byte_size"), 0),
        "distributed_rank_count": _int_value(distributed.get("rank_count"), 0),
        "distributed_total_sample_count": _int_value(distributed.get("total_sample_count"), 0),
        "final_emitted": _int_value(loop_replay.get("final_emitted"), 0),
        "sample_count": _int_value(loop_replay.get("sample_count"), 0),
        "byte_size": _int_value(loop_replay.get("byte_size"), 0),
        "public_python_surface": _trainer_handoff_acceptance_public_python_surface(),
        "trainer_handoff_surface": {
            "format": "cxnfs.trainer-handoff-surface.v1",
            "handoff_loader": "CxnfsTrainerIntegrationPlan.from_file",
            "rank_binding": "CxnfsTrainerRuntimeBinding.from_file",
            "training_loop": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "remote_client_factory": "CxnfsTrainerIntegrationPlan.create_remote_client_factory",
            "uses_mount": False,
            "touches_trainer_code": False,
            "credentials_via_prompt": True,
            "password_cli_argument": False,
        },
        "artifacts": {
            "format": "cxnfs.trainer-handoff-acceptance-artifacts.v1",
            "directory": None if target_dir is None else str(target_dir),
            "artifact_count": len(artifacts),
            "items": artifacts,
        },
        "reports": {
            "handoff": handoff_payload,
            "readiness_bundle": readiness,
        },
    }


def run_trainer_handoff_acceptance_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(
        description=(
            "Accept an existing CXNFS trainer handoff by exercising the public Python "
            "training-loop surface without importing or editing trainer code."
        )
    )
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--output-dir", help="Optional directory to write component reports and rank stream states.")
    parser.add_argument("--output", help="Optional path to write the aggregate handoff acceptance JSON.")
    parser.add_argument("--integration-batches", type=int, default=1, help="Batches consumed by the integration smoke.")
    parser.add_argument("--runtime-batches", type=int, default=1, help="Batches consumed by the rank runtime smoke.")
    parser.add_argument("--runtime-rank", type=int, default=0, help="Rank used by the single-rank runtime smoke.")
    parser.add_argument("--world-size", type=int, default=2, help="Distributed world size to probe.")
    parser.add_argument("--batches-per-rank", type=int, default=1, help="Batches consumed by each distributed rank.")
    parser.add_argument("--loop-steps", type=int, default=2, help="Trainer-shaped replay steps to run.")
    parser.add_argument("--gradient-accumulation-steps", type=int, default=1, help="Microbatches per replayed step.")
    parser.add_argument("--resume-after-steps", type=int, help="Replay resume boundary for the trainer loop.")
    parser.add_argument("--trainer-prefetch-batches", type=int, help="Override trainer-loop prefetch batches.")
    parser.add_argument("--allow-partial-source-coverage", action="store_true", help="Do not require the replay to touch every weighted source.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--host", default="127.0.0.1", help="CXNFS server host for remote trainer probes.")
    parser.add_argument("--control-port", type=int, default=4509, help="CXNFS authenticated control TCP port.")
    parser.add_argument("--transport-port", type=int, default=4510, help="CXNFS bulk transport port.")
    parser.add_argument("--bulk-transport", choices=(PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC), help="Override the remote bulk transport from the handoff.")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, help="Initial cord count for remote bulk reads.")
    parser.add_argument("--max-cords", type=int, help="Maximum cord count for remote bulk reads.")
    parser.add_argument("--provisional-container-size", type=int, help="Provisional remote container size in bytes.")
    parser.add_argument("--expected-size-limit", type=int, help="Expected remote read size limit in bytes.")
    parser.add_argument("--write-connections", type=int, help="Length-prefixed write connection count.")
    parser.add_argument("--bulk-read-retries", type=int, help="Remote bulk read retry count.")
    parser.add_argument("--bulk-write-retries", type=int, help="Remote bulk write retry count.")
    parser.add_argument("--shared-client", action="store_true", help="Reuse one remote client across weighted sources.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        handoff_path = Path(args.handoff)
        handoff = _read_json_mapping(handoff_path, "trainer handoff")
        payload = build_trainer_handoff_acceptance_payload(
            handoff,
            handoff_path=handoff_path,
            output_dir=args.output_dir,
            integration_batches=args.integration_batches,
            runtime_batches=args.runtime_batches,
            runtime_rank=args.runtime_rank,
            world_size=args.world_size,
            batches_per_rank=args.batches_per_rank,
            loop_steps=args.loop_steps,
            gradient_accumulation_steps=args.gradient_accumulation_steps,
            resume_after_steps=args.resume_after_steps,
            trainer_prefetch_batches=args.trainer_prefetch_batches,
            require_remote_quic=args.require_remote_quic,
            allow_partial_source_coverage=args.allow_partial_source_coverage,
            credential_prompt=credential_prompt,
            host=args.host,
            control_port=args.control_port,
            transport_port=args.transport_port,
            timeout=args.timeout,
            server_hostname=args.server_hostname or args.host,
            cafile=args.cafile,
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size=args.provisional_container_size,
            expected_size_limit=args.expected_size_limit,
            write_connections=args.write_connections,
            bulk_transport=args.bulk_transport,
            bulk_read_retries=args.bulk_read_retries,
            bulk_write_retries=args.bulk_write_retries,
            shared_client=True if args.shared_client else None,
            compact=args.compact,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_handoff_acceptance() -> None:
    raise SystemExit(run_trainer_handoff_acceptance_cli())


def build_trainer_acceptance_fixture_payload(
    root: str | Path,
    weights: str,
    *,
    output_dir: str | Path | None = None,
    read_mode: str | TrainerReadMode = TrainerReadMode.DIRECT_BUFFERS,
    batch_size: int = 2,
    contract_batches: int = 2,
    rounds_per_read: int = 1,
    max_workers: int = 16,
    source_read_workers: int | None = 2,
    prefetch_chunks: int = 2,
    trainer_prefetch_batches: int = 1,
    shuffle: bool = False,
    seed: int = 0,
    epoch: int = 0,
    samples_per_epoch: int | None = None,
    repeat: bool = True,
    decoder: str = MdsSampleFormat.RAW.value,
    trainer_batch_iterator: str = TRAINER_BATCH_ITERATOR_DATALOADER,
    data_loader_workers: int = 0,
    tensor_allocation: CxnfsTrainerTensorAllocation | None = None,
    integration_batches: int = 1,
    runtime_batches: int = 1,
    runtime_rank: int = 0,
    world_size: int = 2,
    batches_per_rank: int = 1,
    loop_steps: int = 2,
    gradient_accumulation_steps: int = 1,
    resume_after_steps: int | None = None,
    allow_partial_source_coverage: bool = False,
    min_sources: int = 1,
    min_validation_samples: int = 1,
    min_validation_bytes: int = 1,
    compact: bool = False,
) -> dict[str, Any]:
    active_read_mode = TrainerReadMode(read_mode)
    if trainer_batch_iterator not in TRAINER_BATCH_ITERATORS:
        raise ValueError("trainer_batch_iterator must be native or trainer-dataloader")
    if contract_batches <= 0:
        raise ValueError("contract_batches must be positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    _validate_trainer_prefetch_batches(trainer_prefetch_batches)

    target_dir = Path(output_dir) if output_dir is not None else None
    contract = build_trainer_contract_payload(
        root,
        weights,
        bulk_transport=PYTHON_BULK_TRANSPORT_STREAM,
        bulk_read_retries=0,
        bulk_write_retries=0,
        read_mode=active_read_mode,
        batch_size=batch_size,
        batches=contract_batches,
        rounds_per_read=rounds_per_read,
        max_workers=max_workers,
        source_read_workers=source_read_workers,
        prefetch_chunks=prefetch_chunks,
        trainer_prefetch_batches=trainer_prefetch_batches,
        shuffle=shuffle,
        seed=seed,
        epoch=epoch,
        samples_per_epoch=samples_per_epoch,
        repeat=repeat,
        decoder=decoder,
        trainer_batch_iterator=trainer_batch_iterator,
        data_loader_workers=data_loader_workers,
        tensor_allocation=tensor_allocation,
    )
    handoff = build_trainer_handoff_payload(
        contract,
        require_remote=False,
        require_quic=False,
        min_sources=min_sources,
        min_validation_samples=min_validation_samples,
        min_validation_bytes=min_validation_bytes,
    )
    readiness = build_trainer_readiness_bundle_payload(
        handoff,
        output_dir=target_dir,
        integration_batches=integration_batches,
        runtime_batches=runtime_batches,
        runtime_rank=runtime_rank,
        world_size=world_size,
        batches_per_rank=batches_per_rank,
        loop_steps=loop_steps,
        gradient_accumulation_steps=gradient_accumulation_steps,
        resume_after_steps=resume_after_steps,
        trainer_prefetch_batches=trainer_prefetch_batches,
        allow_partial_source_coverage=allow_partial_source_coverage,
        compact=compact,
    )

    artifacts: list[dict[str, Any]] = []
    if target_dir is not None:
        artifacts.append(_write_trainer_bundle_report(target_dir, "trainer-contract.json", contract, compact=compact))
        artifacts.append(_write_trainer_bundle_report(target_dir, "trainer-handoff.json", handoff, compact=compact))
        artifacts.append(
            _write_trainer_bundle_report(
                target_dir,
                "trainer-readiness-bundle.json",
                readiness,
                compact=compact,
            )
        )
        for artifact in _mapping(readiness.get("report_artifacts")).get("artifacts", ()):
            artifacts.append(dict(_mapping(artifact)))
    else:
        artifacts.extend([
            _trainer_bundle_report_artifact("trainer-contract.json", contract, None),
            _trainer_bundle_report_artifact("trainer-handoff.json", handoff, None),
            _trainer_bundle_report_artifact("trainer-readiness-bundle.json", readiness, None),
        ])

    preflight = _mapping(readiness.get("reports")).get("preflight") or {}
    runtime = _mapping(readiness.get("reports")).get("runtime_smoke") or {}
    loop_replay = _mapping(readiness.get("reports")).get("loop_replay") or {}
    distributed = _mapping(readiness.get("reports")).get("distributed_runtime_smoke") or {}
    passed = (
        bool(readiness.get("passed"))
        and bool(handoff.get("ready_for_trainer_wiring"))
        and not bool(handoff.get("requires_mount"))
        and not bool(handoff.get("touches_trainer_code"))
        and bool(readiness.get("direct_python_streaming_verified"))
        and bool(readiness.get("runtime_training_loop_verified"))
        and bool(readiness.get("distributed_runtime_verified"))
        and bool(readiness.get("loop_resume_verified"))
    )
    trainer_loop_contract = _trainer_loop_contract_payload(
        handoff_path=None,
        output_dir=target_dir,
        preflight=preflight,
        readiness=readiness,
        runtime_rank=runtime_rank,
        world_size=world_size,
        trainer_prefetch_batches=trainer_prefetch_batches,
    )
    artifacts.append(
        _write_trainer_bundle_report(
            target_dir,
            "trainer-loop-contract.json",
            trainer_loop_contract,
            compact=compact,
        )
    )
    return {
        "format": TRAINER_ACCEPTANCE_FIXTURE_FORMAT,
        "passed": passed,
        "status": "passed" if passed else "failed",
        "root": str(root),
        "weights": parse_weighted_mds_spec(weights),
        "output_dir": None if target_dir is None else str(target_dir),
        "contract_format": TRAINER_CONTRACT_FORMAT,
        "handoff_format": TRAINER_HANDOFF_FORMAT,
        "readiness_bundle_format": TRAINER_READINESS_BUNDLE_FORMAT,
        "trainer_code_touched": bool(handoff.get("touches_trainer_code")),
        "requires_mount": bool(handoff.get("requires_mount")),
        "remote_client_required": bool(preflight.get("remote_client_required")),
        "ready_for_trainer_wiring": bool(handoff.get("ready_for_trainer_wiring")),
        "direct_python_streaming_verified": bool(readiness.get("direct_python_streaming_verified")),
        "runtime_training_loop_verified": bool(readiness.get("runtime_training_loop_verified")),
        "pipeline_training_loop_required": bool(readiness.get("pipeline_training_loop_required")),
        "pipeline_training_loop_verified": bool(readiness.get("pipeline_training_loop_verified")),
        "pipeline_training_loop_context": runtime.get("pipeline_training_loop_context")
        or "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
        "pipeline_training_loop_object": runtime.get("pipeline_training_loop_object")
        or "CxnfsTrainerPipelineLoop",
        "distributed_runtime_verified": bool(readiness.get("distributed_runtime_verified")),
        "loop_resume_verified": bool(readiness.get("loop_resume_verified")),
        "cross_rank_overlap_free": bool(distributed.get("no_cross_rank_overlap")),
        "rank_states_written": bool(readiness.get("distributed_rank_states_written")),
        "stream_factory": preflight.get("stream_factory"),
        "integration_plan_training_loop_context": preflight.get("training_loop_context"),
        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
        "stream_object": preflight.get("stream_object"),
        "batch_object": preflight.get("batch_object"),
        "trainer_loop_contract_format": TRAINER_LOOP_CONTRACT_FORMAT,
        "trainer_loop_contract": trainer_loop_contract,
        "read_mode": active_read_mode.value,
        "trainer_batch_iterator": trainer_batch_iterator,
        "data_loader_workers": data_loader_workers,
        "trainer_prefetch_batches": trainer_prefetch_batches,
        "world_size": world_size,
        "runtime_rank": runtime_rank,
        "loop_steps": loop_steps,
        "gradient_accumulation_steps": gradient_accumulation_steps,
        "final_emitted": _int_value(_mapping(loop_replay).get("final_emitted"), 0),
        "sample_count": _int_value(_mapping(loop_replay).get("sample_count"), 0),
        "byte_size": _int_value(_mapping(loop_replay).get("byte_size"), 0),
        "public_python_surface": {
            "handoff_loader": "CxnfsTrainerIntegrationPlan.from_file",
            "runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file",
            "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "stream_factory": "open_weighted_mds_trainer_stream",
            "stream_object": "CxnfsTrainerStream",
            "batch_object": "CxnfsTrainerBatch",
        },
        "artifacts": {
            "format": "cxnfs.trainer-acceptance-fixture-artifacts.v1",
            "directory": None if target_dir is None else str(target_dir),
            "artifact_count": len(artifacts),
            "items": artifacts,
        },
        "reports": {
            "contract": contract,
            "handoff": handoff,
            "readiness_bundle": readiness,
        },
    }


def run_trainer_acceptance_fixture_cli(argv: Sequence[str] | None = None, stdout=None, stderr=None) -> int:
    parser = ArgumentParser(
        description=(
            "Build a local CXNFS-only trainer acceptance fixture from weighted MDS sources "
            "without importing or editing trainer code."
        )
    )
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--output-dir", help="Directory for contract, handoff, readiness, and smoke artifacts.")
    parser.add_argument("--output", help="Optional path to write the aggregate fixture JSON.")
    parser.add_argument("--read-mode", default=TrainerReadMode.DIRECT_BUFFERS.value, choices=tuple(mode.value for mode in TrainerReadMode))
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--contract-batches", type=int, default=2)
    parser.add_argument("--rounds-per-read", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--source-read-workers", type=int, default=2)
    parser.add_argument("--prefetch-chunks", type=int, default=2)
    parser.add_argument("--trainer-prefetch-batches", type=int, default=1)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--epoch", type=int, default=0)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_trainer_repeat_args(parser, default=True)
    parser.add_argument("--decoder", default=MdsSampleFormat.RAW.value)
    parser.add_argument("--trainer-batch-iterator", default=TRAINER_BATCH_ITERATOR_DATALOADER, choices=TRAINER_BATCH_ITERATORS)
    parser.add_argument("--data-loader-workers", type=int, default=0)
    _add_trainer_tensor_allocation_args(parser, default_values=True)
    parser.add_argument("--integration-batches", type=int, default=1)
    parser.add_argument("--runtime-batches", type=int, default=1)
    parser.add_argument("--runtime-rank", type=int, default=0)
    parser.add_argument("--world-size", type=int, default=2)
    parser.add_argument("--batches-per-rank", type=int, default=1)
    parser.add_argument("--loop-steps", type=int, default=2)
    parser.add_argument("--gradient-accumulation-steps", type=int, default=1)
    parser.add_argument("--resume-after-steps", type=int)
    parser.add_argument("--allow-partial-source-coverage", action="store_true")
    parser.add_argument("--min-sources", type=int, default=1)
    parser.add_argument("--min-validation-samples", type=int, default=1)
    parser.add_argument("--min-validation-bytes", type=int, default=1)
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        payload = build_trainer_acceptance_fixture_payload(
            args.root,
            args.weights,
            output_dir=args.output_dir,
            read_mode=args.read_mode,
            batch_size=args.batch_size,
            contract_batches=args.contract_batches,
            rounds_per_read=args.rounds_per_read,
            max_workers=args.max_workers,
            source_read_workers=args.source_read_workers,
            prefetch_chunks=args.prefetch_chunks,
            trainer_prefetch_batches=args.trainer_prefetch_batches,
            shuffle=args.shuffle,
            seed=args.seed,
            epoch=args.epoch,
            samples_per_epoch=args.samples_per_epoch,
            repeat=args.repeat,
            decoder=args.decoder,
            trainer_batch_iterator=args.trainer_batch_iterator,
            data_loader_workers=args.data_loader_workers,
            tensor_allocation=_trainer_tensor_allocation_from_args(args),
            integration_batches=args.integration_batches,
            runtime_batches=args.runtime_batches,
            runtime_rank=args.runtime_rank,
            world_size=args.world_size,
            batches_per_rank=args.batches_per_rank,
            loop_steps=args.loop_steps,
            gradient_accumulation_steps=args.gradient_accumulation_steps,
            resume_after_steps=args.resume_after_steps,
            allow_partial_source_coverage=args.allow_partial_source_coverage,
            min_sources=args.min_sources,
            min_validation_samples=args.min_validation_samples,
            min_validation_bytes=args.min_validation_bytes,
            compact=args.compact,
        )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_acceptance_fixture() -> None:
    raise SystemExit(run_trainer_acceptance_fixture_cli())


def build_trainer_loop_replay_payload(
    handoff_payload: Mapping[str, Any],
    *,
    steps: int = 2,
    gradient_accumulation_steps: int = 1,
    trainer_prefetch_batches: int | None = None,
    replay_resume: bool = True,
    resume_after_steps: int | None = None,
    require_remote_quic: bool = False,
    require_source_coverage: bool = True,
    require_unique_sample_identities: bool = True,
    client_factory: DatasetClientFactory | None = None,
) -> dict[str, Any]:
    if steps <= 0:
        raise ValueError("steps must be positive")
    if gradient_accumulation_steps <= 0:
        raise ValueError("gradient_accumulation_steps must be positive")
    if replay_resume and steps < 2:
        raise ValueError("replay_resume requires at least two steps")
    plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
        handoff_payload,
        require_remote_quic=require_remote_quic,
    )
    plan_payload = plan.to_dict()
    if bool(plan_payload.get("remote_client_required")) and client_factory is None:
        raise ValueError("remote trainer loop replay requires a caller-provided client_factory")
    stream_kwargs = plan.stream_kwargs()
    loop_options = plan.trainer_loop_options()
    active_prefetch = _validate_trainer_prefetch_batches(
        _int_value(
            trainer_prefetch_batches
            if trainer_prefetch_batches is not None
            else loop_options.get("trainer_prefetch_batches"),
            0,
        )
    )
    requested_microbatches = steps * gradient_accumulation_steps
    first_segment_steps = steps
    if replay_resume:
        first_segment_steps = (
            resume_after_steps
            if resume_after_steps is not None
            else max(1, steps // 2)
        )
        if first_segment_steps <= 0 or first_segment_steps >= steps:
            raise ValueError("resume_after_steps must be between 1 and steps - 1")
    phase_specs: list[tuple[str, int]] = [("initial", first_segment_steps)]
    if replay_resume:
        phase_specs.append(("resume", steps - first_segment_steps))

    all_batches: list[CxnfsTrainerBatch] = []
    step_reports: list[dict[str, Any]] = []
    phase_reports: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    sample_identity_counts: dict[str, int] = {}
    duplicate_sample_identities: set[str] = set()
    cursor_trace_complete = True
    cursor_delta_verified = True
    cursor_monotonic = True
    resume_state: Mapping[str, Any] | None = None
    final_state: Mapping[str, Any] | None = None
    first_initial_emitted: int | None = None
    first_final_emitted: int | None = None
    second_initial_emitted: int | None = None
    ended_early = False
    elapsed_start = time.perf_counter()
    last_start_cursor: CxnfsTrainerCursor | None = None
    training_loop_object = ""
    training_loop_base_stream_object = ""
    training_loop_prefetch_applied = False
    read_mode = TrainerReadMode(stream_kwargs.get("read_mode") or TrainerReadMode.DIRECT_BUFFERS.value)
    tensor_allocation = stream_kwargs.get("tensor_allocation")
    if not isinstance(tensor_allocation, CxnfsTrainerTensorAllocation):
        tensor_allocation = CxnfsTrainerTensorAllocation()
    remote_options = _trainer_integration_smoke_remote_options(handoff_payload, stream_kwargs)
    trainer_batch_iterator = str(stream_kwargs.get("trainer_batch_iterator") or TRAINER_BATCH_ITERATOR_NATIVE)
    data_loader_workers = _int_value(stream_kwargs.get("data_loader_workers"), 0)
    worker_partitioned_trace = (
        trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER
        and data_loader_workers > 0
    )
    stream_report: dict[str, Any] | None = None

    for phase_name, phase_steps in phase_specs:
        stream_overrides: dict[str, Any] = {}
        if client_factory is not None:
            stream_overrides["client_factory"] = client_factory
        if phase_name == "resume":
            assert resume_state is not None
            stream_overrides["resume_state"] = resume_state
        with plan.open_training_loop(
            trainer_prefetch_batches=active_prefetch,
            **stream_overrides,
        ) as trainer_loop:
            stream = _trainer_loop_base_stream(trainer_loop)
            if last_start_cursor is None:
                last_start_cursor = stream.cursor
            if phase_name == "initial":
                first_initial_emitted = stream.cursor.emitted
            elif phase_name == "resume":
                second_initial_emitted = stream.cursor.emitted
            training_loop_object = type(trainer_loop).__name__
            training_loop_base_stream_object = type(stream).__name__
            training_loop_prefetch_applied = isinstance(trainer_loop, CxnfsTrainerPrefetchStream)
            phase_initial_emitted = stream.cursor.emitted
            phase_cursor_emitted = phase_initial_emitted
            phase_batch_count = 0
            phase_sample_count = 0
            phase_byte_size = 0
            for _ in range(phase_steps):
                step_index = len(step_reports)
                microbatches: list[dict[str, Any]] = []
                step_sample_count = 0
                step_byte_size = 0
                step_source_counts: dict[str, int] = {}
                step_sample_identities: list[str] = []
                step_start_emitted = phase_cursor_emitted
                step_cursor_delta_verified = True
                step_cursor_monotonic = True
                for accumulation_index in range(gradient_accumulation_steps):
                    try:
                        batch = trainer_loop.next_batch()
                    except StopIteration:
                        ended_early = True
                        break
                    all_batches.append(batch)
                    phase_batch_count += 1
                    phase_sample_count += batch.sample_count
                    phase_byte_size += batch.byte_size
                    step_sample_count += batch.sample_count
                    step_byte_size += batch.byte_size
                    for source, count in batch.source_counts.items():
                        source_counts[source] = source_counts.get(source, 0) + count
                        step_source_counts[source] = step_source_counts.get(source, 0) + count
                    batch_identities = [
                        _trainer_batch_sample_identity(sample)
                        for sample in batch.samples
                    ]
                    for identity in batch_identities:
                        previous = sample_identity_counts.get(identity, 0)
                        if previous:
                            duplicate_sample_identities.add(identity)
                        sample_identity_counts[identity] = previous + 1
                    step_sample_identities.extend(batch_identities)
                    batch_start_emitted = phase_cursor_emitted
                    batch_resume_emitted = (
                        batch.resume_cursor.emitted if batch.resume_cursor is not None else None
                    )
                    batch_reported_resume_emitted = batch_resume_emitted
                    cursor_trace_source = "batch-resume-cursor"
                    if worker_partitioned_trace:
                        batch_resume_emitted = batch_start_emitted + batch.sample_count
                        cursor_trace_source = "logical-worker-step"
                    if batch_resume_emitted is None:
                        cursor_trace_complete = False
                        step_cursor_delta_verified = False
                        cursor_delta_verified = False
                    else:
                        expected_resume_emitted = batch_start_emitted + batch.sample_count
                        batch_delta_verified = batch_resume_emitted == expected_resume_emitted
                        batch_monotonic = batch_resume_emitted >= batch_start_emitted
                        step_cursor_delta_verified = step_cursor_delta_verified and batch_delta_verified
                        cursor_delta_verified = cursor_delta_verified and batch_delta_verified
                        step_cursor_monotonic = step_cursor_monotonic and batch_monotonic
                        cursor_monotonic = cursor_monotonic and batch_monotonic
                        phase_cursor_emitted = batch_resume_emitted
                    batch_summary = _trainer_loop_replay_batch_summary(
                        batch,
                        step_index=step_index,
                        accumulation_index=accumulation_index,
                    )
                    batch_summary.update({
                        "sample_identities": batch_identities,
                        "resume_start_emitted": batch_start_emitted,
                        "reported_resume_emitted": batch_reported_resume_emitted,
                        "cursor_trace_source": cursor_trace_source,
                        "resume_delta": (
                            None
                            if batch_resume_emitted is None
                            else batch_resume_emitted - batch_start_emitted
                        ),
                        "cursor_delta_verified": (
                            False
                            if batch_resume_emitted is None
                            else batch_resume_emitted == batch_start_emitted + batch.sample_count
                        ),
                    })
                    microbatches.append(batch_summary)
                if not microbatches:
                    break
                step_reports.append({
                    "step_index": step_index,
                    "phase": phase_name,
                    "microbatch_count": len(microbatches),
                    "sample_count": step_sample_count,
                    "byte_size": step_byte_size,
                    "source_counts": dict(sorted(step_source_counts.items())),
                    "sample_identities": step_sample_identities,
                    "resume_start_emitted": step_start_emitted,
                    "resume_final_emitted": phase_cursor_emitted,
                    "resume_delta": phase_cursor_emitted - step_start_emitted,
                    "cursor_trace_source": (
                        "logical-worker-step" if worker_partitioned_trace else "batch-resume-cursor"
                    ),
                    "cursor_delta_verified": (
                        step_cursor_delta_verified
                        and phase_cursor_emitted == step_start_emitted + step_sample_count
                    ),
                    "resume_monotonic": step_cursor_monotonic,
                    "optimizer_step_invoked": len(microbatches) == gradient_accumulation_steps,
                    "batches": microbatches,
                })
                if ended_early:
                    break
            final_state = stream.state_dict()
            if phase_name == "initial":
                resume_state = dict(final_state)
                first_final_emitted = _int_value(_mapping(final_state.get("cursor")).get("emitted"), 0)
            stream_report = _trainer_stream_smoke_report(
                stream.dataset,
                all_batches,
                root=str(stream_kwargs.get("root") or ""),
                batch_size=_int_value(stream_kwargs.get("batch_size"), 0),
                requested_batches=requested_microbatches,
                warmup_batches=0,
                elapsed_seconds=max(time.perf_counter() - elapsed_start, 1e-9),
                remote_options=remote_options,
                read_mode=read_mode,
                trainer_batch_iterator=trainer_batch_iterator,
                data_loader_workers=data_loader_workers,
                trainer_prefetch_batches=active_prefetch,
                tensor_allocation=tensor_allocation,
                start_cursor=last_start_cursor,
                stream_state=final_state or {},
                client_factory=client_factory,
            )
            phase_reports.append({
                "phase": phase_name,
                "requested_steps": phase_steps,
                "step_count": sum(1 for step in step_reports if step["phase"] == phase_name),
                "batch_count": phase_batch_count,
                "sample_count": phase_sample_count,
                "byte_size": phase_byte_size,
                "initial_emitted": phase_initial_emitted,
                "final_emitted": _int_value(_mapping(final_state.get("cursor")).get("emitted"), 0),
            })
        if ended_early:
            break

    elapsed_seconds = max(time.perf_counter() - elapsed_start, 1e-9)
    expected_sources = list(_mapping(stream_kwargs.get("weights")).keys())
    observed_sources = sorted(source_counts)
    source_coverage_complete = all(source in source_counts for source in expected_sources)
    sample_identity_count = sum(sample_identity_counts.values())
    no_duplicate_sample_identities = len(duplicate_sample_identities) == 0
    final_cursor = _mapping((final_state or {}).get("cursor"))
    final_emitted = _int_value(final_cursor.get("emitted"), 0)
    initial_emitted = first_initial_emitted if first_initial_emitted is not None else 0
    resume_continuity = (
        not replay_resume
        or second_initial_emitted is None
        or first_final_emitted == second_initial_emitted
    )
    passed = (
        len(step_reports) == steps
        and len(all_batches) == requested_microbatches
        and not ended_early
        and all(step["optimizer_step_invoked"] for step in step_reports)
        and cursor_trace_complete
        and cursor_delta_verified
        and cursor_monotonic
        and (source_coverage_complete or not require_source_coverage)
        and (no_duplicate_sample_identities or not require_unique_sample_identities)
        and resume_continuity
        and bool(final_state)
        and final_state.get("format") == TRAINER_STREAM_STATE_FORMAT
        and final_emitted == initial_emitted + sum(batch.sample_count for batch in all_batches)
    )
    stream_report_payload = _mapping(stream_report)
    final_state_payload = _mapping(final_state)
    read_path_evidence = _mapping(stream_report_payload.get("read_path_evidence"))
    worker_partition = _mapping(stream_report_payload.get("worker_partition_evidence"))
    return {
        "format": TRAINER_LOOP_REPLAY_FORMAT,
        "passed": passed,
        "handoff_format": handoff_payload.get("format"),
        "plan_format": plan_payload.get("format"),
        "ready_for_trainer_wiring": bool(handoff_payload.get("ready_for_trainer_wiring")),
        "ready_for_remote_quic": bool(handoff_payload.get("ready_for_remote_quic")),
        "requires_mount": bool(handoff_payload.get("requires_mount")),
        "touches_trainer_code": bool(handoff_payload.get("touches_trainer_code")),
        "remote_client_required": bool(plan_payload.get("remote_client_required")),
        "stream_factory": plan_payload.get("stream_factory"),
        "training_loop_context": plan_payload.get("training_loop_context")
        or "CxnfsTrainerIntegrationPlan.open_training_loop",
        "training_loop_context_used": True,
        "training_loop_object": training_loop_object,
        "training_loop_base_stream_object": training_loop_base_stream_object,
        "training_loop_prefetch_batches": active_prefetch,
        "training_loop_prefetch_applied": training_loop_prefetch_applied,
        "read_mode": read_mode.value,
        "trainer_batch_iterator": trainer_batch_iterator,
        "data_loader_workers": data_loader_workers,
        "repeat": _bool_value(stream_kwargs.get("repeat"), True),
        "samples_per_epoch": _int_value(stream_kwargs.get("samples_per_epoch"), 0),
        "worker_partitioning_enabled": bool(final_state_payload.get("worker_partitioning_enabled")),
        "worker_cursor_count": _int_value(final_state_payload.get("worker_cursor_count"), 0),
        "worker_batch_read_evidence_observed": bool(read_path_evidence.get("worker_batch_read_evidence_observed")),
        "worker_batch_read_evidence_count": _int_value(read_path_evidence.get("worker_batch_read_evidence_count"), 0),
        "worker_partition_evidence": dict(worker_partition),
        "step_count": len(step_reports),
        "requested_steps": steps,
        "gradient_accumulation_steps": gradient_accumulation_steps,
        "requested_microbatches": requested_microbatches,
        "batch_count": len(all_batches),
        "sample_count": sum(batch.sample_count for batch in all_batches),
        "byte_size": sum(batch.byte_size for batch in all_batches),
        "source_counts": dict(sorted(source_counts.items())),
        "expected_sources": expected_sources,
        "observed_sources": observed_sources,
        "weighted_source_coverage_complete": source_coverage_complete,
        "require_source_coverage": bool(require_source_coverage),
        "step_trace_format": "cxnfs.trainer-step-trace.v1",
        "step_trace_complete": (
            len(step_reports) == steps
            and cursor_trace_complete
            and cursor_delta_verified
            and cursor_monotonic
        ),
        "step_trace": [
            _trainer_loop_replay_step_trace_summary(step)
            for step in step_reports
        ],
        "cursor_trace_complete": cursor_trace_complete,
        "cursor_delta_verified": cursor_delta_verified,
        "cursor_monotonic": cursor_monotonic,
        "sample_identity_format": "source_name:source_index:sample_index",
        "sample_identity_count": sample_identity_count,
        "unique_sample_identity_count": len(sample_identity_counts),
        "duplicate_sample_identity_count": sample_identity_count - len(sample_identity_counts),
        "duplicate_sample_identities": sorted(duplicate_sample_identities),
        "no_duplicate_sample_identities": no_duplicate_sample_identities,
        "require_unique_sample_identities": bool(require_unique_sample_identities),
        "resume_replayed": bool(replay_resume and second_initial_emitted is not None),
        "resume_after_steps": first_segment_steps if replay_resume else None,
        "resume_continuity_verified": resume_continuity,
        "first_initial_emitted": first_initial_emitted,
        "first_final_emitted": first_final_emitted,
        "second_initial_emitted": second_initial_emitted,
        "final_emitted": final_emitted,
        "ended_early": ended_early,
        "elapsed_seconds": elapsed_seconds,
        "steps": step_reports,
        "phases": phase_reports,
        "resume_state": dict(resume_state) if resume_state is not None else None,
        "final_state": dict(final_state) if final_state is not None else None,
        "stream_report": stream_report,
    }


def _trainer_loop_replay_batch_summary(
    batch: CxnfsTrainerBatch,
    *,
    step_index: int,
    accumulation_index: int,
) -> dict[str, Any]:
    return {
        "step_index": step_index,
        "accumulation_index": accumulation_index,
        "batch_index": batch.batch_index,
        "epoch": batch.epoch,
        "sample_count": batch.sample_count,
        "byte_size": batch.byte_size,
        "source_counts": batch.source_counts,
        "resume_emitted": batch.resume_cursor.emitted if batch.resume_cursor is not None else None,
        "samples": [
            _trainer_batch_sample_to_dict(sample, include_sample_data=False)
            for sample in batch.samples
        ],
    }


def _trainer_batch_sample_identity(sample: CxnfsTrainerBatchSample) -> str:
    return f"{sample.source_name}:{sample.source_index}:{sample.sample_index}"


def _trainer_loop_replay_step_trace_summary(step: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "step_index": _int_value(step.get("step_index"), 0),
        "phase": step.get("phase"),
        "microbatch_count": _int_value(step.get("microbatch_count"), 0),
        "sample_count": _int_value(step.get("sample_count"), 0),
        "byte_size": _int_value(step.get("byte_size"), 0),
        "source_counts": dict(_mapping(step.get("source_counts"))),
        "sample_identities": [
            str(identity)
            for identity in step.get("sample_identities", ())
            if isinstance(identity, str)
        ],
        "resume_start_emitted": step.get("resume_start_emitted"),
        "resume_final_emitted": step.get("resume_final_emitted"),
        "resume_delta": step.get("resume_delta"),
        "cursor_trace_source": step.get("cursor_trace_source"),
        "cursor_delta_verified": bool(step.get("cursor_delta_verified")),
        "resume_monotonic": bool(step.get("resume_monotonic")),
    }


def run_trainer_integration_smoke_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Open a CXNFS trainer stream from a trainer handoff artifact and read smoke batches.")
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--batches", type=int, default=1, help="Number of trainer batches to consume.")
    parser.add_argument("--trainer-prefetch-batches", type=int, help="Override trainer-loop prefetch batches from the handoff plan.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--host", default="127.0.0.1", help="CXNFS server host for remote trainer handoff smoke.")
    parser.add_argument("--control-port", type=int, default=4509, help="CXNFS authenticated control TCP port.")
    parser.add_argument("--transport-port", type=int, default=4510, help="CXNFS bulk transport port.")
    parser.add_argument("--bulk-transport", choices=(PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC), help="Override the remote bulk transport from the handoff.")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, help="Initial cord count for remote bulk reads.")
    parser.add_argument("--max-cords", type=int, help="Maximum cord count for remote bulk reads.")
    parser.add_argument("--provisional-container-size", type=int, help="Provisional remote container size in bytes.")
    parser.add_argument("--expected-size-limit", type=int, help="Expected remote read size limit in bytes.")
    parser.add_argument("--write-connections", type=int, help="Length-prefixed write connection count.")
    parser.add_argument("--bulk-read-retries", type=int, help="Remote bulk read retry count.")
    parser.add_argument("--bulk-write-retries", type=int, help="Remote bulk write retry count.")
    parser.add_argument("--shared-client", action="store_true", help="Reuse one remote client across weighted sources.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        handoff = _read_json_mapping(Path(args.handoff), "trainer handoff")
        plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
            handoff,
            require_remote_quic=args.require_remote_quic,
        )
        client_factory = None
        credentials = None
        try:
            if bool(plan.to_dict().get("remote_client_required")):
                credentials = credential_prompt()
                client_factory = _trainer_integration_smoke_client_factory_from_args(
                    plan,
                    args,
                    credentials,
                    control_client_factory,
                    bulk_client_factory,
                    tls_context_factory,
                )
            payload = build_trainer_integration_smoke_payload(
                handoff,
                batches=args.batches,
                trainer_prefetch_batches=args.trainer_prefetch_batches,
                require_remote_quic=args.require_remote_quic,
                client_factory=client_factory,
            )
        finally:
            if client_factory is not None:
                close_client_factory = getattr(client_factory, "close", None)
                if callable(close_client_factory):
                    close_client_factory()
            if credentials is not None:
                credentials.close()
        indent = None if args.compact else 2
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_integration_smoke() -> None:
    raise SystemExit(run_trainer_integration_smoke_cli())


def run_trainer_runtime_smoke_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Open a rank-local CXNFS trainer runtime binding and read smoke batches.")
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--batches", type=int, default=1, help="Number of trainer batches to consume.")
    parser.add_argument("--rank", type=int, help="Distributed trainer rank; defaults to RANK.")
    parser.add_argument("--world-size", type=int, help="Distributed trainer world size; defaults to WORLD_SIZE.")
    parser.add_argument("--trainer-prefetch-batches", type=int, help="Override trainer-loop prefetch batches from the handoff plan.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--host", default="127.0.0.1", help="CXNFS server host for remote trainer runtime smoke.")
    parser.add_argument("--control-port", type=int, default=4509, help="CXNFS authenticated control TCP port.")
    parser.add_argument("--transport-port", type=int, default=4510, help="CXNFS bulk transport port.")
    parser.add_argument("--bulk-transport", choices=(PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC), help="Override the remote bulk transport from the handoff.")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, help="Initial cord count for remote bulk reads.")
    parser.add_argument("--max-cords", type=int, help="Maximum cord count for remote bulk reads.")
    parser.add_argument("--provisional-container-size", type=int, help="Provisional remote container size in bytes.")
    parser.add_argument("--expected-size-limit", type=int, help="Expected remote read size limit in bytes.")
    parser.add_argument("--write-connections", type=int, help="Length-prefixed write connection count.")
    parser.add_argument("--bulk-read-retries", type=int, help="Remote bulk read retry count.")
    parser.add_argument("--bulk-write-retries", type=int, help="Remote bulk write retry count.")
    parser.add_argument("--shared-client", action="store_true", help="Reuse one remote client across weighted sources.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        handoff = _read_json_mapping(Path(args.handoff), "trainer handoff")
        payload = build_trainer_runtime_smoke_payload(
            handoff,
            batches=args.batches,
            rank=args.rank,
            world_size=args.world_size,
            trainer_prefetch_batches=args.trainer_prefetch_batches,
            require_remote_quic=args.require_remote_quic,
            credential_prompt=credential_prompt,
            host=args.host,
            control_port=args.control_port,
            transport_port=args.transport_port,
            timeout=args.timeout,
            server_hostname=args.server_hostname or args.host,
            cafile=args.cafile,
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size=args.provisional_container_size,
            expected_size_limit=args.expected_size_limit,
            write_connections=args.write_connections,
            bulk_transport=args.bulk_transport,
            bulk_read_retries=args.bulk_read_retries,
            bulk_write_retries=args.bulk_write_retries,
            shared_client=True if args.shared_client else None,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        indent = None if args.compact else 2
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_runtime_smoke() -> None:
    raise SystemExit(run_trainer_runtime_smoke_cli())


def run_trainer_distributed_runtime_smoke_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Open CXNFS trainer runtime bindings for every rank and read smoke batches.")
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--world-size", type=int, required=True, help="Distributed trainer world size to probe.")
    parser.add_argument("--batches-per-rank", type=int, default=1, help="Number of trainer batches to consume per rank.")
    parser.add_argument("--trainer-prefetch-batches", type=int, help="Override trainer-loop prefetch batches from the handoff plan.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--host", default="127.0.0.1", help="CXNFS server host for remote trainer runtime smoke.")
    parser.add_argument("--control-port", type=int, default=4509, help="CXNFS authenticated control TCP port.")
    parser.add_argument("--transport-port", type=int, default=4510, help="CXNFS bulk transport port.")
    parser.add_argument("--bulk-transport", choices=(PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC), help="Override the remote bulk transport from the handoff.")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, help="Initial cord count for remote bulk reads.")
    parser.add_argument("--max-cords", type=int, help="Maximum cord count for remote bulk reads.")
    parser.add_argument("--provisional-container-size", type=int, help="Provisional remote container size in bytes.")
    parser.add_argument("--expected-size-limit", type=int, help="Expected remote read size limit in bytes.")
    parser.add_argument("--write-connections", type=int, help="Length-prefixed write connection count.")
    parser.add_argument("--bulk-read-retries", type=int, help="Remote bulk read retry count.")
    parser.add_argument("--bulk-write-retries", type=int, help="Remote bulk write retry count.")
    parser.add_argument("--shared-client", action="store_true", help="Reuse one remote client across weighted sources.")
    parser.add_argument("--write-rank-states-dir", help="Optional directory to write one trainer stream state JSON per rank.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        if argv is not None and any(arg in {"--help", "-h"} for arg in argv):
            parser.print_help(out)
            return 0
        args = parser.parse_args(argv)
        handoff = _read_json_mapping(Path(args.handoff), "trainer handoff")
        payload = build_trainer_distributed_runtime_smoke_payload(
            handoff,
            world_size=args.world_size,
            batches_per_rank=args.batches_per_rank,
            trainer_prefetch_batches=args.trainer_prefetch_batches,
            require_remote_quic=args.require_remote_quic,
            credential_prompt=credential_prompt,
            host=args.host,
            control_port=args.control_port,
            transport_port=args.transport_port,
            timeout=args.timeout,
            server_hostname=args.server_hostname or args.host,
            cafile=args.cafile,
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size=args.provisional_container_size,
            expected_size_limit=args.expected_size_limit,
            write_connections=args.write_connections,
            bulk_transport=args.bulk_transport,
            bulk_read_retries=args.bulk_read_retries,
            bulk_write_retries=args.bulk_write_retries,
            shared_client=True if args.shared_client else None,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        if args.write_rank_states_dir:
            write_trainer_distributed_runtime_rank_states(
                payload,
                args.write_rank_states_dir,
                compact=args.compact,
            )
        indent = None if args.compact else 2
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_distributed_runtime_smoke() -> None:
    raise SystemExit(run_trainer_distributed_runtime_smoke_cli())


def run_trainer_loop_replay_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Replay a CXNFS trainer handoff through a trainer-shaped batch loop.")
    parser.add_argument("handoff", help="Path to a cxnfs.trainer-handoff.v1 JSON report.")
    parser.add_argument("--steps", type=int, default=2, help="Number of optimizer-shaped trainer steps to replay.")
    parser.add_argument("--gradient-accumulation-steps", type=int, default=1, help="Microbatches consumed per replayed step.")
    parser.add_argument("--trainer-prefetch-batches", type=int, help="Override trainer-loop prefetch batches from the handoff plan.")
    parser.add_argument("--resume-after-steps", type=int, help="Step boundary to save and replay trainer stream state.")
    parser.add_argument("--no-replay-resume", action="store_true", help="Replay the loop in one process without a resume split.")
    parser.add_argument("--allow-partial-source-coverage", action="store_true", help="Do not fail when the requested steps do not touch every weighted source.")
    parser.add_argument("--allow-duplicate-sample-identities", action="store_true", help="Do not fail when replayed steps revisit the same source sample identity.")
    parser.add_argument("--require-remote-quic", action="store_true", help="Require the handoff to prove remote QUIC readiness.")
    parser.add_argument("--host", default="127.0.0.1", help="CXNFS server host for remote trainer handoff replay.")
    parser.add_argument("--control-port", type=int, default=4509, help="CXNFS authenticated control TCP port.")
    parser.add_argument("--transport-port", type=int, default=4510, help="CXNFS bulk transport port.")
    parser.add_argument("--bulk-transport", choices=(PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC), help="Override the remote bulk transport from the handoff.")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host.")
    parser.add_argument("--cafile", help="Optional CA bundle for the server certificate.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, help="Initial cord count for remote bulk reads.")
    parser.add_argument("--max-cords", type=int, help="Maximum cord count for remote bulk reads.")
    parser.add_argument("--provisional-container-size", type=int, help="Provisional remote container size in bytes.")
    parser.add_argument("--expected-size-limit", type=int, help="Expected remote read size limit in bytes.")
    parser.add_argument("--write-connections", type=int, help="Length-prefixed write connection count.")
    parser.add_argument("--bulk-read-retries", type=int, help="Remote bulk read retry count.")
    parser.add_argument("--bulk-write-retries", type=int, help="Remote bulk write retry count.")
    parser.add_argument("--shared-client", action="store_true", help="Reuse one remote client across weighted sources.")
    parser.add_argument("--write-resume-state", help="Optional path to write the replayed resume state JSON.")
    parser.add_argument("--write-final-state", help="Optional path to write the final trainer stream state JSON.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        handoff = _read_json_mapping(Path(args.handoff), "trainer handoff")
        plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
            handoff,
            require_remote_quic=args.require_remote_quic,
        )
        client_factory = None
        credentials = None
        try:
            if bool(plan.to_dict().get("remote_client_required")):
                credentials = credential_prompt()
                client_factory = _trainer_integration_smoke_client_factory_from_args(
                    plan,
                    args,
                    credentials,
                    control_client_factory,
                    bulk_client_factory,
                    tls_context_factory,
                )
            payload = build_trainer_loop_replay_payload(
                handoff,
                steps=args.steps,
                gradient_accumulation_steps=args.gradient_accumulation_steps,
                trainer_prefetch_batches=args.trainer_prefetch_batches,
                replay_resume=not args.no_replay_resume,
                resume_after_steps=args.resume_after_steps,
                require_remote_quic=args.require_remote_quic,
                require_source_coverage=not args.allow_partial_source_coverage,
                require_unique_sample_identities=not args.allow_duplicate_sample_identities,
                client_factory=client_factory,
            )
        finally:
            if client_factory is not None:
                close_client_factory = getattr(client_factory, "close", None)
                if callable(close_client_factory):
                    close_client_factory()
            if credentials is not None:
                credentials.close()
        indent = None if args.compact else 2
        if args.write_resume_state and payload.get("resume_state") is not None:
            _write_json_mapping_atomic(Path(args.write_resume_state), _mapping(payload.get("resume_state")), compact=args.compact)
        if args.write_final_state and payload.get("final_state") is not None:
            _write_json_mapping_atomic(Path(args.write_final_state), _mapping(payload.get("final_state")), compact=args.compact)
        text = json.dumps(payload, indent=indent, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_loop_replay() -> None:
    raise SystemExit(run_trainer_loop_replay_cli())


def _trainer_integration_smoke_client_factory_from_args(
    plan: CxnfsTrainerIntegrationPlan,
    args: Any,
    credentials: ClientCredentials,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
    tls_context_factory: Callable[[str | None], Any],
) -> DatasetClientFactory:
    return plan.create_remote_client_factory(
        credentials,
        host=args.host,
        control_port=args.control_port,
        transport_port=args.transport_port,
        timeout=args.timeout,
        server_hostname=args.server_hostname or args.host,
        cafile=args.cafile,
        initial_cords=args.initial_cords,
        max_cords=args.max_cords,
        provisional_container_size=args.provisional_container_size,
        expected_size_limit=args.expected_size_limit,
        write_connections=args.write_connections,
        bulk_transport=args.bulk_transport,
        bulk_read_retries=args.bulk_read_retries,
        bulk_write_retries=args.bulk_write_retries,
        shared_client=True if args.shared_client else None,
        control_client_factory=control_client_factory,
        bulk_client_factory=bulk_client_factory,
        tls_context_factory=tls_context_factory,
    )


def _trainer_integration_smoke_remote_options(
    handoff_payload: Mapping[str, Any],
    stream_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    contract = _mapping(handoff_payload.get("contract"))
    return {
        "transport": contract.get("transport") or ("local-mds" if not stream_kwargs.get("remote_root") else "cxnfs-length-prefixed"),
        "bulk_transport": contract.get("bulk_transport") or PYTHON_BULK_TRANSPORT_STREAM,
        "remote_root": stream_kwargs.get("remote_root"),
        "bulk_read_retries": _int_value(contract.get("bulk_read_retries"), 0),
        "bulk_write_retries": _int_value(contract.get("bulk_write_retries"), 0),
    }


def _trainer_handoff_contract_summary(
    contract_payload: Mapping[str, Any],
    path: str | Path | None,
) -> dict[str, Any]:
    network = _mapping(contract_payload.get("network_contract"))
    read_contract = _mapping(contract_payload.get("read_contract"))
    python_surface = _mapping(contract_payload.get("python_surface"))
    return {
        "path": str(path) if path is not None else None,
        "format": contract_payload.get("format"),
        "root": contract_payload.get("root"),
        "remote_root": contract_payload.get("remote_root"),
        "weights": dict(_mapping(contract_payload.get("weights"))),
        "workload_fingerprint": _mapping(contract_payload.get("workload_fingerprint")) or None,
        "transport": network.get("transport"),
        "bulk_transport": network.get("bulk_transport"),
        "bulk_transport_available": bool(network.get("bulk_transport_available")),
        "max_cords": _int_value(network.get("max_cords"), 0),
        "bulk_read_retries": _int_value(network.get("bulk_read_retries"), 0),
        "bulk_write_retries": _int_value(network.get("bulk_write_retries"), 0),
        "raw_password_sent_over_control": bool(network.get("raw_password_sent_over_control")),
        "read_mode": read_contract.get("read_mode"),
        "trainer_batch_iterator": read_contract.get("trainer_batch_iterator"),
        "data_loader_workers": _int_value(read_contract.get("data_loader_workers"), 0),
        "batch_size": _int_value(read_contract.get("batch_size"), 0),
        "rounds_per_read": _int_value(read_contract.get("rounds_per_read"), 1),
        "max_workers": _int_value(read_contract.get("max_workers"), 16),
        "source_read_workers": _optional_int_value(read_contract.get("source_read_workers")),
        "prefetch_chunks": _int_value(read_contract.get("prefetch_chunks"), 1),
        "trainer_prefetch_batches": _int_value(read_contract.get("trainer_prefetch_batches"), 0),
        "shuffle": bool(read_contract.get("shuffle")),
        "seed": _optional_int_value(read_contract.get("seed")),
        "epoch": _int_value(read_contract.get("epoch"), 0),
        "samples_per_epoch": _optional_int_value(read_contract.get("samples_per_epoch")),
        "training_shard": read_contract.get("training_shard"),
        "worker_partitioning": read_contract.get("worker_partitioning"),
        "tensor_allocation": read_contract.get("tensor_allocation"),
        "requires_mount": bool(python_surface.get("requires_mount")),
        "touches_trainer_code": bool(python_surface.get("touches_trainer_code")),
    }


def _trainer_handoff_surface_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "format": payload.get("format"),
        "passed": bool(payload.get("passed")),
        "status": payload.get("status"),
        "failed_check_count": len(payload.get("failed_checks", ())) if isinstance(payload.get("failed_checks"), list) else 0,
        "requirements": dict(_mapping(payload.get("requirements"))),
        "payload": dict(payload),
    }


def _trainer_handoff_workload_summary(
    payload: Mapping[str, Any] | None,
    path: str | Path | None,
) -> dict[str, Any] | None:
    workload = _mapping(payload)
    if not workload:
        return None
    blockfs_gate = _mapping(workload.get("blockfs_gate"))
    read_reliability = _mapping(workload.get("read_path_reliability"))
    remote_topology = _mapping(workload.get("remote_client_topology"))
    return {
        "path": str(path) if path is not None else None,
        "format": workload.get("format"),
        "transport": workload.get("transport"),
        "bulk_transport": workload.get("bulk_transport"),
        "read_mode": workload.get("read_mode"),
        "remote_root": workload.get("remote_root"),
        "bulk_read_retries": _int_value(workload.get("bulk_read_retries"), 0),
        "bulk_write_retries": _int_value(workload.get("bulk_write_retries"), 0),
        "workload_fingerprint": _mapping(workload.get("workload_fingerprint")) or None,
        "run_count": _int_value(workload.get("run_count"), 0),
        "sample_count": _int_value(workload.get("sample_count"), 0),
        "byte_size": _int_value(workload.get("byte_size"), 0),
        "cord_sweep_count": _workload_cord_sweep_count(workload),
        "read_path_reliability_complete": bool(read_reliability.get("complete")),
        "unrecovered_read_failures": _int_value(read_reliability.get("unrecovered_read_failures"), 0),
        "read_retry_successes": _int_value(read_reliability.get("read_retry_successes"), 0),
        "bulk_transport_reconnects": _int_value(read_reliability.get("bulk_transport_reconnects"), 0),
        "remote_client_topology_complete": bool(remote_topology.get("complete")),
        "remote_client_per_source_complete": bool(remote_topology.get("per_source_client_complete")),
        "remote_client_topology_report_count": _int_value(remote_topology.get("topology_report_count"), 0),
        "read_path_evidence_complete": bool(blockfs_gate.get("read_path_evidence_complete")),
        "memory_path_evidence_complete": bool(blockfs_gate.get("memory_path_evidence_complete")),
        "write_path_evidence_complete": bool(blockfs_gate.get("write_path_evidence_complete")),
        "workload_scale_complete": bool(blockfs_gate.get("workload_scale_complete")),
        "kernel_module_allowed": bool(blockfs_gate.get("kernel_module_allowed")),
    }


def _trainer_handoff_matrix_summary(
    payload: Mapping[str, Any] | None,
    path: str | Path | None,
) -> dict[str, Any] | None:
    matrix = _mapping(payload)
    if not matrix:
        return None
    remote_topology = _mapping(matrix.get("remote_client_topology"))
    cases = [
        _mapping(case)
        for case in matrix.get("cases", ())
        if isinstance(case, Mapping)
    ]
    return {
        "path": str(path) if path is not None else None,
        "format": matrix.get("format"),
        "passed": bool(matrix.get("passed")),
        "status": matrix.get("status"),
        "transport": matrix.get("transport"),
        "bulk_transport": matrix.get("bulk_transport"),
        "case_count": _int_value(matrix.get("case_count"), len(cases)),
        "passed_case_count": _int_value(matrix.get("passed_case_count"), 0),
        "failed_case_count": _int_value(matrix.get("failed_case_count"), 0),
        "read_path_reliability_complete": bool(matrix.get("read_path_reliability_complete")),
        "remote_client_topology_complete": bool(remote_topology.get("complete")),
        "remote_client_per_source_complete": bool(remote_topology.get("per_source_client_complete")),
        "remote_client_topology_report_count": _int_value(remote_topology.get("topology_report_count"), 0),
        "worker_partition_required": bool(matrix.get("worker_partition_required")),
        "worker_partition_complete": bool(matrix.get("worker_partition_complete")),
        "worker_partition_duplicate_source_sample_count": _int_value(matrix.get("worker_partition_duplicate_source_sample_count"), 0),
        "unrecovered_read_failures": _int_value(matrix.get("unrecovered_read_failures"), 0),
        "case_names": [
            str(case.get("case_name") or "")
            for case in cases
        ],
    }


def _trainer_handoff_decision_summary(
    payload: Mapping[str, Any] | None,
    path: str | Path | None,
) -> dict[str, Any] | None:
    decision = _mapping(payload)
    if not decision:
        return None
    return {
        "path": str(path) if path is not None else None,
        "format": decision.get("format"),
        "benchmark_completed": bool(decision.get("benchmark_completed")),
        "trainer_surface_check_passed": decision.get("trainer_surface_check_passed"),
        "trainer_surface_matrix_passed": decision.get("trainer_surface_matrix_passed"),
        "pressure_profile_complete": bool(decision.get("pressure_profile_complete")),
        "kernel_module_allowed": bool(decision.get("kernel_module_allowed")),
        "required_next_evidence": list(decision.get("required_next_evidence", ())),
    }


def _trainer_handoff_blockfs_summary(
    payload: Mapping[str, Any] | None,
    path: str | Path | None,
) -> dict[str, Any] | None:
    blockfs = _mapping(payload)
    if not blockfs:
        return None
    return {
        "path": str(path) if path is not None else None,
        "format": blockfs.get("format"),
        "status": blockfs.get("status"),
        "decision": blockfs.get("decision"),
        "kernel_module_allowed": bool(blockfs.get("kernel_module_allowed")),
        "required_next_evidence": list(blockfs.get("required_next_evidence", ())),
        "missing_readiness_checks": list(blockfs.get("missing_readiness_checks", ())),
    }


def _trainer_handoff_low_level_direct_read_proofs_summary(
    batch_direct_read_payload: Mapping[str, Any] | None,
    batch_direct_read_path: str | Path | None,
    batch_direct_tensor_read_payload: Mapping[str, Any] | None,
    batch_direct_tensor_read_path: str | Path | None,
    *,
    required: bool,
) -> dict[str, Any]:
    batch_direct_read = _trainer_handoff_batch_direct_read_summary(
        batch_direct_read_payload,
        batch_direct_read_path,
    )
    batch_direct_tensor_read = _trainer_handoff_batch_direct_tensor_read_summary(
        batch_direct_tensor_read_payload,
        batch_direct_tensor_read_path,
    )
    missing = []
    if not batch_direct_read["complete"]:
        missing.append("batch_direct_read_proof_complete")
    if not batch_direct_tensor_read["complete"]:
        missing.append("batch_direct_tensor_read_proof_complete")
    return {
        "format": TRAINER_LOW_LEVEL_DIRECT_READ_PROOFS_FORMAT,
        "required": bool(required),
        "complete": not missing,
        "missing": missing,
        "batch_direct_read": batch_direct_read,
        "batch_direct_tensor_read": batch_direct_tensor_read,
    }


def _trainer_handoff_low_level_direct_read_proofs_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    proofs = _mapping(payload.get("low_level_direct_read_proofs"))
    batch_direct_read = _mapping(proofs.get("batch_direct_read"))
    batch_direct_tensor_read = _mapping(proofs.get("batch_direct_tensor_read"))
    missing = []
    if proofs.get("format") != TRAINER_LOW_LEVEL_DIRECT_READ_PROOFS_FORMAT:
        missing.append("low_level_direct_read_proofs_format")
    if not bool(proofs.get("complete")):
        missing.append("low_level_direct_read_proofs_complete")
    if not bool(batch_direct_read.get("complete")):
        missing.append("batch_direct_read_proof_complete")
    if not bool(batch_direct_tensor_read.get("complete")):
        missing.append("batch_direct_tensor_read_proof_complete")
    if batch_direct_read.get("cipher_suite") != CXNFS_TLS_CIPHER_SUITE:
        missing.append("batch_direct_read_cipher_suite")
    if batch_direct_tensor_read.get("cipher_suite") != CXNFS_TLS_CIPHER_SUITE:
        missing.append("batch_direct_tensor_read_cipher_suite")
    return {
        "format": "cxnfs.trainer-low-level-direct-read-proofs-gate.v1",
        "complete": not missing,
        "missing": missing,
        "expected_cipher_suite": CXNFS_TLS_CIPHER_SUITE,
    }


def _trainer_handoff_low_level_direct_write_proofs_summary(
    workload_payload: Mapping[str, Any] | None,
    workload_path: str | Path | None,
    *,
    required: bool,
) -> dict[str, Any]:
    workload = _mapping(workload_payload)
    evidence = _mapping(workload.get("write_path_evidence"))
    gate = _trainer_workload_write_path_gate(workload) if workload else {
        "complete": False,
        "missing": ["workload_report"],
        "requires_quic_transport_evidence": False,
    }
    cipher_counts = _mapping(evidence.get("quic_write_transport_cipher_suite_counts"))
    expected_cipher_count = _int_value(cipher_counts.get(CXNFS_TLS_CIPHER_SUITE), 0)
    evidence_count = _int_value(evidence.get("quic_write_transport_evidence_count"), 0)
    missing = list(gate.get("missing", ()))
    if not workload:
        if "workload_report" not in missing:
            missing.append("workload_report")
    if evidence.get("format") != "cxnfs.workload-write-path-evidence.v1":
        missing.append("write_path_evidence_format")
    if not bool(evidence.get("metadata_pending_observed")):
        missing.append("metadata_pending_observed")
    if not bool(evidence.get("write_ack_frame_identity_verified")):
        missing.append("write_ack_frame_identity_verified")
    if not bool(evidence.get("all_write_probes_verified")):
        missing.append("all_write_probes_verified")
    if not bool(evidence.get("direct_tensor_writes_observed")):
        missing.append("direct_tensor_writes_observed")
    if not bool(evidence.get("quic_write_transport_complete")):
        missing.append("quic_write_transport_complete")
    if not bool(evidence.get("quic_write_transport_all_reports_write_ack_frame_identity_verified")):
        missing.append("quic_write_transport_write_ack_frame_identity_verified")
    if not bool(evidence.get("quic_write_transport_all_reports_write_ack_stream_completion_verified")):
        missing.append("quic_write_transport_write_ack_stream_completion_verified")
    if not bool(evidence.get("quic_write_transport_all_reports_write_ack_stream_eof_verified")):
        missing.append("quic_write_transport_write_ack_stream_eof_verified")
    if _int_value(evidence.get("quic_write_transport_write_ack_verification_failures"), 0) > 0:
        missing.append("quic_write_transport_write_ack_verification_failures")
    if _int_value(evidence.get("quic_write_transport_write_acks_verified"), 0) < max(
        1,
        _int_value(evidence.get("write_frames"), 0),
    ):
        missing.append("quic_write_transport_write_acks_verified")
    attempted_write_streams = _int_value(evidence.get("quic_write_transport_attempted_write_streams"), 0)
    write_ack_streams_eof_verified = _int_value(
        evidence.get("quic_write_transport_write_ack_streams_eof_verified"),
        0,
    )
    if attempted_write_streams > 0 and write_ack_streams_eof_verified != attempted_write_streams:
        missing.append("quic_write_transport_write_ack_streams_eof_verified")
    if evidence_count <= 0:
        missing.append("quic_write_transport_evidence_count")
    if expected_cipher_count < max(1, evidence_count):
        missing.append("quic_write_transport_cipher_suite")
    deduped_missing = _dedupe_strings(str(value) for value in missing if str(value))
    return {
        "format": TRAINER_LOW_LEVEL_DIRECT_WRITE_PROOFS_FORMAT,
        "required": bool(required),
        "complete": not deduped_missing,
        "missing": deduped_missing,
        "workload_path": str(workload_path) if workload_path is not None else None,
        "workload_format": workload.get("format"),
        "write_path_format": evidence.get("format"),
        "workload_name": workload.get("workload_name"),
        "transport": workload.get("transport"),
        "bulk_transport": workload.get("bulk_transport"),
        "run_count": _int_value(evidence.get("run_count"), 0),
        "write_probe_count": _int_value(evidence.get("write_probe_count"), 0),
        "write_probe_cord_sweep_count": _int_value(evidence.get("write_probe_cord_sweep_count"), 0),
        "write_probe_cord_labels": list(evidence.get("write_probe_cord_labels", ()))
        if isinstance(evidence.get("write_probe_cord_labels"), (list, tuple)) else [],
        "all_runs_have_evidence": bool(evidence.get("all_runs_have_evidence")),
        "all_write_probes_verified": bool(evidence.get("all_write_probes_verified")),
        "verified_read_back_count": _int_value(evidence.get("verified_read_back_count"), 0),
        "direct_tensor_write_operations": _int_value(evidence.get("direct_tensor_write_operations"), 0),
        "direct_tensor_writes_observed": bool(evidence.get("direct_tensor_writes_observed")),
        "payload_bytes_written": _int_value(evidence.get("payload_bytes_written"), 0),
        "write_frames": _int_value(evidence.get("write_frames"), 0),
        "write_acks": _int_value(evidence.get("write_acks"), 0),
        "write_ack_frame_verified": _int_value(evidence.get("write_ack_frame_verified"), 0),
        "write_ack_frame_identity_verified": bool(evidence.get("write_ack_frame_identity_verified")),
        "write_ack_metadata_pending": _int_value(evidence.get("write_ack_metadata_pending"), 0),
        "metadata_pending_observed": bool(evidence.get("metadata_pending_observed")),
        "metadata_after_payload_entries": _int_value(evidence.get("metadata_after_payload_entries"), 0),
        "entries_committed": _int_value(evidence.get("entries_committed"), 0),
        "transfers_committed": _int_value(evidence.get("transfers_committed"), 0),
        "max_observed_write_cords": _int_value(evidence.get("max_observed_write_cords"), 0),
        "write_retry_limit": _int_value(evidence.get("write_retry_limit"), 0),
        "write_retry_attempts": _int_value(evidence.get("write_retry_attempts"), 0),
        "write_retry_successes": _int_value(evidence.get("write_retry_successes"), 0),
        "bulk_transport_write_reconnects": _int_value(evidence.get("bulk_transport_write_reconnects"), 0),
        "write_retry_attempts_recovered": bool(evidence.get("write_retry_attempts_recovered", True)),
        "write_retry_reconnects_observed": bool(evidence.get("write_retry_reconnects_observed", True)),
        "quic_write_transport_evidence_count": evidence_count,
        "quic_write_transport_successful_write_operations": _int_value(
            evidence.get("quic_write_transport_successful_write_operations"), 0
        ),
        "quic_write_transport_failed_write_operations": _int_value(
            evidence.get("quic_write_transport_failed_write_operations"), 0
        ),
        "quic_write_transport_attempted_write_streams": attempted_write_streams,
        "quic_write_transport_write_streams_created": _int_value(
            evidence.get("quic_write_transport_write_streams_created"), 0
        ),
        "quic_write_transport_max_active_write_streams": _int_value(
            evidence.get("quic_write_transport_max_active_write_streams"), 0
        ),
        "quic_write_transport_all_reports_reused_connection": bool(
            evidence.get("quic_write_transport_all_reports_reused_connection")
        ),
        "quic_write_transport_write_acks_received": _int_value(
            evidence.get("quic_write_transport_write_acks_received"), 0
        ),
        "quic_write_transport_write_acks_verified": _int_value(
            evidence.get("quic_write_transport_write_acks_verified"), 0
        ),
        "quic_write_transport_write_ack_verification_failures": _int_value(
            evidence.get("quic_write_transport_write_ack_verification_failures"), 0
        ),
        "quic_write_transport_write_ack_identity_report_count": _int_value(
            evidence.get("quic_write_transport_write_ack_identity_report_count"), 0
        ),
        "quic_write_transport_write_ack_stream_completion_report_count": _int_value(
            evidence.get("quic_write_transport_write_ack_stream_completion_report_count"), 0
        ),
        "quic_write_transport_write_ack_stream_eof_report_count": _int_value(
            evidence.get("quic_write_transport_write_ack_stream_eof_report_count"), 0
        ),
        "quic_write_transport_write_ack_streams_eof_verified": write_ack_streams_eof_verified,
        "quic_write_transport_all_reports_write_ack_frame_identity_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_frame_identity_verified")
        ),
        "quic_write_transport_all_reports_write_ack_stream_completion_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_stream_completion_verified")
        ),
        "quic_write_transport_all_reports_write_ack_stream_eof_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_stream_eof_verified")
        ),
        "quic_write_transport_write_ack_metadata_pending": _int_value(
            evidence.get("quic_write_transport_write_ack_metadata_pending"), 0
        ),
        "quic_write_transport_write_ack_metadata_pending_observed": bool(
            evidence.get("quic_write_transport_write_ack_metadata_pending_observed")
        ),
        "quic_write_transport_complete": bool(evidence.get("quic_write_transport_complete")),
        "quic_write_transport_cipher_suite": CXNFS_TLS_CIPHER_SUITE,
        "quic_write_transport_expected_cipher_suite_count": expected_cipher_count,
        "quic_write_transport_cipher_suite_counts": dict(cipher_counts),
        "gate": dict(gate),
    }


def _trainer_handoff_low_level_direct_write_proofs_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    proofs = _mapping(payload.get("low_level_direct_write_proofs"))
    missing = []
    if proofs.get("format") != TRAINER_LOW_LEVEL_DIRECT_WRITE_PROOFS_FORMAT:
        missing.append("low_level_direct_write_proofs_format")
    if not bool(proofs.get("complete")):
        missing.append("low_level_direct_write_proofs_complete")
    if proofs.get("quic_write_transport_cipher_suite") != CXNFS_TLS_CIPHER_SUITE:
        missing.append("quic_write_transport_cipher_suite")
    if _int_value(proofs.get("quic_write_transport_expected_cipher_suite_count"), 0) < max(
        1,
        _int_value(proofs.get("quic_write_transport_evidence_count"), 0),
    ):
        missing.append("quic_write_transport_cipher_suite_count")
    if not bool(proofs.get("all_write_probes_verified")):
        missing.append("all_write_probes_verified")
    if not bool(proofs.get("metadata_pending_observed")):
        missing.append("metadata_pending_observed")
    if not bool(proofs.get("write_ack_frame_identity_verified")):
        missing.append("write_ack_frame_identity_verified")
    if not bool(proofs.get("direct_tensor_writes_observed")):
        missing.append("direct_tensor_writes_observed")
    if not bool(proofs.get("quic_write_transport_complete")):
        missing.append("quic_write_transport_complete")
    if not bool(proofs.get("quic_write_transport_all_reports_write_ack_frame_identity_verified")):
        missing.append("quic_write_transport_write_ack_frame_identity_verified")
    if not bool(proofs.get("quic_write_transport_all_reports_write_ack_stream_completion_verified")):
        missing.append("quic_write_transport_write_ack_stream_completion_verified")
    if not bool(proofs.get("quic_write_transport_all_reports_write_ack_stream_eof_verified")):
        missing.append("quic_write_transport_write_ack_stream_eof_verified")
    if _int_value(proofs.get("quic_write_transport_write_ack_verification_failures"), 0) > 0:
        missing.append("quic_write_transport_write_ack_verification_failures")
    return {
        "format": "cxnfs.trainer-low-level-direct-write-proofs-gate.v1",
        "complete": not missing,
        "missing": missing,
        "expected_cipher_suite": CXNFS_TLS_CIPHER_SUITE,
    }


def _trainer_handoff_batch_direct_read_summary(
    payload: Mapping[str, Any] | None,
    path: str | Path | None,
) -> dict[str, Any]:
    report = _mapping(payload)
    complete = (
        report.get("format") == "cxnfs.quic-batch-direct-read.v1"
        and bool(report.get("ordered_batch_read_verified"))
        and _int_value(report.get("direct_buffer_read_operations"), 0) >= 2
        and _int_value(report.get("direct_destination_read_operations"), 0) >= 2
        and bool(report.get("frame_sink_used_for_all_direct_destination_reads"))
        and bool(report.get("direct_destination_frame_sink_cord_fanout_verified"))
    )
    return {
        "path": str(path) if path is not None else None,
        "format": report.get("format"),
        "complete": complete,
        "transport": report.get("transport"),
        "bulk_transport": report.get("bulk_transport"),
        "tls_version": report.get("tls_version"),
        "cipher_suite": report.get("cipher_suite"),
        "batch_read_count": _int_value(report.get("batch_read_count"), 0),
        "batch_worker_count": _int_value(report.get("batch_worker_count"), 0),
        "ordered_batch_read_verified": bool(report.get("ordered_batch_read_verified")),
        "direct_buffer_read_operations": _int_value(report.get("direct_buffer_read_operations"), 0),
        "direct_destination_read_operations": _int_value(report.get("direct_destination_read_operations"), 0),
        "transport_frame_sink_observed": bool(report.get("transport_frame_sink_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(report.get("frame_sink_used_for_all_direct_destination_reads")),
        "direct_destination_frame_sink_cord_fanout_verified": bool(report.get("direct_destination_frame_sink_cord_fanout_verified")),
        "max_observed_cords": _int_value(report.get("max_observed_cords"), 0),
        "direct_destination_max_observed_cords": _int_value(report.get("direct_destination_max_observed_cords"), 0),
    }


def _trainer_handoff_batch_direct_tensor_read_summary(
    payload: Mapping[str, Any] | None,
    path: str | Path | None,
) -> dict[str, Any]:
    report = _mapping(payload)
    complete = (
        report.get("format") == "cxnfs.quic-batch-direct-tensor-read.v1"
        and bool(report.get("ordered_tensor_read_verified"))
        and _int_value(report.get("direct_tensor_read_operations"), 0) >= 2
        and bool(report.get("network_fills_tensor_destination"))
        and bool(report.get("frame_sink_used_for_all_direct_destination_reads"))
        and bool(report.get("direct_destination_frame_sink_cord_fanout_verified"))
    )
    return {
        "path": str(path) if path is not None else None,
        "format": report.get("format"),
        "complete": complete,
        "transport": report.get("transport"),
        "bulk_transport": report.get("bulk_transport"),
        "tls_version": report.get("tls_version"),
        "cipher_suite": report.get("cipher_suite"),
        "tensor_target": report.get("tensor_target"),
        "tensor_read_count": _int_value(report.get("tensor_read_count"), 0),
        "tensor_worker_count": _int_value(report.get("tensor_worker_count"), 0),
        "ordered_tensor_read_verified": bool(report.get("ordered_tensor_read_verified")),
        "direct_tensor_read_operations": _int_value(report.get("direct_tensor_read_operations"), 0),
        "network_fills_tensor_destination": bool(report.get("network_fills_tensor_destination")),
        "transport_frame_sink_observed": bool(report.get("transport_frame_sink_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(report.get("frame_sink_used_for_all_direct_destination_reads")),
        "direct_destination_frame_sink_cord_fanout_verified": bool(report.get("direct_destination_frame_sink_cord_fanout_verified")),
        "max_observed_cords": _int_value(report.get("max_observed_cords"), 0),
        "direct_destination_max_observed_cords": _int_value(report.get("direct_destination_max_observed_cords"), 0),
    }


def _trainer_handoff_integration_surface(contract_payload: Mapping[str, Any]) -> dict[str, Any]:
    python_surface = _mapping(contract_payload.get("python_surface"))
    read_contract = _mapping(contract_payload.get("read_contract"))
    return {
        "python_client": "CxnfsWeightedTorchDataset",
        "batch_object": python_surface.get("batch_object") or "CxnfsTrainerBatch",
        "dataloader_batch_object": python_surface.get("dataloader_batch_object") or "CxnfsTrainerBatch",
        "stream_object": python_surface.get("stream_object") or "CxnfsTrainerStream",
        "stream_state_format": python_surface.get("stream_state_format") or TRAINER_STREAM_STATE_FORMAT,
        "resume_cursor_format": TRAINER_CURSOR_FORMAT,
        "supports_weighted_sources": bool(python_surface.get("supports_weighted_sources")),
        "supports_per_source_clients": bool(python_surface.get("supports_per_source_clients")),
        "supports_direct_buffers": bool(python_surface.get("supports_direct_buffers")),
        "supports_direct_tensors": bool(python_surface.get("supports_direct_tensors")),
        "supports_trainer_stream": bool(python_surface.get("supports_trainer_stream")),
        "supports_trainer_prefetch_stream": bool(python_surface.get("supports_trainer_prefetch_stream")),
        "supports_trainer_dataloader": bool(python_surface.get("supports_trainer_dataloader")),
        "supports_dataloader_worker_partitioning": bool(python_surface.get("supports_dataloader_worker_partitioning")),
        "supports_training_pipeline": bool(python_surface.get("supports_training_pipeline")),
        "training_pipeline_object": python_surface.get("training_pipeline_object") or "CxnfsTrainingPipeline",
        "supports_pipeline_direct_tensor_batches": bool(python_surface.get("supports_pipeline_direct_tensor_batches")),
        "supports_pipeline_gpu_transforms": bool(python_surface.get("supports_pipeline_gpu_transforms")),
        "supports_pipeline_torch_compile": bool(python_surface.get("supports_pipeline_torch_compile")),
        "pipeline_cpu_augmentation_default": (
            python_surface.get("pipeline_cpu_augmentation_default")
            or "disabled-with-explicit-opt-in"
        ),
        "destination_owner": python_surface.get("destination_owner"),
        "read_mode": read_contract.get("read_mode"),
        "trainer_batch_iterator": read_contract.get("trainer_batch_iterator"),
        "stream_kwargs": _trainer_handoff_stream_kwargs(contract_payload),
    }


def _trainer_handoff_integration_plan(contract_payload: Mapping[str, Any]) -> dict[str, Any]:
    network = _mapping(contract_payload.get("network_contract"))
    stream_kwargs = _trainer_handoff_stream_kwargs(contract_payload)
    remote_root = stream_kwargs.get("remote_root")
    return {
        "format": TRAINER_INTEGRATION_PLAN_FORMAT,
        "stream_factory": "open_weighted_mds_trainer_stream",
        "plan_object": "CxnfsTrainerIntegrationPlan",
        "remote_client_factory": "CxnfsTrainerIntegrationPlan.create_remote_client_factory" if remote_root else None,
        "remote_stream_context": "CxnfsTrainerIntegrationPlan.open_remote_stream" if remote_root else None,
        "batch_object": "CxnfsTrainerBatch",
        "stream_object": "CxnfsTrainerStream",
        "training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop",
        "pipeline_training_loop_context": "CxnfsTrainerIntegrationPlan.open_pipeline_training_loop",
        "supports_training_loop_prefetch": True,
        "training_pipeline_object": "CxnfsTrainingPipeline",
        "training_pipeline_context": "CxnfsTrainingPipeline.read_tensor_batch",
        "supports_pipeline_direct_tensor_batches": True,
        "supports_pipeline_gpu_transforms": True,
        "supports_pipeline_torch_compile": True,
        "pipeline_cpu_augmentation_default": "disabled-with-explicit-opt-in",
        "supports_resume_cursor": True,
        "supports_resume_state_file": True,
        "resume_state_format": TRAINER_STREAM_STATE_FORMAT,
        "stream_kwargs": stream_kwargs,
        "trainer_loop_options": {
            "trainer_prefetch_batches": _int_value(
                _mapping(contract_payload.get("read_contract")).get("trainer_prefetch_batches"),
                0,
            ),
        },
        "remote_client_required": bool(remote_root),
        "remote_client_kwargs": {
            "bulk_transport": network.get("bulk_transport") or PYTHON_BULK_TRANSPORT_STREAM,
            "initial_cords": _int_value(network.get("initial_cords"), 1),
            "bulk_read_retries": _int_value(network.get("bulk_read_retries"), 0),
            "bulk_write_retries": _int_value(network.get("bulk_write_retries"), 0),
            "max_cords": _int_value(network.get("max_cords"), ABSOLUTE_MAX_CORDS),
            "provisional_container_size_bytes": _int_value(
                network.get("provisional_container_size_bytes"),
                CxnfsBulkReadConfig.provisional_container_size_bytes,
            ),
            "expected_size_limit_bytes": _int_value(network.get("expected_size_limit_bytes"), 0),
            "write_connections": _optional_int_value(network.get("write_connections")),
            "shared_client": _bool_value(network.get("shared_client"), False),
            "raw_password_sent_over_control": bool(network.get("raw_password_sent_over_control")),
            "per_source_client_default": bool(network.get("per_source_client_default", True)),
        } if remote_root else {},
        "required_imports": [
            "CxnfsTrainerIntegrationPlan",
            "CxnfsTrainerPipelineLoop",
            "CxnfsTrainingPipeline",
            "CxnfsPipelineConfig",
            "AugmentationPlacement",
            "open_weighted_mds_trainer_stream",
            "prompt_client_credentials",
        ] if remote_root else [
            "CxnfsTrainerPipelineLoop",
            "CxnfsTrainingPipeline",
            "CxnfsPipelineConfig",
            "AugmentationPlacement",
            "open_weighted_mds_trainer_stream",
        ],
        "requires_mount": False,
        "touches_trainer_code": False,
    }


def _trainer_handoff_stream_kwargs(contract_payload: Mapping[str, Any]) -> dict[str, Any]:
    read_contract = _mapping(contract_payload.get("read_contract"))
    kwargs: dict[str, Any] = {
        "root": contract_payload.get("root"),
        "weights": dict(_mapping(contract_payload.get("weights"))),
        "batch_size": _int_value(read_contract.get("batch_size"), 0),
        "remote_root": contract_payload.get("remote_root"),
        "read_mode": read_contract.get("read_mode") or TrainerReadMode.DIRECT_BUFFERS.value,
        "rounds_per_read": _int_value(read_contract.get("rounds_per_read"), 1),
        "max_workers": _int_value(read_contract.get("max_workers"), 16),
        "source_read_workers": _optional_int_value(read_contract.get("source_read_workers")),
        "prefetch_chunks": _int_value(read_contract.get("prefetch_chunks"), 1),
        "samples_per_epoch": _optional_int_value(read_contract.get("samples_per_epoch")),
        "repeat": _bool_value(read_contract.get("repeat"), True),
        "shuffle": bool(read_contract.get("shuffle")),
        "seed": _optional_int_value(read_contract.get("seed")),
        "epoch": _int_value(read_contract.get("epoch"), 0),
        "trainer_batch_iterator": read_contract.get("trainer_batch_iterator") or TRAINER_BATCH_ITERATOR_NATIVE,
        "data_loader_workers": _int_value(read_contract.get("data_loader_workers"), 0),
    }
    training_shard = _mapping(read_contract.get("training_shard"))
    if training_shard:
        kwargs["shard"] = {
            "index": _int_value(training_shard.get("index"), 0),
            "count": _int_value(training_shard.get("count"), 1),
        }
    tensor_allocation = _mapping(read_contract.get("tensor_allocation"))
    if tensor_allocation:
        kwargs["tensor_allocation"] = dict(tensor_allocation)
        kwargs["non_blocking"] = _bool_value(tensor_allocation.get("non_blocking"), True)
    return kwargs


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def run_trainer_contract_benchmark_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Run a weighted trainer workload benchmark from a CXNFS trainer contract.")
    parser.add_argument("contract", help="Path to a cxnfs.trainer-contract.v1 JSON report.")
    parser.add_argument("--root", help="Override the contract MDS root.")
    parser.add_argument("--weights", help="Override the contract source weights.")
    parser.add_argument("--workload-name")
    parser.add_argument("--remote-root", help="Override the contract remote root.")
    parser.add_argument("--host")
    parser.add_argument("--control-port", type=int)
    parser.add_argument("--transport-port", type=int)
    parser.add_argument("--bulk-transport")
    parser.add_argument("--server-hostname")
    parser.add_argument("--cafile")
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--initial-cords", type=int)
    parser.add_argument("--max-cords", type=int)
    parser.add_argument(
        "--cord-sweep",
        help="Comma-separated fixed cord counts to test, or 'auto' for 1/8/16/32/64/128 capped by --max-cords.",
    )
    parser.add_argument("--provisional-container-size", type=int)
    parser.add_argument("--expected-size-limit", type=int)
    parser.add_argument("--write-connections", type=int)
    parser.add_argument("--bulk-read-retries", type=int)
    parser.add_argument("--bulk-write-retries", type=int)
    parser.add_argument("--write-probe-bytes", type=int)
    parser.add_argument("--write-probe-frame-bytes", type=int)
    parser.add_argument("--write-probe-root")
    parser.add_argument("--shared-client", action="store_true")
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--batches", type=int)
    parser.add_argument("--warmup-batches", type=int)
    parser.add_argument("--runs", type=int)
    parser.add_argument("--rounds-per-read", type=int)
    parser.add_argument("--max-workers", type=int)
    parser.add_argument("--source-read-workers", type=int)
    parser.add_argument("--prefetch-chunks", type=int)
    parser.add_argument("--trainer-prefetch-batches", type=int)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--no-shuffle", action="store_true")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--epoch", type=int)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_trainer_repeat_args(parser, default=None)
    parser.add_argument("--decoder")
    parser.add_argument("--read-mode", choices=tuple(mode.value for mode in TrainerReadMode))
    parser.add_argument("--trainer-batch-iterator", choices=TRAINER_BATCH_ITERATORS)
    parser.add_argument("--data-loader-workers", type=int)
    _add_trainer_tensor_allocation_args(parser, default_values=False)
    parser.add_argument("--objective", choices=[AI_TRAINING_25GBE_OBJECTIVE])
    parser.add_argument("--baseline-mb-s", type=float)
    parser.add_argument("--fail-on-miss", action="store_true")
    parser.add_argument("--include-run-batches", action="store_true")
    parser.add_argument("--blockfs-min-runs", type=int, help="Minimum workload runs required before a blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-samples", type=int, help="Minimum workload samples required before a blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-bytes", type=int, help="Minimum workload bytes required before a blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-cord-sweep", type=int, help="Minimum cord sweep entries required before a blockfs gate can allow kernel work.")
    parser.add_argument("--server-disk-evidence", help="Path to server disk evidence in JSON or key=value format.")
    parser.add_argument("--server-cache-evidence", help="Path to server cache evidence in JSON or key=value format.")
    parser.add_argument("--server-cpu-evidence", help="Path to server CPU evidence in JSON or key=value format.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        contract_path, contract_sha256, contract = _read_trainer_contract(args.contract)
        benchmark_argv = _trainer_contract_benchmark_argv(args, contract)
        benchmark_stdout = io.StringIO()
        benchmark_stderr = io.StringIO()
        exit_code = run_trainer_workload_benchmark_cli(
            benchmark_argv,
            stdout=benchmark_stdout,
            stderr=benchmark_stderr,
            credential_prompt=credential_prompt,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        if benchmark_stderr.getvalue():
            err.write(benchmark_stderr.getvalue())
        if not benchmark_stdout.getvalue().strip():
            return exit_code or 1
        payload = json.loads(benchmark_stdout.getvalue())
        payload["contract_applied"] = True
        payload["trainer_contract"] = _trainer_contract_provenance(
            contract_path,
            contract_sha256,
            contract,
        )
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        return exit_code
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_contract_benchmark() -> None:
    raise SystemExit(run_trainer_contract_benchmark_cli())


def _read_trainer_contract(path: str | Path) -> tuple[Path, str, Mapping[str, Any]]:
    contract_path = Path(path)
    raw = contract_path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"trainer contract must be a JSON object: {contract_path}")
    if payload.get("format") != TRAINER_CONTRACT_FORMAT:
        raise ValueError(f"unsupported trainer contract format: {payload.get('format')}")
    return contract_path, hashlib.sha256(raw).hexdigest(), payload


def _trainer_contract_benchmark_argv(args: Any, contract: Mapping[str, Any]) -> list[str]:
    read_contract = _mapping(contract.get("read_contract"))
    network_contract = _mapping(contract.get("network_contract"))
    workload_requirements = _mapping(contract.get("workload_requirements"))
    root = args.root or contract.get("root")
    if not root:
        raise ValueError("trainer contract is missing root")
    weights = args.weights or _weights_spec_from_mapping(_mapping(contract.get("weights")))
    if not weights:
        raise ValueError("trainer contract is missing weights")
    argv = [str(root), "--weights", str(weights)]

    def add(option: str, value: Any) -> None:
        if value is not None:
            argv.extend([option, str(value)])

    add("--workload-name", args.workload_name)
    remote_root = args.remote_root if args.remote_root is not None else contract.get("remote_root")
    add("--remote-root", remote_root)
    add("--host", args.host)
    add("--control-port", args.control_port)
    add("--transport-port", args.transport_port)
    add("--bulk-transport", args.bulk_transport or network_contract.get("bulk_transport"))
    add("--server-hostname", args.server_hostname)
    add("--cafile", args.cafile)
    add("--timeout", args.timeout)
    add("--initial-cords", args.initial_cords)
    add("--max-cords", args.max_cords if args.max_cords is not None else network_contract.get("max_cords"))
    add("--cord-sweep", args.cord_sweep)
    add("--provisional-container-size", args.provisional_container_size)
    add("--expected-size-limit", args.expected_size_limit)
    add("--write-connections", args.write_connections)
    bulk_read_retries = getattr(args, "bulk_read_retries", None)
    add("--bulk-read-retries", bulk_read_retries if bulk_read_retries is not None else network_contract.get("bulk_read_retries"))
    bulk_write_retries = getattr(args, "bulk_write_retries", None)
    add("--bulk-write-retries", bulk_write_retries if bulk_write_retries is not None else network_contract.get("bulk_write_retries"))
    add("--write-probe-bytes", args.write_probe_bytes)
    add("--write-probe-frame-bytes", args.write_probe_frame_bytes)
    add("--write-probe-root", args.write_probe_root)
    add("--batch-size", args.batch_size if args.batch_size is not None else read_contract.get("batch_size"))
    add("--batches", args.batches if args.batches is not None else read_contract.get("requested_batches"))
    add("--warmup-batches", args.warmup_batches)
    add("--runs", args.runs)
    add("--rounds-per-read", args.rounds_per_read if args.rounds_per_read is not None else read_contract.get("rounds_per_read"))
    add("--max-workers", args.max_workers if args.max_workers is not None else read_contract.get("max_workers"))
    add("--source-read-workers", args.source_read_workers if args.source_read_workers is not None else read_contract.get("source_read_workers"))
    add("--prefetch-chunks", args.prefetch_chunks if args.prefetch_chunks is not None else read_contract.get("prefetch_chunks"))
    trainer_prefetch_batches = getattr(args, "trainer_prefetch_batches", None)
    add("--trainer-prefetch-batches", trainer_prefetch_batches if trainer_prefetch_batches is not None else read_contract.get("trainer_prefetch_batches"))
    add("--seed", args.seed if args.seed is not None else read_contract.get("seed"))
    add("--epoch", args.epoch if args.epoch is not None else read_contract.get("epoch"))
    add("--samples-per-epoch", args.samples_per_epoch if args.samples_per_epoch is not None else read_contract.get("samples_per_epoch"))
    add("--decoder", args.decoder)
    add("--read-mode", args.read_mode or read_contract.get("read_mode"))
    add("--trainer-batch-iterator", args.trainer_batch_iterator or read_contract.get("trainer_batch_iterator"))
    add("--data-loader-workers", args.data_loader_workers if args.data_loader_workers is not None else read_contract.get("data_loader_workers"))
    tensor_allocation = _trainer_tensor_allocation_from_args(args, _mapping(read_contract.get("tensor_allocation")))
    add("--tensor-target", tensor_allocation.target.value)
    add("--tensor-dtype", tensor_allocation.dtype)
    add("--tensor-device", tensor_allocation.device)
    add("--tensor-non-blocking", str(tensor_allocation.non_blocking).lower())
    add("--objective", args.objective)
    add("--baseline-mb-s", args.baseline_mb_s)
    add("--blockfs-min-runs", args.blockfs_min_runs if args.blockfs_min_runs is not None else workload_requirements.get("min_workload_runs"))
    add("--blockfs-min-samples", args.blockfs_min_samples if args.blockfs_min_samples is not None else workload_requirements.get("min_workload_samples"))
    add("--blockfs-min-bytes", args.blockfs_min_bytes if args.blockfs_min_bytes is not None else workload_requirements.get("min_workload_bytes"))
    add("--blockfs-min-cord-sweep", args.blockfs_min_cord_sweep if args.blockfs_min_cord_sweep is not None else workload_requirements.get("min_cord_sweep"))
    add("--server-disk-evidence", args.server_disk_evidence)
    add("--server-cache-evidence", args.server_cache_evidence)
    add("--server-cpu-evidence", args.server_cpu_evidence)
    if args.shared_client:
        argv.append("--shared-client")
    if args.shuffle or (read_contract.get("shuffle") and not args.no_shuffle):
        argv.append("--shuffle")
    repeat_override = getattr(args, "repeat", None)
    if repeat_override is None:
        repeat_override = _bool_value(read_contract.get("repeat"), True)
    argv.append("--repeat" if repeat_override else "--no-repeat")
    if args.fail_on_miss:
        argv.append("--fail-on-miss")
    if args.include_run_batches:
        argv.append("--include-run-batches")
    return argv


def _weights_spec_from_mapping(weights: Mapping[str, Any]) -> str:
    parts = []
    for name, value in weights.items():
        parts.append(f"{name}={int(value)}")
    return ",".join(parts)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json_mapping(path: Path, label: str) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def _write_json_mapping_atomic(path: Path, payload: Mapping[str, Any], *, compact: bool = False) -> None:
    text = json.dumps(payload, indent=None if compact else 2, sort_keys=True) + "\n"
    _write_text_atomic(path, text)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{threading.get_ident()}-{time.time_ns()}")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_parent_directory(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _fsync_parent_directory(path: Path) -> None:
    try:
        directory_fd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        return
    finally:
        os.close(directory_fd)


def _int_value(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_int_value(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _validate_non_negative_thresholds(**thresholds: int) -> None:
    for name, value in thresholds.items():
        if value < 0:
            raise ValueError(f"{name.replace('_', '-')} must be non-negative")


def _validate_trainer_batch_iterator_args(args: Any) -> None:
    workers = int(getattr(args, "data_loader_workers", 0) or 0)
    if workers < 0:
        raise ValueError("data-loader-workers must be non-negative")
    iterator = getattr(args, "trainer_batch_iterator", TRAINER_BATCH_ITERATOR_NATIVE)
    if iterator != TRAINER_BATCH_ITERATOR_DATALOADER and workers:
        raise ValueError("--data-loader-workers requires --trainer-batch-iterator trainer-dataloader")


def _validate_trainer_prefetch_batches(value: int | None) -> int:
    active = int(value or 0)
    if active < 0:
        raise ValueError("trainer-prefetch-batches must be non-negative")
    return active


def _validate_pipeline_prefetch_batches(value: int | None) -> int:
    active = int(value or 0)
    if active < 0:
        raise ValueError("pipeline-prefetch-batches must be non-negative")
    return active


def _validate_bulk_read_retries(value: int | None) -> int:
    return CxnfsBulkRetryConfig(read_retries=int(value or 0)).read_retries


def _validate_bulk_write_retries(value: int | None) -> int:
    return CxnfsBulkRetryConfig(write_retries=int(value or 0)).write_retries


def _trainer_contract_workload_requirements_from_values(
    *,
    min_workload_runs: int,
    min_workload_samples: int,
    min_workload_bytes: int,
    min_cord_sweep: int,
) -> dict[str, Any]:
    configured = all(value > 0 for value in (
        min_workload_runs,
        min_workload_samples,
        min_workload_bytes,
        min_cord_sweep,
    ))
    return {
        "format": "cxnfs.trainer-workload-requirements.v1",
        "configured": configured,
        "min_workload_runs": min_workload_runs,
        "min_workload_samples": min_workload_samples,
        "min_workload_bytes": min_workload_bytes,
        "min_cord_sweep": min_cord_sweep,
    }


def _weighted_plan_source_count(plan: Mapping[str, Any]) -> int:
    sources = plan.get("sources")
    return len(sources) if isinstance(sources, (list, tuple)) else 0


def _workload_cord_sweep_count(workload: Mapping[str, Any]) -> int:
    network_summary = _mapping(workload.get("network_path_summary"))
    if "cord_sweep_count" in network_summary:
        return _int_value(network_summary.get("cord_sweep_count"), 0)
    cord_sweep = workload.get("cord_sweep")
    return len(cord_sweep) if isinstance(cord_sweep, (list, tuple)) else 0


def _workload_max_requested_cords(workload: Mapping[str, Any]) -> int:
    maximum = 0
    cord_sweep = workload.get("cord_sweep")
    if isinstance(cord_sweep, (list, tuple)):
        for entry in cord_sweep:
            data = _mapping(entry)
            initial = _int_value(data.get("initial_cords"), 0)
            requested_maximum = _int_value(data.get("max_cords"), initial)
            maximum = max(maximum, initial, requested_maximum)
    return maximum


def _required_observed_cords_for_gate(workload: Mapping[str, Any]) -> int:
    requirements = _mapping(workload.get("blockfs_workload_requirements"))
    if bool(requirements.get("configured")) and _int_value(requirements.get("min_cord_sweep"), 0) >= 5:
        return max(2, min(ABSOLUTE_MAX_CORDS, _workload_max_requested_cords(workload) or 2))
    return 2


def _cord_sweep_pairs(value: Any) -> tuple[tuple[int, int], ...]:
    if not isinstance(value, (list, tuple)):
        return tuple()
    pairs = []
    for entry in value:
        data = _mapping(entry)
        pairs.append((_int_value(data.get("initial_cords"), -1), _int_value(data.get("max_cords"), -1)))
    return tuple(pairs)


def _is_sha256_fingerprint(value: Mapping[str, Any]) -> bool:
    if value.get("algorithm") != "sha256":
        return False
    digest = value.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        return False
    try:
        int(digest, 16)
    except ValueError:
        return False
    return True


def _workload_fingerprint_from_payload(
    payload: Mapping[str, Any],
    weighted_plan: Mapping[str, Any],
) -> dict[str, Any] | None:
    existing = payload.get("workload_fingerprint")
    if isinstance(existing, Mapping):
        return dict(existing)
    if weighted_plan:
        return weighted_mds_plan_fingerprint(weighted_plan)
    return None


def _fingerprint_sha256(value: Mapping[str, Any] | None) -> str | None:
    if not isinstance(value, Mapping):
        return None
    digest = value.get("sha256")
    return str(digest) if digest else None


def _trainer_contract_provenance(
    path: Path,
    sha256: str,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    validation = _mapping(contract.get("validation"))
    return {
        "path": str(path),
        "sha256": sha256,
        "format": contract.get("format"),
        "root": contract.get("root"),
        "remote_root": contract.get("remote_root"),
        "workload_fingerprint": _mapping(contract.get("workload_fingerprint")) or None,
        "workload_requirements": _mapping(contract.get("workload_requirements")) or None,
        "validated": bool(validation.get("validated")),
        "validation_batch_count": int(validation.get("batch_count", 0) or 0),
        "validation_sample_count": int(validation.get("sample_count", 0) or 0),
        "validation_byte_size": int(validation.get("byte_size", 0) or 0),
    }


def run_trainer_stream_smoke_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Smoke-test weighted CXNFS MDS-Shard batches for trainer integration.")
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--remote-root", help="Remote CXNFS namespace root containing the same source directories.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--bulk-transport", default="stream")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    parser.add_argument("--cafile", help="optional CA bundle for the server certificate")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, default=1)
    parser.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    parser.add_argument("--provisional-container-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--expected-size-limit", type=int, default=0)
    parser.add_argument("--write-connections", type=int)
    parser.add_argument("--bulk-read-retries", type=int, default=0)
    parser.add_argument("--shared-client", action="store_true")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--warmup-batches", type=int, default=0)
    parser.add_argument("--rounds-per-read", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--source-read-workers", type=int)
    parser.add_argument("--prefetch-chunks", type=int, default=1)
    parser.add_argument("--trainer-prefetch-batches", type=int, default=0)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--epoch", type=int, default=0)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_training_shard_args(parser)
    _add_trainer_repeat_args(parser, default=True)
    parser.add_argument("--decoder", default=MdsSampleFormat.RAW.value)
    parser.add_argument("--read-mode", default=TrainerReadMode.SAMPLES.value, choices=tuple(mode.value for mode in TrainerReadMode))
    parser.add_argument("--trainer-batch-iterator", default=TRAINER_BATCH_ITERATOR_NATIVE, choices=TRAINER_BATCH_ITERATORS)
    parser.add_argument("--data-loader-workers", type=int, default=0)
    _add_trainer_tensor_allocation_args(parser, default_values=True)
    parser.add_argument("--objective", choices=[AI_TRAINING_25GBE_OBJECTIVE])
    parser.add_argument("--baseline-mb-s", type=float)
    parser.add_argument("--fail-on-miss", action="store_true")
    parser.add_argument("--resume-cursor", help="Path to a cxnfs.trainer-cursor.v1 JSON cursor to resume from.")
    parser.add_argument("--write-resume-cursor", help="Optional path to write the final resume cursor JSON.")
    parser.add_argument("--resume-state", help="Path to a cxnfs.trainer-stream-state.v1 JSON state to resume from.")
    parser.add_argument("--write-resume-state", help="Optional path to write the final trainer stream state JSON.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        if args.batches <= 0:
            raise ValueError("batches must be positive")
        if args.warmup_batches < 0:
            raise ValueError("warmup-batches must be non-negative")
        _validate_trainer_prefetch_batches(args.trainer_prefetch_batches)
        args.bulk_read_retries = _validate_bulk_read_retries(args.bulk_read_retries)
        _validate_trainer_batch_iterator_args(args)
        if args.objective and args.baseline_mb_s is None:
            raise ValueError("--objective requires --baseline-mb-s")
        if args.baseline_mb_s is not None and args.baseline_mb_s <= 0:
            raise ValueError("--baseline-mb-s must be positive")
        if args.resume_cursor and args.resume_state:
            raise ValueError("--resume-cursor and --resume-state cannot be used together")
        bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        remote_options = _trainer_stream_remote_options(args)
        read_config = CxnfsBulkReadConfig(
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size_bytes=args.provisional_container_size,
            expected_size_limit_bytes=args.expected_size_limit,
        )
        if args.remote_root:
            tls_context = tls_context_factory(args.cafile) if args.cafile else None
            server_hostname = args.server_hostname or args.host
            with credential_prompt() as credentials:
                client_factory = _trainer_stream_remote_client_factory(
                    credentials,
                    args.host,
                    args.control_port,
                    args.transport_port,
                    args.timeout,
                    tls_context,
                    server_hostname,
                    read_config,
                    args.write_connections,
                    args.bulk_read_retries,
                    None,
                    args.shared_client,
                    bulk_transport,
                    args.cafile,
                    control_client_factory,
                    bulk_client_factory,
                    tls_context_factory,
                )
                sources = weighted_mds_sources_from_root(
                    args.root,
                    args.weights,
                    decoder=args.decoder,
                    client_factory=client_factory,
                    remote_root=args.remote_root,
                )
                payload = _run_trainer_stream_smoke(
                    args,
                    sources,
                    remote_options,
                    client_factory=client_factory,
                )
        else:
            sources = weighted_mds_sources_from_root(
                args.root,
                args.weights,
                decoder=args.decoder,
            )
            payload = _run_trainer_stream_smoke(args, sources, remote_options)

        _apply_trainer_stream_objective(payload, args.objective, args.baseline_mb_s)
        if args.write_resume_cursor:
            cursor_payload = _mapping(payload.get("final_cursor"))
            if not cursor_payload:
                raise ValueError("trainer stream did not produce a resume cursor")
            _write_json_mapping_atomic(Path(args.write_resume_cursor), cursor_payload)
        if args.write_resume_state:
            state_payload = _mapping(payload.get("stream_state"))
            if not state_payload:
                raise ValueError("trainer stream did not produce a stream state")
            _write_json_mapping_atomic(Path(args.write_resume_state), state_payload)
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            _write_text_atomic(Path(args.output), text)
        out.write(text)
        if args.fail_on_miss and payload.get("objective_passed") is False:
            return 1
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_stream_smoke() -> None:
    raise SystemExit(run_trainer_stream_smoke_cli())


def run_trainer_pipeline_smoke_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Smoke-test weighted CXNFS MDS reads through CxnfsTrainingPipeline tensor destinations.")
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--remote-root", help="Remote CXNFS namespace root containing the same source directories.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--bulk-transport", default="stream")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    parser.add_argument("--cafile", help="optional CA bundle for the server certificate")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, default=1)
    parser.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    parser.add_argument("--provisional-container-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--expected-size-limit", type=int, default=0)
    parser.add_argument("--write-connections", type=int)
    parser.add_argument("--bulk-read-retries", type=int, default=0)
    parser.add_argument("--bulk-write-retries", type=int)
    parser.add_argument("--write-probe-bytes", type=int, default=0, help="Optional direct tensor write probe size under the pipeline root.")
    parser.add_argument("--write-probe-frame-bytes", type=int, default=0, help="Optional write-probe-only bulk frame size.")
    parser.add_argument("--write-probe-root", default="__cxnfs-pipeline-write-probe", help="Relative path under the pipeline root for write probe files.")
    parser.add_argument("--shared-client", action="store_true")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--warmup-batches", type=int, default=0)
    parser.add_argument("--rounds-per-read", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--source-read-workers", type=int)
    parser.add_argument("--prefetch-chunks", type=int, default=1)
    parser.add_argument("--pipeline-workers", type=int)
    parser.add_argument("--pipeline-prefetch-batches", type=int, default=0, help="Use CxnfsTrainingPipeline.prefetch_trainer_stream with this many prefetched tensor batches.")
    parser.add_argument("--pipeline-max-inflight-bytes", type=int, default=1024 * 1024 * 1024)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--epoch", type=int, default=0)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_training_shard_args(parser)
    _add_trainer_repeat_args(parser, default=True)
    _add_trainer_tensor_allocation_args(parser, default_values=True)
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        if args.batches <= 0:
            raise ValueError("batches must be positive")
        if args.warmup_batches < 0:
            raise ValueError("warmup-batches must be non-negative")
        args.bulk_read_retries = _validate_bulk_read_retries(args.bulk_read_retries)
        args.bulk_write_retries = _validate_bulk_write_retries(
            args.bulk_read_retries if args.bulk_write_retries is None else args.bulk_write_retries
        )
        if args.write_probe_bytes < 0:
            raise ValueError("--write-probe-bytes must be non-negative")
        if args.write_probe_frame_bytes < 0:
            raise ValueError("--write-probe-frame-bytes must be non-negative")
        if args.write_probe_frame_bytes > 0 and args.write_probe_bytes <= 0:
            raise ValueError("--write-probe-frame-bytes requires --write-probe-bytes")
        args.pipeline_prefetch_batches = _validate_pipeline_prefetch_batches(args.pipeline_prefetch_batches)
        bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        remote_options = _trainer_stream_remote_options(args)
        pipeline_workers = args.pipeline_workers or args.max_workers
        if pipeline_workers <= 0:
            raise ValueError("pipeline-workers must be positive")
        pipeline_config = CxnfsPipelineConfig(
            network_read_workers=min(pipeline_workers, args.max_cords),
            max_cords=args.max_cords,
            prefetch_batches=args.pipeline_prefetch_batches or CxnfsPipelineConfig().prefetch_batches,
            max_inflight_bytes=args.pipeline_max_inflight_bytes,
        )
        read_config_kwargs = {
            "initial_cords": args.initial_cords,
            "max_cords": args.max_cords,
            "provisional_container_size_bytes": args.provisional_container_size,
            "expected_size_limit_bytes": max(args.expected_size_limit, args.write_probe_bytes),
        }
        if args.write_probe_frame_bytes > 0:
            read_config_kwargs["write_frame_payload_bytes"] = args.write_probe_frame_bytes
        read_config = CxnfsBulkReadConfig(**read_config_kwargs)
        if args.remote_root:
            tls_context = tls_context_factory(args.cafile) if args.cafile else None
            server_hostname = args.server_hostname or args.host
            with credential_prompt() as credentials:
                client_factory = _trainer_stream_remote_client_factory(
                    credentials,
                    args.host,
                    args.control_port,
                    args.transport_port,
                    args.timeout,
                    tls_context,
                    server_hostname,
                    read_config,
                    args.write_connections,
                    args.bulk_read_retries,
                    args.bulk_write_retries,
                    args.shared_client,
                    bulk_transport,
                    args.cafile,
                    control_client_factory,
                    bulk_client_factory,
                    tls_context_factory,
                )
                try:
                    sources = weighted_mds_sources_from_root(
                        args.root,
                        args.weights,
                        client_factory=client_factory,
                        remote_root=args.remote_root,
                    )
                    pipeline_root_client = _connect_trainer_remote_bulk_client(
                        credentials,
                        args.host,
                        args.control_port,
                        args.transport_port,
                        args.timeout,
                        tls_context,
                        server_hostname,
                        read_config,
                        args.write_connections,
                        args.bulk_read_retries,
                        args.bulk_write_retries,
                        bulk_transport,
                        args.cafile,
                        control_client_factory,
                        bulk_client_factory,
                    )
                    with CxnfsTrainingPipeline(
                        CxnfsPathPrefixClient(pipeline_root_client, args.remote_root),
                        pipeline_config,
                        close_client=True,
                    ) as pipeline:
                        payload = _run_trainer_pipeline_smoke(
                            args,
                            sources,
                            pipeline,
                            remote_options,
                            client_factory=client_factory,
                        )
                finally:
                    close_client_factory = getattr(client_factory, "close", None)
                    if callable(close_client_factory):
                        close_client_factory()
        else:
            sources = weighted_mds_sources_from_root(args.root, args.weights)
            with CxnfsTrainingPipeline(
                CxnfsPythonClient(LocalFileTransport(args.root)),
                pipeline_config,
                close_client=True,
            ) as pipeline:
                payload = _run_trainer_pipeline_smoke(args, sources, pipeline, remote_options)

        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            _write_text_atomic(Path(args.output), text)
        out.write(text)
        return 0 if payload["passed"] else 1
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_pipeline_smoke() -> None:
    raise SystemExit(run_trainer_pipeline_smoke_cli())


def run_trainer_surface_matrix_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Run a CXNFS-only trainer surface matrix without touching trainer code.")
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--remote-root", help="Remote CXNFS namespace root containing the same source directories.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--bulk-transport", default="stream")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    parser.add_argument("--cafile", help="optional CA bundle for the server certificate")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, default=1)
    parser.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    parser.add_argument("--provisional-container-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--expected-size-limit", type=int, default=0)
    parser.add_argument("--write-connections", type=int)
    parser.add_argument("--bulk-read-retries", type=int, default=0)
    parser.add_argument("--shared-client", action="store_true")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--warmup-batches", type=int, default=0)
    parser.add_argument("--rounds-per-read", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--source-read-workers", type=int)
    parser.add_argument("--prefetch-chunks", type=int, default=1)
    parser.add_argument("--trainer-prefetch-batches", type=int, default=0)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--epoch", type=int, default=0)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_training_shard_args(parser)
    _add_trainer_repeat_args(parser, default=True)
    parser.add_argument("--decoder", default=MdsSampleFormat.RAW.value)
    parser.add_argument(
        "--read-modes",
        default=f"{TrainerReadMode.DIRECT_BUFFERS.value},{TrainerReadMode.DIRECT_TENSORS.value}",
        help="Comma-separated direct trainer read modes to validate.",
    )
    parser.add_argument(
        "--trainer-batch-iterators",
        default=",".join(TRAINER_BATCH_ITERATORS),
        help="Comma-separated trainer batch iterator shapes to validate.",
    )
    parser.add_argument("--data-loader-workers", type=int, default=0)
    _add_trainer_tensor_allocation_args(parser, default_values=True)
    parser.add_argument("--objective", choices=[AI_TRAINING_25GBE_OBJECTIVE])
    parser.add_argument("--baseline-mb-s", type=float)
    parser.add_argument("--fail-on-miss", action="store_true")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        if args.batches <= 0:
            raise ValueError("batches must be positive")
        if args.warmup_batches < 0:
            raise ValueError("warmup-batches must be non-negative")
        _validate_trainer_prefetch_batches(args.trainer_prefetch_batches)
        args.bulk_read_retries = _validate_bulk_read_retries(args.bulk_read_retries)
        if args.objective and args.baseline_mb_s is None:
            raise ValueError("--objective requires --baseline-mb-s")
        if args.baseline_mb_s is not None and args.baseline_mb_s <= 0:
            raise ValueError("--baseline-mb-s must be positive")
        read_modes = _parse_trainer_surface_matrix_read_modes(args.read_modes)
        iterators = _parse_trainer_surface_matrix_iterators(args.trainer_batch_iterators)
        bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        plan = plan_weighted_mds_sources(args.root, args.weights, remote_root=args.remote_root)
        remote_options = _trainer_stream_remote_options(args)
        read_config = CxnfsBulkReadConfig(
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size_bytes=args.provisional_container_size,
            expected_size_limit_bytes=args.expected_size_limit,
        )

        tls_context = tls_context_factory(args.cafile) if args.remote_root and args.cafile else None
        server_hostname = args.server_hostname or args.host
        credentials: ClientCredentials | None = None
        if args.remote_root:
            credentials = credential_prompt()
        try:
            cases = [
                _run_trainer_surface_matrix_case(
                    args,
                    read_mode,
                    iterator,
                    remote_options,
                    credentials=credentials,
                    tls_context=tls_context,
                    server_hostname=server_hostname,
                    read_config=read_config,
                    bulk_transport=bulk_transport,
                    bulk_read_retries=args.bulk_read_retries,
                    control_client_factory=control_client_factory,
                    bulk_client_factory=bulk_client_factory,
                )
                for read_mode in read_modes
                for iterator in iterators
            ]
        finally:
            if credentials is not None:
                credentials.close()

        payload = _trainer_surface_matrix_report(args, plan, remote_options, cases)
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        if not payload["passed"]:
            return 1
        if args.fail_on_miss and payload.get("objective_passed_all") is False:
            return 1
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_surface_matrix() -> None:
    raise SystemExit(run_trainer_surface_matrix_cli())


def run_trainer_workload_benchmark_cli(
    argv: Sequence[str] | None = None,
    stdout=None,
    stderr=None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    parser = ArgumentParser(description="Benchmark a weighted CXNFS MDS-Shard trainer workload.")
    parser.add_argument("root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", required=True, help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--workload-name", default="weighted-mds")
    parser.add_argument("--remote-root", help="Remote CXNFS namespace root containing the same source directories.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--bulk-transport", default="stream")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    parser.add_argument("--cafile", help="optional CA bundle for the server certificate")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, default=1)
    parser.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    parser.add_argument(
        "--cord-sweep",
        help="Comma-separated fixed cord counts to test, or 'auto' for 1/8/16/32/64/128 capped by --max-cords.",
    )
    parser.add_argument("--provisional-container-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--expected-size-limit", type=int, default=0)
    parser.add_argument("--write-connections", type=int)
    parser.add_argument("--bulk-read-retries", type=int, default=0)
    parser.add_argument("--bulk-write-retries", type=int)
    parser.add_argument("--write-probe-bytes", type=int, default=0, help="Optional direct tensor write probe size for remote workload reports.")
    parser.add_argument("--write-probe-frame-bytes", type=int, default=0, help="Optional write-probe-only bulk frame size.")
    parser.add_argument("--write-probe-root", default="/__cxnfs-write-probe", help="Remote namespace root for optional write probe files.")
    parser.add_argument("--shared-client", action="store_true")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--warmup-batches", type=int, default=0)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--rounds-per-read", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--source-read-workers", type=int)
    parser.add_argument("--prefetch-chunks", type=int, default=1)
    parser.add_argument("--trainer-prefetch-batches", type=int, default=0)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--epoch", type=int, default=0)
    parser.add_argument("--samples-per-epoch", type=int)
    _add_training_shard_args(parser)
    _add_trainer_repeat_args(parser, default=True)
    parser.add_argument("--decoder", default=MdsSampleFormat.RAW.value)
    parser.add_argument("--read-mode", default=TrainerReadMode.DIRECT_BUFFERS.value, choices=tuple(mode.value for mode in TrainerReadMode))
    parser.add_argument("--trainer-batch-iterator", default=TRAINER_BATCH_ITERATOR_NATIVE, choices=TRAINER_BATCH_ITERATORS)
    parser.add_argument("--data-loader-workers", type=int, default=0)
    _add_trainer_tensor_allocation_args(parser, default_values=True)
    parser.add_argument("--objective", choices=[AI_TRAINING_25GBE_OBJECTIVE])
    parser.add_argument("--baseline-mb-s", type=float)
    parser.add_argument("--fail-on-miss", action="store_true")
    parser.add_argument("--include-run-batches", action="store_true")
    parser.add_argument("--blockfs-min-runs", type=int, default=0, help="Minimum workload runs required before a blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-samples", type=int, default=0, help="Minimum workload samples required before a blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-bytes", type=int, default=0, help="Minimum workload bytes required before a blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-cord-sweep", type=int, default=0, help="Minimum cord sweep entries required before a blockfs gate can allow kernel work.")
    parser.add_argument("--server-disk-evidence", help="Path to server disk evidence in JSON or key=value format.")
    parser.add_argument("--server-cache-evidence", help="Path to server cache evidence in JSON or key=value format.")
    parser.add_argument("--server-cpu-evidence", help="Path to server CPU evidence in JSON or key=value format.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON instead of indented JSON.")
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        args = parser.parse_args(argv)
        if args.runs <= 0:
            raise ValueError("runs must be positive")
        if args.batches <= 0:
            raise ValueError("batches must be positive")
        if args.warmup_batches < 0:
            raise ValueError("warmup-batches must be non-negative")
        _validate_trainer_prefetch_batches(args.trainer_prefetch_batches)
        args.bulk_read_retries = _validate_bulk_read_retries(args.bulk_read_retries)
        args.bulk_write_retries = _validate_bulk_write_retries(
            args.bulk_read_retries if args.bulk_write_retries is None else args.bulk_write_retries
        )
        if args.write_probe_bytes < 0:
            raise ValueError("--write-probe-bytes must be non-negative")
        if args.write_probe_frame_bytes < 0:
            raise ValueError("--write-probe-frame-bytes must be non-negative")
        if args.write_probe_bytes > 0 and not args.remote_root:
            raise ValueError("--write-probe-bytes requires --remote-root")
        if args.write_probe_frame_bytes > 0 and args.write_probe_bytes <= 0:
            raise ValueError("--write-probe-frame-bytes requires --write-probe-bytes")
        _validate_trainer_batch_iterator_args(args)
        if args.objective and args.baseline_mb_s is None:
            raise ValueError("--objective requires --baseline-mb-s")
        if args.baseline_mb_s is not None and args.baseline_mb_s <= 0:
            raise ValueError("--baseline-mb-s must be positive")
        _validate_non_negative_thresholds(
            blockfs_min_runs=args.blockfs_min_runs,
            blockfs_min_samples=args.blockfs_min_samples,
            blockfs_min_bytes=args.blockfs_min_bytes,
            blockfs_min_cord_sweep=args.blockfs_min_cord_sweep,
        )
        bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        cord_pairs = _parse_cord_sweep(args.cord_sweep, args.initial_cords, args.max_cords)
        plan = plan_weighted_mds_sources(args.root, args.weights, remote_root=args.remote_root)
        runs = _run_trainer_workload_benchmark(
            args,
            bulk_transport,
            cord_pairs,
            credential_prompt=credential_prompt,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
            tls_context_factory=tls_context_factory,
        )
        evidence = _trainer_workload_server_evidence(args)
        payload = _trainer_workload_benchmark_report(args, plan, cord_pairs, runs, evidence)
        text = json.dumps(payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        out.write(text)
        if args.fail_on_miss and payload.get("objective_passed_any") is False:
            return 1
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def main_trainer_workload_benchmark() -> None:
    raise SystemExit(run_trainer_workload_benchmark_cli())


def _run_trainer_workload_benchmark(
    args,
    bulk_transport: str,
    cord_pairs: Sequence[tuple[int, int]],
    *,
    credential_prompt: Callable[[], ClientCredentials],
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
    tls_context_factory: Callable[[str | None], Any],
) -> list[dict[str, Any]]:
    if args.remote_root:
        tls_context = tls_context_factory(args.cafile) if args.cafile else None
        server_hostname = args.server_hostname or args.host
        with credential_prompt() as credentials:
            return [
                _run_trainer_workload_once(
                    args,
                    bulk_transport,
                    run_index,
                    cord_index,
                    initial_cords,
                    max_cords,
                    credentials=credentials,
                    tls_context=tls_context,
                    server_hostname=server_hostname,
                    control_client_factory=control_client_factory,
                    bulk_client_factory=bulk_client_factory,
                )
                for run_index in range(args.runs)
                for cord_index, (initial_cords, max_cords) in enumerate(cord_pairs)
            ]
    return [
        _run_trainer_workload_once(
            args,
            bulk_transport,
            run_index,
            cord_index,
            initial_cords,
            max_cords,
            credentials=None,
            tls_context=None,
            server_hostname=args.server_hostname or args.host,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
        )
        for run_index in range(args.runs)
        for cord_index, (initial_cords, max_cords) in enumerate(cord_pairs)
    ]


def _run_trainer_workload_once(
    args,
    bulk_transport: str,
    run_index: int,
    cord_index: int,
    initial_cords: int,
    max_cords: int,
    *,
    credentials: ClientCredentials | None,
    tls_context: Any,
    server_hostname: str,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
) -> dict[str, Any]:
    run_args = Namespace(**vars(args))
    run_args.initial_cords = initial_cords
    run_args.max_cords = max_cords
    run_args.epoch = args.epoch + run_index
    remote_options = _trainer_stream_remote_options(run_args)
    if run_args.remote_root:
        if credentials is None:
            raise ValueError("remote workload benchmark requires credentials")
        read_config = CxnfsBulkReadConfig(
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size_bytes=run_args.provisional_container_size,
            expected_size_limit_bytes=run_args.expected_size_limit,
        )
        client_factory = _trainer_stream_remote_client_factory(
            credentials,
            run_args.host,
            run_args.control_port,
            run_args.transport_port,
            run_args.timeout,
            tls_context,
            server_hostname,
            read_config,
            run_args.write_connections,
            run_args.bulk_read_retries,
            run_args.bulk_write_retries,
            run_args.shared_client,
            bulk_transport,
            run_args.cafile,
            control_client_factory,
            bulk_client_factory,
        )
        sources = weighted_mds_sources_from_root(
            run_args.root,
            run_args.weights,
            decoder=run_args.decoder,
            client_factory=client_factory,
            remote_root=run_args.remote_root,
        )
    else:
        sources = weighted_mds_sources_from_root(
            run_args.root,
            run_args.weights,
            decoder=run_args.decoder,
        )
    payload = _run_trainer_stream_smoke(
        run_args,
        sources,
        remote_options,
        client_factory=client_factory if run_args.remote_root else None,
    )
    _apply_trainer_stream_objective(payload, run_args.objective, run_args.baseline_mb_s)
    if run_args.write_probe_bytes > 0:
        payload["write_probe"] = _run_trainer_workload_write_probe(
            run_args,
            bulk_transport,
            run_index,
            cord_index,
            initial_cords,
            max_cords,
            credentials=credentials,
            tls_context=tls_context,
            server_hostname=server_hostname,
            control_client_factory=control_client_factory,
            bulk_client_factory=bulk_client_factory,
        )
    payload.update({
        "run_index": run_index,
        "cord_index": cord_index,
        "initial_cords": initial_cords,
        "max_cords": max_cords,
        "cord_label": f"{initial_cords}-{max_cords}" if initial_cords != max_cords else str(initial_cords),
    })
    if not run_args.include_run_batches:
        payload.pop("batches", None)
    return payload


def _run_trainer_workload_write_probe(
    args,
    bulk_transport: str,
    run_index: int,
    cord_index: int,
    initial_cords: int,
    max_cords: int,
    *,
    credentials: ClientCredentials | None,
    tls_context: Any,
    server_hostname: str,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
) -> dict[str, Any]:
    if credentials is None:
        raise ValueError("remote write probe requires credentials")
    byte_size = int(args.write_probe_bytes)
    if byte_size <= 0:
        raise ValueError("write probe byte size must be positive")
    read_config_kwargs = {
        "initial_cords": initial_cords,
        "max_cords": max_cords,
        "provisional_container_size_bytes": args.provisional_container_size,
        "expected_size_limit_bytes": max(args.expected_size_limit, byte_size),
    }
    if args.write_probe_frame_bytes > 0:
        read_config_kwargs["write_frame_payload_bytes"] = args.write_probe_frame_bytes
    read_config = CxnfsBulkReadConfig(**read_config_kwargs)
    path = join_namespace_path(
        args.write_probe_root,
        _write_probe_file_name(args.workload_name, run_index, cord_index, initial_cords, max_cords),
        label="write_probe_root",
    )
    payload = bytearray(_write_probe_payload(byte_size, run_index, cord_index))
    client = _connect_trainer_remote_bulk_client(
        credentials,
        args.host,
        args.control_port,
        args.transport_port,
        args.timeout,
        tls_context,
        server_hostname,
        read_config,
        args.write_connections,
        getattr(args, "bulk_read_retries", 0),
        getattr(args, "bulk_write_retries", None),
        bulk_transport,
        args.cafile,
        control_client_factory,
        bulk_client_factory,
    )
    try:
        start = time.perf_counter()
        result = client.write_tensor(path, payload, len(payload))
        elapsed_seconds = max(time.perf_counter() - start, 1e-9)
        read_back = client.read_mds_shard(path, 0, len(payload))
        verified_read_back = bytes(read_back) == bytes(payload)
        if not verified_read_back:
            raise ValueError("write probe read-back verification failed")
        evidence_method = getattr(client, "write_path_evidence", None)
        write_evidence = evidence_method() if callable(evidence_method) else {}
        return {
            "format": "cxnfs.trainer-write-probe.v1",
            "path": path,
            "run_index": run_index,
            "cord_index": cord_index,
            "initial_cords": initial_cords,
            "max_cords": max_cords,
            "cord_label": f"{initial_cords}-{max_cords}" if initial_cords != max_cords else str(initial_cords),
            "bulk_write_retries": getattr(args, "bulk_write_retries", getattr(args, "bulk_read_retries", 0)),
            "byte_size": byte_size,
            "frame_bytes": args.write_probe_frame_bytes if args.write_probe_frame_bytes > 0 else None,
            "elapsed_seconds": elapsed_seconds,
            "bytes_per_second": byte_size / elapsed_seconds,
            "mebibytes_per_second": (byte_size / elapsed_seconds) / (1024.0 * 1024.0),
            "verified_read_back": verified_read_back,
            "file_count": getattr(result, "file_count", 0),
            "tensor_file_count": getattr(result, "tensor_file_count", 0),
            "bytes_sent": getattr(result, "bytes_sent", None),
            "write_path_evidence": dict(write_evidence) if isinstance(write_evidence, Mapping) else {},
        }
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def _write_probe_payload(byte_size: int, run_index: int, cord_index: int) -> bytes:
    seed = f"cxnfs-write-probe:{run_index}:{cord_index}:".encode("ascii")
    chunks = []
    remaining = byte_size
    counter = 0
    while remaining > 0:
        digest = hashlib.sha256(seed + str(counter).encode("ascii")).digest()
        chunk = digest[:remaining]
        chunks.append(chunk)
        remaining -= len(chunk)
        counter += 1
    return b"".join(chunks)


def _write_probe_file_name(
    workload_name: str,
    run_index: int,
    cord_index: int,
    initial_cords: int,
    max_cords: int,
) -> str:
    safe = "".join(
        character if character.isascii() and (character.isalnum() or character in "-_.") else "-"
        for character in str(workload_name)
    ).strip(".-")
    safe = safe or "workload"
    return f"{safe}-run-{run_index:04d}-cord-{cord_index:02d}-{initial_cords}x{max_cords}.safetensors"


def _trainer_workload_benchmark_report(
    args,
    plan: CxnfsWeightedMdsPlan,
    cord_pairs: Sequence[tuple[int, int]],
    runs: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    elapsed_seconds = sum(float(run.get("elapsed_seconds", 0.0)) for run in runs)
    byte_size = sum(int(run.get("byte_size", 0)) for run in runs)
    sample_count = sum(int(run.get("sample_count", 0)) for run in runs)
    mib_per_second_values = [float(run.get("mebibytes_per_second", 0.0)) for run in runs]
    objective_values = [
        bool(run["objective_passed"])
        for run in runs
        if "objective_passed" in run
    ]
    plan_payload = plan.to_dict()
    tensor_allocation = _trainer_tensor_allocation_from_args(args)
    remote_client_topology = _aggregate_trainer_remote_client_topology(
        runs,
        _weighted_plan_source_count(plan_payload),
        bool(args.remote_root),
    )
    read_path_evidence = _aggregate_workload_read_path_evidence(runs)
    write_path_evidence = _aggregate_workload_write_path_evidence(runs)
    active_training_shard = (
        dict(_mapping(runs[0].get("training_shard")))
        if runs and isinstance(runs[0].get("training_shard"), Mapping)
        else _training_shard_payload(_training_shard_from_args(args) or current_training_shard())
    )
    active_rank = _int_value(active_training_shard.get("index"), 0)
    active_world_size = _int_value(active_training_shard.get("count"), 1)
    payload: dict[str, Any] = {
        "format": "cxnfs.trainer-workload-benchmark.v1",
        "workload_name": args.workload_name,
        "root": str(args.root),
        "remote_root": _normalize_remote_root(args.remote_root) if args.remote_root else None,
        "weights": parse_weighted_mds_spec(args.weights),
        "weighted_mds_plan": plan_payload,
        "workload_fingerprint": plan_payload["workload_fingerprint"],
        "bulk_transport": normalize_python_bulk_transport(args.bulk_transport),
        "transport": "local-mds" if not args.remote_root else (
            "cxnfs-quic" if normalize_python_bulk_transport(args.bulk_transport) == PYTHON_BULK_TRANSPORT_QUIC
            else "cxnfs-length-prefixed"
        ),
        "read_mode": TrainerReadMode(args.read_mode).value,
        "tensor_allocation": tensor_allocation.to_dict() if TrainerReadMode(args.read_mode) == TrainerReadMode.DIRECT_TENSORS else None,
        "trainer_batch_iterator": args.trainer_batch_iterator,
        "data_loader_workers": args.data_loader_workers,
        "training_shard": active_training_shard,
        "rank": active_rank,
        "world_size": active_world_size,
        "trainer_prefetch_batches": args.trainer_prefetch_batches,
        "trainer_prefetch_enabled": args.trainer_prefetch_batches > 0,
        "bulk_read_retries": getattr(args, "bulk_read_retries", 0),
        "bulk_write_retries": getattr(args, "bulk_write_retries", getattr(args, "bulk_read_retries", 0)),
        "batch_size": args.batch_size,
        "batches_per_run": args.batches,
        "warmup_batches": args.warmup_batches,
        "runs_requested": args.runs,
        "run_count": len(runs),
        "cord_sweep": [
            {"cord_index": index, "initial_cords": initial, "max_cords": maximum}
            for index, (initial, maximum) in enumerate(cord_pairs)
        ],
        "sample_count": sample_count,
        "byte_size": byte_size,
        "elapsed_seconds": elapsed_seconds,
        "samples_per_second": sample_count / max(elapsed_seconds, 1e-9),
        "bytes_per_second": byte_size / max(elapsed_seconds, 1e-9),
        "mebibytes_per_second": (byte_size / max(elapsed_seconds, 1e-9)) / (1024.0 * 1024.0),
        "min_run_mebibytes_per_second": min(mib_per_second_values) if mib_per_second_values else 0.0,
        "max_run_mebibytes_per_second": max(mib_per_second_values) if mib_per_second_values else 0.0,
        "mean_run_mebibytes_per_second": (
            sum(mib_per_second_values) / len(mib_per_second_values)
            if mib_per_second_values else 0.0
        ),
        "rounds_per_read": args.rounds_per_read,
        "max_workers": args.max_workers,
        "source_read_workers": args.source_read_workers,
        "prefetch_chunks": args.prefetch_chunks,
        "shuffle": args.shuffle,
        "seed": args.seed,
        "epoch_start": args.epoch,
        "samples_per_epoch": args.samples_per_epoch,
        "repeat": args.repeat,
        "server_evidence": evidence,
        "blockfs_workload_requirements": _blockfs_workload_requirements(args),
        "benchmark_provenance": _trainer_workload_benchmark_provenance(args, cord_pairs),
        "resume_cursor_evidence": _aggregate_workload_resume_cursor_evidence(runs),
        "read_path_evidence": read_path_evidence,
        "remote_client_topology": remote_client_topology,
        "write_probe": {
            "enabled": args.write_probe_bytes > 0,
            "byte_size": args.write_probe_bytes,
            "frame_bytes": args.write_probe_frame_bytes if args.write_probe_bytes > 0 and args.write_probe_frame_bytes > 0 else None,
            "remote_root": _normalize_remote_root(args.write_probe_root) if args.write_probe_bytes > 0 else None,
        },
        "write_path_evidence": write_path_evidence,
        "runs": list(runs),
    }
    if args.objective:
        payload.update({
            "objective": args.objective,
            "objective_baseline_mb_per_second": args.baseline_mb_s,
            "objective_passed_any": any(objective_values),
            "objective_passed_all": all(objective_values) if objective_values else False,
        })
    network_summary = _trainer_workload_network_summary(payload, cord_pairs, runs)
    payload["network_path_summary"] = network_summary
    payload["read_path_reliability"] = _trainer_read_path_reliability(payload)
    payload["sustained_read_profile"] = _trainer_workload_sustained_read_profile(payload, network_summary)
    blockfs_gate = _trainer_workload_blockfs_gate(payload, network_summary, evidence)
    payload["blockfs_gate"] = blockfs_gate
    payload["pressure_profile"] = _trainer_workload_pressure_profile(payload, network_summary, blockfs_gate)
    return payload


def _trainer_workload_benchmark_provenance(
    args,
    cord_pairs: Sequence[tuple[int, int]],
) -> dict[str, Any]:
    invocation = {
        "root": str(args.root),
        "weights": parse_weighted_mds_spec(args.weights),
        "workload_name": args.workload_name,
        "remote_root": _normalize_remote_root(args.remote_root) if args.remote_root else None,
        "host": args.host if args.remote_root else None,
        "control_port": args.control_port if args.remote_root else None,
        "transport_port": args.transport_port if args.remote_root else None,
        "server_hostname": (args.server_hostname or args.host) if args.remote_root else None,
        "bulk_transport": normalize_python_bulk_transport(args.bulk_transport),
        "read_mode": TrainerReadMode(args.read_mode).value,
        "tensor_allocation": (
            _trainer_tensor_allocation_from_args(args).to_dict()
            if TrainerReadMode(args.read_mode) == TrainerReadMode.DIRECT_TENSORS
            else None
        ),
        "trainer_batch_iterator": args.trainer_batch_iterator,
        "data_loader_workers": args.data_loader_workers,
        "training_shard": _training_shard_payload(_training_shard_from_args(args) or current_training_shard()),
        "trainer_prefetch_batches": args.trainer_prefetch_batches,
        "bulk_read_retries": getattr(args, "bulk_read_retries", 0),
        "bulk_write_retries": getattr(args, "bulk_write_retries", getattr(args, "bulk_read_retries", 0)),
        "initial_cords": args.initial_cords,
        "max_cords": args.max_cords,
        "cord_sweep": [
            {"initial_cords": initial, "max_cords": maximum}
            for initial, maximum in cord_pairs
        ],
        "batch_size": args.batch_size,
        "batches": args.batches,
        "warmup_batches": args.warmup_batches,
        "runs": args.runs,
        "rounds_per_read": args.rounds_per_read,
        "max_workers": args.max_workers,
        "source_read_workers": args.source_read_workers,
        "prefetch_chunks": args.prefetch_chunks,
        "shuffle": args.shuffle,
        "seed": args.seed,
        "epoch": args.epoch,
        "samples_per_epoch": args.samples_per_epoch,
        "repeat": args.repeat,
        "decoder": args.decoder,
        "objective": args.objective,
        "baseline_mb_s": args.baseline_mb_s,
        "write_probe_bytes": args.write_probe_bytes,
        "write_probe_frame_bytes": args.write_probe_frame_bytes if args.write_probe_bytes > 0 and args.write_probe_frame_bytes > 0 else None,
        "write_probe_root": _normalize_remote_root(args.write_probe_root) if args.write_probe_bytes > 0 else None,
        "blockfs_workload_requirements": _blockfs_workload_requirements(args),
    }
    encoded = json.dumps(invocation, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "format": TRAINER_WORKLOAD_PROVENANCE_FORMAT,
        "created_unix_seconds": time.time(),
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "invocation": invocation,
        "invocation_fingerprint": {
            "algorithm": "sha256",
            "sha256": hashlib.sha256(encoded).hexdigest(),
        },
    }


def _aggregate_workload_read_path_evidence(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    reports = [
        run.get("read_path_evidence")
        for run in runs
        if isinstance(run.get("read_path_evidence"), Mapping)
    ]
    aggregate = _aggregate_read_path_evidence(reports)  # type: ignore[arg-type]
    aggregate.update({
        "run_count": len(reports),
        "all_runs_have_evidence": len(reports) == len(runs),
    })
    return aggregate


def _aggregate_workload_resume_cursor_evidence(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    cursor_entries: list[tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]] = []
    for run in runs:
        initial = _mapping(run.get("initial_cursor"))
        final = _mapping(run.get("final_cursor"))
        if initial and final:
            cursor_entries.append((run, initial, final))

    emitted_deltas: list[int] = []
    source_name_matches = 0
    final_cursor_advances = 0
    for run, initial, final in cursor_entries:
        initial_emitted = _int_value(initial.get("emitted"), 0)
        final_emitted = _int_value(final.get("emitted"), 0)
        delta = final_emitted - initial_emitted
        emitted_deltas.append(delta)
        if list(initial.get("source_names", ())) == list(final.get("source_names", ())):
            source_name_matches += 1
        if delta == _int_value(run.get("sample_count"), 0):
            final_cursor_advances += 1

    all_runs_have_cursors = bool(runs) and len(cursor_entries) == len(runs)
    all_final_cursors_advance = (
        bool(cursor_entries)
        and final_cursor_advances == len(cursor_entries)
    )
    all_source_names_match = (
        bool(cursor_entries)
        and source_name_matches == len(cursor_entries)
    )
    return {
        "format": "cxnfs.trainer-workload-cursor-evidence.v1",
        "cursor_format": TRAINER_CURSOR_FORMAT,
        "run_count": len(cursor_entries),
        "all_runs_have_resume_cursors": all_runs_have_cursors,
        "all_final_cursors_advance_by_sample_count": all_final_cursors_advance,
        "all_source_names_match": all_source_names_match,
        "emitted_deltas": emitted_deltas,
        "first_initial_cursor": dict(cursor_entries[0][1]) if cursor_entries else None,
        "last_final_cursor": dict(cursor_entries[-1][2]) if cursor_entries else None,
        "complete": all_runs_have_cursors and all_final_cursors_advance and all_source_names_match,
    }


def _aggregate_workload_write_path_evidence(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    write_probe_entries = [
        (run, _mapping(run.get("write_probe")))
        for run in runs
        if isinstance(run.get("write_probe"), Mapping)
    ]
    write_probes = [probe for _, probe in write_probe_entries]
    reports = [
        probe.get("write_path_evidence")
        for _, probe in write_probe_entries
        if isinstance(probe.get("write_path_evidence"), Mapping)
    ]
    verified_read_back_count = sum(1 for probe in write_probes if bool(probe.get("verified_read_back")))
    cord_labels: list[str] = []
    seen_cord_labels: set[str] = set()
    for run, probe in write_probe_entries:
        label = str(probe.get("cord_label") or run.get("cord_label") or "")
        if not label:
            initial = _int_value(probe.get("initial_cords"), _int_value(run.get("initial_cords"), 0))
            maximum = _int_value(probe.get("max_cords"), _int_value(run.get("max_cords"), initial))
            if initial > 0 and maximum > 0:
                label = f"{initial}-{maximum}" if initial != maximum else str(initial)
        if label and label not in seen_cord_labels:
            seen_cord_labels.add(label)
            cord_labels.append(label)
    totals: dict[str, Any] = {
        "format": "cxnfs.workload-write-path-evidence.v1",
        "enabled": bool(write_probes),
        "write_probe_count": len(write_probes),
        "write_probe_cord_sweep_count": len(cord_labels),
        "write_probe_cord_labels": cord_labels,
        "run_count": len(reports),
        "all_runs_have_evidence": len(reports) == len(runs) and bool(runs),
        "verified_read_back_count": verified_read_back_count,
        "all_write_probes_verified": verified_read_back_count == len(runs) and bool(runs),
        "write_operations": 0,
        "direct_tensor_write_operations": 0,
        "directory_output_write_operations": 0,
        "payload_bytes_written": 0,
        "write_frames": 0,
        "write_acks": 0,
        "write_ack_frame_verified": 0,
        "write_ack_metadata_verified": 0,
        "write_ack_metadata_pending": 0,
        "write_ack_memory": 0,
        "write_ack_spill": 0,
        "metadata_after_payload_entries": 0,
        "entries_committed": 0,
        "transfers_committed": 0,
        "max_observed_write_cords": 0,
        "write_retry_limit": 0,
        "write_retry_attempts": 0,
        "write_retry_successes": 0,
        "bulk_transport_write_reconnects": 0,
        "quic_write_transport_evidence_count": 0,
        "quic_write_transport_connection_reuse_count": 0,
        "quic_write_transport_successful_write_operations": 0,
        "quic_write_transport_failed_write_operations": 0,
        "quic_write_transport_attempted_write_streams": 0,
        "quic_write_transport_write_streams_created": 0,
        "quic_write_transport_max_active_write_streams": 0,
        "quic_write_transport_concurrent_stream_report_count": 0,
        "quic_write_transport_write_stream_success_report_count": 0,
        "quic_write_transport_write_ack_stream_completion_report_count": 0,
        "quic_write_transport_write_ack_stream_eof_report_count": 0,
        "quic_write_transport_write_ack_streams_eof_verified": 0,
        "quic_write_transport_write_acks_received": 0,
        "quic_write_transport_write_acks_verified": 0,
        "quic_write_transport_write_ack_verification_failures": 0,
        "quic_write_transport_write_ack_identity_report_count": 0,
        "quic_write_transport_write_ack_metadata_pending": 0,
        "quic_write_transport_write_ack_metadata_pending_report_count": 0,
        "quic_write_transport_connection_opened_count": 0,
        "quic_write_transport_cipher_suite_counts": {},
        "quic_write_transport_tls_version_counts": {},
    }
    for report in reports:
        report_mapping = _mapping(report)
        for key in (
            "write_operations",
            "direct_tensor_write_operations",
            "directory_output_write_operations",
            "payload_bytes_written",
            "write_frames",
            "write_acks",
            "write_ack_frame_verified",
            "write_ack_metadata_verified",
            "write_ack_metadata_pending",
            "write_ack_memory",
            "write_ack_spill",
            "metadata_after_payload_entries",
            "entries_committed",
            "transfers_committed",
        ):
            totals[key] = int(totals[key]) + int(report_mapping.get(key, 0) or 0)
        totals["max_observed_write_cords"] = max(
            int(totals["max_observed_write_cords"]),
            int(report_mapping.get("max_observed_write_cords", 0) or 0),
        )
        totals["write_retry_limit"] = max(
            int(totals["write_retry_limit"]),
            int(report_mapping.get("write_retry_limit", 0) or 0),
        )
        totals["write_retry_attempts"] = (
            int(totals["write_retry_attempts"])
            + int(report_mapping.get("write_retry_attempts", 0) or 0)
        )
        totals["write_retry_successes"] = (
            int(totals["write_retry_successes"])
            + int(report_mapping.get("write_retry_successes", 0) or 0)
        )
        totals["bulk_transport_write_reconnects"] = (
            int(totals["bulk_transport_write_reconnects"])
            + int(report_mapping.get("bulk_transport_write_reconnects", 0) or 0)
        )
        transport_evidence = _mapping(report_mapping.get("bulk_transport_write_evidence"))
        if transport_evidence:
            cipher_suite = str(transport_evidence.get("cipher_suite") or "")
            if cipher_suite:
                cipher_counts = totals["quic_write_transport_cipher_suite_counts"]
                cipher_counts[cipher_suite] = int(cipher_counts.get(cipher_suite, 0)) + 1
            tls_version = str(transport_evidence.get("tls_version") or "")
            if tls_version:
                tls_counts = totals["quic_write_transport_tls_version_counts"]
                tls_counts[tls_version] = int(tls_counts.get(tls_version, 0)) + 1
            totals["quic_write_transport_evidence_count"] = int(totals["quic_write_transport_evidence_count"]) + 1
            totals["quic_write_transport_connection_reuse_count"] = (
                int(totals["quic_write_transport_connection_reuse_count"])
                + (1 if bool(transport_evidence.get("connection_reused_for_streams")) else 0)
            )
            totals["quic_write_transport_successful_write_operations"] = (
                int(totals["quic_write_transport_successful_write_operations"])
                + int(transport_evidence.get("successful_write_operations", 0) or 0)
            )
            totals["quic_write_transport_failed_write_operations"] = (
                int(totals["quic_write_transport_failed_write_operations"])
                + int(transport_evidence.get("failed_write_operations", 0) or 0)
            )
            totals["quic_write_transport_attempted_write_streams"] = (
                int(totals["quic_write_transport_attempted_write_streams"])
                + int(transport_evidence.get("attempted_write_streams", 0) or 0)
            )
            totals["quic_write_transport_write_streams_created"] = (
                int(totals["quic_write_transport_write_streams_created"])
                + int(transport_evidence.get("write_streams_created", 0) or 0)
            )
            totals["quic_write_transport_max_active_write_streams"] = max(
                int(totals["quic_write_transport_max_active_write_streams"]),
                int(transport_evidence.get("max_active_write_streams", 0) or 0),
            )
            max_active_write_streams = int(transport_evidence.get("max_active_write_streams", 0) or 0)
            if bool(transport_evidence.get("concurrent_write_streams_observed")) or max_active_write_streams > 1:
                totals["quic_write_transport_concurrent_stream_report_count"] = (
                    int(totals["quic_write_transport_concurrent_stream_report_count"]) + 1
                )
            attempted_write_streams = int(transport_evidence.get("attempted_write_streams", 0) or 0)
            write_streams_created = int(transport_evidence.get("write_streams_created", 0) or 0)
            failed_write_operations = int(transport_evidence.get("failed_write_operations", 0) or 0)
            if bool(transport_evidence.get("write_streams_all_succeeded")) or (
                attempted_write_streams > 0
                and failed_write_operations == 0
                and write_streams_created >= attempted_write_streams
            ):
                totals["quic_write_transport_write_stream_success_report_count"] = (
                    int(totals["quic_write_transport_write_stream_success_report_count"]) + 1
                )
            if bool(transport_evidence.get("write_ack_stream_completion_verified")):
                totals["quic_write_transport_write_ack_stream_completion_report_count"] = (
                    int(totals["quic_write_transport_write_ack_stream_completion_report_count"]) + 1
                )
            write_ack_streams_eof_verified = int(
                transport_evidence.get("write_ack_streams_eof_verified", 0) or 0
            )
            totals["quic_write_transport_write_ack_streams_eof_verified"] = (
                int(totals["quic_write_transport_write_ack_streams_eof_verified"])
                + write_ack_streams_eof_verified
            )
            if (
                attempted_write_streams > 0
                and write_ack_streams_eof_verified == attempted_write_streams
                and failed_write_operations == 0
            ) or (
                write_ack_streams_eof_verified <= 0
                and bool(transport_evidence.get("write_ack_stream_eof_verified"))
            ):
                totals["quic_write_transport_write_ack_stream_eof_report_count"] = (
                    int(totals["quic_write_transport_write_ack_stream_eof_report_count"]) + 1
                )
            write_acks_received = int(transport_evidence.get("write_acks_received", 0) or 0)
            write_acks_verified = int(transport_evidence.get("write_acks_verified", 0) or 0)
            write_ack_verification_failures = int(
                transport_evidence.get("write_ack_verification_failures", 0) or 0
            )
            totals["quic_write_transport_write_acks_received"] = (
                int(totals["quic_write_transport_write_acks_received"]) + write_acks_received
            )
            totals["quic_write_transport_write_acks_verified"] = (
                int(totals["quic_write_transport_write_acks_verified"]) + write_acks_verified
            )
            totals["quic_write_transport_write_ack_verification_failures"] = (
                int(totals["quic_write_transport_write_ack_verification_failures"])
                + write_ack_verification_failures
            )
            if bool(transport_evidence.get("write_ack_frame_identity_verified")):
                totals["quic_write_transport_write_ack_identity_report_count"] = (
                    int(totals["quic_write_transport_write_ack_identity_report_count"]) + 1
                )
            write_ack_metadata_pending = int(transport_evidence.get("write_ack_metadata_pending", 0) or 0)
            totals["quic_write_transport_write_ack_metadata_pending"] = (
                int(totals["quic_write_transport_write_ack_metadata_pending"]) + write_ack_metadata_pending
            )
            if bool(transport_evidence.get("write_ack_metadata_pending_observed")) or write_ack_metadata_pending > 0:
                totals["quic_write_transport_write_ack_metadata_pending_report_count"] = (
                    int(totals["quic_write_transport_write_ack_metadata_pending_report_count"]) + 1
                )
            totals["quic_write_transport_connection_opened_count"] = (
                int(totals["quic_write_transport_connection_opened_count"])
                + int(transport_evidence.get("connection_opened_count", 0) or 0)
            )
    totals["metadata_pending_observed"] = int(totals["write_ack_metadata_pending"]) > 0
    totals["write_ack_frame_identity_verified"] = (
        int(totals["write_frames"]) > 0
        and int(totals["write_acks"]) == int(totals["write_frames"])
        and int(totals["write_ack_frame_verified"]) == int(totals["write_frames"])
    )
    totals["direct_tensor_writes_observed"] = int(totals["direct_tensor_write_operations"]) > 0
    totals["write_retry_attempts_recovered"] = (
        int(totals["write_retry_attempts"]) == int(totals["write_retry_successes"])
    )
    totals["write_retry_reconnects_observed"] = (
        int(totals["bulk_transport_write_reconnects"]) >= int(totals["write_retry_attempts"])
    )
    totals["quic_write_transport_all_reports_present"] = (
        bool(reports)
        and int(totals["quic_write_transport_evidence_count"]) == len(reports)
    )
    totals["quic_write_transport_all_reports_reused_connection"] = (
        bool(reports)
        and int(totals["quic_write_transport_connection_reuse_count"]) == len(reports)
    )
    totals["quic_write_transport_expected_cipher_suite_count"] = int(
        _mapping(totals.get("quic_write_transport_cipher_suite_counts")).get(CXNFS_TLS_CIPHER_SUITE, 0) or 0
    )
    totals["quic_write_transport_all_reports_expected_cipher_suite"] = (
        bool(reports)
        and int(totals["quic_write_transport_expected_cipher_suite_count"]) == len(reports)
    )
    totals["quic_write_transport_concurrent_streams_observed"] = (
        int(totals["quic_write_transport_concurrent_stream_report_count"]) > 0
    )
    totals["quic_write_transport_all_reports_write_streams_succeeded"] = (
        bool(reports)
        and int(totals["quic_write_transport_write_stream_success_report_count"]) == len(reports)
    )
    totals["quic_write_transport_all_reports_write_ack_stream_completion_verified"] = (
        bool(reports)
        and int(totals["quic_write_transport_write_ack_stream_completion_report_count"]) == len(reports)
    )
    totals["quic_write_transport_all_reports_write_ack_stream_eof_verified"] = (
        bool(reports)
        and int(totals["quic_write_transport_write_ack_stream_eof_report_count"]) == len(reports)
        and int(totals["quic_write_transport_write_ack_streams_eof_verified"])
        == int(totals["quic_write_transport_attempted_write_streams"])
    )
    totals["quic_write_transport_all_reports_write_ack_frame_identity_verified"] = (
        bool(reports)
        and int(totals["quic_write_transport_write_ack_identity_report_count"]) == len(reports)
    )
    totals["quic_write_transport_write_ack_metadata_pending_observed"] = (
        int(totals["quic_write_transport_write_ack_metadata_pending_report_count"]) > 0
    )
    totals["quic_write_transport_complete"] = (
        bool(totals["quic_write_transport_all_reports_present"])
        and bool(totals["quic_write_transport_all_reports_reused_connection"])
        and bool(totals["quic_write_transport_all_reports_expected_cipher_suite"])
        and bool(totals["quic_write_transport_all_reports_write_streams_succeeded"])
        and bool(totals["quic_write_transport_all_reports_write_ack_stream_completion_verified"])
        and bool(totals["quic_write_transport_all_reports_write_ack_stream_eof_verified"])
        and bool(totals["quic_write_transport_all_reports_write_ack_frame_identity_verified"])
        and bool(totals["quic_write_transport_concurrent_streams_observed"])
        and int(totals["quic_write_transport_failed_write_operations"]) == 0
        and int(totals["quic_write_transport_write_ack_verification_failures"]) == 0
        and int(totals["quic_write_transport_successful_write_operations"]) >= int(totals["write_frames"])
        and int(totals["quic_write_transport_write_streams_created"]) >= int(totals["write_frames"])
        and int(totals["quic_write_transport_write_acks_received"]) >= int(totals["write_frames"])
        and int(totals["quic_write_transport_write_acks_verified"]) >= int(totals["write_frames"])
        and int(totals["quic_write_transport_max_active_write_streams"]) >= min(
            2,
            max(1, int(totals["max_observed_write_cords"])),
        )
    )
    minimum_aggregate_write_cords = min(2, max(1, len(cord_labels)))
    totals["complete"] = (
        bool(totals["enabled"])
        and bool(totals["all_runs_have_evidence"])
        and bool(totals["all_write_probes_verified"])
        and int(totals["direct_tensor_write_operations"]) >= len(runs)
        and int(totals["write_frames"]) > 0
        and bool(totals["write_ack_frame_identity_verified"])
        and int(totals["metadata_after_payload_entries"]) >= len(runs)
        and int(totals["entries_committed"]) >= len(runs)
        and int(totals["transfers_committed"]) >= len(runs)
        and int(totals["write_probe_cord_sweep_count"]) >= min(1, len(runs))
        and int(totals["max_observed_write_cords"]) >= minimum_aggregate_write_cords
        and bool(totals["write_retry_attempts_recovered"])
        and bool(totals["write_retry_reconnects_observed"])
    )
    return totals


def attach_server_evidence_to_trainer_workload_payload(
    payload: Mapping[str, Any],
    *,
    disk_evidence: str | Path | None = None,
    cache_evidence: str | Path | None = None,
    cpu_evidence: str | Path | None = None,
) -> dict[str, Any]:
    updated = dict(payload)
    args = Namespace(
        server_disk_evidence=str(disk_evidence) if disk_evidence else None,
        server_cache_evidence=str(cache_evidence) if cache_evidence else None,
        server_cpu_evidence=str(cpu_evidence) if cpu_evidence else None,
    )
    evidence = _trainer_workload_server_evidence(args)
    updated["server_evidence"] = evidence
    return refresh_trainer_workload_gate(updated)


def apply_blockfs_workload_requirements(
    payload: Mapping[str, Any],
    *,
    min_runs: int = 0,
    min_samples: int = 0,
    min_bytes: int = 0,
    min_cord_sweep: int = 0,
) -> dict[str, Any]:
    _validate_non_negative_thresholds(
        blockfs_min_runs=min_runs,
        blockfs_min_samples=min_samples,
        blockfs_min_bytes=min_bytes,
        blockfs_min_cord_sweep=min_cord_sweep,
    )
    updated = dict(payload)
    if any(value > 0 for value in (min_runs, min_samples, min_bytes, min_cord_sweep)):
        updated["blockfs_workload_requirements"] = _blockfs_workload_requirements_from_values(
            min_runs=min_runs,
            min_samples=min_samples,
            min_bytes=min_bytes,
            min_cord_sweep=min_cord_sweep,
        )
        return refresh_trainer_workload_gate(updated)
    return updated


def refresh_trainer_workload_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    cord_pairs = tuple(
        (
            int(entry.get("initial_cords", 1)),
            int(entry.get("max_cords", entry.get("initial_cords", 1))),
        )
        for entry in updated.get("cord_sweep", ())
        if isinstance(entry, Mapping)
    )
    runs = updated.get("runs", ())
    runs = runs if isinstance(runs, Sequence) else ()
    evidence = _mapping(updated.get("server_evidence"))
    network_summary = _trainer_workload_network_summary(updated, cord_pairs, runs)
    updated["network_path_summary"] = network_summary
    updated["read_path_reliability"] = _trainer_read_path_reliability(updated)
    updated["sustained_read_profile"] = _trainer_workload_sustained_read_profile(updated, network_summary)
    blockfs_gate = _trainer_workload_blockfs_gate(updated, network_summary, evidence)
    updated["blockfs_gate"] = blockfs_gate
    updated["pressure_profile"] = _trainer_workload_pressure_profile(updated, network_summary, blockfs_gate)
    return updated


def _trainer_workload_pressure_profile(
    payload: Mapping[str, Any],
    network_summary: Mapping[str, Any],
    blockfs_gate: Mapping[str, Any],
) -> dict[str, Any]:
    target_bytes_per_second = _trainer_workload_target_bytes_per_second(payload)
    target_mib_per_second = target_bytes_per_second / (1024.0 * 1024.0) if target_bytes_per_second > 0 else 0.0
    aggregate_bytes_per_second = float(payload.get("bytes_per_second", 0.0) or 0.0)
    if aggregate_bytes_per_second <= 0.0:
        aggregate_bytes_per_second = float(payload.get("mebibytes_per_second", 0.0) or 0.0) * 1024.0 * 1024.0
    aggregate_mib_per_second = aggregate_bytes_per_second / (1024.0 * 1024.0)
    best_run_mib_per_second = float(payload.get("max_run_mebibytes_per_second", 0.0) or 0.0)
    best_run_bytes_per_second = best_run_mib_per_second * 1024.0 * 1024.0
    best_cord_mib_per_second = float(network_summary.get("best_cord_mebibytes_per_second", 0.0) or 0.0)
    best_cord_bytes_per_second = best_cord_mib_per_second * 1024.0 * 1024.0
    best_observed_bytes_per_second = max(aggregate_bytes_per_second, best_run_bytes_per_second, best_cord_bytes_per_second)
    best_observed_mib_per_second = best_observed_bytes_per_second / (1024.0 * 1024.0)
    target_gap_bytes_per_second = (
        max(0.0, target_bytes_per_second - best_observed_bytes_per_second)
        if target_bytes_per_second > 0 else 0.0
    )
    cord_sweep_count = int(network_summary.get("cord_sweep_count", 0) or 0)
    cord_scaling_improvement_ratio = float(network_summary.get("cord_scaling_improvement_ratio", 0.0) or 0.0)
    read_path_complete = bool(blockfs_gate.get("read_path_evidence_complete"))
    memory_path_complete = bool(blockfs_gate.get("memory_path_evidence_complete"))
    write_path_complete = bool(blockfs_gate.get("write_path_evidence_complete"))
    workload_scale_complete = bool(blockfs_gate.get("workload_scale_complete"))
    server_evidence_complete = bool(blockfs_gate.get("server_evidence_complete"))
    server_sample_window_complete = bool(blockfs_gate.get("server_evidence_sample_window_complete"))
    read_reliability = _mapping(payload.get("read_path_reliability"))
    read_reliability_complete = bool(read_reliability.get("complete"))
    sustained_read = _mapping(payload.get("sustained_read_profile"))
    sustained_read_complete = bool(sustained_read.get("complete"))
    return {
        "format": TRAINER_WORKLOAD_PRESSURE_FORMAT,
        "objective": payload.get("objective"),
        "objective_present": bool(payload.get("objective")),
        "objective_passed_any": payload.get("objective_passed_any"),
        "objective_passed_all": payload.get("objective_passed_all"),
        "target_bytes_per_second": target_bytes_per_second,
        "target_mebibytes_per_second": target_mib_per_second,
        "aggregate_bytes_per_second": aggregate_bytes_per_second,
        "aggregate_mebibytes_per_second": aggregate_mib_per_second,
        "best_observed_bytes_per_second": best_observed_bytes_per_second,
        "best_observed_mebibytes_per_second": best_observed_mib_per_second,
        "best_run_mebibytes_per_second": best_run_mib_per_second,
        "best_cord_label": network_summary.get("best_cord_label"),
        "best_cord_mebibytes_per_second": best_cord_mib_per_second,
        "target_ratio": (
            best_observed_bytes_per_second / target_bytes_per_second
            if target_bytes_per_second > 0 else None
        ),
        "target_gap_bytes_per_second": target_gap_bytes_per_second,
        "target_gap_mebibytes_per_second": target_gap_bytes_per_second / (1024.0 * 1024.0),
        "cord_sweep_count": cord_sweep_count,
        "cord_scaling_improvement_ratio": cord_scaling_improvement_ratio,
        "cord_plateau_observed": cord_sweep_count >= 2 and cord_scaling_improvement_ratio <= 0.10,
        "java_python_network_path_exercised": bool(network_summary.get("java_python_network_path_exercised")),
        "sustained_read_profile_complete": sustained_read_complete,
        "sustained_read_run_count": _int_value(sustained_read.get("run_count"), 0),
        "sustained_read_total_batches": _int_value(sustained_read.get("total_batches"), 0),
        "sustained_read_total_samples": _int_value(sustained_read.get("sample_count"), 0),
        "sustained_read_total_bytes": _int_value(sustained_read.get("byte_size"), 0),
        "read_path_evidence_complete": read_path_complete,
        "read_path_reliability_complete": read_reliability_complete,
        "unrecovered_read_failures": int(read_reliability.get("unrecovered_read_failures", 0) or 0),
        "read_retry_successes": int(read_reliability.get("read_retry_successes", 0) or 0),
        "bulk_transport_reconnects": int(read_reliability.get("bulk_transport_reconnects", 0) or 0),
        "memory_path_evidence_complete": memory_path_complete,
        "write_path_evidence_complete": write_path_complete,
        "workload_scale_complete": workload_scale_complete,
        "server_evidence_complete": server_evidence_complete,
        "server_evidence_sample_window_complete": server_sample_window_complete,
        "blockfs_decision": blockfs_gate.get("decision"),
        "kernel_module_allowed": bool(blockfs_gate.get("kernel_module_allowed")),
        "decision_ready": (
            bool(payload.get("objective"))
            and bool(network_summary.get("java_python_network_path_exercised"))
            and sustained_read_complete
            and read_path_complete
            and read_reliability_complete
            and memory_path_complete
            and write_path_complete
            and workload_scale_complete
            and server_evidence_complete
            and server_sample_window_complete
        ),
    }


def _trainer_read_path_reliability(payload: Mapping[str, Any]) -> dict[str, Any]:
    evidence = _mapping(payload.get("read_path_evidence"))
    all_runs_have_evidence = bool(evidence.get("all_runs_have_evidence"))
    read_retry_limit = _int_value(evidence.get("read_retry_limit"), 0)
    read_retry_attempts = _int_value(evidence.get("read_retry_attempts"), 0)
    read_retry_successes = _int_value(evidence.get("read_retry_successes"), 0)
    failed_read_operations = _int_value(evidence.get("failed_read_operations"), 0)
    bulk_transport_reconnects = _int_value(evidence.get("bulk_transport_reconnects"), 0)
    recovered_read_failures = min(failed_read_operations, read_retry_successes)
    unrecovered_read_failures = max(0, failed_read_operations - read_retry_successes)
    missing: list[str] = []
    if not evidence:
        missing.append("read_path_evidence")
    elif not all_runs_have_evidence:
        missing.append("all runs must include read-path evidence")
    if unrecovered_read_failures > 0:
        missing.append("all read failures must be recovered by bounded retry")
    if read_retry_attempts != read_retry_successes:
        missing.append("read retry attempts must either complete or fail the workload")
    if bulk_transport_reconnects < read_retry_attempts:
        missing.append("read retry attempts must reconnect the bulk transport")
    return {
        "format": TRAINER_READ_PATH_RELIABILITY_FORMAT,
        "complete": not missing,
        "missing": missing,
        "retry_enabled": read_retry_limit > 0,
        "read_retry_limit": read_retry_limit,
        "read_retry_attempts": read_retry_attempts,
        "read_retry_successes": read_retry_successes,
        "bulk_transport_reconnects": bulk_transport_reconnects,
        "failed_read_operations": failed_read_operations,
        "failed_byte_return_read_operations": _int_value(evidence.get("failed_byte_return_read_operations"), 0),
        "failed_direct_buffer_read_operations": _int_value(evidence.get("failed_direct_buffer_read_operations"), 0),
        "failed_direct_tensor_read_operations": _int_value(evidence.get("failed_direct_tensor_read_operations"), 0),
        "failed_direct_destination_read_operations": _int_value(evidence.get("failed_direct_destination_read_operations"), 0),
        "direct_destination_zeroed_bytes_on_failure": _int_value(
            evidence.get("direct_destination_zeroed_bytes_on_failure"),
            0,
        ),
        "recovered_read_failures": recovered_read_failures,
        "unrecovered_read_failures": unrecovered_read_failures,
        "retry_attempts_recovered": read_retry_attempts == read_retry_successes,
        "retry_reconnects_observed": bulk_transport_reconnects >= read_retry_attempts,
        "all_runs_have_evidence": all_runs_have_evidence,
    }


def _trainer_workload_sustained_read_profile(
    payload: Mapping[str, Any],
    network_summary: Mapping[str, Any],
) -> dict[str, Any]:
    runs_value = payload.get("runs")
    runs = (
        tuple(run for run in runs_value if isinstance(run, Mapping))
        if isinstance(runs_value, Sequence) and not isinstance(runs_value, (str, bytes))
        else tuple()
    )
    run_count = max(_int_value(payload.get("run_count"), 0), len(runs))
    batches_per_run = _int_value(payload.get("batches_per_run"), 0)
    if runs:
        total_batches = sum(_int_value(run.get("batch_count"), 0) for run in runs)
        min_run_batches = min((_int_value(run.get("batch_count"), 0) for run in runs), default=0)
    elif batches_per_run > 0 and run_count > 0:
        total_batches = run_count * batches_per_run
        min_run_batches = batches_per_run
    else:
        total_batches = max(_int_value(payload.get("batch_count"), 0), run_count)
        min_run_batches = 1 if total_batches >= run_count and run_count > 0 else 0
    sample_count = _int_value(payload.get("sample_count"), 0)
    byte_size = _int_value(payload.get("byte_size"), 0)
    read_path = _mapping(payload.get("read_path_evidence"))
    read_reliability = _mapping(payload.get("read_path_reliability")) or _trainer_read_path_reliability(payload)
    cord_sweep_count = _int_value(network_summary.get("cord_sweep_count"), _workload_cord_sweep_count(payload))
    requires_quic = (
        payload.get("transport") == "cxnfs-quic"
        or str(payload.get("bulk_transport") or "") == PYTHON_BULK_TRANSPORT_QUIC
    )
    minimum_runs = max(2, _int_value(_mapping(payload.get("blockfs_workload_requirements")).get("min_runs"), 0))
    minimum_batches = max(2, minimum_runs)
    minimum_cord_sweep = max(2, _int_value(_mapping(payload.get("blockfs_workload_requirements")).get("min_cord_sweep"), 0))
    minimum_observed_cords = _required_observed_cords_for_gate(payload)
    direct_destination_max_cords = _direct_destination_max_observed_cords(read_path)
    fanout_verified = _direct_destination_frame_sink_cord_fanout_verified(read_path)
    missing: list[str] = []
    if run_count < minimum_runs:
        missing.append("at least two workload runs are required")
    if total_batches < minimum_batches:
        missing.append("multiple trainer batches must be delivered across the workload")
    if min_run_batches <= 0:
        missing.append("every workload run must report at least one trainer batch")
    if sample_count <= 0:
        missing.append("workload samples must be read")
    if byte_size <= 0:
        missing.append("workload payload bytes must be read")
    if cord_sweep_count < minimum_cord_sweep:
        missing.append("a multi-cord workload sweep is required")
    if not bool(network_summary.get("java_python_network_path_exercised")) and requires_quic:
        missing.append("authenticated Java/Python QUIC network path must be exercised")
    if not bool(read_path.get("all_runs_have_evidence")):
        missing.append("all runs must include read-path evidence")
    if not bool(read_path.get("direct_destination_reads_observed")):
        missing.append("direct destination reads must be observed")
    if not bool(read_path.get("frame_sink_used_for_all_direct_destination_reads")):
        missing.append("all direct destination reads must use the bulk frame sink")
    if direct_destination_max_cords < minimum_observed_cords or not fanout_verified:
        missing.append("multi-cord direct destination delivery must be observed")
    if requires_quic and not bool(read_path.get("quic_read_transport_complete")):
        missing.append("QUIC read transport stream evidence must be complete")
    if requires_quic and not _quic_read_transport_stream_eof_verified(read_path):
        missing.append("QUIC read streams must reach EOF after payload delivery")
    if not bool(read_reliability.get("complete")):
        missing.append("read-path reliability must be complete")
    if _int_value(read_reliability.get("unrecovered_read_failures"), 0) > 0:
        missing.append("no unrecovered read failures are allowed")
    return {
        "format": TRAINER_SUSTAINED_READ_PROFILE_FORMAT,
        "complete": not missing,
        "missing": missing,
        "requires_quic_transport_evidence": requires_quic,
        "minimum_runs": minimum_runs,
        "minimum_batches": minimum_batches,
        "minimum_cord_sweep": minimum_cord_sweep,
        "minimum_observed_cords": minimum_observed_cords,
        "run_count": run_count,
        "total_batches": total_batches,
        "min_run_batches": min_run_batches,
        "sample_count": sample_count,
        "byte_size": byte_size,
        "cord_sweep_count": cord_sweep_count,
        "java_python_network_path_exercised": bool(network_summary.get("java_python_network_path_exercised")),
        "all_runs_have_read_path_evidence": bool(read_path.get("all_runs_have_evidence")),
        "direct_destination_reads_observed": bool(read_path.get("direct_destination_reads_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(
            read_path.get("frame_sink_used_for_all_direct_destination_reads")
        ),
        "direct_destination_max_observed_cords": direct_destination_max_cords,
        "direct_destination_frame_sink_cord_fanout_verified": fanout_verified,
        "direct_destination_parallel_frame_sink_read_operations": _int_value(
            read_path.get("direct_destination_parallel_frame_sink_read_operations"),
            0,
        ),
        "quic_read_transport_complete": bool(read_path.get("quic_read_transport_complete")),
        "quic_read_transport_stream_eof_verified": _quic_read_transport_stream_eof_verified(read_path),
        "quic_read_transport_evidence_count": _int_value(read_path.get("quic_read_transport_evidence_count"), 0),
        "quic_read_transport_read_streams_created": _int_value(
            read_path.get("quic_read_transport_read_streams_created"),
            0,
        ),
        "quic_read_transport_read_streams_eof_verified": _int_value(
            read_path.get("quic_read_transport_read_streams_eof_verified"),
            0,
        ),
        "quic_read_transport_read_stream_eof_report_count": _int_value(
            read_path.get("quic_read_transport_read_stream_eof_report_count"),
            0,
        ),
        "quic_read_transport_read_partition_count": _int_value(
            read_path.get("quic_read_transport_read_partition_count"),
            0,
        ),
        "quic_read_transport_read_partition_chunk_count": _int_value(
            read_path.get("quic_read_transport_read_partition_chunk_count"),
            0,
        ),
        "quic_read_transport_read_partition_payload_bytes": _int_value(
            read_path.get("quic_read_transport_read_partition_payload_bytes"),
            0,
        ),
        "quic_read_transport_read_partition_coverage_verified": bool(
            read_path.get("quic_read_transport_read_partition_coverage_verified")
        ),
        "quic_read_transport_expected_cipher_suite_count": _int_value(
            read_path.get("quic_read_transport_expected_cipher_suite_count"),
            0,
        ),
        "quic_read_transport_all_reports_expected_cipher_suite": bool(
            read_path.get("quic_read_transport_all_reports_expected_cipher_suite")
        ),
        "read_path_reliability_complete": bool(read_reliability.get("complete")),
        "unrecovered_read_failures": _int_value(read_reliability.get("unrecovered_read_failures"), 0),
    }


def _trainer_workload_target_bytes_per_second(payload: Mapping[str, Any]) -> float:
    objective = str(payload.get("objective") or "")
    if objective == AI_TRAINING_25GBE_OBJECTIVE:
        return float(AI_TRAINING_MINIMUM_BYTES_PER_SECOND)
    return 0.0


def _trainer_workload_network_summary(
    payload: Mapping[str, Any],
    cord_pairs: Sequence[tuple[int, int]],
    runs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    values_by_label: dict[str, list[float]] = {}
    for run in runs:
        label = str(run.get("cord_label") or run.get("max_cords") or "unknown")
        values_by_label.setdefault(label, []).append(float(run.get("mebibytes_per_second", 0.0)))
    averages = {
        label: (sum(values) / len(values) if values else 0.0)
        for label, values in values_by_label.items()
    }
    labels = [
        f"{initial}-{maximum}" if initial != maximum else str(initial)
        for initial, maximum in cord_pairs
    ]
    first_label = labels[0] if labels else None
    first_value = averages.get(first_label, 0.0) if first_label is not None else 0.0
    best_label = max(averages, key=averages.get) if averages else None
    best_value = averages.get(best_label, 0.0) if best_label is not None else 0.0
    scaling_improvement = (best_value / first_value - 1.0) if first_value > 0.0 else 0.0
    write_path_evidence = _mapping(payload.get("write_path_evidence"))
    remote_topology = _mapping(payload.get("remote_client_topology"))
    provenance = _mapping(payload.get("benchmark_provenance"))
    invocation = _mapping(provenance.get("invocation"))
    remote_root = payload.get("remote_root") or invocation.get("remote_root")
    topology_report_count = _int_value(remote_topology.get("topology_report_count"), 0)
    java_python_network_path_exercised = (
        payload.get("transport") in {"cxnfs-quic", "cxnfs-length-prefixed"}
        and bool(remote_root)
        and bool(remote_topology.get("remote_client_required"))
        and bool(remote_topology.get("complete"))
        and bool(remote_topology.get("per_source_client_complete"))
        and topology_report_count > 0
    )
    return {
        "java_python_network_path_exercised": java_python_network_path_exercised,
        "bulk_transport": payload.get("bulk_transport"),
        "cord_sweep_count": len(cord_pairs),
        "cord_average_mebibytes_per_second": averages,
        "first_cord_label": first_label,
        "best_cord_label": best_label,
        "best_cord_mebibytes_per_second": best_value,
        "cord_scaling_improvement_ratio": scaling_improvement,
        "remote_client_topology_complete": bool(remote_topology.get("complete")),
        "remote_client_per_source_complete": bool(remote_topology.get("per_source_client_complete")),
        "remote_client_topology_report_count": topology_report_count,
        "write_probe_enabled": bool(write_path_evidence.get("enabled")),
        "write_probe_completed": bool(write_path_evidence.get("complete")),
    }


def _blockfs_workload_requirements(args: Any) -> dict[str, Any]:
    return _blockfs_workload_requirements_from_values(
        min_runs=int(getattr(args, "blockfs_min_runs", 0) or 0),
        min_samples=int(getattr(args, "blockfs_min_samples", 0) or 0),
        min_bytes=int(getattr(args, "blockfs_min_bytes", 0) or 0),
        min_cord_sweep=int(getattr(args, "blockfs_min_cord_sweep", 0) or 0),
    )


def _blockfs_workload_requirements_from_values(
    *,
    min_runs: int,
    min_samples: int,
    min_bytes: int,
    min_cord_sweep: int,
) -> dict[str, Any]:
    configured = all(value > 0 for value in (min_runs, min_samples, min_bytes, min_cord_sweep))
    return {
        "configured": configured,
        "min_runs": min_runs,
        "min_samples": min_samples,
        "min_bytes": min_bytes,
        "min_cord_sweep": min_cord_sweep,
    }


def _trainer_workload_scale_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    requirements = _mapping(payload.get("blockfs_workload_requirements"))
    min_runs = _int_value(requirements.get("min_runs"), 0)
    min_samples = _int_value(requirements.get("min_samples"), 0)
    min_bytes = _int_value(requirements.get("min_bytes"), 0)
    min_cord_sweep = _int_value(requirements.get("min_cord_sweep"), 0)
    actual_runs = _int_value(payload.get("run_count"), 0)
    actual_samples = _int_value(payload.get("sample_count"), 0)
    actual_bytes = _int_value(payload.get("byte_size"), 0)
    actual_cord_sweep = _workload_cord_sweep_count(payload)
    missing: list[str] = []
    if not bool(requirements.get("configured")):
        missing.append("explicit blockfs workload minimums must be configured")
    if min_runs <= 0:
        missing.append("blockfs minimum runs must be positive")
    elif actual_runs < min_runs:
        missing.append("workload runs must meet the blockfs minimum")
    if min_samples <= 0:
        missing.append("blockfs minimum samples must be positive")
    elif actual_samples < min_samples:
        missing.append("workload samples must meet the blockfs minimum")
    if min_bytes <= 0:
        missing.append("blockfs minimum bytes must be positive")
    elif actual_bytes < min_bytes:
        missing.append("workload bytes must meet the blockfs minimum")
    if min_cord_sweep <= 0:
        missing.append("blockfs minimum cord sweep must be positive")
    elif actual_cord_sweep < min_cord_sweep:
        missing.append("workload cord sweep must meet the blockfs minimum")
    return {
        "complete": not missing,
        "missing": missing,
        "requirements": {
            "min_runs": min_runs,
            "min_samples": min_samples,
            "min_bytes": min_bytes,
            "min_cord_sweep": min_cord_sweep,
        },
        "observed": {
            "run_count": actual_runs,
            "sample_count": actual_samples,
            "byte_size": actual_bytes,
            "cord_sweep_count": actual_cord_sweep,
        },
    }


def _trainer_workload_blockfs_gate(
    payload: Mapping[str, Any],
    network_summary: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    read_path_gate = _trainer_workload_read_path_gate(payload)
    read_path_reliability = _mapping(payload.get("read_path_reliability")) or _trainer_read_path_reliability(payload)
    sustained_read_profile = (
        _mapping(payload.get("sustained_read_profile"))
        or _trainer_workload_sustained_read_profile(payload, network_summary)
    )
    memory_path_gate = _trainer_workload_memory_path_gate(payload)
    write_path_gate = _trainer_workload_write_path_gate(payload)
    workload_scale_gate = _trainer_workload_scale_gate(payload)

    def gate(decision: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return _blockfs_gate_payload(
            decision,
            reason,
            network_summary,
            evidence,
            read_path_reliability=read_path_reliability,
            sustained_read_profile=sustained_read_profile,
            **kwargs,
        )

    if payload.get("transport") != "cxnfs-quic":
        return gate(
            "defer",
            "Run the workload over remote QUIC before considering a Linux blockfs path.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if "objective" not in payload:
        return gate(
            "defer",
            "Run with an objective so the Java/Python path has a concrete target.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if payload.get("objective_passed_any") is True:
        return gate(
            "not-needed",
            "The Java/Python QUIC path already reaches the configured objective in at least one run.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if not bool(network_summary.get("java_python_network_path_exercised")):
        return gate(
            "prove-java-python-network-path",
            "The workload must prove the authenticated Java/Python QUIC path before Linux blockfs work can proceed.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if int(network_summary.get("cord_sweep_count", 0) or 0) < 2:
        return gate(
            "defer",
            "Run a multi-cord QUIC workload sweep before considering Linux blockfs work.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if float(network_summary.get("cord_scaling_improvement_ratio", 0.0)) > 0.10:
        return gate(
            "continue-quic-tuning",
            "Higher cord counts still improve throughput, so continue QUIC and Python path tuning first.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if not read_path_gate["complete"]:
        return gate(
            "prove-quic-read-path",
            "The workload must prove direct destination QUIC reads before Linux blockfs work can proceed.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
        )
    if not bool(read_path_reliability.get("complete")):
        return gate(
            "prove-quic-read-reliability",
            "The workload must prove bounded QUIC read retries recover transport failures without leaving failed reads.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if not bool(sustained_read_profile.get("complete")):
        return gate(
            "prove-sustained-quic-read-path",
            "The workload must prove sustained multi-run, multi-batch QUIC read delivery before Linux blockfs work can proceed.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if not memory_path_gate["complete"]:
        return gate(
            "prove-trainer-memory-path",
            "The workload must prove the selected trainer memory path before Linux blockfs work can proceed.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
        )
    if not write_path_gate["complete"]:
        return gate(
            "prove-quic-write-path",
            "The workload must prove direct tensor writes over QUIC before Linux blockfs work can proceed.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
        )
    if not workload_scale_gate["complete"]:
        return gate(
            "prove-workload-scale",
            "The workload report must meet explicit benchmark scale minimums before Linux blockfs work can proceed.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if not evidence.get("complete"):
        return gate(
            "profile-storage",
            "The QUIC cord sweep plateaued below the objective; collect Linux disk, page-cache, and CPU counters before allowing blockfs kernel work.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if not evidence.get("sample_window_complete"):
        return gate(
            "profile-storage",
            "The server evidence must include timestamped sample windows collected during the workload run.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    evidence_decision = _server_evidence_decision(evidence)
    if evidence_decision["decision"] == "allow-blockfs-prototype":
        return gate(
            "allow-blockfs-prototype",
            "Remote QUIC plateaued below the objective, disk pressure is present, and server CPU is not the primary limit.",
            kernel_module_allowed=True,
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    if evidence_decision["decision"] == "continue-java-python-cpu-profile":
        return gate(
            "continue-java-python-cpu-profile",
            "Server CPU evidence is saturated; continue Java/Python profiling before blockfs kernel work.",
            read_path_gate=read_path_gate,
            memory_path_gate=memory_path_gate,
            write_path_gate=write_path_gate,
            workload_scale_gate=workload_scale_gate,
        )
    return gate(
        "continue-userspace-profiling",
        "Server evidence is complete but does not yet prove storage pressure as the primary remaining limit.",
        read_path_gate=read_path_gate,
        memory_path_gate=memory_path_gate,
        write_path_gate=write_path_gate,
        workload_scale_gate=workload_scale_gate,
    )


def _trainer_workload_read_path_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    minimum_observed_cords = _required_observed_cords_for_gate(payload)
    evidence = _mapping(payload.get("read_path_evidence"))
    requires_quic_transport_evidence = (
        str(payload.get("transport") or "") == "cxnfs-quic"
        or str(payload.get("bulk_transport") or "") == PYTHON_BULK_TRANSPORT_QUIC
    )
    missing: list[str] = []
    if not evidence:
        missing.append("read_path_evidence")
        direct_destination_max_cords = 0
        direct_destination_fanout_verified = False
    else:
        direct_destination_max_cords = _direct_destination_max_observed_cords(evidence)
        direct_destination_fanout_verified = _direct_destination_frame_sink_cord_fanout_verified(evidence)
        if not bool(evidence.get("all_runs_have_evidence")):
            missing.append("all runs must include read-path evidence")
        if int(evidence.get("bulk_payload_transport_count", 0) or 0) <= 0:
            missing.append("bulk payload transport evidence must be present")
        if not bool(evidence.get("direct_destination_reads_observed")):
            missing.append("direct destination reads must be observed")
        if not bool(evidence.get("transport_frame_sink_observed")):
            missing.append("QUIC frame-sink reads must be observed")
        if not bool(evidence.get("frame_sink_used_for_all_direct_destination_reads")):
            missing.append("all direct destination reads must use the QUIC frame sink")
        if direct_destination_max_cords < minimum_observed_cords or not direct_destination_fanout_verified:
            missing.append("multi-cord QUIC delivery must be observed")
        if requires_quic_transport_evidence and not bool(evidence.get("quic_read_transport_complete")):
            missing.append("QUIC read transport stream evidence must be complete")
        if requires_quic_transport_evidence and not _quic_read_transport_stream_eof_verified(evidence):
            missing.append("QUIC read streams must reach EOF after payload delivery")
        if requires_quic_transport_evidence and not bool(
            evidence.get("quic_read_transport_all_reports_expected_cipher_suite")
        ):
            missing.append("QUIC read transport must use the required cipher suite")
    return {
        "complete": not missing,
        "missing": missing,
        "requires_quic_transport_evidence": requires_quic_transport_evidence,
        "minimum_observed_cords": minimum_observed_cords,
        "bulk_payload_transport_count": int(evidence.get("bulk_payload_transport_count", 0) or 0),
        "direct_destination_reads_observed": bool(evidence.get("direct_destination_reads_observed")),
        "transport_frame_sink_observed": bool(evidence.get("transport_frame_sink_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(
            evidence.get("frame_sink_used_for_all_direct_destination_reads")
        ),
        "max_observed_cords": int(evidence.get("max_observed_cords", 0) or 0),
        "direct_destination_max_observed_cords": direct_destination_max_cords,
        "direct_destination_frame_sink_cord_fanout_verified": direct_destination_fanout_verified,
        "direct_destination_parallel_frame_sink_read_operations": int(
            evidence.get("direct_destination_parallel_frame_sink_read_operations", 0) or 0
        ),
        "direct_destination_parallel_frame_sink_frames": int(
            evidence.get("direct_destination_parallel_frame_sink_frames", 0) or 0
        ),
        "quic_read_transport_complete": bool(evidence.get("quic_read_transport_complete")),
        "quic_read_transport_stream_eof_verified": _quic_read_transport_stream_eof_verified(evidence),
        "quic_read_transport_evidence_count": int(evidence.get("quic_read_transport_evidence_count", 0) or 0),
        "quic_read_transport_read_streams_created": int(
            evidence.get("quic_read_transport_read_streams_created", 0) or 0
        ),
        "quic_read_transport_read_streams_eof_verified": int(
            evidence.get("quic_read_transport_read_streams_eof_verified", 0) or 0
        ),
        "quic_read_transport_read_stream_eof_report_count": int(
            evidence.get("quic_read_transport_read_stream_eof_report_count", 0) or 0
        ),
        "quic_read_transport_connection_reuse_count": int(
            evidence.get("quic_read_transport_connection_reuse_count", 0) or 0
        ),
        "quic_read_transport_read_partition_count": int(
            evidence.get("quic_read_transport_read_partition_count", 0) or 0
        ),
        "quic_read_transport_read_partition_chunk_count": int(
            evidence.get("quic_read_transport_read_partition_chunk_count", 0) or 0
        ),
        "quic_read_transport_read_partition_payload_bytes": int(
            evidence.get("quic_read_transport_read_partition_payload_bytes", 0) or 0
        ),
        "quic_read_transport_read_partition_coverage_verified": bool(
            evidence.get("quic_read_transport_read_partition_coverage_verified")
        ),
        "quic_read_transport_expected_cipher_suite_count": int(
            evidence.get("quic_read_transport_expected_cipher_suite_count", 0) or 0
        ),
        "quic_read_transport_all_reports_expected_cipher_suite": bool(
            evidence.get("quic_read_transport_all_reports_expected_cipher_suite")
        ),
        "quic_read_transport_cipher_suite_counts": dict(
            _mapping(evidence.get("quic_read_transport_cipher_suite_counts"))
        ),
        "all_runs_have_evidence": bool(evidence.get("all_runs_have_evidence")),
    }


def _trainer_workload_memory_path_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    evidence = _mapping(payload.get("read_path_evidence"))
    read_mode = str(payload.get("read_mode") or "")
    tensor_allocation = _mapping(payload.get("tensor_allocation"))
    tensor_memory_path = _mapping(payload.get("tensor_memory_path"))
    direct_buffer_reads = int(evidence.get("direct_buffer_read_operations", 0) or 0)
    direct_tensor_reads = int(evidence.get("direct_tensor_read_operations", 0) or 0)
    direct_destination_reads = int(evidence.get("direct_destination_read_operations", 0) or 0)
    missing: list[str] = []
    if not evidence:
        missing.append("read_path_evidence")
    elif not bool(evidence.get("direct_destination_reads_observed")):
        missing.append("direct destination reads must be observed")
    if read_mode == TrainerReadMode.DIRECT_BUFFERS.value:
        if direct_buffer_reads <= 0:
            missing.append("direct buffer reads must be observed")
    elif read_mode == TrainerReadMode.DIRECT_TENSORS.value:
        if not tensor_allocation:
            missing.append("tensor allocation must be declared")
        else:
            if str(tensor_allocation.get("target") or "") not in TRAINER_TENSOR_TARGETS:
                missing.append("tensor allocation target must be supported")
            if not tensor_allocation.get("dtype"):
                missing.append("tensor allocation dtype must be declared")
            if not tensor_allocation.get("device"):
                missing.append("tensor allocation device must be declared")
        if direct_tensor_reads <= 0:
            missing.append("direct tensor reads must be observed")
        if not tensor_memory_path:
            missing.append("tensor memory path must be reported")
        elif not bool(tensor_memory_path.get("complete")):
            missing.append("tensor memory path must be complete")
    elif read_mode == TrainerReadMode.SAMPLES.value:
        missing.append("workload must use direct trainer reads")
    else:
        missing.append("workload read mode must be direct-buffers or direct-tensors")
    return {
        "complete": not missing,
        "missing": missing,
        "read_mode": read_mode,
        "tensor_allocation_declared": bool(tensor_allocation),
        "tensor_allocation_target": tensor_allocation.get("target"),
        "tensor_allocation_dtype": tensor_allocation.get("dtype"),
        "tensor_allocation_device": tensor_allocation.get("device"),
        "direct_buffer_read_operations": direct_buffer_reads,
        "direct_tensor_read_operations": direct_tensor_reads,
        "direct_destination_read_operations": direct_destination_reads,
        "direct_destination_reads_observed": bool(evidence.get("direct_destination_reads_observed")),
        "tensor_memory_path_format": tensor_memory_path.get("format"),
        "tensor_memory_path_complete": bool(tensor_memory_path.get("complete")),
        "tensor_memory_path_target": tensor_memory_path.get("target"),
        "network_fills_tensor_destination": bool(tensor_memory_path.get("network_fills_tensor_destination")),
        "requires_pinned_host_staging": bool(tensor_memory_path.get("requires_pinned_host_staging")),
        "device_copy_required": bool(tensor_memory_path.get("device_copy_required")),
    }


def _trainer_workload_write_path_gate(payload: Mapping[str, Any]) -> dict[str, Any]:
    minimum_observed_write_cords = _required_observed_cords_for_gate(payload)
    evidence = _mapping(payload.get("write_path_evidence"))
    run_count = _int_value(payload.get("run_count"), 0)
    required_probe_cord_sweep = min(2, max(1, _workload_cord_sweep_count(payload)))
    requires_quic_transport_evidence = (
        payload.get("transport") == "cxnfs-quic"
        or str(payload.get("bulk_transport") or "") == PYTHON_BULK_TRANSPORT_QUIC
    )
    missing: list[str] = []
    if not evidence:
        missing.append("write_path_evidence")
    else:
        if not bool(evidence.get("enabled")):
            missing.append("workload write probe must be enabled")
        if not bool(evidence.get("all_runs_have_evidence")):
            missing.append("all runs must include write-path evidence")
        if not bool(evidence.get("all_write_probes_verified")):
            missing.append("all write probes must verify read-back bytes")
        if not bool(evidence.get("direct_tensor_writes_observed")):
            missing.append("direct tensor writes must be observed")
        if int(evidence.get("write_frames", 0) or 0) <= 0:
            missing.append("write frames must be observed")
        if int(evidence.get("write_acks", 0) or 0) <= 0:
            missing.append("write acknowledgements must be observed")
        if not bool(evidence.get("write_ack_frame_identity_verified")):
            missing.append("write acknowledgements must match sent frame identity")
        if int(evidence.get("write_ack_frame_verified", 0) or 0) < int(evidence.get("write_frames", 0) or 0):
            missing.append("write acknowledgement identity verification must cover every frame")
        if not bool(evidence.get("metadata_pending_observed")):
            missing.append("payload acknowledgements must precede metadata verification")
        if int(evidence.get("metadata_after_payload_entries", 0) or 0) < max(1, run_count):
            missing.append("metadata-after-payload entries must cover every run")
        if int(evidence.get("entries_committed", 0) or 0) < max(1, run_count):
            missing.append("write entries must commit for every run")
        if int(evidence.get("transfers_committed", 0) or 0) < max(1, run_count):
            missing.append("write transfers must commit for every run")
        if int(evidence.get("max_observed_write_cords", 0) or 0) < minimum_observed_write_cords:
            missing.append("multi-cord QUIC writes must be observed")
        if int(evidence.get("write_probe_cord_sweep_count", 0) or 0) < required_probe_cord_sweep:
            missing.append("write probes must cover the workload cord sweep")
        if not bool(evidence.get("write_retry_attempts_recovered", True)):
            missing.append("write retry attempts must complete")
        if not bool(evidence.get("write_retry_reconnects_observed", True)):
            missing.append("write retry attempts must reconnect the bulk transport")
        if requires_quic_transport_evidence:
            required_active_write_streams = min(2, max(1, int(evidence.get("max_observed_write_cords", 0) or 0)))
            if int(evidence.get("quic_write_transport_evidence_count", 0) or 0) < max(1, run_count):
                missing.append("all QUIC write probes must include transport stream evidence")
            if int(evidence.get("quic_write_transport_failed_write_operations", 0) or 0) > 0:
                missing.append("QUIC write transport must have no failed write operations")
            if int(evidence.get("quic_write_transport_successful_write_operations", 0) or 0) < int(evidence.get("write_frames", 0) or 0):
                missing.append("QUIC write transport must account for every acknowledged frame")
            if int(evidence.get("quic_write_transport_write_streams_created", 0) or 0) < int(evidence.get("write_frames", 0) or 0):
                missing.append("QUIC write transport must create a stream for every frame")
            if int(evidence.get("quic_write_transport_max_active_write_streams", 0) or 0) < required_active_write_streams:
                missing.append("QUIC write transport must show concurrent active write streams")
            if not bool(evidence.get("quic_write_transport_concurrent_streams_observed")):
                missing.append("QUIC write transport must include a concurrent write-stream report")
            if not bool(evidence.get("quic_write_transport_all_reports_write_streams_succeeded")):
                missing.append("QUIC write transport must prove all attempted write streams succeeded")
            if not bool(evidence.get("quic_write_transport_all_reports_write_ack_stream_completion_verified")):
                missing.append("QUIC write transport must prove every ACK stream completed")
            if not bool(evidence.get("quic_write_transport_all_reports_write_ack_stream_eof_verified")):
                missing.append("QUIC write transport must prove every ACK stream reached EOF")
            attempted_write_streams = int(evidence.get("quic_write_transport_attempted_write_streams", 0) or 0)
            write_ack_streams_eof_verified = int(
                evidence.get("quic_write_transport_write_ack_streams_eof_verified", 0) or 0
            )
            if attempted_write_streams > 0 and write_ack_streams_eof_verified != attempted_write_streams:
                missing.append("QUIC write transport must EOF-verify every attempted ACK stream")
            if int(evidence.get("quic_write_transport_write_acks_received", 0) or 0) < int(evidence.get("write_frames", 0) or 0):
                missing.append("QUIC write transport must receive an ACK for every frame")
            if int(evidence.get("quic_write_transport_write_acks_verified", 0) or 0) < int(evidence.get("write_frames", 0) or 0):
                missing.append("QUIC write transport must verify an ACK for every frame")
            if int(evidence.get("quic_write_transport_write_ack_verification_failures", 0) or 0) > 0:
                missing.append("QUIC write transport must have no ACK verification failures")
            if not bool(evidence.get("quic_write_transport_all_reports_write_ack_frame_identity_verified")):
                missing.append("QUIC write transport must prove ACK frame identity for every report")
            if not bool(evidence.get("quic_write_transport_all_reports_reused_connection")):
                missing.append("QUIC write probes must reuse one connection across write streams")
            if not bool(evidence.get("quic_write_transport_all_reports_expected_cipher_suite")):
                missing.append("QUIC write transport must use the required cipher suite")
    return {
        "complete": not missing,
        "missing": missing,
        "minimum_observed_write_cords": minimum_observed_write_cords,
        "required_write_probe_cord_sweep": required_probe_cord_sweep,
        "requires_quic_transport_evidence": requires_quic_transport_evidence,
        "enabled": bool(evidence.get("enabled")),
        "write_probe_cord_sweep_count": int(evidence.get("write_probe_cord_sweep_count", 0) or 0),
        "write_probe_cord_labels": list(evidence.get("write_probe_cord_labels", ()))
        if isinstance(evidence.get("write_probe_cord_labels"), (list, tuple)) else [],
        "all_runs_have_evidence": bool(evidence.get("all_runs_have_evidence")),
        "all_write_probes_verified": bool(evidence.get("all_write_probes_verified")),
        "direct_tensor_writes_observed": bool(evidence.get("direct_tensor_writes_observed")),
        "metadata_pending_observed": bool(evidence.get("metadata_pending_observed")),
        "verified_read_back_count": int(evidence.get("verified_read_back_count", 0) or 0),
        "write_frames": int(evidence.get("write_frames", 0) or 0),
        "write_acks": int(evidence.get("write_acks", 0) or 0),
        "write_ack_frame_verified": int(evidence.get("write_ack_frame_verified", 0) or 0),
        "write_ack_frame_identity_verified": bool(evidence.get("write_ack_frame_identity_verified")),
        "metadata_after_payload_entries": int(evidence.get("metadata_after_payload_entries", 0) or 0),
        "entries_committed": int(evidence.get("entries_committed", 0) or 0),
        "transfers_committed": int(evidence.get("transfers_committed", 0) or 0),
        "payload_bytes_written": int(evidence.get("payload_bytes_written", 0) or 0),
        "max_observed_write_cords": int(evidence.get("max_observed_write_cords", 0) or 0),
        "write_retry_limit": int(evidence.get("write_retry_limit", 0) or 0),
        "write_retry_attempts": int(evidence.get("write_retry_attempts", 0) or 0),
        "write_retry_successes": int(evidence.get("write_retry_successes", 0) or 0),
        "bulk_transport_write_reconnects": int(evidence.get("bulk_transport_write_reconnects", 0) or 0),
        "write_retry_attempts_recovered": bool(evidence.get("write_retry_attempts_recovered", True)),
        "write_retry_reconnects_observed": bool(evidence.get("write_retry_reconnects_observed", True)),
        "quic_write_transport_evidence_count": int(evidence.get("quic_write_transport_evidence_count", 0) or 0),
        "quic_write_transport_connection_reuse_count": int(evidence.get("quic_write_transport_connection_reuse_count", 0) or 0),
        "quic_write_transport_successful_write_operations": int(
            evidence.get("quic_write_transport_successful_write_operations", 0) or 0
        ),
        "quic_write_transport_failed_write_operations": int(
            evidence.get("quic_write_transport_failed_write_operations", 0) or 0
        ),
        "quic_write_transport_attempted_write_streams": int(
            evidence.get("quic_write_transport_attempted_write_streams", 0) or 0
        ),
        "quic_write_transport_write_streams_created": int(
            evidence.get("quic_write_transport_write_streams_created", 0) or 0
        ),
        "quic_write_transport_max_active_write_streams": int(
            evidence.get("quic_write_transport_max_active_write_streams", 0) or 0
        ),
        "quic_write_transport_concurrent_stream_report_count": int(
            evidence.get("quic_write_transport_concurrent_stream_report_count", 0) or 0
        ),
        "quic_write_transport_concurrent_streams_observed": bool(
            evidence.get("quic_write_transport_concurrent_streams_observed")
        ),
        "quic_write_transport_write_stream_success_report_count": int(
            evidence.get("quic_write_transport_write_stream_success_report_count", 0) or 0
        ),
        "quic_write_transport_all_reports_write_streams_succeeded": bool(
            evidence.get("quic_write_transport_all_reports_write_streams_succeeded")
        ),
        "quic_write_transport_write_ack_stream_completion_report_count": int(
            evidence.get("quic_write_transport_write_ack_stream_completion_report_count", 0) or 0
        ),
        "quic_write_transport_write_ack_stream_eof_report_count": int(
            evidence.get("quic_write_transport_write_ack_stream_eof_report_count", 0) or 0
        ),
        "quic_write_transport_write_ack_streams_eof_verified": int(
            evidence.get("quic_write_transport_write_ack_streams_eof_verified", 0) or 0
        ),
        "quic_write_transport_all_reports_write_ack_stream_completion_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_stream_completion_verified")
        ),
        "quic_write_transport_all_reports_write_ack_stream_eof_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_stream_eof_verified")
        ),
        "quic_write_transport_write_acks_received": int(
            evidence.get("quic_write_transport_write_acks_received", 0) or 0
        ),
        "quic_write_transport_write_acks_verified": int(
            evidence.get("quic_write_transport_write_acks_verified", 0) or 0
        ),
        "quic_write_transport_write_ack_verification_failures": int(
            evidence.get("quic_write_transport_write_ack_verification_failures", 0) or 0
        ),
        "quic_write_transport_write_ack_identity_report_count": int(
            evidence.get("quic_write_transport_write_ack_identity_report_count", 0) or 0
        ),
        "quic_write_transport_all_reports_write_ack_frame_identity_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_frame_identity_verified")
        ),
        "quic_write_transport_all_reports_reused_connection": bool(
            evidence.get("quic_write_transport_all_reports_reused_connection")
        ),
        "quic_write_transport_expected_cipher_suite_count": int(
            evidence.get("quic_write_transport_expected_cipher_suite_count", 0) or 0
        ),
        "quic_write_transport_all_reports_expected_cipher_suite": bool(
            evidence.get("quic_write_transport_all_reports_expected_cipher_suite")
        ),
        "quic_write_transport_cipher_suite_counts": dict(
            _mapping(evidence.get("quic_write_transport_cipher_suite_counts"))
        ),
        "quic_write_transport_complete": bool(evidence.get("quic_write_transport_complete")),
    }


def _blockfs_gate_payload(
    decision: str,
    reason: str,
    network_summary: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    kernel_module_allowed: bool = False,
    read_path_gate: Mapping[str, Any] | None = None,
    read_path_reliability: Mapping[str, Any] | None = None,
    sustained_read_profile: Mapping[str, Any] | None = None,
    memory_path_gate: Mapping[str, Any] | None = None,
    write_path_gate: Mapping[str, Any] | None = None,
    workload_scale_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    missing = tuple(str(value) for value in evidence.get("missing", ()))
    read_path = dict(read_path_gate) if read_path_gate is not None else {
        "complete": False,
        "missing": [],
        "minimum_observed_cords": 2,
        "bulk_payload_transport_count": 0,
        "direct_destination_reads_observed": False,
        "transport_frame_sink_observed": False,
        "frame_sink_used_for_all_direct_destination_reads": False,
        "max_observed_cords": 0,
        "direct_destination_max_observed_cords": 0,
        "direct_destination_frame_sink_cord_fanout_verified": False,
        "direct_destination_parallel_frame_sink_read_operations": 0,
        "direct_destination_parallel_frame_sink_frames": 0,
        "quic_read_transport_expected_cipher_suite_count": 0,
        "quic_read_transport_all_reports_expected_cipher_suite": False,
        "quic_read_transport_cipher_suite_counts": {},
        "all_runs_have_evidence": False,
    }
    read_reliability = dict(read_path_reliability) if read_path_reliability is not None else {
        "format": TRAINER_READ_PATH_RELIABILITY_FORMAT,
        "complete": False,
        "missing": ["read_path_reliability"],
        "unrecovered_read_failures": 0,
    }
    sustained_read = dict(sustained_read_profile) if sustained_read_profile is not None else {
        "format": TRAINER_SUSTAINED_READ_PROFILE_FORMAT,
        "complete": False,
        "missing": ["sustained_read_profile"],
        "run_count": 0,
        "total_batches": 0,
        "sample_count": 0,
        "byte_size": 0,
    }
    memory_path = dict(memory_path_gate) if memory_path_gate is not None else {
        "complete": False,
        "missing": [],
        "read_mode": "",
        "tensor_allocation_declared": False,
        "tensor_allocation_target": None,
        "tensor_allocation_dtype": None,
        "tensor_allocation_device": None,
        "direct_buffer_read_operations": 0,
        "direct_tensor_read_operations": 0,
        "direct_destination_read_operations": 0,
        "direct_destination_reads_observed": False,
    }
    write_path = dict(write_path_gate) if write_path_gate is not None else {
        "complete": False,
        "missing": [],
        "minimum_observed_write_cords": 2,
        "required_write_probe_cord_sweep": 1,
        "requires_quic_transport_evidence": False,
        "enabled": False,
        "write_probe_cord_sweep_count": 0,
        "write_probe_cord_labels": [],
        "all_runs_have_evidence": False,
        "all_write_probes_verified": False,
        "direct_tensor_writes_observed": False,
        "metadata_pending_observed": False,
        "verified_read_back_count": 0,
        "write_frames": 0,
        "write_acks": 0,
        "metadata_after_payload_entries": 0,
        "entries_committed": 0,
        "transfers_committed": 0,
        "payload_bytes_written": 0,
        "max_observed_write_cords": 0,
        "quic_write_transport_evidence_count": 0,
        "quic_write_transport_connection_reuse_count": 0,
        "quic_write_transport_successful_write_operations": 0,
        "quic_write_transport_failed_write_operations": 0,
        "quic_write_transport_attempted_write_streams": 0,
        "quic_write_transport_write_streams_created": 0,
        "quic_write_transport_write_ack_streams_eof_verified": 0,
        "quic_write_transport_max_active_write_streams": 0,
        "quic_write_transport_all_reports_reused_connection": False,
        "quic_write_transport_expected_cipher_suite_count": 0,
        "quic_write_transport_all_reports_expected_cipher_suite": False,
        "quic_write_transport_cipher_suite_counts": {},
        "quic_write_transport_complete": False,
    }
    workload_scale = dict(workload_scale_gate) if workload_scale_gate is not None else {
        "complete": False,
        "missing": [],
        "requirements": {},
        "observed": {},
    }
    missing_read_path = tuple(str(value) for value in read_path.get("missing", ()))
    missing_read_reliability = tuple(str(value) for value in read_reliability.get("missing", ()))
    missing_sustained_read = tuple(str(value) for value in sustained_read.get("missing", ()))
    missing_memory_path = tuple(str(value) for value in memory_path.get("missing", ()))
    missing_write_path = tuple(str(value) for value in write_path.get("missing", ()))
    missing_workload_scale = tuple(str(value) for value in workload_scale.get("missing", ()))
    missing_sample_windows = tuple(str(value) for value in evidence.get("missing_sample_windows", ()))
    missing_network_path = (
        ()
        if bool(network_summary.get("java_python_network_path_exercised"))
        else ("authenticated Java/Python remote QUIC workload proof",)
    )
    return {
        "decision": decision,
        "kernel_module_allowed": kernel_module_allowed,
        "reason": reason,
        "java_python_network_path_exercised": bool(network_summary.get("java_python_network_path_exercised")),
        "cord_sweep_count": int(network_summary.get("cord_sweep_count", 0)),
        "read_path_evidence_complete": bool(read_path.get("complete")),
        "read_path_gate": read_path,
        "read_path_reliability_complete": bool(read_reliability.get("complete")),
        "read_path_reliability": read_reliability,
        "sustained_read_profile_complete": bool(sustained_read.get("complete")),
        "sustained_read_profile": sustained_read,
        "memory_path_evidence_complete": bool(memory_path.get("complete")),
        "memory_path_gate": memory_path,
        "write_path_evidence_complete": bool(write_path.get("complete")),
        "write_path_gate": write_path,
        "workload_scale_complete": bool(workload_scale.get("complete")),
        "workload_scale_gate": workload_scale,
        "server_evidence_complete": bool(evidence.get("complete")),
        "server_evidence_sample_window_complete": bool(evidence.get("sample_window_complete")),
        "server_evidence_sample_window": dict(_mapping(evidence.get("sample_window"))),
        "missing_server_evidence": list(missing),
        "missing_server_evidence_sample_windows": list(missing_sample_windows),
        "required_next_evidence": _blockfs_required_next_evidence(
            kernel_module_allowed,
            missing_network_path,
            missing,
            missing_sample_windows,
            missing_read_path,
            missing_read_reliability,
            missing_sustained_read,
            missing_memory_path,
            missing_write_path,
            missing_workload_scale,
        ),
    }


def _blockfs_required_next_evidence(
    kernel_module_allowed: bool,
    missing_network_path_evidence: Sequence[str],
    missing_server_evidence: Sequence[str],
    missing_server_evidence_sample_windows: Sequence[str],
    missing_read_path_evidence: Sequence[str],
    missing_read_path_reliability: Sequence[str],
    missing_sustained_read_profile: Sequence[str],
    missing_memory_path_evidence: Sequence[str],
    missing_write_path_evidence: Sequence[str],
    missing_workload_scale_evidence: Sequence[str],
) -> list[str]:
    if kernel_module_allowed:
        return [
            "blockfs VM regression report",
            "kernel module rollback plan",
            "repeat remote workload benchmark against blockfs candidate",
        ]
    required = [
        "real weighted dataset benchmark report",
        "remote QUIC workload report",
    ]
    required.extend(missing_network_path_evidence)
    required.extend(missing_read_path_evidence)
    required.extend(missing_read_path_reliability)
    required.extend(missing_sustained_read_profile)
    required.extend(missing_memory_path_evidence)
    required.extend(missing_write_path_evidence)
    required.extend(missing_workload_scale_evidence)
    required.extend(f"server {name} evidence" for name in missing_server_evidence)
    required.extend(f"server {name} evidence sample window" for name in missing_server_evidence_sample_windows)
    return required


def _trainer_workload_server_evidence(args) -> dict[str, Any]:
    paths = {
        "disk": getattr(args, "server_disk_evidence", None),
        "cache": getattr(args, "server_cache_evidence", None),
        "cpu": getattr(args, "server_cpu_evidence", None),
    }
    reports: dict[str, Any] = {}
    missing: list[str] = []
    for name, raw_path in paths.items():
        if not raw_path:
            missing.append(name)
            reports[name] = None
            continue
        path = Path(raw_path)
        reports[name] = _read_server_evidence(path)
    sample_window = _server_evidence_sample_window(reports)
    return {
        "complete": not missing,
        "missing": missing,
        "reports": reports,
        "sample_window_complete": bool(sample_window.get("complete")) and not missing,
        "sample_window": sample_window,
        "missing_sample_windows": list(sample_window.get("missing", ())),
    }


def _server_evidence_sample_window(reports: Mapping[str, Any]) -> dict[str, Any]:
    per_report: dict[str, dict[str, float]] = {}
    missing: list[str] = []
    for name in ("disk", "cache", "cpu"):
        fields = _evidence_fields(reports, name)
        start = _evidence_number(fields, ("sample_start_unix_seconds", "start_unix_seconds"), None)
        end = _evidence_number(fields, ("sample_end_unix_seconds", "end_unix_seconds"), None)
        if start is None or end is None or end < start:
            missing.append(name)
            continue
        per_report[name] = {
            "sample_start_unix_seconds": float(start),
            "sample_end_unix_seconds": float(end),
        }
    starts = [value["sample_start_unix_seconds"] for value in per_report.values()]
    ends = [value["sample_end_unix_seconds"] for value in per_report.values()]
    return {
        "complete": not missing and len(per_report) == 3,
        "missing": missing,
        "sample_start_unix_seconds": min(starts) if starts else None,
        "sample_end_unix_seconds": max(ends) if ends else None,
        "reports": per_report,
    }


def _read_server_evidence(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        raise ValueError(f"server evidence report is empty: {path}")
    if stripped.startswith("{"):
        parsed = json.loads(stripped)
        if not isinstance(parsed, dict):
            raise ValueError(f"server evidence JSON must be an object: {path}")
        return {
            "path": str(path),
            "format": "json",
            "fields": parsed,
        }
    return {
        "path": str(path),
        "format": "key-value",
        "fields": _parse_key_value_evidence(stripped),
    }


def _parse_key_value_evidence(text: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    format_line: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            if format_line is None:
                format_line = line
            continue
        key, value = line.split("=", 1)
        fields[key.strip()] = _parse_evidence_scalar(value.strip())
    if format_line is not None:
        fields.setdefault("format", format_line)
    return fields


def _parse_evidence_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "yes"}:
        return True
    if lowered in {"false", "no"}:
        return False
    if lowered in {"none", "null"}:
        return None
    try:
        if any(marker in value for marker in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def _server_evidence_decision(evidence: Mapping[str, Any]) -> dict[str, Any]:
    reports = evidence.get("reports", {})
    disk = _evidence_fields(reports, "disk")
    cache = _evidence_fields(reports, "cache")
    cpu = _evidence_fields(reports, "cpu")
    disk_pressure = _evidence_bool(disk, ("disk_saturated", "storage_saturated", "io_saturated"))
    if disk_pressure is None:
        disk_pressure = _evidence_number(disk, ("disk_busy_percent", "utilization_percent", "io_util_percent"), 0.0) >= 85.0
    cpu_saturated = _evidence_bool(cpu, ("cpu_saturated", "server_cpu_saturated"))
    if cpu_saturated is None:
        idle = _evidence_number(cpu, ("cpu_idle_percent", "idle_percent"), None)
        busy = _evidence_number(cpu, ("cpu_busy_percent", "busy_percent"), None)
        cpu_saturated = (busy is not None and busy >= 90.0) or (idle is not None and idle <= 10.0)
    cache_effective = _evidence_bool(cache, ("cache_effective", "page_cache_effective"))
    if cache_effective is None:
        hit_ratio = _evidence_number(cache, ("cache_hit_ratio", "page_cache_hit_ratio"), None)
        cache_effective = hit_ratio is not None and hit_ratio >= 0.75
    if cpu_saturated:
        return {"decision": "continue-java-python-cpu-profile"}
    if disk_pressure and not cache_effective:
        return {"decision": "allow-blockfs-prototype"}
    return {"decision": "continue-userspace-profiling"}


def _evidence_fields(reports: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(reports, Mapping):
        return {}
    report = reports.get(name)
    if not isinstance(report, Mapping):
        return {}
    fields = report.get("fields")
    return fields if isinstance(fields, Mapping) else {}


def _evidence_bool(fields: Mapping[str, Any], keys: Sequence[str]) -> bool | None:
    for key in keys:
        value = fields.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"true", "false", "yes", "no"}:
            return value.lower() in {"true", "yes"}
    return None


def _evidence_number(fields: Mapping[str, Any], keys: Sequence[str], default: float | None) -> float | None:
    for key in keys:
        value = fields.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return default


def _parse_cord_sweep(value: str | None, initial_cords: int, max_cords: int) -> tuple[tuple[int, int], ...]:
    if value is None or not value.strip():
        CxnfsBulkReadConfig(initial_cords=initial_cords, max_cords=max_cords)
        return ((initial_cords, max_cords),)
    if value.strip().lower() == "auto":
        CxnfsBulkReadConfig(initial_cords=initial_cords, max_cords=max_cords)
        counts = [count for count in (1, 8, 16, 32, 64, ABSOLUTE_MAX_CORDS) if count <= max_cords]
        if max_cords not in counts:
            counts.append(max_cords)
            counts.sort()
        return tuple((count, count) for count in counts)
    pairs = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        count = int(part)
        CxnfsBulkReadConfig(initial_cords=count, max_cords=count)
        pairs.append((count, count))
    if not pairs:
        raise ValueError("cord-sweep must include at least one cord count")
    return tuple(pairs)


def _parse_trainer_surface_matrix_read_modes(value: str) -> tuple[TrainerReadMode, ...]:
    modes = []
    allowed = {TrainerReadMode.DIRECT_BUFFERS, TrainerReadMode.DIRECT_TENSORS}
    for raw_part in str(value or "").split(","):
        part = raw_part.strip()
        if not part:
            continue
        mode = TrainerReadMode(part)
        if mode not in allowed:
            raise ValueError("trainer surface matrix requires direct-buffers or direct-tensors")
        if mode not in modes:
            modes.append(mode)
    if not modes:
        raise ValueError("read-modes must include at least one direct trainer read mode")
    return tuple(modes)


def _parse_trainer_surface_matrix_iterators(value: str) -> tuple[str, ...]:
    iterators = []
    for raw_part in str(value or "").split(","):
        iterator = raw_part.strip()
        if not iterator:
            continue
        if iterator not in TRAINER_BATCH_ITERATORS:
            raise ValueError(f"unsupported trainer batch iterator: {iterator}")
        if iterator not in iterators:
            iterators.append(iterator)
    if not iterators:
        raise ValueError("trainer-batch-iterators must include at least one iterator")
    return tuple(iterators)


def _run_trainer_surface_matrix_case(
    args,
    read_mode: TrainerReadMode,
    trainer_batch_iterator: str,
    remote_options: Mapping[str, Any],
    *,
    credentials: ClientCredentials | None,
    tls_context: Any,
    server_hostname: str,
    read_config: CxnfsBulkReadConfig,
    bulk_transport: str,
    bulk_read_retries: int,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
) -> dict[str, Any]:
    case_args = Namespace(**vars(args))
    case_args.read_mode = read_mode.value
    case_args.trainer_batch_iterator = trainer_batch_iterator
    case_args.data_loader_workers = (
        int(args.data_loader_workers or 0)
        if trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER
        else 0
    )
    started = time.perf_counter()
    try:
        _validate_trainer_batch_iterator_args(case_args)
        if case_args.remote_root:
            if credentials is None:
                raise ValueError("remote trainer surface matrix requires credentials")
            client_factory = _trainer_stream_remote_client_factory(
                credentials,
                case_args.host,
                case_args.control_port,
                case_args.transport_port,
                case_args.timeout,
                tls_context,
                server_hostname,
                read_config,
                case_args.write_connections,
                bulk_read_retries,
                None,
                case_args.shared_client,
                bulk_transport,
                case_args.cafile,
                control_client_factory,
                bulk_client_factory,
            )
            sources = weighted_mds_sources_from_root(
                case_args.root,
                case_args.weights,
                decoder=case_args.decoder,
                client_factory=client_factory,
                remote_root=case_args.remote_root,
            )
        else:
            sources = weighted_mds_sources_from_root(
                case_args.root,
                case_args.weights,
                decoder=case_args.decoder,
            )
        report = _run_trainer_stream_smoke(
            case_args,
            sources,
            remote_options,
            client_factory=client_factory if case_args.remote_root else None,
        )
        _apply_trainer_stream_objective(report, case_args.objective, case_args.baseline_mb_s)
        return _trainer_surface_matrix_case_report(
            case_args,
            read_mode,
            trainer_batch_iterator,
            report=report,
            elapsed_seconds=max(time.perf_counter() - started, 1e-9),
        )
    except Exception as exc:
        return _trainer_surface_matrix_case_report(
            case_args,
            read_mode,
            trainer_batch_iterator,
            error=exc,
            elapsed_seconds=max(time.perf_counter() - started, 1e-9),
        )


def _trainer_surface_matrix_case_report(
    args,
    read_mode: TrainerReadMode,
    trainer_batch_iterator: str,
    *,
    report: Mapping[str, Any] | None = None,
    error: Exception | None = None,
    elapsed_seconds: float,
) -> dict[str, Any]:
    stream_report = _mapping(report)
    memory_gate = _trainer_workload_memory_path_gate(stream_report) if stream_report else {
        "complete": False,
        "missing": ["trainer surface case failed before read evidence"],
    }
    objective_passed = stream_report.get("objective_passed") if stream_report else None
    worker_partition = _mapping(stream_report.get("worker_partition_evidence"))
    worker_partition_complete = (
        _int_value(getattr(args, "data_loader_workers", 0), 0) <= 0
        or worker_partition.get("complete") is True
    )
    passed = bool(
        error is None
        and stream_report
        and int(stream_report.get("batch_count", 0) or 0) > 0
        and int(stream_report.get("sample_count", 0) or 0) > 0
        and memory_gate.get("complete")
        and _mapping(stream_report.get("read_path_reliability")).get("complete") is True
        and worker_partition_complete
        and (not getattr(args, "fail_on_miss", False) or objective_passed is not False)
    )
    read_path_evidence = _mapping(stream_report.get("read_path_evidence"))
    read_path_reliability = _mapping(stream_report.get("read_path_reliability"))
    remote_topology = _mapping(stream_report.get("remote_client_topology"))
    return {
        "case_name": f"{read_mode.value}/{trainer_batch_iterator}",
        "status": "passed" if passed else "failed",
        "passed": passed,
        "read_mode": read_mode.value,
        "destination_type": _trainer_contract_destination_type(read_mode),
        "trainer_batch_iterator": trainer_batch_iterator,
        "data_loader_workers": int(getattr(args, "data_loader_workers", 0) or 0),
        "training_shard": dict(_mapping(stream_report.get("training_shard"))),
        "rank": stream_report.get("rank"),
        "world_size": stream_report.get("world_size"),
        "trainer_prefetch_batches": _validate_trainer_prefetch_batches(
            getattr(args, "trainer_prefetch_batches", 0),
        ),
        "trainer_prefetch_enabled": bool(stream_report.get("trainer_prefetch_enabled")),
        "bulk_read_retries": _validate_bulk_read_retries(
            getattr(args, "bulk_read_retries", 0),
        ),
        "tensor_allocation": (
            _trainer_tensor_allocation_from_args(args).to_dict()
            if read_mode == TrainerReadMode.DIRECT_TENSORS
            else None
        ),
        "elapsed_seconds": elapsed_seconds,
        "batch_count": int(stream_report.get("batch_count", 0) or 0),
        "sample_count": int(stream_report.get("sample_count", 0) or 0),
        "byte_size": int(stream_report.get("byte_size", 0) or 0),
        "source_counts": _matrix_source_counts(stream_report.get("batches")),
        "read_path_evidence": _trainer_surface_matrix_read_evidence_summary(read_path_evidence),
        "read_path_reliability": dict(read_path_reliability),
        "remote_client_topology": dict(remote_topology),
        "worker_partition_evidence": dict(worker_partition),
        "memory_path_gate": dict(memory_gate),
        "tensor_memory_path": dict(_mapping(stream_report.get("tensor_memory_path"))),
        "objective_passed": objective_passed,
        "error_type": type(error).__name__ if error is not None else None,
        "error": str(error) if error is not None else None,
    }


def _trainer_surface_matrix_read_evidence_summary(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bulk_payload_transport_count": int(evidence.get("bulk_payload_transport_count", 0) or 0),
        "direct_buffer_read_operations": int(evidence.get("direct_buffer_read_operations", 0) or 0),
        "direct_tensor_read_operations": int(evidence.get("direct_tensor_read_operations", 0) or 0),
        "direct_destination_read_operations": int(evidence.get("direct_destination_read_operations", 0) or 0),
        "direct_destination_reads_observed": bool(evidence.get("direct_destination_reads_observed")),
        "transport_frame_sink_observed": bool(evidence.get("transport_frame_sink_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(
            evidence.get("frame_sink_used_for_all_direct_destination_reads")
        ),
        "max_observed_cords": int(evidence.get("max_observed_cords", 0) or 0),
        "direct_destination_max_observed_cords": _direct_destination_max_observed_cords(evidence),
        "direct_destination_frame_sink_cord_fanout_verified": (
            _direct_destination_frame_sink_cord_fanout_verified(evidence)
        ),
        "direct_destination_parallel_frame_sink_read_operations": int(
            evidence.get("direct_destination_parallel_frame_sink_read_operations", 0) or 0
        ),
        "direct_destination_parallel_frame_sink_frames": int(
            evidence.get("direct_destination_parallel_frame_sink_frames", 0) or 0
        ),
        "quic_read_transport_observed": bool(evidence.get("quic_read_transport_observed")),
        "quic_read_transport_complete": bool(evidence.get("quic_read_transport_complete")),
        "quic_read_transport_evidence_count": int(evidence.get("quic_read_transport_evidence_count", 0) or 0),
        "quic_read_transport_connection_reuse_count": int(
            evidence.get("quic_read_transport_connection_reuse_count", 0) or 0
        ),
        "quic_read_transport_read_streams_created": int(
            evidence.get("quic_read_transport_read_streams_created", 0) or 0
        ),
        "quic_read_transport_read_streams_eof_verified": int(
            evidence.get("quic_read_transport_read_streams_eof_verified", 0) or 0
        ),
        "quic_read_transport_max_read_stream_count": int(
            evidence.get("quic_read_transport_max_read_stream_count", 0) or 0
        ),
        "uses_caller_owned_destinations": bool(evidence.get("uses_caller_owned_destinations")),
    }


def _matrix_source_counts(batches: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for batch in batches if isinstance(batches, Sequence) else ():
        data = _mapping(batch)
        for name, count in _mapping(data.get("source_counts")).items():
            counts[str(name)] = counts.get(str(name), 0) + int(count or 0)
    return counts


def _trainer_surface_matrix_report(
    args,
    plan: CxnfsWeightedMdsPlan,
    remote_options: Mapping[str, Any],
    cases: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    plan_payload = plan.to_dict()
    passed_cases = [case for case in cases if bool(case.get("passed"))]
    objective_values = [
        bool(case.get("objective_passed"))
        for case in cases
        if case.get("objective_passed") is not None
    ]
    read_reliability_reports = [
        _mapping(case.get("read_path_reliability"))
        for case in cases
        if isinstance(case.get("read_path_reliability"), Mapping)
    ]
    read_path_reliability_complete = bool(cases) and len(read_reliability_reports) == len(cases) and all(
        report.get("complete") is True for report in read_reliability_reports
    )
    worker_partition_reports = [
        _mapping(case.get("worker_partition_evidence"))
        for case in cases
        if isinstance(case.get("worker_partition_evidence"), Mapping)
    ]
    worker_partition_required = any(
        str(case.get("trainer_batch_iterator") or "") == TRAINER_BATCH_ITERATOR_DATALOADER
        and _int_value(case.get("data_loader_workers"), 0) > 0
        for case in cases
    )
    worker_partition_complete = (
        not worker_partition_required
        or (
            bool(worker_partition_reports)
            and all(report.get("complete") is True for report in worker_partition_reports)
        )
    )
    remote_client_topology = _aggregate_trainer_remote_client_topology(
        cases,
        _weighted_plan_source_count(plan_payload),
        bool(args.remote_root),
    )
    active_training_shard = (
        dict(_mapping(cases[0].get("training_shard")))
        if cases and isinstance(cases[0].get("training_shard"), Mapping)
        else _training_shard_payload(_training_shard_from_args(args) or current_training_shard())
    )
    active_rank = _int_value(active_training_shard.get("index"), 0)
    active_world_size = _int_value(active_training_shard.get("count"), 1)
    payload = {
        "format": TRAINER_SURFACE_MATRIX_FORMAT,
        "root": str(args.root),
        "remote_root": _normalize_remote_root(args.remote_root) if args.remote_root else None,
        "weights": parse_weighted_mds_spec(args.weights),
        "weighted_mds_plan": plan_payload,
        "workload_fingerprint": plan_payload["workload_fingerprint"],
        "transport": remote_options.get("transport"),
        "bulk_transport": remote_options.get("bulk_transport"),
        "requires_mount": False,
        "touches_trainer_code": False,
        "case_count": len(cases),
        "passed_case_count": len(passed_cases),
        "failed_case_count": len(cases) - len(passed_cases),
        "passed": len(cases) > 0 and len(passed_cases) == len(cases),
        "status": "passed" if len(cases) > 0 and len(passed_cases) == len(cases) else "failed",
        "read_modes": [case.get("read_mode") for case in cases],
        "trainer_batch_iterators": [case.get("trainer_batch_iterator") for case in cases],
        "batch_size": args.batch_size,
        "requested_batches": args.batches,
        "warmup_batches": args.warmup_batches,
        "rounds_per_read": args.rounds_per_read,
        "max_workers": args.max_workers,
        "source_read_workers": args.source_read_workers,
        "prefetch_chunks": args.prefetch_chunks,
        "trainer_prefetch_batches": args.trainer_prefetch_batches,
        "bulk_read_retries": getattr(args, "bulk_read_retries", 0),
        "data_loader_workers": args.data_loader_workers,
        "training_shard": active_training_shard,
        "rank": active_rank,
        "world_size": active_world_size,
        "tensor_allocation": _trainer_tensor_allocation_from_args(args).to_dict(),
        "objective": args.objective,
        "objective_passed_all": all(objective_values) if objective_values else None,
        "remote_client_topology": remote_client_topology,
        "remote_client_topology_complete": bool(remote_client_topology.get("complete")),
        "remote_client_per_source_complete": bool(remote_client_topology.get("per_source_client_complete")),
        "read_path_reliability_complete": read_path_reliability_complete,
        "worker_partition_required": worker_partition_required,
        "worker_partition_complete": worker_partition_complete,
        "worker_partition_duplicate_source_sample_count": sum(
            _int_value(report.get("duplicate_source_sample_count"), 0)
            for report in worker_partition_reports
        ),
        "unrecovered_read_failures": sum(
            _int_value(report.get("unrecovered_read_failures"), 0)
            for report in read_reliability_reports
        ),
        "read_retry_successes": sum(
            _int_value(report.get("read_retry_successes"), 0)
            for report in read_reliability_reports
        ),
        "bulk_transport_reconnects": sum(
            _int_value(report.get("bulk_transport_reconnects"), 0)
            for report in read_reliability_reports
        ),
        "cases": [dict(case) for case in cases],
    }
    return payload


def _run_trainer_pipeline_smoke(
    args,
    sources: Sequence[CxnfsWeightedSource],
    pipeline: CxnfsTrainingPipeline,
    remote_options: Mapping[str, Any],
    *,
    client_factory: DatasetClientFactory | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] | None = None
    dataset = CxnfsWeightedTorchDataset(
        sources,
        repeat=getattr(args, "repeat", True),
        rounds_per_read=args.rounds_per_read,
        max_workers=args.max_workers,
        source_read_workers=args.source_read_workers,
        prefetch_chunks=args.prefetch_chunks,
        include_source_metadata=False,
        shard=_training_shard_from_args(args),
        shuffle=args.shuffle,
        seed=args.seed,
        samples_per_epoch=args.samples_per_epoch,
    )
    dataset.set_epoch(args.epoch)
    try:
        tensor_allocation = _trainer_tensor_allocation_from_args(args)
        source_path_prefixes = _trainer_pipeline_source_path_prefixes(sources, remote_options)
        pipeline_prefetch_batches = _validate_pipeline_prefetch_batches(
            getattr(args, "pipeline_prefetch_batches", 0),
        )
        if args.warmup_batches:
            _collect_trainer_pipeline_tensor_batches(
                dataset,
                pipeline,
                args,
                tensor_allocation,
                source_path_prefixes,
                args.warmup_batches,
                pipeline_prefetch_batches,
            )
        start = time.perf_counter()
        batches = _collect_trainer_pipeline_tensor_batches(
            dataset,
            pipeline,
            args,
            tensor_allocation,
            source_path_prefixes,
            args.batches,
            pipeline_prefetch_batches,
        )
        elapsed_seconds = max(time.perf_counter() - start, 1e-9)
        write_probe = _run_trainer_pipeline_write_probe(args, pipeline)
        report = _trainer_pipeline_smoke_report(
            dataset,
            pipeline,
            batches,
            root=args.root,
            batch_size=args.batch_size,
            requested_batches=args.batches,
            warmup_batches=args.warmup_batches,
            elapsed_seconds=elapsed_seconds,
            remote_options=remote_options,
            tensor_allocation=tensor_allocation,
            pipeline_prefetch_batches=pipeline_prefetch_batches,
            client_factory=client_factory,
            write_probe=write_probe,
        )
    finally:
        dataset.close()
    report["remote_client_topology_after_close"] = _trainer_remote_client_topology(
        client_factory,
        remote_options,
        len(dataset.sources),
    )
    report["remote_client_lifecycle_closed"] = bool(
        _mapping(report["remote_client_topology_after_close"]).get("all_open_clients_closed")
    )
    return report


def _collect_trainer_pipeline_tensor_batches(
    dataset: CxnfsWeightedTorchDataset,
    pipeline: CxnfsTrainingPipeline,
    args,
    tensor_allocation: CxnfsTrainerTensorAllocation,
    source_path_prefixes: Mapping[str, str | Path | None] | None,
    limit: int,
    pipeline_prefetch_batches: int,
) -> list[CxnfsTrainerBatch]:
    if limit <= 0:
        raise ValueError("pipeline batch collection limit must be positive")
    active_prefetch = _validate_pipeline_prefetch_batches(pipeline_prefetch_batches)
    if active_prefetch <= 0:
        iterator = dataset.iter_trainer_pipeline_tensor_batches(
            args.batch_size,
            pipeline,
            tensor_allocation=tensor_allocation,
            epoch=args.epoch,
            samples_per_epoch=args.samples_per_epoch,
            include_paths=True,
            non_blocking=tensor_allocation.non_blocking,
            source_path_prefixes=source_path_prefixes,
        )
        batches = []
        for batch in iterator:
            batches.append(batch)
            if len(batches) >= limit:
                break
        return batches

    stream = CxnfsTrainerPipelineStream(
        dataset,
        args.batch_size,
        tensor_allocation=tensor_allocation,
        epoch=args.epoch,
        samples_per_epoch=args.samples_per_epoch,
        include_paths=True,
        source_path_prefixes=source_path_prefixes,
    )
    batches = []
    with pipeline.prefetch_trainer_stream(
        stream,
        max_prefetch_batches=active_prefetch,
        non_blocking=tensor_allocation.non_blocking,
    ) as prefetched:
        for prefetched_batch in prefetched:
            batches.append(prefetched_batch.batch)
            if len(batches) >= limit:
                break
    return batches


def _trainer_pipeline_source_path_prefixes(
    sources: Sequence[CxnfsWeightedSource],
    remote_options: Mapping[str, Any],
) -> dict[str, str] | None:
    remote_root = remote_options.get("remote_root")
    if not remote_root:
        return None
    return {
        source.name: _join_remote_source_path(str(remote_root), source.name)
        for source in sources
    }


def _run_trainer_pipeline_write_probe(args, pipeline: CxnfsTrainingPipeline) -> dict[str, Any]:
    byte_size = int(getattr(args, "write_probe_bytes", 0) or 0)
    root = str(getattr(args, "write_probe_root", "__cxnfs-pipeline-write-probe"))
    frame_bytes = int(getattr(args, "write_probe_frame_bytes", 0) or 0)
    if byte_size <= 0:
        return {
            "format": "cxnfs.trainer-pipeline-write-probe.v1",
            "enabled": False,
            "byte_size": 0,
            "frame_bytes": None,
            "path": None,
            "verified_read_back": False,
            "write_path_evidence": {},
        }
    relative_root = normalize_relative_namespace_path(root, label="write_probe_root")
    file_name = _write_probe_file_name(
        "trainer-pipeline-smoke",
        0,
        0,
        int(getattr(args, "initial_cords", 1)),
        int(getattr(args, "max_cords", ABSOLUTE_MAX_CORDS)),
    )
    path = f"{relative_root}/{file_name}"
    payload = bytearray(_write_probe_payload(byte_size, 0, 0))
    client = getattr(pipeline, "client", None)
    if client is None:
        raise ValueError("training pipeline has no client for write probe")
    start = time.perf_counter()
    result = _require_client_method(client, "write_tensor")(path, payload, len(payload))
    elapsed_seconds = max(time.perf_counter() - start, 1e-9)
    read_back = _require_client_method(client, "read_mds_shard")(path, 0, len(payload))
    verified_read_back = bytes(read_back) == bytes(payload)
    if not verified_read_back:
        raise ValueError("trainer pipeline write probe read-back verification failed")
    write_evidence = _first_client_path_evidence(client, "write_path_evidence")
    return {
        "format": "cxnfs.trainer-pipeline-write-probe.v1",
        "enabled": True,
        "path": path,
        "byte_size": byte_size,
        "frame_bytes": frame_bytes if frame_bytes > 0 else None,
        "elapsed_seconds": elapsed_seconds,
        "bytes_per_second": byte_size / elapsed_seconds,
        "mebibytes_per_second": (byte_size / elapsed_seconds) / (1024.0 * 1024.0),
        "verified_read_back": verified_read_back,
        "file_count": getattr(result, "file_count", 0),
        "tensor_file_count": getattr(result, "tensor_file_count", 0),
        "bytes_sent": getattr(result, "bytes_sent", None),
        "write_path_evidence": write_evidence,
    }


def _run_trainer_stream_smoke(
    args,
    sources: Sequence[CxnfsWeightedSource],
    remote_options: Mapping[str, Any],
    *,
    client_factory: DatasetClientFactory | None = None,
) -> dict[str, Any]:
    read_mode = TrainerReadMode(args.read_mode)
    report: dict[str, Any] | None = None
    dataset = CxnfsWeightedTorchDataset(
        sources,
        repeat=getattr(args, "repeat", True),
        rounds_per_read=args.rounds_per_read,
        max_workers=args.max_workers,
        source_read_workers=args.source_read_workers,
        prefetch_chunks=args.prefetch_chunks,
        include_source_metadata=False,
        shard=_training_shard_from_args(args),
        shuffle=args.shuffle,
        seed=args.seed,
        samples_per_epoch=args.samples_per_epoch,
    )
    dataset.set_epoch(args.epoch)
    try:
        start_state = _trainer_stream_state_from_args(args)
        start_cursor = (
            CxnfsTrainerCursor.from_mapping(_mapping(start_state.get("cursor")))
            if start_state is not None
            else _trainer_cursor_from_args(args)
        )
        tensor_allocation = _trainer_tensor_allocation_from_args(args)
        trainer_prefetch_batches = _validate_trainer_prefetch_batches(
            getattr(args, "trainer_prefetch_batches", 0),
        )
        if args.warmup_batches:
            warmup_stream = _trainer_stream_from_args(
                dataset,
                args,
                read_mode,
                tensor_allocation,
                start_cursor,
            )
            if start_state is not None:
                warmup_stream.load_state_dict(start_state)
            _collect_trainer_stream_batches(
                warmup_stream,
                args.warmup_batches,
                trainer_prefetch_batches,
            )
        stream = _trainer_stream_from_args(
            dataset,
            args,
            read_mode,
            tensor_allocation,
            start_cursor,
        )
        if start_state is not None:
            stream.load_state_dict(start_state)
        start = time.perf_counter()
        batches = _collect_trainer_stream_batches(
            stream,
            args.batches,
            trainer_prefetch_batches,
        )
        elapsed_seconds = max(time.perf_counter() - start, 1e-9)
        report = _trainer_stream_smoke_report(
            dataset,
            batches,
            root=args.root,
            batch_size=args.batch_size,
            requested_batches=args.batches,
            warmup_batches=args.warmup_batches,
            elapsed_seconds=elapsed_seconds,
            remote_options=remote_options,
            read_mode=read_mode,
            trainer_batch_iterator=getattr(args, "trainer_batch_iterator", TRAINER_BATCH_ITERATOR_NATIVE),
            data_loader_workers=int(getattr(args, "data_loader_workers", 0) or 0),
            trainer_prefetch_batches=trainer_prefetch_batches,
            tensor_allocation=tensor_allocation,
            start_cursor=start_cursor,
            stream_state=stream.state_dict(),
            client_factory=client_factory,
        )
    finally:
        dataset.close()
    report["remote_client_topology_after_close"] = _trainer_remote_client_topology(
        client_factory,
        remote_options,
        len(dataset.sources),
    )
    report["remote_client_lifecycle_closed"] = bool(
        _mapping(report["remote_client_topology_after_close"]).get("all_open_clients_closed")
    )
    return report


def _collect_trainer_stream_batches(
    stream: CxnfsTrainerStream,
    limit: int,
    trainer_prefetch_batches: int = 0,
) -> list[CxnfsTrainerBatch]:
    if limit <= 0:
        raise ValueError("batch collection limit must be positive")
    active_prefetch = _validate_trainer_prefetch_batches(trainer_prefetch_batches)
    if active_prefetch <= 0:
        return list(stream.iter_batches(limit=limit))
    with stream.prefetch_batches(max_prefetch_batches=active_prefetch) as prefetched:
        return list(prefetched.iter_batches(limit=limit))


def _trainer_stream_from_args(
    dataset: CxnfsWeightedTorchDataset,
    args,
    read_mode: TrainerReadMode,
    tensor_allocation: CxnfsTrainerTensorAllocation,
    cursor: CxnfsTrainerCursor | Mapping[str, Any] | None,
) -> CxnfsTrainerStream:
    return CxnfsTrainerStream(
        dataset,
        args.batch_size,
        read_mode=read_mode,
        tensor_allocation=tensor_allocation,
        epoch=None if cursor is not None else args.epoch,
        samples_per_epoch=None if cursor is not None else args.samples_per_epoch,
        non_blocking=tensor_allocation.non_blocking,
        trainer_batch_iterator=getattr(args, "trainer_batch_iterator", TRAINER_BATCH_ITERATOR_NATIVE),
        data_loader_workers=int(getattr(args, "data_loader_workers", 0) or 0),
        data_loader_kwargs={"num_workers": int(getattr(args, "data_loader_workers", 0) or 0)}
        if getattr(args, "trainer_batch_iterator", TRAINER_BATCH_ITERATOR_NATIVE) == TRAINER_BATCH_ITERATOR_DATALOADER
        else None,
        cursor=cursor,
    )


def _trainer_batch_iterator(
    dataset: CxnfsWeightedTorchDataset,
    args,
    read_mode: TrainerReadMode,
    *,
    cursor: CxnfsTrainerCursor | Mapping[str, Any] | None = None,
):
    tensor_allocation = _trainer_tensor_allocation_from_args(args)
    cursor_epoch = None if cursor is not None else args.epoch
    cursor_samples_per_epoch = None if cursor is not None else args.samples_per_epoch
    if getattr(args, "trainer_batch_iterator", TRAINER_BATCH_ITERATOR_NATIVE) == TRAINER_BATCH_ITERATOR_DATALOADER:
        loader = dataset.to_trainer_dataloader(
            args.batch_size,
            read_mode=read_mode,
            tensor_allocation=tensor_allocation,
            non_blocking=tensor_allocation.non_blocking,
            samples_per_epoch=cursor_samples_per_epoch,
            data_loader_kwargs={"num_workers": args.data_loader_workers},
            cursor=cursor,
        )
        return iter(loader)
    if read_mode == TrainerReadMode.DIRECT_TENSORS:
        return dataset.iter_trainer_direct_tensor_batches(
            args.batch_size,
            tensor_allocation=tensor_allocation,
            epoch=cursor_epoch,
            samples_per_epoch=cursor_samples_per_epoch,
            non_blocking=tensor_allocation.non_blocking,
            cursor=cursor,
        )
    if read_mode == TrainerReadMode.DIRECT_BUFFERS:
        return dataset.iter_trainer_direct_batches(
            args.batch_size,
            epoch=cursor_epoch,
            samples_per_epoch=cursor_samples_per_epoch,
            cursor=cursor,
        )
    return dataset.iter_trainer_batches(
        args.batch_size,
        epoch=cursor_epoch,
        samples_per_epoch=cursor_samples_per_epoch,
        cursor=cursor,
    )


def _trainer_cursor_from_args(args: Any) -> CxnfsTrainerCursor | None:
    path = getattr(args, "resume_cursor", None)
    if not path:
        return None
    return CxnfsTrainerCursor.from_mapping(_read_json_mapping(Path(path), "trainer cursor"))


def _trainer_stream_state_from_args(args: Any) -> Mapping[str, Any] | None:
    path = getattr(args, "resume_state", None)
    if not path:
        return None
    return _trainer_stream_state_from_value(None, path)


def _trainer_stream_state_from_value(
    resume_state: Mapping[str, Any] | None,
    resume_state_path: str | Path | None,
) -> Mapping[str, Any] | None:
    if resume_state is not None and resume_state_path is not None:
        raise ValueError("use resume_state or resume_state_path, not both")
    payload = resume_state
    if payload is None and resume_state_path is not None:
        payload = _read_json_mapping(Path(resume_state_path), "trainer stream state")
    if payload is None:
        return None
    if payload.get("format") != TRAINER_STREAM_STATE_FORMAT:
        raise ValueError(f"unsupported trainer stream state format: {payload.get('format')}")
    return dict(payload)


def _trainer_stream_remote_options(args) -> dict[str, Any]:
    bulk_transport = normalize_python_bulk_transport(args.bulk_transport)
    if not args.remote_root:
        return {
            "transport": "local-mds",
            "bulk_transport": bulk_transport,
            "remote_root": None,
            "bulk_read_retries": getattr(args, "bulk_read_retries", 0),
            "bulk_write_retries": getattr(args, "bulk_write_retries", getattr(args, "bulk_read_retries", 0)),
        }
    return {
        "transport": "cxnfs-quic" if bulk_transport == PYTHON_BULK_TRANSPORT_QUIC else "cxnfs-length-prefixed",
        "bulk_transport": bulk_transport,
        "remote_root": _normalize_remote_root(args.remote_root),
        "host": args.host,
        "control_port": args.control_port,
        "transport_port": args.transport_port,
        "server_hostname": args.server_hostname or args.host,
        "initial_cords": args.initial_cords,
        "max_cords": args.max_cords,
        "write_connections": args.write_connections,
        "bulk_read_retries": getattr(args, "bulk_read_retries", 0),
        "bulk_write_retries": getattr(args, "bulk_write_retries", getattr(args, "bulk_read_retries", 0)),
        "shared_client": args.shared_client,
    }


def _select_trainer_bulk_client_factory(transport: str, factory: Callable[..., Any]) -> Callable[..., Any]:
    if transport == PYTHON_BULK_TRANSPORT_QUIC and _is_length_prefixed_bulk_connect(factory):
        return CxnfsQuicBulkClient.connect
    return factory


def _is_length_prefixed_bulk_connect(factory: Callable[..., Any]) -> bool:
    return (
        getattr(factory, "__self__", None) is CxnfsLengthPrefixedBulkClient
        and getattr(factory, "__func__", None) is getattr(CxnfsLengthPrefixedBulkClient.connect, "__func__", None)
    )


def _trainer_stream_remote_client_factory(
    credentials: ClientCredentials,
    host: str,
    control_port: int,
    transport_port: int,
    timeout: float | None,
    tls_context: Any,
    server_hostname: str,
    read_config: CxnfsBulkReadConfig,
    write_connections: int | None,
    bulk_read_retries: int,
    bulk_write_retries: int | None,
    shared_client: bool,
    bulk_transport: str,
    cafile: str | None,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> DatasetClientFactory:
    return CxnfsRemoteTrainerClientFactory(
        credentials,
        host,
        control_port,
        transport_port,
        timeout,
        tls_context,
        server_hostname,
        read_config,
        write_connections,
        bulk_read_retries,
        bulk_write_retries,
        shared_client,
        bulk_transport,
        cafile,
        control_client_factory,
        bulk_client_factory,
        tls_context_factory,
    )


def _connect_trainer_remote_bulk_client(
    credentials: ClientCredentials,
    host: str,
    control_port: int,
    transport_port: int,
    timeout: float | None,
    tls_context: Any,
    server_hostname: str,
    read_config: CxnfsBulkReadConfig,
    write_connections: int | None,
    bulk_read_retries: int,
    bulk_write_retries: int | None,
    bulk_transport: str,
    cafile: str | None,
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
):
    selected_transport = normalize_python_bulk_transport(bulk_transport)
    selected_bulk_client_factory = _select_trainer_bulk_client_factory(
        selected_transport,
        bulk_client_factory,
    )
    control = control_client_factory(
        host,
        control_port,
        timeout=timeout,
        tls_context=tls_context,
        server_hostname=server_hostname,
    )
    try:
        control.authenticate(credentials)
        bulk_kwargs = {
            "tls_context": tls_context,
            "server_hostname": server_hostname,
        }
        if selected_transport == PYTHON_BULK_TRANSPORT_QUIC:
            bulk_kwargs["cafile"] = cafile
            active_write_retries = bulk_read_retries if bulk_write_retries is None else bulk_write_retries
            bulk_kwargs["retry_config"] = CxnfsBulkRetryConfig(
                read_retries=bulk_read_retries,
                write_retries=active_write_retries,
            )
        return selected_bulk_client_factory(
            control,
            host,
            transport_port,
            timeout=timeout,
            read_config=read_config,
            write_connections=write_connections,
            close_control_client=True,
            **bulk_kwargs,
        )
    except Exception:
        close = getattr(control, "close", None)
        if callable(close):
            close()
        raise


def _source_client(
    name: str,
    client: CxnfsDatasetClient | None,
    client_factory: DatasetClientFactory | None,
    remote_root: str | Path | None,
) -> CxnfsDatasetClient | None:
    if client_factory is not None:
        return CxnfsLazyDatasetClient(
            name,
            client_factory,
            path_prefix=(
                _join_remote_source_path(remote_root, name)
                if remote_root is not None
                else None
            ),
        )
    source_client = client
    if source_client is None or remote_root is None:
        return source_client
    return CxnfsPathPrefixClient(source_client, _join_remote_source_path(remote_root, name))


def _require_client_method(client: Any, method_name: str):
    method = getattr(client, method_name, None)
    if not callable(method):
        raise TypeError(f"client does not implement {method_name}")
    return method


def _join_remote_source_path(remote_root: str | Path, name: str) -> str:
    return join_namespace_path(remote_root, name, label="dataset source name")


def _pipeline_shard_path(
    source_name: str,
    shard_name: str,
    source_path_prefixes: Mapping[str, str | Path | None] | None,
) -> str:
    prefix = source_name
    if source_path_prefixes is not None and source_name in source_path_prefixes:
        configured = source_path_prefixes[source_name]
        prefix = "" if configured is None else str(configured)
    if not prefix:
        return shard_name
    return join_namespace_path(prefix, shard_name, label="MDS shard name")


def _normalize_remote_root(remote_root: str | Path) -> str:
    return normalize_namespace_root(remote_root, label="remote_root")


def _trainer_pipeline_smoke_report(
    dataset: CxnfsWeightedTorchDataset,
    pipeline: CxnfsTrainingPipeline,
    batches: Sequence[CxnfsTrainerBatch],
    *,
    root: str,
    batch_size: int,
    requested_batches: int,
    warmup_batches: int,
    elapsed_seconds: float,
    remote_options: Mapping[str, Any],
    tensor_allocation: CxnfsTrainerTensorAllocation,
    pipeline_prefetch_batches: int,
    client_factory: DatasetClientFactory | None = None,
    write_probe: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    remote_client_warmup = _warm_trainer_remote_source_clients(dataset, remote_options)
    sample_count = sum(batch.sample_count for batch in batches)
    byte_size = sum(batch.byte_size for batch in batches)
    source_counts = _aggregate_batch_source_counts(batches)
    active_pipeline_prefetch = _validate_pipeline_prefetch_batches(pipeline_prefetch_batches)
    pipeline_prefetch_stream_used = active_pipeline_prefetch > 0
    training_pipeline_context = (
        "CxnfsTrainingPipeline.prefetch_trainer_stream"
        if pipeline_prefetch_stream_used
        else "CxnfsTrainingPipeline.read_into_tensors"
    )
    read_path_evidence = _trainer_pipeline_read_path_evidence(
        dataset,
        pipeline,
        batches,
        pipeline_prefetch_batches=active_pipeline_prefetch,
    )
    read_path_reliability = _trainer_read_path_reliability({"read_path_evidence": read_path_evidence})
    tensor_memory_path = _trainer_tensor_memory_path_report(
        TrainerReadMode.DIRECT_TENSORS,
        tensor_allocation,
        read_path_evidence,
        sample_count,
    )
    pipeline_network_path = _trainer_pipeline_network_path_report(
        pipeline,
        read_path_evidence,
        tensor_memory_path,
        batch_count=len(batches),
        sample_count=sample_count,
        byte_size=byte_size,
        requested_batches=requested_batches,
        pipeline_prefetch_batches=active_pipeline_prefetch,
        remote_options=remote_options,
    )
    final_cursor = batches[-1].resume_cursor.to_dict() if batches and batches[-1].resume_cursor is not None else None
    passed = (
        len(batches) == requested_batches
        and sample_count > 0
        and bool(read_path_reliability.get("complete"))
        and bool(_mapping(tensor_memory_path).get("complete"))
        and bool(pipeline_network_path.get("complete"))
        and (
            not bool(_mapping(write_probe).get("enabled"))
            or bool(_mapping(write_probe).get("verified_read_back"))
        )
    )
    return {
        "format": TRAINER_PIPELINE_SMOKE_FORMAT,
        "passed": passed,
        "root": str(root),
        "remote_root": remote_options.get("remote_root"),
        "transport": remote_options.get("transport"),
        "bulk_transport": remote_options.get("bulk_transport"),
        "source_count": len(dataset.sources),
        "total_weight": sum(source.weight for source in dataset.sources),
        "source_schedule": [dataset.sources[index].name for index in dataset._source_schedule],
        "training_shard": _training_shard_payload(dataset.shard),
        "rank": dataset.shard.index,
        "world_size": dataset.shard.count,
        "batch_size": batch_size,
        "requested_batches": requested_batches,
        "warmup_batches": warmup_batches,
        "read_mode": TrainerReadMode.DIRECT_TENSORS.value,
        "tensor_allocation": tensor_allocation.to_dict(),
        "training_pipeline_object": "CxnfsTrainingPipeline",
        "training_pipeline_context": training_pipeline_context,
        "pipeline_batch_iterator": (
            "CxnfsTrainerPipelineStream.next_batch"
            if pipeline_prefetch_stream_used
            else "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batches"
        ),
        "pipeline_descriptor_iterator": "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors",
        "pipeline_descriptor_stream": "CxnfsTrainerPipelineStream" if pipeline_prefetch_stream_used else None,
        "pipeline_prefetch_stream_used": pipeline_prefetch_stream_used,
        "pipeline_prefetch_batches": active_pipeline_prefetch,
        "pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch" if pipeline_prefetch_stream_used else None,
        "pipeline_config": {
            "network_read_workers": pipeline.config.network_read_workers,
            "max_cords": pipeline.config.max_cords,
            "prefetch_batches": pipeline.config.prefetch_batches,
            "max_inflight_bytes": pipeline.config.max_inflight_bytes,
            "pin_memory": pipeline.config.pin_memory,
            "augmentation_placement": pipeline.config.augmentation_placement.value,
            "torch_compile_enabled": pipeline.config.torch_compile.enabled,
        },
        "rounds_per_read": dataset.rounds_per_read,
        "max_workers": dataset.max_workers,
        "source_read_workers": dataset.source_read_workers,
        "prefetch_chunks": dataset.prefetch_chunks,
        "repeat": dataset.repeat,
        "shuffle": dataset.shuffle,
        "seed": dataset.seed,
        "batch_count": len(batches),
        "sample_count": sample_count,
        "byte_size": byte_size,
        "source_counts": source_counts,
        "final_cursor": final_cursor,
        "elapsed_seconds": elapsed_seconds,
        "samples_per_second": sample_count / elapsed_seconds,
        "bytes_per_second": byte_size / elapsed_seconds,
        "mebibytes_per_second": (byte_size / elapsed_seconds) / (1024.0 * 1024.0),
        "read_path_evidence": read_path_evidence,
        "read_path_reliability": read_path_reliability,
        "tensor_memory_path": tensor_memory_path,
        "pipeline_network_path": pipeline_network_path,
        "pipeline_network_path_format": pipeline_network_path.get("format"),
        "pipeline_network_path_complete": bool(pipeline_network_path.get("complete")),
        "pipeline_network_read_workers": pipeline.config.network_read_workers,
        "pipeline_max_inflight_bytes": pipeline.config.max_inflight_bytes,
        "pipeline_python_byte_batch_avoided": bool(pipeline_network_path.get("python_byte_batch_avoided")),
        "write_probe": dict(write_probe) if isinstance(write_probe, Mapping) else {
            "format": "cxnfs.trainer-pipeline-write-probe.v1",
            "enabled": False,
            "verified_read_back": False,
            "write_path_evidence": {},
        },
        "remote_client_warmup": remote_client_warmup,
        "remote_client_topology": _trainer_remote_client_topology(
            client_factory,
            remote_options,
            len(dataset.sources),
        ),
        "batches": [
            _trainer_contract_batch_summary(batch, TrainerReadMode.DIRECT_TENSORS)
            for batch in batches
        ],
    }


def _trainer_pipeline_read_path_evidence(
    dataset: CxnfsWeightedTorchDataset,
    pipeline: CxnfsTrainingPipeline,
    batches: Sequence[CxnfsTrainerBatch],
    *,
    pipeline_prefetch_batches: int = 0,
) -> dict[str, Any]:
    descriptor_evidence = _weighted_dataset_read_path_evidence(dataset, TrainerReadMode.DIRECT_TENSORS)
    pipeline_reports = _collect_client_read_path_evidence(getattr(pipeline, "client", None))
    read_path_evidence = _aggregate_read_path_evidence([descriptor_evidence, *pipeline_reports])
    active_pipeline_prefetch = _validate_pipeline_prefetch_batches(pipeline_prefetch_batches)
    pipeline_prefetch_stream_used = active_pipeline_prefetch > 0
    read_path_evidence.update({
        "read_mode": TrainerReadMode.DIRECT_TENSORS.value,
        "all_runs_have_evidence": True,
        "uses_caller_owned_destinations": True,
        "source_count": len(dataset.sources),
        "training_pipeline_used": True,
        "training_pipeline_object": "CxnfsTrainingPipeline",
        "training_pipeline_context": (
            "CxnfsTrainingPipeline.prefetch_trainer_stream"
            if pipeline_prefetch_stream_used
            else "CxnfsTrainingPipeline.read_into_tensors"
        ),
        "pipeline_descriptor_planning_used": True,
        "pipeline_descriptor_stream_used": pipeline_prefetch_stream_used,
        "pipeline_prefetch_stream_used": pipeline_prefetch_stream_used,
        "pipeline_prefetch_batches": active_pipeline_prefetch,
        "pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch" if pipeline_prefetch_stream_used else None,
        "pipeline_payload_transport_count": len(pipeline_reports),
        "pipeline_payload_transports": pipeline_reports,
        "pipeline_python_byte_batch_avoided": True,
        "sources": descriptor_evidence.get("sources", []),
    })
    trainer_batches = [
        getattr(batch, "batch", batch)
        for batch in batches
    ]
    _add_trainer_batch_memory_observations(read_path_evidence, trainer_batches, TrainerReadMode.DIRECT_TENSORS)
    return read_path_evidence


def _trainer_pipeline_network_path_report(
    pipeline: CxnfsTrainingPipeline,
    read_path_evidence: Mapping[str, Any],
    tensor_memory_path: Mapping[str, Any] | None,
    *,
    batch_count: int,
    sample_count: int,
    byte_size: int,
    requested_batches: int,
    pipeline_prefetch_batches: int,
    remote_options: Mapping[str, Any],
) -> dict[str, Any]:
    tensor_path = _mapping(tensor_memory_path)
    network_read_workers = _int_value(pipeline.config.network_read_workers, 0)
    max_cords = _int_value(pipeline.config.max_cords, 0)
    max_inflight_bytes = _int_value(pipeline.config.max_inflight_bytes, 0)
    direct_tensor_reads = _int_value(read_path_evidence.get("direct_tensor_read_operations"), 0)
    direct_destination_reads = _int_value(read_path_evidence.get("direct_destination_read_operations"), 0)
    failed_direct_destination_reads = _int_value(
        read_path_evidence.get("failed_direct_destination_read_operations"),
        0,
    )
    direct_destination_max_cords = _direct_destination_max_observed_cords(read_path_evidence)
    payload_bytes_read = _int_value(read_path_evidence.get("payload_bytes_read"), 0)
    configured_fanout = network_read_workers > 1 and max_cords > 1
    direct_memory_complete = bool(tensor_path.get("complete"))
    python_byte_batch_avoided = (
        bool(read_path_evidence.get("pipeline_python_byte_batch_avoided"))
        and bool(tensor_path.get("python_byte_batch_avoided"))
    )
    network_fills_or_stages_destination = (
        bool(tensor_path.get("network_fills_tensor_destination"))
        or bool(tensor_path.get("requires_pinned_host_staging"))
    )
    frame_sink_fanout_verified = _direct_destination_frame_sink_cord_fanout_verified(read_path_evidence)
    quic_required = (
        str(remote_options.get("bulk_transport") or "").lower() == PYTHON_BULK_TRANSPORT_QUIC
        or bool(read_path_evidence.get("quic_read_transport_observed"))
    )
    quic_complete = (
        bool(read_path_evidence.get("quic_read_transport_complete"))
        and bool(read_path_evidence.get("quic_read_transport_stream_completion_verified"))
        and _quic_read_transport_stream_eof_verified(read_path_evidence)
        and _int_value(read_path_evidence.get("quic_read_transport_max_read_stream_count"), 0) > 1
    )
    observed_parallel_direct_path = (
        direct_destination_max_cords > 1
        or _int_value(read_path_evidence.get("direct_destination_parallel_frame_sink_read_operations"), 0) > 0
        or _int_value(read_path_evidence.get("quic_read_transport_max_read_stream_count"), 0) > 1
    )
    missing: list[str] = []
    if int(batch_count) <= 0 or int(batch_count) != int(requested_batches):
        missing.append("requested pipeline batches must be consumed")
    if int(sample_count) <= 0:
        missing.append("pipeline samples must be observed")
    if int(byte_size) <= 0:
        missing.append("pipeline payload bytes must be observed")
    if not configured_fanout:
        missing.append("pipeline network fanout must configure more than one worker and cord")
    if int(pipeline_prefetch_batches) <= 0:
        missing.append("pipeline prefetch must be enabled")
    if max_inflight_bytes <= 0 or (byte_size > 0 and byte_size > max_inflight_bytes):
        missing.append("pipeline inflight byte ceiling must cover the observed batch bytes")
    if direct_tensor_reads <= 0:
        missing.append("direct tensor reads must be observed")
    if direct_destination_reads <= 0:
        missing.append("direct destination reads must be observed")
    if failed_direct_destination_reads != 0:
        missing.append("direct destination reads must not fail")
    if payload_bytes_read <= 0:
        missing.append("payload bytes must be read through the pipeline client")
    if not direct_memory_complete:
        missing.append("direct tensor memory path must be complete")
    if not python_byte_batch_avoided:
        missing.append("Python byte batches must be avoided")
    if not network_fills_or_stages_destination:
        missing.append("network reads must fill or stage caller-owned tensor destinations")
    if not observed_parallel_direct_path and quic_required:
        missing.append("parallel direct-destination fanout must be observed")
    if quic_required and not bool(read_path_evidence.get("transport_frame_sink_observed")):
        missing.append("QUIC pipeline reads must observe the frame sink")
    if quic_required and not bool(read_path_evidence.get("frame_sink_used_for_all_direct_destination_reads")):
        missing.append("QUIC pipeline reads must use the frame sink for direct destinations")
    if quic_required and not frame_sink_fanout_verified:
        missing.append("QUIC pipeline reads must prove direct-destination cord fanout")
    if quic_required and not quic_complete:
        missing.append("QUIC pipeline read stream completion and EOF must be verified")
    return {
        "format": TRAINER_PIPELINE_NETWORK_PATH_FORMAT,
        "complete": not missing,
        "missing": missing,
        "quic_required": quic_required,
        "quic_complete": quic_complete,
        "training_pipeline_object": "CxnfsTrainingPipeline",
        "training_pipeline_context": read_path_evidence.get("training_pipeline_context"),
        "network_read_workers": network_read_workers,
        "max_cords": max_cords,
        "prefetch_batches": _int_value(pipeline.config.prefetch_batches, 0),
        "active_pipeline_prefetch_batches": int(pipeline_prefetch_batches),
        "max_inflight_bytes": max_inflight_bytes,
        "batch_count": int(batch_count),
        "requested_batches": int(requested_batches),
        "sample_count": int(sample_count),
        "byte_size": int(byte_size),
        "payload_bytes_read": payload_bytes_read,
        "network_fanout_configured": configured_fanout,
        "observed_parallel_direct_path": observed_parallel_direct_path,
        "direct_tensor_read_operations": direct_tensor_reads,
        "direct_destination_read_operations": direct_destination_reads,
        "failed_direct_destination_read_operations": failed_direct_destination_reads,
        "direct_destination_max_observed_cords": direct_destination_max_cords,
        "direct_destination_parallel_frame_sink_read_operations": _int_value(
            read_path_evidence.get("direct_destination_parallel_frame_sink_read_operations"),
            0,
        ),
        "direct_destination_parallel_frame_sink_frames": _int_value(
            read_path_evidence.get("direct_destination_parallel_frame_sink_frames"),
            0,
        ),
        "transport_frame_sink_observed": bool(read_path_evidence.get("transport_frame_sink_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(
            read_path_evidence.get("frame_sink_used_for_all_direct_destination_reads")
        ),
        "direct_destination_frame_sink_cord_fanout_verified": frame_sink_fanout_verified,
        "uses_caller_owned_destinations": bool(read_path_evidence.get("uses_caller_owned_destinations")),
        "tensor_memory_path_complete": direct_memory_complete,
        "python_byte_batch_avoided": python_byte_batch_avoided,
        "network_fills_tensor_destination": bool(tensor_path.get("network_fills_tensor_destination")),
        "requires_pinned_host_staging": bool(tensor_path.get("requires_pinned_host_staging")),
        "quic_read_transport_observed": bool(read_path_evidence.get("quic_read_transport_observed")),
        "quic_read_transport_complete": bool(read_path_evidence.get("quic_read_transport_complete")),
        "quic_read_transport_stream_completion_verified": bool(
            read_path_evidence.get("quic_read_transport_stream_completion_verified")
        ),
        "quic_read_transport_stream_eof_verified": _quic_read_transport_stream_eof_verified(read_path_evidence),
        "quic_read_transport_read_streams_eof_verified": _int_value(
            read_path_evidence.get("quic_read_transport_read_streams_eof_verified"),
            0,
        ),
        "quic_read_transport_max_read_stream_count": _int_value(
            read_path_evidence.get("quic_read_transport_max_read_stream_count"),
            0,
        ),
    }


def _trainer_stream_smoke_report(
    dataset: CxnfsWeightedTorchDataset,
    batches: Sequence[CxnfsTrainerBatch],
    *,
    root: str,
    batch_size: int,
    requested_batches: int,
    warmup_batches: int,
    elapsed_seconds: float,
    remote_options: Mapping[str, Any],
    read_mode: TrainerReadMode,
    trainer_batch_iterator: str,
    data_loader_workers: int,
    trainer_prefetch_batches: int,
    tensor_allocation: CxnfsTrainerTensorAllocation,
    start_cursor: CxnfsTrainerCursor | None,
    stream_state: Mapping[str, Any],
    client_factory: DatasetClientFactory | None = None,
) -> dict[str, Any]:
    remote_client_warmup = _warm_trainer_remote_source_clients(dataset, remote_options)
    sample_count = sum(batch.sample_count for batch in batches)
    byte_size = sum(batch.byte_size for batch in batches)
    initial_cursor = start_cursor or dataset.initial_trainer_cursor(
        epoch=dataset.epoch,
        samples_per_epoch=dataset.samples_per_epoch,
    )
    final_cursor = batches[-1].resume_cursor if batches and batches[-1].resume_cursor is not None else initial_cursor
    read_path_evidence = _weighted_dataset_read_path_evidence(dataset, read_mode)
    worker_read_path_evidence = _collect_batch_worker_read_path_evidence(batches)
    if worker_read_path_evidence:
        read_path_evidence = _aggregate_read_path_evidence([
            read_path_evidence,
            *worker_read_path_evidence,
        ])
        read_path_evidence.update({
            "read_mode": read_mode.value,
            "all_runs_have_evidence": True,
            "uses_caller_owned_destinations": read_mode in {TrainerReadMode.DIRECT_BUFFERS, TrainerReadMode.DIRECT_TENSORS},
            "source_count": len(dataset.sources),
            "sources": [
                {
                    "source": source.name,
                    "bulk_payload_transport_count": 0,
                    "bulk_payload_transports": [],
                }
                for source in dataset.sources
            ],
            "worker_batch_read_evidence_observed": True,
            "worker_batch_read_evidence_count": len(worker_read_path_evidence),
            "worker_process_read_path_evidence": worker_read_path_evidence,
        })
    else:
        read_path_evidence["worker_batch_read_evidence_observed"] = False
        read_path_evidence["worker_batch_read_evidence_count"] = 0
    _add_trainer_batch_memory_observations(read_path_evidence, batches, read_mode)
    tensor_memory_path = _trainer_tensor_memory_path_report(
        read_mode,
        tensor_allocation,
        read_path_evidence,
        sample_count,
    )
    read_path_reliability = _trainer_read_path_reliability({"read_path_evidence": read_path_evidence})
    worker_partition = _trainer_worker_partition_evidence(
        dataset,
        batches,
        trainer_batch_iterator=trainer_batch_iterator,
        data_loader_workers=data_loader_workers,
    )
    payload = {
        "format": "cxnfs.trainer-stream-smoke.v1",
        "root": str(root),
        "source_count": len(dataset.sources),
        "total_weight": sum(source.weight for source in dataset.sources),
        "source_schedule": [dataset.sources[index].name for index in dataset._source_schedule],
        "training_shard": _training_shard_payload(dataset.shard),
        "rank": dataset.shard.index,
        "world_size": dataset.shard.count,
        "batch_size": batch_size,
        "requested_batches": requested_batches,
        "warmup_batches": warmup_batches,
        "read_mode": read_mode.value,
        "tensor_allocation": tensor_allocation.to_dict() if read_mode == TrainerReadMode.DIRECT_TENSORS else None,
        "trainer_batch_iterator": trainer_batch_iterator,
        "data_loader_workers": data_loader_workers,
        "trainer_prefetch_batches": trainer_prefetch_batches,
        "trainer_prefetch_enabled": trainer_prefetch_batches > 0,
        "trainer_prefetch_stream_object": "CxnfsTrainerPrefetchStream" if trainer_prefetch_batches > 0 else None,
        "stream_object": "CxnfsTrainerStream",
        "stream_state_format": TRAINER_STREAM_STATE_FORMAT,
        "stream_state": dict(stream_state),
        "stream_state_matches_final_cursor": _mapping(stream_state.get("cursor")) == final_cursor.to_dict(),
        "rounds_per_read": dataset.rounds_per_read,
        "max_workers": dataset.max_workers,
        "source_read_workers": dataset.source_read_workers,
        "prefetch_chunks": dataset.prefetch_chunks,
        "repeat": dataset.repeat,
        "shuffle": dataset.shuffle,
        "seed": dataset.seed,
        "initial_cursor": initial_cursor.to_dict(),
        "final_cursor": final_cursor.to_dict(),
        "resume_cursor_format": TRAINER_CURSOR_FORMAT,
        "batch_count": len(batches),
        "sample_count": sample_count,
        "byte_size": byte_size,
        "elapsed_seconds": elapsed_seconds,
        "samples_per_second": sample_count / elapsed_seconds,
        "bytes_per_second": byte_size / elapsed_seconds,
        "mebibytes_per_second": (byte_size / elapsed_seconds) / (1024.0 * 1024.0),
        "read_path_evidence": read_path_evidence,
        "read_path_reliability": read_path_reliability,
        "tensor_memory_path": tensor_memory_path,
        "remote_client_warmup": remote_client_warmup,
        "remote_client_topology": _trainer_remote_client_topology(
            client_factory,
            remote_options,
            len(dataset.sources),
        ),
        "worker_partition_evidence": worker_partition,
        "batches": [batch.to_dict() for batch in batches],
    }
    payload.update(remote_options)
    return payload


def _trainer_remote_client_topology(
    client_factory: DatasetClientFactory | None,
    remote_options: Mapping[str, Any],
    source_count: int,
) -> dict[str, Any]:
    method = getattr(client_factory, "connection_topology", None)
    if callable(method):
        topology = method()
        if isinstance(topology, Mapping):
            payload = dict(topology)
            payload.setdefault("expected_source_count", int(source_count))
            payload.setdefault("remote_client_required", bool(remote_options.get("remote_root")))
            return payload
    remote_required = bool(remote_options.get("remote_root"))
    return {
        "format": "cxnfs.trainer-remote-client-topology.v1",
        "remote_client_required": remote_required,
        "expected_source_count": int(source_count),
        "shared_client": bool(remote_options.get("shared_client", False)) if remote_required else False,
        "source_request_count": 0,
        "unique_source_request_count": 0,
        "source_request_names": [],
        "unique_source_request_names": [],
        "connection_count": 0,
        "connection_source_names": [],
        "unique_connection_source_names": [],
        "client_open_count": 0,
        "client_open_source_names": [],
        "unique_client_open_source_names": [],
        "client_close_count": 0,
        "client_close_source_names": [],
        "unique_client_close_source_names": [],
        "active_client_count": 0,
        "active_client_source_names": [],
        "unique_active_client_source_names": [],
        "shared_client_reuse_count": 0,
        "factory_close_count": 0,
        "all_open_clients_closed": True,
        "connection_source_set_matches_requests": not remote_required,
        "client_open_source_set_matches_requests": not remote_required,
        "client_close_source_set_matches_opens": True,
        "client_lifecycle_source_set_complete": True,
        "per_source_client_observed": False,
        "shared_client_observed": False,
        "complete": not remote_required,
    }


def _warm_trainer_remote_source_clients(
    dataset: CxnfsWeightedTorchDataset,
    remote_options: Mapping[str, Any],
) -> dict[str, Any]:
    source_names = [source.name for source in dataset.sources]
    remote_required = bool(remote_options.get("remote_root"))
    if not remote_required:
        return {
            "format": "cxnfs.trainer-remote-client-warmup.v1",
            "remote_client_required": False,
            "source_count": len(source_names),
            "warmed_source_count": 0,
            "warmed_source_names": [],
            "complete": True,
        }
    warmed: list[str] = []
    for source, source_dataset in zip(dataset.sources, dataset.datasets):
        mds = getattr(source_dataset, "mds", None)
        client = getattr(mds, "client", None)
        if _warm_lazy_dataset_client(client):
            warmed.append(source.name)
    return {
        "format": "cxnfs.trainer-remote-client-warmup.v1",
        "remote_client_required": True,
        "source_count": len(source_names),
        "warmed_source_count": len(warmed),
        "warmed_source_names": warmed,
        "complete": len(warmed) == len(source_names),
    }


def _warm_lazy_dataset_client(client: Any, seen: set[int] | None = None) -> bool:
    if client is None:
        return False
    active_seen = seen if seen is not None else set()
    identity = id(client)
    if identity in active_seen:
        return False
    active_seen.add(identity)
    if isinstance(client, CxnfsLazyDatasetClient):
        client.client()
        return True
    delegate = getattr(client, "delegate", None)
    if delegate is not None and _warm_lazy_dataset_client(delegate, active_seen):
        return True
    prefix_transport = getattr(client, "_prefix_transport", None)
    prefix_delegate = getattr(prefix_transport, "delegate", None)
    if prefix_delegate is not None and _warm_lazy_dataset_client(prefix_delegate, active_seen):
        return True
    return False


def _aggregate_trainer_remote_client_topology(
    reports: Sequence[Mapping[str, Any]],
    expected_source_count: int,
    remote_required: bool,
) -> dict[str, Any]:
    topologies: list[Mapping[str, Any]] = []
    source_request_names: list[str] = []
    connection_source_names: list[str] = []
    client_open_source_names: list[str] = []
    client_close_source_names: list[str] = []
    active_client_source_names: list[str] = []
    for report in reports:
        topology = _mapping(report.get("remote_client_topology"))
        if topology:
            topologies.append(topology)
            source_request_names.extend(str(name) for name in topology.get("source_request_names", ()))
            connection_source_names.extend(str(name) for name in topology.get("connection_source_names", ()))
            client_open_source_names.extend(str(name) for name in topology.get("client_open_source_names", ()))
            client_close_source_names.extend(str(name) for name in topology.get("client_close_source_names", ()))
            active_client_source_names.extend(str(name) for name in topology.get("active_client_source_names", ()))

    report_count = len(reports)
    topology_count = len(topologies)
    all_reports_have_topology = topology_count == report_count
    all_topologies_complete = (
        all(bool(topology.get("complete")) for topology in topologies)
        if topologies
        else not remote_required
    )
    per_source_observed_count = sum(
        1 for topology in topologies if bool(topology.get("per_source_client_observed"))
    )
    shared_observed_count = sum(
        1 for topology in topologies if bool(topology.get("shared_client_observed"))
    )
    connection_source_set_match_count = sum(
        1 for topology in topologies if bool(topology.get("connection_source_set_matches_requests"))
    )
    client_open_source_set_match_count = sum(
        1 for topology in topologies if bool(topology.get("client_open_source_set_matches_requests"))
    )
    client_lifecycle_source_set_complete_count = sum(
        1 for topology in topologies if bool(topology.get("client_lifecycle_source_set_complete"))
    )
    unique_request_counts = [
        _int_value(topology.get("unique_source_request_count"), 0)
        for topology in topologies
    ]
    connection_counts = [
        _int_value(topology.get("connection_count"), 0)
        for topology in topologies
    ]
    client_open_counts = [
        _int_value(topology.get("client_open_count"), 0)
        for topology in topologies
    ]
    client_close_counts = [
        _int_value(topology.get("client_close_count"), 0)
        for topology in topologies
    ]
    active_client_counts = [
        _int_value(topology.get("active_client_count"), 0)
        for topology in topologies
    ]
    shared_client_reuse_counts = [
        _int_value(topology.get("shared_client_reuse_count"), 0)
        for topology in topologies
    ]
    minimum_unique_request_count = min(unique_request_counts) if unique_request_counts else 0
    minimum_connection_count = min(connection_counts) if connection_counts else 0
    expected = max(0, int(expected_source_count or 0))
    has_remote_reports = report_count > 0 and all_reports_have_topology and all_topologies_complete
    all_connection_source_sets_match_requests = (
        all(bool(topology.get("connection_source_set_matches_requests")) for topology in topologies)
        if topologies
        else not remote_required
    )
    all_client_open_source_sets_match_requests = (
        all(bool(topology.get("client_open_source_set_matches_requests")) for topology in topologies)
        if topologies
        else not remote_required
    )
    all_client_lifecycle_source_sets_complete = (
        all(bool(topology.get("client_lifecycle_source_set_complete")) for topology in topologies)
        if topologies
        else not remote_required
    )
    per_source_complete = (
        not remote_required
        or (
            has_remote_reports
            and per_source_observed_count == report_count
            and minimum_unique_request_count >= expected
            and minimum_connection_count >= expected
            and all_connection_source_sets_match_requests
            and all_client_open_source_sets_match_requests
        )
    )
    shared_complete = (
        not remote_required
        or (
            has_remote_reports
            and shared_observed_count == report_count
            and minimum_unique_request_count >= expected
            and minimum_connection_count >= 1
        )
    )
    return {
        "format": "cxnfs.trainer-remote-client-topology-aggregate.v1",
        "remote_client_required": bool(remote_required),
        "expected_source_count": expected,
        "report_count": report_count,
        "topology_report_count": topology_count,
        "all_reports_have_topology": all_reports_have_topology,
        "all_topologies_complete": all_topologies_complete,
        "per_source_client_observed_count": per_source_observed_count,
        "shared_client_observed_count": shared_observed_count,
        "connection_source_set_matches_requests_count": connection_source_set_match_count,
        "client_open_source_set_matches_requests_count": client_open_source_set_match_count,
        "client_lifecycle_source_set_complete_count": client_lifecycle_source_set_complete_count,
        "all_connection_source_sets_match_requests": all_connection_source_sets_match_requests,
        "all_client_open_source_sets_match_requests": all_client_open_source_sets_match_requests,
        "all_client_lifecycle_source_sets_complete": all_client_lifecycle_source_sets_complete,
        "minimum_unique_source_request_count": minimum_unique_request_count,
        "minimum_connection_count": minimum_connection_count,
        "source_request_count": sum(_int_value(topology.get("source_request_count"), 0) for topology in topologies),
        "connection_count": sum(connection_counts),
        "client_open_count": sum(client_open_counts),
        "client_close_count": sum(client_close_counts),
        "active_client_count": sum(active_client_counts),
        "shared_client_reuse_count": sum(shared_client_reuse_counts),
        "unique_source_request_names": _dedupe_strings(source_request_names),
        "unique_connection_source_names": _dedupe_strings(connection_source_names),
        "unique_client_open_source_names": _dedupe_strings(client_open_source_names),
        "unique_client_close_source_names": _dedupe_strings(client_close_source_names),
        "unique_active_client_source_names": _dedupe_strings(active_client_source_names),
        "client_lifecycle_observed_count": sum(
            1 for topology in topologies if _int_value(topology.get("client_open_count"), 0) > 0
        ),
        "all_open_clients_closed_count": sum(
            1 for topology in topologies if bool(topology.get("all_open_clients_closed"))
        ),
        "all_open_clients_closed": (
            all(bool(topology.get("all_open_clients_closed")) for topology in topologies)
            if topologies
            else not remote_required
        ),
        "per_source_client_complete": per_source_complete,
        "shared_client_complete": shared_complete,
        "complete": (
            (all_reports_have_topology and all_topologies_complete)
            if not remote_required
            else (per_source_complete or shared_complete)
        ),
    }


def _trainer_remote_client_topology_gate(
    topology: Mapping[str, Any],
    *,
    expected_source_count: int,
    require_remote: bool,
    require_per_source: bool,
) -> dict[str, Any]:
    missing: list[str] = []
    if not require_remote:
        return {
            "complete": True,
            "missing": missing,
            "remote_client_required": False,
            "per_source_client_required": bool(require_per_source),
            "per_source_client_complete": bool(topology.get("per_source_client_complete", True)),
            "shared_client_complete": bool(topology.get("shared_client_complete", False)),
            "all_connection_source_sets_match_requests": True,
            "all_client_open_source_sets_match_requests": True,
        }
    expected = max(0, int(expected_source_count or 0))
    if not topology:
        missing.append("remote client topology is missing")
        return {
            "complete": False,
            "missing": missing,
            "remote_client_required": True,
            "per_source_client_required": bool(require_per_source),
            "per_source_client_complete": False,
            "shared_client_complete": False,
            "all_connection_source_sets_match_requests": False,
            "all_client_open_source_sets_match_requests": False,
            "topology_report_count": 0,
            "report_count": 0,
        }

    topology_format = topology.get("format")
    if topology_format == "cxnfs.trainer-remote-client-topology.v1":
        unique_request_count = _int_value(topology.get("unique_source_request_count"), 0)
        connection_count = _int_value(topology.get("connection_count"), 0)
        per_source_complete = (
            bool(topology.get("per_source_client_observed"))
            and unique_request_count >= expected
            and connection_count >= expected
            and bool(topology.get("connection_source_set_matches_requests"))
            and bool(topology.get("client_open_source_set_matches_requests"))
        )
        shared_complete = (
            bool(topology.get("shared_client_observed"))
            and unique_request_count >= expected
            and connection_count >= 1
        )
        if _int_value(topology.get("expected_source_count"), 0) < expected:
            missing.append("topology expected source count is below the weighted plan")
        if not bool(topology.get("complete")):
            missing.append("remote client topology is incomplete")
        if require_per_source and not per_source_complete:
            missing.append("per-source remote clients were not observed for every weighted source")
        if require_per_source and not bool(topology.get("connection_source_set_matches_requests")):
            missing.append("remote client connection sources did not match requested weighted sources")
        if require_per_source and not bool(topology.get("client_open_source_set_matches_requests")):
            missing.append("remote client open sources did not match requested weighted sources")
        if not require_per_source and not (per_source_complete or shared_complete):
            missing.append("remote client topology is incomplete")
        return {
            "complete": not missing,
            "missing": missing,
            "remote_client_required": True,
            "per_source_client_required": bool(require_per_source),
            "per_source_client_complete": per_source_complete,
            "shared_client_complete": shared_complete,
            "connection_source_set_matches_requests": bool(topology.get("connection_source_set_matches_requests")),
            "client_open_source_set_matches_requests": bool(topology.get("client_open_source_set_matches_requests")),
            "topology_report_count": 1,
            "report_count": 1,
        }

    if topology_format != "cxnfs.trainer-remote-client-topology-aggregate.v1":
        missing.append("remote client topology format is missing")
    else:
        if not bool(topology.get("all_reports_have_topology")):
            missing.append("one or more trainer reports did not include remote client topology")
        if not bool(topology.get("all_topologies_complete")):
            missing.append("one or more remote client topology reports are incomplete")
        if _int_value(topology.get("expected_source_count"), 0) < expected:
            missing.append("topology expected source count is below the weighted plan")
        if require_per_source and not bool(topology.get("per_source_client_complete")):
            missing.append("per-source remote clients were not observed for every weighted source")
        if require_per_source and not bool(topology.get("all_connection_source_sets_match_requests")):
            missing.append("remote client connection sources did not match requested weighted sources")
        if require_per_source and not bool(topology.get("all_client_open_source_sets_match_requests")):
            missing.append("remote client open sources did not match requested weighted sources")
        if not require_per_source and not bool(topology.get("complete")):
            missing.append("remote client topology is incomplete")
    return {
        "complete": not missing,
        "missing": missing,
        "remote_client_required": True,
        "per_source_client_required": bool(require_per_source),
        "per_source_client_complete": bool(topology.get("per_source_client_complete")),
        "shared_client_complete": bool(topology.get("shared_client_complete")),
        "all_connection_source_sets_match_requests": bool(topology.get("all_connection_source_sets_match_requests")),
        "all_client_open_source_sets_match_requests": bool(topology.get("all_client_open_source_sets_match_requests")),
        "topology_report_count": _int_value(topology.get("topology_report_count"), 0),
        "report_count": _int_value(topology.get("report_count"), 0),
    }


def _trainer_worker_partition_evidence(
    dataset: CxnfsWeightedTorchDataset,
    batches: Sequence[CxnfsTrainerBatch],
    *,
    trainer_batch_iterator: str,
    data_loader_workers: int,
) -> dict[str, Any]:
    active_workers = int(data_loader_workers or 0)
    worker_sharding_enabled = (
        trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER
        and active_workers > 0
    )
    expected_effective_training_shards = dataset.shard.count * (active_workers if worker_sharding_enabled else 1)
    observed_shards = []
    observed_indices: set[int] = set()
    observed_counts: set[int] = set()
    for batch in batches:
        cursor = batch.resume_cursor
        if cursor is None:
            continue
        observed_indices.add(cursor.shard_index)
        observed_counts.add(cursor.shard_count)
        observed_shards.append({
            "index": cursor.shard_index,
            "count": cursor.shard_count,
        })

    sample_keys = [
        f"{sample.source_name}:{sample.sample_index}"
        for batch in batches
        for sample in batch.samples
    ]
    seen: set[str] = set()
    duplicates: set[str] = set()
    for key in sample_keys:
        if key in seen:
            duplicates.add(key)
        seen.add(key)

    observed_expected_shard_count = (
        not worker_sharding_enabled
        or (
            bool(observed_counts)
            and observed_counts == {expected_effective_training_shards}
        )
    )
    observed_all_worker_shards = (
        not worker_sharding_enabled
        or len(observed_indices) >= active_workers
    )
    no_duplicate_source_samples = len(duplicates) == 0
    complete = (
        not worker_sharding_enabled
        or (
            bool(sample_keys)
            and observed_expected_shard_count
            and observed_all_worker_shards
            and no_duplicate_source_samples
        )
    )
    return {
        "format": TRAINER_WORKER_PARTITION_FORMAT,
        "complete": complete,
        "trainer_batch_iterator": trainer_batch_iterator,
        "worker_sharding_enabled": worker_sharding_enabled,
        "data_loader_workers": active_workers,
        "base_training_shard": {
            "index": dataset.shard.index,
            "count": dataset.shard.count,
        },
        "expected_effective_training_shards": expected_effective_training_shards,
        "observed_training_shards": observed_shards,
        "observed_training_shard_count": len(observed_indices),
        "observed_expected_shard_count": observed_expected_shard_count,
        "observed_all_worker_shards": observed_all_worker_shards,
        "sample_identity_count": len(sample_keys),
        "unique_sample_identity_count": len(seen),
        "duplicate_source_sample_count": len(sample_keys) - len(seen),
        "duplicated_source_samples": sorted(duplicates),
        "no_duplicate_source_samples": no_duplicate_source_samples,
    }


def _trainer_tensor_memory_path_report(
    read_mode: TrainerReadMode,
    allocation: CxnfsTrainerTensorAllocation,
    evidence: Mapping[str, Any],
    sample_count: int,
) -> dict[str, Any] | None:
    if read_mode != TrainerReadMode.DIRECT_TENSORS:
        return None
    policy = allocation.memory_path()
    direct_tensor_reads = int(evidence.get("direct_tensor_read_operations", 0) or 0)
    direct_destination_reads = int(evidence.get("direct_destination_read_operations", 0) or 0)
    missing = []
    if direct_tensor_reads <= 0:
        missing.append("direct tensor reads must be observed")
    if direct_destination_reads <= 0:
        missing.append("direct destination tensor reads must be observed")
    if not bool(policy.get("caller_owned_destination")):
        missing.append("caller-owned tensor destination must be used")
    if not bool(policy.get("python_byte_batch_avoided")):
        missing.append("Python byte batches must be avoided for direct tensor reads")
    return {
        **policy,
        "complete": not missing,
        "missing": missing,
        "read_mode": read_mode.value,
        "sample_count": int(sample_count),
        "direct_tensor_read_operations": direct_tensor_reads,
        "direct_destination_read_operations": direct_destination_reads,
    }


def _add_trainer_batch_memory_observations(
    evidence: dict[str, Any],
    batches: Sequence[CxnfsTrainerBatch],
    read_mode: TrainerReadMode,
) -> None:
    direct_samples = sum(
        1
        for batch in batches
        for sample in batch.samples
        if isinstance(sample, (CxnfsTrainerDirectBatchSample, CxnfsTrainerPipelineBatchSample))
    )
    if direct_samples <= 0:
        return
    byte_size = sum(batch.byte_size for batch in batches)
    if read_mode == TrainerReadMode.DIRECT_BUFFERS:
        evidence["direct_buffer_read_operations"] = max(
            int(evidence.get("direct_buffer_read_operations", 0) or 0),
            direct_samples,
        )
    elif read_mode == TrainerReadMode.DIRECT_TENSORS:
        evidence["direct_tensor_read_operations"] = max(
            int(evidence.get("direct_tensor_read_operations", 0) or 0),
            direct_samples,
        )
    else:
        return
    if int(evidence.get("direct_destination_read_operations", 0) or 0) <= 0:
        evidence["direct_destination_read_operations"] = direct_samples
        evidence["direct_destination_reads_observed"] = True
    if int(evidence.get("read_operations", 0) or 0) <= 0:
        evidence["read_operations"] = direct_samples
    if int(evidence.get("payload_bytes_read", 0) or 0) <= 0:
        evidence["payload_bytes_read"] = byte_size


def _weighted_dataset_read_path_evidence(
    dataset: CxnfsWeightedTorchDataset,
    read_mode: TrainerReadMode,
) -> dict[str, Any]:
    source_reports = []
    collected = []
    descriptor_reports = []
    for source, torch_dataset in zip(dataset.sources, dataset.datasets):
        evidence = _collect_client_read_path_evidence(getattr(torch_dataset.mds, "client", None))
        descriptor_evidence = _collect_mds_descriptor_cache_evidence(getattr(torch_dataset, "mds", None))
        if descriptor_evidence:
            descriptor_reports.append(descriptor_evidence)
        collected.extend(evidence)
        source_reports.append({
            "source": source.name,
            "bulk_payload_transport_count": len(evidence),
            "bulk_payload_transports": evidence,
            "mds_descriptor_cache": descriptor_evidence,
        })

    totals = _aggregate_read_path_evidence(collected)
    descriptor_totals = _aggregate_mds_descriptor_cache_evidence(descriptor_reports)
    prefetch_admission = dataset.prefetch_admission_evidence()
    totals.update({
        "read_mode": read_mode.value,
        "all_runs_have_evidence": True,
        "uses_caller_owned_destinations": read_mode in {TrainerReadMode.DIRECT_BUFFERS, TrainerReadMode.DIRECT_TENSORS},
        "source_count": len(dataset.sources),
        "bulk_payload_transport_count": len(collected),
        "sources": source_reports,
        "weighted_prefetch_admission": prefetch_admission,
        "weighted_prefetch_chunk_window": prefetch_admission["chunk_window"],
        "weighted_prefetch_max_bytes": prefetch_admission["max_prefetch_bytes"],
        "weighted_prefetch_bounded": prefetch_admission["bounded"],
        "weighted_prefetch_observed": prefetch_admission["observed"],
        "weighted_prefetch_plan_count": prefetch_admission["plan_count"],
        "weighted_prefetch_delayed_plan_count": prefetch_admission["delayed_plan_count"],
        "weighted_prefetch_oversized_plan_count": prefetch_admission["oversized_plan_count"],
        "weighted_prefetch_estimated_payload_bytes": prefetch_admission["estimated_payload_bytes"],
        "weighted_prefetch_max_plan_bytes": prefetch_admission["max_plan_bytes"],
        "weighted_prefetch_max_observed_inflight_bytes": prefetch_admission["max_observed_inflight_bytes"],
        "weighted_prefetch_byte_window_engaged": prefetch_admission["byte_window_engaged"],
        "weighted_prefetch_byte_window_respected": prefetch_admission["byte_window_respected"],
        **descriptor_totals,
    })
    return totals


def _collect_mds_descriptor_cache_evidence(mds: Any) -> dict[str, Any]:
    method = getattr(mds, "descriptor_cache_evidence", None)
    if not callable(method):
        return {}
    evidence = method()
    return dict(evidence) if isinstance(evidence, Mapping) else {}


def _aggregate_mds_descriptor_cache_evidence(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    header_entries = sum(_int_value(report.get("sample_header_cache_entries"), 0) for report in reports)
    header_hits = sum(_int_value(report.get("sample_header_cache_hits"), 0) for report in reports)
    header_misses = sum(_int_value(report.get("sample_header_cache_misses"), 0) for report in reports)
    path_entries = sum(_int_value(report.get("sample_path_cache_entries"), 0) for report in reports)
    path_hits = sum(_int_value(report.get("sample_path_cache_hits"), 0) for report in reports)
    path_misses = sum(_int_value(report.get("sample_path_cache_misses"), 0) for report in reports)
    return {
        "mds_descriptor_cache_observed": bool(reports),
        "mds_descriptor_cache_source_count": len(reports),
        "mds_sample_header_cache_entries": header_entries,
        "mds_sample_header_cache_hits": header_hits,
        "mds_sample_header_cache_misses": header_misses,
        "mds_sample_header_cache_reuse_observed": header_hits > 0,
        "mds_sample_path_cache_entries": path_entries,
        "mds_sample_path_cache_hits": path_hits,
        "mds_sample_path_cache_misses": path_misses,
        "mds_sample_path_cache_reuse_observed": path_hits > 0,
    }


def _collect_client_read_path_evidence(client: Any) -> list[dict[str, Any]]:
    visited: set[int] = set()
    collected: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if value is None:
            return
        marker = id(value)
        if marker in visited:
            return
        visited.add(marker)
        method = getattr(value, "read_path_evidence", None)
        if callable(method):
            evidence = method()
            if isinstance(evidence, Mapping):
                collected.append(dict(evidence))
            return
        for attribute in ("delegate", "client", "_transport", "_prefix_transport", "_payload_transport"):
            if hasattr(value, attribute):
                visit(getattr(value, attribute))

    visit(client)
    return collected


def _first_client_path_evidence(client: Any, method_name: str) -> dict[str, Any]:
    visited: set[int] = set()

    def visit(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        marker = id(value)
        if marker in visited:
            return {}
        visited.add(marker)
        method = getattr(value, method_name, None)
        if callable(method):
            evidence = method()
            if isinstance(evidence, Mapping):
                return dict(evidence)
        for attribute in ("delegate", "client", "_transport", "_prefix_transport", "_payload_transport"):
            if hasattr(value, attribute):
                nested = visit(getattr(value, attribute))
                if nested:
                    return nested
        return {}

    return visit(client)


def _collect_batch_worker_read_path_evidence(batches: Sequence[CxnfsTrainerBatch]) -> list[dict[str, Any]]:
    reports = []
    for batch in batches:
        evidence = getattr(batch, "worker_read_path_evidence", None)
        if isinstance(evidence, Mapping):
            reports.append(dict(evidence))
    return reports


def _read_path_evidence_delta(
    current: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if previous is None:
        return dict(current)
    delta = dict(current)
    sum_keys = {
        "read_operations",
        "byte_return_read_operations",
        "direct_buffer_read_operations",
        "direct_tensor_read_operations",
        "direct_destination_read_operations",
        "frame_sink_read_operations",
        "direct_destination_frame_sink_read_operations",
        "direct_destination_parallel_frame_sink_read_operations",
        "direct_destination_parallel_frame_sink_frames",
        "fallback_frame_iter_read_operations",
        "failed_read_operations",
        "failed_byte_return_read_operations",
        "failed_direct_buffer_read_operations",
        "failed_direct_tensor_read_operations",
        "failed_direct_destination_read_operations",
        "direct_destination_zeroed_bytes_on_failure",
        "read_retry_attempts",
        "read_retry_successes",
        "bulk_transport_reconnects",
        "quic_read_transport_evidence_count",
        "quic_read_transport_connection_opened_count",
        "quic_read_transport_connection_attempt_count",
        "quic_read_transport_connection_retry_count",
        "quic_read_transport_successful_read_operations",
        "quic_read_transport_failed_read_operations",
        "quic_read_transport_attempted_read_streams",
        "quic_read_transport_read_streams_created",
        "quic_read_transport_read_streams_eof_verified",
        "quic_read_transport_total_attempted_streams",
        "quic_read_transport_total_successful_streams",
        "quic_read_transport_connection_reuse_count",
        "quic_read_transport_read_partition_count",
        "quic_read_transport_read_partition_chunk_count",
        "quic_read_transport_read_partition_requested_bytes",
        "quic_read_transport_read_partition_payload_bytes",
        "quic_read_transport_read_partition_coverage_report_count",
        "weighted_prefetch_plan_count",
        "weighted_prefetch_delayed_plan_count",
        "weighted_prefetch_oversized_plan_count",
        "weighted_prefetch_estimated_payload_bytes",
        "mds_descriptor_cache_source_count",
        "mds_sample_header_cache_entries",
        "mds_sample_header_cache_hits",
        "mds_sample_header_cache_misses",
        "mds_sample_path_cache_entries",
        "mds_sample_path_cache_hits",
        "mds_sample_path_cache_misses",
        "frame_sink_frames",
        "fallback_frame_iter_frames",
        "payload_bytes_read",
    }
    for key in sum_keys:
        delta[key] = max(0, _int_value(current.get(key), 0) - _int_value(previous.get(key), 0))
    for key in (
        "read_retry_limit",
        "max_observed_cords",
        "last_observed_cords",
        "direct_destination_max_observed_cords",
        "direct_destination_last_observed_cords",
        "source_count",
        "weighted_prefetch_chunk_window",
        "weighted_prefetch_max_plan_bytes",
        "weighted_prefetch_max_observed_inflight_bytes",
        "quic_read_transport_max_read_stream_count",
        "quic_read_transport_last_read_stream_count",
        "quic_read_transport_last_attempted_read_stream_count",
    ):
        if key in current:
            delta[key] = _int_value(current.get(key), 0)
    delta["bulk_payload_transport_count"] = _int_value(current.get("bulk_payload_transport_count"), 0)
    delta["transport_frame_sink_supported"] = bool(current.get("transport_frame_sink_supported"))
    delta["transport_frame_sink_observed"] = bool(current.get("transport_frame_sink_observed"))
    delta["direct_destination_reads_observed"] = bool(current.get("direct_destination_reads_observed"))
    delta["direct_destination_failure_zeroing_enabled"] = bool(current.get("direct_destination_failure_zeroing_enabled", True))
    delta["frame_sink_used_for_all_direct_destination_reads"] = bool(
        current.get("frame_sink_used_for_all_direct_destination_reads")
    )
    delta["direct_destination_used_parallel_cords"] = bool(current.get("direct_destination_used_parallel_cords"))
    delta["direct_destination_frame_sink_cord_fanout_verified"] = bool(
        current.get("direct_destination_frame_sink_cord_fanout_verified")
    )
    delta["quic_read_transport_observed"] = bool(current.get("quic_read_transport_observed"))
    delta["quic_read_transport_connection_reuse_observed"] = bool(
        current.get("quic_read_transport_connection_reuse_observed")
    )
    delta["quic_read_transport_complete"] = bool(current.get("quic_read_transport_complete"))
    delta["quic_read_transport_read_partition_coverage_verified"] = bool(
        current.get("quic_read_transport_read_partition_coverage_verified")
    )
    delta["weighted_prefetch_bounded"] = bool(current.get("weighted_prefetch_bounded"))
    delta["weighted_prefetch_observed"] = bool(current.get("weighted_prefetch_observed"))
    delta["weighted_prefetch_byte_window_engaged"] = bool(current.get("weighted_prefetch_byte_window_engaged"))
    delta["weighted_prefetch_byte_window_respected"] = bool(current.get("weighted_prefetch_byte_window_respected", True))
    delta["mds_descriptor_cache_observed"] = bool(current.get("mds_descriptor_cache_observed"))
    delta["mds_sample_header_cache_reuse_observed"] = bool(
        current.get("mds_sample_header_cache_reuse_observed")
    )
    delta["mds_sample_path_cache_reuse_observed"] = bool(current.get("mds_sample_path_cache_reuse_observed"))
    return delta


def _aggregate_read_path_evidence(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    totals = {
        "read_operations": 0,
        "byte_return_read_operations": 0,
        "direct_buffer_read_operations": 0,
        "direct_tensor_read_operations": 0,
        "direct_destination_read_operations": 0,
        "frame_sink_read_operations": 0,
        "direct_destination_frame_sink_read_operations": 0,
        "direct_destination_parallel_frame_sink_read_operations": 0,
        "direct_destination_parallel_frame_sink_frames": 0,
        "fallback_frame_iter_read_operations": 0,
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
        "fallback_frame_iter_frames": 0,
        "payload_bytes_read": 0,
        "max_observed_cords": 0,
        "direct_destination_max_observed_cords": 0,
        "direct_destination_last_observed_cords": 0,
        "bulk_payload_transport_count": 0,
        "quic_read_transport_evidence_count": 0,
        "quic_read_transport_connection_opened_count": 0,
        "quic_read_transport_connection_attempt_count": 0,
        "quic_read_transport_connection_retry_count": 0,
        "quic_read_transport_successful_read_operations": 0,
        "quic_read_transport_failed_read_operations": 0,
        "quic_read_transport_last_read_stream_count": 0,
        "quic_read_transport_last_attempted_read_stream_count": 0,
        "quic_read_transport_attempted_read_streams": 0,
        "quic_read_transport_read_streams_created": 0,
        "quic_read_transport_read_streams_eof_verified": 0,
        "quic_read_transport_read_stream_completion_report_count": 0,
        "quic_read_transport_read_stream_eof_report_count": 0,
        "quic_read_transport_total_attempted_streams": 0,
        "quic_read_transport_total_successful_streams": 0,
        "quic_read_transport_connection_reuse_count": 0,
        "quic_read_transport_max_read_stream_count": 0,
        "quic_read_transport_read_partition_count": 0,
        "quic_read_transport_read_partition_chunk_count": 0,
        "quic_read_transport_read_partition_requested_bytes": 0,
        "quic_read_transport_read_partition_payload_bytes": 0,
        "quic_read_transport_read_partition_coverage_report_count": 0,
        "quic_read_transport_cipher_suite_counts": {},
        "quic_read_transport_tls_version_counts": {},
        "weighted_prefetch_plan_count": 0,
        "weighted_prefetch_delayed_plan_count": 0,
        "weighted_prefetch_oversized_plan_count": 0,
        "weighted_prefetch_estimated_payload_bytes": 0,
        "weighted_prefetch_chunk_window": 0,
        "weighted_prefetch_max_plan_bytes": 0,
        "weighted_prefetch_max_observed_inflight_bytes": 0,
        "mds_descriptor_cache_source_count": 0,
        "mds_sample_header_cache_entries": 0,
        "mds_sample_header_cache_hits": 0,
        "mds_sample_header_cache_misses": 0,
        "mds_sample_path_cache_entries": 0,
        "mds_sample_path_cache_hits": 0,
        "mds_sample_path_cache_misses": 0,
    }
    transport_frame_sink_supported = False
    mds_descriptor_cache_observed = False
    weighted_prefetch_bounded = False
    weighted_prefetch_observed = False
    weighted_prefetch_byte_window_engaged = False
    weighted_prefetch_byte_window_respected = True
    for report in reports:
        transport_frame_sink_supported = transport_frame_sink_supported or bool(report.get("transport_frame_sink_supported"))
        mds_descriptor_cache_observed = mds_descriptor_cache_observed or bool(
            report.get("mds_descriptor_cache_observed")
        )
        weighted_prefetch_bounded = weighted_prefetch_bounded or bool(report.get("weighted_prefetch_bounded"))
        weighted_prefetch_observed = weighted_prefetch_observed or bool(report.get("weighted_prefetch_observed"))
        weighted_prefetch_byte_window_engaged = weighted_prefetch_byte_window_engaged or bool(
            report.get("weighted_prefetch_byte_window_engaged")
        )
        weighted_prefetch_byte_window_respected = (
            weighted_prefetch_byte_window_respected
            and bool(report.get("weighted_prefetch_byte_window_respected", True))
        )
        dict_total_keys = {
            "quic_read_transport_cipher_suite_counts",
            "quic_read_transport_tls_version_counts",
        }
        for key in totals:
            if key in dict_total_keys:
                target = totals[key]
                for name, count in _mapping(report.get(key)).items():
                    if name:
                        target[str(name)] = int(target.get(str(name), 0)) + _int_value(count, 0)
                continue
            value = report.get(key, 0)
            if key in {
                "max_observed_cords",
                "direct_destination_max_observed_cords",
                "direct_destination_last_observed_cords",
                "read_retry_limit",
                "weighted_prefetch_chunk_window",
                "weighted_prefetch_max_plan_bytes",
                "weighted_prefetch_max_observed_inflight_bytes",
                "quic_read_transport_max_read_stream_count",
                "quic_read_transport_last_read_stream_count",
                "quic_read_transport_last_attempted_read_stream_count",
            }:
                totals[key] = max(int(totals[key]), int(value or 0))
            else:
                totals[key] = int(totals[key]) + int(value or 0)
        bulk_transport_evidence = _mapping(report.get("bulk_transport_read_evidence"))
        if (
            bulk_transport_evidence.get("format") == "cxnfs.quic-bulk-transport-evidence.v1"
            and _int_value(report.get("quic_read_transport_evidence_count"), 0) == 0
        ):
            last_stream_count = _int_value(bulk_transport_evidence.get("last_read_stream_count"), 0)
            last_attempted_stream_count = _int_value(
                bulk_transport_evidence.get("last_attempted_read_stream_count"),
                0,
            )
            cipher_suite = str(bulk_transport_evidence.get("cipher_suite") or "")
            if cipher_suite:
                cipher_counts = totals["quic_read_transport_cipher_suite_counts"]
                cipher_counts[cipher_suite] = int(cipher_counts.get(cipher_suite, 0)) + 1
            tls_version = str(bulk_transport_evidence.get("tls_version") or "")
            if tls_version:
                tls_counts = totals["quic_read_transport_tls_version_counts"]
                tls_counts[tls_version] = int(tls_counts.get(tls_version, 0)) + 1
            totals["quic_read_transport_evidence_count"] = int(totals["quic_read_transport_evidence_count"]) + 1
            totals["quic_read_transport_connection_opened_count"] = (
                int(totals["quic_read_transport_connection_opened_count"])
                + _int_value(bulk_transport_evidence.get("connection_opened_count"), 0)
            )
            totals["quic_read_transport_connection_attempt_count"] = (
                int(totals["quic_read_transport_connection_attempt_count"])
                + _int_value(bulk_transport_evidence.get("connection_attempt_count"), 0)
            )
            totals["quic_read_transport_connection_retry_count"] = (
                int(totals["quic_read_transport_connection_retry_count"])
                + _int_value(bulk_transport_evidence.get("connection_retry_count"), 0)
            )
            totals["quic_read_transport_successful_read_operations"] = (
                int(totals["quic_read_transport_successful_read_operations"])
                + _int_value(bulk_transport_evidence.get("successful_read_operations"), 0)
            )
            totals["quic_read_transport_failed_read_operations"] = (
                int(totals["quic_read_transport_failed_read_operations"])
                + _int_value(bulk_transport_evidence.get("failed_read_operations"), 0)
            )
            totals["quic_read_transport_last_read_stream_count"] = max(
                int(totals["quic_read_transport_last_read_stream_count"]),
                last_stream_count,
            )
            totals["quic_read_transport_last_attempted_read_stream_count"] = max(
                int(totals["quic_read_transport_last_attempted_read_stream_count"]),
                last_attempted_stream_count,
            )
            totals["quic_read_transport_attempted_read_streams"] = (
                int(totals["quic_read_transport_attempted_read_streams"])
                + _int_value(bulk_transport_evidence.get("attempted_read_streams"), 0)
            )
            totals["quic_read_transport_read_streams_created"] = (
                int(totals["quic_read_transport_read_streams_created"])
                + _int_value(bulk_transport_evidence.get("read_streams_created"), 0)
            )
            totals["quic_read_transport_read_streams_eof_verified"] = (
                int(totals["quic_read_transport_read_streams_eof_verified"])
                + _int_value(bulk_transport_evidence.get("read_streams_eof_verified"), 0)
            )
            attempted_read_streams = _int_value(bulk_transport_evidence.get("attempted_read_streams"), 0)
            read_streams_created = _int_value(bulk_transport_evidence.get("read_streams_created"), 0)
            read_streams_eof_verified = _int_value(
                bulk_transport_evidence.get("read_streams_eof_verified"),
                0,
            )
            failed_read_operations = _int_value(bulk_transport_evidence.get("failed_read_operations"), 0)
            if bool(bulk_transport_evidence.get("read_stream_completion_verified")) or (
                attempted_read_streams > 0
                and failed_read_operations == 0
                and read_streams_created == attempted_read_streams
            ):
                totals["quic_read_transport_read_stream_completion_report_count"] = (
                    int(totals["quic_read_transport_read_stream_completion_report_count"]) + 1
                )
            if (
                attempted_read_streams > 0
                and read_streams_eof_verified == attempted_read_streams
                and failed_read_operations == 0
            ) or (
                read_streams_eof_verified <= 0
                and bool(bulk_transport_evidence.get("read_stream_eof_verified"))
            ):
                totals["quic_read_transport_read_stream_eof_report_count"] = (
                    int(totals["quic_read_transport_read_stream_eof_report_count"]) + 1
                )
            totals["quic_read_transport_total_attempted_streams"] = (
                int(totals["quic_read_transport_total_attempted_streams"])
                + _int_value(bulk_transport_evidence.get("total_attempted_streams"), 0)
            )
            totals["quic_read_transport_total_successful_streams"] = (
                int(totals["quic_read_transport_total_successful_streams"])
                + _int_value(bulk_transport_evidence.get("total_successful_streams"), 0)
            )
            totals["quic_read_transport_max_read_stream_count"] = max(
                int(totals["quic_read_transport_max_read_stream_count"]),
                last_stream_count,
                last_attempted_stream_count,
            )
            totals["quic_read_transport_read_partition_count"] = (
                int(totals["quic_read_transport_read_partition_count"])
                + _int_value(bulk_transport_evidence.get("read_partition_count"), 0)
            )
            totals["quic_read_transport_read_partition_chunk_count"] = (
                int(totals["quic_read_transport_read_partition_chunk_count"])
                + _int_value(bulk_transport_evidence.get("read_partition_chunk_count"), 0)
            )
            totals["quic_read_transport_read_partition_requested_bytes"] = (
                int(totals["quic_read_transport_read_partition_requested_bytes"])
                + _int_value(bulk_transport_evidence.get("read_partition_requested_bytes"), 0)
            )
            totals["quic_read_transport_read_partition_payload_bytes"] = (
                int(totals["quic_read_transport_read_partition_payload_bytes"])
                + _int_value(bulk_transport_evidence.get("read_partition_payload_bytes"), 0)
            )
            if bool(bulk_transport_evidence.get("read_partition_covers_plan")):
                totals["quic_read_transport_read_partition_coverage_report_count"] = (
                    int(totals["quic_read_transport_read_partition_coverage_report_count"]) + 1
                )
            if bool(bulk_transport_evidence.get("connection_reused_for_streams")):
                totals["quic_read_transport_connection_reuse_count"] = (
                    int(totals["quic_read_transport_connection_reuse_count"]) + 1
                )
    direct_destination_reads = int(totals["direct_destination_read_operations"])
    direct_destination_sink_reads = int(totals["direct_destination_frame_sink_read_operations"])
    direct_destination_parallel_sink_reads = int(totals["direct_destination_parallel_frame_sink_read_operations"])
    direct_destination_max_cords = int(totals["direct_destination_max_observed_cords"])
    if direct_destination_max_cords <= 0 and direct_destination_reads > 0:
        direct_destination_max_cords = int(totals["max_observed_cords"])
        totals["direct_destination_max_observed_cords"] = direct_destination_max_cords
    if (
        direct_destination_parallel_sink_reads <= 0
        and direct_destination_reads > 0
        and direct_destination_sink_reads == direct_destination_reads
        and direct_destination_max_cords > 1
    ):
        direct_destination_parallel_sink_reads = direct_destination_sink_reads
        totals["direct_destination_parallel_frame_sink_read_operations"] = direct_destination_parallel_sink_reads
        totals["direct_destination_parallel_frame_sink_frames"] = int(totals["frame_sink_frames"])
    quic_evidence_count = int(totals["quic_read_transport_evidence_count"])
    quic_read_streams = int(totals["quic_read_transport_read_streams_created"])
    quic_successful_reads = int(totals["quic_read_transport_successful_read_operations"])
    quic_failed_reads = int(totals["quic_read_transport_failed_read_operations"])
    quic_max_stream_count = int(totals["quic_read_transport_max_read_stream_count"])
    quic_attempted_read_streams = int(totals["quic_read_transport_attempted_read_streams"])
    quic_eof_verified_streams = int(totals["quic_read_transport_read_streams_eof_verified"])
    quic_completion_report_count = int(totals["quic_read_transport_read_stream_completion_report_count"])
    quic_eof_report_count = int(totals["quic_read_transport_read_stream_eof_report_count"])
    quic_partition_coverage_count = int(totals["quic_read_transport_read_partition_coverage_report_count"])
    quic_expected_cipher_suite_count = int(
        _mapping(totals.get("quic_read_transport_cipher_suite_counts")).get(CXNFS_TLS_CIPHER_SUITE, 0) or 0
    )
    quic_all_reports_expected_cipher_suite = (
        quic_evidence_count > 0
        and quic_expected_cipher_suite_count == quic_evidence_count
    )
    quic_stream_completion_verified = (
        quic_evidence_count > 0
        and quic_completion_report_count == quic_evidence_count
        and quic_attempted_read_streams > 0
        and quic_read_streams == quic_attempted_read_streams
        and quic_failed_reads == 0
    )
    quic_stream_eof_verified = (
        quic_evidence_count > 0
        and quic_eof_report_count == quic_evidence_count
        and quic_eof_verified_streams == quic_attempted_read_streams
        and quic_stream_completion_verified
    )
    totals.update({
        "transport_frame_sink_supported": transport_frame_sink_supported,
        "transport_frame_sink_observed": int(totals["frame_sink_read_operations"]) > 0,
        "direct_destination_reads_observed": direct_destination_reads > 0,
        "direct_destination_failure_zeroing_enabled": True,
        "frame_sink_used_for_all_direct_destination_reads": (
            direct_destination_reads > 0 and direct_destination_sink_reads == direct_destination_reads
        ),
        "direct_destination_used_parallel_cords": direct_destination_max_cords > 1,
        "direct_destination_frame_sink_cord_fanout_verified": (
            direct_destination_reads > 0
            and direct_destination_sink_reads == direct_destination_reads
            and direct_destination_parallel_sink_reads > 0
            and direct_destination_max_cords > 1
        ),
        "quic_read_transport_observed": quic_evidence_count > 0,
        "quic_read_transport_connection_reuse_observed": (
            int(totals["quic_read_transport_connection_reuse_count"]) > 0
        ),
        "quic_read_transport_stream_completion_verified": quic_stream_completion_verified,
        "quic_read_transport_stream_eof_verified": quic_stream_eof_verified,
        "quic_read_transport_read_partition_coverage_verified": (
            quic_evidence_count > 0
            and quic_partition_coverage_count == quic_evidence_count
            and int(totals["quic_read_transport_read_partition_count"]) > 0
        ),
        "quic_read_transport_expected_cipher_suite_count": quic_expected_cipher_suite_count,
        "quic_read_transport_all_reports_expected_cipher_suite": quic_all_reports_expected_cipher_suite,
        "weighted_prefetch_bounded": weighted_prefetch_bounded,
        "weighted_prefetch_observed": weighted_prefetch_observed,
        "weighted_prefetch_byte_window_engaged": weighted_prefetch_byte_window_engaged,
        "weighted_prefetch_byte_window_respected": weighted_prefetch_byte_window_respected,
        "quic_read_transport_complete": (
            quic_evidence_count > 0
            and quic_read_streams > 0
            and quic_successful_reads > 0
            and quic_failed_reads == 0
            and quic_max_stream_count > 1
            and quic_stream_completion_verified
            and quic_stream_eof_verified
            and quic_all_reports_expected_cipher_suite
        ),
        "mds_descriptor_cache_observed": mds_descriptor_cache_observed,
        "mds_sample_header_cache_reuse_observed": int(totals["mds_sample_header_cache_hits"]) > 0,
        "mds_sample_path_cache_reuse_observed": int(totals["mds_sample_path_cache_hits"]) > 0,
    })
    return totals


def _trainer_contract_report(
    plan: CxnfsWeightedMdsPlan,
    dataset: CxnfsWeightedTorchDataset,
    batches: Sequence[CxnfsTrainerBatch],
    *,
    root: str | Path,
    remote_root: str | Path | None,
    bulk_transport: str,
    bulk_read_retries: int,
    bulk_write_retries: int,
    read_mode: TrainerReadMode,
    batch_size: int,
    requested_batches: int,
    rounds_per_read: int,
    max_workers: int,
    source_read_workers: int | None,
    prefetch_chunks: int,
    trainer_prefetch_batches: int,
    shuffle: bool,
    seed: int,
    epoch: int,
    samples_per_epoch: int | None,
    repeat: bool,
    trainer_batch_iterator: str,
    data_loader_workers: int,
    tensor_allocation: CxnfsTrainerTensorAllocation,
    min_workload_runs: int,
    min_workload_samples: int,
    min_workload_bytes: int,
    min_cord_sweep: int,
) -> dict[str, Any]:
    sample_count = sum(batch.sample_count for batch in batches)
    byte_size = sum(batch.byte_size for batch in batches)
    source_counts = _aggregate_batch_source_counts(batches)
    remote_root_value = _normalize_remote_root(remote_root) if remote_root is not None else None
    plan_payload = plan.to_dict()
    tensor_allocation_payload = tensor_allocation.to_dict()
    return {
        "format": TRAINER_CONTRACT_FORMAT,
        "root": str(root),
        "remote_root": remote_root_value,
        "weights": {source.name: source.weight for source in plan.sources},
        "weighted_mds_plan": plan_payload,
        "workload_fingerprint": plan_payload["workload_fingerprint"],
        "workload_requirements": _trainer_contract_workload_requirements_from_values(
            min_workload_runs=min_workload_runs,
            min_workload_samples=min_workload_samples,
            min_workload_bytes=min_workload_bytes,
            min_cord_sweep=min_cord_sweep,
        ),
        "python_surface": {
            "requires_mount": False,
            "touches_trainer_code": False,
            "supports_weighted_sources": True,
            "supports_per_source_clients": True,
            "supports_direct_buffers": True,
            "supports_direct_tensors": True,
            "supports_tensor_allocation_policy": True,
            "supports_trainer_dataloader": True,
            "supports_dataloader_worker_partitioning": True,
            "supports_trainer_stream": True,
            "supports_trainer_prefetch_stream": True,
            "supports_trainer_cursor": True,
            "supports_training_pipeline": True,
            "training_pipeline_object": "CxnfsTrainingPipeline",
            "supports_pipeline_direct_tensor_batches": True,
            "supports_pipeline_gpu_transforms": True,
            "supports_pipeline_torch_compile": True,
            "pipeline_cpu_augmentation_default": "disabled-with-explicit-opt-in",
            "direct_tensors_require_torch": True,
            "destination_owner": "trainer",
            "default_tensor_allocation": CxnfsTrainerTensorAllocation().to_dict(),
            "stream_object": "CxnfsTrainerStream",
            "stream_state_format": TRAINER_STREAM_STATE_FORMAT,
            "batch_object": "CxnfsTrainerBatch",
            "dataloader_batch_object": "CxnfsTrainerBatch",
            "dataloader_batch_size": None,
        },
        "network_contract": {
            "transport": "local-mds" if remote_root_value is None else (
                "cxnfs-quic" if bulk_transport == PYTHON_BULK_TRANSPORT_QUIC
                else "cxnfs-length-prefixed"
            ),
            "bulk_transport": bulk_transport,
            "bulk_transport_available": _python_bulk_transport_available(bulk_transport),
            "remote_reads_require_credentials": remote_root_value is not None,
            "raw_password_sent_over_control": False,
            "per_source_client_default": True,
            "max_cords": ABSOLUTE_MAX_CORDS,
            "bulk_read_retries": bulk_read_retries,
            "bulk_write_retries": bulk_write_retries,
        },
        "read_contract": {
            "read_mode": read_mode.value,
            "destination_type": _trainer_contract_destination_type(read_mode),
            "trainer_batch_iterator": trainer_batch_iterator,
            "data_loader_workers": data_loader_workers,
            "worker_partitioning": {
                "format": TRAINER_WORKER_PARTITION_FORMAT,
                "enabled": trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER and data_loader_workers > 0,
                "base_training_shard": {
                    "index": dataset.shard.index,
                    "count": dataset.shard.count,
                },
                "effective_training_shard_count": dataset.shard.count * (
                    data_loader_workers
                    if trainer_batch_iterator == TRAINER_BATCH_ITERATOR_DATALOADER and data_loader_workers > 0
                    else 1
                ),
            },
            "tensor_allocation": tensor_allocation_payload if read_mode == TrainerReadMode.DIRECT_TENSORS else None,
            "batch_size": batch_size,
            "requested_batches": requested_batches,
            "rounds_per_read": rounds_per_read,
            "max_workers": max_workers,
            "source_read_workers": source_read_workers,
            "prefetch_chunks": prefetch_chunks,
            "trainer_prefetch_batches": trainer_prefetch_batches,
            "shuffle": shuffle,
            "seed": seed,
            "epoch": epoch,
            "samples_per_epoch": samples_per_epoch,
            "repeat": repeat,
            "training_shard": {
                "index": dataset.shard.index,
                "count": dataset.shard.count,
            },
        },
        "validation": {
            "validated": True,
            "batch_count": len(batches),
            "sample_count": sample_count,
            "byte_size": byte_size,
            "source_counts": source_counts,
            "batches": [
                _trainer_contract_batch_summary(batch, read_mode)
                for batch in batches
            ],
        },
    }


def _aggregate_batch_source_counts(batches: Sequence[CxnfsTrainerBatch]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for batch in batches:
        for name, count in batch.source_counts.items():
            counts[name] = counts.get(name, 0) + count
    return counts


def _trainer_contract_batch_summary(batch: CxnfsTrainerBatch, read_mode: TrainerReadMode) -> dict[str, Any]:
    payload = batch.to_dict(include_sample_data=False)
    payload["read_mode"] = read_mode.value
    payload["destination_type"] = _trainer_contract_destination_type(read_mode)
    return payload


def _trainer_contract_destination_type(read_mode: TrainerReadMode) -> str:
    if read_mode == TrainerReadMode.DIRECT_TENSORS:
        return "torch-uint8-tensor"
    if read_mode == TrainerReadMode.DIRECT_BUFFERS:
        return "writable-byte-buffer"
    return "decoded-sample"


def _python_bulk_transport_available(bulk_transport: str) -> bool:
    try:
        require_supported_python_bulk_transport(bulk_transport)
        return True
    except RuntimeError:
        return False


def _apply_trainer_stream_objective(
    payload: dict[str, Any],
    objective_name: str | None,
    baseline_mb_per_second: float | None,
) -> None:
    if not objective_name:
        return
    result = NativeReadBenchmarkResult(
        "cxnfs-trainer-stream",
        str(payload.get("remote_root") or payload.get("root") or "weighted-mds"),
        max(1, int(payload.get("batch_count", 0))),
        int(payload.get("warmup_batches", 0)),
        max(1, int(payload.get("batch_size", 1))),
        int(payload.get("byte_size", 0)),
        float(payload.get("elapsed_seconds", 1e-9)),
    )
    evaluation = evaluate_native_read_objective(
        result,
        objective_name=objective_name,
        baseline_mb_per_second=baseline_mb_per_second,
    )
    payload.update({
        "objective": evaluation.objective_name,
        "objective_baseline": evaluation.baseline_name,
        "objective_bottleneck_gbit_s": evaluation.bottleneck_gbit_s,
        "objective_minimum_link_utilization": evaluation.minimum_link_utilization,
        "objective_candidate_link_utilization": evaluation.candidate_link_utilization,
        "objective_minimum_candidate_mb_per_second": evaluation.minimum_bytes_per_second / 1_000_000.0,
        "objective_candidate_mb_per_second": evaluation.candidate.megabytes_per_second,
        "objective_baseline_mb_per_second": evaluation.baseline_bytes_per_second / 1_000_000.0,
        "objective_minimum_baseline_improvement": evaluation.minimum_baseline_improvement,
        "objective_candidate_improvement": evaluation.improvement_ratio,
        "objective_link_passed": evaluation.link_utilization_passed,
        "objective_throughput_passed": evaluation.minimum_throughput_passed,
        "objective_improvement_passed": evaluation.baseline_improvement_passed,
        "objective_passed": evaluation.passed,
    })


def _trainer_batch_sample_to_dict(
    sample: CxnfsTrainerBatchSample,
    *,
    include_sample_data: bool,
) -> dict[str, Any]:
    payload = {
        "source_name": sample.source_name,
        "source_index": sample.source_index,
        "sample_index": sample.sample_index,
        "byte_size": _trainer_sample_nbytes(sample),
    }
    sample_path = getattr(sample, "path", None)
    if sample_path is not None:
        payload["path"] = sample_path
    sample_read = getattr(sample, "read", None)
    if isinstance(sample_read, SampleRead):
        payload["read"] = {
            "shard_path": sample_read.shard_path,
            "offset": sample_read.offset,
            "nbytes": sample_read.nbytes,
            "sample_id": sample_read.sample_id,
        }
        payload["pipeline_filled_destination"] = True
    if include_sample_data:
        payload["data"] = sample.data
    return payload


def _trainer_sample_nbytes(sample: Any) -> int:
    direct_size = getattr(sample, "byte_size", None)
    if isinstance(direct_size, int):
        return direct_size
    return _payload_nbytes(sample.data)


def _payload_nbytes(value: Any) -> int:
    if isinstance(value, CxnfsWeightedSample):
        return _payload_nbytes(value.data)
    if isinstance(value, MdsSample):
        return len(value.data)
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, bytearray):
        return len(value)
    try:
        return memoryview(value).nbytes
    except TypeError:
        return 0


def _identity_collate(value):
    return value


def _default_direct_destination_factory(
    _source_name: str,
    _source_index: int,
    _sample_index: int,
    data_length: int,
) -> bytearray:
    return bytearray(data_length)


def trainer_tensor_factory(
    allocation: CxnfsTrainerTensorAllocation | None = None,
) -> DirectTensorFactory:
    active = allocation or CxnfsTrainerTensorAllocation()

    def allocate(
        _source_name: str,
        _source_index: int,
        _sample_index: int,
        data_length: int,
    ):
        return _allocate_trainer_tensor(active, data_length)

    return allocate


def _resolve_direct_tensor_factory(
    tensor_factory: DirectTensorFactory | None,
    tensor_allocation: CxnfsTrainerTensorAllocation | None,
) -> DirectTensorFactory:
    if tensor_factory is not None and tensor_allocation is not None:
        raise ValueError("use tensor_factory or tensor_allocation, not both")
    if tensor_factory is not None:
        return tensor_factory
    if tensor_allocation is not None:
        return tensor_allocation.factory()
    return _default_direct_tensor_factory


def _allocate_trainer_tensor(allocation: CxnfsTrainerTensorAllocation, data_length: int):
    if data_length <= 0:
        raise ValueError("data_length must be positive")
    torch = _require_torch()
    dtype = _torch_dtype(torch, allocation.dtype)
    item_size = _dtype_nbytes(dtype)
    if data_length % item_size != 0:
        raise ValueError("data_length must align with tensor dtype byte size")
    shape = (data_length // item_size,)
    if allocation.target == TensorMemoryTarget.PYTORCH_CUDA:
        return torch.empty(shape, dtype=dtype, device=allocation.device)
    return _torch_empty(
        torch,
        shape,
        dtype=dtype,
        device="cpu",
        pin_memory=allocation.target == TensorMemoryTarget.PYTORCH_PINNED_CPU,
    )


def _default_direct_tensor_factory(
    _source_name: str,
    _source_index: int,
    _sample_index: int,
    data_length: int,
):
    return _allocate_trainer_tensor(CxnfsTrainerTensorAllocation(), data_length)


def _close_weighted_dataset(dataset: CxnfsTorchDataset, seen: set[int]) -> None:
    mds = getattr(dataset, "mds", None)
    client = getattr(mds, "client", None)
    if client is not None:
        _close_client_tree(client, seen)
        return

    close = getattr(dataset, "close", None)
    if callable(close):
        _close_once(dataset, close, seen)


def _close_client_tree(client, seen: set[int]) -> None:
    prefix_transport = getattr(client, "_prefix_transport", None)
    prefix_delegate = getattr(prefix_transport, "delegate", None)
    if prefix_delegate is not None:
        _close_client_tree(prefix_delegate, seen)
        return

    delegate = getattr(client, "delegate", None)
    clear = getattr(client, "clear", None)
    if delegate is not None and callable(clear):
        _close_once(client, lambda: clear(reset_stats=True), seen)
        _close_client_tree(delegate, seen)
        return

    close = getattr(client, "close", None)
    if callable(close):
        _close_once(client, close, seen)


def _close_once(owner, close: Callable[[], None], seen: set[int]) -> None:
    identity = id(owner)
    if identity in seen:
        return
    seen.add(identity)
    close()


def current_training_shard() -> CxnfsTrainingShard:
    rank = int(os.environ.get("RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    return CxnfsTrainingShard(rank, world_size)


def _cursor_for_training_shard(
    cursor: CxnfsTrainerCursor | Mapping[str, Any] | None,
    worker_cursors: Mapping[tuple[int, int], CxnfsTrainerCursor],
    shard: CxnfsTrainingShard,
) -> CxnfsTrainerCursor | None:
    key = (shard.index, shard.count)
    if key in worker_cursors:
        return worker_cursors[key]
    if cursor is None:
        return None
    active_cursor = cursor if isinstance(cursor, CxnfsTrainerCursor) else CxnfsTrainerCursor.from_mapping(cursor)
    if active_cursor.shard_index == shard.index and active_cursor.shard_count == shard.count:
        return active_cursor
    if (
        active_cursor.emitted == 0
        and active_cursor.chunk_index == 0
        and active_cursor.schedule_position == 0
        and all(value == 0 for value in active_cursor.cursors)
    ):
        return CxnfsTrainerCursor(
            epoch=active_cursor.epoch,
            cursors=tuple(0 for _ in active_cursor.cursors),
            emitted=0,
            chunk_index=0,
            schedule_position=0,
            sample_limit=active_cursor.sample_limit,
            shard_index=shard.index,
            shard_count=shard.count,
            source_names=active_cursor.source_names,
        )
    raise ValueError("multi-worker trainer resume requires worker cursor state")


def _torch_training_shard(base: CxnfsTrainingShard) -> CxnfsTrainingShard:
    worker = _torch_worker_info()
    worker_id = int(worker.id) if worker is not None else 0
    worker_count = int(worker.num_workers) if worker is not None else 1
    return CxnfsTrainingShard(
        (base.index * worker_count) + worker_id,
        base.count * worker_count,
    )


def _torch_worker_info():
    try:
        import torch  # type: ignore
    except Exception:
        return None
    return torch.utils.data.get_worker_info()


def _require_torch():
    try:
        import torch  # type: ignore
    except Exception as exc:  # pragma: no cover - exercised only without torch installed
        raise RuntimeError("PyTorch is required for CXNFS tensor dataset integration") from exc
    return torch
