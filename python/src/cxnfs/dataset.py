from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
import hashlib
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence, TypeVar, runtime_checkable

from .namespace import (
    clean_namespace_path,
    join_namespace_path,
    normalize_namespace_file_path,
    normalize_namespace_root,
    normalize_relative_namespace_path,
    require_namespace_path_inside_prefix,
)

MDS_SHARDS = "mds-shards"
SAFETENSORS = "safetensors"
_T = TypeVar("_T")


@dataclass(frozen=True)
class MdsShardReadOperation:
    shard_path: str
    offset: int = 0
    length: int | None = None
    format: str = MDS_SHARDS

    def __post_init__(self) -> None:
        if not self.shard_path:
            raise ValueError("shard_path is required")
        object.__setattr__(self, "shard_path", _normalize_cxnfs_file_path(self.shard_path, "shard_path"))
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length is not None and self.length < 0:
            raise ValueError("length must be non-negative")
        if self.format != MDS_SHARDS:
            raise ValueError("MDS shard reads must use format=mds-shards")


@dataclass(frozen=True)
class SafetensorsFileEntry:
    path: str
    size: int
    sha256: str
    tensor_file: bool = False

    def __post_init__(self) -> None:
        _validate_relative_file_path(self.path)
        if self.size < 0:
            raise ValueError("file size must be non-negative")
        if len(self.sha256) != 64:
            raise ValueError("sha256 must be a 64-character hex digest")
        try:
            int(self.sha256, 16)
        except ValueError as exc:
            raise ValueError("sha256 must be hexadecimal") from exc


@dataclass(frozen=True)
class SafetensorsSendOperation:
    output_dir: str
    files: tuple[str, ...]
    tensor_files: tuple[str, ...]
    format: str = SAFETENSORS
    file_entries: tuple[SafetensorsFileEntry, ...] = ()
    remote_root: str | None = None

    def __post_init__(self) -> None:
        if not self.output_dir:
            raise ValueError("output_dir is required")
        files = tuple(self.files)
        tensor_files = tuple(self.tensor_files)
        file_entries = tuple(self.file_entries)
        object.__setattr__(self, "files", files)
        object.__setattr__(self, "tensor_files", tensor_files)
        object.__setattr__(self, "file_entries", file_entries)
        if not files:
            raise ValueError("files are required")
        for file_path in files:
            _validate_relative_file_path(file_path)
        if not tensor_files:
            raise ValueError("at least one .safetensors file is required")
        for tensor_file in tensor_files:
            _validate_relative_file_path(tensor_file)
        if not set(tensor_files).issubset(set(files)):
            raise ValueError("tensor_files must be a subset of files")
        if file_entries:
            entry_paths = {entry.path for entry in file_entries}
            if entry_paths != set(files):
                raise ValueError("file_entries must describe every file exactly once")
            tensor_entries = {entry.path for entry in file_entries if entry.tensor_file}
            if tensor_entries != set(tensor_files):
                raise ValueError("file_entries tensor flags must match tensor_files")
        if self.remote_root is not None:
            object.__setattr__(self, "remote_root", _normalize_cxnfs_root(self.remote_root))
        if self.format != SAFETENSORS:
            raise ValueError("checkpoint sends must use format=safetensors")


@dataclass(frozen=True)
class SendResult:
    file_count: int
    tensor_file_count: int
    bytes_sent: int | None = None


class TensorMemoryTarget(str, Enum):
    BYTES = "bytes"
    PYTORCH_CPU = "pytorch-cpu"
    PYTORCH_PINNED_CPU = "pytorch-pinned-cpu"
    PYTORCH_CUDA = "pytorch-cuda"


@dataclass(frozen=True)
class TensorReadOperation:
    shard_path: str
    offset: int
    nbytes: int
    dtype: str
    shape: tuple[int, ...]
    target: TensorMemoryTarget = TensorMemoryTarget.PYTORCH_PINNED_CPU
    device: str = "cpu"
    non_blocking: bool = True
    format: str = MDS_SHARDS

    def __post_init__(self) -> None:
        if not self.shard_path:
            raise ValueError("shard_path is required")
        object.__setattr__(self, "shard_path", _normalize_cxnfs_file_path(self.shard_path, "shard_path"))
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.nbytes <= 0:
            raise ValueError("nbytes must be positive")
        if not self.dtype:
            raise ValueError("dtype is required")
        if not self.shape:
            raise ValueError("shape is required")
        if any(dimension <= 0 for dimension in self.shape):
            raise ValueError("shape dimensions must be positive")
        if self.target == TensorMemoryTarget.PYTORCH_CUDA and not self.device.startswith("cuda"):
            raise ValueError("cuda tensor reads require a cuda device")
        if self.format != MDS_SHARDS:
            raise ValueError("tensor reads must use format=mds-shards")


@dataclass(frozen=True)
class TensorDestination:
    tensor: Any
    target: TensorMemoryTarget
    device: str = "cpu"
    non_blocking: bool = True

    def __post_init__(self) -> None:
        if self.tensor is None:
            raise ValueError("tensor is required")
        if self.target == TensorMemoryTarget.PYTORCH_CUDA and not self.device.startswith("cuda"):
            raise ValueError("cuda tensor destinations require a cuda device")


@dataclass(frozen=True)
class CxnfsBufferRead:
    shard_path: str | Path
    offset: int
    buffer: Any
    nbytes: int | None = None

    def __post_init__(self) -> None:
        if self.buffer is None:
            raise ValueError("buffer is required")
        if self.nbytes is not None and self.nbytes <= 0:
            raise ValueError("nbytes must be positive")


@dataclass(frozen=True)
class CxnfsTensorRead:
    shard_path: str | Path
    offset: int
    tensor: Any
    nbytes: int
    target: TensorMemoryTarget | None = None
    device: str | None = None
    non_blocking: bool = True

    def __post_init__(self) -> None:
        if self.tensor is None:
            raise ValueError("tensor is required")
        if self.nbytes <= 0:
            raise ValueError("nbytes must be positive")


@dataclass(frozen=True)
class ReadIntoTensorOperation:
    shard_path: str
    offset: int
    nbytes: int
    destination: TensorDestination
    format: str = MDS_SHARDS

    def __post_init__(self) -> None:
        if not self.shard_path:
            raise ValueError("shard_path is required")
        object.__setattr__(self, "shard_path", _normalize_cxnfs_file_path(self.shard_path, "shard_path"))
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.nbytes <= 0:
            raise ValueError("nbytes must be positive")
        if self.destination is None:
            raise ValueError("destination is required")
        if self.format != MDS_SHARDS:
            raise ValueError("tensor reads must use format=mds-shards")


@dataclass(frozen=True)
class BufferReadOperation:
    shard_path: str
    offset: int
    nbytes: int
    destination: memoryview
    format: str = MDS_SHARDS

    def __post_init__(self) -> None:
        if not self.shard_path:
            raise ValueError("shard_path is required")
        object.__setattr__(self, "shard_path", _normalize_cxnfs_file_path(self.shard_path, "shard_path"))
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.nbytes <= 0:
            raise ValueError("nbytes must be positive")
        if self.destination is None:
            raise ValueError("destination is required")
        if self.destination.readonly:
            raise ValueError("destination buffer must be writable")
        if self.nbytes > self.destination.nbytes:
            raise ValueError("nbytes exceeds destination buffer size")
        if self.format != MDS_SHARDS:
            raise ValueError("buffer reads must use format=mds-shards")


@dataclass(frozen=True)
class DirectWriteOperation:
    path: str
    source: Any
    nbytes: int
    format: str = SAFETENSORS

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", _normalize_cxnfs_file_path(self.path, "path"))
        if self.source is None:
            raise ValueError("source is required")
        if self.nbytes <= 0:
            raise ValueError("nbytes must be positive")
        if self.format != SAFETENSORS:
            raise ValueError("direct tensor writes must use format=safetensors")


@runtime_checkable
class CxnfsTransport(Protocol):
    def read_mds_shard(self, operation: MdsShardReadOperation) -> bytes:
        """Read bytes through the native CXNFS read path, without a mounted filesystem."""

    def send_safetensors_output(self, operation: SafetensorsSendOperation) -> SendResult:
        """Write checkpoint output through the native CXNFS write path, without a mount."""


@runtime_checkable
class CxnfsTorchTransport(CxnfsTransport, Protocol):
    def read_tensor(self, operation: TensorReadOperation) -> Any:
        """Read directly into a PyTorch-owned destination when the backend supports it."""

    def read_into_tensor(self, operation: ReadIntoTensorOperation) -> int:
        """Read into a caller-allocated PyTorch tensor destination."""

    def read_into_buffer(self, operation: BufferReadOperation) -> int:
        """Read into a caller-owned writable Python buffer."""

    def write_from_tensor(self, operation: DirectWriteOperation) -> SendResult:
        """Write from a tensor/buffer without routing through a mounted filesystem."""


class CxnfsDatasetClient:
    def __init__(self, transport: CxnfsTransport) -> None:
        self._transport = transport

    def read_mds_shard(self, shard_path: str | Path, offset: int = 0, length: int | None = None) -> bytes:
        operation = MdsShardReadOperation(str(shard_path), offset, length)
        return self._transport.read_mds_shard(operation)

    def send_safetensors_output(self, output_dir: str | Path, remote_root: str | Path | None = None) -> SendResult:
        root = Path(output_dir)
        if not root.is_dir():
            raise FileNotFoundError(f"output directory does not exist: {root}")

        paths = tuple(path for path in sorted(root.rglob("*")) if path.is_file())
        files = tuple(path.relative_to(root).as_posix() for path in paths)
        tensor_files = tuple(path for path in files if path.endswith(".safetensors"))
        entries = tuple(
            SafetensorsFileEntry(
                path.relative_to(root).as_posix(),
                path.stat().st_size,
                sha256_file(path),
                path.relative_to(root).as_posix().endswith(".safetensors"),
            )
            for path in paths
        )
        operation = SafetensorsSendOperation(
            str(root),
            files,
            tensor_files,
            file_entries=entries,
            remote_root=None if remote_root is None else str(remote_root),
        )
        return self._transport.send_safetensors_output(operation)


class CxnfsPathPrefixTransport:
    def __init__(self, delegate: CxnfsDatasetClient, path_prefix: str | Path) -> None:
        if delegate is None:
            raise ValueError("delegate is required")
        prefix = _normalize_cxnfs_prefix(str(path_prefix))
        if not prefix:
            raise ValueError("path_prefix is required")
        self.delegate = delegate
        self.path_prefix = prefix

    def read_mds_shard(self, operation: MdsShardReadOperation) -> bytes:
        return self.delegate.read_mds_shard(
            _join_cxnfs_path(self.path_prefix, operation.shard_path),
            operation.offset,
            operation.length,
        )

    def send_safetensors_output(self, operation: SafetensorsSendOperation) -> SendResult:
        remote_root = self.resolve_remote_root(operation.remote_root)
        return self.delegate.send_safetensors_output(
            operation.output_dir,
            remote_root=remote_root,
        )

    def resolve_remote_root(self, remote_root: str | Path | None = None) -> str:
        if remote_root is None:
            return self.path_prefix
        raw = str(remote_root)
        if clean_namespace_path(raw).startswith("/"):
            resolved = _normalize_cxnfs_root(raw)
            _require_cxnfs_path_inside_prefix(self.path_prefix, resolved, "remote_root")
            return resolved
        return _join_cxnfs_path(self.path_prefix, raw)

    def read_tensor(self, operation: TensorReadOperation) -> Any:
        shard_path = _join_cxnfs_path(self.path_prefix, operation.shard_path)
        return _require_delegate_method(self.delegate, "read_tensor")(
            shard_path,
            operation.offset,
            operation.nbytes,
            operation.dtype,
            operation.shape,
            operation.target,
            operation.device,
            operation.non_blocking,
        )

    def read_into_tensor(self, operation: ReadIntoTensorOperation) -> int:
        shard_path = _join_cxnfs_path(self.path_prefix, operation.shard_path)
        return _require_delegate_method(self.delegate, "read_into_tensor")(
            shard_path,
            operation.offset,
            operation.destination.tensor,
            operation.nbytes,
            operation.destination.target,
            operation.destination.device,
            operation.destination.non_blocking,
        )

    def read_into_buffer(self, operation: BufferReadOperation) -> int:
        shard_path = _join_cxnfs_path(self.path_prefix, operation.shard_path)
        return _require_delegate_method(self.delegate, "read_into_buffer")(
            shard_path,
            operation.offset,
            operation.destination,
            operation.nbytes,
        )

    def write_from_tensor(self, operation: DirectWriteOperation) -> SendResult:
        path = _join_cxnfs_path(self.path_prefix, operation.path)
        return _require_delegate_method(self.delegate, "write_tensor")(
            path,
            operation.source,
            operation.nbytes,
        )

    def close(self) -> None:
        close = getattr(self.delegate, "close", None)
        if callable(close):
            close()


class CxnfsPathPrefixClient(CxnfsDatasetClient):
    def __init__(self, delegate: CxnfsDatasetClient, path_prefix: str | Path) -> None:
        self._prefix_transport = CxnfsPathPrefixTransport(delegate, path_prefix)
        super().__init__(self._prefix_transport)

    def close(self) -> None:
        self._prefix_transport.close()

    def send_safetensors_output(self, output_dir: str | Path, remote_root: str | Path | None = None) -> SendResult:
        mapped_root = self._prefix_transport.resolve_remote_root(remote_root)
        return self._prefix_transport.delegate.send_safetensors_output(output_dir, remote_root=mapped_root)

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
        return CxnfsPythonClient.read_tensor(
            self,
            shard_path,
            offset,
            nbytes,
            dtype,
            shape,
            target,
            device,
            non_blocking,
        )

    def read_cuda_tensor(
        self,
        shard_path: str | Path,
        offset: int,
        nbytes: int,
        dtype: str,
        shape: tuple[int, ...],
        device: str = "cuda:0",
    ) -> Any:
        return CxnfsPythonClient.read_cuda_tensor(
            self,
            shard_path,
            offset,
            nbytes,
            dtype,
            shape,
            device,
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
        return CxnfsPythonClient.read_into_tensor(
            self,
            shard_path,
            offset,
            tensor,
            nbytes,
            target,
            device,
            non_blocking,
        )

    def read_into_buffer(
        self,
        shard_path: str | Path,
        offset: int,
        buffer: Any,
        nbytes: int | None = None,
    ) -> int:
        return CxnfsPythonClient.read_into_buffer(self, shard_path, offset, buffer, nbytes)

    def read_into_buffers(
        self,
        reads: Sequence[CxnfsBufferRead],
        *,
        max_workers: int | None = None,
    ) -> tuple[int, ...]:
        return _run_ordered_direct_reads(
            tuple(reads),
            lambda read: self.read_into_buffer(read.shard_path, read.offset, read.buffer, read.nbytes),
            max_workers=max_workers,
        )

    def read_into_tensors(
        self,
        reads: Sequence[CxnfsTensorRead],
        *,
        max_workers: int | None = None,
    ) -> tuple[int, ...]:
        return _run_ordered_direct_reads(
            tuple(reads),
            lambda read: self.read_into_tensor(
                read.shard_path,
                read.offset,
                read.tensor,
                read.nbytes,
                read.target,
                read.device,
                read.non_blocking,
            ),
            max_workers=max_workers,
        )

    def write_tensor(self, path: str | Path, tensor: Any, nbytes: int) -> SendResult:
        return CxnfsPythonClient.write_tensor(self, path, tensor, nbytes)

    def close(self) -> None:
        self._prefix_transport.close()


class CxnfsPythonClient(CxnfsDatasetClient):
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
        if not isinstance(self._transport, CxnfsTorchTransport):
            raise TypeError("transport does not implement CxnfsTorchTransport")
        operation = TensorReadOperation(
            str(shard_path),
            offset,
            nbytes,
            dtype,
            tuple(shape),
            target,
            device,
            non_blocking,
        )
        return self._transport.read_tensor(operation)

    def read_cuda_tensor(
        self,
        shard_path: str | Path,
        offset: int,
        nbytes: int,
        dtype: str,
        shape: tuple[int, ...],
        device: str = "cuda:0",
    ) -> Any:
        return self.read_tensor(
            shard_path,
            offset,
            nbytes,
            dtype,
            shape,
            TensorMemoryTarget.PYTORCH_CUDA,
            device,
            True,
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
        if not isinstance(self._transport, CxnfsTorchTransport):
            raise TypeError("transport does not implement CxnfsTorchTransport")
        destination = TensorDestination(
            tensor,
            target or infer_tensor_target(tensor),
            device or infer_tensor_device(tensor),
            non_blocking,
        )
        operation = ReadIntoTensorOperation(str(shard_path), offset, nbytes, destination)
        return self._transport.read_into_tensor(operation)

    def read_into_buffer(
        self,
        shard_path: str | Path,
        offset: int,
        buffer: Any,
        nbytes: int | None = None,
    ) -> int:
        if not isinstance(self._transport, CxnfsTorchTransport):
            raise TypeError("transport does not implement CxnfsTorchTransport")
        destination = memoryview(buffer)
        operation = BufferReadOperation(str(shard_path), offset, nbytes or destination.nbytes, destination)
        return self._transport.read_into_buffer(operation)

    def read_into_buffers(
        self,
        reads: Sequence[CxnfsBufferRead],
        *,
        max_workers: int | None = None,
    ) -> tuple[int, ...]:
        return _run_ordered_direct_reads(
            tuple(reads),
            lambda read: self.read_into_buffer(read.shard_path, read.offset, read.buffer, read.nbytes),
            max_workers=max_workers,
        )

    def read_into_tensors(
        self,
        reads: Sequence[CxnfsTensorRead],
        *,
        max_workers: int | None = None,
    ) -> tuple[int, ...]:
        return _run_ordered_direct_reads(
            tuple(reads),
            lambda read: self.read_into_tensor(
                read.shard_path,
                read.offset,
                read.tensor,
                read.nbytes,
                read.target,
                read.device,
                read.non_blocking,
            ),
            max_workers=max_workers,
        )

    def write_tensor(self, path: str | Path, tensor: Any, nbytes: int) -> SendResult:
        if not isinstance(self._transport, CxnfsTorchTransport):
            raise TypeError("transport does not implement CxnfsTorchTransport")
        return self._transport.write_from_tensor(DirectWriteOperation(str(path), tensor, nbytes))


def _run_ordered_direct_reads(
    reads: tuple[_T, ...],
    worker: Callable[[_T], int],
    *,
    max_workers: int | None,
) -> tuple[int, ...]:
    if not reads:
        return ()
    workers = len(reads) if max_workers is None else max_workers
    if workers <= 0:
        raise ValueError("max_workers must be positive")
    workers = min(workers, len(reads))
    if workers == 1:
        return tuple(worker(read) for read in reads)
    results = [0] * len(reads)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(worker, read): index
            for index, read in enumerate(reads)
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return tuple(results)


def infer_tensor_target(tensor: Any) -> TensorMemoryTarget:
    if bool(getattr(tensor, "is_cuda", False)):
        return TensorMemoryTarget.PYTORCH_CUDA
    is_pinned = getattr(tensor, "is_pinned", None)
    if callable(is_pinned) and bool(is_pinned()):
        return TensorMemoryTarget.PYTORCH_PINNED_CPU
    return TensorMemoryTarget.PYTORCH_CPU


def infer_tensor_device(tensor: Any) -> str:
    device = getattr(tensor, "device", None)
    return "cpu" if device is None else str(device)


def _normalize_cxnfs_prefix(path: str) -> str:
    normalized = clean_namespace_path(path)
    if not normalized:
        raise ValueError("path_prefix is required")
    if not normalized.startswith("/"):
        raise ValueError("path_prefix must be absolute")
    return normalize_namespace_root(normalized, label="path_prefix")


def _normalize_cxnfs_root(path: str) -> str:
    return normalize_namespace_root(path, label="remote_root")


def _normalize_cxnfs_file_path(path: str, label: str) -> str:
    raw = str(path).replace("\\", "/").strip()
    if raw.endswith("/"):
        raise ValueError(f"{label} must identify a file")
    cleaned = clean_namespace_path(raw)
    if cleaned.startswith("/"):
        return normalize_namespace_file_path(cleaned, label=label)
    return normalize_relative_namespace_path(cleaned, label=label)


def _join_cxnfs_path(prefix: str, shard_path: str) -> str:
    cleaned = clean_namespace_path(shard_path)
    if cleaned.startswith("/"):
        normalized = normalize_namespace_file_path(cleaned, label="prefixed path")
        try:
            _require_cxnfs_path_inside_prefix(prefix, normalized, "prefixed path")
        except ValueError as exc:
            raise ValueError("prefixed path must be relative or stay inside path_prefix") from exc
        return normalized
    return join_namespace_path(prefix, shard_path, label="prefixed path")


def _require_cxnfs_path_inside_prefix(prefix: str, path: str, label: str) -> None:
    require_namespace_path_inside_prefix(prefix, path, label=label)


def _normalize_relative_cxnfs_path(path: str, label: str) -> str:
    return normalize_relative_namespace_path(path, label=label)


def _require_delegate_method(delegate: Any, method_name: str):
    method = getattr(delegate, method_name, None)
    if not callable(method):
        raise TypeError("delegate does not implement CxnfsTorchTransport")
    return method


def _validate_relative_file_path(path: str) -> None:
    if path is None or not str(path).strip():
        raise ValueError("file path is required")
    normalized = str(path).replace("\\", "/")
    candidate = Path(normalized)
    if candidate.is_absolute() or normalized.startswith("/"):
        raise ValueError("file paths must be relative")
    if any(part in ("", ".", "..") for part in normalized.split("/")):
        raise ValueError("file paths must not contain empty, current, or parent segments")


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
