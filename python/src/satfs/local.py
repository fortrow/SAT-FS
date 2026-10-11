from __future__ import annotations

from pathlib import Path
from typing import Any

from .dataset import (
    BufferReadOperation,
    DirectWriteOperation,
    MdsShardReadOperation,
    ReadIntoTensorOperation,
    SafetensorsSendOperation,
    SendResult,
    TensorDestination,
    TensorMemoryTarget,
    TensorReadOperation,
    sha256_file,
)
from .tensor_memory import (
    copy_staging_to_tensor,
    readable_source_memoryview,
    require_tensor_read_nbytes_match,
    require_torch,
    tensor_shape,
    tensor_writable_memoryview,
    torch_dtype,
    torch_empty,
    try_memoryview,
)


class LocalFileTransport:
    """Filesystem-backed CXNFS transport for tests and local preprocessing.

    This backend exercises the Python client contract without using a mount.
    It is not the final QUIC backend; CUDA destinations use pinned CPU staging.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def read_mds_shard(self, operation: MdsShardReadOperation) -> bytes:
        path = self._resolve(operation.shard_path)
        with path.open("rb") as file:
            file.seek(operation.offset)
            if operation.length is None:
                return file.read()
            data = file.read(operation.length)
        if len(data) != operation.length:
            raise ValueError("short local MDS shard read")
        return data

    def send_safetensors_output(self, operation: SafetensorsSendOperation) -> SendResult:
        root = Path(operation.output_dir)
        total = 0
        entries = {entry.path: entry for entry in operation.file_entries}
        for relative in operation.files:
            path = root / relative
            size = path.stat().st_size
            total += size
            entry = entries.get(relative)
            if entry is not None:
                if entry.size != size:
                    raise ValueError(f"checkpoint file size changed before send: {relative}")
                if entry.sha256 != sha256_file(path):
                    raise ValueError(f"checkpoint file hash changed before send: {relative}")
        return SendResult(
            file_count=len(operation.files),
            tensor_file_count=len(operation.tensor_files),
            bytes_sent=total,
        )

    def read_tensor(self, operation: TensorReadOperation) -> Any:
        if operation.target == TensorMemoryTarget.BYTES:
            return self.read_mds_shard(MdsShardReadOperation(operation.shard_path, operation.offset, operation.nbytes))

        torch = require_torch()
        dtype = torch_dtype(torch, operation.dtype)
        require_tensor_read_nbytes_match(operation.nbytes, dtype, operation.shape)
        if operation.target == TensorMemoryTarget.PYTORCH_CUDA:
            staging = torch_empty(
                torch,
                operation.shape,
                dtype=dtype,
                device="cpu",
                pin_memory=True,
            )
            self.read_into_tensor(ReadIntoTensorOperation(
                operation.shard_path,
                operation.offset,
                operation.nbytes,
                destination=TensorDestination(
                    staging,
                    TensorMemoryTarget.PYTORCH_PINNED_CPU,
                    "cpu",
                    operation.non_blocking,
                ),
                format=operation.format,
            ))
            return staging.to(operation.device, non_blocking=operation.non_blocking)

        tensor = torch_empty(
            torch,
            operation.shape,
            dtype=dtype,
            device="cpu",
            pin_memory=operation.target == TensorMemoryTarget.PYTORCH_PINNED_CPU,
        )
        self.read_into_tensor(ReadIntoTensorOperation(
            operation.shard_path,
            operation.offset,
            operation.nbytes,
            destination=TensorDestination(
                tensor,
                operation.target,
                "cpu",
                operation.non_blocking,
            ),
            format=operation.format,
        ))
        return tensor

    def read_into_tensor(self, operation: ReadIntoTensorOperation) -> int:
        if operation.destination.target == TensorMemoryTarget.PYTORCH_CUDA:
            tensor = operation.destination.tensor
            shape = tensor_shape(tensor)
            dtype = getattr(tensor, "dtype", None)
            if dtype is None:
                raise TypeError("cuda tensor destination must expose dtype")
            require_tensor_read_nbytes_match(operation.nbytes, dtype, shape)
            torch = require_torch()
            staging = torch_empty(
                torch,
                shape,
                dtype=dtype,
                device="cpu",
                pin_memory=True,
            )
            bytes_read = self.read_into_buffer(BufferReadOperation(
                operation.shard_path,
                operation.offset,
                operation.nbytes,
                tensor_writable_memoryview(staging),
                operation.format,
            ))
            copy_staging_to_tensor(
                staging,
                tensor,
                non_blocking=operation.destination.non_blocking,
            )
            return bytes_read
        writable = tensor_writable_memoryview(operation.destination.tensor)
        return self.read_into_buffer(BufferReadOperation(
            operation.shard_path,
            operation.offset,
            operation.nbytes,
            writable,
            operation.format,
        ))

    def read_into_buffer(self, operation: BufferReadOperation) -> int:
        path = self._resolve(operation.shard_path)
        with path.open("rb") as file:
            file.seek(operation.offset)
            data = file.read(operation.nbytes)
        if len(data) != operation.nbytes:
            raise ValueError("short local MDS shard read")
        operation.destination[:len(data)] = data
        return len(data)

    def write_from_tensor(self, operation: DirectWriteOperation) -> SendResult:
        path = self._resolve(operation.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        writable = readable_source_memoryview(operation.source)
        if operation.nbytes > writable.nbytes:
            raise ValueError("nbytes exceeds readable source size")
        with path.open("wb") as file:
            file.write(writable[:operation.nbytes])
        return SendResult(file_count=1, tensor_file_count=1, bytes_sent=operation.nbytes)

    def _resolve(self, path: str | Path) -> Path:
        candidate = Path(path)
        if candidate.is_absolute():
            relative = Path(str(candidate).lstrip("/"))
        else:
            relative = candidate
        resolved = (self.root / relative).resolve()
        root = self.root.resolve()
        if resolved != root and root not in resolved.parents:
            raise ValueError(f"path escapes transport root: {path}")
        return resolved
