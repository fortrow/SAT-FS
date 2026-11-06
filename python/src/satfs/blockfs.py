"""Linux CXBFS control-device client.

The current module path remains ``cxnfs.blockfs`` for compatibility while the
public project vocabulary moves to CXBFS for the Linux-only filesystem.
"""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import errno
import fcntl
import hashlib
import os
from pathlib import Path
import struct
import sys
from typing import Any, BinaryIO, Callable


BLOCK_SIZE = 4096
DEFAULT_STAGING_CHUNK_SIZE = 1024 * 1024
DEFAULT_DEVICE = (
    "/dev/satfs_blockfs_ctl"
    if os.path.exists("/dev/satfs_blockfs_ctl")
    else "/dev/cxnfs_blockfs_ctl"
)

ABI_VERSION = 1
IOC_MAGIC = ord("C")


EXTENT_VERIFIED = 1 << 0
EXTENT_CAN_READ_EARLY = 1 << 1
EXTENT_PUBLISH_NEEDS_TRAILING = 1 << 2
DEFAULT_EXTENT_FLAGS = (
    EXTENT_VERIFIED | EXTENT_CAN_READ_EARLY | EXTENT_PUBLISH_NEEDS_TRAILING
)
MISS_EVENT_NONBLOCK = 1 << 0
MOUNT_VERIFY_HASHES = 1 << 0

MISS_REASON_READ = 1
MISS_REASON_READAHEAD = 2
MISS_REASON_PAYLOAD_IO = 3
MISS_REASON_HASH_MISMATCH = 4
MISS_REASON_NAMES = {
    MISS_REASON_READ: "read",
    MISS_REASON_READAHEAD: "readahead",
    MISS_REASON_PAYLOAD_IO: "payload-io",
    MISS_REASON_HASH_MISMATCH: "hash-mismatch",
}

EXTENT_STRUCT = struct.Struct("<IIQQQII32s")
INVALIDATE_STRUCT = struct.Struct("<IIQQQ")
STATS_STRUCT = struct.Struct("<IIQQQQQ")
STATS_V2_STRUCT = struct.Struct("<IIQQQQQQQQQQQQQQQQQQ")
FILE_STRUCT = struct.Struct("<IIQQ256s")
MISS_EVENT_STRUCT = struct.Struct("<IIQQQQIIQQIIII256s")
PATH_MAX = 256

IOC_NRBITS = 8
IOC_TYPEBITS = 8
IOC_SIZEBITS = 14
IOC_NRSHIFT = 0
IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS
IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS
IOC_DIRSHIFT = IOC_SIZESHIFT + IOC_SIZEBITS
IOC_WRITE = 1
IOC_READ = 2


def _ioc(direction: int, request_number: int, size: int) -> int:
    return (
        (direction << IOC_DIRSHIFT)
        | (IOC_MAGIC << IOC_TYPESHIFT)
        | (request_number << IOC_NRSHIFT)
        | (size << IOC_SIZESHIFT)
    )


IOCTL_REGISTER_EXTENT = _ioc(IOC_WRITE, 1, EXTENT_STRUCT.size)
IOCTL_INVALIDATE = _ioc(IOC_WRITE, 2, INVALIDATE_STRUCT.size)
IOCTL_STATS = _ioc(IOC_READ, 3, STATS_STRUCT.size)
IOCTL_REGISTER_FILE = _ioc(IOC_WRITE, 4, FILE_STRUCT.size)
IOCTL_NEXT_MISS_EVENT = _ioc(IOC_READ | IOC_WRITE, 6, MISS_EVENT_STRUCT.size)
IOCTL_STATS_V2 = _ioc(IOC_READ, 7, STATS_V2_STRUCT.size)


@dataclass(frozen=True)
class BlockfsExtent:
    inode_number: int
    file_offset: int
    payload_block: int
    payload_offset: int
    payload_length: int
    expected_hash: bytes
    flags: int = DEFAULT_EXTENT_FLAGS

    def __post_init__(self) -> None:
        if self.inode_number <= 0:
            raise ValueError("inode_number must be positive")
        if self.file_offset < 0:
            raise ValueError("file_offset must be non-negative")
        if self.payload_block < 0:
            raise ValueError("payload_block must be non-negative")
        if self.payload_offset < 0:
            raise ValueError("payload_offset must be non-negative")
        if self.payload_length <= 0:
            raise ValueError("payload_length must be positive")
        if len(self.expected_hash) != 32:
            raise ValueError("expected_hash must be 32 bytes")
        if not self.flags & EXTENT_VERIFIED:
            raise ValueError("verified extent flag is required")

    def pack(self) -> bytes:
        return EXTENT_STRUCT.pack(
            ABI_VERSION,
            self.flags,
            self.inode_number,
            self.file_offset,
            self.payload_block,
            self.payload_offset,
            self.payload_length,
            self.expected_hash,
        )


@dataclass(frozen=True)
class BlockfsInvalidation:
    inode_number: int
    offset: int
    length: int
    flags: int = 0

    def __post_init__(self) -> None:
        if self.inode_number <= 0:
            raise ValueError("inode_number must be positive")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length < 0:
            raise ValueError("length must be non-negative")

    def pack(self) -> bytes:
        return INVALIDATE_STRUCT.pack(
            ABI_VERSION,
            self.flags,
            self.inode_number,
            self.offset,
            self.length,
        )


@dataclass(frozen=True)
class BlockfsFile:
    inode_number: int
    path: str
    size: int
    mode: int = 0o444

    def __post_init__(self) -> None:
        if self.inode_number <= 0:
            raise ValueError("inode_number must be positive")
        if not self.path:
            raise ValueError("path must be a relative file path")
        if self.path.startswith("/") or self.path.endswith("/") or "\\" in self.path:
            raise ValueError("path must be a relative file path")
        for component in self.path.split("/"):
            if component in ("", ".", ".."):
                raise ValueError("path must not contain empty, dot, or parent components")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.path):
            raise ValueError("path must not contain control characters")
        encoded = self.path.encode("utf-8")
        if len(encoded) >= PATH_MAX:
            raise ValueError("path is too long")
        if self.size < 0:
            raise ValueError("size must be non-negative")
        if self.mode < 0 or self.mode > 0o777:
            raise ValueError("mode must fit in permission bits")

    def pack(self) -> bytes:
        encoded = self.path.encode("utf-8")
        return FILE_STRUCT.pack(
            ABI_VERSION,
            self.mode,
            self.inode_number,
            self.size,
            encoded + b"\0" * (PATH_MAX - len(encoded)),
        )


@dataclass(frozen=True)
class BlockfsNamespaceSnapshot:
    generation: int
    files: tuple[BlockfsFile, ...] = ()

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("generation must be non-negative")
        object.__setattr__(self, "files", tuple(self.files))
        self._validate_files()

    def _validate_files(self) -> None:
        inodes: set[int] = set()
        paths: set[str] = set()
        for file in self.files:
            if file.inode_number in inodes:
                raise ValueError(f"duplicate namespace inode: {file.inode_number}")
            if file.path in paths:
                raise ValueError(f"duplicate namespace path: {file.path}")
            inodes.add(file.inode_number)
            paths.add(file.path)
        for file in self.files:
            prefix = file.path + "/"
            if any(path.startswith(prefix) for path in paths):
                raise ValueError(
                    f"file path conflicts with implicit directory: {file.path}"
                )


@dataclass(frozen=True)
class BlockfsExtentHintsSnapshot:
    generation: int
    extents: tuple[BlockfsExtent, ...] = ()

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("generation must be non-negative")
        object.__setattr__(self, "extents", tuple(self.extents))
        self._validate_extents()

    def _validate_extents(self) -> None:
        ordered = sorted(self.extents, key=lambda extent: (extent.inode_number, extent.file_offset))
        previous: BlockfsExtent | None = None
        for current in ordered:
            if previous and previous.inode_number == current.inode_number:
                previous_end = previous.file_offset + previous.payload_length
                if current.file_offset < previous_end:
                    raise ValueError(
                        f"overlapping extent hints for inode: {current.inode_number}"
                    )
            previous = current


@dataclass(frozen=True)
class BlockfsPayloadAllocation:
    inode_number: int
    file_offset: int
    payload_block: int
    payload_offset: int
    payload_length: int

    def __post_init__(self) -> None:
        if self.inode_number <= 0:
            raise ValueError("inode_number must be positive")
        if self.file_offset < 0:
            raise ValueError("file_offset must be non-negative")
        if self.payload_block < 0:
            raise ValueError("payload_block must be non-negative")
        if self.payload_offset < 0:
            raise ValueError("payload_offset must be non-negative")
        if self.payload_length <= 0:
            raise ValueError("payload_length must be positive")


@dataclass(frozen=True)
class BlockfsPayloadAllocationSnapshot:
    generation: int
    allocations: tuple[BlockfsPayloadAllocation, ...] = ()

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("generation must be non-negative")
        object.__setattr__(self, "allocations", tuple(self.allocations))
        ordered = sorted(
            self.allocations,
            key=lambda allocation: (allocation.inode_number, allocation.file_offset),
        )
        previous: BlockfsPayloadAllocation | None = None
        for current in ordered:
            if previous and previous.inode_number == current.inode_number:
                previous_end = previous.file_offset + previous.payload_length
                if current.file_offset < previous_end:
                    raise ValueError(
                        f"overlapping payload allocations for inode: {current.inode_number}"
                    )
            previous = current


class BlockfsNamespaceStore:
    relative_path = Path("namespace/current.cxn")
    sat_relative_path = Path("namespace/current.sat")

    def snapshot_path(self, metadata_root: str | os.PathLike[str]) -> Path:
        return Path(metadata_root) / self.relative_path

    def load(self, metadata_root: str | os.PathLike[str]) -> BlockfsNamespaceSnapshot:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return self.load_file(sat_path)

    def load_file(self, path: str | os.PathLike[str]) -> BlockfsNamespaceSnapshot:
        values = self._read_properties(Path(path))
        version = self._int(values, "version")
        if version != 1:
            raise ValueError(f"unsupported namespace snapshot version: {version}")
        generation = self._int(values, "generation")
        count = self._int(values, "file.count")
        files: list[BlockfsFile] = []
        for index in range(count):
            prefix = f"file.{index}."
            files.append(
                BlockfsFile(
                    inode_number=self._int(values, prefix + "inode"),
                    path=self._required(values, prefix + "path"),
                    size=self._int(values, prefix + "size"),
                    mode=int(self._required(values, prefix + "mode"), 8),
                )
            )
        return BlockfsNamespaceSnapshot(generation, tuple(files))

    def write(
        self,
        metadata_root: str | os.PathLike[str],
        snapshot: BlockfsNamespaceSnapshot,
    ) -> Path:
        primary = self.snapshot_path(metadata_root)
        primary.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(primary, snapshot)
        sat_path = Path(metadata_root) / self.sat_relative_path
        sat_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(sat_path, snapshot)
        return primary

    def write_file(
        self,
        path: str | os.PathLike[str],
        snapshot: BlockfsNamespaceSnapshot,
    ) -> None:
        path = Path(path)
        lines = [
            "version=1",
            f"generation={snapshot.generation}",
            f"file.count={len(snapshot.files)}",
        ]
        for index, file in enumerate(snapshot.files):
            prefix = f"file.{index}."
            lines.extend(
                [
                    f"{prefix}inode={file.inode_number}",
                    f"{prefix}path={file.path}",
                    f"{prefix}size={file.size}",
                    f"{prefix}mode=0{file.mode:o}",
                ]
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    @staticmethod
    def _read_properties(path: Path) -> dict[str, str]:
        values: dict[str, str] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("!"):
                continue
            separator = line.find("=")
            if separator < 0:
                separator = line.find(":")
            if separator <= 0:
                continue
            values[line[:separator].strip()] = line[separator + 1 :].strip()
        return values

    @staticmethod
    def _required(values: dict[str, str], key: str) -> str:
        value = values.get(key)
        if value is None or value == "":
            raise ValueError(f"missing namespace field: {key}")
        return value

    @classmethod
    def _int(cls, values: dict[str, str], key: str) -> int:
        return int(cls._required(values, key), 10)


class BlockfsExtentHintStore:
    relative_path = Path("extent-hints/current.cxn")
    sat_relative_path = Path("extent-hints/current.sat")

    def snapshot_path(self, metadata_root: str | os.PathLike[str]) -> Path:
        return Path(metadata_root) / self.relative_path

    def load(
        self,
        metadata_root: str | os.PathLike[str],
    ) -> BlockfsExtentHintsSnapshot:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return self.load_file(sat_path)

    def load_if_exists(
        self,
        metadata_root: str | os.PathLike[str],
    ) -> BlockfsExtentHintsSnapshot | None:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return None

    def load_file(
        self,
        path: str | os.PathLike[str],
    ) -> BlockfsExtentHintsSnapshot:
        values = BlockfsNamespaceStore._read_properties(Path(path))
        version = BlockfsNamespaceStore._int(values, "version")
        if version != 1:
            raise ValueError(f"unsupported extent hint snapshot version: {version}")
        generation = BlockfsNamespaceStore._int(values, "generation")
        count = BlockfsNamespaceStore._int(values, "extent.count")
        extents: list[BlockfsExtent] = []
        for index in range(count):
            prefix = f"extent.{index}."
            expected_hash = bytes.fromhex(
                BlockfsNamespaceStore._required(values, prefix + "hash")
            )
            extents.append(
                BlockfsExtent(
                    inode_number=BlockfsNamespaceStore._int(values, prefix + "inode"),
                    file_offset=BlockfsNamespaceStore._int(values, prefix + "file-offset"),
                    payload_block=BlockfsNamespaceStore._int(values, prefix + "payload-block"),
                    payload_offset=BlockfsNamespaceStore._int(values, prefix + "payload-offset"),
                    payload_length=BlockfsNamespaceStore._int(values, prefix + "payload-length"),
                    expected_hash=expected_hash,
                )
            )
        return BlockfsExtentHintsSnapshot(generation, tuple(extents))

    def write(
        self,
        metadata_root: str | os.PathLike[str],
        snapshot: BlockfsExtentHintsSnapshot,
    ) -> Path:
        primary = self.snapshot_path(metadata_root)
        primary.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(primary, snapshot)
        sat_path = Path(metadata_root) / self.sat_relative_path
        sat_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(sat_path, snapshot)
        return primary

    def write_file(
        self,
        path: str | os.PathLike[str],
        snapshot: BlockfsExtentHintsSnapshot,
    ) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "version=1",
            f"generation={snapshot.generation}",
            f"extent.count={len(snapshot.extents)}",
        ]
        for index, extent in enumerate(snapshot.extents):
            prefix = f"extent.{index}."
            lines.extend(
                [
                    f"{prefix}inode={extent.inode_number}",
                    f"{prefix}file-offset={extent.file_offset}",
                    f"{prefix}payload-block={extent.payload_block}",
                    f"{prefix}payload-offset={extent.payload_offset}",
                    f"{prefix}payload-length={extent.payload_length}",
                    f"{prefix}hash={extent.expected_hash.hex()}",
                ]
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class BlockfsPayloadAllocationStore:
    relative_path = Path("payload-map/current.cxn")
    sat_relative_path = Path("payload-map/current.sat")

    def snapshot_path(self, metadata_root: str | os.PathLike[str]) -> Path:
        return Path(metadata_root) / self.relative_path

    def load(
        self,
        metadata_root: str | os.PathLike[str],
    ) -> BlockfsPayloadAllocationSnapshot:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return self.load_file(sat_path)

    def load_if_exists(
        self,
        metadata_root: str | os.PathLike[str],
    ) -> BlockfsPayloadAllocationSnapshot | None:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return None

    def load_file(
        self,
        path: str | os.PathLike[str],
    ) -> BlockfsPayloadAllocationSnapshot:
        values = BlockfsNamespaceStore._read_properties(Path(path))
        version = BlockfsNamespaceStore._int(values, "version")
        if version != 1:
            raise ValueError(f"unsupported payload allocation version: {version}")
        generation = BlockfsNamespaceStore._int(values, "generation")
        count = BlockfsNamespaceStore._int(values, "allocation.count")
        allocations: list[BlockfsPayloadAllocation] = []
        for index in range(count):
            prefix = f"allocation.{index}."
            allocations.append(
                BlockfsPayloadAllocation(
                    inode_number=BlockfsNamespaceStore._int(values, prefix + "inode"),
                    file_offset=BlockfsNamespaceStore._int(values, prefix + "file-offset"),
                    payload_block=BlockfsNamespaceStore._int(values, prefix + "payload-block"),
                    payload_offset=BlockfsNamespaceStore._int(values, prefix + "payload-offset"),
                    payload_length=BlockfsNamespaceStore._int(values, prefix + "payload-length"),
                )
            )
        return BlockfsPayloadAllocationSnapshot(generation, tuple(allocations))

    def write(
        self,
        metadata_root: str | os.PathLike[str],
        snapshot: BlockfsPayloadAllocationSnapshot,
    ) -> Path:
        primary = self.snapshot_path(metadata_root)
        primary.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(primary, snapshot)
        sat_path = Path(metadata_root) / self.sat_relative_path
        sat_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(sat_path, snapshot)
        return primary

    def write_file(
        self,
        path: str | os.PathLike[str],
        snapshot: BlockfsPayloadAllocationSnapshot,
    ) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "version=1",
            f"generation={snapshot.generation}",
            f"allocation.count={len(snapshot.allocations)}",
        ]
        for index, allocation in enumerate(snapshot.allocations):
            prefix = f"allocation.{index}."
            lines.extend(
                [
                    f"{prefix}inode={allocation.inode_number}",
                    f"{prefix}file-offset={allocation.file_offset}",
                    f"{prefix}payload-block={allocation.payload_block}",
                    f"{prefix}payload-offset={allocation.payload_offset}",
                    f"{prefix}payload-length={allocation.payload_length}",
                ]
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class BlockfsManifestChunk:
    file_offset: int
    length: int
    chunk_hash: bytes

    def __post_init__(self) -> None:
        if self.file_offset < 0:
            raise ValueError("file_offset must be non-negative")
        if self.length <= 0:
            raise ValueError("length must be positive")
        if len(self.chunk_hash) != 32:
            raise ValueError("chunk_hash must be 32 bytes")


@dataclass(frozen=True)
class BlockfsManifestEntry:
    inode_number: int
    path: str
    size: int
    file_hash: bytes
    chunks: tuple[BlockfsManifestChunk, ...] = ()

    def __post_init__(self) -> None:
        BlockfsFile(self.inode_number, self.path, self.size)
        if len(self.file_hash) != 32:
            raise ValueError("file_hash must be 32 bytes")
        object.__setattr__(self, "chunks", tuple(self.chunks))
        expected_offset = 0
        for chunk in self.chunks:
            if chunk.file_offset != expected_offset:
                raise ValueError("manifest chunks must be contiguous")
            expected_offset += chunk.length
        if expected_offset != self.size:
            raise ValueError("manifest chunk lengths must equal file size")


@dataclass(frozen=True)
class BlockfsManifestIndexSnapshot:
    generation: int
    entries: tuple[BlockfsManifestEntry, ...] = ()

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("generation must be non-negative")
        object.__setattr__(self, "entries", tuple(self.entries))
        inodes: set[int] = set()
        paths: set[str] = set()
        for entry in self.entries:
            if entry.inode_number in inodes:
                raise ValueError(f"duplicate manifest inode: {entry.inode_number}")
            if entry.path in paths:
                raise ValueError(f"duplicate manifest path: {entry.path}")
            inodes.add(entry.inode_number)
            paths.add(entry.path)

    @property
    def chunk_count(self) -> int:
        return sum(len(entry.chunks) for entry in self.entries)


class BlockfsManifestIndexStore:
    relative_path = Path("manifest-index/current.cxn")
    sat_relative_path = Path("manifest-index/current.sat")

    def snapshot_path(self, metadata_root: str | os.PathLike[str]) -> Path:
        return Path(metadata_root) / self.relative_path

    def load(
        self,
        metadata_root: str | os.PathLike[str],
    ) -> BlockfsManifestIndexSnapshot:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return self.load_file(sat_path)

    def load_if_exists(
        self,
        metadata_root: str | os.PathLike[str],
    ) -> BlockfsManifestIndexSnapshot | None:
        sat_path = Path(metadata_root) / self.sat_relative_path
        if sat_path.exists():
            return self.load_file(sat_path)
        primary = self.snapshot_path(metadata_root)
        if primary.exists():
            return self.load_file(primary)
        return None

    def load_file(
        self,
        path: str | os.PathLike[str],
    ) -> BlockfsManifestIndexSnapshot:
        values = BlockfsNamespaceStore._read_properties(Path(path))
        version = BlockfsNamespaceStore._int(values, "version")
        if version != 1:
            raise ValueError(f"unsupported manifest index version: {version}")
        generation = BlockfsNamespaceStore._int(values, "generation")
        file_count = BlockfsNamespaceStore._int(values, "file.count")
        entries: list[BlockfsManifestEntry] = []
        for file_index in range(file_count):
            prefix = f"file.{file_index}."
            chunk_count = BlockfsNamespaceStore._int(values, prefix + "chunk.count")
            chunks: list[BlockfsManifestChunk] = []
            for chunk_index in range(chunk_count):
                chunk_prefix = f"{prefix}chunk.{chunk_index}."
                chunks.append(
                    BlockfsManifestChunk(
                        file_offset=BlockfsNamespaceStore._int(
                            values, chunk_prefix + "file-offset"
                        ),
                        length=BlockfsNamespaceStore._int(
                            values, chunk_prefix + "length"
                        ),
                        chunk_hash=bytes.fromhex(
                            BlockfsNamespaceStore._required(
                                values, chunk_prefix + "hash"
                            )
                        ),
                    )
                )
            entries.append(
                BlockfsManifestEntry(
                    inode_number=BlockfsNamespaceStore._int(values, prefix + "inode"),
                    path=BlockfsNamespaceStore._required(values, prefix + "path"),
                    size=BlockfsNamespaceStore._int(values, prefix + "size"),
                    file_hash=bytes.fromhex(
                        BlockfsNamespaceStore._required(values, prefix + "file-hash")
                    ),
                    chunks=tuple(chunks),
                )
            )
        return BlockfsManifestIndexSnapshot(generation, tuple(entries))

    def write(
        self,
        metadata_root: str | os.PathLike[str],
        snapshot: BlockfsManifestIndexSnapshot,
    ) -> Path:
        primary = self.snapshot_path(metadata_root)
        primary.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(primary, snapshot)
        sat_path = Path(metadata_root) / self.sat_relative_path
        sat_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_file(sat_path, snapshot)
        return primary

    def write_file(
        self,
        path: str | os.PathLike[str],
        snapshot: BlockfsManifestIndexSnapshot,
    ) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "version=1",
            f"generation={snapshot.generation}",
            f"file.count={len(snapshot.entries)}",
        ]
        for file_index, entry in enumerate(snapshot.entries):
            prefix = f"file.{file_index}."
            lines.extend(
                [
                    f"{prefix}inode={entry.inode_number}",
                    f"{prefix}path={entry.path}",
                    f"{prefix}size={entry.size}",
                    f"{prefix}file-hash={entry.file_hash.hex()}",
                    f"{prefix}chunk.count={len(entry.chunks)}",
                ]
            )
            for chunk_index, chunk in enumerate(entry.chunks):
                chunk_prefix = f"{prefix}chunk.{chunk_index}."
                lines.extend(
                    [
                        f"{chunk_prefix}file-offset={chunk.file_offset}",
                        f"{chunk_prefix}length={chunk.length}",
                        f"{chunk_prefix}hash={chunk.chunk_hash.hex()}",
                    ]
                )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class BlockfsMetadataAuditResult:
    namespace_files: int
    manifest_files: int
    manifest_chunks: int
    extent_hints: int


@dataclass(frozen=True)
class BlockfsExtentHintBuildResult:
    generation: int
    manifest_files: int
    manifest_chunks: int
    allocations: int
    extent_hints: int
    complete: bool


def build_blockfs_extent_hints(
    manifest_index: BlockfsManifestIndexSnapshot,
    allocations: BlockfsPayloadAllocationSnapshot,
    *,
    require_complete: bool = False,
) -> BlockfsExtentHintsSnapshot:
    if manifest_index.generation != allocations.generation:
        raise ValueError(
            "payload allocation generation "
            f"{allocations.generation} does not match manifest index generation "
            f"{manifest_index.generation}"
        )
    chunks_by_key: dict[tuple[int, int, int], BlockfsManifestChunk] = {}
    for entry in manifest_index.entries:
        for chunk in entry.chunks:
            chunks_by_key[(entry.inode_number, chunk.file_offset, chunk.length)] = chunk

    extents: list[BlockfsExtent] = []
    for allocation in allocations.allocations:
        key = (
            allocation.inode_number,
            allocation.file_offset,
            allocation.payload_length,
        )
        chunk = chunks_by_key.get(key)
        if chunk is None:
            raise ValueError("payload allocation does not match a manifest chunk")
        extents.append(
            BlockfsExtent(
                inode_number=allocation.inode_number,
                file_offset=allocation.file_offset,
                payload_block=allocation.payload_block,
                payload_offset=allocation.payload_offset,
                payload_length=allocation.payload_length,
                expected_hash=chunk.chunk_hash,
            )
        )
    if require_complete and len(extents) != len(chunks_by_key):
        raise ValueError("payload allocations do not cover every manifest chunk")
    return BlockfsExtentHintsSnapshot(
        manifest_index.generation,
        tuple(sorted(extents, key=lambda extent: (extent.inode_number, extent.file_offset))),
    )


def describe_blockfs_extent_hint_build(
    manifest_index: BlockfsManifestIndexSnapshot,
    allocations: BlockfsPayloadAllocationSnapshot,
    extent_hints: BlockfsExtentHintsSnapshot,
) -> BlockfsExtentHintBuildResult:
    return BlockfsExtentHintBuildResult(
        generation=manifest_index.generation,
        manifest_files=len(manifest_index.entries),
        manifest_chunks=manifest_index.chunk_count,
        allocations=len(allocations.allocations),
        extent_hints=len(extent_hints.extents),
        complete=len(extent_hints.extents) == manifest_index.chunk_count,
    )


def audit_blockfs_metadata(
    namespace: BlockfsNamespaceSnapshot,
    manifest_index: BlockfsManifestIndexSnapshot | None,
    extent_hints: BlockfsExtentHintsSnapshot | None,
) -> BlockfsMetadataAuditResult:
    files_by_inode = {file.inode_number: file for file in namespace.files}
    chunks_by_key: dict[tuple[int, int, int], BlockfsManifestChunk] = {}
    manifest_files = 0
    manifest_chunks = 0
    if manifest_index is not None:
        _require_same_generation("manifest index", manifest_index.generation, namespace.generation)
        entries_by_inode: dict[int, BlockfsManifestEntry] = {}
        for entry in manifest_index.entries:
            file = files_by_inode.get(entry.inode_number)
            if file is None:
                raise ValueError(f"manifest entry is not in namespace: {entry.path}")
            if file.path != entry.path:
                raise ValueError(f"manifest path mismatch for inode: {entry.inode_number}")
            if file.size != entry.size:
                raise ValueError(f"manifest size mismatch for inode: {entry.inode_number}")
            entries_by_inode[entry.inode_number] = entry
            for chunk in entry.chunks:
                chunks_by_key[(entry.inode_number, chunk.file_offset, chunk.length)] = chunk
        for file in namespace.files:
            if file.inode_number not in entries_by_inode:
                raise ValueError(f"namespace file missing from manifest index: {file.path}")
        manifest_files = len(manifest_index.entries)
        manifest_chunks = manifest_index.chunk_count

    extent_count = 0
    if extent_hints is not None:
        _require_same_generation("extent hints", extent_hints.generation, namespace.generation)
        if manifest_index is None:
            raise ValueError("extent hints require manifest index audit")
        for extent in extent_hints.extents:
            if extent.inode_number not in files_by_inode:
                raise ValueError(f"extent hint inode is not in namespace: {extent.inode_number}")
            chunk = chunks_by_key.get(
                (extent.inode_number, extent.file_offset, extent.payload_length)
            )
            if chunk is None:
                raise ValueError("extent hint does not match a manifest chunk")
            if chunk.chunk_hash != extent.expected_hash:
                raise ValueError(f"extent hint hash mismatch for inode: {extent.inode_number}")
        extent_count = len(extent_hints.extents)

    return BlockfsMetadataAuditResult(
        namespace_files=len(namespace.files),
        manifest_files=manifest_files,
        manifest_chunks=manifest_chunks,
        extent_hints=extent_count,
    )


def _require_same_generation(name: str, generation: int, namespace_generation: int) -> None:
    if generation != namespace_generation:
        raise ValueError(
            f"{name} generation {generation} does not match namespace generation {namespace_generation}"
        )


@dataclass(frozen=True)
class BlockfsStats:
    registered_extents: int
    invalidations: int
    read_folio_calls: int
    readahead_calls: int
    verified_page_fills: int
    miss_events_queued: int = 0
    miss_events_drained: int = 0
    miss_events_pending: int = 0
    miss_events_dropped: int = 0
    miss_wait_successes: int = 0
    miss_wait_timeouts: int = 0
    miss_wait_interruptions: int = 0
    extent_generation: int = 0
    hash_verifications: int = 0
    hash_mismatches: int = 0
    hash_bytes_verified: int = 0
    readahead_miss_windows: int = 0
    readahead_miss_bytes: int = 0
    abi_version: int = ABI_VERSION

    @classmethod
    def unpack(cls, payload: bytes | bytearray) -> "BlockfsStats":
        (
            abi_version,
            _reserved,
            registered_extents,
            invalidations,
            read_folio_calls,
            readahead_calls,
            verified_page_fills,
        ) = STATS_STRUCT.unpack(payload)
        if abi_version != ABI_VERSION:
            raise ValueError(f"unsupported blockfs ABI version: {abi_version}")
        return cls(
            registered_extents=registered_extents,
            invalidations=invalidations,
            read_folio_calls=read_folio_calls,
            readahead_calls=readahead_calls,
            verified_page_fills=verified_page_fills,
            abi_version=abi_version,
        )

    @classmethod
    def unpack_v2(cls, payload: bytes | bytearray) -> "BlockfsStats":
        (
            abi_version,
            _reserved,
            registered_extents,
            invalidations,
            read_folio_calls,
            readahead_calls,
            verified_page_fills,
            miss_events_queued,
            miss_events_drained,
            miss_events_pending,
            miss_events_dropped,
            miss_wait_successes,
            miss_wait_timeouts,
            miss_wait_interruptions,
            extent_generation,
            hash_verifications,
            hash_mismatches,
            hash_bytes_verified,
            readahead_miss_windows,
            readahead_miss_bytes,
        ) = STATS_V2_STRUCT.unpack(payload)
        if abi_version != ABI_VERSION:
            raise ValueError(f"unsupported blockfs ABI version: {abi_version}")
        return cls(
            registered_extents=registered_extents,
            invalidations=invalidations,
            read_folio_calls=read_folio_calls,
            readahead_calls=readahead_calls,
            verified_page_fills=verified_page_fills,
            miss_events_queued=miss_events_queued,
            miss_events_drained=miss_events_drained,
            miss_events_pending=miss_events_pending,
            miss_events_dropped=miss_events_dropped,
            miss_wait_successes=miss_wait_successes,
            miss_wait_timeouts=miss_wait_timeouts,
            miss_wait_interruptions=miss_wait_interruptions,
            extent_generation=extent_generation,
            hash_verifications=hash_verifications,
            hash_mismatches=hash_mismatches,
            hash_bytes_verified=hash_bytes_verified,
            readahead_miss_windows=readahead_miss_windows,
            readahead_miss_bytes=readahead_miss_bytes,
            abi_version=abi_version,
        )


@dataclass(frozen=True)
class BlockfsMissEvent:
    event_id: int
    reason: str
    inode_number: int
    offset: int
    length: int
    dropped_events: int
    mount_id: int = 0
    metadata_root: str = ""
    max_cords: int = 0
    readahead_pages: int = 0
    miss_wait_ms: int = 0
    verify_hashes: bool = False
    mount_flags: int = 0
    abi_version: int = ABI_VERSION
    flags: int = 0

    @classmethod
    def unpack(cls, payload: bytes | bytearray) -> "BlockfsMissEvent":
        (
            abi_version,
            flags,
            event_id,
            inode_number,
            offset,
            length,
            reason_code,
            _reserved,
            dropped_events,
            mount_id,
            max_cords,
            readahead_pages,
            miss_wait_ms,
            mount_flags,
            metadata_root_bytes,
        ) = MISS_EVENT_STRUCT.unpack(payload)
        if abi_version != ABI_VERSION:
            raise ValueError(f"unsupported blockfs ABI version: {abi_version}")
        metadata_root = metadata_root_bytes.split(b"\0", 1)[0].decode("utf-8", "replace")
        return cls(
            event_id=event_id,
            reason=MISS_REASON_NAMES.get(reason_code, "unknown"),
            inode_number=inode_number,
            offset=offset,
            length=length,
            dropped_events=dropped_events,
            mount_id=mount_id,
            metadata_root="" if metadata_root == "-" else metadata_root,
            max_cords=max_cords,
            readahead_pages=readahead_pages,
            miss_wait_ms=miss_wait_ms,
            verify_hashes=bool(mount_flags & MOUNT_VERIFY_HASHES),
            mount_flags=mount_flags,
            abi_version=abi_version,
            flags=flags,
        )


class LinuxBlockfsControl:
    def __init__(
        self,
        device: str | None = None,
        *,
        opener: Callable[[str, int], int] = os.open,
        closer: Callable[[int], None] = os.close,
        ioctl: Callable[..., int | bytes | bytearray] = fcntl.ioctl,
    ) -> None:
        if device is None:
            device = (
                "/dev/satfs_blockfs_ctl"
                if os.path.exists("/dev/satfs_blockfs_ctl")
                else "/dev/cxnfs_blockfs_ctl"
            )
        self.device = device
        self._opener = opener
        self._closer = closer
        self._ioctl = ioctl

    def stats(self) -> BlockfsStats:
        buffer_v2 = bytearray(STATS_V2_STRUCT.pack(ABI_VERSION, 0, *([0] * 18)))
        fd = self._opener(self.device, os.O_RDWR)
        try:
            try:
                self._ioctl(fd, IOCTL_STATS_V2, buffer_v2, True)
                return BlockfsStats.unpack_v2(buffer_v2)
            except OSError as exc:
                if exc.errno != errno.ENOTTY:
                    raise
            buffer = bytearray(STATS_STRUCT.pack(ABI_VERSION, 0, 0, 0, 0, 0, 0))
            self._ioctl(fd, IOCTL_STATS, buffer, True)
            return BlockfsStats.unpack(buffer)
        finally:
            self._closer(fd)

    def next_miss(self, *, nonblock: bool = True) -> BlockfsMissEvent | None:
        flags = MISS_EVENT_NONBLOCK if nonblock else 0
        buffer = bytearray(
            MISS_EVENT_STRUCT.pack(
                ABI_VERSION,
                flags,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                b"\0" * PATH_MAX,
            )
        )
        fd = self._opener(self.device, os.O_RDWR)
        try:
            try:
                self._ioctl(fd, IOCTL_NEXT_MISS_EVENT, buffer, True)
            except OSError as exc:
                if nonblock and exc.errno == errno.EAGAIN:
                    return None
                raise
            return BlockfsMissEvent.unpack(buffer)
        finally:
            self._closer(fd)

    def register_extent(
        self,
        extent: BlockfsExtent,
        *,
        allow_unsupported: bool = False,
    ) -> bool:
        return self._write_ioctl(
            IOCTL_REGISTER_EXTENT,
            extent.pack(),
            allow_unsupported=allow_unsupported,
        )

    def register_file(self, file: BlockfsFile) -> bool:
        return self._write_ioctl(
            IOCTL_REGISTER_FILE,
            file.pack(),
            allow_unsupported=False,
        )

    def publish_namespace(self, snapshot: BlockfsNamespaceSnapshot) -> int:
        published = 0
        for file in snapshot.files:
            self.register_file(file)
            published += 1
        return published

    def publish_extent_hints(
        self,
        snapshot: BlockfsExtentHintsSnapshot,
        *,
        allow_unsupported: bool = False,
    ) -> int:
        published = 0
        for extent in snapshot.extents:
            if self.register_extent(extent, allow_unsupported=allow_unsupported):
                published += 1
        return published

    def invalidate(
        self,
        invalidation: BlockfsInvalidation,
        *,
        allow_unsupported: bool = False,
    ) -> bool:
        return self._write_ioctl(
            IOCTL_INVALIDATE,
            invalidation.pack(),
            allow_unsupported=allow_unsupported,
        )

    def _write_ioctl(
        self,
        request: int,
        payload: bytes,
        *,
        allow_unsupported: bool,
    ) -> bool:
        fd = self._opener(self.device, os.O_RDWR)
        try:
            self._ioctl(fd, request, payload)
            return True
        except OSError as exc:
            if allow_unsupported and exc.errno == errno.EOPNOTSUPP:
                return False
            raise
        finally:
            self._closer(fd)


CXBFS_ABI_VERSION = ABI_VERSION
CXBFS_DEFAULT_EXTENT_FLAGS = DEFAULT_EXTENT_FLAGS
CxbfsExtent = BlockfsExtent
CxbfsExtentHintBuildResult = BlockfsExtentHintBuildResult
CxbfsExtentHintsSnapshot = BlockfsExtentHintsSnapshot
CxbfsExtentHintStore = BlockfsExtentHintStore
CxbfsFile = BlockfsFile
CxbfsInvalidation = BlockfsInvalidation
CxbfsMissEvent = BlockfsMissEvent
CxbfsManifestChunk = BlockfsManifestChunk
CxbfsManifestEntry = BlockfsManifestEntry
CxbfsManifestIndexSnapshot = BlockfsManifestIndexSnapshot
CxbfsManifestIndexStore = BlockfsManifestIndexStore
CxbfsMetadataAuditResult = BlockfsMetadataAuditResult
CxbfsNamespaceSnapshot = BlockfsNamespaceSnapshot
CxbfsNamespaceStore = BlockfsNamespaceStore
CxbfsPayloadAllocation = BlockfsPayloadAllocation
CxbfsPayloadAllocationSnapshot = BlockfsPayloadAllocationSnapshot
CxbfsPayloadAllocationStore = BlockfsPayloadAllocationStore
CxbfsStats = BlockfsStats
LinuxCxbfsControl = LinuxBlockfsControl
audit_cxbfs_metadata = audit_blockfs_metadata
build_cxbfs_extent_hints = build_blockfs_extent_hints
describe_cxbfs_extent_hint_build = describe_blockfs_extent_hint_build


@dataclass(frozen=True)
class BlockfsStageResult:
    inode_number: int
    path: str
    size: int
    file_hash: bytes
    chunks_count: int
    extents_count: int
    published_to_kernel: bool = False
    metadata_generation: int = 1


def stage_stream_to_blockfs(
    metadata_root: str | os.PathLike[str],
    payload_target: str | os.PathLike[str] | None,
    stream: BinaryIO | Any,
    target_relative_path: str,
    *,
    size: int | None = None,
    chunk_size: int = DEFAULT_STAGING_CHUNK_SIZE,
    ctl_client: LinuxBlockfsControl | None = None,
    allow_overwrite: bool = False,
    mode: int = 0o444,
) -> BlockfsStageResult:
    if not target_relative_path or target_relative_path.startswith("/") or target_relative_path.endswith("/"):
        raise ValueError(f"invalid relative path: {target_relative_path}")
    for part in target_relative_path.split("/"):
        if part in ("", ".", ".."):
            raise ValueError(f"invalid relative path component: {target_relative_path}")

    metadata_dir = Path(metadata_root)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    ns_store = BlockfsNamespaceStore()
    manifest_store = BlockfsManifestIndexStore()
    alloc_store = BlockfsPayloadAllocationStore()
    hints_store = BlockfsExtentHintStore()

    try:
        ns = ns_store.load(metadata_dir)
        manifest_index = manifest_store.load(metadata_dir)
        alloc_snapshot = alloc_store.load(metadata_dir)
        hints_snapshot = hints_store.load_if_exists(metadata_dir)
    except (FileNotFoundError, ValueError):
        ns = BlockfsNamespaceSnapshot(0, ())
        manifest_index = BlockfsManifestIndexSnapshot(0, ())
        alloc_snapshot = BlockfsPayloadAllocationSnapshot(0, ())
        hints_snapshot = BlockfsExtentHintsSnapshot(0, ())

    existing_file = next((f for f in ns.files if f.path == target_relative_path), None)
    if existing_file is not None:
        if not allow_overwrite:
            raise FileExistsError(f"file already exists in blockfs namespace: {target_relative_path}")
        inode_number = existing_file.inode_number
        files = [f for f in ns.files if f.inode_number != inode_number]
        manifest_entries = [e for e in manifest_index.entries if e.inode_number != inode_number]
        allocations = [a for a in alloc_snapshot.allocations if a.inode_number != inode_number]
        hints = [h for h in (hints_snapshot.extents if hints_snapshot else ()) if h.inode_number != inode_number]
    else:
        used_inodes = {f.inode_number for f in ns.files}
        inode_number = 2
        while inode_number in used_inodes:
            inode_number += 1
        files = list(ns.files)
        manifest_entries = list(manifest_index.entries)
        allocations = list(alloc_snapshot.allocations)
        hints = list(hints_snapshot.extents if hints_snapshot else ())

    if allocations:
        highest_byte = max(a.payload_block * BLOCK_SIZE + a.payload_offset + a.payload_length for a in allocations)
        start_byte = ((highest_byte + BLOCK_SIZE - 1) // BLOCK_SIZE) * BLOCK_SIZE
    else:
        start_byte = 0

    if payload_target is None:
        payload_target_path = metadata_dir / "cache" / "container.cache"
        payload_target_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        payload_target_path = Path(payload_target)

    if payload_target_path.exists():
        pfd = os.open(str(payload_target_path), os.O_RDWR)
    else:
        payload_target_path.parent.mkdir(parents=True, exist_ok=True)
        pfd = os.open(str(payload_target_path), os.O_RDWR | os.O_CREAT, 0o644)

    hasher = hashlib.sha256()
    file_offset = 0
    new_chunks: list[BlockfsManifestChunk] = []
    new_allocs: list[BlockfsPayloadAllocation] = []
    new_extents: list[BlockfsExtent] = []

    try:
        os.lseek(pfd, start_byte, os.SEEK_SET)
        while True:
            to_read = chunk_size
            if size is not None:
                remaining = size - file_offset
                if remaining <= 0:
                    break
                to_read = min(chunk_size, remaining)

            chunk_data = stream.read(to_read)
            if not chunk_data:
                break
            chunk_len = len(chunk_data)
            hasher.update(chunk_data)
            chunk_hash = hashlib.sha256(chunk_data).digest()

            written = 0
            while written < chunk_len:
                n = os.write(pfd, chunk_data[written:])
                if n == 0:
                    raise OSError("failed to write payload to target")
                written += n

            abs_offset = start_byte + file_offset
            p_block = abs_offset // BLOCK_SIZE
            p_offset = abs_offset % BLOCK_SIZE

            new_chunks.append(
                BlockfsManifestChunk(
                    file_offset=file_offset,
                    length=chunk_len,
                    chunk_hash=chunk_hash,
                )
            )
            new_allocs.append(
                BlockfsPayloadAllocation(
                    inode_number=inode_number,
                    file_offset=file_offset,
                    payload_block=p_block,
                    payload_offset=p_offset,
                    payload_length=chunk_len,
                )
            )
            new_extents.append(
                BlockfsExtent(
                    inode_number=inode_number,
                    file_offset=file_offset,
                    payload_block=p_block,
                    payload_offset=p_offset,
                    payload_length=chunk_len,
                    expected_hash=chunk_hash,
                    flags=DEFAULT_EXTENT_FLAGS,
                )
            )
            file_offset += chunk_len
    finally:
        os.close(pfd)

    total_size = file_offset
    total_hash = hasher.digest()

    staged_file = BlockfsFile(
        inode_number=inode_number,
        path=target_relative_path,
        size=total_size,
        mode=mode,
    )
    files.append(staged_file)
    manifest_entries.append(
        BlockfsManifestEntry(
            inode_number=inode_number,
            path=target_relative_path,
            size=total_size,
            file_hash=total_hash,
            chunks=tuple(new_chunks),
        )
    )
    allocations.extend(new_allocs)
    hints.extend(new_extents)

    next_gen = ns.generation + 1
    new_ns = BlockfsNamespaceSnapshot(next_gen, tuple(files))
    new_manifest = BlockfsManifestIndexSnapshot(next_gen, tuple(manifest_entries))
    new_alloc = BlockfsPayloadAllocationSnapshot(next_gen, tuple(allocations))
    new_hints = BlockfsExtentHintsSnapshot(next_gen, tuple(hints))

    ns_store.write(metadata_dir, new_ns)
    manifest_store.write(metadata_dir, new_manifest)
    alloc_store.write(metadata_dir, new_alloc)
    hints_store.write(metadata_dir, new_hints)

    published = False
    if ctl_client is not None:
        ctl_client.register_file(staged_file)
        for ext in new_extents:
            ctl_client.register_extent(ext, allow_unsupported=True)
        published = True

    return BlockfsStageResult(
        inode_number=inode_number,
        path=target_relative_path,
        size=total_size,
        file_hash=total_hash,
        chunks_count=len(new_chunks),
        extents_count=len(new_extents),
        published_to_kernel=published,
        metadata_generation=next_gen,
    )


def stage_file_to_blockfs(
    metadata_root: str | os.PathLike[str],
    payload_target: str | os.PathLike[str] | None,
    source_file_path: str | os.PathLike[str],
    target_relative_path: str | None = None,
    *,
    chunk_size: int = DEFAULT_STAGING_CHUNK_SIZE,
    ctl_client: LinuxBlockfsControl | None = None,
    allow_overwrite: bool = False,
    mode: int = 0o444,
) -> BlockfsStageResult:
    source_path = Path(source_file_path)
    if not source_path.is_file():
        raise FileNotFoundError(f"source file not found: {source_file_path}")
    if target_relative_path is None:
        target_relative_path = source_path.name
    with source_path.open("rb") as f:
        return stage_stream_to_blockfs(
            metadata_root=metadata_root,
            payload_target=payload_target,
            stream=f,
            target_relative_path=target_relative_path,
            size=source_path.stat().st_size,
            chunk_size=chunk_size,
            ctl_client=ctl_client,
            allow_overwrite=allow_overwrite,
            mode=mode,
        )


def stage_download_to_blockfs(
    metadata_root: str | os.PathLike[str],
    payload_target: str | os.PathLike[str] | None,
    url: str,
    target_relative_path: str,
    *,
    chunk_size: int = DEFAULT_STAGING_CHUNK_SIZE,
    ctl_client: LinuxBlockfsControl | None = None,
    allow_overwrite: bool = False,
    mode: int = 0o444,
    timeout: float = 30.0,
) -> BlockfsStageResult:
    import urllib.request

    req = urllib.request.Request(url, headers={"User-Agent": "SATFS-BlockfsStager/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        content_length_header = response.headers.get("Content-Length")
        size = int(content_length_header) if content_length_header else None
        return stage_stream_to_blockfs(
            metadata_root=metadata_root,
            payload_target=payload_target,
            stream=response,
            target_relative_path=target_relative_path,
            size=size,
            chunk_size=chunk_size,
            ctl_client=ctl_client,
            allow_overwrite=allow_overwrite,
            mode=mode,
        )


def stage_from_network_server(
    metadata_root: str | os.PathLike[str],
    payload_target: str | os.PathLike[str] | None,
    reader: Callable[[str, int, int], bytes] | Any,
    remote_path: str,
    target_relative_path: str | None = None,
    *,
    remote_size: int | None = None,
    chunk_size: int = DEFAULT_STAGING_CHUNK_SIZE,
    ctl_client: LinuxBlockfsControl | None = None,
    allow_overwrite: bool = False,
    mode: int = 0o444,
) -> BlockfsStageResult:
    if target_relative_path is None:
        target_relative_path = remote_path.lstrip("/").split("/")[-1]

    class _NetworkReaderStream:
        def __init__(self, r_func: Callable[[str, int, int], bytes], r_path: str, total_len: int | None):
            self.r_func = r_func
            self.r_path = r_path
            self.total_len = total_len
            self.offset = 0

        def read(self, n: int = -1) -> bytes:
            if n <= 0:
                n = chunk_size
            if self.total_len is not None and self.offset >= self.total_len:
                return b""
            if self.total_len is not None:
                n = min(n, self.total_len - self.offset)
            data = self.r_func(self.r_path, self.offset, n)
            self.offset += len(data)
            return data

    if callable(reader):
        stream = _NetworkReaderStream(reader, remote_path, remote_size)
    elif hasattr(reader, "read_range"):
        stream = _NetworkReaderStream(lambda p, off, ln: reader.read_range(p, off, ln), remote_path, remote_size)
    elif hasattr(reader, "read"):
        stream = reader
    else:
        raise TypeError(f"unsupported reader type: {type(reader)}")

    return stage_stream_to_blockfs(
        metadata_root=metadata_root,
        payload_target=payload_target,
        stream=stream,
        target_relative_path=target_relative_path,
        size=remote_size,
        chunk_size=chunk_size,
        ctl_client=ctl_client,
        allow_overwrite=allow_overwrite,
        mode=mode,
    )


CxbfsStageResult = BlockfsStageResult
stage_file_to_cxbfs = stage_file_to_blockfs
stage_stream_to_cxbfs = stage_stream_to_blockfs
stage_download_to_cxbfs = stage_download_to_blockfs
stage_from_network_server_cxbfs = stage_from_network_server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="satfs.blockfs", description="SATFS BlockFS staging and management")
    subparsers = parser.add_subparsers(dest="command", required=True)

    stage_file_parser = subparsers.add_parser("stage-file", help="Stage a local file into blockfs")
    stage_file_parser.add_argument("--metadata-root", required=True, help="BlockFS metadata directory")
    stage_file_parser.add_argument("--payload-target", default=None, help="Payload device or file path")
    stage_file_parser.add_argument("--file", required=True, help="Local file to stage")
    stage_file_parser.add_argument("--target-path", default=None, help="Dataset relative path")
    stage_file_parser.add_argument("--ctl-device", default=None, help="Control device for kernel registration")
    stage_file_parser.add_argument("--allow-overwrite", action="store_true", help="Allow overwriting existing files")

    stage_dl_parser = subparsers.add_parser("stage-download", help="Download and stage a remote URL directly into blockfs")
    stage_dl_parser.add_argument("--metadata-root", required=True, help="BlockFS metadata directory")
    stage_dl_parser.add_argument("--payload-target", default=None, help="Payload device or file path")
    stage_dl_parser.add_argument("--url", required=True, help="Remote URL to download")
    stage_dl_parser.add_argument("--target-path", required=True, help="Dataset relative path")
    stage_dl_parser.add_argument("--ctl-device", default=None, help="Control device for kernel registration")
    stage_dl_parser.add_argument("--allow-overwrite", action="store_true", help="Allow overwriting existing files")

    sync_parser = subparsers.add_parser("sync", help="Synchronize metadata to kernel blockfs device")
    sync_parser.add_argument("--metadata-root", required=True, help="BlockFS metadata directory")
    sync_parser.add_argument("--ctl-device", default=None, help="Control device")

    args = parser.parse_args(argv)

    if args.command == "stage-file":
        ctl = LinuxBlockfsControl(args.ctl_device) if args.ctl_device else None
        res = stage_file_to_blockfs(
            metadata_root=args.metadata_root,
            payload_target=args.payload_target,
            source_file_path=args.file,
            target_relative_path=args.target_path,
            ctl_client=ctl,
            allow_overwrite=args.allow_overwrite,
        )
        print(f"staged {res.path}: inode={res.inode_number} size={res.size} hash={res.file_hash.hex()} published={res.published_to_kernel}")
        return 0
    elif args.command == "stage-download":
        ctl = LinuxBlockfsControl(args.ctl_device) if args.ctl_device else None
        res = stage_download_to_blockfs(
            metadata_root=args.metadata_root,
            payload_target=args.payload_target,
            url=args.url,
            target_relative_path=args.target_path,
            ctl_client=ctl,
            allow_overwrite=args.allow_overwrite,
        )
        print(f"staged {res.path}: inode={res.inode_number} size={res.size} hash={res.file_hash.hex()} published={res.published_to_kernel}")
        return 0
    elif args.command == "sync":
        ctl = LinuxBlockfsControl(args.ctl_device)
        ns = BlockfsNamespaceStore().load(args.metadata_root)
        hints = BlockfsExtentHintStore().load_if_exists(args.metadata_root)
        ctl.publish_namespace(ns)
        if hints:
            ctl.publish_extent_hints(hints, allow_unsupported=True)
        print(f"synchronized namespace ({len(ns.files)} files) and extents ({len(hints.extents) if hints else 0})")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
