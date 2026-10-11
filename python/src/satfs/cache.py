from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from .dataset import CxnfsDatasetClient, SendResult


@dataclass(frozen=True)
class RangeCacheStats:
    hits: int
    misses: int
    evictions: int
    bytes_cached: int
    entries: int


@dataclass(frozen=True)
class _RangeEntry:
    shard_path: str
    offset: int
    data: bytes

    @property
    def end(self) -> int:
        return self.offset + len(self.data)


class CxnfsRangeCacheClient:
    """Bounded in-process cache for CXNFS shard byte ranges."""

    def __init__(self, delegate: CxnfsDatasetClient, max_bytes: int = 512 * 1024 * 1024) -> None:
        if delegate is None:
            raise ValueError("delegate is required")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.delegate = delegate
        self.max_bytes = max_bytes
        self._entries: OrderedDict[tuple[str, int, int], _RangeEntry] = OrderedDict()
        self._bytes_cached = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._lock = RLock()

    def read_mds_shard(self, shard_path: str | Path, offset: int = 0, length: int | None = None) -> bytes:
        if length is None:
            with self._lock:
                self._misses += 1
            return self.delegate.read_mds_shard(shard_path, offset, length)
        if length < 0:
            raise ValueError("length must be non-negative")

        normalized_path = str(shard_path)
        with self._lock:
            cached = self._find_containing(normalized_path, offset, length)
            if cached is not None:
                self._hits += 1
                start = offset - cached.offset
                return cached.data[start:start + length]
            self._misses += 1

        data = self.delegate.read_mds_shard(normalized_path, offset, length)
        if len(data) == length:
            with self._lock:
                self._store(normalized_path, offset, data)
        return data

    def prefetch_mds_shard(self, shard_path: str | Path, offset: int, length: int) -> bool:
        if length <= 0:
            raise ValueError("length must be positive")
        normalized_path = str(shard_path)
        with self._lock:
            cached = self._find_containing(normalized_path, offset, length)
            if cached is not None:
                self._hits += 1
                return False
            self._misses += 1

        data = self.delegate.read_mds_shard(normalized_path, offset, length)
        if len(data) != length:
            return False
        with self._lock:
            self._store(normalized_path, offset, data)
        return True

    def has_range(self, shard_path: str | Path, offset: int, length: int) -> bool:
        if length <= 0:
            raise ValueError("length must be positive")
        normalized_path = str(shard_path)
        with self._lock:
            return self._find_containing(normalized_path, offset, length) is not None

    def read_cached(self, shard_path: str | Path, offset: int, length: int) -> bytes | None:
        if length <= 0:
            raise ValueError("length must be positive")
        normalized_path = str(shard_path)
        with self._lock:
            cached = self._find_containing(normalized_path, offset, length)
            if cached is None:
                return None
            self._hits += 1
            start = offset - cached.offset
            return cached.data[start:start + length]

    def store_range(self, shard_path: str | Path, offset: int, data: bytes) -> bool:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if not data:
            return False
        normalized_path = str(shard_path)
        with self._lock:
            before = self._bytes_cached
            self._store(normalized_path, offset, data)
            return self._bytes_cached != before or self.has_range(normalized_path, offset, len(data))

    def send_safetensors_output(self, output_dir: str | Path) -> SendResult:
        return self.delegate.send_safetensors_output(output_dir)

    def clear(self, reset_stats: bool = False) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes_cached = 0
            if reset_stats:
                self._hits = 0
                self._misses = 0
                self._evictions = 0

    def reset_stats(self) -> None:
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._evictions = 0

    def close(self) -> None:
        self.clear()
        close = getattr(self.delegate, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> CxnfsRangeCacheClient:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    @property
    def stats(self) -> RangeCacheStats:
        with self._lock:
            return RangeCacheStats(
                self._hits,
                self._misses,
                self._evictions,
                self._bytes_cached,
                len(self._entries),
            )

    def _find_containing(self, shard_path: str, offset: int, length: int) -> _RangeEntry | None:
        end = offset + length
        for key, entry in list(self._entries.items()):
            if entry.shard_path == shard_path and entry.offset <= offset and end <= entry.end:
                self._entries.move_to_end(key)
                return entry
        return None

    def _store(self, shard_path: str, offset: int, data: bytes) -> None:
        if len(data) > self.max_bytes:
            return
        key = (shard_path, offset, len(data))
        previous = self._entries.pop(key, None)
        if previous is not None:
            self._bytes_cached -= len(previous.data)
        self._entries[key] = _RangeEntry(shard_path, offset, data)
        self._bytes_cached += len(data)
        self._evict_to_budget()

    def _evict_to_budget(self) -> None:
        while self._bytes_cached > self.max_bytes and self._entries:
            _, entry = self._entries.popitem(last=False)
            self._bytes_cached -= len(entry.data)
            self._evictions += 1
