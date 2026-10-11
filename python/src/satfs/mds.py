from __future__ import annotations

import json
import random
from bisect import bisect_right
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Iterator, Sequence

from .cache import CxnfsRangeCacheClient
from .dataset import CxnfsDatasetClient
from .tensor_memory import tensor_writable_memoryview


@dataclass(frozen=True)
class MdsShardMetadata:
    shard_name: str
    sample_count: int
    byte_size: int
    column_names: tuple[str, ...]
    column_encodings: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if self.sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        if self.byte_size < 0:
            raise ValueError("byte_size must be non-negative")
        if self.column_names != ("data", "path"):
            raise ValueError("CXNFS MDS reader expects data,path columns")
        if self.column_encodings != ("bytes", "str"):
            raise ValueError("CXNFS MDS reader expects bytes,str encodings")


@dataclass(frozen=True)
class MdsSample:
    global_index: int
    shard_index: int
    local_index: int
    shard_name: str
    path: str
    data: bytes

    def __post_init__(self) -> None:
        if self.global_index < 0:
            raise ValueError("global_index must be non-negative")
        if self.shard_index < 0:
            raise ValueError("shard_index must be non-negative")
        if self.local_index < 0:
            raise ValueError("local_index must be non-negative")
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if not self.path:
            raise ValueError("path is required")
        if not isinstance(self.data, bytes):
            raise TypeError("data must be bytes")


@dataclass(frozen=True)
class MdsSampleLocation:
    global_index: int
    shard_index: int
    local_index: int
    shard_name: str
    offset: int
    length: int

    def __post_init__(self) -> None:
        if self.global_index < 0:
            raise ValueError("global_index must be non-negative")
        if self.shard_index < 0:
            raise ValueError("shard_index must be non-negative")
        if self.local_index < 0:
            raise ValueError("local_index must be non-negative")
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length <= 0:
            raise ValueError("length must be positive")


@dataclass(frozen=True)
class MdsDirectSample:
    global_index: int
    shard_index: int
    local_index: int
    shard_name: str
    data_offset: int
    data_length: int
    bytes_read: int
    path: str | None = None

    def __post_init__(self) -> None:
        if self.global_index < 0:
            raise ValueError("global_index must be non-negative")
        if self.shard_index < 0:
            raise ValueError("shard_index must be non-negative")
        if self.local_index < 0:
            raise ValueError("local_index must be non-negative")
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if self.data_offset < 0:
            raise ValueError("data_offset must be non-negative")
        if self.data_length < 0:
            raise ValueError("data_length must be non-negative")
        if self.bytes_read < 0:
            raise ValueError("bytes_read must be non-negative")
        if self.bytes_read != self.data_length:
            raise ValueError("bytes_read must match data_length")


@dataclass(frozen=True)
class MdsDataReadPlan:
    global_index: int
    shard_index: int
    local_index: int
    shard_name: str
    data_offset: int
    data_length: int
    path: str | None = None

    def __post_init__(self) -> None:
        if self.global_index < 0:
            raise ValueError("global_index must be non-negative")
        if self.shard_index < 0:
            raise ValueError("shard_index must be non-negative")
        if self.local_index < 0:
            raise ValueError("local_index must be non-negative")
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if self.data_offset < 0:
            raise ValueError("data_offset must be non-negative")
        if self.data_length < 0:
            raise ValueError("data_length must be non-negative")


@dataclass(frozen=True)
class MdsDirectBufferSample:
    metadata: MdsDirectSample
    destination: Any

    def __post_init__(self) -> None:
        if self.metadata is None:
            raise ValueError("metadata is required")
        if self.destination is None:
            raise ValueError("destination is required")

    @property
    def data(self) -> memoryview:
        return _writable_byte_view(self.destination)[:self.metadata.data_length]

    @property
    def path(self) -> str | None:
        return self.metadata.path

    @property
    def data_length(self) -> int:
        return self.metadata.data_length

    @property
    def bytes_read(self) -> int:
        return self.metadata.bytes_read


@dataclass(frozen=True)
class MdsDirectTensorSample:
    metadata: MdsDirectSample
    destination: Any

    def __post_init__(self) -> None:
        if self.metadata is None:
            raise ValueError("metadata is required")
        if self.destination is None:
            raise ValueError("destination is required")

    @property
    def tensor(self) -> Any:
        return self.destination

    @property
    def data(self) -> Any:
        return self.destination

    @property
    def path(self) -> str | None:
        return self.metadata.path

    @property
    def data_length(self) -> int:
        return self.metadata.data_length

    @property
    def bytes_read(self) -> int:
        return self.metadata.bytes_read


@dataclass(frozen=True)
class MdsReadSpan:
    shard_index: int
    shard_name: str
    offset: int
    length: int
    locations: tuple[MdsSampleLocation, ...]
    result_positions: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.shard_index < 0:
            raise ValueError("shard_index must be non-negative")
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length <= 0:
            raise ValueError("length must be positive")
        if not self.locations:
            raise ValueError("locations are required")
        if len(self.locations) != len(self.result_positions):
            raise ValueError("locations and result_positions must have equal lengths")
        if any(position < 0 for position in self.result_positions):
            raise ValueError("result positions must be non-negative")


@dataclass(frozen=True)
class MdsBatch:
    batch_index: int
    indices: tuple[int, ...]
    spans: tuple[MdsReadSpan, ...]

    def __post_init__(self) -> None:
        if self.batch_index < 0:
            raise ValueError("batch_index must be non-negative")
        if not self.indices:
            raise ValueError("indices are required")

    @property
    def sample_count(self) -> int:
        return len(self.indices)

    @property
    def bytes_planned(self) -> int:
        return sum(span.length for span in self.spans)


@dataclass(frozen=True)
class MdsPrefetchResult:
    spans: tuple[MdsReadSpan, ...]
    sample_count: int
    bytes_requested: int
    ranges_fetched: int
    bytes_fetched: int

    def __post_init__(self) -> None:
        if self.sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        if self.bytes_requested < 0:
            raise ValueError("bytes_requested must be non-negative")
        if self.ranges_fetched < 0:
            raise ValueError("ranges_fetched must be non-negative")
        if self.bytes_fetched < 0:
            raise ValueError("bytes_fetched must be non-negative")


@dataclass(frozen=True)
class MdsIndex:
    root: str
    shards: tuple[MdsShardMetadata, ...]

    def __post_init__(self) -> None:
        if not self.root:
            raise ValueError("root is required")
        if not self.shards:
            raise ValueError("shards are required")

    @property
    def sample_count(self) -> int:
        return sum(shard.sample_count for shard in self.shards)

    @classmethod
    def load(cls, root: str | Path) -> "MdsIndex":
        dataset_root = Path(root)
        index_path = dataset_root / "index.json"
        raw = json.loads(index_path.read_text(encoding="utf-8"))
        shards = []
        for shard in raw.get("shards", []):
            raw_data = shard.get("raw_data") or {}
            shards.append(
                MdsShardMetadata(
                    raw_data.get("basename", ""),
                    int(shard.get("samples", 0)),
                    int(raw_data.get("bytes", 0)),
                    tuple(shard.get("column_names", ())),
                    tuple(shard.get("column_encodings", ())),
                )
            )
        return cls(str(dataset_root), tuple(shards))


class MdsShardReader:
    def __init__(self, metadata: MdsShardMetadata, client: CxnfsDatasetClient) -> None:
        self.metadata = metadata
        self.client = client
        self._offsets: tuple[int, ...] | None = None
        self._sample_headers: dict[int, tuple[int, int]] = {}
        self._sample_paths: dict[int, str] = {}
        self._sample_header_cache_hits = 0
        self._sample_header_cache_misses = 0
        self._sample_path_cache_hits = 0
        self._sample_path_cache_misses = 0
        self._descriptor_lock = RLock()

    def read_sample(self, local_index: int, global_index: int = 0, shard_index: int = 0) -> MdsSample:
        return self.read_location(self.sample_location(local_index, global_index, shard_index))

    def sample_location(self, local_index: int, global_index: int = 0, shard_index: int = 0) -> MdsSampleLocation:
        if local_index < 0 or local_index >= self.metadata.sample_count:
            raise IndexError("MDS local sample index out of range")
        offsets = self.offsets()
        offset = offsets[local_index]
        length = offsets[local_index + 1] - offset
        return MdsSampleLocation(
            global_index,
            shard_index,
            local_index,
            self.metadata.shard_name,
            offset,
            length,
        )

    def read_location(self, location: MdsSampleLocation) -> MdsSample:
        self._require_location(location)
        payload = self.client.read_mds_shard(location.shard_name, location.offset, location.length)
        if len(payload) != location.length:
            raise ValueError("short MDS sample read")
        return _decode_sample_payload(payload, location)

    def read_location_data_into_buffer(
        self,
        location: MdsSampleLocation,
        destination,
        *,
        include_path: bool = False,
    ) -> MdsDirectSample:
        self._require_location(location)
        data_size, path_size = self.sample_header(location)
        return self._read_location_data_into_buffer_with_header(
            location,
            destination,
            data_size,
            path_size,
            include_path=include_path,
        )

    def read_location_data_into_tensor(
        self,
        location: MdsSampleLocation,
        tensor,
        *,
        include_path: bool = False,
        non_blocking: bool = True,
    ) -> MdsDirectSample:
        self._require_location(location)
        data_size, path_size = self.sample_header(location)
        return self._read_location_data_into_tensor_with_header(
            location,
            tensor,
            data_size,
            path_size,
            include_path=include_path,
            non_blocking=non_blocking,
        )

    def _read_location_data_into_buffer_with_header(
        self,
        location: MdsSampleLocation,
        destination,
        data_size: int,
        path_size: int,
        *,
        include_path: bool,
    ) -> MdsDirectSample:
        destination_view = _writable_byte_view(destination)
        if data_size > destination_view.nbytes:
            raise ValueError("destination buffer is smaller than MDS data payload")
        data_offset = location.offset + 8
        bytes_read = _read_client_into_buffer(
            self.client,
            location.shard_name,
            data_offset,
            destination_view[:data_size],
            data_size,
        )
        if bytes_read != data_size:
            raise ValueError("short MDS data read")
        path = self.sample_path(location, data_size, path_size) if include_path else None
        return _direct_sample(location, data_offset, data_size, bytes_read, path)

    def _read_location_data_into_tensor_with_header(
        self,
        location: MdsSampleLocation,
        tensor,
        data_size: int,
        path_size: int,
        *,
        include_path: bool,
        non_blocking: bool,
    ) -> MdsDirectSample:
        data_offset = location.offset + 8
        bytes_read = _read_client_into_tensor(
            self.client,
            location.shard_name,
            data_offset,
            tensor,
            data_size,
            non_blocking,
        )
        if bytes_read != data_size:
            raise ValueError("short MDS tensor read")
        path = self.sample_path(location, data_size, path_size) if include_path else None
        return _direct_sample(location, data_offset, data_size, bytes_read, path)

    def sample_header(self, location: MdsSampleLocation) -> tuple[int, int]:
        self._require_location(location)
        with self._descriptor_lock:
            cached = self._sample_headers.get(location.local_index)
            if cached is not None:
                self._sample_header_cache_hits += 1
        if cached is not None:
            return cached
        header = _read_sample_header(self.client, location)
        with self._descriptor_lock:
            if location.local_index in self._sample_headers:
                self._sample_header_cache_hits += 1
                return self._sample_headers[location.local_index]
            self._sample_header_cache_misses += 1
            return self._sample_headers.setdefault(location.local_index, header)

    def sample_path(self, location: MdsSampleLocation, data_size: int, path_size: int) -> str:
        self._require_location(location)
        with self._descriptor_lock:
            cached = self._sample_paths.get(location.local_index)
            if cached is not None:
                self._sample_path_cache_hits += 1
        if cached is not None:
            return cached
        path = _read_sample_path(self.client, location, data_size, path_size)
        with self._descriptor_lock:
            if location.local_index in self._sample_paths:
                self._sample_path_cache_hits += 1
                return self._sample_paths[location.local_index]
            self._sample_path_cache_misses += 1
            return self._sample_paths.setdefault(location.local_index, path)

    def _require_location(self, location: MdsSampleLocation) -> None:
        if location.shard_name != self.metadata.shard_name:
            raise ValueError("sample location belongs to a different shard")

    def descriptor_cache_evidence(self) -> dict[str, Any]:
        with self._descriptor_lock:
            header_entries = len(self._sample_headers)
            path_entries = len(self._sample_paths)
            header_hits = self._sample_header_cache_hits
            header_misses = self._sample_header_cache_misses
            path_hits = self._sample_path_cache_hits
            path_misses = self._sample_path_cache_misses
        return {
            "format": "cxnfs.mds-shard-descriptor-cache.v1",
            "shard_name": self.metadata.shard_name,
            "sample_count": self.metadata.sample_count,
            "sample_header_cache_entries": header_entries,
            "sample_header_cache_hits": header_hits,
            "sample_header_cache_misses": header_misses,
            "sample_header_cache_reuse_observed": header_hits > 0,
            "sample_path_cache_entries": path_entries,
            "sample_path_cache_hits": path_hits,
            "sample_path_cache_misses": path_misses,
            "sample_path_cache_reuse_observed": path_hits > 0,
        }

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        state["_descriptor_lock"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._sample_headers = dict(getattr(self, "_sample_headers", {}))
        self._sample_paths = dict(getattr(self, "_sample_paths", {}))
        self._sample_header_cache_hits = int(getattr(self, "_sample_header_cache_hits", 0) or 0)
        self._sample_header_cache_misses = int(getattr(self, "_sample_header_cache_misses", 0) or 0)
        self._sample_path_cache_hits = int(getattr(self, "_sample_path_cache_hits", 0) or 0)
        self._sample_path_cache_misses = int(getattr(self, "_sample_path_cache_misses", 0) or 0)
        self._descriptor_lock = RLock()

    def offsets(self) -> tuple[int, ...]:
        if self._offsets is None:
            header_size = 4 + (4 * (self.metadata.sample_count + 1))
            header = self.client.read_mds_shard(self.metadata.shard_name, 0, header_size)
            if len(header) != header_size:
                raise ValueError("short MDS header read")
            sample_count = _read_uint32_le(header, 0)
            if sample_count != self.metadata.sample_count:
                raise ValueError("MDS shard sample count does not match index")
            self._offsets = tuple(
                _read_uint32_le(header, 4 + (index * 4))
                for index in range(sample_count + 1)
            )
        return self._offsets


class MdsDataset:
    def __init__(
        self,
        root: str | Path,
        client: CxnfsDatasetClient | None = None,
        cache_bytes: int | None = None,
    ) -> None:
        self.index = MdsIndex.load(root)
        base_client = client or _local_client(root)
        self.client = CxnfsRangeCacheClient(base_client, cache_bytes) if cache_bytes is not None else base_client
        self._prefix = _prefix_counts(self.index.shards)
        self._readers = tuple(MdsShardReader(shard, self.client) for shard in self.index.shards)

    def __len__(self) -> int:
        return self.index.sample_count

    def __getitem__(self, index: int) -> MdsSample:
        return self.read_sample(index)

    def read_sample(self, index: int) -> MdsSample:
        location = self.sample_location(index)
        return self._readers[location.shard_index].read_location(location)

    def sample_location(self, index: int) -> MdsSampleLocation:
        normalized = self._normalize_index(index)
        shard_index = bisect_right(self._prefix, normalized) - 1
        local_index = normalized - self._prefix[shard_index]
        return self._readers[shard_index].sample_location(local_index, normalized, shard_index)

    def sample_locations(self, indices: Sequence[int]) -> tuple[MdsSampleLocation, ...]:
        return tuple(self.sample_location(index) for index in indices)

    def read_spans(self, indices: Sequence[int]) -> tuple[MdsReadSpan, ...]:
        locations = self.sample_locations(indices)
        return _coalesce_locations(locations)

    def plan_data_reads(
        self,
        indices: Sequence[int],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDataReadPlan, ...]:
        locations = self.sample_locations(indices)
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if not locations:
            return tuple()

        def plan_one(location: MdsSampleLocation) -> MdsDataReadPlan:
            reader = self._readers[location.shard_index]
            data_size, path_size = reader.sample_header(location)
            path = reader.sample_path(location, data_size, path_size) if include_paths else None
            return _data_read_plan(location, location.offset + 8, data_size, path)

        if len(locations) <= 1 or max_workers == 1:
            return tuple(plan_one(location) for location in locations)
        workers = min(max_workers, len(locations))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return tuple(executor.map(plan_one, locations))

    def plan_batch_data_reads(
        self,
        batch: MdsBatch,
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDataReadPlan, ...]:
        return self.plan_data_reads(
            batch.indices,
            max_workers=max_workers,
            include_paths=include_paths,
        )

    def batch_indices(
        self,
        batch_size: int,
        *,
        indices: Sequence[int] | None = None,
        shuffle: bool = False,
        seed: int | None = None,
        drop_last: bool = False,
    ) -> tuple[tuple[int, ...], ...]:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        normalized = (
            list(range(len(self)))
            if indices is None
            else [self._normalize_index(index) for index in indices]
        )
        if shuffle:
            random.Random(seed).shuffle(normalized)
        batches = []
        for start in range(0, len(normalized), batch_size):
            batch = tuple(normalized[start:start + batch_size])
            if len(batch) < batch_size and drop_last:
                continue
            if batch:
                batches.append(batch)
        return tuple(batches)

    def plan_batches(
        self,
        batch_size: int,
        *,
        indices: Sequence[int] | None = None,
        shuffle: bool = False,
        seed: int | None = None,
        drop_last: bool = False,
    ) -> tuple[MdsBatch, ...]:
        batches = self.batch_indices(
            batch_size,
            indices=indices,
            shuffle=shuffle,
            seed=seed,
            drop_last=drop_last,
        )
        return tuple(
            MdsBatch(batch_index, batch, self.read_spans(batch))
            for batch_index, batch in enumerate(batches)
        )

    def read_samples(
        self,
        indices: Sequence[int],
        max_workers: int = 16,
        coalesce: bool = True,
    ) -> tuple[MdsSample, ...]:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        locations = self.sample_locations(indices)
        if not locations:
            return tuple()
        spans = _coalesce_locations(locations) if coalesce else _single_location_spans(locations)
        results: list[MdsSample | None] = [None] * len(locations)
        if len(spans) <= 1 or max_workers == 1:
            for span in spans:
                for position, sample in self._read_span(span):
                    results[position] = sample
        else:
            workers = min(max_workers, len(spans))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for span_result in executor.map(self._read_span, spans):
                    for position, sample in span_result:
                        results[position] = sample
        return tuple(_require_sample(sample) for sample in results)

    def read_samples_uncollated(self, indices: Sequence[int], max_workers: int = 16) -> tuple[MdsSample, ...]:
        return self.read_samples(indices, max_workers=max_workers, coalesce=False)

    def read_batch(self, batch: MdsBatch, max_workers: int = 16) -> tuple[MdsSample, ...]:
        return self.read_samples(batch.indices, max_workers=max_workers)

    def read_data_into_buffers(
        self,
        indices: Sequence[int],
        destinations: Sequence[object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDirectSample, ...]:
        locations = self.sample_locations(indices)
        self._validate_direct_read_inputs(locations, destinations, max_workers)
        if not locations:
            return tuple()

        def read_one(pair) -> MdsDirectSample:
            location, destination = pair
            return self._readers[location.shard_index].read_location_data_into_buffer(
                location,
                destination,
                include_path=include_paths,
            )

        if len(locations) <= 1 or max_workers == 1:
            return tuple(read_one(pair) for pair in zip(locations, destinations))
        workers = min(max_workers, len(locations))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return tuple(executor.map(read_one, zip(locations, destinations)))

    def read_batch_data_into_buffers(
        self,
        batch: MdsBatch,
        destinations: Sequence[object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDirectSample, ...]:
        return self.read_data_into_buffers(
            batch.indices,
            destinations,
            max_workers=max_workers,
            include_paths=include_paths,
        )

    def read_data_with_buffer_factory(
        self,
        indices: Sequence[int],
        destination_factory: Callable[[MdsSampleLocation, int], object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDirectBufferSample, ...]:
        if destination_factory is None:
            raise ValueError("destination_factory is required")
        locations = self.sample_locations(indices)
        self._validate_direct_factory_inputs(locations, max_workers)
        if not locations:
            return tuple()

        def read_one(location: MdsSampleLocation) -> MdsDirectBufferSample:
            reader = self._readers[location.shard_index]
            data_size, path_size = reader.sample_header(location)
            destination = destination_factory(location, data_size)
            metadata = reader._read_location_data_into_buffer_with_header(
                location,
                destination,
                data_size,
                path_size,
                include_path=include_paths,
            )
            return MdsDirectBufferSample(metadata, destination)

        if len(locations) <= 1 or max_workers == 1:
            return tuple(read_one(location) for location in locations)
        workers = min(max_workers, len(locations))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return tuple(executor.map(read_one, locations))

    def read_batch_data_with_buffer_factory(
        self,
        batch: MdsBatch,
        destination_factory: Callable[[MdsSampleLocation, int], object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
    ) -> tuple[MdsDirectBufferSample, ...]:
        return self.read_data_with_buffer_factory(
            batch.indices,
            destination_factory,
            max_workers=max_workers,
            include_paths=include_paths,
        )

    def read_data_with_tensor_factory(
        self,
        indices: Sequence[int],
        tensor_factory: Callable[[MdsSampleLocation, int], object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
        non_blocking: bool = True,
    ) -> tuple[MdsDirectTensorSample, ...]:
        if tensor_factory is None:
            raise ValueError("tensor_factory is required")
        locations = self.sample_locations(indices)
        self._validate_direct_factory_inputs(locations, max_workers)
        if not locations:
            return tuple()

        def read_one(location: MdsSampleLocation) -> MdsDirectTensorSample:
            reader = self._readers[location.shard_index]
            data_size, path_size = reader.sample_header(location)
            tensor = tensor_factory(location, data_size)
            metadata = reader._read_location_data_into_tensor_with_header(
                location,
                tensor,
                data_size,
                path_size,
                include_path=include_paths,
                non_blocking=non_blocking,
            )
            return MdsDirectTensorSample(metadata, tensor)

        if len(locations) <= 1 or max_workers == 1:
            return tuple(read_one(location) for location in locations)
        workers = min(max_workers, len(locations))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return tuple(executor.map(read_one, locations))

    def read_batch_data_with_tensor_factory(
        self,
        batch: MdsBatch,
        tensor_factory: Callable[[MdsSampleLocation, int], object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
        non_blocking: bool = True,
    ) -> tuple[MdsDirectTensorSample, ...]:
        return self.read_data_with_tensor_factory(
            batch.indices,
            tensor_factory,
            max_workers=max_workers,
            include_paths=include_paths,
            non_blocking=non_blocking,
        )

    def read_data_into_tensors(
        self,
        indices: Sequence[int],
        tensors: Sequence[object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
        non_blocking: bool = True,
    ) -> tuple[MdsDirectSample, ...]:
        locations = self.sample_locations(indices)
        self._validate_direct_read_inputs(locations, tensors, max_workers)
        if not locations:
            return tuple()

        def read_one(pair) -> MdsDirectSample:
            location, tensor = pair
            return self._readers[location.shard_index].read_location_data_into_tensor(
                location,
                tensor,
                include_path=include_paths,
                non_blocking=non_blocking,
            )

        if len(locations) <= 1 or max_workers == 1:
            return tuple(read_one(pair) for pair in zip(locations, tensors))
        workers = min(max_workers, len(locations))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return tuple(executor.map(read_one, zip(locations, tensors)))

    def read_batch_data_into_tensors(
        self,
        batch: MdsBatch,
        tensors: Sequence[object],
        *,
        max_workers: int = 16,
        include_paths: bool = False,
        non_blocking: bool = True,
    ) -> tuple[MdsDirectSample, ...]:
        return self.read_data_into_tensors(
            batch.indices,
            tensors,
            max_workers=max_workers,
            include_paths=include_paths,
            non_blocking=non_blocking,
        )

    def prefetch(
        self,
        indices: Sequence[int],
        max_workers: int = 16,
        coalesce: bool = True,
    ) -> MdsPrefetchResult:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        locations = self.sample_locations(indices)
        if not locations:
            return MdsPrefetchResult(tuple(), 0, 0, 0, 0)
        spans = _coalesce_locations(locations) if coalesce else _single_location_spans(locations)
        if len(spans) <= 1 or max_workers == 1:
            fetched = tuple(self._prefetch_span(span) for span in spans)
        else:
            workers = min(max_workers, len(spans))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                fetched = tuple(executor.map(self._prefetch_span, spans))
        ranges_fetched = sum(1 for value in fetched if value > 0)
        bytes_fetched = sum(fetched)
        return MdsPrefetchResult(
            spans,
            len(locations),
            sum(span.length for span in spans),
            ranges_fetched,
            bytes_fetched,
        )

    def prefetch_batch(self, batch: MdsBatch, max_workers: int = 16) -> MdsPrefetchResult:
        return self._prefetch_spans(batch.spans, batch.sample_count, max_workers)

    def prefetch_batches(
        self,
        batches: Sequence[MdsBatch],
        max_workers: int = 16,
    ) -> tuple[MdsPrefetchResult, ...]:
        return tuple(self.prefetch_batch(batch, max_workers=max_workers) for batch in batches)

    def _read_span(self, span: MdsReadSpan) -> tuple[tuple[int, MdsSample], ...]:
        payload = self.client.read_mds_shard(span.shard_name, span.offset, span.length)
        if len(payload) != span.length:
            raise ValueError("short MDS span read")
        samples = []
        for position, location in zip(span.result_positions, span.locations):
            start = location.offset - span.offset
            end = start + location.length
            samples.append((position, _decode_sample_payload(payload[start:end], location)))
        return tuple(samples)

    def _prefetch_spans(
        self,
        spans: Sequence[MdsReadSpan],
        sample_count: int,
        max_workers: int,
    ) -> MdsPrefetchResult:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if not spans:
            return MdsPrefetchResult(tuple(), sample_count, 0, 0, 0)
        if len(spans) <= 1 or max_workers == 1:
            fetched = tuple(self._prefetch_span(span) for span in spans)
        else:
            workers = min(max_workers, len(spans))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                fetched = tuple(executor.map(self._prefetch_span, spans))
        ranges_fetched = sum(1 for value in fetched if value > 0)
        bytes_fetched = sum(fetched)
        return MdsPrefetchResult(
            tuple(spans),
            sample_count,
            sum(span.length for span in spans),
            ranges_fetched,
            bytes_fetched,
        )

    def _prefetch_span(self, span: MdsReadSpan) -> int:
        if isinstance(self.client, CxnfsRangeCacheClient):
            return span.length if self.client.prefetch_mds_shard(span.shard_name, span.offset, span.length) else 0
        data = self.client.read_mds_shard(span.shard_name, span.offset, span.length)
        if len(data) != span.length:
            raise ValueError("short MDS span prefetch")
        return span.length

    def _read_location(self, location: MdsSampleLocation) -> MdsSample:
        return self._readers[location.shard_index].read_location(location)

    @staticmethod
    def _validate_direct_read_inputs(
        locations: Sequence[MdsSampleLocation],
        destinations: Sequence[object],
        max_workers: int,
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if len(locations) != len(destinations):
            raise ValueError("destinations must match requested indices")

    @staticmethod
    def _validate_direct_factory_inputs(
        locations: Sequence[MdsSampleLocation],
        max_workers: int,
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")

    def _normalize_index(self, index: int) -> int:
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("MDS sample index out of range")
        return index

    def iter_samples(self) -> Iterator[MdsSample]:
        for index in range(len(self)):
            yield self.read_sample(index)

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if callable(close):
            close()

    def descriptor_cache_evidence(self) -> dict[str, Any]:
        shards = tuple(reader.descriptor_cache_evidence() for reader in self._readers)
        header_entries = sum(int(shard["sample_header_cache_entries"]) for shard in shards)
        header_hits = sum(int(shard["sample_header_cache_hits"]) for shard in shards)
        header_misses = sum(int(shard["sample_header_cache_misses"]) for shard in shards)
        path_entries = sum(int(shard["sample_path_cache_entries"]) for shard in shards)
        path_hits = sum(int(shard["sample_path_cache_hits"]) for shard in shards)
        path_misses = sum(int(shard["sample_path_cache_misses"]) for shard in shards)
        return {
            "format": "cxnfs.mds-descriptor-cache.v1",
            "shard_count": len(shards),
            "sample_header_cache_entries": header_entries,
            "sample_header_cache_hits": header_hits,
            "sample_header_cache_misses": header_misses,
            "sample_header_cache_reuse_observed": header_hits > 0,
            "sample_path_cache_entries": path_entries,
            "sample_path_cache_hits": path_hits,
            "sample_path_cache_misses": path_misses,
            "sample_path_cache_reuse_observed": path_hits > 0,
            "shards": [dict(shard) for shard in shards],
        }

    def __enter__(self) -> MdsDataset:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


def _prefix_counts(shards: Sequence[MdsShardMetadata]) -> tuple[int, ...]:
    prefix = [0]
    total = 0
    for shard in shards:
        total += shard.sample_count
        prefix.append(total)
    return tuple(prefix)


def _coalesce_locations(locations: Sequence[MdsSampleLocation]) -> tuple[MdsReadSpan, ...]:
    sorted_locations = sorted(
        enumerate(locations),
        key=lambda item: (item[1].shard_index, item[1].offset, item[0]),
    )
    spans: list[MdsReadSpan] = []
    current_positions: list[int] = []
    current_locations: list[MdsSampleLocation] = []
    current_offset = 0
    current_end = 0
    current_shard_index = -1
    current_shard_name = ""

    for position, location in sorted_locations:
        adjacent = (
            current_locations
            and location.shard_index == current_shard_index
            and location.offset == current_end
        )
        if not adjacent:
            if current_locations:
                spans.append(_span(current_shard_index, current_shard_name, current_offset, current_end, current_locations, current_positions))
            current_positions = [position]
            current_locations = [location]
            current_offset = location.offset
            current_end = location.offset + location.length
            current_shard_index = location.shard_index
            current_shard_name = location.shard_name
        else:
            current_positions.append(position)
            current_locations.append(location)
            current_end += location.length

    if current_locations:
        spans.append(_span(current_shard_index, current_shard_name, current_offset, current_end, current_locations, current_positions))
    return tuple(spans)


def _single_location_spans(locations: Sequence[MdsSampleLocation]) -> tuple[MdsReadSpan, ...]:
    return tuple(
        MdsReadSpan(
            location.shard_index,
            location.shard_name,
            location.offset,
            location.length,
            (location,),
            (position,),
        )
        for position, location in enumerate(locations)
    )


def _span(
    shard_index: int,
    shard_name: str,
    offset: int,
    end: int,
    locations: Sequence[MdsSampleLocation],
    result_positions: Sequence[int],
) -> MdsReadSpan:
    return MdsReadSpan(
        shard_index,
        shard_name,
        offset,
        end - offset,
        tuple(locations),
        tuple(result_positions),
    )


def _require_sample(sample: MdsSample | None) -> MdsSample:
    if sample is None:
        raise RuntimeError("MDS sample read did not produce a result")
    return sample


def _read_uint32_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset:offset + 4], "little", signed=False)


def _read_sample_header(client: CxnfsDatasetClient, location: MdsSampleLocation) -> tuple[int, int]:
    header = client.read_mds_shard(location.shard_name, location.offset, 8)
    if len(header) != 8:
        raise ValueError("short MDS sample header read")
    data_size = _read_uint32_le(header, 0)
    path_size = _read_uint32_le(header, 4)
    if 8 + data_size + path_size != location.length:
        raise ValueError("invalid MDS sample payload size")
    return data_size, path_size


def _read_sample_path(
    client: CxnfsDatasetClient,
    location: MdsSampleLocation,
    data_size: int,
    path_size: int,
) -> str:
    path = client.read_mds_shard(location.shard_name, location.offset + 8 + data_size, path_size)
    if len(path) != path_size:
        raise ValueError("short MDS sample path read")
    return path.decode("utf-8")


def _direct_sample(
    location: MdsSampleLocation,
    data_offset: int,
    data_size: int,
    bytes_read: int,
    path: str | None,
) -> MdsDirectSample:
    return MdsDirectSample(
        location.global_index,
        location.shard_index,
        location.local_index,
        location.shard_name,
        data_offset,
        data_size,
        bytes_read,
        path,
    )


def _data_read_plan(
    location: MdsSampleLocation,
    data_offset: int,
    data_size: int,
    path: str | None,
) -> MdsDataReadPlan:
    return MdsDataReadPlan(
        location.global_index,
        location.shard_index,
        location.local_index,
        location.shard_name,
        data_offset,
        data_size,
        path,
    )


def _read_client_into_buffer(
    client: CxnfsDatasetClient,
    shard_name: str,
    offset: int,
    destination,
    nbytes: int,
) -> int:
    method = getattr(client, "read_into_buffer", None)
    if callable(method):
        return int(method(shard_name, offset, destination, nbytes))
    payload = client.read_mds_shard(shard_name, offset, nbytes)
    if len(payload) != nbytes:
        raise ValueError("short MDS data read")
    view = _writable_byte_view(destination)
    view[:nbytes] = payload
    return nbytes


def _read_client_into_tensor(
    client: CxnfsDatasetClient,
    shard_name: str,
    offset: int,
    tensor,
    nbytes: int,
    non_blocking: bool,
) -> int:
    method = getattr(client, "read_into_tensor", None)
    if callable(method):
        return int(method(shard_name, offset, tensor, nbytes, None, None, non_blocking))
    return _read_client_into_buffer(client, shard_name, offset, tensor, nbytes)


def _writable_byte_view(destination) -> memoryview:
    try:
        view = memoryview(destination)
    except TypeError:
        view = tensor_writable_memoryview(destination)
    if view.readonly:
        raise ValueError("destination buffer must be writable")
    return view if view.format == "B" and view.ndim == 1 else view.cast("B")


def _decode_sample_payload(payload: bytes, location: MdsSampleLocation) -> MdsSample:
    data_size = _read_uint32_le(payload, 0)
    path_size = _read_uint32_le(payload, 4)
    data_start = 8
    path_start = data_start + data_size
    path_end = path_start + path_size
    if path_end != len(payload):
        raise ValueError("invalid MDS sample payload size")
    return MdsSample(
        location.global_index,
        location.shard_index,
        location.local_index,
        location.shard_name,
        payload[path_start:path_end].decode("utf-8"),
        payload[data_start:path_start],
    )


def _local_client(root: str | Path) -> CxnfsDatasetClient:
    from .local import LocalFileTransport

    return CxnfsDatasetClient(LocalFileTransport(root))
