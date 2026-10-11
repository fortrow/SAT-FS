from __future__ import annotations

import base64
import datetime as _datetime
import decimal
import io
import json
import ssl
import sys
import tarfile
from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence, TextIO

from .auth import ClientCredentials, prompt_client_credentials
from .bulk import ABSOLUTE_MAX_CORDS, CxnfsBulkReadConfig, CxnfsLengthPrefixedBulkClient
from .control import CxnfsControlClient
from .tls import create_client_tls_context

MDS_SHARDS = "mds-shards"
DEFAULT_TARGET_SHARD_SIZE_BYTES = 256 * 1024 * 1024

SOURCE_EXTENSIONS = {
    "files": (),
    "hdf5": (".h5", ".hdf5"),
    "tfrecord": (".tfrecord", ".tfrecords"),
    "webdataset": (".tar", ".tgz", ".tar.gz"),
    "parquet": (".parquet",),
}


@dataclass(frozen=True)
class SourceFile:
    relative_path: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.relative_path:
            raise ValueError("relative_path is required")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")


@dataclass(frozen=True)
class MdsShardPlanEntry:
    shard_name: str
    source_files: tuple[SourceFile, ...]
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if not self.source_files:
            raise ValueError("source_files are required")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")


@dataclass(frozen=True)
class PreparedSample:
    path: str
    data: bytes

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        if not isinstance(self.data, bytes):
            raise TypeError("data must be bytes")

    @property
    def encoded_size(self) -> int:
        return 8 + len(self.data) + len(self.path.encode("utf-8"))


@dataclass(frozen=True)
class MdsShardArtifact:
    shard_name: str
    path: str
    sample_count: int
    byte_size: int

    def __post_init__(self) -> None:
        if not self.shard_name:
            raise ValueError("shard_name is required")
        if not self.path:
            raise ValueError("path is required")
        if self.sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        if self.byte_size < 0:
            raise ValueError("byte_size must be non-negative")


@dataclass(frozen=True)
class DatasetConversionResult:
    output_dir: str
    index_path: str
    shard_count: int
    sample_count: int
    bytes_written: int
    shards: tuple[MdsShardArtifact, ...]

    def __post_init__(self) -> None:
        if not self.output_dir:
            raise ValueError("output_dir is required")
        if not self.index_path:
            raise ValueError("index_path is required")
        if self.shard_count != len(self.shards):
            raise ValueError("shard_count must match shards")
        if self.sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        if self.bytes_written < 0:
            raise ValueError("bytes_written must be non-negative")


@dataclass(frozen=True)
class DatasetPreparationPlan:
    input_dir: str
    dataset_name: str
    source_format: str
    target_format: str
    target_shard_size_bytes: int
    source_files: tuple[SourceFile, ...]
    shards: tuple[MdsShardPlanEntry, ...]

    def __post_init__(self) -> None:
        if not self.input_dir:
            raise ValueError("input_dir is required")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if self.source_format not in SOURCE_EXTENSIONS:
            raise ValueError(f"unknown source_format: {self.source_format}")
        if self.target_format != MDS_SHARDS:
            raise ValueError("target_format must be mds-shards")
        if self.target_shard_size_bytes <= 0:
            raise ValueError("target_shard_size_bytes must be positive")

    @property
    def total_source_bytes(self) -> int:
        return sum(source.size_bytes for source in self.source_files)

    def to_manifest(self) -> dict:
        return {
            "input_dir": self.input_dir,
            "dataset_name": self.dataset_name,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "target_shard_size_bytes": self.target_shard_size_bytes,
            "total_source_bytes": self.total_source_bytes,
            "source_files": [
                {"relative_path": source.relative_path, "size_bytes": source.size_bytes}
                for source in self.source_files
            ],
            "shards": [
                {
                    "shard_name": shard.shard_name,
                    "size_bytes": shard.size_bytes,
                    "source_files": [
                        {"relative_path": source.relative_path, "size_bytes": source.size_bytes}
                        for source in shard.source_files
                    ],
                }
                for shard in self.shards
            ],
        }

    def write_manifest(self, output_path: str | Path) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_manifest(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path


class DatasetPreparer:
    def __init__(self, target_shard_size_bytes: int = DEFAULT_TARGET_SHARD_SIZE_BYTES) -> None:
        if target_shard_size_bytes <= 0:
            raise ValueError("target_shard_size_bytes must be positive")
        self.target_shard_size_bytes = target_shard_size_bytes

    def plan(
        self,
        input_dir: str | Path,
        dataset_name: str,
        source_format: str = "files",
        target_format: str = MDS_SHARDS,
    ) -> DatasetPreparationPlan:
        if source_format not in SOURCE_EXTENSIONS:
            raise ValueError(f"unknown source_format: {source_format}")
        if target_format != MDS_SHARDS:
            raise ValueError("target_format must be mds-shards")

        root = Path(input_dir)
        if not root.is_dir():
            raise FileNotFoundError(f"input directory does not exist: {root}")

        sources = tuple(inventory_sources(root, source_format))
        if not sources:
            raise ValueError(f"no {source_format} source files found in {root}")

        return DatasetPreparationPlan(
            str(root),
            dataset_name,
            source_format,
            target_format,
            self.target_shard_size_bytes,
            sources,
            tuple(group_sources_into_shards(dataset_name, sources, self.target_shard_size_bytes)),
        )


class DatasetConverter:
    """Convert source records into the locked MDS-Shards read target."""

    def execute(self, plan: DatasetPreparationPlan, output_dir: str | Path) -> DatasetConversionResult:
        root = Path(plan.input_dir)
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)

        writer = MdsShardWriter(plan.dataset_name, plan.target_shard_size_bytes)
        artifacts: list[MdsShardArtifact] = []
        current: list[PreparedSample] = []
        current_size = 0

        for sample in iter_prepared_samples(root, plan.source_format, plan.source_files):
            if current and current_size + sample.encoded_size > plan.target_shard_size_bytes:
                artifacts.append(writer.write(output, len(artifacts), current))
                current = []
                current_size = 0
            current.append(sample)
            current_size += sample.encoded_size

        if current:
            artifacts.append(writer.write(output, len(artifacts), current))

        if not artifacts:
            raise ValueError("no samples produced for conversion")

        index_path = write_mds_index(output, plan.target_shard_size_bytes, artifacts)
        bytes_written = sum(shard.byte_size for shard in artifacts) + index_path.stat().st_size
        return DatasetConversionResult(
            str(output),
            str(index_path),
            len(artifacts),
            sum(shard.sample_count for shard in artifacts),
            bytes_written,
            tuple(artifacts),
        )


class MdsShardWriter:
    def __init__(self, dataset_name: str, target_shard_size_bytes: int) -> None:
        if not dataset_name:
            raise ValueError("dataset_name is required")
        if target_shard_size_bytes <= 0:
            raise ValueError("target_shard_size_bytes must be positive")
        self.dataset_name = dataset_name
        self.target_shard_size_bytes = target_shard_size_bytes

    def write(self, output_dir: Path, shard_index: int, samples: Sequence[PreparedSample]) -> MdsShardArtifact:
        if not samples:
            raise ValueError("samples are required")
        shard_name = f"{self.dataset_name}-{shard_index:06d}.mds"
        shard_path = output_dir / shard_name
        config = _shard_config(self.target_shard_size_bytes).encode("utf-8")
        offsets = _sample_offsets(samples, len(config))

        with shard_path.open("wb") as handle:
            _write_uint32_le(handle, len(samples))
            for offset in offsets:
                _write_uint32_le(handle, offset)
            handle.write(config)
            for sample in samples:
                path_bytes = sample.path.encode("utf-8")
                _write_uint32_le(handle, len(sample.data))
                _write_uint32_le(handle, len(path_bytes))
                handle.write(sample.data)
                handle.write(path_bytes)

        return MdsShardArtifact(shard_name, str(shard_path), len(samples), shard_path.stat().st_size)


def iter_prepared_samples(
    root: Path,
    source_format: str,
    sources: Iterable[SourceFile],
) -> Iterator[PreparedSample]:
    if source_format == "parquet":
        yield from iter_parquet_row_samples(root, sources)
    elif source_format == "webdataset":
        yield from iter_webdataset_samples(root, sources)
    else:
        yield from iter_file_samples(root, sources)


def iter_file_samples(root: Path, sources: Iterable[SourceFile]) -> Iterator[PreparedSample]:
    for source in sources:
        yield PreparedSample(source.relative_path, _source_path(root, source.relative_path).read_bytes())


def iter_parquet_row_samples(root: Path, sources: Iterable[SourceFile], batch_size: int = 1024) -> Iterator[PreparedSample]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - exercised only without optional dependency
        raise RuntimeError("row-level parquet conversion requires pyarrow") from exc

    for source in sources:
        path = _source_path(root, source.relative_path)
        parquet_file = pq.ParquetFile(path)
        row_number = 0
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            for row in batch.to_pylist():
                data = json.dumps(
                    _json_ready(row),
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                yield PreparedSample(f"{source.relative_path}#row{row_number:012d}", data)
                row_number += 1


def iter_webdataset_samples(root: Path, sources: Iterable[SourceFile]) -> Iterator[PreparedSample]:
    for source in sources:
        grouped: dict[str, list[tuple[str, bytes]]] = {}
        with tarfile.open(_source_path(root, source.relative_path), "r:*") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                key = _webdataset_key(member.name)
                grouped.setdefault(key, []).append((member.name, extracted.read()))
        for key in sorted(grouped):
            yield PreparedSample(f"{source.relative_path}#{key}", _webdataset_sample_tar(grouped[key]))


def inventory_sources(root: Path, source_format: str) -> list[SourceFile]:
    extensions = SOURCE_EXTENSIONS[source_format]
    sources = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(root).as_posix()
        if matches_source_format(relative_path, extensions):
            sources.append(SourceFile(relative_path, path.stat().st_size))
    return sources


def matches_source_format(relative_path: str, extensions: tuple[str, ...]) -> bool:
    if not extensions:
        return True
    lowered = relative_path.lower()
    return any(lowered.endswith(extension) for extension in extensions)


def group_sources_into_shards(
    dataset_name: str,
    sources: Iterable[SourceFile],
    target_shard_size_bytes: int,
) -> list[MdsShardPlanEntry]:
    shards = []
    current: list[SourceFile] = []
    current_size = 0

    for source in sources:
        if current and current_size + source.size_bytes > target_shard_size_bytes:
            shards.append(make_shard(dataset_name, len(shards), current, current_size))
            current = []
            current_size = 0
        current.append(source)
        current_size += source.size_bytes

    if current:
        shards.append(make_shard(dataset_name, len(shards), current, current_size))
    return shards


def make_shard(dataset_name: str, index: int, sources: list[SourceFile], size_bytes: int) -> MdsShardPlanEntry:
    return MdsShardPlanEntry(f"{dataset_name}-{index:06d}.mds", tuple(sources), size_bytes)


def write_mds_index(
    output_dir: Path,
    target_shard_size_bytes: int,
    artifacts: Sequence[MdsShardArtifact],
) -> Path:
    index = {
        "version": 2,
        "shards": [
            {
                "version": 2,
                "format": "mds",
                "compression": None,
                "hashes": [],
                "size_limit": target_shard_size_bytes,
                "column_names": ["data", "path"],
                "column_encodings": ["bytes", "str"],
                "column_sizes": [None, None],
                "samples": artifact.sample_count,
                "raw_data": {
                    "basename": artifact.shard_name,
                    "bytes": artifact.byte_size,
                    "hashes": {},
                },
                "zip_data": None,
            }
            for artifact in artifacts
        ],
    }
    index_path = output_dir / "index.json"
    index_path.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return index_path


def _shard_config(target_shard_size_bytes: int) -> str:
    return json.dumps(
        {
            "column_encodings": ["bytes", "str"],
            "column_names": ["data", "path"],
            "column_sizes": [None, None],
            "compression": None,
            "format": "mds",
            "hashes": [],
            "size_limit": target_shard_size_bytes,
            "version": 2,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _sample_offsets(samples: Sequence[PreparedSample], config_size: int) -> list[int]:
    next_offset = 4 + (4 * (len(samples) + 1)) + config_size
    offsets = [next_offset]
    for sample in samples:
        next_offset += sample.encoded_size
        offsets.append(next_offset)
    return offsets


def _write_uint32_le(handle: Any, value: int) -> None:
    if value < 0 or value > 0xFFFFFFFF:
        raise ValueError(f"value exceeds uint32 MDS limit: {value}")
    handle.write(value.to_bytes(4, "little", signed=False))


def _source_path(root: Path, relative_path: str) -> Path:
    path = (root / relative_path).resolve()
    resolved_root = root.resolve()
    if resolved_root != path and resolved_root not in path.parents:
        raise ValueError(f"source file escapes input directory: {relative_path}")
    return path


def _webdataset_key(member_name: str) -> str:
    name = member_name.lstrip("./")
    return name.rsplit(".", 1)[0] if "." in name else name


def _webdataset_sample_tar(members: Sequence[tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, payload in sorted(members, key=lambda item: item[0]):
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, bytes):
        return {"__bytes_base64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (_datetime.datetime, _datetime.date, _datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if hasattr(value, "as_py"):
        return _json_ready(value.as_py())
    if hasattr(value, "item"):
        try:
            return _json_ready(value.item())
        except Exception:
            pass
    return value


def run(
    argv: Sequence[str] | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
    *,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
) -> int:
    output = out if out is not None else sys.stdout
    error = err if err is not None else sys.stderr
    parser = _argument_parser(error)
    try:
        try:
            args = parser.parse_args(list(argv) if argv is not None else None)
        except SystemExit as exc:
            return int(exc.code) if isinstance(exc.code, int) else 2
        if args.command == "send":
            return _run_send(
                args,
                output,
                credential_prompt=credential_prompt,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
            )
        plan = DatasetPreparer(args.target_shard_size_bytes).plan(
            args.input_dir,
            args.dataset_name,
            args.source_format,
            args.format,
        )
        _print_plan(plan, output)
        if args.plan_manifest:
            manifest_path = plan.write_manifest(args.plan_manifest)
            print(f"conversion_manifest={manifest_path}", file=output)
        if args.command == "execute":
            result = DatasetConverter().execute(plan, args.output_dir)
            print(f"converted_shards={result.shard_count}", file=output)
            print(f"converted_samples={result.sample_count}", file=output)
            print(f"converted_bytes={result.bytes_written}", file=output)
            print(f"dataset_index={result.index_path}", file=output)
        return 0
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
        print(str(exc), file=error)
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


def _argument_parser(error_output: TextIO | None = None) -> ArgumentParser:
    parser = _CxnfsArgumentParser(prog="cxnfs-prepare", error_output=error_output)
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        parser_class=lambda *args, **kwargs: _CxnfsArgumentParser(
            *args,
            error_output=error_output,
            **kwargs,
        ),
    )
    _add_common_prepare_args(subparsers.add_parser("plan", help="write or print a dataset conversion plan"))
    execute = subparsers.add_parser("execute", help="convert source records into MDS-Shards")
    _add_common_prepare_args(execute)
    execute.add_argument("--output-dir", required=True, help="directory for generated .mds shards and index.json")
    send = subparsers.add_parser("send", help="send Safetensors checkpoint output through native CXNFS writes")
    send.add_argument("output_dir", help="local checkpoint output directory")
    send.add_argument("--remote-root", required=True, help="remote CXNFS namespace root to publish under")
    send.add_argument("--host", required=True, help="CXNFS fileserver host")
    send.add_argument("--control-port", type=int, default=4509)
    send.add_argument("--transport-port", type=int, default=4510)
    send.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    send.add_argument("--cafile", help="optional CA bundle for the server certificate")
    send.add_argument("--timeout", type=float, default=10.0)
    send.add_argument("--initial-cords", type=int, default=1)
    send.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    send.add_argument("--provisional-container-size", dest="provisional_container_size_bytes", type=int, default=64 * 1024 * 1024)
    send.add_argument("--expected-size-limit", dest="expected_size_limit_bytes", type=int, default=0)
    return parser


def _add_common_prepare_args(parser: ArgumentParser) -> None:
    parser.add_argument("input_dir")
    parser.add_argument("dataset_name")
    parser.add_argument("--source-format", default="files", choices=tuple(SOURCE_EXTENSIONS))
    parser.add_argument("--format", default=MDS_SHARDS, choices=(MDS_SHARDS,))
    parser.add_argument("--target-shard-size", dest="target_shard_size_bytes", type=int, default=DEFAULT_TARGET_SHARD_SIZE_BYTES)
    parser.add_argument("--plan-manifest", help="optional path for a JSON conversion plan")


def _print_plan(plan: DatasetPreparationPlan, output: TextIO) -> None:
    print("dataset prepare plan", file=output)
    print(f"input_dir={plan.input_dir}", file=output)
    print(f"dataset_name={plan.dataset_name}", file=output)
    print(f"source_format={plan.source_format}", file=output)
    print(f"target_format={plan.target_format}", file=output)
    print(f"target_shard_size_bytes={plan.target_shard_size_bytes}", file=output)
    print(f"source_files={len(plan.source_files)}", file=output)
    print(f"planned_shards={len(plan.shards)}", file=output)


class _CxnfsArgumentParser(ArgumentParser):
    def __init__(self, *args: Any, error_output: TextIO | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._error_output = error_output

    def error(self, message: str) -> None:
        output = self._error_output if self._error_output is not None else sys.stderr
        self.print_usage(output)
        print(f"{self.prog}: error: {message}", file=output)
        raise SystemExit(2)


def _run_send(
    args: Any,
    output: TextIO,
    *,
    credential_prompt: Callable[[], ClientCredentials],
    control_client_factory: Callable[..., Any],
    bulk_client_factory: Callable[..., Any],
) -> int:
    tls_context = _client_tls_context(args.cafile)
    server_hostname = args.server_hostname or args.host
    read_config = CxnfsBulkReadConfig(
        initial_cords=args.initial_cords,
        max_cords=args.max_cords,
        provisional_container_size_bytes=args.provisional_container_size_bytes,
        expected_size_limit_bytes=args.expected_size_limit_bytes,
    )
    with credential_prompt() as credentials:
        with control_client_factory(
            args.host,
            args.control_port,
            timeout=args.timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
        ) as control:
            accepted = control.authenticate(credentials)
            with bulk_client_factory(
                control,
                args.host,
                args.transport_port,
                timeout=args.timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                read_config=read_config,
            ) as bulk_client:
                result = bulk_client.send_safetensors_output(args.output_dir, remote_root=args.remote_root)

    print("dataset send result", file=output)
    print(f"host={args.host}", file=output)
    print(f"control_port={args.control_port}", file=output)
    print(f"transport_port={args.transport_port}", file=output)
    print(f"remote_root={args.remote_root}", file=output)
    print(f"session_id={accepted.session_id}", file=output)
    print(f"files_sent={result.file_count}", file=output)
    print(f"tensor_files_sent={result.tensor_file_count}", file=output)
    if result.bytes_sent is not None:
        print(f"bytes_sent={result.bytes_sent}", file=output)
    return 0


def _client_tls_context(cafile: str | None) -> ssl.SSLContext:
    return create_client_tls_context(cafile)


if __name__ == "__main__":
    raise SystemExit(main())
