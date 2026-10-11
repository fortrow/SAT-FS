from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
import time
from typing import Any, Callable, TextIO

from .auth import ClientCredentials, prompt_client_credentials
from .bulk import (
    ABSOLUTE_MAX_CORDS,
    CxnfsBulkReadConfig,
    CxnfsLengthPrefixedBulkClient,
    CxnfsQuicBulkClient,
    PYTHON_BULK_TRANSPORT_QUIC,
    PYTHON_BULK_TRANSPORT_STREAM,
    normalize_python_bulk_transport,
    require_supported_python_bulk_transport,
)
from .control import CxnfsControlClient, FileType
from .tls import create_client_tls_context


READ_TO_EOF = -1
DEFAULT_ITERATIONS = 3
DEFAULT_WARMUP_ITERATIONS = 1
DEFAULT_BLOCK_SIZE_BYTES = 8 * 1024 * 1024
AI_TRAINING_25GBE_OBJECTIVE = "ai-training-25gbe"
AI_TRAINING_BASELINE_NAME = "legacy-mounted-share"
AI_TRAINING_BOTTLENECK_GBIT_S = 2.5
AI_TRAINING_MINIMUM_LINK_UTILIZATION = 0.85
AI_TRAINING_MINIMUM_BASELINE_IMPROVEMENT = 0.05
AI_TRAINING_MINIMUM_BYTES_PER_SECOND = int(
    (AI_TRAINING_BOTTLENECK_GBIT_S * 1_000_000_000 / 8.0)
    * AI_TRAINING_MINIMUM_LINK_UTILIZATION
)


@dataclass(frozen=True)
class NativeReadBenchmarkConfig:
    label: str
    path: str
    offset: int = 0
    length: int = READ_TO_EOF
    iterations: int = DEFAULT_ITERATIONS
    warmup_iterations: int = DEFAULT_WARMUP_ITERATIONS
    block_size: int = DEFAULT_BLOCK_SIZE_BYTES

    def __post_init__(self) -> None:
        if not self.label:
            raise ValueError("label is required")
        if not self.path:
            raise ValueError("path is required")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length < READ_TO_EOF:
            raise ValueError("length must be non-negative or READ_TO_EOF")
        if self.iterations <= 0:
            raise ValueError("iterations must be positive")
        if self.warmup_iterations < 0:
            raise ValueError("warmup_iterations must be non-negative")
        if self.block_size <= 0:
            raise ValueError("block_size must be positive")


@dataclass(frozen=True)
class NativeReadBenchmarkResult:
    label: str
    path: str
    iterations: int
    warmup_iterations: int
    block_size: int
    bytes_read: int
    elapsed_seconds: float

    def __post_init__(self) -> None:
        if not self.label:
            raise ValueError("label is required")
        if not self.path:
            raise ValueError("path is required")
        if self.iterations <= 0:
            raise ValueError("iterations must be positive")
        if self.warmup_iterations < 0:
            raise ValueError("warmup_iterations must be non-negative")
        if self.block_size <= 0:
            raise ValueError("block_size must be positive")
        if self.bytes_read < 0:
            raise ValueError("bytes_read must be non-negative")
        if self.elapsed_seconds <= 0:
            raise ValueError("elapsed_seconds must be positive")

    @property
    def bytes_per_second(self) -> float:
        return self.bytes_read / self.elapsed_seconds

    @property
    def mebibytes_per_second(self) -> float:
        return self.bytes_per_second / (1024.0 * 1024.0)

    @property
    def megabytes_per_second(self) -> float:
        return self.bytes_per_second / 1_000_000.0

    @property
    def gigabits_per_second(self) -> float:
        return self.bytes_per_second * 8.0 / 1_000_000_000.0


@dataclass(frozen=True)
class NativeReadObjectiveEvaluation:
    objective_name: str
    baseline_name: str
    baseline_bytes_per_second: float
    candidate: NativeReadBenchmarkResult
    bottleneck_gbit_s: float = AI_TRAINING_BOTTLENECK_GBIT_S
    minimum_link_utilization: float = AI_TRAINING_MINIMUM_LINK_UTILIZATION
    minimum_baseline_improvement: float = AI_TRAINING_MINIMUM_BASELINE_IMPROVEMENT
    minimum_bytes_per_second: int = AI_TRAINING_MINIMUM_BYTES_PER_SECOND

    def __post_init__(self) -> None:
        if not self.objective_name:
            raise ValueError("objective_name is required")
        if not self.baseline_name:
            raise ValueError("baseline_name is required")
        if self.baseline_bytes_per_second <= 0:
            raise ValueError("baseline_bytes_per_second must be positive")
        if self.candidate is None:
            raise ValueError("candidate is required")
        if self.bottleneck_gbit_s <= 0:
            raise ValueError("bottleneck_gbit_s must be positive")
        if self.minimum_link_utilization <= 0 or self.minimum_link_utilization > 1:
            raise ValueError("minimum_link_utilization must be between 0 and 1")
        if self.minimum_baseline_improvement < 0:
            raise ValueError("minimum_baseline_improvement must be non-negative")
        if self.minimum_bytes_per_second <= 0:
            raise ValueError("minimum_bytes_per_second must be positive")

    @property
    def candidate_link_utilization(self) -> float:
        bottleneck_bytes_per_second = self.bottleneck_gbit_s * 1_000_000_000 / 8.0
        return self.candidate.bytes_per_second / bottleneck_bytes_per_second

    @property
    def improvement_ratio(self) -> float:
        return self.candidate.bytes_per_second / self.baseline_bytes_per_second - 1.0

    @property
    def link_utilization_passed(self) -> bool:
        return self.candidate_link_utilization >= self.minimum_link_utilization

    @property
    def minimum_throughput_passed(self) -> bool:
        return self.candidate.bytes_per_second >= self.minimum_bytes_per_second

    @property
    def baseline_improvement_passed(self) -> bool:
        return self.improvement_ratio >= self.minimum_baseline_improvement

    @property
    def passed(self) -> bool:
        return (
            self.link_utilization_passed
            and self.minimum_throughput_passed
            and self.baseline_improvement_passed
        )


def evaluate_native_read_objective(
    result: NativeReadBenchmarkResult,
    *,
    objective_name: str,
    baseline_mb_per_second: float,
) -> NativeReadObjectiveEvaluation:
    normalized = _normalize_token(objective_name)
    if normalized != _normalize_token(AI_TRAINING_25GBE_OBJECTIVE):
        raise ValueError(f"unknown benchmark objective: {objective_name}")
    if baseline_mb_per_second <= 0:
        raise ValueError("baseline_mb_per_second must be positive")
    return NativeReadObjectiveEvaluation(
        AI_TRAINING_25GBE_OBJECTIVE,
        AI_TRAINING_BASELINE_NAME,
        baseline_mb_per_second * 1_000_000.0,
        result,
    )


def run_native_read_benchmark(
    control_client: Any,
    bulk_client: Any,
    config: NativeReadBenchmarkConfig,
) -> NativeReadBenchmarkResult:
    if control_client is None:
        raise ValueError("control_client is required")
    if bulk_client is None:
        raise ValueError("bulk_client is required")
    if config is None:
        raise ValueError("config is required")

    metadata_response = control_client.stat_path(config.path)
    metadata = getattr(metadata_response, "metadata", metadata_response)
    if metadata is None:
        raise ValueError(f"benchmark path does not exist: {config.path}")
    if metadata.type != FileType.REGULAR_FILE:
        raise ValueError(f"benchmark path must be a regular file: {config.path}")
    length = _read_length(config, metadata.size)

    for _ in range(config.warmup_iterations):
        _read_once(bulk_client, config, length)

    bytes_read = 0
    start = time.perf_counter()
    for _ in range(config.iterations):
        bytes_read += _read_once(bulk_client, config, length)
    elapsed = max(time.perf_counter() - start, 1e-9)

    return NativeReadBenchmarkResult(
        config.label,
        config.path,
        config.iterations,
        config.warmup_iterations,
        config.block_size,
        bytes_read,
        elapsed,
    )


def _select_python_bulk_client_factory(transport: str, factory: Callable[..., Any]) -> Callable[..., Any]:
    if transport == PYTHON_BULK_TRANSPORT_QUIC and _is_length_prefixed_bulk_connect(factory):
        return CxnfsQuicBulkClient.connect
    return factory


def _is_length_prefixed_bulk_connect(factory: Callable[..., Any]) -> bool:
    return (
        getattr(factory, "__self__", None) is CxnfsLengthPrefixedBulkClient
        and getattr(factory, "__func__", None) is getattr(CxnfsLengthPrefixedBulkClient.connect, "__func__", None)
    )


def connect_and_run_native_read_benchmark(
    credentials: ClientCredentials,
    config: NativeReadBenchmarkConfig,
    *,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    timeout: float | None = 10.0,
    tls_context: Any | None = None,
    server_hostname: str | None = None,
    read_config: CxnfsBulkReadConfig | None = None,
    write_connections: int | None = None,
    bulk_transport: str = PYTHON_BULK_TRANSPORT_STREAM,
    cafile: str | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
) -> tuple[Any, NativeReadBenchmarkResult]:
    tls_kwargs = {
        "tls_context": tls_context,
        "server_hostname": server_hostname or host,
    }
    selected_transport = normalize_python_bulk_transport(bulk_transport)
    selected_bulk_client_factory = _select_python_bulk_client_factory(
        selected_transport,
        bulk_client_factory,
    )
    bulk_kwargs = dict(tls_kwargs)
    if selected_transport == PYTHON_BULK_TRANSPORT_QUIC:
        bulk_kwargs["cafile"] = cafile
    with control_client_factory(host, control_port, timeout=timeout, **tls_kwargs) as control_client:
        accepted = control_client.authenticate(credentials)
        read_only_write_connections = 1 if write_connections is None else write_connections
        with selected_bulk_client_factory(
            control_client,
            host,
            transport_port,
            timeout=timeout,
            read_config=read_config,
            write_connections=read_only_write_connections,
            **bulk_kwargs,
        ) as bulk_client:
            result = run_native_read_benchmark(control_client, bulk_client, config)
            return accepted, result


def run_benchmark_cli(
    argv: list[str] | tuple[str, ...] | None = None,
    *,
    out: TextIO | None = None,
    err: TextIO | None = None,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    parser = _benchmark_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        config = NativeReadBenchmarkConfig(
            args.label,
            args.path,
            args.offset,
            args.length if args.length is not None else READ_TO_EOF,
            args.iterations,
            args.warmup,
            args.block_size,
        )
        read_config = CxnfsBulkReadConfig(
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size_bytes=args.provisional_container_size,
            expected_size_limit_bytes=args.expected_size_limit,
        )
        bulk_transport = normalize_python_bulk_transport(args.bulk_transport)
        if args.objective and args.baseline_mb_s is None:
            raise ValueError("--objective requires --baseline-mb-s")
        if args.baseline_mb_s is not None and args.baseline_mb_s <= 0:
            raise ValueError("--baseline-mb-s must be positive")
    except SystemExit as exc:
        return int(exc.code)
    except ValueError as exc:
        print(str(exc), file=err)
        return 2

    try:
        bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        tls_context = tls_context_factory(args.cafile) if args.cafile else None
        server_hostname = args.server_hostname or args.host
        with credential_prompt() as credentials:
            accepted, result = connect_and_run_native_read_benchmark(
                credentials,
                config,
                host=args.host,
                control_port=args.control_port,
                transport_port=args.transport_port,
                timeout=args.timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                read_config=read_config,
                write_connections=args.write_connections,
                bulk_transport=bulk_transport,
                cafile=args.cafile,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
            )
        evaluation = (
            evaluate_native_read_objective(
                result,
                objective_name=args.objective,
                baseline_mb_per_second=args.baseline_mb_s,
            )
            if args.objective
            else None
        )
        _write_benchmark_result(
            out,
            args.host,
            args.control_port,
            args.transport_port,
            accepted.session_id,
            bulk_transport,
            read_config,
            result,
            evaluation,
        )
        if args.report:
            report = Path(args.report).resolve()
            _write_benchmark_report(
                report,
                args.host,
                args.control_port,
                args.transport_port,
                accepted.session_id,
                bulk_transport,
                read_config,
                result,
                evaluation,
            )
            print(f"report={report}", file=out)
        if args.fail_on_miss and evaluation is not None and not evaluation.passed:
            return 1
        return 0
    except Exception as exc:
        print(str(exc), file=err)
        return 1


def main() -> None:
    raise SystemExit(run_benchmark_cli())


def _read_length(config: NativeReadBenchmarkConfig, size: int) -> int:
    if config.offset > size:
        raise ValueError(f"offset is beyond end of file: {config.offset}")
    available = size - config.offset
    if config.length == READ_TO_EOF:
        return available
    return min(config.length, available)


def _read_once(bulk_client: Any, config: NativeReadBenchmarkConfig, length: int) -> int:
    remaining = length
    offset = config.offset
    bytes_read = 0
    while remaining > 0:
        request_length = min(remaining, config.block_size)
        payload = bulk_client.read_mds_shard(config.path, offset, request_length)
        returned = len(payload)
        if returned != request_length:
            raise ValueError(
                f"native read returned {returned} bytes for requested range of {request_length} bytes"
            )
        bytes_read += returned
        offset += returned
        remaining -= returned
    return bytes_read


def _write_benchmark_result(
    out: TextIO,
    host: str,
    control_port: int,
    transport_port: int,
    session_id: Any,
    bulk_transport: str,
    read_config: CxnfsBulkReadConfig,
    result: NativeReadBenchmarkResult,
    evaluation: NativeReadObjectiveEvaluation | None = None,
) -> None:
    print(
        f"{result.label} {result.path} iterations={result.iterations} "
        f"warmup={result.warmup_iterations} block={_human_bytes(result.block_size)} "
        f"bytes={result.bytes_read} seconds={result.elapsed_seconds:.3f} "
        f"MiB/s={result.mebibytes_per_second:.2f} "
        f"MB/s={result.megabytes_per_second:.2f} "
        f"Gbit/s={result.gigabits_per_second:.3f}",
        file=out,
    )
    print(
        f"cxnfs_native host={host} control_port={control_port} transport_port={transport_port} "
        f"bulk_transport={bulk_transport} session_id={session_id} payload_format=mds_shards "
        f"initial_cords={read_config.initial_cords} max_cords={read_config.max_cords}",
        file=out,
    )
    if evaluation is not None:
        print(_format_objective_evaluation(evaluation), file=out)


def _write_benchmark_report(
    report: Path,
    host: str,
    control_port: int,
    transport_port: int,
    session_id: Any,
    bulk_transport: str,
    read_config: CxnfsBulkReadConfig,
    result: NativeReadBenchmarkResult,
    evaluation: NativeReadObjectiveEvaluation | None = None,
) -> None:
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        _benchmark_report_text(
            host,
            control_port,
            transport_port,
            session_id,
            bulk_transport,
            read_config,
            result,
            evaluation,
        ),
        encoding="utf-8",
    )


def _benchmark_report_text(
    host: str,
    control_port: int,
    transport_port: int,
    session_id: Any,
    bulk_transport: str,
    read_config: CxnfsBulkReadConfig,
    result: NativeReadBenchmarkResult,
    evaluation: NativeReadObjectiveEvaluation | None = None,
) -> str:
    lines = [
        "cxnfs.python.benchmark.native-read.v1",
        f"label={result.label}",
        f"host={host}",
        f"control_port={control_port}",
        f"transport_port={transport_port}",
        f"bulk_transport={bulk_transport}",
        f"session_id={session_id}",
        f"path={result.path}",
        f"iterations={result.iterations}",
        f"warmup={result.warmup_iterations}",
        f"block_size={result.block_size}",
        "payload_format=mds_shards",
        f"initial_cords={read_config.initial_cords}",
        f"max_cords={read_config.max_cords}",
        f"bytes={result.bytes_read}",
        f"seconds={result.elapsed_seconds:.6f}",
        f"mib_per_second={result.mebibytes_per_second:.2f}",
        f"mb_per_second={result.megabytes_per_second:.2f}",
        f"gbit_per_second={result.gigabits_per_second:.3f}",
    ]
    if evaluation is not None:
        lines.extend(_objective_report_lines(evaluation))
    return "\n".join(lines) + "\n"


def _benchmark_parser() -> argparse.ArgumentParser:
    parser = _BenchmarkArgumentParser(prog="cxnfs-benchmark")
    parser.add_argument("path")
    parser.add_argument("--label", default="cxnfs-python-native")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--bulk-transport", default="stream")
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    parser.add_argument("--cafile", help="optional CA bundle for the server certificate")
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--length", type=int)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP_ITERATIONS)
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE_BYTES)
    parser.add_argument("--initial-cords", type=int, default=1)
    parser.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    parser.add_argument("--provisional-container-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--expected-size-limit", type=int, default=0)
    parser.add_argument("--write-connections", type=int)
    parser.add_argument("--objective", choices=[AI_TRAINING_25GBE_OBJECTIVE])
    parser.add_argument("--baseline-mb-s", type=float)
    parser.add_argument("--fail-on-miss", action="store_true")
    parser.add_argument("--report", help="write stable key/value benchmark report")
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser


def _format_objective_evaluation(evaluation: NativeReadObjectiveEvaluation) -> str:
    return (
        f"objective={evaluation.objective_name} "
        f"baseline={evaluation.baseline_name} "
        f"bottleneck_gbit_s={evaluation.bottleneck_gbit_s:.3f} "
        f"minimum_link_utilization={evaluation.minimum_link_utilization:.3f} "
        f"candidate_link_utilization={evaluation.candidate_link_utilization:.3f} "
        f"minimum_candidate_MB_s={evaluation.minimum_bytes_per_second / 1_000_000.0:.2f} "
        f"candidate_MB_s={evaluation.candidate.megabytes_per_second:.2f} "
        f"baseline_MB_s={evaluation.baseline_bytes_per_second / 1_000_000.0:.2f} "
        f"minimum_baseline_improvement={evaluation.minimum_baseline_improvement * 100.0:.2f}% "
        f"candidate_improvement={evaluation.improvement_ratio * 100.0:.2f}% "
        f"link_passed={str(evaluation.link_utilization_passed).lower()} "
        f"throughput_passed={str(evaluation.minimum_throughput_passed).lower()} "
        f"improvement_passed={str(evaluation.baseline_improvement_passed).lower()} "
        f"objective_passed={str(evaluation.passed).lower()}"
    )


def _objective_report_lines(evaluation: NativeReadObjectiveEvaluation) -> list[str]:
    return [
        f"objective={evaluation.objective_name}",
        f"objective_baseline={evaluation.baseline_name}",
        f"objective_bottleneck_gbit_s={evaluation.bottleneck_gbit_s:.3f}",
        f"objective_minimum_link_utilization={evaluation.minimum_link_utilization:.3f}",
        f"objective_candidate_link_utilization={evaluation.candidate_link_utilization:.3f}",
        f"objective_minimum_candidate_mb_per_second={evaluation.minimum_bytes_per_second / 1_000_000.0:.2f}",
        f"objective_candidate_mb_per_second={evaluation.candidate.megabytes_per_second:.2f}",
        f"objective_baseline_mb_per_second={evaluation.baseline_bytes_per_second / 1_000_000.0:.2f}",
        f"objective_minimum_baseline_improvement={evaluation.minimum_baseline_improvement:.6f}",
        f"objective_candidate_improvement={evaluation.improvement_ratio:.6f}",
        f"objective_link_passed={str(evaluation.link_utilization_passed).lower()}",
        f"objective_throughput_passed={str(evaluation.minimum_throughput_passed).lower()}",
        f"objective_improvement_passed={str(evaluation.baseline_improvement_passed).lower()}",
        f"objective_passed={str(evaluation.passed).lower()}",
    ]


def _normalize_token(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _human_bytes(value: int) -> str:
    if value >= 1024 * 1024 * 1024 and value % (1024 * 1024 * 1024) == 0:
        return f"{value // (1024 * 1024 * 1024)}GiB"
    if value >= 1024 * 1024 and value % (1024 * 1024) == 0:
        return f"{value // (1024 * 1024)}MiB"
    if value >= 1024 and value % 1024 == 0:
        return f"{value // 1024}KiB"
    return str(value)


class _BenchmarkArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


if __name__ == "__main__":
    main()
