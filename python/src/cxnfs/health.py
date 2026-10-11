from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Callable, TextIO
from uuid import UUID

from .auth import ClientCredentials, prompt_client_credentials
from .bulk import (
    CxnfsLengthPrefixedBulkClient,
    CxnfsQuicBulkClient,
    PYTHON_BULK_TRANSPORT_QUIC,
    PYTHON_BULK_TRANSPORT_STREAM,
    normalize_python_bulk_transport,
    require_supported_python_bulk_transport,
)
from .control import CxnfsControlClient, DirectoryListing
from .tls import create_client_tls_context


@dataclass(frozen=True)
class CxnfsHealthResult:
    status: str
    session_id: UUID
    probe_path: str
    probe_entries: int
    bulk_probe: str
    bulk_transport: str = PYTHON_BULK_TRANSPORT_STREAM
    bulk_probe_path: str | None = None
    bulk_probe_bytes: int = 0

    @property
    def ready(self) -> bool:
        return self.status == "ready"


def probe_cxnfs_health(
    control_client: Any,
    credentials: ClientCredentials,
    *,
    probe_path: str = "/",
    bulk_client: Any | None = None,
    bulk_transport: str = PYTHON_BULK_TRANSPORT_STREAM,
    bulk_probe_path: str | None = None,
    bulk_probe_length: int = 1,
) -> CxnfsHealthResult:
    if control_client is None:
        raise ValueError("control_client is required")
    if credentials is None:
        raise ValueError("credentials are required")
    if not probe_path:
        raise ValueError("probe_path is required")
    if bulk_probe_path is not None and bulk_client is None:
        raise ValueError("bulk_client is required when bulk_probe_path is set")
    if bulk_probe_path is not None and bulk_probe_length <= 0:
        raise ValueError("bulk_probe_length must be positive")
    selected_bulk_transport = normalize_python_bulk_transport(bulk_transport)

    accepted = control_client.authenticate(credentials)
    listing_response = control_client.list_directory(probe_path)
    listing = _listing_from_response(listing_response)

    if bulk_probe_path is None:
        return CxnfsHealthResult(
            status="ready",
            session_id=accepted.session_id,
            probe_path=listing.path,
            probe_entries=len(listing.entries),
            bulk_transport=selected_bulk_transport,
            bulk_probe="skipped",
        )

    payload = bulk_client.read_mds_shard(bulk_probe_path, 0, bulk_probe_length)
    returned_bytes = len(payload)
    if returned_bytes != bulk_probe_length:
        raise ValueError("short bulk health probe")
    return CxnfsHealthResult(
        status="ready",
        session_id=accepted.session_id,
        probe_path=listing.path,
        probe_entries=len(listing.entries),
        bulk_transport=selected_bulk_transport,
        bulk_probe="ok",
        bulk_probe_path=bulk_probe_path,
        bulk_probe_bytes=returned_bytes,
    )


def connect_and_probe_cxnfs_health(
    credentials: ClientCredentials,
    *,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    probe_path: str = "/",
    bulk_probe_path: str | None = None,
    bulk_probe_length: int = 1,
    bulk_transport: str = PYTHON_BULK_TRANSPORT_STREAM,
    timeout: float | None = 10.0,
    tls_context: Any | None = None,
    server_hostname: str | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    bulk_client_factory: Callable[..., Any] = CxnfsLengthPrefixedBulkClient.connect,
) -> CxnfsHealthResult:
    selected_bulk_transport = normalize_python_bulk_transport(bulk_transport)
    selected_bulk_client_factory = _select_bulk_client_factory(
        selected_bulk_transport,
        bulk_client_factory,
    )
    tls_kwargs = {
        "tls_context": tls_context,
        "server_hostname": server_hostname or host,
    }
    with control_client_factory(host, control_port, timeout=timeout, **tls_kwargs) as control_client:
        if bulk_probe_path is None:
            return probe_cxnfs_health(
                control_client,
                credentials,
                probe_path=probe_path,
                bulk_transport=selected_bulk_transport,
            )
        with selected_bulk_client_factory(
            control_client,
            host,
            transport_port,
            timeout=timeout,
            **tls_kwargs,
        ) as bulk_client:
            return probe_cxnfs_health(
                control_client,
                credentials,
                probe_path=probe_path,
                bulk_client=bulk_client,
                bulk_transport=selected_bulk_transport,
                bulk_probe_path=bulk_probe_path,
                bulk_probe_length=bulk_probe_length,
            )


def run_health_cli(
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
    parser = _health_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        if args.read_length is not None and args.read_path is None:
            raise ValueError("--read-length requires --read-path")
        bulk_transport = normalize_python_bulk_transport(args.bulk_transport)
        read_length = args.read_length if args.read_length is not None else 1
        if args.read_path is not None and read_length <= 0:
            raise ValueError("--read-length must be positive")
    except SystemExit as exc:
        return int(exc.code)
    except ValueError as exc:
        print(str(exc), file=err)
        return 2

    try:
        if args.read_path is not None:
            bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        tls_context = tls_context_factory(args.cafile) if args.cafile else None
        server_hostname = args.server_hostname or args.host
        with credential_prompt() as credentials:
            result = connect_and_probe_cxnfs_health(
                credentials,
                host=args.host,
                control_port=args.control_port,
                transport_port=args.transport_port,
                probe_path=args.path,
                bulk_probe_path=args.read_path,
                bulk_probe_length=read_length,
                bulk_transport=bulk_transport,
                timeout=args.timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                control_client_factory=control_client_factory,
                bulk_client_factory=bulk_client_factory,
            )
        _write_health_result(out, args.host, args.control_port, args.transport_port, result)
        if args.report:
            report = Path(args.report).resolve()
            _write_health_report(report, args.host, args.control_port, args.transport_port, result)
            print(f"report={report}", file=out)
        return 0
    except Exception as exc:
        print(str(exc), file=err)
        return 1


def main() -> None:
    raise SystemExit(run_health_cli())


def _listing_from_response(response: Any) -> DirectoryListing:
    listing = getattr(response, "listing", response)
    if not isinstance(listing, DirectoryListing):
        raise TypeError(f"expected DirectoryListing, got {type(listing).__name__}")
    return listing


def _select_bulk_client_factory(transport: str, factory: Callable[..., Any]) -> Callable[..., Any]:
    if transport == PYTHON_BULK_TRANSPORT_QUIC and _is_length_prefixed_bulk_connect(factory):
        return CxnfsQuicBulkClient.connect
    return factory


def _is_length_prefixed_bulk_connect(factory: Callable[..., Any]) -> bool:
    return (
        getattr(factory, "__self__", None) is CxnfsLengthPrefixedBulkClient
        and getattr(factory, "__func__", None) is getattr(CxnfsLengthPrefixedBulkClient.connect, "__func__", None)
    )


def _write_health_result(
    out: TextIO,
    host: str,
    control_port: int,
    transport_port: int,
    result: CxnfsHealthResult,
) -> None:
    print("cxnfs python health", file=out)
    print(f"host={host}", file=out)
    print(f"control_port={control_port}", file=out)
    print(f"transport_port={transport_port}", file=out)
    print("authenticated=true", file=out)
    print(f"session_id={result.session_id}", file=out)
    print(f"probe_path={result.probe_path}", file=out)
    print(f"probe_entries={result.probe_entries}", file=out)
    print(f"bulk_transport={result.bulk_transport}", file=out)
    print(f"bulk_probe={result.bulk_probe}", file=out)
    if result.bulk_probe_path is not None:
        print(f"bulk_probe_path={result.bulk_probe_path}", file=out)
        print(f"bulk_probe_bytes={result.bulk_probe_bytes}", file=out)
    print(f"status={result.status}", file=out)


def _write_health_report(
    report: Path,
    host: str,
    control_port: int,
    transport_port: int,
    result: CxnfsHealthResult,
) -> None:
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        _health_report_text(host, control_port, transport_port, result),
        encoding="utf-8",
    )


def _health_report_text(
    host: str,
    control_port: int,
    transport_port: int,
    result: CxnfsHealthResult,
) -> str:
    lines = [
        "cxnfs.python.health.v1",
        f"host={host}",
        f"control_port={control_port}",
        f"transport_port={transport_port}",
        "authenticated=true",
        f"session_id={result.session_id}",
        f"probe_path={result.probe_path}",
        f"probe_entries={result.probe_entries}",
        f"bulk_transport={result.bulk_transport}",
        f"bulk_probe={result.bulk_probe}",
    ]
    if result.bulk_probe_path is not None:
        lines.append(f"bulk_probe_path={result.bulk_probe_path}")
        lines.append(f"bulk_probe_bytes={result.bulk_probe_bytes}")
    lines.append(f"status={result.status}")
    return "\n".join(lines) + "\n"


def _health_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cxnfs-health")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=4509)
    parser.add_argument("--transport-port", type=int, default=4510)
    parser.add_argument("--server-hostname", help="TLS server name; defaults to --host")
    parser.add_argument("--cafile", help="optional CA bundle for the server certificate")
    parser.add_argument("--path", default="/")
    parser.add_argument("--bulk-transport", default=PYTHON_BULK_TRANSPORT_STREAM)
    parser.add_argument("--read-path")
    parser.add_argument("--read-length", type=int)
    parser.add_argument("--report", help="write stable key/value health report")
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser
