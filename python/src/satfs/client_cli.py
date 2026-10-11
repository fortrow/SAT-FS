from __future__ import annotations

import json
import mmap
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Callable, TextIO

from .auth import ClientCredentials, prompt_client_credentials
from .bulk import (
    ABSOLUTE_MAX_CORDS,
    CxnfsBulkReadConfig,
    CxnfsLengthPrefixedBulkClient,
    CxnfsQuicBulkClient,
    PYTHON_BULK_TRANSPORT_QUIC,
    normalize_python_bulk_transport,
    require_supported_python_bulk_transport,
)
from .control import CxnfsControlClient, DirectoryListing, FileMetadata, FileType
from .dataset import SendResult
from .tls import create_client_tls_context


DEFAULT_CONTROL_PORT = 4509
DEFAULT_TRANSPORT_PORT = 4510
CLIENT_RESULT_FORMAT = "cxnfs.python-client-result.v1"


def run_client_cli(
    argv: list[str] | None = None,
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
    parser = _client_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        read_config = CxnfsBulkReadConfig(
            initial_cords=args.initial_cords,
            max_cords=args.max_cords,
            provisional_container_size_bytes=args.provisional_container_size,
            expected_size_limit_bytes=args.expected_size_limit,
            write_frame_payload_bytes=args.write_frame_payload_bytes,
        )
        bulk_transport = normalize_python_bulk_transport(args.bulk_transport)
        if args.timeout is not None and args.timeout <= 0:
            raise ValueError("--timeout must be positive")
    except SystemExit as exc:
        return int(exc.code)
    except ValueError as exc:
        print(str(exc), file=err)
        return 2

    try:
        if _command_uses_bulk(args.command):
            bulk_transport = require_supported_python_bulk_transport(args.bulk_transport)
        tls_context = tls_context_factory(args.cafile) if args.cafile else None
        server_hostname = args.server_name or args.host
        with credential_prompt() as credentials:
            control_client = control_client_factory(
                args.host,
                args.control_port,
                timeout=args.timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
            )
            with control_client as control:
                accepted = control.authenticate(credentials)
                if args.command == "list":
                    response = control.list_directory(args.path)
                    _write_json_result(
                        out,
                        args,
                        accepted,
                        {"listing": _listing_payload(response.listing)},
                    )
                    return 0
                if args.command == "stat":
                    response = control.stat_path(args.path)
                    _write_json_result(
                        out,
                        args,
                        accepted,
                        {"metadata": None if response.metadata is None else _metadata_payload(response.metadata)},
                    )
                    return 0

                bulk_factory = _select_bulk_client_factory(bulk_transport, bulk_client_factory)
                bulk_kwargs = {
                    "timeout": args.timeout,
                    "tls_context": tls_context,
                    "server_hostname": server_hostname,
                    "read_config": read_config,
                    "write_connections": args.write_connections,
                    "close_control_client": False,
                }
                if bulk_transport == PYTHON_BULK_TRANSPORT_QUIC:
                    bulk_kwargs["cafile"] = args.cafile
                bulk_client = bulk_factory(
                    control,
                    args.host,
                    args.transport_port,
                    **bulk_kwargs,
                )
                with bulk_client as bulk:
                    if args.command == "read-mds":
                        payload = bulk.read_mds_shard(args.path, args.offset, args.length)
                        output_path = Path(args.output)
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        output_path.write_bytes(payload)
                        _write_json_result(
                            out,
                            args,
                            accepted,
                            {
                                "path": args.path,
                                "offset": args.offset,
                                "length": args.length,
                                "bytes_read": len(payload),
                                "output": str(output_path),
                                "read_path_evidence": _optional_client_evidence(bulk, "read_path_evidence"),
                            },
                        )
                        return 0
                    if args.command == "send-checkpoint":
                        result = bulk.send_safetensors_output(args.output_dir, remote_root=args.remote_root)
                        _write_json_result(
                            out,
                            args,
                            accepted,
                            {
                                "result": _send_result_payload(result),
                                "write_path_evidence": _optional_client_evidence(bulk, "write_path_evidence"),
                            },
                        )
                        return 0
                    if args.command == "send-file":
                        source_path = Path(args.source)
                        result = _send_file_with_direct_write(bulk, source_path, args.remote_path)
                        _write_json_result(
                            out,
                            args,
                            accepted,
                            {
                                "source": str(source_path),
                                "remote_path": args.remote_path,
                                "result": _send_result_payload(result),
                                "write_path_evidence": _optional_client_evidence(bulk, "write_path_evidence"),
                            },
                        )
                        return 0
        raise ValueError(f"unsupported command: {args.command}")
    except Exception as exc:
        print(str(exc), file=err)
        return 1


def main() -> None:
    raise SystemExit(run_client_cli())


def _client_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="cxnfs-client",
        description="Use native CXNFS Python reads and writes without a mounted filesystem.",
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--control-port", type=int, default=DEFAULT_CONTROL_PORT)
    parser.add_argument("--transport-port", type=int, default=DEFAULT_TRANSPORT_PORT)
    parser.add_argument("--bulk-transport", default="stream")
    parser.add_argument("--cafile")
    parser.add_argument("--server-name")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--initial-cords", type=int, default=1)
    parser.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    parser.add_argument("--provisional-container-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--expected-size-limit", type=int, default=0)
    parser.add_argument("--write-frame-payload-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--write-connections", type=int)

    subcommands = parser.add_subparsers(dest="command", required=True)

    list_parser = subcommands.add_parser("list", help="List a remote directory.")
    list_parser.add_argument("path")

    stat_parser = subcommands.add_parser("stat", help="Stat a remote path.")
    stat_parser.add_argument("path")

    read_parser = subcommands.add_parser("read-mds", help="Read a byte range from an MDS shard.")
    read_parser.add_argument("path")
    read_parser.add_argument("--offset", type=int, default=0)
    read_parser.add_argument("--length", type=int, required=True)
    read_parser.add_argument("--output", required=True)

    send_parser = subcommands.add_parser("send-checkpoint", help="Send a local checkpoint output directory.")
    send_parser.add_argument("output_dir")
    send_parser.add_argument("--remote-root", required=True)

    send_file_parser = subcommands.add_parser("send-file", help="Send one file through the direct memory write path.")
    send_file_parser.add_argument("source")
    send_file_parser.add_argument("--remote-path", required=True)

    return parser


def _command_uses_bulk(command: str) -> bool:
    return command in {"read-mds", "send-checkpoint", "send-file"}


def _select_bulk_client_factory(transport: str, factory: Callable[..., Any]) -> Callable[..., Any]:
    if transport == PYTHON_BULK_TRANSPORT_QUIC and _is_length_prefixed_bulk_connect(factory):
        return CxnfsQuicBulkClient.connect
    return factory


def _is_length_prefixed_bulk_connect(factory: Callable[..., Any]) -> bool:
    return (
        getattr(factory, "__self__", None) is CxnfsLengthPrefixedBulkClient
        and getattr(factory, "__func__", None) is getattr(CxnfsLengthPrefixedBulkClient.connect, "__func__", None)
    )


def _write_json_result(out: TextIO, args: Any, accepted: Any, payload: dict[str, Any]) -> None:
    result = {
        "format": CLIENT_RESULT_FORMAT,
        "command": args.command,
        "host": args.host,
        "control_port": args.control_port,
        "transport_port": args.transport_port,
        "bulk_transport": normalize_python_bulk_transport(args.bulk_transport),
        "session_id": str(accepted.session_id),
        **payload,
    }
    json.dump(result, out, sort_keys=True)
    out.write("\n")


def _listing_payload(listing: DirectoryListing) -> dict[str, Any]:
    return {
        "path": listing.path,
        "entries": [_metadata_payload(entry) for entry in listing.entries],
    }


def _metadata_payload(metadata: FileMetadata) -> dict[str, Any]:
    return {
        "path": metadata.path,
        "name": metadata.name,
        "type": _file_type_name(metadata.type),
        "size": metadata.size,
        "content_hash": None if metadata.content_hash is None else bytes(metadata.content_hash).hex(),
        "modified_at_seconds": metadata.modified_at_seconds,
        "modified_at_nanos": metadata.modified_at_nanos,
    }


def _file_type_name(file_type: FileType) -> str:
    if file_type == FileType.DIRECTORY:
        return "directory"
    if file_type == FileType.REGULAR_FILE:
        return "regular_file"
    return str(file_type)


def _send_result_payload(result: SendResult) -> dict[str, int | None]:
    return {
        "file_count": result.file_count,
        "tensor_file_count": result.tensor_file_count,
        "bytes_sent": result.bytes_sent,
    }


def _send_file_with_direct_write(bulk: Any, source_path: Path, remote_path: str) -> SendResult:
    if not source_path.is_file():
        raise FileNotFoundError(f"send source file does not exist: {source_path}")
    size = source_path.stat().st_size
    if size <= 0:
        raise ValueError("send source file must be non-empty")
    with source_path.open("rb") as handle:
        with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            return bulk.write_tensor(remote_path, mapped, size)


def _optional_client_evidence(client: Any, method_name: str) -> dict[str, Any] | None:
    method = getattr(client, method_name, None)
    if not callable(method):
        return None
    evidence = method()
    return dict(evidence) if isinstance(evidence, dict) else None


if __name__ == "__main__":
    main()
