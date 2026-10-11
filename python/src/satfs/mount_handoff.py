from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Callable, TextIO

from .auth import ClientCredentials, prompt_client_credentials
from .bulk import ABSOLUTE_MAX_CORDS
from .control import (
    CxnfsControlClient,
    DirectoryListing,
    FileType,
    MountBackend,
    MountCapability,
    MountNegotiatedResponse,
    MountStrategy,
)
from .namespace import normalize_namespace_root
from .tls import create_client_tls_context


HANDOFF_FORMAT = "cxnfs.mount.handoff.v1"
LAUNCH_PLAN_FORMAT = "cxnfs.mount.launch-plan.v1"
LIST_REPORT_FORMAT = "cxnfs.mount.list.v1"


@dataclass(frozen=True)
class MountAdapterHandoff:
    host: str
    control_port: int
    transport_port: int
    requested_backend: MountBackend
    requested_capabilities: tuple[MountCapability, ...]
    strategy: MountStrategy
    remote_root: str
    mount_point: str
    initial_cords: int
    max_cords: int
    provisional_container_size_bytes: int
    expected_size_limit_bytes: int
    connect_timeout_ms: int
    read_timeout_ms: int
    truststore_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host is required")
        if self.control_port <= 0 or self.control_port > 65535:
            raise ValueError("control_port must be between 1 and 65535")
        if self.transport_port <= 0 or self.transport_port > 65535:
            raise ValueError("transport_port must be between 1 and 65535")
        if self.initial_cords <= 0:
            raise ValueError("initial_cords must be positive")
        if self.max_cords <= 0 or self.initial_cords > self.max_cords:
            raise ValueError("cord limits are invalid")
        if self.provisional_container_size_bytes <= 0:
            raise ValueError("provisional_container_size_bytes must be positive")
        if self.expected_size_limit_bytes < 0:
            raise ValueError("expected_size_limit_bytes must be non-negative")
        if self.connect_timeout_ms <= 0 or self.read_timeout_ms <= 0:
            raise ValueError("timeouts must be positive")
        if not self.mount_point:
            raise ValueError("mount_point is required")
        object.__setattr__(self, "remote_root", normalize_namespace_root(self.remote_root))
        object.__setattr__(
            self,
            "requested_capabilities",
            _normalize_capabilities(self.requested_capabilities),
        )

    @property
    def adapter_kind(self) -> str:
        return _adapter_kind(self.strategy.backend)

    @property
    def adapter_binary(self) -> str:
        return _adapter_binary(self.strategy.backend)


def mount_handoff_report_text(handoff: MountAdapterHandoff) -> str:
    lines = [
        HANDOFF_FORMAT,
        f"host={handoff.host}",
        f"control_port={handoff.control_port}",
        f"transport_port={handoff.transport_port}",
        f"requested_backend={_backend_name(handoff.requested_backend)}",
        f"requested_capabilities={_capability_names(handoff.requested_capabilities)}",
        f"backend={_backend_name(handoff.strategy.backend)}",
        f"capabilities={_capability_names(handoff.strategy.capabilities)}",
        f"adapter_kind={handoff.adapter_kind}",
        f"adapter_binary={handoff.adapter_binary}",
        "adapter_argument_style=handoff-file",
        f"remote_root={handoff.remote_root}",
        f"mount_point={handoff.mount_point}",
        f"initial_cords={handoff.initial_cords}",
        f"max_cords={handoff.max_cords}",
        f"provisional_container_size_bytes={handoff.provisional_container_size_bytes}",
        f"expected_size_limit_bytes={handoff.expected_size_limit_bytes}",
        f"connect_timeout_ms={handoff.connect_timeout_ms}",
        f"read_timeout_ms={handoff.read_timeout_ms}",
        f"truststore_path={handoff.truststore_path or ''}",
        "truststore_password_inline=false",
        "auth_password_inline=false",
    ]
    return "\n".join(lines) + "\n"


def parse_mount_handoff_text(text: str) -> MountAdapterHandoff:
    if not text or not text.strip():
        raise ValueError("mount handoff is empty")
    lines = text.splitlines()
    if not lines or lines[0].strip() != HANDOFF_FORMAT:
        raise ValueError("unsupported mount handoff format")
    fields = _parse_key_value_lines(lines[1:])
    _require_false(fields, "truststore_password_inline")
    _require_false(fields, "auth_password_inline")
    return MountAdapterHandoff(
        host=_required(fields, "host"),
        control_port=_integer(fields, "control_port"),
        transport_port=_integer(fields, "transport_port"),
        requested_backend=_backend(_required(fields, "requested_backend")),
        requested_capabilities=_capabilities(_required(fields, "requested_capabilities")),
        strategy=MountStrategy(
            _backend(_required(fields, "backend")),
            _capabilities(_required(fields, "capabilities")),
        ),
        remote_root=_required(fields, "remote_root"),
        mount_point=_required(fields, "mount_point"),
        initial_cords=_integer(fields, "initial_cords"),
        max_cords=_integer(fields, "max_cords"),
        provisional_container_size_bytes=_integer(fields, "provisional_container_size_bytes"),
        expected_size_limit_bytes=_integer(fields, "expected_size_limit_bytes"),
        connect_timeout_ms=_integer(fields, "connect_timeout_ms"),
        read_timeout_ms=_integer(fields, "read_timeout_ms"),
        truststore_path=_optional_path(fields.get("truststore_path")),
    )


def read_mount_handoff(path: str | Path) -> MountAdapterHandoff:
    return parse_mount_handoff_text(Path(path).read_text(encoding="utf-8"))


def mount_launch_plan_text(
    handoff: MountAdapterHandoff,
    handoff_path: str | Path,
    *,
    adapter_binary: str | None = None,
) -> str:
    handoff_path = Path(handoff_path).resolve()
    binary = adapter_binary or handoff.adapter_binary
    argv = (binary, "--handoff", str(handoff_path))
    lines = [
        LAUNCH_PLAN_FORMAT,
        f"handoff_report={handoff_path}",
        f"backend={_backend_name(handoff.strategy.backend)}",
        f"adapter_kind={handoff.adapter_kind}",
        f"adapter_binary={binary}",
        f"remote_root={handoff.remote_root}",
        f"mount_point={handoff.mount_point}",
        f"host={handoff.host}",
        f"control_port={handoff.control_port}",
        f"transport_port={handoff.transport_port}",
        f"max_cords={handoff.max_cords}",
        f"argv_count={len(argv)}",
    ]
    lines.extend(f"argv_{index}={value}" for index, value in enumerate(argv))
    lines.extend([
        "auth_password_inline=false",
        "truststore_password_inline=false",
    ])
    return "\n".join(lines) + "\n"


def mount_list_report_text(
    handoff: MountAdapterHandoff,
    handoff_path: str | Path,
    listing: DirectoryListing,
) -> str:
    handoff_path = Path(handoff_path).resolve()
    lines = [
        LIST_REPORT_FORMAT,
        f"handoff_report={handoff_path}",
        f"backend={_backend_name(handoff.strategy.backend)}",
        f"remote_root={handoff.remote_root}",
        f"mount_point={handoff.mount_point}",
        f"host={handoff.host}",
        f"path={listing.path}",
        f"entries={len(listing.entries)}",
    ]
    for entry in listing.entries:
        content_hash = entry.content_hash.hex() if entry.content_hash is not None else "-"
        lines.append(f"{entry.type.name.lower()}\t{entry.size}\t{content_hash}\t{entry.path}")
    return "\n".join(lines) + "\n"


def connect_and_list_mount_handoff(
    credentials: ClientCredentials,
    handoff: MountAdapterHandoff,
    *,
    path: str | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> DirectoryListing:
    if MountCapability.LIST not in handoff.strategy.capabilities:
        raise ValueError("mount handoff does not include list capability")
    tls_context = tls_context_factory(str(handoff.truststore_path) if handoff.truststore_path else None)
    with control_client_factory(
        handoff.host,
        handoff.control_port,
        timeout=handoff.connect_timeout_ms / 1000.0,
        tls_context=tls_context,
        server_hostname=handoff.host,
    ) as client:
        client.authenticate(credentials)
        response = client.list_directory(path or handoff.remote_root)
        listing = getattr(response, "listing", response)
        if not isinstance(listing, DirectoryListing):
            raise TypeError(f"expected DirectoryListing, got {type(listing).__name__}")
        return listing


def connect_and_negotiate_mount_handoff(
    credentials: ClientCredentials,
    *,
    host: str = "127.0.0.1",
    control_port: int = 4509,
    transport_port: int = 4510,
    backend: MountBackend = MountBackend.CLI_LIST_ONLY,
    requested_capabilities: tuple[MountCapability, ...] = (MountCapability.LIST, MountCapability.READ),
    remote_root: str = "/",
    mount_point: str,
    initial_cords: int = 1,
    max_cords: int = ABSOLUTE_MAX_CORDS,
    provisional_container_size_bytes: int = 64 * 1024 * 1024,
    expected_size_limit_bytes: int = 0,
    connect_timeout_ms: int = 15_000,
    read_timeout_ms: int = 120_000,
    truststore_path: str | Path | None = None,
    server_hostname: str | None = None,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> MountAdapterHandoff:
    requested_capabilities = _normalize_capabilities(requested_capabilities)
    truststore = None if truststore_path is None else Path(truststore_path)
    tls_context = tls_context_factory(str(truststore) if truststore else None)
    with control_client_factory(
        host,
        control_port,
        timeout=connect_timeout_ms / 1000.0,
        tls_context=tls_context,
        server_hostname=server_hostname or host,
    ) as client:
        client.authenticate(credentials)
        response = client.mount_negotiate(backend, requested_capabilities)
        strategy = _strategy_from_response(response)
        return MountAdapterHandoff(
            host=host,
            control_port=control_port,
            transport_port=transport_port,
            requested_backend=backend,
            requested_capabilities=requested_capabilities,
            strategy=strategy,
            remote_root=remote_root,
            mount_point=mount_point,
            initial_cords=initial_cords,
            max_cords=max_cords,
            provisional_container_size_bytes=provisional_container_size_bytes,
            expected_size_limit_bytes=expected_size_limit_bytes,
            connect_timeout_ms=connect_timeout_ms,
            read_timeout_ms=read_timeout_ms,
            truststore_path=truststore,
        )


def run_mount_handoff_cli(
    argv: list[str] | tuple[str, ...] | None = None,
    *,
    out: TextIO | None = None,
    err: TextIO | None = None,
    credential_prompt: Callable[[], ClientCredentials] = prompt_client_credentials,
    control_client_factory: Callable[..., Any] = CxnfsControlClient.connect,
    tls_context_factory: Callable[[str | None], Any] = create_client_tls_context,
) -> int:
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    parser = _parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        if args.command == "negotiate":
            backend = _backend(args.backend)
            requested_capabilities = _capabilities(args.capabilities)
            with credential_prompt() as credentials:
                handoff = connect_and_negotiate_mount_handoff(
                    credentials,
                    host=args.host,
                    control_port=args.control_port,
                    transport_port=args.transport_port,
                    backend=backend,
                    requested_capabilities=requested_capabilities,
                    remote_root=args.remote_root,
                    mount_point=args.mount_point,
                    initial_cords=args.initial_cords,
                    max_cords=args.max_cords,
                    provisional_container_size_bytes=args.provisional_container_size_bytes,
                    expected_size_limit_bytes=args.expected_size_limit_bytes,
                    connect_timeout_ms=args.connect_timeout_ms,
                    read_timeout_ms=args.read_timeout_ms,
                    truststore_path=args.cafile,
                    server_hostname=args.server_hostname,
                    control_client_factory=control_client_factory,
                    tls_context_factory=tls_context_factory,
                )
            report = mount_handoff_report_text(handoff)
            out.write(report)
            if args.handoff_report:
                _write_report(Path(args.handoff_report), report)
            return 0
        handoff_path = Path(args.handoff).resolve()
        handoff = read_mount_handoff(handoff_path)
        if args.command == "launch-plan":
            report = mount_launch_plan_text(
                handoff,
                handoff_path,
                adapter_binary=args.adapter_binary,
            )
            out.write(report)
            if args.report:
                _write_report(Path(args.report), report)
            return 0
        if args.command == "list":
            with credential_prompt() as credentials:
                listing = connect_and_list_mount_handoff(
                    credentials,
                    handoff,
                    path=args.path,
                    control_client_factory=control_client_factory,
                    tls_context_factory=tls_context_factory,
                )
            report = mount_list_report_text(handoff, handoff_path, listing)
            out.write(report)
            if args.report:
                _write_report(Path(args.report), report)
            return 0
        parser.print_help(out)
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        print(str(exc), file=err)
        return 1


def main() -> None:
    raise SystemExit(run_mount_handoff_cli())


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cxnfs-mount")
    subcommands = parser.add_subparsers(dest="command", required=True)
    negotiate = subcommands.add_parser("negotiate")
    negotiate.add_argument("backend")
    negotiate.add_argument("--capabilities", default="list,read")
    negotiate.add_argument("--host", default="127.0.0.1")
    negotiate.add_argument("--control-port", type=int, default=4509)
    negotiate.add_argument("--transport-port", type=int, default=4510)
    negotiate.add_argument("--server-hostname")
    negotiate.add_argument("--cafile")
    negotiate.add_argument("--remote-root", default="/")
    negotiate.add_argument("--mount-point", required=True)
    negotiate.add_argument("--handoff-report")
    negotiate.add_argument("--initial-cords", type=int, default=1)
    negotiate.add_argument("--max-cords", type=int, default=ABSOLUTE_MAX_CORDS)
    negotiate.add_argument("--provisional-container-size-bytes", type=int, default=64 * 1024 * 1024)
    negotiate.add_argument("--expected-size-limit-bytes", type=int, default=0)
    negotiate.add_argument("--connect-timeout-ms", type=int, default=15_000)
    negotiate.add_argument("--read-timeout-ms", type=int, default=120_000)
    launch = subcommands.add_parser("launch-plan")
    launch.add_argument("handoff")
    launch.add_argument("--adapter-binary")
    launch.add_argument("--report")
    list_command = subcommands.add_parser("list")
    list_command.add_argument("handoff")
    list_command.add_argument("--path")
    list_command.add_argument("--report")
    return parser


def _write_report(path: Path, text: str) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _parse_key_value_lines(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for index, line in enumerate(lines, start=2):
        if not line.strip():
            continue
        if "=" not in line or line.startswith("="):
            raise ValueError(f"invalid mount handoff field on line {index}")
        key, value = line.split("=", 1)
        fields[key] = value
    return fields


def _required(fields: dict[str, str], key: str) -> str:
    value = fields.get(key)
    if value is None or not value:
        raise ValueError(f"mount handoff missing field: {key}")
    return value


def _integer(fields: dict[str, str], key: str) -> int:
    return int(_required(fields, key))


def _require_false(fields: dict[str, str], key: str) -> None:
    value = _required(fields, key)
    if value != "false":
        raise ValueError(f"mount handoff rejects inline secret field: {key}")


def _optional_path(value: str | None) -> Path | None:
    return None if value is None or not value else Path(value)


def _backend(value: str) -> MountBackend:
    normalized = _normalize_token(value)
    for backend in MountBackend:
        if _normalize_token(backend.name) == normalized:
            return backend
    raise ValueError(f"unknown mount backend: {value}")


def _backend_name(backend: MountBackend) -> str:
    return backend.name.lower()


def _capabilities(value: str) -> tuple[MountCapability, ...]:
    capabilities: list[MountCapability] = []
    for token in value.split(","):
        normalized = _normalize_token(token)
        if not normalized:
            continue
        for capability in MountCapability:
            if _normalize_token(capability.name) == normalized:
                capabilities.append(capability)
                break
        else:
            raise ValueError(f"unknown mount capability: {token}")
    return _normalize_capabilities(tuple(capabilities))


def _normalize_capabilities(capabilities: tuple[MountCapability, ...]) -> tuple[MountCapability, ...]:
    normalized = tuple(sorted({MountCapability(capability) for capability in capabilities}, key=lambda item: item.value))
    if not normalized:
        raise ValueError("at least one mount capability is required")
    return normalized


def _capability_names(capabilities: tuple[MountCapability, ...]) -> str:
    return ",".join(capability.name.lower() for capability in _normalize_capabilities(capabilities))


def _strategy_from_response(response: Any) -> MountStrategy:
    if isinstance(response, MountNegotiatedResponse):
        return response.strategy
    strategy = getattr(response, "strategy", response)
    if not isinstance(strategy, MountStrategy):
        raise TypeError(f"expected MountStrategy, got {type(strategy).__name__}")
    return strategy


def _adapter_kind(backend: MountBackend) -> str:
    if backend in (MountBackend.LINUX_FUSE, MountBackend.MACOS_FUSE_T, MountBackend.MACOS_MACFUSE):
        return "fuse"
    if backend == MountBackend.WINDOWS_WINFSP:
        return "winfsp"
    if backend == MountBackend.WINDOWS_PROJFS:
        return "projfs"
    return "cli"


def _adapter_binary(backend: MountBackend) -> str:
    if backend == MountBackend.LINUX_FUSE:
        return "cxnfs-fuse"
    if backend == MountBackend.MACOS_FUSE_T:
        return "cxnfs-fuse-t"
    if backend == MountBackend.MACOS_MACFUSE:
        return "cxnfs-macfuse"
    if backend == MountBackend.WINDOWS_WINFSP:
        return "cxnfs-winfsp"
    if backend == MountBackend.WINDOWS_PROJFS:
        return "cxnfs-projfs"
    return "cxnfs"


def _normalize_token(value: str) -> str:
    return value.strip().lower().replace("-", "_")


if __name__ == "__main__":
    main()
