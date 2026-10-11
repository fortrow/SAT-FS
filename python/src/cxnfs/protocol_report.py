from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import TextIO

from .bulk import (
    decode_bulk_frame,
    decode_bulk_read_request,
    decode_bulk_write_ack,
    encode_bulk_frame,
    encode_bulk_read_request,
    encode_bulk_write_ack,
)
from .control import (
    ControlOperation,
    decode_control_frame,
    decode_control_payload,
    encode_control_frame,
    encode_control_payload,
)


@dataclass(frozen=True)
class ProtocolFixtureReport:
    fixture_root: Path
    fixture_files: int
    control_fixture_files: int
    bulk_fixture_files: int
    control_operations_covered: tuple[ControlOperation, ...]
    control_operations_missing: tuple[ControlOperation, ...]
    bulk_fixtures_covered: tuple[str, ...]
    issues: tuple[str, ...]

    @property
    def clean(self) -> bool:
        return not self.issues

    @property
    def coverage_complete(self) -> bool:
        return not self.control_operations_missing


def scan_protocol_fixtures(fixture_root: str | Path) -> ProtocolFixtureReport:
    root = Path(fixture_root).resolve()
    if not root.is_dir():
        raise OSError(f"fixture root is not a directory: {root}")

    fixture_paths = sorted(root.glob("*.hex"), key=lambda path: path.name)
    covered: set[ControlOperation] = set()
    bulk_fixtures: set[str] = set()
    issues: list[str] = []
    control_count = 0
    bulk_count = 0

    for path in fixture_paths:
        name = path.name
        try:
            fixture = _fixture_bytes(path)
        except Exception as exc:
            issues.append(f"{name}: invalid hex fixture: {exc}")
            continue

        try:
            if name.startswith("control-"):
                control_count += 1
                operation = _scan_control_fixture(name, fixture, issues)
                if operation is not None:
                    covered.add(operation)
            elif name == "bulk-read-request.hex":
                bulk_count += 1
                request = decode_bulk_read_request(fixture)
                if encode_bulk_read_request(request) != fixture:
                    issues.append(f"{name}: bulk read request did not round trip")
                bulk_fixtures.add("BULK_READ_REQUEST")
            elif name == "bulk-data-frame.hex":
                bulk_count += 1
                frame = decode_bulk_frame(fixture)
                if encode_bulk_frame(frame) != fixture:
                    issues.append(f"{name}: bulk data frame did not round trip")
                bulk_fixtures.add("BULK_DATA_FRAME")
            elif name == "bulk-write-ack.hex":
                bulk_count += 1
                ack = decode_bulk_write_ack(fixture)
                if encode_bulk_write_ack(ack) != fixture:
                    issues.append(f"{name}: bulk write acknowledgement did not round trip")
                bulk_fixtures.add("BULK_WRITE_ACK")
            else:
                issues.append(f"{name}: unknown fixture kind")
        except Exception as exc:
            issues.append(f"{name}: decode failure: {exc}")

    covered_sorted = tuple(sorted(covered, key=lambda operation: operation.value))
    missing = tuple(operation for operation in ControlOperation if operation not in covered)
    return ProtocolFixtureReport(
        fixture_root=root,
        fixture_files=len(fixture_paths),
        control_fixture_files=control_count,
        bulk_fixture_files=bulk_count,
        control_operations_covered=covered_sorted,
        control_operations_missing=missing,
        bulk_fixtures_covered=tuple(sorted(bulk_fixtures)),
        issues=tuple(issues),
    )


def format_protocol_fixture_report(report: ProtocolFixtureReport) -> str:
    lines = [
        "cxnfs.protocol.fixtures.v1",
        f"fixture_root={report.fixture_root}",
        f"fixture_files={report.fixture_files}",
        f"control_fixture_files={report.control_fixture_files}",
        f"bulk_fixture_files={report.bulk_fixture_files}",
        f"control_operations_covered_count={len(report.control_operations_covered)}",
        f"control_operations_covered={_join_operations(report.control_operations_covered)}",
        f"control_operations_missing_count={len(report.control_operations_missing)}",
        f"control_operations_missing={_join_operations(report.control_operations_missing)}",
        f"bulk_fixtures_covered_count={len(report.bulk_fixtures_covered)}",
        f"bulk_fixtures_covered={','.join(report.bulk_fixtures_covered)}",
        f"coverage_complete={str(report.coverage_complete).lower()}",
        f"issue_count={len(report.issues)}",
    ]
    lines.extend(f"issue.{index}={issue}" for index, issue in enumerate(report.issues))
    lines.append(f"status={'clean' if report.clean else 'issues'}")
    return "\n".join(lines) + "\n"


def run_protocol_fixture_report_cli(
    argv: list[str] | tuple[str, ...] | None = None,
    *,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    parser = _parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
    except SystemExit as exc:
        return int(exc.code)

    try:
        report = scan_protocol_fixtures(args.fixture_dir)
    except Exception as exc:
        print(f"failed to scan protocol fixtures: {exc}", file=err)
        return 2

    text = format_protocol_fixture_report(report)
    out.write(text)
    if args.report:
        report_path = Path(args.report).resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(text, encoding="utf-8")
        print(f"report={report_path}", file=out)
    if args.fail_on_issues and not report.clean:
        return 1
    if args.fail_on_missing and not report.coverage_complete:
        return 1
    return 0


def main() -> None:
    raise SystemExit(run_protocol_fixture_report_cli())


def _scan_control_fixture(
    name: str,
    fixture: bytes,
    issues: list[str],
) -> ControlOperation | None:
    frame = decode_control_frame(fixture)
    payload = decode_control_payload(frame.operation, frame.payload)
    payload_bytes = encode_control_payload(payload)
    if payload_bytes != frame.payload:
        issues.append(f"{name}: control payload did not round trip")
    encoded_frame = encode_control_frame(type(frame)(
        frame.operation,
        payload_bytes,
        frame.message_id,
        frame.session_id,
        frame.protocol_major,
        frame.protocol_minor,
    ))
    if encoded_frame != fixture:
        issues.append(f"{name}: control frame did not round trip")
    return frame.operation


def _fixture_bytes(path: Path) -> bytes:
    return bytes.fromhex("".join(path.read_text(encoding="utf-8").split()))


def _join_operations(operations: tuple[ControlOperation, ...]) -> str:
    return ",".join(operation.name.lower() for operation in operations)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cxnfs-protocol-fixtures")
    parser.add_argument("fixture_dir")
    parser.add_argument("--report", help="write stable key/value fixture report")
    parser.add_argument("--fail-on-issues", type=_bool_arg, default=True)
    parser.add_argument("--fail-on-missing", type=_bool_arg, default=False)
    return parser


def _bool_arg(value: str) -> bool:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise argparse.ArgumentTypeError("expected true or false")


if __name__ == "__main__":
    main()
