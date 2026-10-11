from __future__ import annotations

import io
import json
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, TextIO

from .torch_dataset import (
    apply_blockfs_workload_requirements,
    attach_server_evidence_to_trainer_workload_payload,
    build_trainer_surface_check_payload,
    run_trainer_contract_benchmark_cli,
    run_trainer_surface_matrix_cli,
    run_trainer_workload_benchmark_cli,
)
from .workload_evidence import collect_workload_evidence


WORKLOAD_DECISION_FORMAT = "cxnfs.workload-decision.v1"
BLOCKFS_HANDOFF_FORMAT = "cxnfs.blockfs-handoff.v1"
DEFAULT_WORKLOAD_REPORT = "trainer-workload-benchmark.json"
DEFAULT_DECISION_JSON = "workload-decision.json"
DEFAULT_DECISION_REPORT = "decision.report"
DEFAULT_BLOCKFS_HANDOFF_JSON = "blockfs-handoff.json"
DEFAULT_EVIDENCE_DIR = "server-evidence"
DEFAULT_MATRIX_REPORT = "trainer-surface-matrix.json"
DEFAULT_SURFACE_CHECK_JSON = "trainer-surface-check.json"


def run_workload_decision_cli(
    argv: Sequence[str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    *,
    trainer_workload_runner: Callable[..., int] | None = None,
    trainer_matrix_runner: Callable[..., int] | None = None,
    evidence_collector: Callable[..., Mapping[str, Path]] = collect_workload_evidence,
) -> int:
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    parser = _decision_parser()
    try:
        raw_argv = list(argv) if argv is not None else sys.argv[1:]
        leading_root = None
        if raw_argv and not raw_argv[0].startswith("-"):
            leading_root = raw_argv.pop(0)
        args, benchmark_args = parser.parse_known_args(raw_argv)
        if leading_root is not None and args.root is None:
            args.root = leading_root
        _validate_decision_inputs(args)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        workload_report = _inside(output_dir, args.workload_report, DEFAULT_WORKLOAD_REPORT)
        matrix_report = _inside(output_dir, args.matrix_report, DEFAULT_MATRIX_REPORT)
        decision_json = _inside(output_dir, args.decision_json, DEFAULT_DECISION_JSON)
        decision_report = _inside(output_dir, args.decision_report, DEFAULT_DECISION_REPORT)
        blockfs_handoff_json = _inside(output_dir, args.blockfs_handoff_json, DEFAULT_BLOCKFS_HANDOFF_JSON)
        surface_check_json = _inside(output_dir, args.surface_check_json, DEFAULT_SURFACE_CHECK_JSON)
        surface_contract_path = Path(args.surface_contract) if args.surface_contract else (
            Path(args.contract) if args.contract else None
        )
        surface_contract = _read_json_object(surface_contract_path) if surface_contract_path else None

        evidence_dir: Path | None = None
        evidence_paths = _known_evidence_paths(args)
        if args.collect_local_server_evidence:
            if evidence_paths:
                raise ValueError("--collect-local-server-evidence cannot be combined with server evidence paths")
            evidence_dir = _inside(output_dir, args.evidence_dir, DEFAULT_EVIDENCE_DIR)
            evidence_paths = {
                "disk": str(evidence_dir / "disk.report"),
                "cache": str(evidence_dir / "cache.json"),
                "cpu": str(evidence_dir / "cpu.report"),
            }
        if args.input_workload_report:
            benchmark_exit_code = args.benchmark_exit_code
            workload_payload = _read_existing_workload_report(Path(args.input_workload_report))
        else:
            benchmark_stdout = io.StringIO()
            benchmark_stderr = io.StringIO()
            benchmark_exit_code: int | None = None
            benchmark_argv = _benchmark_argv(
                args,
                benchmark_args,
                workload_report,
                {} if args.collect_local_server_evidence else evidence_paths,
            )
            selected_trainer_workload_runner = trainer_workload_runner or (
                run_trainer_contract_benchmark_cli if args.contract else run_trainer_workload_benchmark_cli
            )

            def run_benchmark() -> None:
                nonlocal benchmark_exit_code
                if benchmark_exit_code is not None:
                    raise RuntimeError("trainer workload benchmark already ran")
                benchmark_exit_code = selected_trainer_workload_runner(
                    benchmark_argv,
                    stdout=benchmark_stdout,
                    stderr=benchmark_stderr,
                )

            if args.collect_local_server_evidence:
                collected = evidence_collector(
                    evidence_dir,
                    disk_device=args.disk_device,
                    sample_seconds=args.evidence_sample_seconds,
                    proc_root=args.proc_root,
                    sleep_fn=lambda _seconds: run_benchmark(),
                )
                evidence_paths = {name: str(path) for name, path in collected.items()}
            else:
                run_benchmark()

            assert benchmark_exit_code is not None
            if benchmark_stderr.getvalue():
                err.write(benchmark_stderr.getvalue())
            if not workload_report.exists():
                err.write(f"trainer workload report was not written: {workload_report}\n")
                return benchmark_exit_code or 1
            workload_payload = json.loads(workload_report.read_text(encoding="utf-8"))

        if evidence_paths:
            workload_payload = attach_server_evidence_to_trainer_workload_payload(
                workload_payload,
                disk_evidence=evidence_paths.get("disk"),
                cache_evidence=evidence_paths.get("cache"),
                cpu_evidence=evidence_paths.get("cpu"),
            )
        workload_payload = apply_blockfs_workload_requirements(
            workload_payload,
            min_runs=args.blockfs_min_runs,
            min_samples=args.blockfs_min_samples,
            min_bytes=args.blockfs_min_bytes,
            min_cord_sweep=args.blockfs_min_cord_sweep,
        )
        matrix_payload = None
        matrix_exit_code = None
        if args.input_matrix_report:
            matrix_payload = _read_existing_matrix_report(Path(args.input_matrix_report))
            matrix_report.write_text(
                json.dumps(matrix_payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            matrix_exit_code = 0
        elif args.run_surface_matrix:
            matrix_stdout = io.StringIO()
            matrix_stderr = io.StringIO()
            matrix_argv = _matrix_argv(args, benchmark_args, matrix_report, surface_contract)
            selected_matrix_runner = trainer_matrix_runner or run_trainer_surface_matrix_cli
            matrix_exit_code = selected_matrix_runner(
                matrix_argv,
                stdout=matrix_stdout,
                stderr=matrix_stderr,
            )
            if matrix_stderr.getvalue():
                err.write(matrix_stderr.getvalue())
            if not matrix_report.exists():
                err.write(f"trainer surface matrix report was not written: {matrix_report}\n")
                return matrix_exit_code or 1
            matrix_payload = _read_existing_matrix_report(matrix_report)
        surface_check_payload = None
        if surface_contract is not None:
            surface_check_payload = build_trainer_surface_check_payload(
                surface_contract,
                workload_payload=workload_payload,
                matrix_payload=matrix_payload,
                require_remote=args.surface_require_remote,
                require_quic=args.surface_require_quic,
                require_workload=True,
                require_matrix=args.surface_require_matrix,
                min_sources=args.surface_min_sources,
                min_validation_samples=args.surface_min_validation_samples,
                min_validation_bytes=args.surface_min_validation_bytes,
                min_workload_runs=args.surface_min_workload_runs,
                min_workload_samples=args.surface_min_workload_samples,
                min_workload_bytes=args.surface_min_workload_bytes,
                min_cord_sweep=args.surface_min_cord_sweep,
            )
            surface_check_json.write_text(
                json.dumps(surface_check_payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        workload_report.write_text(json.dumps(workload_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        decision_payload = build_workload_decision_payload(
            workload_payload,
            workload_report=workload_report,
            matrix_report=matrix_report if matrix_payload is not None else None,
            decision_json=decision_json,
            decision_report=decision_report,
            blockfs_handoff_report=blockfs_handoff_json,
            surface_check_report=surface_check_json if surface_check_payload is not None else None,
            surface_check_payload=surface_check_payload,
            matrix_payload=matrix_payload,
            evidence_paths=evidence_paths,
            benchmark_exit_code=benchmark_exit_code,
            matrix_exit_code=matrix_exit_code,
        )
        blockfs_handoff_payload = build_blockfs_handoff_payload(
            decision_payload,
            blockfs_handoff_report=blockfs_handoff_json,
        )
        blockfs_handoff_json.write_text(
            json.dumps(blockfs_handoff_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        text = json.dumps(decision_payload, indent=None if args.compact else 2, sort_keys=True) + "\n"
        decision_json.write_text(text, encoding="utf-8")
        decision_report.write_text(format_workload_decision_report(decision_payload), encoding="utf-8")
        out.write(text)
        if args.fail_on_surface_check and surface_check_payload is not None and not surface_check_payload.get("passed"):
            return benchmark_exit_code or 1
        if args.fail_on_surface_check and matrix_exit_code not in (None, 0):
            return matrix_exit_code or 1
        return benchmark_exit_code
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        err.write(str(exc) + "\n")
        return 1


def build_workload_decision_payload(
    workload_payload: Mapping[str, Any],
    *,
    workload_report: Path,
    matrix_report: Path | None = None,
    decision_json: Path | None = None,
    decision_report: Path,
    blockfs_handoff_report: Path | None = None,
    evidence_paths: Mapping[str, str],
    benchmark_exit_code: int,
    matrix_exit_code: int | None = None,
    surface_check_report: Path | None = None,
    surface_check_payload: Mapping[str, Any] | None = None,
    matrix_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    blockfs_gate = _mapping(workload_payload.get("blockfs_gate"))
    network_summary = _mapping(workload_payload.get("network_path_summary"))
    server_evidence = _mapping(workload_payload.get("server_evidence"))
    weighted_plan = _mapping(workload_payload.get("weighted_mds_plan"))
    pressure_profile = _mapping(workload_payload.get("pressure_profile"))
    read_path_reliability = _mapping(workload_payload.get("read_path_reliability"))
    read_path_summary = _read_path_decision_summary(workload_payload, blockfs_gate)
    memory_path_summary = _memory_path_decision_summary(workload_payload, blockfs_gate)
    write_path_summary = _write_path_decision_summary(workload_payload, blockfs_gate)
    surface_summary = _surface_check_summary(surface_check_report, surface_check_payload)
    matrix_summary = _surface_matrix_summary(matrix_report, matrix_payload)
    return {
        "format": WORKLOAD_DECISION_FORMAT,
        "workload_report": {
            "path": str(workload_report),
            "format": workload_payload.get("format"),
            "workload_name": workload_payload.get("workload_name"),
        },
        "workload_fingerprint": _mapping(workload_payload.get("workload_fingerprint")) or None,
        "trainer_surface_matrix": matrix_summary,
        "trainer_surface_matrix_passed": bool(matrix_summary.get("passed")) if matrix_summary else None,
        "decision_json": str(decision_json) if decision_json is not None else None,
        "decision_report": str(decision_report),
        "blockfs_handoff": {
            "path": str(blockfs_handoff_report) if blockfs_handoff_report is not None else None,
            "format": BLOCKFS_HANDOFF_FORMAT,
            "decision": blockfs_gate.get("decision"),
            "kernel_module_allowed": bool(blockfs_gate.get("kernel_module_allowed")),
        },
        "trainer_surface_check": surface_summary,
        "trainer_surface_check_passed": bool(surface_summary.get("passed")) if surface_summary else None,
        "benchmark_exit_code": benchmark_exit_code,
        "matrix_exit_code": matrix_exit_code,
        "benchmark_completed": benchmark_exit_code == 0,
        "matrix_completed": matrix_exit_code == 0 if matrix_exit_code is not None else None,
        "transport": workload_payload.get("transport"),
        "bulk_transport": workload_payload.get("bulk_transport"),
        "read_mode": workload_payload.get("read_mode"),
        "remote_root": workload_payload.get("remote_root"),
        "contract_applied": bool(workload_payload.get("contract_applied")),
        "trainer_contract": _trainer_contract_summary(workload_payload.get("trainer_contract")),
        "weighted_source_count": len(weighted_plan.get("sources", ())) if isinstance(weighted_plan.get("sources"), list) else 0,
        "run_count": int(workload_payload.get("run_count", 0) or 0),
        "sample_count": int(workload_payload.get("sample_count", 0) or 0),
        "byte_size": int(workload_payload.get("byte_size", 0) or 0),
        "mebibytes_per_second": float(workload_payload.get("mebibytes_per_second", 0.0) or 0.0),
        "pressure_profile": dict(pressure_profile),
        "pressure_profile_complete": bool(pressure_profile.get("decision_ready")),
        "read_path_reliability": dict(read_path_reliability),
        "read_path_reliability_complete": bool(read_path_reliability.get("complete")),
        "unrecovered_read_failures": int(read_path_reliability.get("unrecovered_read_failures", 0) or 0),
        "target_gap_mebibytes_per_second": pressure_profile.get("target_gap_mebibytes_per_second"),
        "cord_plateau_observed": pressure_profile.get("cord_plateau_observed"),
        "cord_sweep_count": int(network_summary.get("cord_sweep_count", 0) or 0),
        "workload_scale_complete": bool(blockfs_gate.get("workload_scale_complete")),
        "workload_scale_gate": dict(_mapping(blockfs_gate.get("workload_scale_gate"))),
        "java_python_network_path_exercised": bool(network_summary.get("java_python_network_path_exercised")),
        "read_path_evidence": read_path_summary,
        "read_path_evidence_complete": bool(read_path_summary.get("complete")),
        "memory_path_evidence": memory_path_summary,
        "memory_path_evidence_complete": bool(memory_path_summary.get("complete")),
        "write_path_evidence": write_path_summary,
        "write_path_evidence_complete": bool(write_path_summary.get("complete")),
        "server_evidence_complete": bool(server_evidence.get("complete")),
        "server_evidence_sample_window_complete": bool(server_evidence.get("sample_window_complete")),
        "server_evidence_sample_window": dict(_mapping(server_evidence.get("sample_window"))),
        "server_evidence_paths": dict(evidence_paths),
        "trainer_surface_verified": _trainer_surface_verified(workload_payload),
        "objective": workload_payload.get("objective"),
        "objective_passed_any": workload_payload.get("objective_passed_any"),
        "objective_passed_all": workload_payload.get("objective_passed_all"),
        "blockfs_gate": dict(blockfs_gate),
        "kernel_module_allowed": bool(blockfs_gate.get("kernel_module_allowed")),
        "required_next_evidence": list(blockfs_gate.get("required_next_evidence", ())),
    }


def build_blockfs_handoff_payload(
    decision_payload: Mapping[str, Any],
    *,
    blockfs_handoff_report: Path | None = None,
) -> dict[str, Any]:
    blockfs_gate = _mapping(decision_payload.get("blockfs_gate"))
    pressure_profile = _mapping(decision_payload.get("pressure_profile"))
    workload_report = _mapping(decision_payload.get("workload_report"))
    surface_check = _mapping(decision_payload.get("trainer_surface_check"))
    surface_matrix = _mapping(decision_payload.get("trainer_surface_matrix"))
    allowed = bool(decision_payload.get("kernel_module_allowed"))
    readiness = {
        "benchmark_completed": bool(decision_payload.get("benchmark_completed")),
        "trainer_surface_verified": bool(decision_payload.get("trainer_surface_verified")),
        "trainer_surface_check_passed": decision_payload.get("trainer_surface_check_passed"),
        "trainer_surface_matrix_passed": decision_payload.get("trainer_surface_matrix_passed"),
        "java_python_network_path_exercised": bool(decision_payload.get("java_python_network_path_exercised")),
        "pressure_profile_complete": bool(decision_payload.get("pressure_profile_complete")),
        "read_path_reliability_complete": bool(decision_payload.get("read_path_reliability_complete")),
        "workload_scale_complete": bool(decision_payload.get("workload_scale_complete")),
        "read_path_evidence_complete": bool(decision_payload.get("read_path_evidence_complete")),
        "memory_path_evidence_complete": bool(decision_payload.get("memory_path_evidence_complete")),
        "write_path_evidence_complete": bool(decision_payload.get("write_path_evidence_complete")),
        "server_evidence_complete": bool(decision_payload.get("server_evidence_complete")),
        "server_evidence_sample_window_complete": bool(
            decision_payload.get("server_evidence_sample_window_complete")
        ),
    }
    missing_checks = [
        name
        for name, value in readiness.items()
        if value is False or value is None
    ]
    candidate = None
    if allowed:
        candidate = {
            "safe_to_start_kernel_module": True,
            "source_pressure_profile_complete": bool(pressure_profile.get("decision_ready")),
            "must_repeat_remote_workload_benchmark": True,
            "required_reports": list(decision_payload.get("required_next_evidence", ())),
            "metadata_root_policy": "store blockfs metadata, structures, journals, cache indexes, and slack on the OS ext/zfs root",
            "container_cache_policy": "verified container and chunk cache may live on one or more partner drives",
            "publish_policy": "daemon verifies fetched extents before the kernel publishes cached pages",
        }
    return {
        "format": BLOCKFS_HANDOFF_FORMAT,
        "path": str(blockfs_handoff_report) if blockfs_handoff_report is not None else None,
        "status": "ready-for-linux-blockfs-prototype" if allowed else "deferred",
        "decision": blockfs_gate.get("decision"),
        "reason": blockfs_gate.get("reason"),
        "kernel_module_allowed": allowed,
        "required_next_evidence": list(decision_payload.get("required_next_evidence", ())),
        "missing_readiness_checks": missing_checks,
        "decision_bundle": {
            "workload_report": workload_report.get("path"),
            "decision_json": decision_payload.get("decision_json"),
            "decision_report": decision_payload.get("decision_report"),
            "surface_check": surface_check.get("path"),
            "surface_matrix": surface_matrix.get("path"),
        },
        "workload": {
            "workload_name": workload_report.get("workload_name"),
            "workload_format": workload_report.get("format"),
            "workload_fingerprint": _mapping(decision_payload.get("workload_fingerprint")) or None,
            "transport": decision_payload.get("transport"),
            "bulk_transport": decision_payload.get("bulk_transport"),
            "read_mode": decision_payload.get("read_mode"),
            "remote_root": decision_payload.get("remote_root"),
            "run_count": int(decision_payload.get("run_count", 0) or 0),
            "sample_count": int(decision_payload.get("sample_count", 0) or 0),
            "byte_size": int(decision_payload.get("byte_size", 0) or 0),
            "weighted_source_count": int(decision_payload.get("weighted_source_count", 0) or 0),
        },
        "network_proof": {
            "cord_sweep_count": int(decision_payload.get("cord_sweep_count", 0) or 0),
            "cord_plateau_observed": bool(decision_payload.get("cord_plateau_observed")),
            "java_python_network_path_exercised": bool(decision_payload.get("java_python_network_path_exercised")),
            "target_gap_mebibytes_per_second": decision_payload.get("target_gap_mebibytes_per_second"),
        },
        "server_evidence": {
            "complete": bool(decision_payload.get("server_evidence_complete")),
            "sample_window_complete": bool(decision_payload.get("server_evidence_sample_window_complete")),
            "sample_window": dict(_mapping(decision_payload.get("server_evidence_sample_window"))),
            "paths": dict(_mapping(decision_payload.get("server_evidence_paths"))),
        },
        "read_path_evidence": dict(_mapping(decision_payload.get("read_path_evidence"))),
        "read_path_reliability": dict(_mapping(decision_payload.get("read_path_reliability"))),
        "memory_path_evidence": dict(_mapping(decision_payload.get("memory_path_evidence"))),
        "write_path_evidence": dict(_mapping(decision_payload.get("write_path_evidence"))),
        "pressure_profile": dict(pressure_profile),
        "linux_blockfs_candidate": candidate,
    }


def format_workload_decision_report(payload: Mapping[str, Any]) -> str:
    blockfs_gate = _mapping(payload.get("blockfs_gate"))
    blockfs_handoff = _mapping(payload.get("blockfs_handoff"))
    lines = [
        WORKLOAD_DECISION_FORMAT,
        f"workload_report={_mapping(payload.get('workload_report')).get('path', '')}",
        f"blockfs_handoff={blockfs_handoff.get('path', '')}",
        f"blockfs_handoff_format={blockfs_handoff.get('format', '')}",
        f"benchmark_exit_code={payload.get('benchmark_exit_code', '')}",
        f"benchmark_completed={str(bool(payload.get('benchmark_completed'))).lower()}",
        f"transport={payload.get('transport', '')}",
        f"bulk_transport={payload.get('bulk_transport', '')}",
        f"read_mode={payload.get('read_mode', '')}",
        f"trainer_surface_verified={str(bool(payload.get('trainer_surface_verified'))).lower()}",
        f"trainer_surface_matrix_passed={str(bool(payload.get('trainer_surface_matrix_passed'))).lower()}",
        f"trainer_surface_check_passed={str(bool(payload.get('trainer_surface_check_passed'))).lower()}",
        f"pressure_profile_complete={str(bool(payload.get('pressure_profile_complete'))).lower()}",
        f"read_path_reliability_complete={str(bool(payload.get('read_path_reliability_complete'))).lower()}",
        f"unrecovered_read_failures={payload.get('unrecovered_read_failures', '')}",
        f"cord_plateau_observed={str(bool(payload.get('cord_plateau_observed'))).lower()}",
        f"workload_scale_complete={str(bool(payload.get('workload_scale_complete'))).lower()}",
        f"java_python_network_path_exercised={str(bool(payload.get('java_python_network_path_exercised'))).lower()}",
        f"read_path_evidence_complete={str(bool(payload.get('read_path_evidence_complete'))).lower()}",
        f"memory_path_evidence_complete={str(bool(payload.get('memory_path_evidence_complete'))).lower()}",
        f"write_path_evidence_complete={str(bool(payload.get('write_path_evidence_complete'))).lower()}",
        f"server_evidence_complete={str(bool(payload.get('server_evidence_complete'))).lower()}",
        f"server_evidence_sample_window_complete={str(bool(payload.get('server_evidence_sample_window_complete'))).lower()}",
        f"blockfs_decision={blockfs_gate.get('decision', '')}",
        f"kernel_module_allowed={str(bool(payload.get('kernel_module_allowed'))).lower()}",
    ]
    for index, value in enumerate(payload.get("required_next_evidence", ())):
        lines.append(f"required_next_evidence_{index}={value}")
    return "\n".join(lines + [""])


def main() -> None:
    raise SystemExit(run_workload_decision_cli())


def _decision_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="cxnfs-workload-decision",
        description="Run a weighted trainer workload benchmark and write the blockfs decision bundle.",
        allow_abbrev=False,
    )
    parser.add_argument("--root", help="Directory containing one MDS dataset directory per source name.")
    parser.add_argument("--weights", help="Comma-separated source weights, for example chat=4,summary=1.")
    parser.add_argument("--contract", help="Path to a cxnfs.trainer-contract.v1 report to drive the workload benchmark.")
    parser.add_argument("--input-workload-report", help="Existing trainer workload benchmark JSON to copy into the decision bundle.")
    parser.add_argument("--benchmark-exit-code", type=int, default=0, help="Exit code to record when using --input-workload-report.")
    parser.add_argument("--output-dir", required=True, help="Directory for the workload report, evidence, and decision files.")
    parser.add_argument("--workload-report", help="Workload benchmark JSON path, relative to --output-dir unless absolute.")
    parser.add_argument("--matrix-report", help="Surface matrix JSON path, relative to --output-dir unless absolute.")
    parser.add_argument("--input-matrix-report", help="Existing cxnfs.trainer-surface-matrix.v1 JSON report to copy into the decision bundle.")
    parser.add_argument("--run-surface-matrix", action="store_true", help="Run the trainer surface matrix and include it in the decision bundle.")
    parser.add_argument("--decision-json", help="Decision JSON path, relative to --output-dir unless absolute.")
    parser.add_argument("--decision-report", help="Decision key/value report path, relative to --output-dir unless absolute.")
    parser.add_argument("--blockfs-handoff-json", help="Blockfs handoff JSON path, relative to --output-dir unless absolute.")
    parser.add_argument("--surface-contract", help="Trainer contract JSON to use for the bundled surface check.")
    parser.add_argument("--surface-check-json", help="Surface-check JSON path, relative to --output-dir unless absolute.")
    parser.add_argument("--surface-require-matrix", action="store_true", help="Require a matrix report in the bundled surface check.")
    parser.add_argument("--surface-require-remote", action="store_true", help="Require remote CXNFS reads in the bundled surface check.")
    parser.add_argument("--surface-require-quic", action="store_true", help="Require QUIC read-path proof in the bundled surface check.")
    parser.add_argument("--surface-min-sources", type=int, default=1, help="Minimum weighted source count for the bundled surface check.")
    parser.add_argument("--surface-min-validation-samples", type=int, default=1, help="Minimum contract validation samples for the bundled surface check.")
    parser.add_argument("--surface-min-validation-bytes", type=int, default=1, help="Minimum contract validation bytes for the bundled surface check.")
    parser.add_argument("--surface-min-workload-runs", type=int, default=1, help="Minimum workload runs for the bundled surface check.")
    parser.add_argument("--surface-min-workload-samples", type=int, default=1, help="Minimum workload samples for the bundled surface check.")
    parser.add_argument("--surface-min-workload-bytes", type=int, default=1, help="Minimum workload bytes for the bundled surface check.")
    parser.add_argument("--surface-min-cord-sweep", type=int, default=1, help="Minimum cord sweep entries for the bundled surface check.")
    parser.add_argument("--fail-on-surface-check", action="store_true", help="Return nonzero if the bundled surface check fails.")
    parser.add_argument("--blockfs-min-runs", type=int, default=0, help="Minimum workload runs required before the blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-samples", type=int, default=0, help="Minimum workload samples required before the blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-bytes", type=int, default=0, help="Minimum workload bytes required before the blockfs gate can allow kernel work.")
    parser.add_argument("--blockfs-min-cord-sweep", type=int, default=0, help="Minimum cord sweep entries required before the blockfs gate can allow kernel work.")
    parser.add_argument("--collect-local-server-evidence", action="store_true", help="Sample local Linux server counters across the benchmark run.")
    parser.add_argument("--evidence-dir", help="Evidence output directory, relative to --output-dir unless absolute.")
    parser.add_argument("--server-disk-evidence", help="Existing disk evidence file.")
    parser.add_argument("--server-cache-evidence", help="Existing cache evidence file.")
    parser.add_argument("--server-cpu-evidence", help="Existing CPU evidence file.")
    parser.add_argument("--disk-device", help="Linux disk device to sample when collecting local evidence.")
    parser.add_argument("--evidence-sample-seconds", type=float, default=0.0, help="Minimum evidence sample seconds; 0 samples exactly across the benchmark window.")
    parser.add_argument("--proc-root", default="/proc", help="Linux proc root to sample when collecting local evidence.")
    parser.add_argument("--compact", action="store_true", help="Emit compact decision JSON.")
    return parser


def _benchmark_argv(
    args: Any,
    benchmark_args: Sequence[str],
    workload_report: Path,
    evidence_paths: Mapping[str, str],
) -> list[str]:
    if args.contract:
        argv = [args.contract, *benchmark_args, "--output", str(workload_report)]
        if args.root:
            argv.extend(["--root", args.root])
        if args.weights:
            argv.extend(["--weights", args.weights])
    else:
        argv = [args.root, "--weights", args.weights, *benchmark_args, "--output", str(workload_report)]
    if evidence_paths.get("disk"):
        argv.extend(["--server-disk-evidence", evidence_paths["disk"]])
    if evidence_paths.get("cache"):
        argv.extend(["--server-cache-evidence", evidence_paths["cache"]])
    if evidence_paths.get("cpu"):
        argv.extend(["--server-cpu-evidence", evidence_paths["cpu"]])
    if args.blockfs_min_runs > 0:
        argv.extend(["--blockfs-min-runs", str(args.blockfs_min_runs)])
    if args.blockfs_min_samples > 0:
        argv.extend(["--blockfs-min-samples", str(args.blockfs_min_samples)])
    if args.blockfs_min_bytes > 0:
        argv.extend(["--blockfs-min-bytes", str(args.blockfs_min_bytes)])
    if args.blockfs_min_cord_sweep > 0:
        argv.extend(["--blockfs-min-cord-sweep", str(args.blockfs_min_cord_sweep)])
    return argv


def _matrix_argv(
    args: Any,
    benchmark_args: Sequence[str],
    matrix_report: Path,
    contract: Mapping[str, Any] | None,
) -> list[str]:
    read_contract = _mapping(contract.get("read_contract")) if contract else {}
    network_contract = _mapping(contract.get("network_contract")) if contract else {}
    root = (contract or {}).get("root") or args.root
    weights = _weights_spec_from_mapping(_mapping((contract or {}).get("weights"))) if contract else args.weights
    if not root:
        raise ValueError("root is required to run the trainer surface matrix")
    if not weights:
        raise ValueError("--weights is required to run the trainer surface matrix")
    argv = [str(root), "--weights", str(weights), "--output", str(matrix_report)]

    def add(option: str, value: Any) -> None:
        if value is not None:
            argv.extend([option, str(value)])

    add("--remote-root", (contract or {}).get("remote_root"))
    add("--bulk-transport", network_contract.get("bulk_transport"))
    add("--max-cords", network_contract.get("max_cords"))
    add("--bulk-read-retries", network_contract.get("bulk_read_retries"))
    add("--batch-size", read_contract.get("batch_size"))
    add("--batches", read_contract.get("requested_batches"))
    add("--rounds-per-read", read_contract.get("rounds_per_read"))
    add("--max-workers", read_contract.get("max_workers"))
    add("--source-read-workers", read_contract.get("source_read_workers"))
    add("--prefetch-chunks", read_contract.get("prefetch_chunks"))
    add("--trainer-prefetch-batches", read_contract.get("trainer_prefetch_batches"))
    add("--seed", read_contract.get("seed"))
    add("--epoch", read_contract.get("epoch"))
    add("--samples-per-epoch", read_contract.get("samples_per_epoch"))
    add("--data-loader-workers", read_contract.get("data_loader_workers"))
    tensor_allocation = _mapping(read_contract.get("tensor_allocation"))
    add("--tensor-target", tensor_allocation.get("target") or "pytorch-cpu")
    add("--tensor-dtype", tensor_allocation.get("dtype") or "uint8")
    add("--tensor-device", tensor_allocation.get("device") or "cpu")
    tensor_non_blocking = tensor_allocation.get("non_blocking")
    if isinstance(tensor_non_blocking, bool):
        tensor_non_blocking = str(tensor_non_blocking).lower()
    add("--tensor-non-blocking", tensor_non_blocking)
    argv.extend(["--read-modes", "direct-buffers,direct-tensors"])
    argv.extend(["--trainer-batch-iterators", "native,trainer-dataloader"])
    argv.extend(_filter_matrix_args(benchmark_args))
    return argv


def _filter_matrix_args(raw_args: Sequence[str]) -> list[str]:
    supported_value_options = {
        "--remote-root",
        "--host",
        "--control-port",
        "--transport-port",
        "--bulk-transport",
        "--server-hostname",
        "--cafile",
        "--timeout",
        "--initial-cords",
        "--max-cords",
        "--provisional-container-size",
        "--expected-size-limit",
        "--write-connections",
        "--bulk-read-retries",
        "--batch-size",
        "--batches",
        "--warmup-batches",
        "--rounds-per-read",
        "--max-workers",
        "--source-read-workers",
        "--prefetch-chunks",
        "--trainer-prefetch-batches",
        "--seed",
        "--epoch",
        "--samples-per-epoch",
        "--decoder",
        "--read-modes",
        "--trainer-batch-iterators",
        "--data-loader-workers",
        "--tensor-target",
        "--tensor-dtype",
        "--tensor-device",
        "--tensor-non-blocking",
        "--objective",
        "--baseline-mb-s",
    }
    supported_flag_options = {"--shared-client", "--shuffle", "--fail-on-miss"}
    skipped_value_options = {
        "--workload-name",
        "--cord-sweep",
        "--runs",
        "--write-probe-bytes",
        "--write-probe-frame-bytes",
        "--write-probe-root",
        "--server-disk-evidence",
        "--server-cache-evidence",
        "--server-cpu-evidence",
        "--blockfs-min-runs",
        "--blockfs-min-samples",
        "--blockfs-min-bytes",
        "--blockfs-min-cord-sweep",
        "--read-mode",
        "--trainer-batch-iterator",
    }
    skipped_flag_options = {"--include-run-batches", "--no-shuffle"}
    filtered: list[str] = []
    index = 0
    while index < len(raw_args):
        option = raw_args[index]
        if option in supported_value_options:
            if index + 1 >= len(raw_args):
                raise ValueError(f"{option} requires a value")
            filtered.extend([option, raw_args[index + 1]])
            index += 2
        elif option in supported_flag_options:
            filtered.append(option)
            index += 1
        elif option in skipped_value_options:
            index += 2
        elif option in skipped_flag_options:
            index += 1
        elif option.startswith("--"):
            index += 2 if index + 1 < len(raw_args) and not raw_args[index + 1].startswith("--") else 1
        else:
            index += 1
    return filtered


def _validate_decision_inputs(args: Any) -> None:
    for name in ("blockfs_min_runs", "blockfs_min_samples", "blockfs_min_bytes", "blockfs_min_cord_sweep"):
        if int(getattr(args, name, 0) or 0) < 0:
            raise ValueError(f"{name.replace('_', '-')} must be non-negative")
    if args.input_workload_report:
        if args.contract or args.root or args.weights:
            raise ValueError("--input-workload-report cannot be combined with --contract, --root, or --weights")
        if args.collect_local_server_evidence:
            raise ValueError("--input-workload-report cannot be combined with --collect-local-server-evidence")
    if args.input_matrix_report and args.run_surface_matrix:
        raise ValueError("--input-matrix-report cannot be combined with --run-surface-matrix")
    if args.input_workload_report:
        return
    if args.contract:
        return
    if not args.root:
        raise ValueError("root is required unless --contract is provided")
    if not args.weights:
        raise ValueError("--weights is required unless --contract is provided")


def _known_evidence_paths(args: Any) -> dict[str, str]:
    paths = {
        "disk": args.server_disk_evidence,
        "cache": args.server_cache_evidence,
        "cpu": args.server_cpu_evidence,
    }
    return {name: value for name, value in paths.items() if value}


def _inside(output_dir: Path, raw_path: str | None, default_name: str) -> Path:
    if raw_path is None:
        return output_dir / default_name
    path = Path(raw_path)
    return path if path.is_absolute() else output_dir / path


def _read_existing_workload_report(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"trainer workload report must be a JSON object: {path}")
    if payload.get("format") != "cxnfs.trainer-workload-benchmark.v1":
        raise ValueError(f"unsupported trainer workload report format: {payload.get('format')}")
    return payload


def _read_existing_matrix_report(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"trainer surface matrix report must be a JSON object: {path}")
    if payload.get("format") != "cxnfs.trainer-surface-matrix.v1":
        raise ValueError(f"unsupported trainer surface matrix report format: {payload.get('format')}")
    return payload


def _weights_spec_from_mapping(value: Mapping[str, Any]) -> str:
    return ",".join(f"{name}={int(weight)}" for name, weight in value.items())


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def _trainer_contract_summary(value: Any) -> dict[str, Any] | None:
    contract = _mapping(value)
    if not contract:
        return None
    return {
        "path": contract.get("path"),
        "sha256": contract.get("sha256"),
        "format": contract.get("format"),
        "validated": bool(contract.get("validated")),
        "validation_sample_count": int(contract.get("validation_sample_count", 0) or 0),
        "validation_byte_size": int(contract.get("validation_byte_size", 0) or 0),
        "workload_fingerprint": _mapping(contract.get("workload_fingerprint")) or None,
        "workload_requirements": _mapping(contract.get("workload_requirements")) or None,
    }


def _trainer_surface_verified(workload_payload: Mapping[str, Any]) -> bool:
    return (
        workload_payload.get("format") == "cxnfs.trainer-workload-benchmark.v1"
        and _has_benchmark_provenance(workload_payload)
        and int(workload_payload.get("run_count", 0) or 0) > 0
        and int(workload_payload.get("sample_count", 0) or 0) > 0
        and str(workload_payload.get("read_mode") or "").startswith("direct-")
    )


def _has_benchmark_provenance(workload_payload: Mapping[str, Any]) -> bool:
    provenance = _mapping(workload_payload.get("benchmark_provenance"))
    fingerprint = _mapping(provenance.get("invocation_fingerprint"))
    digest = fingerprint.get("sha256")
    if provenance.get("format") != "cxnfs.trainer-workload-provenance.v1":
        return False
    if fingerprint.get("algorithm") != "sha256" or not isinstance(digest, str) or len(digest) != 64:
        return False
    try:
        int(digest, 16)
    except ValueError:
        return False
    return bool(_mapping(provenance.get("invocation")))


def _read_path_decision_summary(
    workload_payload: Mapping[str, Any],
    blockfs_gate: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _mapping(workload_payload.get("read_path_evidence"))
    gate = _mapping(blockfs_gate.get("read_path_gate"))
    if "direct_destination_max_observed_cords" in gate:
        direct_destination_max_cords = int(gate.get("direct_destination_max_observed_cords", 0) or 0)
    elif "direct_destination_max_observed_cords" in evidence:
        direct_destination_max_cords = int(evidence.get("direct_destination_max_observed_cords", 0) or 0)
    else:
        direct_destination_max_cords = int(evidence.get("max_observed_cords", 0) or 0)
    if "direct_destination_frame_sink_cord_fanout_verified" in gate:
        direct_destination_fanout_verified = bool(gate.get("direct_destination_frame_sink_cord_fanout_verified"))
    elif "direct_destination_frame_sink_cord_fanout_verified" in evidence:
        direct_destination_fanout_verified = bool(
            evidence.get("direct_destination_frame_sink_cord_fanout_verified")
        )
    else:
        direct_destination_fanout_verified = (
            bool(evidence.get("frame_sink_used_for_all_direct_destination_reads"))
            and direct_destination_max_cords > 1
        )
    return {
        "complete": bool(blockfs_gate.get("read_path_evidence_complete") or gate.get("complete")),
        "missing": list(gate.get("missing", ())),
        "bulk_payload_transport_count": int(evidence.get("bulk_payload_transport_count", 0) or 0),
        "direct_buffer_read_operations": int(evidence.get("direct_buffer_read_operations", 0) or 0),
        "direct_tensor_read_operations": int(evidence.get("direct_tensor_read_operations", 0) or 0),
        "direct_destination_read_operations": int(evidence.get("direct_destination_read_operations", 0) or 0),
        "direct_destination_reads_observed": bool(evidence.get("direct_destination_reads_observed")),
        "transport_frame_sink_observed": bool(evidence.get("transport_frame_sink_observed")),
        "frame_sink_used_for_all_direct_destination_reads": bool(
            evidence.get("frame_sink_used_for_all_direct_destination_reads")
        ),
        "max_observed_cords": int(evidence.get("max_observed_cords", 0) or 0),
        "direct_destination_max_observed_cords": direct_destination_max_cords,
        "direct_destination_frame_sink_cord_fanout_verified": direct_destination_fanout_verified,
        "direct_destination_parallel_frame_sink_read_operations": int(
            evidence.get("direct_destination_parallel_frame_sink_read_operations", 0) or 0
        ),
        "quic_read_transport_attempted_read_streams": int(
            evidence.get("quic_read_transport_attempted_read_streams", 0) or 0
        ),
        "quic_read_transport_read_streams_created": int(
            evidence.get("quic_read_transport_read_streams_created", 0) or 0
        ),
        "quic_read_transport_read_streams_eof_verified": int(
            evidence.get("quic_read_transport_read_streams_eof_verified", 0) or 0
        ),
        "quic_read_transport_stream_eof_verified": bool(
            evidence.get("quic_read_transport_stream_eof_verified")
        ),
        "quic_read_transport_read_partition_count": int(
            evidence.get("quic_read_transport_read_partition_count", 0) or 0
        ),
        "quic_read_transport_read_partition_chunk_count": int(
            evidence.get("quic_read_transport_read_partition_chunk_count", 0) or 0
        ),
        "quic_read_transport_read_partition_payload_bytes": int(
            evidence.get("quic_read_transport_read_partition_payload_bytes", 0) or 0
        ),
        "quic_read_transport_read_partition_coverage_verified": bool(
            evidence.get("quic_read_transport_read_partition_coverage_verified")
        ),
        "quic_read_transport_complete": bool(evidence.get("quic_read_transport_complete")),
        "all_runs_have_evidence": bool(evidence.get("all_runs_have_evidence")),
    }


def _memory_path_decision_summary(
    workload_payload: Mapping[str, Any],
    blockfs_gate: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _mapping(workload_payload.get("read_path_evidence"))
    gate = _mapping(blockfs_gate.get("memory_path_gate"))
    return {
        "complete": bool(blockfs_gate.get("memory_path_evidence_complete") or gate.get("complete")),
        "missing": list(gate.get("missing", ())),
        "read_mode": workload_payload.get("read_mode"),
        "tensor_allocation": _mapping(workload_payload.get("tensor_allocation")) or None,
        "direct_buffer_read_operations": int(evidence.get("direct_buffer_read_operations", 0) or 0),
        "direct_tensor_read_operations": int(evidence.get("direct_tensor_read_operations", 0) or 0),
        "direct_destination_read_operations": int(evidence.get("direct_destination_read_operations", 0) or 0),
        "direct_destination_reads_observed": bool(evidence.get("direct_destination_reads_observed")),
    }


def _write_path_decision_summary(
    workload_payload: Mapping[str, Any],
    blockfs_gate: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _mapping(workload_payload.get("write_path_evidence"))
    gate = _mapping(blockfs_gate.get("write_path_gate"))
    return {
        "complete": bool(blockfs_gate.get("write_path_evidence_complete") or gate.get("complete")),
        "missing": list(gate.get("missing", ())),
        "enabled": bool(evidence.get("enabled")),
        "all_runs_have_evidence": bool(evidence.get("all_runs_have_evidence")),
        "all_write_probes_verified": bool(evidence.get("all_write_probes_verified")),
        "direct_tensor_writes_observed": bool(evidence.get("direct_tensor_writes_observed")),
        "metadata_pending_observed": bool(evidence.get("metadata_pending_observed")),
        "verified_read_back_count": int(evidence.get("verified_read_back_count", 0) or 0),
        "write_frames": int(evidence.get("write_frames", 0) or 0),
        "write_acks": int(evidence.get("write_acks", 0) or 0),
        "metadata_after_payload_entries": int(evidence.get("metadata_after_payload_entries", 0) or 0),
        "entries_committed": int(evidence.get("entries_committed", 0) or 0),
        "transfers_committed": int(evidence.get("transfers_committed", 0) or 0),
        "payload_bytes_written": int(evidence.get("payload_bytes_written", 0) or 0),
        "requires_quic_transport_evidence": bool(gate.get("requires_quic_transport_evidence")),
        "quic_write_transport_evidence_count": int(evidence.get("quic_write_transport_evidence_count", 0) or 0),
        "quic_write_transport_connection_reuse_count": int(
            evidence.get("quic_write_transport_connection_reuse_count", 0) or 0
        ),
        "quic_write_transport_successful_write_operations": int(
            evidence.get("quic_write_transport_successful_write_operations", 0) or 0
        ),
        "quic_write_transport_failed_write_operations": int(
            evidence.get("quic_write_transport_failed_write_operations", 0) or 0
        ),
        "quic_write_transport_attempted_write_streams": int(
            evidence.get("quic_write_transport_attempted_write_streams", 0) or 0
        ),
        "quic_write_transport_write_streams_created": int(
            evidence.get("quic_write_transport_write_streams_created", 0) or 0
        ),
        "quic_write_transport_write_ack_streams_eof_verified": int(
            evidence.get("quic_write_transport_write_ack_streams_eof_verified", 0) or 0
        ),
        "quic_write_transport_all_reports_write_ack_stream_eof_verified": bool(
            evidence.get("quic_write_transport_all_reports_write_ack_stream_eof_verified")
        ),
        "quic_write_transport_max_active_write_streams": int(
            evidence.get("quic_write_transport_max_active_write_streams", 0) or 0
        ),
        "quic_write_transport_all_reports_reused_connection": bool(
            evidence.get("quic_write_transport_all_reports_reused_connection")
        ),
        "quic_write_transport_complete": bool(evidence.get("quic_write_transport_complete")),
    }


def _surface_check_summary(
    path: Path | None,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    surface = _mapping(payload)
    if not surface:
        return None
    return {
        "path": str(path) if path is not None else None,
        "format": surface.get("format"),
        "passed": bool(surface.get("passed")),
        "status": surface.get("status"),
        "failed_check_count": len(surface.get("failed_checks", ())) if isinstance(surface.get("failed_checks"), list) else 0,
        "requirements": dict(_mapping(surface.get("requirements"))),
    }


def _surface_matrix_summary(
    path: Path | None,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    matrix = _mapping(payload)
    if not matrix:
        return None
    return {
        "path": str(path) if path is not None else None,
        "format": matrix.get("format"),
        "passed": bool(matrix.get("passed")),
        "status": matrix.get("status"),
        "transport": matrix.get("transport"),
        "bulk_transport": matrix.get("bulk_transport"),
        "case_count": int(matrix.get("case_count", 0) or 0),
        "passed_case_count": int(matrix.get("passed_case_count", 0) or 0),
        "failed_case_count": int(matrix.get("failed_case_count", 0) or 0),
        "case_names": [
            str(_mapping(case).get("case_name") or "")
            for case in matrix.get("cases", ())
            if isinstance(case, Mapping)
        ],
        "workload_fingerprint": _mapping(matrix.get("workload_fingerprint")) or None,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    main()
