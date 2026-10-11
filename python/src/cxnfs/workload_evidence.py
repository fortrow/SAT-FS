from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TextIO


SECTOR_BYTES = 512
DEFAULT_SAMPLE_SECONDS = 1.0
DISK_REPORT_FORMAT = "cxnfs.server.disk.v1"
CACHE_REPORT_FORMAT = "cxnfs.server.cache.v1"
CPU_REPORT_FORMAT = "cxnfs.server.cpu.v1"
COLLECT_REPORT_FORMAT = "cxnfs.workload-evidence.collect.v1"


@dataclass(frozen=True)
class CpuSnapshot:
    idle_ticks: int
    total_ticks: int


@dataclass(frozen=True)
class DiskSnapshot:
    device: str
    sectors_read: int
    sectors_written: int
    io_time_ms: int


@dataclass(frozen=True)
class DiskEvidence:
    device: str
    sample_seconds: float
    read_bytes_per_second: float
    write_bytes_per_second: float
    disk_busy_percent: float
    sample_start_unix_seconds: float | None = None
    sample_end_unix_seconds: float | None = None

    @property
    def disk_saturated(self) -> bool:
        return self.disk_busy_percent >= 85.0


@dataclass(frozen=True)
class CpuEvidence:
    sample_seconds: float
    cpu_idle_percent: float
    cpu_busy_percent: float
    sample_start_unix_seconds: float | None = None
    sample_end_unix_seconds: float | None = None

    @property
    def cpu_saturated(self) -> bool:
        return self.cpu_busy_percent >= 90.0


@dataclass(frozen=True)
class CacheEvidence:
    mem_total_kib: int
    mem_available_kib: int
    cached_kib: int
    sreclaimable_kib: int

    @property
    def available_percent(self) -> float:
        return _percent(self.mem_available_kib, self.mem_total_kib)

    @property
    def page_cache_percent(self) -> float:
        return _percent(self.cached_kib + self.sreclaimable_kib, self.mem_total_kib)

    @property
    def cache_effective(self) -> bool:
        return self.available_percent >= 10.0 or self.page_cache_percent >= 10.0


def collect_workload_evidence(
    output_dir: str | Path,
    *,
    disk_device: str | None = None,
    sample_seconds: float = DEFAULT_SAMPLE_SECONDS,
    proc_root: str | Path = "/proc",
    sleep_fn: Callable[[float], None] = time.sleep,
    time_fn: Callable[[], float] = time.time,
) -> dict[str, Path]:
    if sample_seconds < 0:
        raise ValueError("sample_seconds must be non-negative")
    root = Path(proc_root)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    first_cpu = parse_proc_stat((root / "stat").read_text(encoding="utf-8"))
    first_diskstats = (root / "diskstats").read_text(encoding="utf-8")
    active_device = disk_device or choose_disk_device(first_diskstats)
    first_disk = parse_diskstats(first_diskstats, active_device)

    start_unix_seconds = time_fn()
    start = time.monotonic()
    sleep_fn(sample_seconds)
    elapsed = max(time.monotonic() - start, sample_seconds, 1e-9)

    second_cpu = parse_proc_stat((root / "stat").read_text(encoding="utf-8"))
    second_disk = parse_diskstats((root / "diskstats").read_text(encoding="utf-8"), active_device)
    cache = parse_meminfo((root / "meminfo").read_text(encoding="utf-8"))
    end_unix_seconds = max(start_unix_seconds, time_fn())

    disk = disk_evidence(
        first_disk,
        second_disk,
        elapsed,
        sample_start_unix_seconds=start_unix_seconds,
        sample_end_unix_seconds=end_unix_seconds,
    )
    cpu = cpu_evidence(
        first_cpu,
        second_cpu,
        elapsed,
        sample_start_unix_seconds=start_unix_seconds,
        sample_end_unix_seconds=end_unix_seconds,
    )
    paths = {
        "disk": output / "disk.report",
        "cache": output / "cache.json",
        "cpu": output / "cpu.report",
    }
    paths["disk"].write_text(format_disk_evidence(disk), encoding="utf-8")
    paths["cache"].write_text(json.dumps(
        cache_evidence_payload(
            cache,
            sample_seconds=elapsed,
            sample_start_unix_seconds=start_unix_seconds,
            sample_end_unix_seconds=end_unix_seconds,
        ),
        indent=2,
        sort_keys=True,
    ) + "\n", encoding="utf-8")
    paths["cpu"].write_text(format_cpu_evidence(cpu), encoding="utf-8")
    return paths


def parse_proc_stat(text: str) -> CpuSnapshot:
    for line in text.splitlines():
        if line.startswith("cpu "):
            values = [int(value) for value in line.split()[1:]]
            if len(values) < 4:
                raise ValueError("cpu stat line is incomplete")
            idle = values[3] + (values[4] if len(values) > 4 else 0)
            return CpuSnapshot(idle, sum(values))
    raise ValueError("missing aggregate cpu line")


def choose_disk_device(diskstats_text: str) -> str:
    for line in diskstats_text.splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        name = parts[2]
        if name.startswith(("loop", "ram", "fd")):
            continue
        return name
    raise ValueError("no disk device found in diskstats")


def parse_diskstats(text: str, device: str) -> DiskSnapshot:
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 14 or parts[2] != device:
            continue
        return DiskSnapshot(
            device=device,
            sectors_read=int(parts[5]),
            sectors_written=int(parts[9]),
            io_time_ms=int(parts[12]),
        )
    raise ValueError(f"disk device not found in diskstats: {device}")


def parse_meminfo(text: str) -> CacheEvidence:
    values: dict[str, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, raw_value = line.split(":", 1)
        parts = raw_value.strip().split()
        if not parts:
            continue
        values[key] = int(parts[0])
    total = values.get("MemTotal", 0)
    if total <= 0:
        raise ValueError("MemTotal is required in meminfo")
    return CacheEvidence(
        mem_total_kib=total,
        mem_available_kib=values.get("MemAvailable", 0),
        cached_kib=values.get("Cached", 0),
        sreclaimable_kib=values.get("SReclaimable", 0),
    )


def disk_evidence(
    first: DiskSnapshot,
    second: DiskSnapshot,
    elapsed_seconds: float,
    *,
    sample_start_unix_seconds: float | None = None,
    sample_end_unix_seconds: float | None = None,
) -> DiskEvidence:
    if first.device != second.device:
        raise ValueError("disk snapshots must use the same device")
    elapsed = max(elapsed_seconds, 1e-9)
    read_bytes = max(0, second.sectors_read - first.sectors_read) * SECTOR_BYTES
    write_bytes = max(0, second.sectors_written - first.sectors_written) * SECTOR_BYTES
    busy_ms = max(0, second.io_time_ms - first.io_time_ms)
    return DiskEvidence(
        device=first.device,
        sample_seconds=elapsed,
        read_bytes_per_second=read_bytes / elapsed,
        write_bytes_per_second=write_bytes / elapsed,
        disk_busy_percent=min(100.0, busy_ms / (elapsed * 10.0)),
        sample_start_unix_seconds=sample_start_unix_seconds,
        sample_end_unix_seconds=sample_end_unix_seconds,
    )


def cpu_evidence(
    first: CpuSnapshot,
    second: CpuSnapshot,
    elapsed_seconds: float,
    *,
    sample_start_unix_seconds: float | None = None,
    sample_end_unix_seconds: float | None = None,
) -> CpuEvidence:
    total_delta = max(0, second.total_ticks - first.total_ticks)
    idle_delta = max(0, second.idle_ticks - first.idle_ticks)
    idle_percent = 100.0 if total_delta == 0 else _percent(idle_delta, total_delta)
    return CpuEvidence(
        sample_seconds=max(elapsed_seconds, 1e-9),
        cpu_idle_percent=idle_percent,
        cpu_busy_percent=max(0.0, 100.0 - idle_percent),
        sample_start_unix_seconds=sample_start_unix_seconds,
        sample_end_unix_seconds=sample_end_unix_seconds,
    )


def cache_evidence_payload(
    cache: CacheEvidence,
    *,
    sample_seconds: float | None = None,
    sample_start_unix_seconds: float | None = None,
    sample_end_unix_seconds: float | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "format": CACHE_REPORT_FORMAT,
        "mem_total_kib": cache.mem_total_kib,
        "mem_available_kib": cache.mem_available_kib,
        "cached_kib": cache.cached_kib,
        "sreclaimable_kib": cache.sreclaimable_kib,
        "available_percent": cache.available_percent,
        "page_cache_percent": cache.page_cache_percent,
        "cache_effective": cache.cache_effective,
    }
    if sample_seconds is not None:
        payload["sample_seconds"] = max(sample_seconds, 1e-9)
    if sample_start_unix_seconds is not None:
        payload["sample_start_unix_seconds"] = sample_start_unix_seconds
    if sample_end_unix_seconds is not None:
        payload["sample_end_unix_seconds"] = sample_end_unix_seconds
    return payload


def format_disk_evidence(evidence: DiskEvidence) -> str:
    lines = [
        DISK_REPORT_FORMAT,
        f"device={evidence.device}",
        f"sample_seconds={evidence.sample_seconds:.6f}",
    ]
    if evidence.sample_start_unix_seconds is not None:
        lines.append(f"sample_start_unix_seconds={evidence.sample_start_unix_seconds:.6f}")
    if evidence.sample_end_unix_seconds is not None:
        lines.append(f"sample_end_unix_seconds={evidence.sample_end_unix_seconds:.6f}")
    lines.extend([
        f"read_bytes_per_second={evidence.read_bytes_per_second:.6f}",
        f"write_bytes_per_second={evidence.write_bytes_per_second:.6f}",
        f"disk_busy_percent={evidence.disk_busy_percent:.6f}",
        f"disk_saturated={str(evidence.disk_saturated).lower()}",
        "",
    ])
    return "\n".join(lines)


def format_cpu_evidence(evidence: CpuEvidence) -> str:
    lines = [
        CPU_REPORT_FORMAT,
        f"sample_seconds={evidence.sample_seconds:.6f}",
    ]
    if evidence.sample_start_unix_seconds is not None:
        lines.append(f"sample_start_unix_seconds={evidence.sample_start_unix_seconds:.6f}")
    if evidence.sample_end_unix_seconds is not None:
        lines.append(f"sample_end_unix_seconds={evidence.sample_end_unix_seconds:.6f}")
    lines.extend([
        f"cpu_idle_percent={evidence.cpu_idle_percent:.6f}",
        f"cpu_busy_percent={evidence.cpu_busy_percent:.6f}",
        f"cpu_saturated={str(evidence.cpu_saturated).lower()}",
        "",
    ])
    return "\n".join(lines)


def run_workload_evidence_cli(argv: list[str] | tuple[str, ...] | None = None, stdout: TextIO | None = None, stderr: TextIO | None = None) -> int:
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    parser = argparse.ArgumentParser(prog="cxnfs-workload-evidence")
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect = subparsers.add_parser("collect", help="Collect server disk, cache, and CPU evidence.")
    collect.add_argument("--output-dir", required=True)
    collect.add_argument("--disk-device")
    collect.add_argument("--sample-seconds", type=float, default=DEFAULT_SAMPLE_SECONDS)
    collect.add_argument("--proc-root", default="/proc")
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        paths = collect_workload_evidence(
            args.output_dir,
            disk_device=args.disk_device,
            sample_seconds=args.sample_seconds,
            proc_root=args.proc_root,
        )
        print(COLLECT_REPORT_FORMAT, file=out)
        for name in ("disk", "cache", "cpu"):
            print(f"{name}_evidence={paths[name]}", file=out)
        print("status=collected", file=out)
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except Exception as exc:
        print(str(exc), file=err)
        return 1


def main() -> None:
    raise SystemExit(run_workload_evidence_cli())


def _percent(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) * 100.0 / float(denominator)


if __name__ == "__main__":
    main()
