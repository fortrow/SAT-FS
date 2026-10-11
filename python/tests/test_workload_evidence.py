import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from cxnfs import collect_workload_evidence as exported_collect_workload_evidence
from cxnfs.workload_evidence import (
    CACHE_REPORT_FORMAT,
    CPU_REPORT_FORMAT,
    DISK_REPORT_FORMAT,
    CpuSnapshot,
    DiskSnapshot,
    cache_evidence_payload,
    choose_disk_device,
    collect_workload_evidence,
    cpu_evidence,
    disk_evidence,
    parse_diskstats,
    parse_meminfo,
    parse_proc_stat,
    run_workload_evidence_cli,
)


FIRST_CPU = "cpu 100 0 100 600 200 0 0 0 0 0\n"
SECOND_CPU = "cpu 250 0 750 700 300 0 0 0 0 0\n"
FIRST_DISK = "8 0 vda 10 0 1000 0 20 0 2000 0 0 1000 0\n"
SECOND_DISK = "8 0 vda 30 0 2000 0 40 0 4000 0 0 2700 0\n"
MEMINFO = "\n".join((
    "MemTotal:       100000 kB",
    "MemAvailable:    5000 kB",
    "Cached:          3000 kB",
    "SReclaimable:    2000 kB",
    "",
))


class WorkloadEvidenceTest(unittest.TestCase):
    def test_parses_proc_counters_and_formats_evidence(self):
        first_cpu = parse_proc_stat(FIRST_CPU)
        second_cpu = parse_proc_stat(SECOND_CPU)
        self.assertEqual(CpuSnapshot(idle_ticks=800, total_ticks=1000), first_cpu)
        self.assertEqual(CpuSnapshot(idle_ticks=1000, total_ticks=2000), second_cpu)

        diskstats = "7 0 loop0 0 0 0 0 0 0 0 0 0 0 0 0\n" + FIRST_DISK
        self.assertEqual("vda", choose_disk_device(diskstats))
        self.assertEqual(DiskSnapshot("vda", 1000, 2000, 1000), parse_diskstats(diskstats, "vda"))

        disk = disk_evidence(parse_diskstats(FIRST_DISK, "vda"), parse_diskstats(SECOND_DISK, "vda"), 2.0)
        self.assertEqual("vda", disk.device)
        self.assertEqual(256000.0, disk.read_bytes_per_second)
        self.assertEqual(512000.0, disk.write_bytes_per_second)
        self.assertEqual(85.0, disk.disk_busy_percent)
        self.assertTrue(disk.disk_saturated)

        cpu = cpu_evidence(first_cpu, second_cpu, 2.0)
        self.assertEqual(20.0, cpu.cpu_idle_percent)
        self.assertEqual(80.0, cpu.cpu_busy_percent)
        self.assertFalse(cpu.cpu_saturated)

        cache_payload = cache_evidence_payload(parse_meminfo(MEMINFO))
        self.assertEqual(CACHE_REPORT_FORMAT, cache_payload["format"])
        self.assertEqual(5.0, cache_payload["available_percent"])
        self.assertEqual(5.0, cache_payload["page_cache_percent"])
        self.assertFalse(cache_payload["cache_effective"])

    def test_collects_disk_cache_and_cpu_reports_from_proc_root(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            proc_root = root / "proc"
            output_dir = root / "evidence"
            _write_proc(proc_root, FIRST_CPU, FIRST_DISK)

            def advance(_seconds):
                _write_proc(proc_root, SECOND_CPU, SECOND_DISK)

            paths = collect_workload_evidence(
                output_dir,
                disk_device="vda",
                sample_seconds=2.0,
                proc_root=proc_root,
                sleep_fn=advance,
                time_fn=iter((1000.0, 1002.0)).__next__,
            )

            self.assertEqual(exported_collect_workload_evidence, collect_workload_evidence)
            self.assertEqual({"disk", "cache", "cpu"}, set(paths))
            self.assertTrue(paths["disk"].is_file())
            self.assertTrue(paths["cache"].is_file())
            self.assertTrue(paths["cpu"].is_file())
            self.assertIn(DISK_REPORT_FORMAT, paths["disk"].read_text(encoding="utf-8"))
            self.assertIn("read_bytes_per_second=256000.000000", paths["disk"].read_text(encoding="utf-8"))
            self.assertIn("disk_saturated=true", paths["disk"].read_text(encoding="utf-8"))
            self.assertIn("sample_start_unix_seconds=1000.000000", paths["disk"].read_text(encoding="utf-8"))
            self.assertIn("sample_end_unix_seconds=1002.000000", paths["disk"].read_text(encoding="utf-8"))
            self.assertIn(CPU_REPORT_FORMAT, paths["cpu"].read_text(encoding="utf-8"))
            self.assertIn("cpu_busy_percent=80.000000", paths["cpu"].read_text(encoding="utf-8"))
            cache_payload = json.loads(paths["cache"].read_text(encoding="utf-8"))
            self.assertFalse(cache_payload["cache_effective"])
            self.assertEqual(1000.0, cache_payload["sample_start_unix_seconds"])
            self.assertEqual(1002.0, cache_payload["sample_end_unix_seconds"])

    def test_cli_collects_reports_and_hides_host_proc_details_behind_arguments(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            proc_root = root / "proc"
            output_dir = root / "evidence"
            _write_proc(proc_root, FIRST_CPU, FIRST_DISK)
            out = io.StringIO()
            err = io.StringIO()

            exit_code = run_workload_evidence_cli(
                [
                    "collect",
                    "--output-dir",
                    str(output_dir),
                    "--proc-root",
                    str(proc_root),
                    "--disk-device",
                    "vda",
                    "--sample-seconds",
                    "0",
                ],
                stdout=out,
                stderr=err,
            )

            self.assertEqual(0, exit_code, err.getvalue())
            self.assertIn("cxnfs.workload-evidence.collect.v1", out.getvalue())
            self.assertIn("disk_evidence=", out.getvalue())
            self.assertIn("status=collected", out.getvalue())
            self.assertIn(DISK_REPORT_FORMAT, (output_dir / "disk.report").read_text(encoding="utf-8"))
            self.assertIn(CPU_REPORT_FORMAT, (output_dir / "cpu.report").read_text(encoding="utf-8"))
            self.assertIn("sample_start_unix_seconds=", (output_dir / "disk.report").read_text(encoding="utf-8"))

    def test_cli_returns_failure_for_unknown_disk_device(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            proc_root = root / "proc"
            _write_proc(proc_root, FIRST_CPU, FIRST_DISK)
            out = io.StringIO()
            err = io.StringIO()

            exit_code = run_workload_evidence_cli(
                [
                    "collect",
                    "--output-dir",
                    str(root / "evidence"),
                    "--proc-root",
                    str(proc_root),
                    "--disk-device",
                    "missing0",
                    "--sample-seconds",
                    "0",
                ],
                stdout=out,
                stderr=err,
            )

            self.assertEqual(1, exit_code)
            self.assertIn("disk device not found", err.getvalue())


def _write_proc(root: Path, cpu_line: str, disk_line: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "stat").write_text(cpu_line, encoding="utf-8")
    (root / "diskstats").write_text(disk_line, encoding="utf-8")
    (root / "meminfo").write_text(MEMINFO, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
