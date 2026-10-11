import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from cxnfs import (
    DatasetConverter,
    DatasetPreparer,
    build_trainer_contract_payload,
    run_workload_decision_cli,
    weighted_mds_plan_fingerprint,
)
from cxnfs.workload_decision import BLOCKFS_HANDOFF_FORMAT, WORKLOAD_DECISION_FORMAT


PROC_STAT = "cpu 100 0 100 600 200 0 0 0 0 0\n"
PROC_DISKSTATS = "8 0 vda 10 0 1000 0 20 0 2000 0 0 1000 0\n"
PROC_MEMINFO = "\n".join((
    "MemTotal:       100000 kB",
    "MemAvailable:   20000 kB",
    "Cached:         15000 kB",
    "SReclaimable:    5000 kB",
    "",
))


class WorkloadDecisionTest(unittest.TestCase):
    def test_cli_collects_local_evidence_and_writes_decision_bundle(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            proc_root = root / "proc"
            write_proc_fixture(proc_root)
            output_dir = root / "decision"
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=2,summary=1",
                    "--output-dir",
                    str(output_dir),
                    "--collect-local-server-evidence",
                    "--proc-root",
                    str(proc_root),
                    "--disk-device",
                    "vda",
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--runs",
                    "1",
                    "--cord-sweep",
                    "1",
                    "--samples-per-epoch",
                    "3",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertEqual(WORKLOAD_DECISION_FORMAT, decision["format"])
            self.assertTrue(decision["benchmark_completed"])
            self.assertTrue(decision["trainer_surface_verified"])
            self.assertTrue(decision["server_evidence_complete"])
            self.assertTrue(decision["server_evidence_sample_window_complete"])
            self.assertTrue(decision["read_path_reliability_complete"])
            self.assertEqual(0, decision["unrecovered_read_failures"])
            self.assertEqual("local-mds", decision["transport"])
            self.assertEqual("stream", decision["bulk_transport"])
            self.assertEqual("defer", decision["blockfs_gate"]["decision"])
            self.assertFalse(decision["kernel_module_allowed"])
            self.assertEqual(
                str(output_dir / "trainer-workload-benchmark.json"),
                decision["workload_report"]["path"],
            )
            self.assertTrue((output_dir / "server-evidence/disk.report").is_file())
            self.assertTrue((output_dir / "server-evidence/cache.json").is_file())
            self.assertTrue((output_dir / "server-evidence/cpu.report").is_file())
            self.assertEqual(stdout.getvalue(), (output_dir / "workload-decision.json").read_text(encoding="utf-8"))
            handoff = json.loads((output_dir / "blockfs-handoff.json").read_text(encoding="utf-8"))
            self.assertEqual(BLOCKFS_HANDOFF_FORMAT, handoff["format"])
            self.assertEqual("deferred", handoff["status"])
            self.assertFalse(handoff["kernel_module_allowed"])
            self.assertIsNone(handoff["linux_blockfs_candidate"])
            self.assertEqual(
                str(output_dir / "blockfs-handoff.json"),
                decision["blockfs_handoff"]["path"],
            )
            report = (output_dir / "decision.report").read_text(encoding="utf-8")
            self.assertIn(WORKLOAD_DECISION_FORMAT, report)
            self.assertIn(f"blockfs_handoff={output_dir / 'blockfs-handoff.json'}", report)
            self.assertIn("trainer_surface_verified=true", report)
            self.assertIn("server_evidence_complete=true", report)
            self.assertIn("server_evidence_sample_window_complete=true", report)
            self.assertIn("read_path_reliability_complete=true", report)
            self.assertIn("unrecovered_read_failures=0", report)
            workload = json.loads((output_dir / "trainer-workload-benchmark.json").read_text(encoding="utf-8"))
            self.assertTrue(workload["server_evidence"]["complete"])
            self.assertTrue(workload["server_evidence"]["sample_window_complete"])

    def test_cli_writes_decision_even_when_objective_gate_fails(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            output_dir = root / "decision"
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    str(root / "mds"),
                    "--weights",
                    "chat=1",
                    "--output-dir",
                    str(output_dir),
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--runs",
                    "1",
                    "--objective",
                    "ai-training-25gbe",
                    "--baseline-mb-s",
                    "1000000000000",
                    "--fail-on-miss",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(1, exit_code)
            self.assertEqual("", stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertEqual(1, decision["benchmark_exit_code"])
            self.assertFalse(decision["benchmark_completed"])
            self.assertTrue(decision["trainer_surface_verified"])
            self.assertFalse(decision["objective_passed_any"])
            self.assertEqual("defer", decision["blockfs_gate"]["decision"])
            self.assertTrue((output_dir / "workload-decision.json").is_file())
            self.assertTrue((output_dir / "decision.report").is_file())
            self.assertTrue((output_dir / "trainer-workload-benchmark.json").is_file())

    def test_cli_can_drive_workload_benchmark_from_trainer_contract(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_file_dataset(root, "chat", ("chat-0", "chat-1"))
            write_file_dataset(root, "summary", ("summary-0",))
            contract = build_trainer_contract_payload(
                root / "mds",
                "chat=2,summary=1",
                read_mode="direct-buffers",
                batch_size=2,
                batches=1,
                samples_per_epoch=3,
            )
            contract_path = root / "trainer-contract.json"
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            output_dir = root / "decision"
            proc_root = root / "proc"
            write_proc_fixture(proc_root)
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--contract",
                    str(contract_path),
                    "--output-dir",
                    str(output_dir),
                    "--runs",
                    "1",
                    "--cord-sweep",
                    "1",
                    "--collect-local-server-evidence",
                    "--proc-root",
                    str(proc_root),
                    "--disk-device",
                    "vda",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertEqual(WORKLOAD_DECISION_FORMAT, decision["format"])
            self.assertTrue(decision["trainer_surface_verified"])
            self.assertTrue(decision["trainer_surface_check_passed"])
            self.assertEqual(
                str(output_dir / "trainer-surface-check.json"),
                decision["trainer_surface_check"]["path"],
            )
            self.assertEqual("local-mds", decision["transport"])
            workload = json.loads((output_dir / "trainer-workload-benchmark.json").read_text(encoding="utf-8"))
            self.assertTrue(workload["contract_applied"])
            self.assertEqual(str(contract_path), workload["trainer_contract"]["path"])
            self.assertEqual("cxnfs.trainer-contract.v1", workload["trainer_contract"]["format"])
            self.assertEqual(2, workload["sample_count"])
            surface = json.loads((output_dir / "trainer-surface-check.json").read_text(encoding="utf-8"))
            self.assertTrue(surface["passed"])
            self.assertEqual("cxnfs.trainer-surface-check.v1", surface["format"])

    def test_cli_forwards_write_probe_options_to_contract_benchmark_runner(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "trainer-contract.json"
            output_dir = root / "decision"
            contract_path.write_text(
                json.dumps(proven_quic_trainer_contract_payload(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            captured = {}

            def trainer_workload_runner(argv, stdout, stderr):
                captured["argv"] = list(argv)
                report_path = Path(argv[argv.index("--output") + 1])
                weighted_plan = proven_quic_weighted_plan_payload()
                report_path.write_text(json.dumps({
                    "format": "cxnfs.trainer-workload-benchmark.v1",
                    "workload_name": "quic-contract",
                    "root": "/var/cache/cxnfs/indexes/stage0-mds",
                    "weights": {"chat": 2, "summary": 1},
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "read_mode": "direct-buffers",
                    "trainer_batch_iterator": "native",
                    "data_loader_workers": 0,
                    "bulk_read_retries": 1,
                    "bulk_write_retries": 1,
                    "remote_root": "/stage0-mds",
                    "contract_applied": True,
                    "weighted_mds_plan": weighted_plan,
                    "workload_fingerprint": weighted_plan["workload_fingerprint"],
                    "cord_sweep": [{"cord_index": 0, "initial_cords": 1, "max_cords": 1}],
                    "runs": [{"cord_label": "1", "mebibytes_per_second": 1.0}],
                    "run_count": 1,
                    "sample_count": 6,
                    "byte_size": 42,
                    "benchmark_provenance": proven_quic_benchmark_provenance(),
                    "read_path_evidence": proven_quic_read_path_evidence(),
                    "read_path_reliability": proven_quic_read_path_reliability(),
                    "write_path_evidence": proven_quic_write_path_evidence(),
                    "objective": "ai-training-25gbe",
                    "objective_passed_any": False,
                    "blockfs_gate": {
                        "decision": "prove-workload-scale",
                        "kernel_module_allowed": False,
                        "required_next_evidence": [],
                    },
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                return 0

            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--contract",
                    str(contract_path),
                    "--output-dir",
                    str(output_dir),
                    "--write-probe-bytes",
                    "4096",
                    "--write-probe-frame-bytes",
                    "1024",
                    "--write-probe-root",
                    "/trainer-write-probes",
                ],
                stdout=stdout,
                stderr=stderr,
                trainer_workload_runner=trainer_workload_runner,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            argv = captured["argv"]
            self.assertEqual(str(contract_path), argv[0])
            self.assertEqual("4096", argv[argv.index("--write-probe-bytes") + 1])
            self.assertEqual("1024", argv[argv.index("--write-probe-frame-bytes") + 1])
            self.assertEqual("/trainer-write-probes", argv[argv.index("--write-probe-root") + 1])
            decision = json.loads(stdout.getvalue())
            self.assertTrue(decision["write_path_evidence"]["metadata_pending_observed"])
            self.assertTrue(decision["write_path_evidence"]["all_write_probes_verified"])
            self.assertEqual(128, decision["read_path_evidence"]["quic_read_transport_read_streams_eof_verified"])
            self.assertTrue(decision["read_path_evidence"]["quic_read_transport_read_partition_coverage_verified"])
            self.assertEqual(128, decision["read_path_evidence"]["quic_read_transport_read_partition_count"])
            self.assertEqual(5, decision["write_path_evidence"]["quic_write_transport_write_ack_streams_eof_verified"])

    def test_cli_runs_surface_matrix_and_bundles_matrix_surface_check(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "trainer-contract.json"
            output_dir = root / "decision"
            contract = proven_quic_trainer_contract_payload()
            contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            captured = {}

            def trainer_workload_runner(argv, stdout, stderr):
                captured["workload_argv"] = list(argv)
                report_path = Path(argv[argv.index("--output") + 1])
                report_path.write_text(
                    json.dumps(proven_quic_workload_payload(), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                return 0

            def trainer_matrix_runner(argv, stdout, stderr):
                captured["matrix_argv"] = list(argv)
                report_path = Path(argv[argv.index("--output") + 1])
                report_path.write_text(
                    json.dumps(proven_surface_matrix_payload(contract), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                return 0

            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--contract",
                    str(contract_path),
                    "--output-dir",
                    str(output_dir),
                    "--run-surface-matrix",
                    "--surface-require-matrix",
                    "--surface-require-remote",
                    "--surface-require-quic",
                    "--surface-min-sources",
                    "2",
                    "--surface-min-validation-samples",
                    "6",
                    "--surface-min-validation-bytes",
                    "42",
                    "--surface-min-workload-runs",
                    "5",
                    "--surface-min-workload-samples",
                    "30",
                    "--surface-min-workload-bytes",
                    "210",
                    "--surface-min-cord-sweep",
                    "5",
                    "--blockfs-min-runs",
                    "5",
                    "--blockfs-min-samples",
                    "30",
                    "--blockfs-min-bytes",
                    "210",
                    "--blockfs-min-cord-sweep",
                    "5",
                    "--host",
                    "localhost",
                    "--runs",
                    "1",
                    "--cord-sweep",
                    "1,4",
                    "--batch-size",
                    "3",
                    "--trainer-prefetch-batches",
                    "2",
                ],
                stdout=stdout,
                stderr=stderr,
                trainer_workload_runner=trainer_workload_runner,
                trainer_matrix_runner=trainer_matrix_runner,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            self.assertEqual("", stderr.getvalue())
            matrix_argv = captured["matrix_argv"]
            self.assertIn("--host", matrix_argv)
            self.assertEqual("localhost", matrix_argv[matrix_argv.index("--host") + 1])
            self.assertIn("--batch-size", matrix_argv)
            self.assertEqual("3", matrix_argv[matrix_argv.index("--batch-size") + 1])
            self.assertIn("--trainer-prefetch-batches", matrix_argv)
            prefetch_values = [
                matrix_argv[index + 1]
                for index, value in enumerate(matrix_argv[:-1])
                if value == "--trainer-prefetch-batches"
            ]
            self.assertEqual(["2", "2"], prefetch_values)
            self.assertNotIn("--runs", matrix_argv)
            self.assertNotIn("--cord-sweep", matrix_argv)
            self.assertEqual("pytorch-cpu", matrix_argv[matrix_argv.index("--tensor-target") + 1])
            self.assertEqual("direct-buffers,direct-tensors", matrix_argv[matrix_argv.index("--read-modes") + 1])
            self.assertEqual("native,trainer-dataloader", matrix_argv[matrix_argv.index("--trainer-batch-iterators") + 1])
            decision = json.loads(stdout.getvalue())
            self.assertEqual(WORKLOAD_DECISION_FORMAT, decision["format"])
            self.assertTrue(decision["matrix_completed"])
            self.assertTrue(decision["trainer_surface_matrix_passed"])
            self.assertTrue(decision["trainer_surface_check_passed"])
            self.assertEqual(4, decision["trainer_surface_matrix"]["case_count"])
            self.assertEqual(
                str(output_dir / "trainer-surface-matrix.json"),
                decision["trainer_surface_matrix"]["path"],
            )
            surface = json.loads((output_dir / "trainer-surface-check.json").read_text(encoding="utf-8"))
            self.assertTrue(surface["passed"], surface["failed_checks"])
            self.assertTrue(surface["requirements"]["require_matrix"])
            self.assertTrue(surface["matrix"]["quic_read_paths_complete"])
            self.assertTrue(surface["matrix"]["read_path_reliability_complete"])
            self.assertEqual(0, surface["matrix"]["unrecovered_read_failures"])
            report = (output_dir / "decision.report").read_text(encoding="utf-8")
            self.assertIn("trainer_surface_matrix_passed=true", report)

    def test_cli_builds_decision_from_existing_quic_contract_workload_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "quic-contract-workload.json"
            matrix = root / "trainer-surface-matrix.json"
            output_dir = root / "decision"
            disk = root / "disk.report"
            cache = root / "cache.json"
            cpu = root / "cpu.report"
            contract = root / "trainer-contract.json"
            write_server_evidence_files(disk, cache, cpu)
            weighted_plan = proven_quic_weighted_plan_payload()
            contract.write_text(
                json.dumps(proven_quic_trainer_contract_payload(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            matrix.write_text(
                json.dumps(proven_surface_matrix_payload(proven_quic_trainer_contract_payload()), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            report.write_text(json.dumps({
                "format": "cxnfs.trainer-workload-benchmark.v1",
                "workload_name": "quic-contract",
                "root": "/var/cache/cxnfs/indexes/stage0-mds",
                "weights": {"chat": 2, "summary": 1},
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_batch_iterator": "native",
                "data_loader_workers": 0,
                "bulk_read_retries": 1,
                "bulk_write_retries": 1,
                "remote_root": "/stage0-mds",
                "contract_applied": True,
                "trainer_contract": {
                    "path": "/var/log/cxnfs/trainer-contract.json",
                    "sha256": "00" * 32,
                    "format": "cxnfs.trainer-contract.v1",
                    "validated": True,
                    "validation_sample_count": 6,
                    "validation_byte_size": 42,
                },
                "weighted_mds_plan": weighted_plan,
                "workload_fingerprint": weighted_plan["workload_fingerprint"],
                "cord_sweep": [
                    {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                    {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                    {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                    {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                    {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
                ],
                "runs": [
                    {
                        "cord_label": "1",
                        "batch_count": 2,
                        "mebibytes_per_second": 10.0,
                        "remote_client_topology": proven_remote_client_topology(),
                    },
                    {
                        "cord_label": "4",
                        "batch_count": 2,
                        "mebibytes_per_second": 10.0,
                        "remote_client_topology": proven_remote_client_topology(),
                    },
                    {
                        "cord_label": "8",
                        "batch_count": 2,
                        "mebibytes_per_second": 10.0,
                        "remote_client_topology": proven_remote_client_topology(),
                    },
                    {
                        "cord_label": "16",
                        "batch_count": 2,
                        "mebibytes_per_second": 10.0,
                        "remote_client_topology": proven_remote_client_topology(),
                    },
                    {
                        "cord_label": "64",
                        "batch_count": 2,
                        "mebibytes_per_second": 10.0,
                        "remote_client_topology": proven_remote_client_topology(),
                    },
                ],
                "run_count": 5,
                "runs_requested": 1,
                "sample_count": 30,
                "byte_size": 210,
                "mebibytes_per_second": 10.0,
                "benchmark_provenance": proven_quic_benchmark_provenance(),
                "read_path_evidence": proven_quic_read_path_evidence(),
                "read_path_reliability": proven_quic_read_path_reliability(),
                "remote_client_topology": proven_remote_client_topology_aggregate(report_count=5),
                "write_path_evidence": proven_quic_write_path_evidence(),
                "objective": "ai-training-25gbe",
                "objective_passed_any": False,
                "objective_passed_all": False,
                "network_path_summary": {
                    "java_python_network_path_exercised": True,
                    "cord_sweep_count": 5,
                    "cord_scaling_improvement_ratio": 0.0,
                    "remote_client_topology_complete": True,
                    "remote_client_per_source_complete": True,
                    "remote_client_topology_report_count": 5,
                },
                "server_evidence": {"complete": False, "missing": ["disk", "cache", "cpu"], "reports": {}},
                "blockfs_gate": {
                    "decision": "profile-storage",
                    "kernel_module_allowed": False,
                    "required_next_evidence": ["server disk evidence"],
                },
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--input-workload-report",
                    str(report),
                    "--input-matrix-report",
                    str(matrix),
                    "--output-dir",
                    str(output_dir),
                    "--server-disk-evidence",
                    str(disk),
                    "--server-cache-evidence",
                    str(cache),
                    "--server-cpu-evidence",
                    str(cpu),
                    "--surface-contract",
                    str(contract),
                    "--surface-require-remote",
                    "--surface-require-quic",
                    "--surface-require-matrix",
                    "--surface-min-sources",
                    "2",
                    "--surface-min-validation-samples",
                    "6",
                    "--surface-min-validation-bytes",
                    "42",
                    "--surface-min-workload-runs",
                    "5",
                    "--surface-min-workload-samples",
                    "30",
                    "--surface-min-workload-bytes",
                    "210",
                    "--surface-min-cord-sweep",
                    "5",
                    "--blockfs-min-runs",
                    "5",
                    "--blockfs-min-samples",
                    "30",
                    "--blockfs-min-bytes",
                    "210",
                    "--blockfs-min-cord-sweep",
                    "5",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertEqual(WORKLOAD_DECISION_FORMAT, decision["format"])
            self.assertTrue(decision["benchmark_completed"])
            self.assertTrue(decision["contract_applied"])
            self.assertEqual("cxnfs.trainer-contract.v1", decision["trainer_contract"]["format"])
            self.assertTrue(decision["trainer_surface_verified"])
            self.assertTrue(decision["trainer_surface_matrix_passed"])
            self.assertTrue(decision["trainer_surface_check_passed"])
            self.assertEqual("passed", decision["trainer_surface_matrix"]["status"])
            self.assertEqual("passed", decision["trainer_surface_check"]["status"])
            self.assertTrue(decision["java_python_network_path_exercised"])
            self.assertTrue(decision["read_path_evidence_complete"])
            self.assertTrue(decision["read_path_reliability_complete"])
            self.assertEqual(0, decision["unrecovered_read_failures"])
            self.assertTrue(decision["write_path_evidence_complete"])
            self.assertTrue(decision["read_path_evidence"]["frame_sink_used_for_all_direct_destination_reads"])
            self.assertTrue(decision["read_path_evidence"]["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(64, decision["read_path_evidence"]["direct_destination_max_observed_cords"])
            self.assertEqual(128, decision["read_path_evidence"]["quic_read_transport_read_streams_eof_verified"])
            self.assertTrue(decision["read_path_evidence"]["quic_read_transport_read_partition_coverage_verified"])
            self.assertEqual(128, decision["read_path_evidence"]["quic_read_transport_read_partition_count"])
            self.assertEqual(5, decision["write_path_evidence"]["quic_write_transport_write_ack_streams_eof_verified"])
            self.assertTrue(decision["server_evidence_complete"])
            self.assertTrue(decision["server_evidence_sample_window_complete"])
            self.assertTrue(decision["workload_scale_complete"])
            self.assertTrue(decision["pressure_profile_complete"])
            self.assertTrue(decision["cord_plateau_observed"])
            self.assertGreater(decision["target_gap_mebibytes_per_second"], 0.0)
            self.assertEqual("cxnfs.trainer-workload-pressure.v1", decision["pressure_profile"]["format"])
            self.assertTrue(decision["pressure_profile"]["decision_ready"])
            self.assertEqual("allow-blockfs-prototype", decision["pressure_profile"]["blockfs_decision"])
            self.assertEqual("cxnfs-quic", decision["transport"])
            self.assertEqual("allow-blockfs-prototype", decision["blockfs_gate"]["decision"])
            self.assertTrue(decision["kernel_module_allowed"])
            handoff = json.loads((output_dir / "blockfs-handoff.json").read_text(encoding="utf-8"))
            self.assertEqual(BLOCKFS_HANDOFF_FORMAT, handoff["format"])
            self.assertEqual("ready-for-linux-blockfs-prototype", handoff["status"])
            self.assertEqual("allow-blockfs-prototype", handoff["decision"])
            self.assertTrue(handoff["kernel_module_allowed"])
            self.assertEqual([], handoff["missing_readiness_checks"])
            self.assertEqual("/stage0-mds", handoff["workload"]["remote_root"])
            self.assertEqual(5, handoff["network_proof"]["cord_sweep_count"])
            self.assertTrue(handoff["network_proof"]["cord_plateau_observed"])
            self.assertTrue(handoff["server_evidence"]["sample_window_complete"])
            self.assertTrue(handoff["read_path_evidence"]["complete"])
            self.assertTrue(handoff["read_path_evidence"]["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(64, handoff["read_path_evidence"]["direct_destination_max_observed_cords"])
            self.assertTrue(handoff["read_path_reliability"]["complete"])
            self.assertEqual(0, handoff["read_path_reliability"]["unrecovered_read_failures"])
            self.assertTrue(handoff["write_path_evidence"]["complete"])
            self.assertTrue(handoff["linux_blockfs_candidate"]["safe_to_start_kernel_module"])
            self.assertTrue(handoff["linux_blockfs_candidate"]["must_repeat_remote_workload_benchmark"])
            self.assertIn("blockfs VM regression report", handoff["required_next_evidence"])
            self.assertEqual(
                str(output_dir / "workload-decision.json"),
                handoff["decision_bundle"]["decision_json"],
            )
            copied = json.loads((output_dir / "trainer-workload-benchmark.json").read_text(encoding="utf-8"))
            self.assertTrue(copied["server_evidence"]["complete"])
            self.assertEqual("allow-blockfs-prototype", copied["blockfs_gate"]["decision"])
            self.assertTrue(copied["blockfs_gate"]["read_path_evidence_complete"])
            self.assertTrue(copied["blockfs_gate"]["read_path_reliability_complete"])
            self.assertTrue(copied["blockfs_gate"]["workload_scale_complete"])
            self.assertTrue(copied["pressure_profile"]["decision_ready"])
            report_text = (output_dir / "decision.report").read_text(encoding="utf-8")
            self.assertIn("pressure_profile_complete=true", report_text)
            self.assertIn("read_path_reliability_complete=true", report_text)
            self.assertIn("unrecovered_read_failures=0", report_text)
            self.assertIn("cord_plateau_observed=true", report_text)
            self.assertIn(f"blockfs_handoff={output_dir / 'blockfs-handoff.json'}", report_text)
            surface = json.loads((output_dir / "trainer-surface-check.json").read_text(encoding="utf-8"))
            self.assertTrue(surface["passed"])
            self.assertEqual(5, surface["requirements"]["min_cord_sweep"])
            self.assertTrue(surface["requirements"]["require_matrix"])
            copied_matrix = json.loads((output_dir / "trainer-surface-matrix.json").read_text(encoding="utf-8"))
            self.assertTrue(copied_matrix["passed"])
            self.assertEqual(4, copied_matrix["case_count"])

    def test_cli_defers_existing_quic_workload_without_blockfs_scale_requirements(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "quic-contract-workload.json"
            output_dir = root / "decision"
            disk = root / "disk.report"
            cache = root / "cache.json"
            cpu = root / "cpu.report"
            write_server_evidence_files(disk, cache, cpu)
            weighted_plan = proven_quic_weighted_plan_payload()
            report.write_text(json.dumps({
                "format": "cxnfs.trainer-workload-benchmark.v1",
                "workload_name": "quic-contract",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_batch_iterator": "native",
                "data_loader_workers": 0,
                "bulk_read_retries": 1,
                "bulk_write_retries": 1,
                "remote_root": "/stage0-mds",
                "contract_applied": True,
                "weighted_mds_plan": weighted_plan,
                "workload_fingerprint": weighted_plan["workload_fingerprint"],
                "cord_sweep": [
                    {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                    {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                    {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                    {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                    {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
                ],
                "runs": [
                    {"cord_label": "1", "batch_count": 2, "mebibytes_per_second": 10.0},
                    {"cord_label": "4", "batch_count": 2, "mebibytes_per_second": 10.0},
                    {"cord_label": "8", "batch_count": 2, "mebibytes_per_second": 10.0},
                    {"cord_label": "16", "batch_count": 2, "mebibytes_per_second": 10.0},
                    {"cord_label": "64", "batch_count": 2, "mebibytes_per_second": 10.0},
                ],
                "run_count": 5,
                "sample_count": 30,
                "byte_size": 210,
                "mebibytes_per_second": 10.0,
                "read_path_evidence": proven_quic_read_path_evidence(),
                "read_path_reliability": proven_quic_read_path_reliability(),
                "remote_client_topology": proven_remote_client_topology_aggregate(report_count=5),
                "write_path_evidence": proven_quic_write_path_evidence(),
                "objective": "ai-training-25gbe",
                "objective_passed_any": False,
                "objective_passed_all": False,
                "server_evidence": {"complete": False, "missing": ["disk", "cache", "cpu"], "reports": {}},
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--input-workload-report",
                    str(report),
                    "--output-dir",
                    str(output_dir),
                    "--server-disk-evidence",
                    str(disk),
                    "--server-cache-evidence",
                    str(cache),
                    "--server-cpu-evidence",
                    str(cpu),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertFalse(decision["workload_scale_complete"])
            self.assertEqual("prove-workload-scale", decision["blockfs_gate"]["decision"])
            self.assertFalse(decision["kernel_module_allowed"])
            self.assertIn("explicit blockfs workload minimums must be configured", decision["required_next_evidence"])
            handoff = json.loads((output_dir / "blockfs-handoff.json").read_text(encoding="utf-8"))
            self.assertEqual(BLOCKFS_HANDOFF_FORMAT, handoff["format"])
            self.assertEqual("deferred", handoff["status"])
            self.assertFalse(handoff["kernel_module_allowed"])
            self.assertIsNone(handoff["linux_blockfs_candidate"])
            self.assertIn("workload_scale_complete", handoff["missing_readiness_checks"])
            self.assertIn("explicit blockfs workload minimums must be configured", handoff["required_next_evidence"])

    def test_cli_defers_blockfs_when_java_python_network_path_is_not_proven(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "quic-contract-workload.json"
            output_dir = root / "decision"
            disk = root / "disk.report"
            cache = root / "cache.json"
            cpu = root / "cpu.report"
            write_server_evidence_files(disk, cache, cpu)
            weighted_plan = proven_quic_weighted_plan_payload()
            report.write_text(json.dumps({
                "format": "cxnfs.trainer-workload-benchmark.v1",
                "workload_name": "quic-contract",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_batch_iterator": "native",
                "remote_root": "/stage0-mds",
                "contract_applied": True,
                "weighted_mds_plan": weighted_plan,
                "workload_fingerprint": weighted_plan["workload_fingerprint"],
                "cord_sweep": [
                    {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                    {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                    {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                    {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
                    {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
                ],
                "runs": [
                    {"cord_label": "1", "mebibytes_per_second": 10.0},
                    {"cord_label": "4", "mebibytes_per_second": 10.0},
                    {"cord_label": "8", "mebibytes_per_second": 10.0},
                    {"cord_label": "16", "mebibytes_per_second": 10.0},
                    {"cord_label": "64", "mebibytes_per_second": 10.0},
                ],
                "run_count": 5,
                "sample_count": 30,
                "byte_size": 210,
                "mebibytes_per_second": 10.0,
                "benchmark_provenance": proven_quic_benchmark_provenance(),
                "read_path_evidence": proven_quic_read_path_evidence(),
                "read_path_reliability": proven_quic_read_path_reliability(),
                "write_path_evidence": proven_quic_write_path_evidence(),
                "objective": "ai-training-25gbe",
                "objective_passed_any": False,
                "objective_passed_all": False,
                "network_path_summary": {
                    "java_python_network_path_exercised": False,
                    "cord_sweep_count": 5,
                    "cord_scaling_improvement_ratio": 0.0,
                },
                "server_evidence": {"complete": False, "missing": ["disk", "cache", "cpu"], "reports": {}},
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--input-workload-report",
                    str(report),
                    "--output-dir",
                    str(output_dir),
                    "--server-disk-evidence",
                    str(disk),
                    "--server-cache-evidence",
                    str(cache),
                    "--server-cpu-evidence",
                    str(cpu),
                    "--blockfs-min-runs",
                    "5",
                    "--blockfs-min-samples",
                    "30",
                    "--blockfs-min-bytes",
                    "210",
                    "--blockfs-min-cord-sweep",
                    "5",
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertEqual("prove-java-python-network-path", decision["blockfs_gate"]["decision"])
            self.assertFalse(decision["java_python_network_path_exercised"])
            self.assertFalse(decision["kernel_module_allowed"])
            self.assertIn(
                "authenticated Java/Python remote QUIC workload proof",
                decision["required_next_evidence"],
            )
            handoff = json.loads((output_dir / "blockfs-handoff.json").read_text(encoding="utf-8"))
            self.assertEqual(BLOCKFS_HANDOFF_FORMAT, handoff["format"])
            self.assertEqual("deferred", handoff["status"])
            self.assertFalse(handoff["kernel_module_allowed"])
            self.assertIn("java_python_network_path_exercised", handoff["missing_readiness_checks"])
            self.assertIn(
                "authenticated Java/Python remote QUIC workload proof",
                handoff["required_next_evidence"],
            )

    def test_cli_defers_existing_quic_workload_without_direct_read_path_evidence(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "quic-contract-workload.json"
            output_dir = root / "decision"
            disk = root / "disk.report"
            cache = root / "cache.json"
            cpu = root / "cpu.report"
            write_server_evidence_files(disk, cache, cpu)
            report.write_text(json.dumps({
                "format": "cxnfs.trainer-workload-benchmark.v1",
                "workload_name": "quic-contract",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_batch_iterator": "native",
                "data_loader_workers": 0,
                "bulk_read_retries": 1,
                "bulk_write_retries": 1,
                "remote_root": "/stage0-mds",
                "contract_applied": True,
                "trainer_contract": {
                    "path": "/var/log/cxnfs/trainer-contract.json",
                    "sha256": "00" * 32,
                    "format": "cxnfs.trainer-contract.v1",
                    "validated": True,
                    "validation_sample_count": 6,
                    "validation_byte_size": 42,
                },
                "weighted_mds_plan": {"sources": [{"name": "chat"}, {"name": "summary"}]},
                "cord_sweep": [
                    {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                    {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
                    {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
                ],
                "runs": [
                    {"cord_label": "1", "mebibytes_per_second": 10.0},
                    {"cord_label": "4", "mebibytes_per_second": 10.0},
                    {"cord_label": "8", "mebibytes_per_second": 10.0},
                ],
                "run_count": 3,
                "sample_count": 30,
                "byte_size": 210,
                "mebibytes_per_second": 10.0,
                "remote_client_topology": proven_remote_client_topology_aggregate(report_count=3),
                "objective": "ai-training-25gbe",
                "objective_passed_any": False,
                "objective_passed_all": False,
                "network_path_summary": {
                    "java_python_network_path_exercised": True,
                    "cord_sweep_count": 3,
                    "cord_scaling_improvement_ratio": 0.0,
                },
                "server_evidence": {"complete": False, "missing": ["disk", "cache", "cpu"], "reports": {}},
                "blockfs_gate": {
                    "decision": "profile-storage",
                    "kernel_module_allowed": False,
                    "required_next_evidence": ["server disk evidence"],
                },
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            exit_code = run_workload_decision_cli(
                [
                    "--input-workload-report",
                    str(report),
                    "--output-dir",
                    str(output_dir),
                    "--server-disk-evidence",
                    str(disk),
                    "--server-cache-evidence",
                    str(cache),
                    "--server-cpu-evidence",
                    str(cpu),
                ],
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(0, exit_code, stderr.getvalue())
            decision = json.loads(stdout.getvalue())
            self.assertFalse(decision["read_path_evidence_complete"])
            self.assertEqual("prove-quic-read-path", decision["blockfs_gate"]["decision"])
            self.assertFalse(decision["kernel_module_allowed"])
            self.assertIn("read_path_evidence", decision["required_next_evidence"])


def write_file_dataset(root: Path, name: str, values: tuple[str, ...]) -> None:
    raw = root / "raw" / name
    raw.mkdir(parents=True)
    for index, value in enumerate(values):
        (raw / f"{index:04d}.txt").write_text(value, encoding="utf-8")
    plan = DatasetPreparer(target_shard_size_bytes=4096).plan(raw, name, "files")
    DatasetConverter().execute(plan, root / "mds" / name)


def write_proc_fixture(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "stat").write_text(PROC_STAT, encoding="utf-8")
    (root / "diskstats").write_text(PROC_DISKSTATS, encoding="utf-8")
    (root / "meminfo").write_text(PROC_MEMINFO, encoding="utf-8")


def write_server_evidence_files(disk: Path, cache: Path, cpu: Path) -> None:
    start = 1000.0
    end = 1005.0
    disk.write_text(
        "\n".join((
            "cxnfs.server.disk.v1",
            f"sample_start_unix_seconds={start:.6f}",
            f"sample_end_unix_seconds={end:.6f}",
            "disk_busy_percent=96",
            "",
        )),
        encoding="utf-8",
    )
    cache.write_text(json.dumps({
        "format": "cxnfs.server.cache.v1",
        "sample_start_unix_seconds": start,
        "sample_end_unix_seconds": end,
        "cache_effective": False,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    cpu.write_text(
        "\n".join((
            "cxnfs.server.cpu.v1",
            f"sample_start_unix_seconds={start:.6f}",
            f"sample_end_unix_seconds={end:.6f}",
            "cpu_idle_percent=35",
            "",
        )),
        encoding="utf-8",
    )


def proven_quic_read_path_evidence() -> dict[str, object]:
    return {
        "all_runs_have_evidence": True,
        "read_retry_limit": 1,
        "read_retry_attempts": 0,
        "read_retry_successes": 0,
        "bulk_transport_reconnects": 0,
        "failed_read_operations": 0,
        "failed_byte_return_read_operations": 0,
        "failed_direct_buffer_read_operations": 0,
        "failed_direct_tensor_read_operations": 0,
        "failed_direct_destination_read_operations": 0,
        "direct_destination_zeroed_bytes_on_failure": 0,
        "bulk_payload_transport_count": 5,
        "direct_destination_reads_observed": True,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_buffer_read_operations": 8,
        "direct_tensor_read_operations": 0,
        "direct_destination_read_operations": 8,
        "direct_destination_frame_sink_read_operations": 8,
        "direct_destination_parallel_frame_sink_read_operations": 8,
        "direct_destination_parallel_frame_sink_frames": 32,
        "direct_destination_max_observed_cords": 64,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "frame_sink_read_operations": 8,
        "max_observed_cords": 64,
        "quic_read_transport_observed": True,
        "quic_read_transport_complete": True,
        "quic_read_transport_evidence_count": 2,
        "quic_read_transport_connection_opened_count": 2,
        "quic_read_transport_connection_attempt_count": 2,
        "quic_read_transport_connection_retry_count": 0,
        "quic_read_transport_successful_read_operations": 8,
        "quic_read_transport_failed_read_operations": 0,
        "quic_read_transport_last_read_stream_count": 64,
        "quic_read_transport_last_attempted_read_stream_count": 64,
        "quic_read_transport_attempted_read_streams": 128,
        "quic_read_transport_read_streams_created": 128,
        "quic_read_transport_read_streams_eof_verified": 128,
        "quic_read_transport_read_stream_completion_report_count": 2,
        "quic_read_transport_read_stream_eof_report_count": 2,
        "quic_read_transport_stream_completion_verified": True,
        "quic_read_transport_stream_eof_verified": True,
        "quic_read_transport_read_partition_count": 128,
        "quic_read_transport_read_partition_chunk_count": 128,
        "quic_read_transport_read_partition_requested_bytes": 1_048_576,
        "quic_read_transport_read_partition_payload_bytes": 1_048_576,
        "quic_read_transport_read_partition_coverage_report_count": 2,
        "quic_read_transport_read_partition_coverage_verified": True,
        "quic_read_transport_total_attempted_streams": 128,
        "quic_read_transport_total_successful_streams": 128,
        "quic_read_transport_connection_reuse_count": 2,
        "quic_read_transport_max_read_stream_count": 64,
        "quic_read_transport_connection_reuse_observed": True,
        "quic_read_transport_expected_cipher_suite_count": 2,
        "quic_read_transport_all_reports_expected_cipher_suite": True,
        "quic_read_transport_cipher_suite_counts": {"TLS_AES_128_GCM_SHA256": 2},
        "quic_read_transport_tls_version_counts": {"TLSv1.3": 2},
    }


def proven_quic_read_path_reliability() -> dict[str, object]:
    return {
        "format": "cxnfs.trainer-read-path-reliability.v1",
        "complete": True,
        "missing": [],
        "retry_enabled": True,
        "read_retry_limit": 1,
        "read_retry_attempts": 0,
        "read_retry_successes": 0,
        "bulk_transport_reconnects": 0,
        "failed_read_operations": 0,
        "failed_byte_return_read_operations": 0,
        "failed_direct_buffer_read_operations": 0,
        "failed_direct_tensor_read_operations": 0,
        "failed_direct_destination_read_operations": 0,
        "direct_destination_zeroed_bytes_on_failure": 0,
        "recovered_read_failures": 0,
        "unrecovered_read_failures": 0,
        "retry_attempts_recovered": True,
        "retry_reconnects_observed": True,
        "all_runs_have_evidence": True,
    }


def proven_quic_write_path_evidence() -> dict[str, object]:
    return {
        "format": "cxnfs.workload-write-path-evidence.v1",
        "enabled": True,
        "write_probe_count": 5,
        "write_probe_cord_sweep_count": 5,
        "write_probe_cord_labels": ["1", "4", "8", "16", "64"],
        "run_count": 5,
        "all_runs_have_evidence": True,
        "verified_read_back_count": 5,
        "all_write_probes_verified": True,
        "write_operations": 5,
        "direct_tensor_write_operations": 5,
        "directory_output_write_operations": 0,
        "payload_bytes_written": 20480,
        "write_frames": 5,
        "write_acks": 5,
        "write_ack_frame_verified": 5,
        "write_ack_frame_identity_verified": True,
        "write_ack_metadata_verified": 0,
        "write_ack_metadata_pending": 5,
        "write_ack_memory": 5,
        "write_ack_spill": 0,
        "metadata_after_payload_entries": 5,
        "entries_committed": 5,
        "transfers_committed": 5,
        "max_observed_write_cords": 64,
        "write_retry_limit": 1,
        "write_retry_attempts": 0,
        "write_retry_successes": 0,
        "bulk_transport_write_reconnects": 0,
        "write_retry_attempts_recovered": True,
        "write_retry_reconnects_observed": True,
        "metadata_pending_observed": True,
        "direct_tensor_writes_observed": True,
        "quic_write_transport_evidence_count": 5,
        "quic_write_transport_connection_reuse_count": 5,
        "quic_write_transport_successful_write_operations": 5,
        "quic_write_transport_failed_write_operations": 0,
        "quic_write_transport_attempted_write_streams": 5,
        "quic_write_transport_write_streams_created": 5,
        "quic_write_transport_write_ack_streams_eof_verified": 5,
        "quic_write_transport_max_active_write_streams": 2,
        "quic_write_transport_concurrent_stream_report_count": 4,
        "quic_write_transport_concurrent_streams_observed": True,
        "quic_write_transport_write_stream_success_report_count": 5,
        "quic_write_transport_all_reports_write_streams_succeeded": True,
        "quic_write_transport_write_ack_stream_completion_report_count": 5,
        "quic_write_transport_write_ack_stream_eof_report_count": 5,
        "quic_write_transport_all_reports_write_ack_stream_completion_verified": True,
        "quic_write_transport_all_reports_write_ack_stream_eof_verified": True,
        "quic_write_transport_write_acks_received": 5,
        "quic_write_transport_write_acks_verified": 5,
        "quic_write_transport_write_ack_verification_failures": 0,
        "quic_write_transport_write_ack_identity_report_count": 5,
        "quic_write_transport_all_reports_write_ack_frame_identity_verified": True,
        "quic_write_transport_write_ack_metadata_pending": 5,
        "quic_write_transport_write_ack_metadata_pending_observed": True,
        "quic_write_transport_connection_opened_count": 5,
        "quic_write_transport_all_reports_present": True,
        "quic_write_transport_all_reports_reused_connection": True,
        "quic_write_transport_expected_cipher_suite_count": 5,
        "quic_write_transport_all_reports_expected_cipher_suite": True,
        "quic_write_transport_cipher_suite_counts": {"TLS_AES_128_GCM_SHA256": 5},
        "quic_write_transport_tls_version_counts": {"TLSv1.3": 5},
        "quic_write_transport_complete": True,
        "complete": True,
    }


def proven_quic_weighted_plan_payload() -> dict[str, object]:
    plan: dict[str, object] = {
        "format": "cxnfs.weighted-mds-plan.v1",
        "source_count": 2,
        "total_weight": 3,
        "total_samples": 6,
        "total_bytes": 42,
        "source_schedule": ["chat", "chat", "summary"],
        "sources": [
            {"name": "chat", "weight": 2, "sample_count": 4, "shard_count": 1, "byte_size": 28, "shards": ["chat-000000.mds"]},
            {"name": "summary", "weight": 1, "sample_count": 2, "shard_count": 1, "byte_size": 14, "shards": ["summary-000000.mds"]},
        ],
    }
    plan["workload_fingerprint"] = weighted_mds_plan_fingerprint(plan)
    return plan


def proven_quic_benchmark_provenance() -> dict[str, object]:
    return {
        "format": "cxnfs.trainer-workload-provenance.v1",
        "invocation": {
            "root": "/var/cache/cxnfs/indexes/stage0-mds",
            "weights": {"chat": 2, "summary": 1},
            "workload_name": "quic-contract",
            "remote_root": "/stage0-mds",
            "bulk_transport": "quic",
            "read_mode": "direct-buffers",
            "trainer_batch_iterator": "native",
            "data_loader_workers": 0,
            "bulk_read_retries": 1,
            "bulk_write_retries": 1,
            "runs": 1,
            "cord_sweep": [
                {"initial_cords": 1, "max_cords": 1},
                {"initial_cords": 4, "max_cords": 4},
                {"initial_cords": 8, "max_cords": 8},
                {"initial_cords": 16, "max_cords": 16},
                {"initial_cords": 64, "max_cords": 64},
            ],
        },
        "invocation_fingerprint": {"algorithm": "sha256", "sha256": "1" * 64},
    }


def proven_remote_client_topology(expected_source_count=2, shared_client=False) -> dict[str, object]:
    names = ["chat", "summary"][:expected_source_count]
    connection_names = names[:1] if shared_client else list(names)
    open_names = names[:1] if shared_client else list(names)
    connection_source_set_matches_requests = set(connection_names) == set(names)
    client_open_source_set_matches_requests = set(open_names) == set(names)
    return {
        "format": "cxnfs.trainer-remote-client-topology.v1",
        "remote_client_required": True,
        "expected_source_count": expected_source_count,
        "shared_client": shared_client,
        "source_request_count": len(names),
        "unique_source_request_count": len(names),
        "source_request_names": list(names),
        "unique_source_request_names": list(names),
        "connection_count": len(connection_names),
        "connection_source_names": list(connection_names),
        "unique_connection_source_names": list(connection_names),
        "client_open_count": len(open_names),
        "client_open_source_names": list(open_names),
        "unique_client_open_source_names": list(open_names),
        "client_close_count": 0,
        "client_close_source_names": [],
        "unique_client_close_source_names": [],
        "active_client_count": len(open_names),
        "active_client_source_names": list(open_names),
        "unique_active_client_source_names": list(open_names),
        "connection_source_set_matches_requests": connection_source_set_matches_requests,
        "client_open_source_set_matches_requests": client_open_source_set_matches_requests,
        "client_close_source_set_matches_opens": False,
        "client_lifecycle_source_set_complete": False,
        "per_source_client_observed": not shared_client,
        "shared_client_observed": shared_client,
        "complete": True,
    }


def proven_remote_client_topology_aggregate(report_count=5, expected_source_count=2) -> dict[str, object]:
    names = ["chat", "summary"][:expected_source_count]
    return {
        "format": "cxnfs.trainer-remote-client-topology-aggregate.v1",
        "remote_client_required": True,
        "expected_source_count": expected_source_count,
        "report_count": report_count,
        "topology_report_count": report_count,
        "all_reports_have_topology": True,
        "all_topologies_complete": True,
        "per_source_client_observed_count": report_count,
        "shared_client_observed_count": 0,
        "connection_source_set_matches_requests_count": report_count,
        "client_open_source_set_matches_requests_count": report_count,
        "client_lifecycle_source_set_complete_count": 0,
        "all_connection_source_sets_match_requests": True,
        "all_client_open_source_sets_match_requests": True,
        "all_client_lifecycle_source_sets_complete": False,
        "minimum_unique_source_request_count": expected_source_count,
        "minimum_connection_count": expected_source_count,
        "source_request_count": report_count * expected_source_count,
        "connection_count": report_count * expected_source_count,
        "client_open_count": report_count * expected_source_count,
        "client_close_count": 0,
        "active_client_count": report_count * expected_source_count,
        "unique_source_request_names": names,
        "unique_connection_source_names": names,
        "unique_client_open_source_names": names,
        "unique_client_close_source_names": [],
        "unique_active_client_source_names": names,
        "per_source_client_complete": True,
        "shared_client_complete": False,
        "complete": True,
    }


def proven_quic_workload_payload() -> dict[str, object]:
    weighted_plan = proven_quic_weighted_plan_payload()
    return {
        "format": "cxnfs.trainer-workload-benchmark.v1",
        "workload_name": "quic-contract",
        "root": "/var/cache/cxnfs/indexes/stage0-mds",
        "weights": {"chat": 2, "summary": 1},
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "read_mode": "direct-buffers",
        "trainer_batch_iterator": "native",
        "data_loader_workers": 0,
        "bulk_read_retries": 1,
        "bulk_write_retries": 1,
        "remote_root": "/stage0-mds",
        "contract_applied": True,
        "trainer_contract": {
            "path": "/var/log/cxnfs/trainer-contract.json",
            "sha256": "00" * 32,
            "format": "cxnfs.trainer-contract.v1",
            "validated": True,
            "validation_sample_count": 6,
            "validation_byte_size": 42,
            "workload_fingerprint": weighted_plan["workload_fingerprint"],
        },
        "weighted_mds_plan": weighted_plan,
        "workload_fingerprint": weighted_plan["workload_fingerprint"],
        "cord_sweep": [
            {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
            {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            {"cord_index": 2, "initial_cords": 8, "max_cords": 8},
            {"cord_index": 3, "initial_cords": 16, "max_cords": 16},
            {"cord_index": 4, "initial_cords": 64, "max_cords": 64},
        ],
        "runs": [
            {
                "cord_label": "1",
                "batch_count": 2,
                "mebibytes_per_second": 10.0,
                "remote_client_topology": proven_remote_client_topology(),
            },
            {
                "cord_label": "4",
                "batch_count": 2,
                "mebibytes_per_second": 10.0,
                "remote_client_topology": proven_remote_client_topology(),
            },
            {
                "cord_label": "8",
                "batch_count": 2,
                "mebibytes_per_second": 10.0,
                "remote_client_topology": proven_remote_client_topology(),
            },
            {
                "cord_label": "16",
                "batch_count": 2,
                "mebibytes_per_second": 10.0,
                "remote_client_topology": proven_remote_client_topology(),
            },
            {
                "cord_label": "64",
                "batch_count": 2,
                "mebibytes_per_second": 10.0,
                "remote_client_topology": proven_remote_client_topology(),
            },
        ],
        "runs_requested": 1,
        "run_count": 5,
        "sample_count": 30,
        "byte_size": 210,
        "mebibytes_per_second": 10.0,
        "benchmark_provenance": proven_quic_benchmark_provenance(),
        "read_path_evidence": proven_quic_read_path_evidence(),
        "read_path_reliability": proven_quic_read_path_reliability(),
        "remote_client_topology": proven_remote_client_topology_aggregate(report_count=5),
        "write_path_evidence": proven_quic_write_path_evidence(),
        "objective": "ai-training-25gbe",
        "objective_passed_any": False,
        "objective_passed_all": False,
        "network_path_summary": {
            "java_python_network_path_exercised": True,
            "cord_sweep_count": 5,
            "cord_scaling_improvement_ratio": 0.0,
            "remote_client_topology_complete": True,
            "remote_client_per_source_complete": True,
            "remote_client_topology_report_count": 5,
        },
        "server_evidence": {"complete": False, "missing": ["disk", "cache", "cpu"], "reports": {}},
    }


def proven_surface_matrix_payload(contract: dict[str, object]) -> dict[str, object]:
    weighted_plan = contract["weighted_mds_plan"]
    read_contract = contract.get("read_contract")
    network_contract = contract.get("network_contract")
    trainer_prefetch_batches = 0
    if isinstance(read_contract, dict):
        trainer_prefetch_batches = int(read_contract.get("trainer_prefetch_batches", 0) or 0)
    bulk_read_retries = 0
    if isinstance(network_contract, dict):
        bulk_read_retries = int(network_contract.get("bulk_read_retries", 0) or 0)
    cases = []
    for read_mode in ("direct-buffers", "direct-tensors"):
        for iterator in ("native", "trainer-dataloader"):
            is_tensor = read_mode == "direct-tensors"
            read_path_evidence = proven_quic_read_path_evidence()
            read_path_evidence.update({
                "bulk_payload_transport_count": 2,
                "direct_buffer_read_operations": 0 if is_tensor else 6,
                "direct_tensor_read_operations": 6 if is_tensor else 0,
                "direct_destination_read_operations": 6,
                "direct_destination_frame_sink_read_operations": 6,
                "frame_sink_read_operations": 6,
                "max_observed_cords": 2,
                "uses_caller_owned_destinations": True,
                "quic_read_transport_evidence_count": 2,
                "quic_read_transport_successful_read_operations": 6,
                "quic_read_transport_attempted_read_streams": 12,
                "quic_read_transport_read_streams_created": 12,
                "quic_read_transport_read_streams_eof_verified": 12,
                "quic_read_transport_read_partition_count": 12,
                "quic_read_transport_read_partition_chunk_count": 12,
                "quic_read_transport_read_partition_requested_bytes": 98_304,
                "quic_read_transport_read_partition_payload_bytes": 98_304,
                "quic_read_transport_read_partition_coverage_report_count": 2,
                "quic_read_transport_read_partition_coverage_verified": True,
                "quic_read_transport_total_attempted_streams": 12,
                "quic_read_transport_total_successful_streams": 12,
                "quic_read_transport_connection_reuse_count": 2,
                "quic_read_transport_max_read_stream_count": 2,
            })
            cases.append({
                "case_name": f"{read_mode}/{iterator}",
                "status": "passed",
                "passed": True,
                "read_mode": read_mode,
                "destination_type": "torch-uint8-tensor" if is_tensor else "writable-byte-buffer",
                "trainer_batch_iterator": iterator,
                "data_loader_workers": 0,
                "trainer_prefetch_batches": trainer_prefetch_batches,
                "trainer_prefetch_enabled": trainer_prefetch_batches > 0,
                "tensor_allocation": {
                    "format": "cxnfs.trainer-tensor-allocation.v1",
                    "target": "pytorch-cpu",
                    "dtype": "uint8",
                    "device": "cpu",
                    "non_blocking": True,
                    "uses_pinned_memory": False,
                    "caller_owned_destination": True,
                } if is_tensor else None,
                "batch_count": 1,
                "sample_count": 6,
                "byte_size": 42,
                "source_counts": {"chat": 4, "summary": 2},
                "read_path_evidence": read_path_evidence,
                "read_path_reliability": proven_quic_read_path_reliability(),
                "remote_client_topology": proven_remote_client_topology(),
                "worker_partition_evidence": {
                    "format": "cxnfs.trainer-worker-partition.v1",
                    "complete": True,
                    "trainer_batch_iterator": iterator,
                    "worker_sharding_enabled": False,
                    "data_loader_workers": 0,
                    "expected_effective_training_shards": 1,
                    "observed_training_shard_count": 1,
                    "duplicate_source_sample_count": 0,
                    "no_duplicate_source_samples": True,
                },
                "memory_path_gate": {
                    "complete": True,
                    "missing": [],
                    "read_mode": read_mode,
                    "direct_buffer_read_operations": 0 if is_tensor else 6,
                    "direct_tensor_read_operations": 6 if is_tensor else 0,
                    "direct_destination_read_operations": 6,
                    "direct_destination_reads_observed": True,
                    "tensor_allocation_declared": is_tensor,
                    "tensor_allocation_target": "pytorch-cpu" if is_tensor else None,
                    "tensor_allocation_dtype": "uint8" if is_tensor else None,
                    "tensor_allocation_device": "cpu" if is_tensor else None,
                },
            })
    return {
        "format": "cxnfs.trainer-surface-matrix.v1",
        "root": contract["root"],
        "remote_root": contract.get("remote_root"),
        "weights": contract["weights"],
        "weighted_mds_plan": weighted_plan,
        "workload_fingerprint": weighted_plan["workload_fingerprint"],
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "requires_mount": False,
        "touches_trainer_code": False,
        "case_count": len(cases),
        "passed_case_count": len(cases),
        "failed_case_count": 0,
        "trainer_prefetch_batches": trainer_prefetch_batches,
        "bulk_read_retries": bulk_read_retries,
        "read_path_reliability_complete": True,
        "worker_partition_required": False,
        "worker_partition_complete": True,
        "worker_partition_duplicate_source_sample_count": 0,
        "unrecovered_read_failures": 0,
        "read_retry_successes": 0,
        "bulk_transport_reconnects": 0,
        "remote_client_topology": proven_remote_client_topology_aggregate(report_count=len(cases)),
        "remote_client_topology_complete": True,
        "remote_client_per_source_complete": True,
        "passed": True,
        "status": "passed",
        "cases": cases,
    }


def proven_quic_trainer_contract_payload() -> dict[str, object]:
    plan = proven_quic_weighted_plan_payload()
    return {
        "format": "cxnfs.trainer-contract.v1",
        "root": "/var/cache/cxnfs/indexes/stage0-mds",
        "remote_root": "/stage0-mds",
        "weights": {"chat": 2, "summary": 1},
        "weighted_mds_plan": plan,
        "workload_fingerprint": plan["workload_fingerprint"],
        "python_surface": {
            "requires_mount": False,
            "touches_trainer_code": False,
            "supports_weighted_sources": True,
            "supports_per_source_clients": True,
            "supports_direct_buffers": True,
            "supports_direct_tensors": True,
            "supports_tensor_allocation_policy": True,
            "supports_dataloader_worker_partitioning": True,
            "default_tensor_allocation": {
                "format": "cxnfs.trainer-tensor-allocation.v1",
                "target": "pytorch-pinned-cpu",
                "dtype": "uint8",
                "device": "cpu",
                "non_blocking": True,
                "uses_pinned_memory": True,
                "caller_owned_destination": True,
            },
            "supports_trainer_dataloader": True,
            "supports_trainer_stream": True,
            "supports_trainer_cursor": True,
            "supports_trainer_prefetch_stream": True,
            "stream_object": "CxnfsTrainerStream",
            "stream_state_format": "cxnfs.trainer-stream-state.v1",
            "dataloader_batch_object": "CxnfsTrainerBatch",
            "dataloader_batch_size": None,
        },
        "network_contract": {
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "bulk_transport_available": True,
            "remote_reads_require_credentials": True,
            "raw_password_sent_over_control": False,
            "max_cords": 64,
            "bulk_read_retries": 1,
            "bulk_write_retries": 1,
        },
        "read_contract": {
            "read_mode": "direct-buffers",
            "tensor_allocation": None,
            "trainer_batch_iterator": "native",
            "data_loader_workers": 0,
            "worker_partitioning": {
                "format": "cxnfs.trainer-worker-partition.v1",
                "enabled": False,
                "base_training_shard": {"index": 0, "count": 1},
                "effective_training_shard_count": 1,
            },
            "trainer_prefetch_batches": 2,
        },
        "validation": {
            "validated": True,
            "batch_count": 2,
            "sample_count": 6,
            "byte_size": 42,
        },
    }


if __name__ == "__main__":
    unittest.main()
