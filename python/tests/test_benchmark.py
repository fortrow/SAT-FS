import io
import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import UUID

from cxnfs.auth import AuthMechanism, ClientCredentials
from cxnfs.benchmark import (
    NativeReadBenchmarkConfig,
    NativeReadBenchmarkResult,
    evaluate_native_read_objective,
    run_benchmark_cli,
    run_native_read_benchmark,
)
from cxnfs.bulk import CxnfsBulkReadConfig
from cxnfs.control import AuthAccepted, FileMetadata, FileType, StatPathResponse


class FakeControlClient:
    def __init__(self, size=4096):
        self.size = size
        self.authenticated = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.closed = True

    def authenticate(self, credentials):
        self.authenticated = True
        return AuthAccepted(
            UUID("00000000-0000-0000-0000-000000000123"),
            (AuthMechanism.USERNAME_PASSWORD,),
        )

    def stat_path(self, path):
        return StatPathResponse(
            FileMetadata(
                path,
                Path(path).name,
                FileType.REGULAR_FILE,
                self.size,
            )
        )


class FakeBulkClient:
    def __init__(self):
        self.closed = False
        self.reads = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.closed = True

    def read_mds_shard(self, path, offset=0, length=None):
        self.reads.append((path, offset, length))
        return b"x" * length


class CxnfsBenchmarkTest(unittest.TestCase):
    def test_native_read_benchmark_reads_warmup_and_measured_blocks(self):
        control = FakeControlClient(size=4096)
        bulk = FakeBulkClient()

        result = run_native_read_benchmark(
            control,
            bulk,
            NativeReadBenchmarkConfig(
                "cxnfs-python-native",
                "/datasets/train/shard.00000.mds",
                iterations=2,
                warmup_iterations=1,
                block_size=1024,
            ),
        )

        self.assertEqual(8192, result.bytes_read)
        self.assertEqual(12, len(bulk.reads))
        self.assertEqual(("/datasets/train/shard.00000.mds", 0, 1024), bulk.reads[0])
        self.assertEqual(("/datasets/train/shard.00000.mds", 3072, 1024), bulk.reads[-1])

    def test_benchmark_cli_uses_hidden_prompt_and_writes_report(self):
        control = FakeControlClient(size=2048)
        bulk = FakeBulkClient()
        captured_bulk_options = []
        out = io.StringIO()
        err = io.StringIO()
        with TemporaryDirectory() as directory:
            report = Path(directory, "benchmark.txt")

            exit_code = run_benchmark_cli(
                [
                    "/datasets/train/shard.00000.mds",
                    "--host", "cxnfs.example.internal",
                    "--iterations", "2",
                    "--warmup", "0",
                    "--block-size", "512",
                    "--bulk-transport", "stream",
                    "--initial-cords", "8",
                    "--max-cords", "64",
                    "--report", str(report),
                ],
                out=out,
                err=err,
                credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                control_client_factory=lambda *args, **kwargs: control,
                bulk_client_factory=lambda *args, **kwargs: _capture_bulk_config(
                    bulk,
                    captured_bulk_options,
                    kwargs.get("read_config"),
                    kwargs.get("write_connections"),
                ),
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, err.getvalue())
            self.assertTrue(control.authenticated)
            self.assertTrue(control.closed)
            self.assertTrue(bulk.closed)
            read_config, write_connections = captured_bulk_options[0]
            self.assertEqual(8, read_config.initial_cords)
            self.assertEqual(64, read_config.max_cords)
            self.assertEqual(1, write_connections)
            self.assertIn("bytes=4096", out.getvalue())
            self.assertIn("bulk_transport=stream", out.getvalue())
            report_text = report.read_text(encoding="utf-8")
            self.assertIn("cxnfs.python.benchmark.native-read.v1", report_text)
            self.assertIn("path=/datasets/train/shard.00000.mds", report_text)
            self.assertIn("payload_format=mds_shards", report_text)
            self.assertIn("bulk_transport=stream", report_text)
            self.assertIn("bytes=4096", report_text)

    @unittest.skipIf(importlib.util.find_spec("aioquic"), "aioquic is installed")
    def test_benchmark_cli_rejects_python_quic_without_optional_dependency(self):
        out = io.StringIO()
        err = io.StringIO()
        prompted = []

        exit_code = run_benchmark_cli(
            ["/datasets/train/shard.00000.mds", "--bulk-transport", "quic"],
            out=out,
            err=err,
            credential_prompt=lambda: prompted.append(True) or ClientCredentials("trainer", "secret"),
            tls_context_factory=lambda cafile: None,
        )

        self.assertEqual(1, exit_code)
        self.assertEqual("", out.getvalue())
        self.assertIn("Python QUIC bulk transport requires the optional aioquic dependency", err.getvalue())
        self.assertEqual([], prompted)

    def test_evaluates_ai_training_objective(self):
        result = NativeReadBenchmarkResult(
            "cxnfs-python-native",
            "/datasets/train/shard.00000.mds",
            1,
            0,
            1024,
            270_000_000,
            1.0,
        )

        evaluation = evaluate_native_read_objective(
            result,
            objective_name="ai-training-25gbe",
            baseline_mb_per_second=250.0,
        )

        self.assertTrue(evaluation.link_utilization_passed)
        self.assertTrue(evaluation.minimum_throughput_passed)
        self.assertTrue(evaluation.baseline_improvement_passed)
        self.assertTrue(evaluation.passed)

    def test_benchmark_cli_reports_objective_evaluation(self):
        control = FakeControlClient(size=2048)
        bulk = FakeBulkClient()
        out = io.StringIO()
        err = io.StringIO()
        with TemporaryDirectory() as directory:
            report = Path(directory, "benchmark-objective.txt")

            exit_code = run_benchmark_cli(
                [
                    "/datasets/train/shard.00000.mds",
                    "--iterations", "1",
                    "--warmup", "0",
                    "--block-size", "512",
                    "--objective", "ai-training-25gbe",
                    "--baseline-mb-s", "1.0",
                    "--report", str(report),
                ],
                out=out,
                err=err,
                credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                control_client_factory=lambda *args, **kwargs: control,
                bulk_client_factory=lambda *args, **kwargs: bulk,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, err.getvalue())
            self.assertIn("objective=ai-training-25gbe", out.getvalue())
            self.assertIn("objective_passed=", out.getvalue())
            report_text = report.read_text(encoding="utf-8")
            self.assertIn("objective=ai-training-25gbe", report_text)
            self.assertIn("objective_baseline=legacy-mounted-share", report_text)
            self.assertIn("objective_passed=", report_text)

    def test_benchmark_cli_can_fail_on_missed_objective(self):
        control = FakeControlClient(size=512)
        bulk = FakeBulkClient()
        out = io.StringIO()
        err = io.StringIO()

        exit_code = run_benchmark_cli(
            [
                "/datasets/train/shard.00000.mds",
                "--iterations", "1",
                "--warmup", "0",
                "--block-size", "512",
                "--objective", "ai-training-25gbe",
                "--baseline-mb-s", "1000000000000",
                "--fail-on-miss",
            ],
            out=out,
            err=err,
            credential_prompt=lambda: ClientCredentials("trainer", "secret"),
            control_client_factory=lambda *args, **kwargs: control,
            bulk_client_factory=lambda *args, **kwargs: bulk,
            tls_context_factory=lambda cafile: None,
        )

        self.assertEqual(1, exit_code)
        self.assertIn("objective_passed=false", out.getvalue())

    def test_benchmark_cli_requires_baseline_for_objective(self):
        out = io.StringIO()
        err = io.StringIO()

        exit_code = run_benchmark_cli(
            ["/datasets/train/shard.00000.mds", "--objective", "ai-training-25gbe"],
            out=out,
            err=err,
            credential_prompt=lambda: ClientCredentials("trainer", "secret"),
        )

        self.assertEqual(2, exit_code)
        self.assertIn("--objective requires --baseline-mb-s", err.getvalue())

    def test_benchmark_cli_rejects_password_argument(self):
        out = io.StringIO()
        err = io.StringIO()

        exit_code = run_benchmark_cli(
            ["/datasets/train/shard.00000.mds", "--password", "secret"],
            out=out,
            err=err,
            credential_prompt=lambda: ClientCredentials("trainer", "secret"),
        )

        self.assertEqual(2, exit_code)
        self.assertIn("unrecognized arguments", err.getvalue())


def _capture_bulk_config(bulk, captured, read_config, write_connections):
    if not isinstance(read_config, CxnfsBulkReadConfig):
        raise AssertionError("read_config is required")
    captured.append((read_config, write_connections))
    return bulk


if __name__ == "__main__":
    unittest.main()
