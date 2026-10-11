import io
from unittest.mock import patch
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

from cxnfs.auth import AuthMechanism, ClientCredentials
from cxnfs.control import (
    AuthAccepted,
    DirectoryListing,
    DirectoryListResponse,
    FileMetadata,
    FileType,
)
from cxnfs.health import (
    CxnfsHealthResult,
    probe_cxnfs_health,
    run_health_cli,
)


class CxnfsHealthTest(unittest.TestCase):
    def test_probe_authenticates_lists_and_reads_optional_bulk_path(self):
        control = RecordingControlClient()
        bulk = RecordingBulkClient(b"x")

        with ClientCredentials("trainer", "secret") as credentials:
            result = probe_cxnfs_health(
                control,
                credentials,
                probe_path="/datasets/train",
                bulk_client=bulk,
                bulk_probe_path="/datasets/train/shard.00000.mds",
                bulk_probe_length=1,
            )

        self.assertTrue(result.ready)
        self.assertEqual(CxnfsHealthResult(
            status="ready",
            session_id=control.session_id,
            probe_path="/datasets/train",
            probe_entries=1,
            bulk_probe="ok",
            bulk_transport="stream",
            bulk_probe_path="/datasets/train/shard.00000.mds",
            bulk_probe_bytes=1,
        ), result)
        self.assertEqual("trainer", control.authenticated_username)
        self.assertEqual(["/datasets/train"], control.list_paths)
        self.assertEqual([("/datasets/train/shard.00000.mds", 0, 1)], bulk.reads)

    def test_probe_can_skip_bulk_path(self):
        control = RecordingControlClient()

        with ClientCredentials("trainer", "secret") as credentials:
            result = probe_cxnfs_health(control, credentials, probe_path="/")

        self.assertEqual("skipped", result.bulk_probe)
        self.assertEqual(0, result.bulk_probe_bytes)
        self.assertEqual([], control.bulk_reads if hasattr(control, "bulk_reads") else [])

    def test_probe_rejects_bulk_path_without_bulk_client(self):
        with ClientCredentials("trainer", "secret") as credentials:
            with self.assertRaisesRegex(ValueError, "bulk_client is required"):
                probe_cxnfs_health(
                    RecordingControlClient(),
                    credentials,
                    bulk_probe_path="/datasets/train/shard.00000.mds",
                )

    def test_probe_rejects_short_bulk_read(self):
        with ClientCredentials("trainer", "secret") as credentials:
            with self.assertRaisesRegex(ValueError, "short bulk health probe"):
                probe_cxnfs_health(
                    RecordingControlClient(),
                    credentials,
                    bulk_client=RecordingBulkClient(b""),
                    bulk_probe_path="/datasets/train/shard.00000.mds",
                    bulk_probe_length=1,
                )

    def test_cli_uses_hidden_prompt_factories_and_writes_machine_readable_status(self):
        prompt = RecordingPrompt()
        control_factory = RecordingControlFactory()
        bulk_factory = RecordingBulkFactory(b"x")
        out = io.StringIO()
        report_dir = tempfile.TemporaryDirectory()
        self.addCleanup(report_dir.cleanup)
        report_path = Path(report_dir.name) / "health.txt"

        exit_code = run_health_cli(
            [
                "--host",
                "cxnfs.example.internal",
                "--control-port",
                "4444",
                "--transport-port",
                "4445",
                "--server-hostname",
                "trainer.cxnfs.internal",
                "--cafile",
                "/etc/cxnfs/ca.pem",
                "--path",
                "/datasets/train",
                "--read-path",
                "/datasets/train/shard.00000.mds",
                "--read-length",
                "1",
                "--report",
                str(report_path),
                "--timeout",
                "2.5",
            ],
            out=out,
            credential_prompt=prompt,
            control_client_factory=control_factory,
            bulk_client_factory=bulk_factory,
            tls_context_factory=RecordingTlsContextFactory(),
        )

        self.assertEqual(0, exit_code)
        self.assertTrue(prompt.credentials.closed)
        self.assertEqual("cxnfs.example.internal", control_factory.calls[0]["host"])
        self.assertEqual(4444, control_factory.calls[0]["port"])
        self.assertEqual(2.5, control_factory.calls[0]["timeout"])
        self.assertEqual("trainer.cxnfs.internal", control_factory.calls[0]["server_hostname"])
        self.assertEqual("/etc/cxnfs/ca.pem", control_factory.calls[0]["tls_context"].cafile)
        self.assertEqual("cxnfs.example.internal", bulk_factory.calls[0]["host"])
        self.assertEqual(4445, bulk_factory.calls[0]["port"])
        self.assertEqual(2.5, bulk_factory.calls[0]["timeout"])
        self.assertEqual("trainer.cxnfs.internal", bulk_factory.calls[0]["server_hostname"])
        self.assertEqual("/etc/cxnfs/ca.pem", bulk_factory.calls[0]["tls_context"].cafile)
        self.assertTrue(control_factory.client.closed)
        self.assertTrue(bulk_factory.client.closed)
        output = out.getvalue()
        self.assertIn("cxnfs python health", output)
        self.assertIn("host=cxnfs.example.internal", output)
        self.assertIn("control_port=4444", output)
        self.assertIn("transport_port=4445", output)
        self.assertIn("authenticated=true", output)
        self.assertIn(f"session_id={control_factory.client.session_id}", output)
        self.assertIn("probe_path=/datasets/train", output)
        self.assertIn("probe_entries=1", output)
        self.assertIn("bulk_transport=stream", output)
        self.assertIn("bulk_probe=ok", output)
        self.assertIn("bulk_probe_bytes=1", output)
        self.assertIn("status=ready", output)
        self.assertIn(f"report={report_path.resolve()}", output)
        self.assertNotIn("secret", output)
        report_text = report_path.read_text(encoding="utf-8")
        self.assertIn("cxnfs.python.health.v1", report_text)
        self.assertIn("host=cxnfs.example.internal", report_text)
        self.assertIn("control_port=4444", report_text)
        self.assertIn("transport_port=4445", report_text)
        self.assertIn("bulk_transport=stream", report_text)
        self.assertIn("bulk_probe=ok", report_text)
        self.assertIn("bulk_probe_path=/datasets/train/shard.00000.mds", report_text)
        self.assertIn("bulk_probe_bytes=1", report_text)
        self.assertIn("status=ready", report_text)
        self.assertNotIn("secret", report_text)

    def test_cli_can_select_quic_bulk_transport_for_read_probe(self):
        prompt = RecordingPrompt()
        control_factory = RecordingControlFactory()
        quic_bulk_factory = RecordingBulkFactory(b"x")
        out = io.StringIO()

        with patch("cxnfs.bulk.importlib.util.find_spec", lambda _name: object()), \
            patch("cxnfs.health.CxnfsQuicBulkClient.connect", quic_bulk_factory):
            exit_code = run_health_cli(
                [
                    "--host",
                    "cxnfs.example.internal",
                    "--control-port",
                    "4444",
                    "--transport-port",
                    "4445",
                    "--bulk-transport",
                    "quic",
                    "--read-path",
                    "/datasets/train/shard.00000.mds",
                    "--read-length",
                    "1",
                ],
                out=out,
                credential_prompt=prompt,
                control_client_factory=control_factory,
            )

        self.assertEqual(0, exit_code)
        self.assertEqual("cxnfs.example.internal", quic_bulk_factory.calls[0]["host"])
        self.assertEqual(4445, quic_bulk_factory.calls[0]["port"])
        self.assertEqual([("/datasets/train/shard.00000.mds", 0, 1)], quic_bulk_factory.client.reads)
        self.assertIn("bulk_transport=quic", out.getvalue())
        self.assertIn("bulk_probe=ok", out.getvalue())

    def test_cli_rejects_missing_quic_dependency_before_prompting(self):
        prompt = RecordingPrompt()
        err = io.StringIO()

        with patch("cxnfs.bulk.importlib.util.find_spec", lambda _name: None):
            exit_code = run_health_cli(
                [
                    "--bulk-transport",
                    "quic",
                    "--read-path",
                    "/datasets/train/shard.00000.mds",
                ],
                err=err,
                credential_prompt=prompt,
            )

        self.assertEqual(1, exit_code)
        self.assertEqual(0, prompt.calls)
        self.assertIn("Python QUIC bulk transport requires", err.getvalue())

    def test_cli_rejects_read_length_without_read_path_before_prompting(self):
        prompt = RecordingPrompt()
        err = io.StringIO()

        exit_code = run_health_cli(["--read-length", "1"], err=err, credential_prompt=prompt)

        self.assertEqual(2, exit_code)
        self.assertEqual(0, prompt.calls)
        self.assertIn("--read-length requires --read-path", err.getvalue())

    def test_cli_returns_unhealthy_exit_for_failed_authenticated_probe(self):
        prompt = RecordingPrompt()
        err = io.StringIO()

        exit_code = run_health_cli(
            [
                "--read-path",
                "/datasets/train/shard.00000.mds",
                "--read-length",
                "1",
            ],
            err=err,
            credential_prompt=prompt,
            control_client_factory=RecordingControlFactory(),
            bulk_client_factory=RecordingBulkFactory(b""),
        )

        self.assertEqual(1, exit_code)
        self.assertEqual(1, prompt.calls)
        self.assertIn("short bulk health probe", err.getvalue())


class RecordingControlClient:
    def __init__(self):
        self.session_id = UUID("00000000-0000-0000-0000-000000000101")
        self.authenticated_username = None
        self.list_paths = []
        self.closed = False

    def authenticate(self, credentials):
        self.authenticated_username = credentials.username
        return AuthAccepted(self.session_id, (AuthMechanism.USERNAME_PASSWORD,))

    def list_directory(self, path):
        self.list_paths.append(path)
        return DirectoryListResponse(DirectoryListing(path, (
            FileMetadata(
                "/datasets/train/shard.00000.mds",
                "shard.00000.mds",
                FileType.REGULAR_FILE,
                1,
            ),
        )))

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class RecordingBulkClient:
    def __init__(self, payload):
        self.payload = payload
        self.reads = []
        self.closed = False

    def read_mds_shard(self, path, offset=0, length=None):
        self.reads.append((path, offset, length))
        return self.payload

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class RecordingPrompt:
    def __init__(self):
        self.calls = 0
        self.credentials = None

    def __call__(self):
        self.calls += 1
        self.credentials = ClientCredentials("trainer", "secret")
        return self.credentials


class RecordingControlFactory:
    def __init__(self):
        self.client = RecordingControlClient()
        self.calls = []

    def __call__(self, host, port, **kwargs):
        self.calls.append({"host": host, "port": port, **kwargs})
        return self.client


class RecordingBulkFactory:
    def __init__(self, payload):
        self.client = RecordingBulkClient(payload)
        self.calls = []

    def __call__(self, control_client, host, port, **kwargs):
        self.calls.append({"host": host, "port": port, **kwargs})
        self.control_client = control_client
        return self.client


class RecordingTlsContext:
    def __init__(self, cafile):
        self.cafile = cafile


class RecordingTlsContextFactory:
    def __call__(self, cafile):
        return RecordingTlsContext(cafile)


if __name__ == "__main__":
    unittest.main()
