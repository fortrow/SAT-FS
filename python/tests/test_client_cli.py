import io
import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import UUID

from cxnfs.auth import AuthMechanism, ClientCredentials
from cxnfs.bulk import CxnfsBulkReadConfig
from cxnfs.client_cli import run_client_cli
from cxnfs.control import (
    AuthAccepted,
    DirectoryListing,
    DirectoryListResponse,
    FileMetadata,
    FileType,
    StatPathResponse,
)
from cxnfs.dataset import SendResult


class FakeControlClient:
    def __init__(self):
        self.authenticated = False
        self.closed = False
        self.seen_credentials_repr = ""
        self.lists = []
        self.stats = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self):
        self.closed = True

    def authenticate(self, credentials):
        self.authenticated = True
        self.seen_credentials_repr = repr(credentials)
        return AuthAccepted(
            UUID("00000000-0000-0000-0000-000000000123"),
            (AuthMechanism.USERNAME_PASSWORD,),
        )

    def list_directory(self, path):
        self.lists.append(path)
        return DirectoryListResponse(DirectoryListing(
            path,
            (
                FileMetadata(f"{path}/train", "train", FileType.DIRECTORY, 0),
                FileMetadata(f"{path}/shard.00000.mds", "shard.00000.mds", FileType.REGULAR_FILE, 4096),
            ),
        ))

    def stat_path(self, path):
        self.stats.append(path)
        return StatPathResponse(FileMetadata(path, Path(path).name, FileType.REGULAR_FILE, 4096))


class FakeBulkClient:
    def __init__(self, payload=b"abcdef"):
        self.payload = payload
        self.closed = False
        self.reads = []
        self.sends = []
        self.tensor_writes = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self):
        self.closed = True

    def read_mds_shard(self, path, offset=0, length=None):
        self.reads.append((path, offset, length))
        return self.payload[:length]

    def send_safetensors_output(self, output_dir, remote_root=None):
        self.sends.append((str(output_dir), remote_root))
        return SendResult(file_count=2, tensor_file_count=1, bytes_sent=8192)

    def write_tensor(self, path, source, nbytes):
        view = memoryview(source)
        self.tensor_writes.append((path, bytes(view[:nbytes]), nbytes))
        return SendResult(file_count=1, tensor_file_count=1, bytes_sent=nbytes)

    def read_path_evidence(self):
        return {
            "direct_destination_frame_sink_cord_fanout_verified": True,
            "direct_destination_max_observed_cords": 4,
        }

    def write_path_evidence(self):
        return {
            "metadata_after_payload_entries": 1,
            "max_observed_write_cords": 4,
        }


class CxnfsClientCliTest(unittest.TestCase):
    def test_list_prompts_hidden_credentials_and_does_not_open_bulk(self):
        control = FakeControlClient()
        credentials = ClientCredentials("trainer", "secret")
        bulk_calls = []
        out = io.StringIO()
        err = io.StringIO()

        exit_code = run_client_cli(
            [
                "--host", "cxnfs.example.internal",
                "--bulk-transport", "quic",
                "list",
                "/datasets",
            ],
            out=out,
            err=err,
            credential_prompt=lambda: credentials,
            control_client_factory=lambda *args, **kwargs: control,
            bulk_client_factory=lambda *args, **kwargs: bulk_calls.append((args, kwargs)),
            tls_context_factory=lambda cafile: None,
        )

        self.assertEqual(0, exit_code, err.getvalue())
        self.assertTrue(control.authenticated)
        self.assertTrue(control.closed)
        self.assertTrue(credentials.closed)
        self.assertEqual([], bulk_calls)
        self.assertNotIn("secret", out.getvalue())
        self.assertNotIn("secret", err.getvalue())
        self.assertNotIn("secret", control.seen_credentials_repr)
        payload = json.loads(out.getvalue())
        self.assertEqual("cxnfs.python-client-result.v1", payload["format"])
        self.assertEqual("list", payload["command"])
        self.assertEqual("quic", payload["bulk_transport"])
        self.assertEqual("/datasets", payload["listing"]["path"])
        self.assertEqual("directory", payload["listing"]["entries"][0]["type"])
        self.assertEqual("regular_file", payload["listing"]["entries"][1]["type"])

    def test_read_mds_writes_output_file_and_propagates_cord_config(self):
        control = FakeControlClient()
        bulk = FakeBulkClient(b"0123456789")
        captured = {}
        out = io.StringIO()
        err = io.StringIO()
        with TemporaryDirectory() as directory:
            output = Path(directory, "read.bin")

            exit_code = run_client_cli(
                [
                    "--host", "cxnfs.example.internal",
                    "--transport-port", "4510",
                    "--initial-cords", "8",
                    "--max-cords", "64",
                    "--write-connections", "8",
                    "read-mds",
                    "/datasets/train/shard.00000.mds",
                    "--offset", "5",
                    "--length", "6",
                    "--output", str(output),
                ],
                out=out,
                err=err,
                credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                control_client_factory=lambda *args, **kwargs: control,
                bulk_client_factory=lambda *args, **kwargs: _capture_bulk_client(bulk, captured, args, kwargs),
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, err.getvalue())
            self.assertEqual(b"012345", output.read_bytes())
            self.assertEqual([("/datasets/train/shard.00000.mds", 5, 6)], bulk.reads)
            self.assertTrue(bulk.closed)
            self.assertEqual("cxnfs.example.internal", captured["args"][1])
            self.assertEqual(4510, captured["args"][2])
            read_config = captured["kwargs"]["read_config"]
            self.assertIsInstance(read_config, CxnfsBulkReadConfig)
            self.assertEqual(8, read_config.initial_cords)
            self.assertEqual(64, read_config.max_cords)
            self.assertEqual(8, captured["kwargs"]["write_connections"])
            payload = json.loads(out.getvalue())
            self.assertEqual("read-mds", payload["command"])
            self.assertEqual(6, payload["bytes_read"])
            self.assertTrue(payload["read_path_evidence"]["direct_destination_frame_sink_cord_fanout_verified"])

    def test_send_checkpoint_reports_write_evidence(self):
        control = FakeControlClient()
        bulk = FakeBulkClient()
        out = io.StringIO()
        err = io.StringIO()
        with TemporaryDirectory() as directory:
            root = Path(directory, "checkpoint")
            root.mkdir()
            (root / "model.safetensors").write_bytes(b"checkpoint")

            exit_code = run_client_cli(
                [
                    "send-checkpoint",
                    str(root),
                    "--remote-root", "/outputs/run-1",
                ],
                out=out,
                err=err,
                credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                control_client_factory=lambda *args, **kwargs: control,
                bulk_client_factory=lambda *args, **kwargs: bulk,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, err.getvalue())
            self.assertEqual([(str(root), "/outputs/run-1")], bulk.sends)
            payload = json.loads(out.getvalue())
            self.assertEqual("send-checkpoint", payload["command"])
            self.assertEqual(2, payload["result"]["file_count"])
            self.assertEqual(1, payload["result"]["tensor_file_count"])
            self.assertEqual(8192, payload["result"]["bytes_sent"])
            self.assertEqual(1, payload["write_path_evidence"]["metadata_after_payload_entries"])

    def test_send_file_uses_direct_write_path(self):
        control = FakeControlClient()
        bulk = FakeBulkClient()
        out = io.StringIO()
        err = io.StringIO()
        with TemporaryDirectory() as directory:
            source = Path(directory, "model.safetensors")
            source.write_bytes(b"direct-checkpoint")

            exit_code = run_client_cli(
                [
                    "--initial-cords", "4",
                    "--max-cords", "64",
                    "send-file",
                    str(source),
                    "--remote-path", "/outputs/run-1/model.safetensors",
                ],
                out=out,
                err=err,
                credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                control_client_factory=lambda *args, **kwargs: control,
                bulk_client_factory=lambda *args, **kwargs: bulk,
                tls_context_factory=lambda cafile: None,
            )

            self.assertEqual(0, exit_code, err.getvalue())
            self.assertTrue(bulk.closed)
            self.assertEqual(
                [("/outputs/run-1/model.safetensors", b"direct-checkpoint", len(b"direct-checkpoint"))],
                bulk.tensor_writes,
            )
            self.assertNotIn("secret", out.getvalue())
            self.assertNotIn("secret", err.getvalue())
            payload = json.loads(out.getvalue())
            self.assertEqual("send-file", payload["command"])
            self.assertEqual(str(source), payload["source"])
            self.assertEqual("/outputs/run-1/model.safetensors", payload["remote_path"])
            self.assertEqual(1, payload["result"]["file_count"])
            self.assertEqual(1, payload["result"]["tensor_file_count"])
            self.assertEqual(len(b"direct-checkpoint"), payload["result"]["bytes_sent"])
            self.assertEqual(4, payload["write_path_evidence"]["max_observed_write_cords"])

    def test_module_help_entrypoint_lists_safe_commands(self):
        result = subprocess.run(
            [sys.executable, "-m", "cxnfs.client_cli", "--help"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertIn("read-mds", result.stdout)
        self.assertIn("send-checkpoint", result.stdout)
        self.assertIn("send-file", result.stdout)
        self.assertIn("--bulk-transport", result.stdout)
        self.assertNotIn("--password", result.stdout)
        self.assertEqual("", result.stderr)


def _capture_bulk_client(client, captured, args, kwargs):
    captured["args"] = args
    captured["kwargs"] = kwargs
    return client


if __name__ == "__main__":
    unittest.main()
