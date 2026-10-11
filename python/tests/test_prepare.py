from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4
import importlib.util
import io
import json
import tarfile
import unittest
from unittest.mock import patch

from cxnfs import AuthAccepted, AuthMechanism, ClientCredentials, DatasetConverter, DatasetPreparer, SendResult
from cxnfs.prepare import run as run_prepare_cli


class DatasetPreparerTest(unittest.TestCase):
    def test_plans_parquet_sources_into_mds_shards(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nested").mkdir()
            (root / "part-000.parquet").write_bytes(b"aaaa")
            (root / "nested/part-001.parquet").write_bytes(b"bbbb")
            (root / "notes.txt").write_bytes(b"skip")

            plan = DatasetPreparer(target_shard_size_bytes=5).plan(root, "train", "parquet")

            self.assertEqual("mds-shards", plan.target_format)
            self.assertEqual(2, len(plan.source_files))
            self.assertEqual(2, len(plan.shards))
            self.assertEqual("train-000000.mds", plan.shards[0].shard_name)
            self.assertEqual(8, plan.total_source_bytes)

    def test_plans_webdataset_sources(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "samples-000.tar").write_bytes(b"aaa")
            (root / "samples-001.tar.gz").write_bytes(b"bbb")
            (root / "samples-002.tgz").write_bytes(b"ccc")
            (root / "skip.parquet").write_bytes(b"skip")

            plan = DatasetPreparer(target_shard_size_bytes=1024).plan(root, "images", "webdataset")

            self.assertEqual(3, len(plan.source_files))
            self.assertEqual(1, len(plan.shards))
            self.assertTrue(all(source.relative_path.startswith("samples-") for source in plan.source_files))

    def test_writes_prepare_manifest(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part-000.parquet").write_bytes(b"aaaa")
            plan = DatasetPreparer(target_shard_size_bytes=1024).plan(root, "train", "parquet")
            output = root / "plan/cxnfs-prepare-plan.json"

            plan.write_manifest(output)

            manifest = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual("train", manifest["dataset_name"])
            self.assertEqual("parquet", manifest["source_format"])
            self.assertEqual("mds-shards", manifest["target_format"])
            self.assertEqual("train-000000.mds", manifest["shards"][0]["shard_name"])

    def test_cli_writes_plan_manifest(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part-000.parquet").write_bytes(b"aaaa")
            output = io.StringIO()
            manifest_path = root / "plan.json"

            exit_code = run_prepare_cli(
                [
                    "plan",
                    str(root),
                    "train",
                    "--source-format",
                    "parquet",
                    "--plan-manifest",
                    str(manifest_path),
                ],
                out=output,
            )

            self.assertEqual(0, exit_code)
            self.assertIn("dataset prepare plan", output.getvalue())
            self.assertIn("source_files=1", output.getvalue())
            self.assertTrue(manifest_path.exists())

    def test_rejects_missing_selected_source_format(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "notes.txt").write_bytes(b"skip")

            with self.assertRaisesRegex(ValueError, "no parquet"):
                DatasetPreparer().plan(root, "train", "parquet")

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow is required for parquet conversion")
    def test_converts_parquet_rows_into_mds_samples(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as directory:
            root = Path(directory)
            pq.write_table(
                pa.table({"id": [1, 2], "text": ["alpha", "beta"]}),
                root / "part-000.parquet",
            )
            plan = DatasetPreparer(target_shard_size_bytes=4096).plan(root, "train", "parquet")

            result = DatasetConverter().execute(plan, root / "mds")

            self.assertEqual(1, result.shard_count)
            self.assertEqual(2, result.sample_count)
            first, second = decode_mds_samples(Path(result.shards[0].path))
            self.assertEqual("part-000.parquet#row000000000000", first["path"])
            self.assertEqual({"id": 1, "text": "alpha"}, json.loads(first["data"].decode("utf-8")))
            self.assertEqual("part-000.parquet#row000000000001", second["path"])
            self.assertEqual({"id": 2, "text": "beta"}, json.loads(second["data"].decode("utf-8")))
            index = json.loads(Path(result.index_path).read_text(encoding="utf-8"))
            self.assertEqual(["data", "path"], index["shards"][0]["column_names"])

    def test_converts_webdataset_tar_groups_into_mds_samples(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            make_tar(
                root / "samples-000.tar",
                {
                    "000001.jpg": b"image-one",
                    "000001.txt": b"caption one",
                    "000002.jpg": b"image-two",
                    "000002.cls": b"7",
                },
            )
            plan = DatasetPreparer(target_shard_size_bytes=65536).plan(root, "images", "webdataset")

            result = DatasetConverter().execute(plan, root / "mds")

            self.assertEqual(1, result.shard_count)
            self.assertEqual(2, result.sample_count)
            first, second = decode_mds_samples(Path(result.shards[0].path))
            self.assertEqual("samples-000.tar#000001", first["path"])
            self.assertEqual("samples-000.tar#000002", second["path"])
            self.assertEqual(
                {"000001.jpg": b"image-one", "000001.txt": b"caption one"},
                read_tar_payload(first["data"]),
            )
            self.assertEqual(
                {"000002.cls": b"7", "000002.jpg": b"image-two"},
                read_tar_payload(second["data"]),
            )

    def test_cli_executes_webdataset_conversion(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            make_tar(
                root / "samples-000.tar",
                {
                    "000001.jpg": b"image-one",
                    "000001.txt": b"caption one",
                    "000002.jpg": b"image-two",
                },
            )
            output = io.StringIO()
            output_dir = root / "mds"

            exit_code = run_prepare_cli(
                [
                    "execute",
                    str(root),
                    "images",
                    "--source-format",
                    "webdataset",
                    "--output-dir",
                    str(output_dir),
                ],
                out=output,
            )

            self.assertEqual(0, exit_code)
            self.assertIn("converted_shards=1", output.getvalue())
            self.assertIn("converted_samples=2", output.getvalue())
            self.assertTrue((output_dir / "images-000000.mds").exists())
            self.assertTrue((output_dir / "index.json").exists())

    def test_cli_sends_safetensors_output_through_authenticated_remote_client(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_bytes(b"tensor")
            output = io.StringIO()
            prompt = RecordingCredentialPrompt()
            control_factory = RecordingControlFactory()
            bulk_factory = RecordingBulkFactory()

            tls_context = object()
            with patch("cxnfs.prepare.create_client_tls_context", return_value=tls_context) as tls_factory:
                exit_code = run_prepare_cli(
                    [
                        "send",
                        str(root),
                        "--remote-root",
                        "/outputs/run-1",
                        "--host",
                        "cxnfs.example.internal",
                        "--control-port",
                        "4509",
                        "--transport-port",
                        "4510",
                        "--initial-cords",
                        "8",
                        "--max-cords",
                        "64",
                    ],
                    out=output,
                    credential_prompt=prompt,
                    control_client_factory=control_factory,
                    bulk_client_factory=bulk_factory,
                )

        self.assertEqual(0, exit_code)
        tls_factory.assert_called_once_with(None)
        self.assertTrue(prompt.credentials.closed)
        self.assertEqual("trainer", control_factory.client.authenticated_username)
        self.assertTrue(control_factory.client.closed)
        self.assertEqual("cxnfs.example.internal", control_factory.calls[0]["host"])
        self.assertEqual(4509, control_factory.calls[0]["port"])
        self.assertEqual("cxnfs.example.internal", control_factory.calls[0]["server_hostname"])
        self.assertIs(tls_context, control_factory.calls[0]["tls_context"])
        self.assertIs(tls_context, bulk_factory.calls[0]["tls_context"])
        self.assertEqual(8, bulk_factory.calls[0]["read_config"].initial_cords)
        self.assertEqual(64, bulk_factory.calls[0]["read_config"].max_cords)
        self.assertTrue(bulk_factory.client.closed)
        self.assertEqual(str(root), bulk_factory.client.sent_output_dir)
        self.assertEqual("/outputs/run-1", bulk_factory.client.sent_remote_root)
        self.assertIn("dataset send result", output.getvalue())
        self.assertIn("files_sent=1", output.getvalue())
        self.assertIn("tensor_files_sent=1", output.getvalue())

    def test_cli_send_requires_remote_root_and_host_without_prompting(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_bytes(b"tensor")
            prompt = RecordingCredentialPrompt()

            exit_code = run_prepare_cli(["send", str(root)], err=io.StringIO(), credential_prompt=prompt)

            self.assertEqual(2, exit_code)
            self.assertEqual(0, prompt.calls)

def make_tar(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))


def decode_mds_samples(path: Path) -> list[dict[str, bytes | str]]:
    blob = path.read_bytes()
    sample_count = int.from_bytes(blob[0:4], "little")
    offsets = [
        int.from_bytes(blob[4 + (index * 4):8 + (index * 4)], "little")
        for index in range(sample_count + 1)
    ]
    samples = []
    for offset in offsets[:-1]:
        data_size = int.from_bytes(blob[offset:offset + 4], "little")
        path_size = int.from_bytes(blob[offset + 4:offset + 8], "little")
        data_start = offset + 8
        path_start = data_start + data_size
        samples.append(
            {
                "data": blob[data_start:path_start],
                "path": blob[path_start:path_start + path_size].decode("utf-8"),
            }
        )
    return samples


def read_tar_payload(payload: bytes) -> dict[str, bytes]:
    result = {}
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as archive:
        for member in archive:
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            if extracted is not None:
                result[member.name] = extracted.read()
    return result


class RecordingCredentialPrompt:
    def __init__(self):
        self.calls = 0
        self.credentials = ClientCredentials("trainer", "secret")

    def __call__(self):
        self.calls += 1
        return self.credentials


class RecordingControlFactory:
    def __init__(self):
        self.calls = []
        self.client = RecordingControlClient()

    def __call__(self, host, port, **kwargs):
        self.calls.append({"host": host, "port": port, **kwargs})
        return self.client


class RecordingControlClient:
    def __init__(self):
        self.session_id = uuid4()
        self.authenticated_username = None
        self.closed = False

    def authenticate(self, credentials):
        self.authenticated_username = credentials.username
        return AuthAccepted(self.session_id, (AuthMechanism.USERNAME_PASSWORD,))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.closed = True


class RecordingBulkFactory:
    def __init__(self):
        self.calls = []
        self.client = RecordingBulkClient()

    def __call__(self, control, host, port, **kwargs):
        self.calls.append({"control": control, "host": host, "port": port, **kwargs})
        return self.client


class RecordingBulkClient:
    def __init__(self):
        self.sent_output_dir = None
        self.sent_remote_root = None
        self.closed = False

    def send_safetensors_output(self, output_dir, remote_root=None):
        self.sent_output_dir = str(output_dir)
        self.sent_remote_root = remote_root
        return SendResult(file_count=1, tensor_file_count=1, bytes_sent=6)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.closed = True


if __name__ == "__main__":
    unittest.main()
