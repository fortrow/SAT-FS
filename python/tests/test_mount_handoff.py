import contextlib
import io
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
    MountBackend,
    MountCapability,
    MountNegotiatedResponse,
    MountStrategy,
)
from cxnfs.mount_handoff import (
    HANDOFF_FORMAT,
    LAUNCH_PLAN_FORMAT,
    LIST_REPORT_FORMAT,
    connect_and_list_mount_handoff,
    mount_handoff_report_text,
    mount_launch_plan_text,
    parse_mount_handoff_text,
    run_mount_handoff_cli,
)


HANDOFF_TEXT = """cxnfs.mount.handoff.v1
host=cxnfs.example.internal
control_port=4509
transport_port=4510
requested_backend=macos_fuse_t
requested_capabilities=list,read
backend=macos_fuse_t
capabilities=list,read,write
adapter_kind=fuse
adapter_binary=cxnfs-fuse-t
adapter_argument_style=handoff-file
remote_root=/datasets/train
mount_point=/Volumes/CXNFS
initial_cords=8
max_cords=64
provisional_container_size_bytes=67108864
expected_size_limit_bytes=0
connect_timeout_ms=15000
read_timeout_ms=120000
truststore_path=
truststore_password_inline=false
auth_password_inline=false
"""


class MountHandoffTest(unittest.TestCase):
    def test_parses_java_handoff_report_and_normalizes_values(self):
        handoff = parse_mount_handoff_text(HANDOFF_TEXT)

        self.assertEqual("cxnfs.example.internal", handoff.host)
        self.assertEqual(4509, handoff.control_port)
        self.assertEqual(4510, handoff.transport_port)
        self.assertEqual(MountBackend.MACOS_FUSE_T, handoff.requested_backend)
        self.assertEqual(
            (MountCapability.LIST, MountCapability.READ),
            handoff.requested_capabilities,
        )
        self.assertEqual(MountBackend.MACOS_FUSE_T, handoff.strategy.backend)
        self.assertEqual(
            (MountCapability.LIST, MountCapability.READ, MountCapability.WRITE),
            handoff.strategy.capabilities,
        )
        self.assertEqual("/datasets/train", handoff.remote_root)
        self.assertEqual("/Volumes/CXNFS", handoff.mount_point)
        self.assertEqual(8, handoff.initial_cords)
        self.assertEqual(64, handoff.max_cords)
        self.assertIsNone(handoff.truststore_path)
        self.assertEqual("fuse", handoff.adapter_kind)
        self.assertEqual("cxnfs-fuse-t", handoff.adapter_binary)

    def test_rejects_inline_secret_fields(self):
        with self.assertRaisesRegex(ValueError, "inline secret field"):
            parse_mount_handoff_text(HANDOFF_TEXT.replace(
                "auth_password_inline=false",
                "auth_password_inline=true",
            ))

        with self.assertRaisesRegex(ValueError, "inline secret field"):
            parse_mount_handoff_text(HANDOFF_TEXT.replace(
                "truststore_password_inline=false",
                "truststore_password_inline=true",
            ))

    def test_launch_plan_report_does_not_prompt_or_open_network(self):
        with tempfile.TemporaryDirectory() as temp:
            handoff_path = Path(temp) / "cxnfs-mount.handoff"
            launch_path = Path(temp) / "cxnfs-mount.launch"
            handoff_path.write_text(HANDOFF_TEXT, encoding="utf-8")
            prompt = ExplodingPrompt()
            network = ExplodingNetwork()
            out = io.StringIO()

            exit_code = run_mount_handoff_cli(
                [
                    "launch-plan",
                    str(handoff_path),
                    "--adapter-binary",
                    "/opt/cxnfs/bin/cxnfs-fuse-t",
                    "--report",
                    str(launch_path),
                ],
                out=out,
                credential_prompt=prompt,
                control_client_factory=network,
            )

            self.assertEqual(0, exit_code)
            self.assertEqual(0, prompt.calls)
            self.assertEqual(0, network.calls)
            output = out.getvalue()
            self.assertEqual(output, launch_path.read_text(encoding="utf-8"))
            self.assertIn(LAUNCH_PLAN_FORMAT, output)
            self.assertIn(f"handoff_report={handoff_path.resolve()}", output)
            self.assertIn("adapter_binary=/opt/cxnfs/bin/cxnfs-fuse-t", output)
            self.assertIn("adapter_kind=fuse", output)
            self.assertIn("argv_count=3", output)
            self.assertIn("argv_1=--handoff", output)
            self.assertIn("auth_password_inline=false", output)
            self.assertIn("truststore_password_inline=false", output)
            self.assertNotIn("secret", output)

    def test_list_uses_handoff_endpoint_and_hidden_prompt(self):
        with tempfile.TemporaryDirectory() as temp:
            handoff_path = Path(temp) / "cxnfs-mount.handoff"
            report_path = Path(temp) / "cxnfs-mount-list.txt"
            handoff_path.write_text(HANDOFF_TEXT, encoding="utf-8")
            prompt = RecordingPrompt()
            factory = RecordingControlFactory()
            tls_factory = RecordingTlsContextFactory()
            out = io.StringIO()

            exit_code = run_mount_handoff_cli(
                [
                    "list",
                    str(handoff_path),
                    "--path",
                    "/datasets/train",
                    "--report",
                    str(report_path),
                ],
                out=out,
                credential_prompt=prompt,
                control_client_factory=factory,
                tls_context_factory=tls_factory,
            )

            self.assertEqual(0, exit_code)
            self.assertEqual(1, prompt.calls)
            self.assertTrue(prompt.credentials.closed)
            self.assertEqual("trainer", factory.client.authenticated_username)
            self.assertEqual("cxnfs.example.internal", factory.calls[0]["host"])
            self.assertEqual(4509, factory.calls[0]["port"])
            self.assertEqual(15.0, factory.calls[0]["timeout"])
            self.assertEqual("cxnfs.example.internal", factory.calls[0]["server_hostname"])
            self.assertIsNone(factory.calls[0]["tls_context"].cafile)
            self.assertEqual([None], tls_factory.cafiles)
            self.assertEqual(["/datasets/train"], factory.client.list_paths)
            self.assertTrue(factory.client.closed)
            output = out.getvalue()
            self.assertEqual(output, report_path.read_text(encoding="utf-8"))
            self.assertIn(LIST_REPORT_FORMAT, output)
            self.assertIn(f"handoff_report={handoff_path.resolve()}", output)
            self.assertIn("backend=macos_fuse_t", output)
            self.assertIn("remote_root=/datasets/train", output)
            self.assertIn("mount_point=/Volumes/CXNFS", output)
            self.assertIn("path=/datasets/train", output)
            self.assertIn("regular_file\t42\t-\t/datasets/train/shard.mds", output)
            self.assertNotIn("secret", output)

    def test_negotiate_authenticates_and_writes_secret_free_handoff_report(self):
        with tempfile.TemporaryDirectory() as temp:
            report_path = Path(temp) / "cxnfs-mount.handoff"
            prompt = RecordingPrompt()
            factory = RecordingControlFactory()
            tls_factory = RecordingTlsContextFactory()
            out = io.StringIO()

            exit_code = run_mount_handoff_cli(
                [
                    "negotiate",
                    "macos-fuse-t",
                    "--capabilities",
                    "list,read",
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
                    "--remote-root",
                    "/datasets/train",
                    "--mount-point",
                    "/Volumes/CXNFS",
                    "--handoff-report",
                    str(report_path),
                    "--initial-cords",
                    "8",
                    "--max-cords",
                    "64",
                    "--connect-timeout-ms",
                    "15000",
                    "--read-timeout-ms",
                    "120000",
                ],
                out=out,
                credential_prompt=prompt,
                control_client_factory=factory,
                tls_context_factory=tls_factory,
            )

            self.assertEqual(0, exit_code)
            self.assertEqual(1, prompt.calls)
            self.assertTrue(prompt.credentials.closed)
            self.assertEqual("trainer", factory.client.authenticated_username)
            self.assertEqual("cxnfs.example.internal", factory.calls[0]["host"])
            self.assertEqual(4444, factory.calls[0]["port"])
            self.assertEqual(15.0, factory.calls[0]["timeout"])
            self.assertEqual("trainer.cxnfs.internal", factory.calls[0]["server_hostname"])
            self.assertEqual("/etc/cxnfs/ca.pem", factory.calls[0]["tls_context"].cafile)
            self.assertEqual(["/etc/cxnfs/ca.pem"], tls_factory.cafiles)
            self.assertEqual(MountBackend.MACOS_FUSE_T, factory.client.mount_backend)
            self.assertEqual(
                (MountCapability.LIST, MountCapability.READ),
                factory.client.mount_capabilities,
            )
            self.assertTrue(factory.client.closed)
            output = out.getvalue()
            self.assertEqual(output, report_path.read_text(encoding="utf-8"))
            self.assertIn(HANDOFF_FORMAT, output)
            self.assertIn("host=cxnfs.example.internal", output)
            self.assertIn("control_port=4444", output)
            self.assertIn("transport_port=4445", output)
            self.assertIn("requested_backend=macos_fuse_t", output)
            self.assertIn("requested_capabilities=list,read", output)
            self.assertIn("backend=macos_fuse_t", output)
            self.assertIn("capabilities=list,read,write", output)
            self.assertIn("adapter_kind=fuse", output)
            self.assertIn("adapter_binary=cxnfs-fuse-t", output)
            self.assertIn("remote_root=/datasets/train", output)
            self.assertIn("mount_point=/Volumes/CXNFS", output)
            self.assertIn("initial_cords=8", output)
            self.assertIn("max_cords=64", output)
            self.assertIn("truststore_path=/etc/cxnfs/ca.pem", output)
            self.assertIn("truststore_password_inline=false", output)
            self.assertIn("auth_password_inline=false", output)
            self.assertNotIn("secret", output)

    def test_negotiate_requires_mount_point_before_prompting(self):
        prompt = ExplodingPrompt()
        err = io.StringIO()

        with contextlib.redirect_stderr(err):
            exit_code = run_mount_handoff_cli(
                ["negotiate", "linux-fuse"],
                err=err,
                credential_prompt=prompt,
                control_client_factory=ExplodingNetwork(),
            )

        self.assertEqual(2, exit_code)
        self.assertEqual(0, prompt.calls)
        self.assertIn("--mount-point", err.getvalue())

    def test_connect_and_list_rejects_missing_list_capability(self):
        handoff = parse_mount_handoff_text(HANDOFF_TEXT.replace(
            "capabilities=list,read,write",
            "capabilities=read,write",
        ))
        with ClientCredentials("trainer", "secret") as credentials:
            with self.assertRaisesRegex(ValueError, "list capability"):
                connect_and_list_mount_handoff(
                    credentials,
                    handoff,
                    control_client_factory=ExplodingNetwork(),
                    tls_context_factory=RecordingTlsContextFactory(),
                )

    def test_launch_plan_text_matches_report_contract(self):
        handoff = parse_mount_handoff_text(HANDOFF_TEXT)
        text = mount_launch_plan_text(
            handoff,
            "/tmp/cxnfs-mount.handoff",
            adapter_binary="/opt/cxnfs/bin/cxnfs-fuse-t",
        )

        self.assertTrue(text.startswith(f"{LAUNCH_PLAN_FORMAT}\n"))
        self.assertIn("backend=macos_fuse_t", text)
        self.assertIn("max_cords=64", text)
        self.assertIn("argv_0=/opt/cxnfs/bin/cxnfs-fuse-t", text)
        self.assertIn("argv_1=--handoff", text)
        self.assertIn("auth_password_inline=false", text)
        self.assertNotIn("secret", text)

    def test_handoff_report_text_matches_java_key_value_contract(self):
        handoff = parse_mount_handoff_text(HANDOFF_TEXT)
        text = mount_handoff_report_text(handoff)

        self.assertTrue(text.startswith(f"{HANDOFF_FORMAT}\n"))
        self.assertIn("requested_backend=macos_fuse_t", text)
        self.assertIn("requested_capabilities=list,read", text)
        self.assertIn("adapter_argument_style=handoff-file", text)
        self.assertIn("auth_password_inline=false", text)

    def test_rejects_unknown_or_empty_handoff_format(self):
        with self.assertRaisesRegex(ValueError, "empty"):
            parse_mount_handoff_text("")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            parse_mount_handoff_text("cxnfs.mount.handoff.v0\n")

    def test_handoff_format_constant_is_stable(self):
        self.assertEqual("cxnfs.mount.handoff.v1", HANDOFF_FORMAT)

    def test_package_exports_are_available_lazily(self):
        import cxnfs

        self.assertIs(cxnfs.MountAdapterHandoff, type(parse_mount_handoff_text(HANDOFF_TEXT)))
        self.assertEqual("cxnfs.mount.launch-plan.v1", cxnfs.LAUNCH_PLAN_FORMAT)


class RecordingControlClient:
    def __init__(self):
        self.session_id = UUID("00000000-0000-0000-0000-000000000101")
        self.authenticated_username = None
        self.list_paths = []
        self.mount_backend = None
        self.mount_capabilities = None
        self.closed = False

    def authenticate(self, credentials):
        self.authenticated_username = credentials.username
        return AuthAccepted(self.session_id, (AuthMechanism.USERNAME_PASSWORD,))

    def list_directory(self, path):
        self.list_paths.append(path)
        return DirectoryListResponse(DirectoryListing(path, (
            FileMetadata(
                "/datasets/train/shard.mds",
                "shard.mds",
                FileType.REGULAR_FILE,
                42,
            ),
        )))

    def mount_negotiate(self, backend, requested_capabilities):
        self.mount_backend = backend
        self.mount_capabilities = requested_capabilities
        if backend == MountBackend.MACOS_FUSE_T:
            return MountNegotiatedResponse(MountStrategy(
                MountBackend.MACOS_FUSE_T,
                (MountCapability.LIST, MountCapability.READ, MountCapability.WRITE),
            ))
        return MountNegotiatedResponse(MountStrategy(backend, requested_capabilities))

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class RecordingControlFactory:
    def __init__(self):
        self.client = RecordingControlClient()
        self.calls = []

    def __call__(self, host, port, **kwargs):
        self.calls.append({"host": host, "port": port, **kwargs})
        return self.client


class RecordingPrompt:
    def __init__(self):
        self.calls = 0
        self.credentials = None

    def __call__(self):
        self.calls += 1
        self.credentials = ClientCredentials("trainer", "secret")
        return self.credentials


class RecordingTlsContext:
    def __init__(self, cafile):
        self.cafile = cafile


class RecordingTlsContextFactory:
    def __init__(self):
        self.cafiles = []

    def __call__(self, cafile):
        self.cafiles.append(cafile)
        return RecordingTlsContext(cafile)


class ExplodingPrompt:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        raise AssertionError("credential prompt must not run")


class ExplodingNetwork:
    def __init__(self):
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("network must not be opened")


if __name__ == "__main__":
    unittest.main()
