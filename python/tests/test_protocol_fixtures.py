from pathlib import Path
from uuid import UUID
import unittest

from cxnfs import (
    AuthMechanism,
    AuthStart,
    ControlFrame,
    ControlOperation,
    DeletePathResponse,
    ErrorResponse,
    FileType,
    MountBackend,
    MountCapability,
    MountNegotiateRequest,
    MountNegotiatedResponse,
    ResponseStatus,
    ReadContainerRangeRequest,
    ReadFileRangeResponse,
    RenamePathResponse,
    TransferAcceptedResponse,
    TransferBootstrapRequest,
    TransferCommittedResponse,
    TransferCommitRequest,
    TransferEntryAction,
    TransferEntryAcceptedResponse,
    TransferEntryCommittedResponse,
    TransferEntryCommitRequest,
    TransferEntryUpdate,
    TransferDirection,
    TransferMetadataAcceptedResponse,
    TransferMetadataUpdate,
    TransferPayloadFormat,
    decode_control_frame,
    decode_control_payload,
    encode_control_frame,
    encode_control_payload,
)
from cxnfs.bulk import (
    decode_bulk_frame,
    decode_bulk_read_request,
    decode_bulk_write_ack,
    encode_bulk_frame,
    encode_bulk_read_request,
    encode_bulk_write_ack,
)


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "protocol-fixtures"
SESSION_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
TRANSFER_ID = UUID("01234567-89ab-cdef-0123-456789abcdef")
OUTPUT_ROOT = "/outputs/run-1"
OUTPUT_FILE = "/outputs/run-1/model.safetensors"
FILE_HASH = b"\x04" * 32
TRANSFER_HASH = b"\x05" * 32


class ProtocolFixtureCompatibilityTest(unittest.TestCase):
    def test_all_control_fixture_files_decode_and_reencode(self):
        control_fixtures = sorted(FIXTURE_ROOT.glob("control-*.hex"))

        self.assertEqual(34, len(control_fixtures))
        self.assertEqual(set(ControlOperation), {
            decode_control_frame(fixture_bytes(path.name)).operation
            for path in control_fixtures
        })
        for path in control_fixtures:
            fixture = fixture_bytes(path.name)
            frame = decode_control_frame(fixture)
            payload = decode_control_payload(frame.operation, frame.payload)

            assert_control_round_trip(self, fixture, frame, payload)

    def test_all_bulk_fixture_files_decode_and_reencode(self):
        bulk_fixtures = sorted(FIXTURE_ROOT.glob("bulk-*.hex"))

        self.assertEqual(3, len(bulk_fixtures))
        for path in bulk_fixtures:
            fixture = fixture_bytes(path.name)
            if path.name == "bulk-read-request.hex":
                self.assertEqual(fixture, encode_bulk_read_request(decode_bulk_read_request(fixture)))
            elif path.name == "bulk-data-frame.hex":
                self.assertEqual(fixture, encode_bulk_frame(decode_bulk_frame(fixture)))
            elif path.name == "bulk-write-ack.hex":
                self.assertEqual(fixture, encode_bulk_write_ack(decode_bulk_write_ack(fixture)))
            else:
                self.fail(f"unknown bulk fixture: {path.name}")

    def test_decodes_and_reencodes_auth_start_control_frame_fixture(self):
        fixture = fixture_bytes("control-auth-start-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.AUTH_START, frame.operation)
        self.assertIsNone(frame.session_id)
        self.assertEqual(UUID("11111111-1111-1111-1111-111111111111"), frame.message_id)
        self.assertIsInstance(payload, AuthStart)
        self.assertEqual(AuthMechanism.USERNAME_PASSWORD, payload.mechanism)
        self.assertEqual("trainer", payload.principal)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_read_range_result_control_frame_fixture(self):
        fixture = fixture_bytes("control-read-file-range-result-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.READ_FILE_RANGE_RESULT, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertIsInstance(payload, ReadFileRangeResponse)
        self.assertEqual("/datasets/train/shard-00000.mds", payload.path)
        self.assertEqual(8192, payload.plan.file_size)
        self.assertEqual(128, payload.plan.requested_range.offset)
        self.assertEqual(1024, payload.plan.requested_range.length)
        self.assertEqual(2, len(payload.plan.chunks))
        self.assertEqual(0, payload.plan.chunks[0].chunk_index)
        self.assertEqual(3968, payload.plan.chunks[0].bytes_to_read)
        self.assertEqual(1, payload.plan.chunks[1].chunk_index)
        self.assertEqual(1152, payload.plan.chunks[1].bytes_to_read)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_entry_update_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-entry-update-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_ENTRY_UPDATE, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertIsInstance(payload, TransferEntryUpdate)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(7, payload.entry_sequence)
        self.assertEqual(TransferEntryAction.CREATE_OR_REPLACE, payload.action)
        self.assertEqual("/outputs/run-1/model.safetensors", payload.path)
        self.assertEqual("", payload.target_path)
        self.assertEqual(123456789, payload.size_hint_bytes)
        self.assertEqual(TransferPayloadFormat.SAFETENSORS, payload.payload_format)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_bootstrap_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-bootstrap-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_BOOTSTRAP, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("55555555-5555-5555-5555-555555555555"), frame.message_id)
        self.assertIsInstance(payload, TransferBootstrapRequest)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(TransferDirection.WRITE, payload.direction)
        self.assertEqual(OUTPUT_ROOT, payload.path)
        self.assertEqual(TransferPayloadFormat.SAFETENSORS, payload.payload_format)
        self.assertEqual(8, payload.initial_cords)
        self.assertEqual(64, payload.max_cords)
        self.assertEqual(268_435_456, payload.provisional_container_size_bytes)
        self.assertEqual(5_368_709_120, payload.expected_size_limit_bytes)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_accepted_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-accepted-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_ACCEPTED, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("66666666-6666-6666-6666-666666666666"), frame.message_id)
        self.assertIsInstance(payload, TransferAcceptedResponse)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(TransferDirection.WRITE, payload.direction)
        self.assertEqual(8, payload.accepted_initial_cords)
        self.assertEqual(64, payload.accepted_max_cords)
        self.assertEqual(1_048_576, payload.flow_control_window_bytes)
        self.assertTrue(payload.write_payload_admission_granted)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_entry_accepted_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-entry-accepted-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_ENTRY_ACCEPTED, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("77777777-7777-7777-7777-777777777777"), frame.message_id)
        self.assertIsInstance(payload, TransferEntryAcceptedResponse)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(7, payload.entry_sequence)
        self.assertEqual(OUTPUT_FILE, payload.path)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_metadata_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-metadata-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_METADATA, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("88888888-8888-8888-8888-888888888888"), frame.message_id)
        self.assertIsInstance(payload, TransferMetadataUpdate)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(7, payload.entry_sequence)
        self.assertEqual(2, payload.metadata_sequence)
        self.assertTrue(payload.final_metadata)
        self.assertEqual(123_456_789, payload.bytes_covered)
        self.assertEqual((b"\x06" * 32, b"\x07" * 32), payload.chunk_hashes)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_metadata_accepted_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-metadata-accepted-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_METADATA_ACCEPTED, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("99999999-9999-9999-9999-999999999999"), frame.message_id)
        self.assertIsInstance(payload, TransferMetadataAcceptedResponse)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(7, payload.entry_sequence)
        self.assertEqual(2, payload.metadata_sequence)
        self.assertEqual(2, payload.chunk_hashes_accepted)
        self.assertEqual(2, payload.staged_payloads_verified)
        self.assertTrue(payload.final_metadata)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_entry_commit_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-entry-commit-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_ENTRY_COMMIT, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("abababab-abab-abab-abab-abababababab"), frame.message_id)
        self.assertIsInstance(payload, TransferEntryCommitRequest)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(7, payload.entry_sequence)
        self.assertEqual(OUTPUT_FILE, payload.path)
        self.assertEqual(FILE_HASH, payload.content_hash)
        self.assertEqual(123_456_789, payload.bytes_written)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_entry_committed_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-entry-committed-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_ENTRY_COMMITTED, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc"), frame.message_id)
        self.assertIsInstance(payload, TransferEntryCommittedResponse)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(7, payload.entry_sequence)
        self.assertEqual(OUTPUT_FILE, payload.path)
        self.assertEqual(FILE_HASH, payload.content_hash)
        self.assertEqual(123_456_789, payload.bytes_written)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_commit_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-commit-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_COMMIT, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd"), frame.message_id)
        self.assertIsInstance(payload, TransferCommitRequest)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(OUTPUT_ROOT, payload.path)
        self.assertEqual(TRANSFER_HASH, payload.content_hash)
        self.assertEqual(123_456_789, payload.bytes_written)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_transfer_committed_control_frame_fixture(self):
        fixture = fixture_bytes("control-transfer-committed-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.TRANSFER_COMMITTED, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("dededede-dede-dede-dede-dededededede"), frame.message_id)
        self.assertIsInstance(payload, TransferCommittedResponse)
        self.assertEqual(TRANSFER_ID, payload.transfer_id)
        self.assertEqual(TRANSFER_HASH, payload.content_hash)
        self.assertEqual(123_456_789, payload.bytes_written)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_error_control_frame_fixture(self):
        fixture = fixture_bytes("control-error-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.ERROR, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("efefefef-efef-efef-efef-efefefefefef"), frame.message_id)
        self.assertIsInstance(payload, ErrorResponse)
        self.assertEqual(ResponseStatus.CONFLICT, payload.status)
        self.assertEqual("transfer summary mismatch", payload.message)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_read_container_range_control_frame_fixture(self):
        fixture = fixture_bytes("control-read-container-range-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.READ_CONTAINER_RANGE, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("22222222-2222-2222-2222-222222222222"), frame.message_id)
        self.assertIsInstance(payload, ReadContainerRangeRequest)
        self.assertEqual(b"\x03" * 32, payload.container_hash)
        self.assertEqual(2, payload.first_chunk_index)
        self.assertEqual(4, payload.chunk_count)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_rename_path_result_control_frame_fixture(self):
        fixture = fixture_bytes("control-rename-path-result-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.RENAME_PATH_RESULT, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("33333333-3333-3333-3333-333333333333"), frame.message_id)
        self.assertIsInstance(payload, RenamePathResponse)
        self.assertEqual("/datasets/train/old.mds", payload.source_path)
        self.assertEqual("/datasets/train/new.mds", payload.target_path)
        self.assertEqual("/datasets/train/new.mds", payload.metadata.path)
        self.assertEqual("new.mds", payload.metadata.name)
        self.assertEqual(FileType.REGULAR_FILE, payload.metadata.type)
        self.assertEqual(4096, payload.metadata.size)
        self.assertEqual(b"\x01" * 32, payload.metadata.content_hash)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_delete_path_result_control_frame_fixture(self):
        fixture = fixture_bytes("control-delete-path-result-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.DELETE_PATH_RESULT, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("44444444-4444-4444-4444-444444444444"), frame.message_id)
        self.assertIsInstance(payload, DeletePathResponse)
        self.assertEqual("/datasets/train/new.mds", payload.path)
        self.assertEqual(1, payload.deleted_entries)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_mount_negotiate_control_frame_fixture(self):
        fixture = fixture_bytes("control-mount-negotiate-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.MOUNT_NEGOTIATE, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("12121212-1212-1212-1212-121212121212"), frame.message_id)
        self.assertIsInstance(payload, MountNegotiateRequest)
        self.assertEqual(MountBackend.MACOS_FUSE_T, payload.backend)
        self.assertEqual((MountCapability.LIST, MountCapability.READ), payload.requested_capabilities)
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_mount_negotiated_control_frame_fixture(self):
        fixture = fixture_bytes("control-mount-negotiated-frame.hex")

        frame = decode_control_frame(fixture)
        payload = decode_control_payload(frame.operation, frame.payload)

        self.assertEqual(ControlOperation.MOUNT_NEGOTIATED, frame.operation)
        self.assertEqual(SESSION_ID, frame.session_id)
        self.assertEqual(UUID("13131313-1313-1313-1313-131313131313"), frame.message_id)
        self.assertIsInstance(payload, MountNegotiatedResponse)
        self.assertEqual(MountBackend.MACOS_FUSE_T, payload.strategy.backend)
        self.assertEqual(
            (MountCapability.LIST, MountCapability.READ, MountCapability.WRITE),
            payload.strategy.capabilities,
        )
        assert_control_round_trip(self, fixture, frame, payload)

    def test_decodes_and_reencodes_bulk_read_request_fixture(self):
        fixture = fixture_bytes("bulk-read-request.hex")

        request = decode_bulk_read_request(fixture)

        self.assertEqual(SESSION_ID, request.session_id)
        self.assertEqual(TRANSFER_ID, request.transfer_id)
        self.assertEqual("/datasets/train/shard-00000.mds", request.path)
        self.assertEqual(128, request.offset)
        self.assertEqual(1024, request.length)
        self.assertEqual(16, request.active_cords)
        self.assertEqual(fixture, encode_bulk_read_request(request))

    def test_decodes_and_reencodes_bulk_data_frame_fixture(self):
        fixture = fixture_bytes("bulk-data-frame.hex")

        frame = decode_bulk_frame(fixture)

        self.assertEqual(TRANSFER_ID, frame.transfer_id)
        self.assertEqual(3, frame.cord_index)
        self.assertEqual(7, frame.entry_sequence)
        self.assertEqual(11, frame.chunk_sequence)
        self.assertEqual(4096, frame.entry_offset)
        self.assertTrue(frame.final_frame_for_chunk)
        self.assertEqual(b"CXNFS fixture payload", frame.payload)
        self.assertEqual(fixture, encode_bulk_frame(frame))

    def test_decodes_and_reencodes_bulk_write_ack_fixture(self):
        fixture = fixture_bytes("bulk-write-ack.hex")

        ack = decode_bulk_write_ack(fixture)

        self.assertEqual(TRANSFER_ID, ack.transfer_id)
        self.assertEqual(7, ack.entry_sequence)
        self.assertEqual(11, ack.chunk_sequence)
        self.assertEqual(21, ack.payload_length)
        self.assertTrue(ack.metadata_verified)
        self.assertEqual("MEMORY", ack.storage)
        self.assertEqual(fixture, encode_bulk_write_ack(ack))


def assert_control_round_trip(testcase, fixture, frame, payload):
    payload_bytes = encode_control_payload(payload)
    testcase.assertEqual(frame.payload, payload_bytes)
    testcase.assertEqual(
        fixture,
        encode_control_frame(ControlFrame(
            frame.operation,
            payload_bytes,
            frame.message_id,
            frame.session_id,
            frame.protocol_major,
            frame.protocol_minor,
        )),
    )


def fixture_bytes(name: str) -> bytes:
    return bytes.fromhex("".join((FIXTURE_ROOT / name).read_text().split()))


if __name__ == "__main__":
    unittest.main()
