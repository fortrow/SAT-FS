from uuid import UUID
import unittest

from cxnfs import (
    AuthAccepted,
    AuthChallenge,
    AuthMechanism,
    AuthProof,
    AuthStart,
    ByteRange,
    ChunkRef,
    ClientCredentials,
    ControlFrame,
    ControlOperation,
    CxnfsControlClient,
    CxnfsControlError,
    DeletePathRequest,
    DeletePathResponse,
    DirectoryListing,
    DirectoryListRequest,
    DirectoryListResponse,
    ErrorResponse,
    FileManifest,
    FileMetadata,
    FileType,
    ManifestRequest,
    ManifestResponse,
    MountBackend,
    MountCapability,
    MountNegotiateRequest,
    MountNegotiatedResponse,
    MountStrategy,
    PlannedChunkRead,
    PROOF_ALGORITHM,
    PutFileRequest,
    PutFileResponse,
    ReconstructionPlan,
    ReconstructionPlanRequest,
    ReconstructionPlanResponse,
    ReconstructionTerm,
    ReadContainerRangeRequest,
    ReadFileRangeRequest,
    ReadFileRangeResponse,
    ReadPlan,
    RenamePathRequest,
    RenamePathResponse,
    ResponseStatus,
    StatPathRequest,
    StatPathResponse,
    TransferAcceptedResponse,
    TransferBootstrapRequest,
    TransferCommitRequest,
    TransferCommittedResponse,
    TransferDirection,
    TransferEntryAcceptedResponse,
    TransferEntryAction,
    TransferEntryCommitRequest,
    TransferEntryCommittedResponse,
    TransferEntryUpdate,
    TransferMetadataAcceptedResponse,
    TransferMetadataUpdate,
    TransferPayloadFormat,
    decode_control_frame,
    decode_control_payload,
    encode_control_frame,
    encode_control_payload,
    verify_password_proof,
)


class ScriptedTransport:
    def __init__(self):
        self.requests = []
        self.closed = False
        self.challenge = AuthChallenge(
            UUID("00000000-0000-0000-0000-000000000011"),
            AuthMechanism.USERNAME_PASSWORD,
            "trainer",
            b"control-nonce",
            PROOF_ALGORITHM,
            2,
        )
        self.session_id = UUID("00000000-0000-0000-0000-000000000022")
        self.read_plan = ReadPlan(
            b"\x01" * 32,
            4096,
            ByteRange(128, 512),
            (
                PlannedChunkRead(0, b"\x02" * 32, 0, 1024, 128, 512),
            ),
        )
        self.chunk = ChunkRef(b"\x02" * 32, 1024)
        self.manifest = FileManifest(
            "/datasets/train/shard.00000.mds",
            self.read_plan.file_hash,
            4096,
            (self.chunk,),
        )
        self.listing = DirectoryListing(
            "/datasets/train",
            (
                FileMetadata(
                    "/datasets/train/shard.00000.mds",
                    "shard.00000.mds",
                    FileType.REGULAR_FILE,
                    4096,
                    self.read_plan.file_hash,
                    1,
                    2,
                ),
            ),
        )
        self.reconstruction = ReconstructionPlan(
            self.read_plan.file_hash,
            4096,
            (ReconstructionTerm(b"\x03" * 32, 0, 1),),
        )
        self.transfer_id = UUID("00000000-0000-0000-0000-000000000033")
        self.transfer_response_override = None
        self.entry_accepted_response_override = None
        self.metadata_accepted_response_override = None
        self.entry_committed_response_override = None
        self.transfer_committed_response_override = None

    def exchange(self, request_frame):
        frame = decode_control_frame(request_frame)
        payload = decode_control_payload(frame.operation, frame.payload)
        self.requests.append(payload)
        if isinstance(payload, AuthStart):
            return response(self.challenge)
        if isinstance(payload, AuthProof):
            if not verify_password_proof("secret", self.challenge.nonce, self.challenge.iterations, payload.proof):
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "authentication failed"))
            return response(AuthAccepted(self.session_id, (AuthMechanism.USERNAME_PASSWORD,)), self.session_id)
        if isinstance(payload, DirectoryListRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(DirectoryListResponse(self.listing), self.session_id)
        if isinstance(payload, StatPathRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(StatPathResponse(self.listing.entries[0]), self.session_id)
        if isinstance(payload, ManifestRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(ManifestResponse(self.manifest), self.session_id)
        if isinstance(payload, ReconstructionPlanRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(ReconstructionPlanResponse(self.reconstruction), self.session_id)
        if isinstance(payload, PutFileRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(PutFileResponse(FileManifest(payload.path, payload.file_hash, payload.size, payload.chunks), (b"\x04" * 32,)), self.session_id)
        if isinstance(payload, ReadFileRangeRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(ReadFileRangeResponse(payload.path, self.read_plan), self.session_id)
        if isinstance(payload, RenamePathRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(
                RenamePathResponse(
                    payload.source_path,
                    payload.target_path,
                    FileMetadata(
                        payload.target_path,
                        payload.target_path.rsplit("/", 1)[-1],
                        FileType.REGULAR_FILE,
                        4096,
                        self.read_plan.file_hash,
                        1,
                        2,
                    ),
                ),
                self.session_id,
            )
        if isinstance(payload, DeletePathRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(DeletePathResponse(payload.path, 1), self.session_id)
        if isinstance(payload, MountNegotiateRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            return response(
                MountNegotiatedResponse(
                    MountStrategy(payload.backend, (MountCapability.LIST, MountCapability.READ, MountCapability.WRITE))
                ),
                self.session_id,
            )
        if isinstance(payload, TransferBootstrapRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            if self.transfer_response_override is not None:
                return response(self.transfer_response_override, self.session_id)
            return response(
                TransferAcceptedResponse(
                    payload.transfer_id,
                    payload.direction,
                    payload.initial_cords,
                    payload.max_cords,
                    128 * 1024 * 1024,
                    payload.direction == TransferDirection.WRITE,
                ),
                self.session_id,
            )
        if isinstance(payload, TransferEntryUpdate):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            if self.entry_accepted_response_override is not None:
                return response(self.entry_accepted_response_override, self.session_id)
            return response(
                TransferEntryAcceptedResponse(payload.transfer_id, payload.entry_sequence, payload.path),
                self.session_id,
            )
        if isinstance(payload, TransferMetadataUpdate):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            if self.metadata_accepted_response_override is not None:
                return response(self.metadata_accepted_response_override, self.session_id)
            return response(
                TransferMetadataAcceptedResponse(
                    payload.transfer_id,
                    payload.entry_sequence,
                    payload.metadata_sequence,
                    len(payload.chunk_hashes),
                    1 if payload.chunk_hashes else 0,
                    payload.final_metadata,
                ),
                self.session_id,
            )
        if isinstance(payload, TransferEntryCommitRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            if self.entry_committed_response_override is not None:
                return response(self.entry_committed_response_override, self.session_id)
            return response(
                TransferEntryCommittedResponse(
                    payload.transfer_id,
                    payload.entry_sequence,
                    payload.path,
                    payload.content_hash,
                    payload.bytes_written,
                ),
                self.session_id,
            )
        if isinstance(payload, TransferCommitRequest):
            if frame.session_id != self.session_id:
                return response(ErrorResponse(ResponseStatus.UNAUTHENTICATED, "session required"))
            if self.transfer_committed_response_override is not None:
                return response(self.transfer_committed_response_override, self.session_id)
            return response(
                TransferCommittedResponse(payload.transfer_id, payload.content_hash, payload.bytes_written),
                self.session_id,
            )
        raise AssertionError(f"unexpected payload: {payload!r}")

    def close(self):
        self.closed = True


class CxnfsControlTest(unittest.TestCase):
    def test_auth_payloads_round_trip_through_java_compatible_codec(self):
        challenge_id = UUID("00000000-0000-0000-0000-000000000001")
        payloads = [
            AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer"),
            AuthChallenge(challenge_id, AuthMechanism.USERNAME_PASSWORD, "trainer", b"abc", PROOF_ALGORITHM, 2),
            AuthProof(challenge_id, "trainer", b"proof"),
            AuthAccepted(challenge_id, (AuthMechanism.USERNAME_PASSWORD,)),
            DirectoryListRequest("/datasets/train"),
            DirectoryListResponse(
                DirectoryListing(
                    "/datasets/train",
                    (
                        FileMetadata(
                            "/datasets/train/shard.00000.mds",
                            "shard.00000.mds",
                            FileType.REGULAR_FILE,
                            4096,
                            b"\x01" * 32,
                            1,
                            2,
                        ),
                    ),
                )
            ),
            StatPathRequest("/datasets/train/shard.00000.mds"),
            StatPathResponse(
                FileMetadata(
                    "/datasets/train/shard.00000.mds",
                    "shard.00000.mds",
                    FileType.REGULAR_FILE,
                    4096,
                    b"\x01" * 32,
                    1,
                    2,
                )
            ),
            ManifestRequest(b"\x01" * 32),
            ManifestResponse(FileManifest("/datasets/train/shard.00000.mds", b"\x01" * 32, 1024, (ChunkRef(b"\x02" * 32, 1024),))),
            ReconstructionPlanRequest(b"\x01" * 32),
            ReconstructionPlanResponse(ReconstructionPlan(b"\x01" * 32, 1024, (ReconstructionTerm(b"\x03" * 32, 0, 1),))),
            PutFileRequest("/outputs/model.safetensors", 1024, b"\x01" * 32, (ChunkRef(b"\x02" * 32, 1024),)),
            PutFileResponse(
                FileManifest("/outputs/model.safetensors", b"\x01" * 32, 1024, (ChunkRef(b"\x02" * 32, 1024),)),
                (b"\x02" * 32,),
            ),
            ReadFileRangeRequest("/datasets/train/shard.00000.mds", 128, 512),
            ReadFileRangeResponse(
                "/datasets/train/shard.00000.mds",
                ReadPlan(
                    b"\x01" * 32,
                    4096,
                    ByteRange(128, 512),
                    (PlannedChunkRead(0, b"\x02" * 32, 0, 1024, 128, 512),),
                ),
            ),
            ReadContainerRangeRequest(b"\x03" * 32, 2, 4),
            RenamePathRequest("/datasets/train/old.mds", "/datasets/train/new.mds"),
            RenamePathResponse(
                "/datasets/train/old.mds",
                "/datasets/train/new.mds",
                FileMetadata(
                    "/datasets/train/new.mds",
                    "new.mds",
                    FileType.REGULAR_FILE,
                    4096,
                    b"\x01" * 32,
                    1,
                    2,
                ),
            ),
            DeletePathRequest("/datasets/train/new.mds", False),
            DeletePathResponse("/datasets/train/new.mds", 1),
            MountNegotiateRequest(
                MountBackend.MACOS_FUSE_T,
                (MountCapability.READ, MountCapability.LIST),
            ),
            MountNegotiatedResponse(
                MountStrategy(
                    MountBackend.MACOS_FUSE_T,
                    (MountCapability.WRITE, MountCapability.READ, MountCapability.LIST),
                )
            ),
            TransferBootstrapRequest(
                UUID("00000000-0000-0000-0000-000000000010"),
                TransferDirection.READ,
                "/datasets/train/shard.00000.mds",
                TransferPayloadFormat.MDS_SHARDS,
                8,
                64,
                64 * 1024 * 1024,
                0,
            ),
            TransferAcceptedResponse(
                UUID("00000000-0000-0000-0000-000000000010"),
                TransferDirection.READ,
                8,
                64,
                128 * 1024 * 1024,
                False,
            ),
            TransferEntryUpdate(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/run-1/model.safetensors",
                "",
                1024,
                TransferPayloadFormat.SAFETENSORS,
            ),
            TransferEntryAcceptedResponse(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                "/outputs/run-1/model.safetensors",
            ),
            TransferMetadataUpdate(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                0,
                True,
                1024,
                (b"\x02" * 32,),
            ),
            TransferMetadataAcceptedResponse(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                0,
                1,
                1,
                True,
            ),
            TransferEntryCommitRequest(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                "/outputs/run-1/model.safetensors",
                b"\x01" * 32,
                1024,
            ),
            TransferEntryCommittedResponse(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                "/outputs/run-1/model.safetensors",
                b"\x01" * 32,
                1024,
            ),
            TransferCommitRequest(
                UUID("00000000-0000-0000-0000-000000000010"),
                "/outputs/run-1",
                b"\x01" * 32,
                1024,
            ),
            TransferCommittedResponse(
                UUID("00000000-0000-0000-0000-000000000010"),
                b"\x01" * 32,
                1024,
            ),
            ErrorResponse(ResponseStatus.UNAUTHENTICATED, "nope"),
        ]

        for payload in payloads:
            operation = ControlOperation[operation_name(payload)]
            decoded = decode_control_payload(operation, encode_control_payload(payload))

            self.assertEqual(payload, decoded)

    def test_namespace_control_payloads_reject_traversal_segments(self):
        with self.assertRaisesRegex(ValueError, "parent"):
            DirectoryListRequest("/datasets/../train")
        with self.assertRaisesRegex(ValueError, "current"):
            StatPathRequest("/datasets/./train")
        with self.assertRaisesRegex(ValueError, "parent"):
            PutFileRequest("/outputs/../model.safetensors", 1, b"\x01" * 32, (ChunkRef(b"\x02" * 32, 1),))
        with self.assertRaisesRegex(ValueError, "control"):
            ReadFileRangeRequest("/datasets/bad\nname.mds", 0, 1)
        with self.assertRaisesRegex(ValueError, "parent"):
            RenamePathRequest("/datasets/train/old.mds", "/datasets/../new.mds")
        with self.assertRaisesRegex(ValueError, "parent"):
            DeletePathRequest("/datasets/../train", True)
        with self.assertRaisesRegex(ValueError, "parent"):
            TransferBootstrapRequest(
                UUID("00000000-0000-0000-0000-000000000010"),
                TransferDirection.READ,
                "/datasets/../shard.mds",
                TransferPayloadFormat.MDS_SHARDS,
                1,
                64,
                64 * 1024 * 1024,
                0,
            )
        with self.assertRaisesRegex(ValueError, "parent"):
            TransferEntryUpdate(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/../model.safetensors",
            )
        with self.assertRaisesRegex(ValueError, "parent"):
            TransferEntryCommitRequest(
                UUID("00000000-0000-0000-0000-000000000010"),
                0,
                "/outputs/../model.safetensors",
                b"\x01" * 32,
                1,
            )
        with self.assertRaisesRegex(ValueError, "parent"):
            TransferCommitRequest(
                UUID("00000000-0000-0000-0000-000000000010"),
                "/outputs/../run",
                b"\x01" * 32,
                1,
            )

    def test_control_frame_round_trips_payload_bytes(self):
        payload = encode_control_payload(AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer"))
        frame = ControlFrame(
            ControlOperation.AUTH_START,
            payload,
            UUID("00000000-0000-0000-0000-000000000001"),
        )

        decoded = decode_control_frame(encode_control_frame(frame))

        self.assertEqual(frame, decoded)

    def test_control_client_authenticates_with_scripted_server(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            accepted = client.authenticate(credentials)

        self.assertEqual(transport.session_id, accepted.session_id)
        self.assertEqual(transport.session_id, client.session_id)
        self.assertIsInstance(transport.requests[0], AuthStart)
        self.assertIsInstance(transport.requests[1], AuthProof)
        self.assertEqual("trainer", transport.requests[0].principal)

    def test_control_client_requests_read_file_range_plan_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        response_payload = client.read_file_range("/datasets/train/shard.00000.mds", 128, 512)

        self.assertEqual("/datasets/train/shard.00000.mds", response_payload.path)
        self.assertEqual(transport.read_plan, response_payload.plan)
        self.assertIsInstance(transport.requests[2], ReadFileRangeRequest)
        self.assertEqual(128, transport.requests[2].offset)
        self.assertEqual(512, transport.requests[2].length)

    def test_control_client_rejects_mismatched_read_file_range_plan(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "range"):
            client.read_file_range("/datasets/train/shard.00000.mds", 0, 512)

    def test_control_client_lists_stats_manifests_and_reconstruction_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        listing = client.list_directory("/datasets/train")
        stat = client.stat_path("/datasets/train/shard.00000.mds")
        manifest = client.manifest(transport.read_plan.file_hash)
        reconstruction = client.reconstruction_plan(transport.read_plan.file_hash)

        self.assertEqual(transport.listing, listing.listing)
        self.assertEqual(transport.listing.entries[0], stat.metadata)
        self.assertEqual(transport.manifest, manifest.manifest)
        self.assertEqual(transport.reconstruction, reconstruction.plan)
        self.assertIsInstance(transport.requests[2], DirectoryListRequest)
        self.assertIsInstance(transport.requests[3], StatPathRequest)
        self.assertIsInstance(transport.requests[4], ManifestRequest)
        self.assertIsInstance(transport.requests[5], ReconstructionPlanRequest)

    def test_control_client_prepares_put_file_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        response_payload = client.prepare_put_file(
            "/outputs/model.safetensors",
            1024,
            b"\x01" * 32,
            (ChunkRef(b"\x02" * 32, 1024),),
        )

        self.assertEqual("/outputs/model.safetensors", response_payload.manifest.path)
        self.assertEqual((b"\x04" * 32,), response_payload.missing_chunks)
        self.assertIsInstance(transport.requests[2], PutFileRequest)

    def test_control_client_renames_and_deletes_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        rename = client.rename_path("/datasets/train/old.mds", "/datasets/train/new.mds")
        delete = client.delete_path("/datasets/train/new.mds")

        self.assertEqual("/datasets/train/new.mds", rename.target_path)
        self.assertEqual(transport.read_plan.file_hash, rename.metadata.content_hash)
        self.assertEqual(1, delete.deleted_entries)
        self.assertIsInstance(transport.requests[2], RenamePathRequest)
        self.assertIsInstance(transport.requests[3], DeletePathRequest)

    def test_control_client_negotiates_mount_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        negotiated = client.mount_negotiate(
            MountBackend.MACOS_FUSE_T,
            (MountCapability.READ, MountCapability.LIST),
        )

        self.assertEqual(MountBackend.MACOS_FUSE_T, negotiated.strategy.backend)
        self.assertEqual(
            (MountCapability.LIST, MountCapability.READ, MountCapability.WRITE),
            negotiated.strategy.capabilities,
        )
        self.assertIsInstance(transport.requests[2], MountNegotiateRequest)
        self.assertEqual(
            (MountCapability.LIST, MountCapability.READ),
            transport.requests[2].requested_capabilities,
        )

    def test_control_client_bootstraps_read_transfer_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000044")

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        accepted = client.bootstrap_read_transfer(
            "/datasets/train/shard.00000.mds",
            initial_cords=8,
            max_cords=64,
            transfer_id=transfer_id,
        )

        self.assertEqual(transfer_id, accepted.transfer_id)
        self.assertEqual(TransferDirection.READ, accepted.direction)
        self.assertEqual(8, accepted.accepted_initial_cords)
        self.assertEqual(64, accepted.accepted_max_cords)
        self.assertFalse(accepted.write_payload_admission_granted)
        self.assertIsInstance(transport.requests[2], TransferBootstrapRequest)

    def test_control_client_rejects_mismatched_read_transfer_admission(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000066")
        transport.transfer_response_override = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.WRITE,
            1,
            64,
            128 * 1024 * 1024,
            True,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "read transfer admission"):
            client.bootstrap_read_transfer("/datasets/train/shard.00000.mds", transfer_id=transfer_id)

    def test_control_client_bootstraps_write_transfer_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000055")

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        accepted = client.bootstrap_write_transfer(
            "/outputs/run-1",
            payload_format=TransferPayloadFormat.SAFETENSORS,
            initial_cords=16,
            max_cords=64,
            transfer_id=transfer_id,
        )

        self.assertEqual(transfer_id, accepted.transfer_id)
        self.assertEqual(TransferDirection.WRITE, accepted.direction)
        self.assertEqual(16, accepted.accepted_initial_cords)
        self.assertEqual(64, accepted.accepted_max_cords)
        self.assertTrue(accepted.write_payload_admission_granted)
        self.assertIsInstance(transport.requests[2], TransferBootstrapRequest)
        self.assertEqual(TransferDirection.WRITE, transport.requests[2].direction)

    def test_control_client_rejects_mismatched_write_transfer_admission(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000077")
        transport.transfer_response_override = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "write transfer admission"):
            client.bootstrap_write_transfer("/outputs/run-1", transfer_id=transfer_id)

    def test_control_client_rejects_transfer_admission_for_wrong_id(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000088")
        transport.transfer_response_override = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000099"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "admission id"):
            client.bootstrap_read_transfer("/datasets/train/shard.00000.mds", transfer_id=transfer_id)

    def test_control_client_sends_write_entry_metadata_and_commits_after_auth(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000066")

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)
        entry = client.accept_write_entry(
            transfer_id,
            0,
            TransferEntryAction.CREATE_OR_REPLACE,
            "/outputs/run-1/model.safetensors",
            size_hint_bytes=1024,
        )
        metadata = client.accept_write_metadata(
            transfer_id,
            0,
            0,
            True,
            1024,
            (b"\x02" * 32,),
        )
        entry_commit = client.commit_write_entry(
            transfer_id,
            0,
            "/outputs/run-1/model.safetensors",
            b"\x01" * 32,
            1024,
        )
        transfer_commit = client.commit_write_transfer(
            transfer_id,
            "/outputs/run-1",
            b"\x01" * 32,
            1024,
        )

        self.assertEqual("/outputs/run-1/model.safetensors", entry.path)
        self.assertEqual(1, metadata.chunk_hashes_accepted)
        self.assertEqual(1, metadata.staged_payloads_verified)
        self.assertEqual("/outputs/run-1/model.safetensors", entry_commit.path)
        self.assertEqual(1024, transfer_commit.bytes_written)
        self.assertIsInstance(transport.requests[2], TransferEntryUpdate)
        self.assertIsInstance(transport.requests[3], TransferMetadataUpdate)
        self.assertIsInstance(transport.requests[4], TransferEntryCommitRequest)
        self.assertIsInstance(transport.requests[5], TransferCommitRequest)

    def test_control_client_rejects_mismatched_write_entry_acknowledgement(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000066")
        transport.entry_accepted_response_override = TransferEntryAcceptedResponse(
            transfer_id,
            0,
            "/outputs/run-2/model.safetensors",
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "write entry acknowledgement"):
            client.accept_write_entry(
                transfer_id,
                0,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/run-1/model.safetensors",
                size_hint_bytes=1024,
            )

    def test_control_client_rejects_mismatched_write_metadata_acknowledgement(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000066")
        transport.metadata_accepted_response_override = TransferMetadataAcceptedResponse(
            transfer_id,
            0,
            0,
            2,
            1,
            True,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "write metadata acknowledgement"):
            client.accept_write_metadata(
                transfer_id,
                0,
                0,
                True,
                1024,
                (b"\x02" * 32,),
            )

    def test_control_client_rejects_mismatched_write_entry_commit_acknowledgement(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000066")
        transport.entry_committed_response_override = TransferEntryCommittedResponse(
            transfer_id,
            0,
            "/outputs/run-1/model.safetensors",
            b"\x09" * 32,
            1024,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "write entry commit acknowledgement"):
            client.commit_write_entry(
                transfer_id,
                0,
                "/outputs/run-1/model.safetensors",
                b"\x01" * 32,
                1024,
            )

    def test_control_client_rejects_mismatched_write_transfer_commit_acknowledgement(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)
        transfer_id = UUID("00000000-0000-0000-0000-000000000066")
        transport.transfer_committed_response_override = TransferCommittedResponse(
            transfer_id,
            b"\x09" * 32,
            1024,
        )

        with ClientCredentials("trainer", "secret") as credentials:
            client.authenticate(credentials)

        with self.assertRaisesRegex(ValueError, "write transfer commit acknowledgement"):
            client.commit_write_transfer(
                transfer_id,
                "/outputs/run-1",
                b"\x01" * 32,
                1024,
            )

    def test_control_client_raises_protocol_error_response(self):
        transport = ScriptedTransport()
        client = CxnfsControlClient(transport)

        with ClientCredentials("trainer", "wrong") as credentials:
            with self.assertRaises(CxnfsControlError) as raised:
                client.authenticate(credentials)

        self.assertEqual(ResponseStatus.UNAUTHENTICATED, raised.exception.status)

    def test_control_client_closes_transport(self):
        transport = ScriptedTransport()

        with CxnfsControlClient(transport):
            pass

        self.assertTrue(transport.closed)


def response(payload, session_id=None):
    payload_bytes = encode_control_payload(payload)
    frame = ControlFrame(
        ControlOperation[operation_name(payload)],
        payload_bytes,
        UUID("00000000-0000-0000-0000-000000000099"),
        session_id,
    )
    return encode_control_frame(frame)


def operation_name(payload):
    if isinstance(payload, AuthStart):
        return "AUTH_START"
    if isinstance(payload, AuthChallenge):
        return "AUTH_CHALLENGE"
    if isinstance(payload, AuthProof):
        return "AUTH_PROOF"
    if isinstance(payload, AuthAccepted):
        return "AUTH_ACCEPTED"
    if isinstance(payload, DirectoryListRequest):
        return "LIST_DIRECTORY"
    if isinstance(payload, DirectoryListResponse):
        return "LIST_DIRECTORY_RESULT"
    if isinstance(payload, StatPathRequest):
        return "STAT_PATH"
    if isinstance(payload, StatPathResponse):
        return "STAT_PATH_RESULT"
    if isinstance(payload, ManifestRequest):
        return "GET_MANIFEST"
    if isinstance(payload, ManifestResponse):
        return "MANIFEST_RESULT"
    if isinstance(payload, ReconstructionPlanRequest):
        return "GET_RECONSTRUCTION_PLAN"
    if isinstance(payload, ReconstructionPlanResponse):
        return "RECONSTRUCTION_PLAN_RESULT"
    if isinstance(payload, PutFileRequest):
        return "PUT_FILE"
    if isinstance(payload, PutFileResponse):
        return "PUT_FILE_RESULT"
    if isinstance(payload, ReadFileRangeRequest):
        return "READ_FILE_RANGE"
    if isinstance(payload, ReadFileRangeResponse):
        return "READ_FILE_RANGE_RESULT"
    if isinstance(payload, ReadContainerRangeRequest):
        return "READ_CONTAINER_RANGE"
    if isinstance(payload, RenamePathRequest):
        return "RENAME_PATH"
    if isinstance(payload, RenamePathResponse):
        return "RENAME_PATH_RESULT"
    if isinstance(payload, DeletePathRequest):
        return "DELETE_PATH"
    if isinstance(payload, DeletePathResponse):
        return "DELETE_PATH_RESULT"
    if isinstance(payload, MountNegotiateRequest):
        return "MOUNT_NEGOTIATE"
    if isinstance(payload, MountNegotiatedResponse):
        return "MOUNT_NEGOTIATED"
    if isinstance(payload, TransferBootstrapRequest):
        return "TRANSFER_BOOTSTRAP"
    if isinstance(payload, TransferAcceptedResponse):
        return "TRANSFER_ACCEPTED"
    if isinstance(payload, TransferEntryUpdate):
        return "TRANSFER_ENTRY_UPDATE"
    if isinstance(payload, TransferEntryAcceptedResponse):
        return "TRANSFER_ENTRY_ACCEPTED"
    if isinstance(payload, TransferMetadataUpdate):
        return "TRANSFER_METADATA"
    if isinstance(payload, TransferMetadataAcceptedResponse):
        return "TRANSFER_METADATA_ACCEPTED"
    if isinstance(payload, TransferEntryCommitRequest):
        return "TRANSFER_ENTRY_COMMIT"
    if isinstance(payload, TransferEntryCommittedResponse):
        return "TRANSFER_ENTRY_COMMITTED"
    if isinstance(payload, TransferCommitRequest):
        return "TRANSFER_COMMIT"
    if isinstance(payload, TransferCommittedResponse):
        return "TRANSFER_COMMITTED"
    if isinstance(payload, ErrorResponse):
        return "ERROR"
    raise AssertionError(f"unsupported payload: {payload!r}")


if __name__ == "__main__":
    unittest.main()
