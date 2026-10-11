import asyncio
from concurrent.futures import ThreadPoolExecutor
import socket
import struct
import sys
from pathlib import Path
import tempfile
import threading
from uuid import UUID
import unittest
from unittest.mock import patch

from cxnfs import (
    BulkDataFrame,
    BulkReadBufferReceiver,
    BulkReadFrameReceiver,
    BulkReadRequest,
    BulkWriteAck,
    ByteRange,
    CXNFS_TLS_CIPHER_SUITE,
    CxnfsBufferRead,
    CxnfsBulkPayloadTransport,
    CxnfsBulkReadConfig,
    CxnfsBulkRetryConfig,
    CxnfsDatasetClient,
    CxnfsLengthPrefixedBulkClient,
    CxnfsPythonClient,
    CxnfsQuicBulkClient,
    LengthPrefixedBulkFrameTransport,
    PlannedChunkRead,
    PooledLengthPrefixedBulkFrameTransport,
    QUIC_BULK_APPLICATION_PROTOCOL,
    QuicBulkFrameTransport,
    ReadPlan,
    ReadFileRangeResponse,
    TransferAcceptedResponse,
    TransferDirection,
    TransferEntryAcceptedResponse,
    TransferEntryAction,
    TransferEntryCommittedResponse,
    TransferPayloadFormat,
    TransferMetadataAcceptedResponse,
    TensorMemoryTarget,
    TransferCommittedResponse,
    assemble_read_plan_payload,
    cxnfs_chunk_hash,
    cxnfs_file_hash,
    cxnfs_transfer_hash,
    decode_bulk_frame,
    decode_bulk_read_request,
    decode_bulk_write_ack,
    encode_bulk_frame,
    encode_bulk_read_request,
    encode_bulk_write_ack,
    read_encoded_bulk_frame,
    read_encoded_bulk_read_request,
    read_encoded_bulk_write_ack,
    write_encoded_bulk_frame,
    write_encoded_bulk_read_request,
    write_encoded_bulk_write_ack,
)
from cxnfs.bulk import _quic_read_frames_into


class CxnfsBulkTest(unittest.TestCase):
    def test_bulk_frame_codec_matches_payload_contract(self):
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000001"),
            7,
            3,
            11,
            4096,
            True,
            b"chunk bytes",
        )

        decoded = decode_bulk_frame(encode_bulk_frame(frame))

        self.assertEqual(frame, decoded)

    def test_bulk_frame_stream_round_trips_length_prefixed_frames(self):
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000001"),
            7,
            3,
            11,
            4096,
            True,
            b"chunk bytes",
        )
        encoded = encode_bulk_frame(frame)
        left, right = socket.socketpair()
        try:
            write_encoded_bulk_frame(left, encoded)
            self.assertEqual(encoded, read_encoded_bulk_frame(right))
        finally:
            left.close()
            right.close()

    def test_bulk_read_request_codec_matches_java_stream_contract(self):
        request = BulkReadRequest(
            UUID("00000000-0000-0000-0000-000000000101"),
            UUID("00000000-0000-0000-0000-000000000001"),
            "datasets/train/shard.00000.mds",
            128,
            4096,
            16,
        )

        decoded = decode_bulk_read_request(encode_bulk_read_request(request))

        self.assertEqual(request.session_id, decoded.session_id)
        self.assertEqual(request.transfer_id, decoded.transfer_id)
        self.assertEqual("/datasets/train/shard.00000.mds", decoded.path)
        self.assertEqual(128, decoded.offset)
        self.assertEqual(4096, decoded.length)
        self.assertEqual(16, decoded.active_cords)

    def test_bulk_read_request_rejects_confusing_namespace_paths(self):
        with self.assertRaisesRegex(ValueError, "parent"):
            BulkReadRequest(
                UUID("00000000-0000-0000-0000-000000000101"),
                UUID("00000000-0000-0000-0000-000000000001"),
                "/datasets/train/../shard.00000.mds",
                128,
                4096,
                16,
            )

    def test_bulk_read_request_stream_round_trips_length_prefixed_requests(self):
        request = BulkReadRequest(
            UUID("00000000-0000-0000-0000-000000000101"),
            UUID("00000000-0000-0000-0000-000000000001"),
            "/datasets/train/shard.00000.mds",
            128,
            4096,
            16,
        )
        encoded = encode_bulk_read_request(request)
        left, right = socket.socketpair()
        try:
            write_encoded_bulk_read_request(left, encoded)
            self.assertEqual(encoded, read_encoded_bulk_read_request(right))
        finally:
            left.close()
            right.close()

    def test_bulk_write_ack_codec_matches_java_stream_contract(self):
        ack = BulkWriteAck(
            UUID("00000000-0000-0000-0000-000000000123"),
            4,
            9,
            8192,
            True,
            "SPILL",
        )

        decoded = decode_bulk_write_ack(encode_bulk_write_ack(ack))

        self.assertEqual(ack, decoded)

    def test_bulk_write_ack_stream_round_trips_length_prefixed_acks(self):
        ack = BulkWriteAck(
            UUID("00000000-0000-0000-0000-000000000124"),
            2,
            5,
            4096,
            False,
            "MEMORY",
        )
        encoded = encode_bulk_write_ack(ack)
        left, right = socket.socketpair()
        try:
            write_encoded_bulk_write_ack(left, encoded)
            self.assertEqual(encoded, read_encoded_bulk_write_ack(right))
        finally:
            left.close()
            right.close()

    def test_length_prefixed_transport_sends_request_then_reads_touched_chunk_frames(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        session_id = UUID("00000000-0000-0000-0000-000000000101")
        frames = [
            BulkDataFrame(transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]),
        ]
        observed_requests = []
        left, right = socket.socketpair()
        left.settimeout(2.0)
        right.settimeout(2.0)

        def serve():
            observed_requests.append(decode_bulk_read_request(read_encoded_bulk_read_request(left)))
            for frame in frames:
                write_encoded_bulk_frame(left, encode_bulk_frame(frame))

        server_thread = threading.Thread(target=serve)
        try:
            server_thread.start()
            transport = LengthPrefixedBulkFrameTransport(right)

            self.assertEqual(frames, list(transport.read_frames(
                session_id,
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
            )))
            server_thread.join(timeout=2.0)
            self.assertFalse(server_thread.is_alive())
            self.assertEqual(1, len(observed_requests))
            self.assertEqual(session_id, observed_requests[0].session_id)
            self.assertEqual(transfer.transfer_id, observed_requests[0].transfer_id)
            self.assertEqual("/datasets/train/shard.00000.mds", observed_requests[0].path)
            self.assertEqual(0, observed_requests[0].offset)
            self.assertEqual(16, observed_requests[0].length)
            self.assertEqual(1, observed_requests[0].active_cords)
        finally:
            left.close()
            right.close()

    def test_quic_transport_sends_request_then_reads_touched_chunk_frames(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            1,
            128 * 1024 * 1024,
            False,
        )
        session_id = UUID("00000000-0000-0000-0000-000000000101")
        frames = [
            BulkDataFrame(transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(transfer.transfer_id, 0, 0, 1, 8, True, chunks[1]),
        ]
        protocol = FakeQuicBulkProtocol(lambda request: frames)
        transport = QuicBulkFrameTransport(protocol, run_async)

        self.assertEqual(frames, list(transport.read_frames(
            session_id,
            transfer,
            "/datasets/train/shard.00000.mds",
            plan,
        )))
        self.assertEqual(QUIC_BULK_APPLICATION_PROTOCOL, "cxnfs-bulk/1")
        self.assertEqual(1, transport.last_read_stream_count)
        self.assertEqual(1, len(protocol.requests))
        self.assertEqual(session_id, protocol.requests[0].session_id)
        self.assertEqual(transfer.transfer_id, protocol.requests[0].transfer_id)
        self.assertEqual("/datasets/train/shard.00000.mds", protocol.requests[0].path)
        self.assertEqual(0, protocol.requests[0].offset)
        self.assertEqual(16, protocol.requests[0].length)
        self.assertEqual(1, protocol.requests[0].active_cords)

    def test_quic_transport_fans_read_plan_across_admitted_cords(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        frames = [
            BulkDataFrame(UUID("00000000-0000-0000-0000-000000000010"), index % 2, 0, index, index * 8, True, chunk)
            for index, chunk in enumerate(chunks)
        ]
        plan = ReadPlan(
            b"\x09" * 32,
            24,
            ByteRange(0, 24),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            2,
            2,
            128 * 1024 * 1024,
            False,
        )

        def frames_for_request(request):
            return [
                frame for frame in frames
                if request.offset <= frame.entry_offset < request.offset + request.length
            ]

        protocol = FakeQuicBulkProtocol(frames_for_request)
        transport = QuicBulkFrameTransport(protocol, run_async)

        observed = list(transport.read_frames(
            UUID("00000000-0000-0000-0000-000000000101"),
            transfer,
            "/datasets/train/shard.00000.mds",
            plan,
        ))

        self.assertEqual(frames, sorted(observed, key=lambda frame: frame.chunk_sequence))
        self.assertEqual(2, transport.last_read_stream_count)
        self.assertEqual(2, len(protocol.requests))
        self.assertEqual([(0, 16), (16, 8)], [(request.offset, request.length) for request in protocol.requests])
        self.assertEqual([2, 2], [request.active_cords for request in protocol.requests])
        evidence = transport.read_path_evidence()
        self.assertEqual(2, evidence["attempted_read_streams"])
        self.assertEqual(2, evidence["read_streams_created"])
        self.assertEqual(2, evidence["read_streams_eof_verified"])
        self.assertTrue(evidence["read_stream_completion_verified"])
        self.assertTrue(evidence["read_stream_eof_verified"])
        self.assertEqual(2, evidence["total_successful_streams"])
        self.assertEqual("TLSv1.3", evidence["tls_version"])
        self.assertEqual(CXNFS_TLS_CIPHER_SUITE, evidence["cipher_suite"])
        self.assertTrue(evidence["connection_reused_for_streams"])
        self.assertEqual(2, evidence["read_partition_count"])
        self.assertEqual(3, evidence["read_partition_chunk_count"])
        self.assertEqual(24, evidence["read_partition_requested_bytes"])
        self.assertEqual(24, evidence["read_partition_payload_bytes"])
        self.assertTrue(evidence["read_partition_covers_plan"])
        self.assertEqual(
            [
                {
                    "stream_index": 0,
                    "offset": 0,
                    "length": 16,
                    "chunk_count": 2,
                    "payload_bytes": 16,
                    "first_chunk_sequence": 0,
                    "last_chunk_sequence": 1,
                },
                {
                    "stream_index": 1,
                    "offset": 16,
                    "length": 8,
                    "chunk_count": 1,
                    "payload_bytes": 8,
                    "first_chunk_sequence": 2,
                    "last_chunk_sequence": 2,
                },
            ],
            evidence["read_partitions"],
        )

    def test_quic_transport_scales_default_initial_to_touched_chunks(self):
        chunks = [bytes((ord("a") + index,)) * 8 for index in range(4)]
        transfer_id = UUID("00000000-0000-0000-0000-000000000011")
        frames = [
            BulkDataFrame(transfer_id, index, 0, index, index * 8, True, chunk)
            for index, chunk in enumerate(chunks)
        ]
        plan = ReadPlan(
            b"\x09" * 32,
            len(chunks) * 8,
            ByteRange(0, len(chunks) * 8),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )

        def frames_for_request(request):
            return [
                frame for frame in frames
                if request.offset <= frame.entry_offset < request.offset + request.length
            ]

        protocol = FakeQuicBulkProtocol(frames_for_request)
        transport = QuicBulkFrameTransport(protocol, run_async)

        observed = list(transport.read_frames(
            UUID("00000000-0000-0000-0000-000000000101"),
            transfer,
            "/datasets/train/shard.00000.mds",
            plan,
        ))

        self.assertEqual(frames, sorted(observed, key=lambda frame: frame.chunk_sequence))
        self.assertEqual(4, transport.last_read_stream_count)
        self.assertEqual(4, len(protocol.requests))
        self.assertEqual([(index * 8, 8) for index in range(4)], [(request.offset, request.length) for request in protocol.requests])
        self.assertEqual([4, 4, 4, 4], [request.active_cords for request in protocol.requests])
        evidence = transport.read_path_evidence()
        self.assertEqual(4, evidence["last_attempted_read_stream_count"])
        self.assertEqual(4, evidence["read_streams_created"])
        self.assertEqual(4, evidence["read_streams_eof_verified"])
        self.assertTrue(evidence["read_stream_completion_verified"])
        self.assertTrue(evidence["read_stream_eof_verified"])
        self.assertTrue(evidence["connection_reused_for_streams"])
        self.assertEqual(4, evidence["read_partition_count"])
        self.assertEqual(4, evidence["read_partition_chunk_count"])
        self.assertEqual([(index * 8, 8) for index in range(4)], [
            (partition["offset"], partition["length"])
            for partition in evidence["read_partitions"]
        ])

    def test_quic_transport_can_stream_frames_into_receiver_callback(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        frames = [
            BulkDataFrame(transfer_id, index % 2, 0, index, index * 8, True, chunk)
            for index, chunk in enumerate(chunks)
        ]
        plan = ReadPlan(
            b"\x09" * 32,
            24,
            ByteRange(0, 24),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            2,
            2,
            128 * 1024 * 1024,
            False,
        )

        def frames_for_request(request):
            return [
                frame for frame in frames
                if request.offset <= frame.entry_offset < request.offset + request.length
            ]

        protocol = FakeQuicBulkProtocol(frames_for_request)
        transport = QuicBulkFrameTransport(protocol, run_async)
        observed = []

        count = transport.read_frames_into(
            UUID("00000000-0000-0000-0000-000000000101"),
            transfer,
            "/datasets/train/shard.00000.mds",
            plan,
            observed.append,
        )

        self.assertEqual(3, count)
        self.assertEqual(frames, sorted(observed, key=lambda frame: frame.chunk_sequence))
        self.assertEqual(2, transport.last_read_stream_count)
        self.assertEqual(2, len(protocol.requests))

    def test_quic_transport_rejects_frame_for_wrong_transfer_before_callback(self):
        chunk = b"abcdefgh"
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            1,
            128 * 1024 * 1024,
            False,
        )
        wrong_transfer_id = UUID("00000000-0000-0000-0000-000000000999")
        protocol = FakeQuicBulkProtocol(lambda _request: (
            BulkDataFrame(wrong_transfer_id, 0, 0, 0, 0, True, chunk),
        ))
        transport = QuicBulkFrameTransport(protocol, run_async)
        observed = []

        with self.assertRaisesRegex(ValueError, "different transfer"):
            transport.read_frames_into(
                UUID("00000000-0000-0000-0000-000000000101"),
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
                observed.append,
            )

        self.assertEqual([], observed)
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["last_attempted_read_stream_count"])
        self.assertEqual(1, evidence["read_partition_count"])
        self.assertEqual(1, evidence["read_partition_chunk_count"])
        self.assertEqual(8, evidence["read_partition_payload_bytes"])
        self.assertTrue(evidence["read_partition_covers_plan"])
        self.assertFalse(evidence["read_stream_completion_verified"])
        self.assertFalse(evidence["read_stream_eof_verified"])
        self.assertIn("different transfer", evidence["last_read_error"])

    def test_quic_transport_rejects_duplicate_frame_sequence_before_callback(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            1,
            128 * 1024 * 1024,
            False,
        )
        protocol = FakeQuicBulkProtocol(lambda _request: (
            BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),
        ))
        transport = QuicBulkFrameTransport(protocol, run_async)
        observed = []

        with self.assertRaisesRegex(ValueError, "duplicate QUIC bulk frame chunk sequence"):
            transport.read_frames_into(
                UUID("00000000-0000-0000-0000-000000000101"),
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
                observed.append,
            )

        self.assertEqual(1, len(observed))
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertIn("duplicate QUIC bulk frame chunk sequence", evidence["last_read_error"])

    def test_quic_transport_rejects_frame_outside_active_cord_set_before_callback(self):
        chunk = b"abcdefgh"
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        protocol = FakeQuicBulkProtocol(lambda _request: (
            BulkDataFrame(transfer_id, 7, 0, 0, 0, True, chunk),
        ))
        transport = QuicBulkFrameTransport(protocol, run_async)
        observed = []

        with self.assertRaisesRegex(ValueError, "outside active cord set"):
            transport.read_frames_into(
                UUID("00000000-0000-0000-0000-000000000101"),
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
                observed.append,
            )

        self.assertEqual([], observed)
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["last_attempted_read_stream_count"])
        self.assertIn("outside active cord set", evidence["last_read_error"])

    def test_quic_transport_can_fan_read_plan_across_sixty_four_cords(self):
        chunks = [bytes((index % 256,)) * 8 for index in range(64)]
        transfer_id = UUID("00000000-0000-0000-0000-000000000064")
        frames = [
            BulkDataFrame(transfer_id, index, 0, index, index * 8, True, chunk)
            for index, chunk in enumerate(chunks)
        ]
        plan = ReadPlan(
            b"\x09" * 32,
            len(chunks) * 8,
            ByteRange(0, len(chunks) * 8),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            64,
            64,
            128 * 1024 * 1024,
            False,
        )

        def frames_for_request(request):
            return [
                frame for frame in frames
                if request.offset <= frame.entry_offset < request.offset + request.length
            ]

        protocol = FakeQuicBulkProtocol(frames_for_request)
        transport = QuicBulkFrameTransport(protocol, run_async)

        observed = list(transport.read_frames(
            UUID("00000000-0000-0000-0000-000000000101"),
            transfer,
            "/datasets/train/shard.00000.mds",
            plan,
        ))

        self.assertEqual(frames, sorted(observed, key=lambda frame: frame.chunk_sequence))
        self.assertEqual(64, transport.last_read_stream_count)
        self.assertEqual(64, len(protocol.requests))
        self.assertEqual([(index * 8, 8) for index in range(64)], [(request.offset, request.length) for request in protocol.requests])
        self.assertTrue(all(request.active_cords == 64 for request in protocol.requests))

    def test_quic_transport_rejects_extra_frames_after_expected_partition_frames(self):
        chunks = [b"abcdefgh"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        protocol = FakeQuicBulkProtocol(lambda _request: (
            BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(transfer_id, 0, 0, 99, 99, True, b"unexpected"),
        ))
        transport = QuicBulkFrameTransport(protocol, run_async)

        with self.assertRaisesRegex(ValueError, "extra QUIC bulk frame"):
            list(transport.read_frames(
                UUID("00000000-0000-0000-0000-000000000101"),
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
            ))
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["last_attempted_read_stream_count"])
        self.assertIn("extra QUIC bulk frame", evidence["last_read_error"])

        with self.assertRaisesRegex(OSError, "closed"):
            list(transport.read_frames(
                UUID("00000000-0000-0000-0000-000000000101"),
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
            ))

    def test_quic_transport_closes_after_failed_partition_read(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            2,
            2,
            128 * 1024 * 1024,
            False,
        )

        def frames_for_request(request):
            if request.offset == 0:
                return (BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),)
            return tuple()

        protocol = FakeQuicBulkProtocol(frames_for_request)
        transport = QuicBulkFrameTransport(protocol, run_async)
        observed = []

        with self.assertRaisesRegex(EOFError, "fake QUIC stream ended"):
            transport.read_frames_into(
                UUID("00000000-0000-0000-0000-000000000101"),
                transfer,
                "/datasets/train/shard.00000.mds",
                plan,
                observed.append,
            )
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(2, evidence["last_attempted_read_stream_count"])
        self.assertIn("EOFError", evidence["last_read_error"])

    def test_quic_transport_writes_frame_and_reads_ack(self):
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000222"),
            3,
            1,
            5,
            2048,
            True,
            b"payload",
        )
        protocol = FakeQuicBulkProtocol(
            lambda _request: tuple(),
            write_ack=BulkWriteAck(frame.transfer_id, frame.entry_sequence, frame.chunk_sequence, len(frame.payload), True, "MEMORY"),
        )
        transport = QuicBulkFrameTransport(protocol, run_async)

        ack = transport.write_frame(frame)

        self.assertEqual(frame, protocol.writes[0])
        self.assertEqual(frame.transfer_id, ack.transfer_id)
        self.assertEqual(frame.chunk_sequence, ack.chunk_sequence)
        evidence = transport.write_path_evidence()
        self.assertEqual("cxnfs.quic-bulk-write-transport-evidence.v1", evidence["format"])
        self.assertFalse(evidence["closed"])
        self.assertEqual("TLSv1.3", evidence["tls_version"])
        self.assertEqual(CXNFS_TLS_CIPHER_SUITE, evidence["cipher_suite"])
        self.assertEqual(1, evidence["connection_opened_count"])
        self.assertEqual(1, evidence["successful_write_operations"])
        self.assertEqual(0, evidence["failed_write_operations"])
        self.assertEqual(1, evidence["attempted_write_streams"])
        self.assertEqual(1, evidence["write_streams_created"])
        self.assertEqual(1, evidence["write_ack_streams_eof_verified"])
        self.assertTrue(evidence["write_streams_all_succeeded"])
        self.assertTrue(evidence["write_ack_stream_completion_verified"])
        self.assertTrue(evidence["write_ack_stream_eof_verified"])
        self.assertEqual(1, evidence["write_acks_received"])
        self.assertEqual(1, evidence["write_acks_verified"])
        self.assertEqual(0, evidence["write_ack_verification_failures"])
        self.assertTrue(evidence["write_ack_frame_identity_verified"])
        self.assertEqual(1, evidence["write_ack_metadata_verified"])
        self.assertEqual(0, evidence["write_ack_metadata_pending"])
        self.assertFalse(evidence["write_ack_metadata_pending_observed"])
        self.assertEqual(1, evidence["write_ack_memory"])
        self.assertEqual(0, evidence["write_ack_spill"])
        self.assertEqual(0, evidence["active_write_streams"])
        self.assertEqual(1, evidence["max_active_write_streams"])
        self.assertFalse(evidence["concurrent_write_streams_observed"])
        self.assertEqual(1, evidence["total_attempted_streams"])
        self.assertEqual(1, evidence["total_successful_streams"])
        self.assertFalse(evidence["connection_reused_for_streams"])
        self.assertEqual("", evidence["last_write_error"])

    def test_quic_transport_reports_peak_active_write_streams(self):
        protocol = BlockingWriteQuicBulkProtocol()
        transport = QuicBulkFrameTransport(protocol, run_async)
        frames = [
            BulkDataFrame(
                UUID("00000000-0000-0000-0000-000000000222"),
                index,
                1,
                index,
                index * 2048,
                True,
                f"payload-{index}".encode("utf-8"),
            )
            for index in range(2)
        ]

        with ThreadPoolExecutor(max_workers=2) as executor:
            acks = tuple(executor.map(transport.write_frame, frames))

        self.assertEqual([0, 1], [ack.chunk_sequence for ack in acks])
        evidence = transport.write_path_evidence()
        self.assertEqual(2, evidence["successful_write_operations"])
        self.assertEqual(0, evidence["failed_write_operations"])
        self.assertEqual(2, evidence["attempted_write_streams"])
        self.assertEqual(2, evidence["write_streams_created"])
        self.assertEqual(2, evidence["write_ack_streams_eof_verified"])
        self.assertTrue(evidence["write_streams_all_succeeded"])
        self.assertTrue(evidence["write_ack_stream_completion_verified"])
        self.assertTrue(evidence["write_ack_stream_eof_verified"])
        self.assertEqual(2, evidence["write_acks_received"])
        self.assertEqual(2, evidence["write_acks_verified"])
        self.assertEqual(0, evidence["write_ack_verification_failures"])
        self.assertTrue(evidence["write_ack_frame_identity_verified"])
        self.assertEqual(0, evidence["active_write_streams"])
        self.assertEqual(2, evidence["max_active_write_streams"])
        self.assertTrue(evidence["concurrent_write_streams_observed"])
        self.assertTrue(evidence["connection_reused_for_streams"])

    def test_quic_transport_records_failed_write_stream_and_closes(self):
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000222"),
            3,
            1,
            5,
            2048,
            True,
            b"payload",
        )
        protocol = FakeQuicBulkProtocol(
            lambda _request: tuple(),
            write_ack=BulkWriteAck(frame.transfer_id, frame.entry_sequence, frame.chunk_sequence + 1, len(frame.payload), True, "MEMORY"),
        )
        transport = QuicBulkFrameTransport(protocol, run_async)

        with self.assertRaisesRegex(ValueError, "ack chunk"):
            transport.write_frame(frame)

        evidence = transport.write_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(0, evidence["successful_write_operations"])
        self.assertEqual(1, evidence["failed_write_operations"])
        self.assertEqual(1, evidence["attempted_write_streams"])
        self.assertEqual(0, evidence["write_streams_created"])
        self.assertEqual(1, evidence["write_ack_streams_eof_verified"])
        self.assertFalse(evidence["write_streams_all_succeeded"])
        self.assertFalse(evidence["write_ack_stream_completion_verified"])
        self.assertFalse(evidence["write_ack_stream_eof_verified"])
        self.assertEqual(1, evidence["write_acks_received"])
        self.assertEqual(0, evidence["write_acks_verified"])
        self.assertEqual(1, evidence["write_ack_verification_failures"])
        self.assertFalse(evidence["write_ack_frame_identity_verified"])
        self.assertEqual(0, evidence["active_write_streams"])
        self.assertEqual(1, evidence["max_active_write_streams"])
        self.assertFalse(evidence["concurrent_write_streams_observed"])
        self.assertEqual(1, evidence["total_attempted_streams"])
        self.assertEqual(0, evidence["total_successful_streams"])
        self.assertIn("ack chunk", evidence["last_write_error"])

    def test_quic_transport_rejects_extra_write_ack_after_expected_ack(self):
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000222"),
            3,
            1,
            5,
            2048,
            True,
            b"payload",
        )
        protocol = FakeQuicBulkProtocol(
            lambda _request: tuple(),
            write_ack=BulkWriteAck(frame.transfer_id, frame.entry_sequence, frame.chunk_sequence, len(frame.payload), True, "MEMORY"),
            trailing_write_acks=(
                BulkWriteAck(frame.transfer_id, frame.entry_sequence, frame.chunk_sequence + 1, len(frame.payload), False, "MEMORY"),
            ),
        )
        transport = QuicBulkFrameTransport(protocol, run_async)

        with self.assertRaisesRegex(ValueError, "extra QUIC bulk write ack"):
            transport.write_frame(frame)

        evidence = transport.write_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(0, evidence["successful_write_operations"])
        self.assertEqual(1, evidence["failed_write_operations"])
        self.assertEqual(1, evidence["attempted_write_streams"])
        self.assertEqual(0, evidence["write_streams_created"])
        self.assertEqual(0, evidence["write_ack_streams_eof_verified"])
        self.assertFalse(evidence["write_streams_all_succeeded"])
        self.assertFalse(evidence["write_ack_stream_completion_verified"])
        self.assertFalse(evidence["write_ack_stream_eof_verified"])
        self.assertEqual(0, evidence["write_acks_received"])
        self.assertEqual(0, evidence["write_acks_verified"])
        self.assertEqual(1, evidence["write_ack_verification_failures"])
        self.assertFalse(evidence["write_ack_frame_identity_verified"])
        self.assertIn("extra QUIC bulk write ack", evidence["last_write_error"])

    def test_quic_transport_counts_missing_write_ack_as_verification_failure(self):
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000222"),
            3,
            1,
            5,
            2048,
            True,
            b"payload",
        )
        protocol = FakeQuicBulkProtocol(lambda _request: tuple(), missing_write_ack=True)
        transport = QuicBulkFrameTransport(protocol, run_async)

        with self.assertRaisesRegex(EOFError, "fake QUIC stream ended"):
            transport.write_frame(frame)

        evidence = transport.write_path_evidence()
        self.assertTrue(evidence["closed"])
        self.assertEqual(0, evidence["successful_write_operations"])
        self.assertEqual(1, evidence["failed_write_operations"])
        self.assertEqual(1, evidence["attempted_write_streams"])
        self.assertEqual(0, evidence["write_streams_created"])
        self.assertFalse(evidence["write_streams_all_succeeded"])
        self.assertFalse(evidence["write_ack_stream_completion_verified"])
        self.assertFalse(evidence["write_ack_stream_eof_verified"])
        self.assertEqual(0, evidence["write_acks_received"])
        self.assertEqual(0, evidence["write_acks_verified"])
        self.assertEqual(1, evidence["write_ack_verification_failures"])
        self.assertFalse(evidence["write_ack_frame_identity_verified"])
        self.assertIn("fake QUIC stream ended", evidence["last_write_error"])

    def test_length_prefixed_transport_serializes_concurrent_request_response_cycles(self):
        first_chunk = b"abcdefgh"
        second_chunk = b"ijklmnop"
        first_plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(first_chunk), 0, 8, 0, 8),),
        )
        second_plan = ReadPlan(
            b"\x08" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(second_chunk), 0, 8, 0, 8),),
        )
        first_transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000020"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        second_transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000021"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        session_id = UUID("00000000-0000-0000-0000-000000000101")
        left, right = socket.socketpair()
        left.settimeout(2.0)
        right.settimeout(2.0)
        first_request_seen = threading.Event()
        second_read_attempting = threading.Event()
        observed_requests = []
        server_errors = []

        def serve():
            try:
                observed_requests.append(decode_bulk_read_request(read_encoded_bulk_read_request(left)))
                first_request_seen.set()
                if not second_read_attempting.wait(timeout=2.0):
                    raise AssertionError("second read did not attempt to use the shared transport")
                left.settimeout(0.2)
                try:
                    read_encoded_bulk_read_request(left)
                    server_errors.append(AssertionError("second request arrived before first response completed"))
                except socket.timeout:
                    pass
                finally:
                    left.settimeout(2.0)
                write_encoded_bulk_frame(left, encode_bulk_frame(BulkDataFrame(
                    first_transfer.transfer_id,
                    0,
                    0,
                    0,
                    0,
                    True,
                    first_chunk,
                )))
                observed_requests.append(decode_bulk_read_request(read_encoded_bulk_read_request(left)))
                write_encoded_bulk_frame(left, encode_bulk_frame(BulkDataFrame(
                    second_transfer.transfer_id,
                    0,
                    0,
                    0,
                    0,
                    True,
                    second_chunk,
                )))
            except BaseException as exc:  # pragma: no cover - surfaced below
                server_errors.append(exc)

        transport = LengthPrefixedBulkFrameTransport(right)
        results = {}

        def read_first():
            results["first"] = list(transport.read_frames(
                session_id,
                first_transfer,
                "/datasets/train/first.mds",
                first_plan,
            ))

        def read_second():
            second_read_attempting.set()
            results["second"] = list(transport.read_frames(
                session_id,
                second_transfer,
                "/datasets/train/second.mds",
                second_plan,
            ))

        server_thread = threading.Thread(target=serve)
        first_thread = threading.Thread(target=read_first)
        second_thread = threading.Thread(target=read_second)
        try:
            server_thread.start()
            first_thread.start()
            self.assertTrue(first_request_seen.wait(timeout=2.0))
            second_thread.start()
            first_thread.join(timeout=2.0)
            second_thread.join(timeout=2.0)
            server_thread.join(timeout=2.0)

            self.assertFalse(first_thread.is_alive())
            self.assertFalse(second_thread.is_alive())
            self.assertFalse(server_thread.is_alive())
            if server_errors:
                raise server_errors[0]
            self.assertEqual(first_chunk, results["first"][0].payload)
            self.assertEqual(second_chunk, results["second"][0].payload)
            self.assertEqual(
                [first_transfer.transfer_id, second_transfer.transfer_id],
                [request.transfer_id for request in observed_requests],
            )
        finally:
            left.close()
            right.close()

    def test_assembles_requested_range_after_verifying_full_touched_chunks(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        frames = [
            BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(transfer_id, 1, 0, 1, 8, True, chunks[1]),
            BulkDataFrame(transfer_id, 0, 0, 2, 16, True, chunks[2]),
        ]

        payload = assemble_read_plan_payload(plan, frames)

        self.assertEqual(b"fghijklmnopqrst", payload)

    def test_rejects_bulk_frames_for_the_wrong_transfer(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000010"),
            0,
            0,
            0,
            0,
            True,
            chunk,
        )

        with self.assertRaisesRegex(ValueError, "different transfer"):
            assemble_read_plan_payload(
                plan,
                [frame],
                transfer_id=UUID("00000000-0000-0000-0000-000000000011"),
            )

    def test_rejects_bulk_frames_outside_the_accepted_cord_range(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000010"),
            7,
            0,
            0,
            0,
            True,
            chunk,
        )

        with self.assertRaisesRegex(ValueError, "accepted cord range"):
            assemble_read_plan_payload(plan, [frame], max_cords=4)

    def test_rejects_partial_chunk_payload_because_plan_hash_cannot_verify_it(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(2, 3),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 2, 3),),
        )
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000010"),
            0,
            0,
            0,
            0,
            True,
            b"cde",
        )

        with self.assertRaisesRegex(ValueError, "full touched chunk"):
            assemble_read_plan_payload(plan, [frame])

    def test_rejects_corrupted_payload_before_slicing(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000010"),
            0,
            0,
            0,
            0,
            True,
            b"abcdEfgh",
        )

        with self.assertRaisesRegex(ValueError, "hash verification"):
            assemble_read_plan_payload(plan, [frame])

    def test_streaming_receiver_accepts_verified_frames_before_final_assembly(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        receiver = BulkReadFrameReceiver(transfer, plan)

        receiver.accept(BulkDataFrame(transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]))
        receiver.accept(BulkDataFrame(transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]))
        receiver.accept(BulkDataFrame(transfer.transfer_id, 2, 0, 2, 16, True, chunks[2]))

        self.assertEqual(3, receiver.received_count)
        self.assertEqual(b"fghijklmnopqrst", receiver.complete())

    def test_streaming_receiver_rejects_wrong_transfer_before_buffering(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        receiver = BulkReadFrameReceiver(transfer, plan)

        with self.assertRaisesRegex(ValueError, "different transfer"):
            receiver.accept(BulkDataFrame(
                UUID("00000000-0000-0000-0000-000000000011"),
                0,
                0,
                0,
                0,
                True,
                chunk,
            ))

    def test_streaming_receiver_requires_every_touched_chunk(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        receiver = BulkReadFrameReceiver(transfer, plan)
        receiver.accept(BulkDataFrame(transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]))

        with self.assertRaisesRegex(ValueError, "missing bulk frame"):
            receiver.complete()

    def test_buffer_receiver_writes_verified_slices_directly_into_destination(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        destination = bytearray(15)
        receiver = BulkReadBufferReceiver(transfer, plan, destination)

        receiver.accept(BulkDataFrame(transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]))
        receiver.accept(BulkDataFrame(transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]))
        receiver.accept(BulkDataFrame(transfer.transfer_id, 2, 0, 2, 16, True, chunks[2]))

        self.assertEqual(15, receiver.complete())
        self.assertEqual(bytearray(b"fghijklmnopqrst"), destination)

    def test_buffer_receiver_can_clear_requested_destination_range_after_failed_read(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        destination = bytearray(b"x" * 20)
        receiver = BulkReadBufferReceiver(transfer, plan, destination)
        receiver.accept(BulkDataFrame(transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]))

        zeroed = receiver.clear_destination()

        self.assertEqual(16, zeroed)
        self.assertEqual(bytearray(b"\x00" * 16 + b"xxxx"), destination)
        self.assertEqual(0, receiver.received_count)

    def test_dataset_client_can_read_mds_shard_through_control_plan_and_bulk_frames(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(transfer_id, 1, 0, 1, 8, True, chunks[1]),
            BulkDataFrame(transfer_id, 0, 0, 2, 16, True, chunks[2]),
        ])
        client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(control, bulk))

        payload = client.read_mds_shard("/datasets/train/shard.00000.mds", 5, 15)

        self.assertEqual(b"fghijklmnopqrst", payload)
        self.assertEqual(("/datasets/train/shard.00000.mds", 1, 128, 64 * 1024 * 1024, 0), control.bootstraps[0])
        self.assertEqual(("/datasets/train/shard.00000.mds", 5, 15), control.requests[0])
        self.assertEqual(control.transfer, bulk.transfers[0])
        self.assertEqual(control.session_id, bulk.sessions[0])
        self.assertEqual("/datasets/train/shard.00000.mds", bulk.paths[0])
        self.assertEqual(plan, bulk.plans[0])

    def test_bulk_payload_transport_uses_configured_cord_request(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        transport = CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(
                initial_cords=8,
                max_cords=32,
                provisional_container_size_bytes=32 * 1024 * 1024,
                expected_size_limit_bytes=1024,
            ),
        )
        client = CxnfsDatasetClient(transport)

        self.assertEqual(b"abcdefgh", client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8))
        self.assertEqual(
            ("/datasets/train/shard.00000.mds", 8, 32, 32 * 1024 * 1024, 1024),
            control.bootstraps[0],
        )

    def test_bulk_read_config_validates_write_frame_payload_size(self):
        with self.assertRaisesRegex(ValueError, "write_frame_payload_bytes"):
            CxnfsBulkReadConfig(write_frame_payload_bytes=0)

    def test_bulk_retry_config_validates_write_retries(self):
        self.assertEqual(3, CxnfsBulkRetryConfig(write_retries=2).write_attempts)
        with self.assertRaisesRegex(ValueError, "write_retries"):
            CxnfsBulkRetryConfig(write_retries=-1)
        with self.assertRaisesRegex(ValueError, "write_retries"):
            CxnfsBulkRetryConfig(write_retries=9)

    def test_bulk_payload_transport_rejects_mismatched_read_response_path_before_bulk(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = BadReadResponseControlClient(plan, response_path="/datasets/train/other.mds")
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "path"):
            client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8)

        self.assertEqual([], bulk.sessions)

    def test_bulk_payload_transport_rejects_mismatched_read_plan_range_before_bulk(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(1, 7),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 1, 7),),
        )
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "range"):
            client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8)

        self.assertEqual([], bulk.sessions)

    def test_bulk_payload_transport_requires_authenticated_control_session(self):
        chunk = b"abcdefgh"
        plan = ReadPlan(
            b"\x09" * 32,
            8,
            ByteRange(0, 8),
            (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, 8, 0, 8),),
        )
        control = FakeControlClient(plan)
        del control.session_id
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "authenticated control session id"):
            client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8)

    def test_bulk_payload_transport_reads_directly_into_writable_buffer(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]),
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(control.transfer.transfer_id, 2, 0, 2, 16, True, chunks[2]),
        ])
        transport = CxnfsBulkPayloadTransport(control, bulk)
        client = CxnfsPythonClient(transport)
        destination = bytearray(15)

        bytes_read = client.read_into_buffer("/datasets/train/shard.00000.mds", 5, destination)

        self.assertEqual(15, bytes_read)
        self.assertEqual(bytearray(b"fghijklmnopqrst"), destination)
        self.assertEqual(control.session_id, bulk.sessions[0])
        self.assertEqual(("/datasets/train/shard.00000.mds", 5, 15), control.requests[0])

    def test_bulk_payload_transport_supports_ordered_batch_direct_buffer_reads(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]),
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(control.transfer.transfer_id, 2, 0, 2, 16, True, chunks[2]),
        ])
        transport = CxnfsBulkPayloadTransport(control, bulk)
        client = CxnfsPythonClient(transport)
        first = bytearray(15)
        second = bytearray(15)

        result = client.read_into_buffers(
            (
                CxnfsBufferRead("/datasets/train/shard.00000.mds", 5, first, 15),
                CxnfsBufferRead("/datasets/train/shard.00000.mds", 5, second, 15),
            ),
            max_workers=1,
        )

        self.assertEqual((15, 15), result)
        self.assertEqual(bytearray(b"fghijklmnopqrst"), first)
        self.assertEqual(bytearray(b"fghijklmnopqrst"), second)
        self.assertEqual(2, len(control.requests))
        self.assertEqual(2, len(bulk.sessions))
        evidence = transport.read_path_evidence()
        self.assertEqual(2, evidence["read_operations"])
        self.assertEqual(2, evidence["direct_buffer_read_operations"])
        self.assertEqual(2, evidence["direct_destination_read_operations"])
        self.assertEqual(30, evidence["payload_bytes_read"])

    def test_bulk_payload_transport_prefers_transport_frame_sink_for_direct_buffer_reads(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        control = FakeControlClient(plan)
        bulk = CallbackOnlyBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]),
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(control.transfer.transfer_id, 2, 0, 2, 16, True, chunks[2]),
        ])
        transport = CxnfsBulkPayloadTransport(control, bulk)
        client = CxnfsPythonClient(transport)
        destination = bytearray(15)

        bytes_read = client.read_into_buffer("/datasets/train/shard.00000.mds", 5, destination)

        self.assertEqual(15, bytes_read)
        self.assertEqual(bytearray(b"fghijklmnopqrst"), destination)
        self.assertEqual(1, bulk.frame_sink_calls)
        self.assertEqual(0, bulk.read_frames_calls)
        self.assertEqual(control.session_id, bulk.sessions[0])
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["transport_frame_sink_supported"])
        self.assertTrue(evidence["transport_frame_sink_observed"])
        self.assertEqual(1, evidence["read_operations"])
        self.assertEqual(1, evidence["direct_buffer_read_operations"])
        self.assertEqual(1, evidence["direct_destination_read_operations"])
        self.assertEqual(1, evidence["direct_destination_frame_sink_read_operations"])
        self.assertEqual(0, evidence["fallback_frame_iter_read_operations"])
        self.assertEqual(3, evidence["frame_sink_frames"])
        self.assertEqual(15, evidence["payload_bytes_read"])
        self.assertTrue(evidence["frame_sink_used_for_all_direct_destination_reads"])

    def test_quic_bulk_payload_transport_records_direct_buffer_cord_fanout(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            24,
            ByteRange(0, 24),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        control = FakeControlClient(plan)
        control.transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            3,
            3,
            128 * 1024 * 1024,
            False,
        )
        frames = [
            BulkDataFrame(transfer_id, index, 0, index, index * 8, True, chunk)
            for index, chunk in enumerate(chunks)
        ]

        def frames_for_request(request):
            return [
                frame for frame in frames
                if request.offset <= frame.entry_offset < request.offset + request.length
            ]

        protocol = FakeQuicBulkProtocol(frames_for_request)
        quic = QuicBulkFrameTransport(protocol, run_async)
        transport = CxnfsBulkPayloadTransport(
            control,
            quic,
            CxnfsBulkReadConfig(initial_cords=3, max_cords=3),
        )
        client = CxnfsPythonClient(transport)
        destination = bytearray(24)

        bytes_read = client.read_into_buffer("/datasets/train/shard.00000.mds", 0, destination, 24)

        self.assertEqual(24, bytes_read)
        self.assertEqual(bytearray(b"abcdefghijklmnopqrstuvwx"), destination)
        self.assertEqual(
            [(0, 8), (8, 8), (16, 8)],
            sorted((request.offset, request.length) for request in protocol.requests),
        )
        self.assertEqual([3, 3, 3], [request.active_cords for request in protocol.requests])
        evidence = transport.read_path_evidence()
        self.assertTrue(evidence["transport_frame_sink_observed"])
        self.assertEqual(1, evidence["direct_buffer_read_operations"])
        self.assertEqual(1, evidence["direct_destination_read_operations"])
        self.assertEqual(1, evidence["direct_destination_frame_sink_read_operations"])
        self.assertEqual(1, evidence["direct_destination_parallel_frame_sink_read_operations"])
        self.assertEqual(3, evidence["direct_destination_parallel_frame_sink_frames"])
        self.assertEqual(3, evidence["direct_destination_last_observed_cords"])
        self.assertEqual(3, evidence["direct_destination_max_observed_cords"])
        self.assertEqual(3, evidence["last_accepted_initial_cords"])
        self.assertEqual(3, evidence["last_accepted_max_cords"])
        self.assertTrue(evidence["observed_cords_within_admission"])
        self.assertTrue(evidence["direct_destination_used_parallel_cords"])
        self.assertTrue(evidence["frame_sink_used_for_all_direct_destination_reads"])
        self.assertTrue(evidence["direct_destination_frame_sink_cord_fanout_verified"])
        quic_evidence = evidence["bulk_transport_read_evidence"]
        self.assertEqual(3, quic_evidence["last_read_stream_count"])
        self.assertEqual(3, quic_evidence["read_streams_created"])
        self.assertEqual(3, quic_evidence["read_streams_eof_verified"])
        self.assertTrue(quic_evidence["read_stream_completion_verified"])
        self.assertTrue(quic_evidence["read_stream_eof_verified"])
        self.assertTrue(quic_evidence["connection_reused_for_streams"])

    def test_quic_bulk_payload_transport_grows_default_initial_for_direct_buffer_reads(self):
        chunks = [bytes((ord("a") + index,)) * 8 for index in range(4)]
        transfer_id = UUID("00000000-0000-0000-0000-000000000012")
        plan = ReadPlan(
            b"\x09" * 32,
            len(chunks) * 8,
            ByteRange(0, len(chunks) * 8),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        control = FakeControlClient(plan)
        control.transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        frames = [
            BulkDataFrame(transfer_id, index, 0, index, index * 8, True, chunk)
            for index, chunk in enumerate(chunks)
        ]

        def frames_for_request(request):
            return [
                frame for frame in frames
                if request.offset <= frame.entry_offset < request.offset + request.length
            ]

        protocol = FakeQuicBulkProtocol(frames_for_request)
        quic = QuicBulkFrameTransport(protocol, run_async)
        transport = CxnfsBulkPayloadTransport(control, quic)
        client = CxnfsPythonClient(transport)
        destination = bytearray(len(chunks) * 8)

        bytes_read = client.read_into_buffer("/datasets/train/shard.00000.mds", 0, destination, len(destination))

        self.assertEqual(len(destination), bytes_read)
        self.assertEqual(bytearray(b"".join(chunks)), destination)
        self.assertEqual([(index * 8, 8) for index in range(4)], sorted(
            (request.offset, request.length) for request in protocol.requests
        ))
        self.assertEqual([4, 4, 4, 4], [request.active_cords for request in protocol.requests])
        evidence = transport.read_path_evidence()
        self.assertEqual(1, evidence["direct_buffer_read_operations"])
        self.assertEqual(4, evidence["direct_destination_last_observed_cords"])
        self.assertEqual(4, evidence["direct_destination_max_observed_cords"])
        self.assertEqual(1, evidence["last_accepted_initial_cords"])
        self.assertEqual(64, evidence["last_accepted_max_cords"])
        self.assertTrue(evidence["observed_cords_within_admission"])
        self.assertTrue(evidence["direct_destination_used_parallel_cords"])
        self.assertTrue(evidence["direct_destination_frame_sink_cord_fanout_verified"])
        quic_evidence = evidence["bulk_transport_read_evidence"]
        self.assertEqual(4, quic_evidence["last_attempted_read_stream_count"])
        self.assertEqual(4, quic_evidence["last_read_stream_count"])
        self.assertEqual(4, quic_evidence["read_streams_created"])
        self.assertEqual(4, quic_evidence["read_streams_eof_verified"])
        self.assertTrue(quic_evidence["read_stream_completion_verified"])
        self.assertTrue(quic_evidence["read_stream_eof_verified"])

    def test_bulk_payload_transport_reconnects_and_retries_byte_return_reads(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        first = FailingReadBulkFrameTransport("cord closed before payload")
        replacement = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        reconnects = []

        def reconnect():
            reconnects.append("called")
            return replacement

        transport = CxnfsBulkPayloadTransport(
            control,
            first,
            retry_config=CxnfsBulkRetryConfig(read_retries=1),
            bulk_transport_reconnector=reconnect,
        )
        client = CxnfsPythonClient(transport)

        payload = client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8)

        self.assertEqual(b"abcdefgh", payload)
        self.assertEqual(1, first.close_calls)
        self.assertEqual(["called"], reconnects)
        self.assertEqual(2, len(control.bootstraps))
        self.assertEqual(2, len(control.requests))
        self.assertEqual(control.session_id, replacement.sessions[0])
        evidence = transport.read_path_evidence()
        self.assertEqual(1, evidence["read_operations"])
        self.assertEqual(1, evidence["byte_return_read_operations"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["failed_byte_return_read_operations"])
        self.assertEqual(1, evidence["read_retry_attempts"])
        self.assertEqual(1, evidence["read_retry_successes"])
        self.assertEqual(1, evidence["bulk_transport_reconnects"])
        self.assertEqual("", evidence["last_read_retry_error"])

    def test_bulk_payload_transport_concurrent_read_retries_share_replacement(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        first = BarrierFailingReadBulkFrameTransport("cord closed before payload", parties=2)
        replacement = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        reconnects = []
        reconnect_lock = threading.Lock()

        def reconnect():
            with reconnect_lock:
                reconnects.append("called")
            return replacement

        transport = CxnfsBulkPayloadTransport(
            control,
            first,
            retry_config=CxnfsBulkRetryConfig(read_retries=1),
            bulk_transport_reconnector=reconnect,
        )
        client = CxnfsPythonClient(transport)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(
                lambda _index: client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8),
                range(2),
            ))

        self.assertEqual((chunk, chunk), results)
        self.assertEqual(["called"], reconnects)
        self.assertEqual(1, first.close_calls)
        self.assertEqual(2, len(replacement.sessions))
        self.assertEqual(4, len(control.bootstraps))
        self.assertEqual(4, len(control.requests))
        evidence = transport.read_path_evidence()
        self.assertEqual(2, evidence["read_operations"])
        self.assertEqual(2, evidence["byte_return_read_operations"])
        self.assertEqual(2, evidence["failed_read_operations"])
        self.assertEqual(2, evidence["failed_byte_return_read_operations"])
        self.assertEqual(2, evidence["read_retry_attempts"])
        self.assertEqual(2, evidence["read_retry_successes"])
        self.assertEqual(1, evidence["bulk_transport_reconnects"])
        self.assertEqual("", evidence["last_read_retry_error"])

    def test_quic_bulk_client_closes_reconnected_bulk_transport(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        first = FailingReadBulkFrameTransport("cord closed before payload")
        replacement = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsQuicBulkClient(
            control,
            first,
            read_config=CxnfsBulkReadConfig(),
            retry_config=CxnfsBulkRetryConfig(read_retries=1),
            bulk_transport_reconnector=lambda: replacement,
        )

        payload = client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8)
        client.close()

        self.assertEqual(chunk, payload)
        self.assertEqual(1, first.close_calls)
        self.assertEqual(1, replacement.close_calls)
        self.assertEqual(1, client.read_path_evidence()["bulk_transport_reconnects"])

    def test_bulk_payload_transport_does_not_retry_corrupted_byte_return_payload(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        corrupted = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, b"ABCDEFGH"),
        ])
        reconnects = []
        transport = CxnfsBulkPayloadTransport(
            control,
            corrupted,
            retry_config=CxnfsBulkRetryConfig(read_retries=1),
            bulk_transport_reconnector=lambda: reconnects.append("called"),
        )
        client = CxnfsPythonClient(transport)

        with self.assertRaisesRegex(ValueError, "chunk hash verification failed"):
            client.read_mds_shard("/datasets/train/shard.00000.mds", 0, 8)

        self.assertEqual([], reconnects)
        self.assertEqual(1, len(control.bootstraps))
        self.assertEqual(1, len(control.requests))
        evidence = transport.read_path_evidence()
        self.assertEqual(0, evidence["read_operations"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["failed_byte_return_read_operations"])
        self.assertEqual(0, evidence["read_retry_attempts"])
        self.assertEqual(0, evidence["read_retry_successes"])
        self.assertEqual(0, evidence["bulk_transport_reconnects"])

    def test_bulk_payload_transport_clears_direct_buffer_then_retries_on_reconnected_transport(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        control = FakeControlClient(plan)
        first = FailingCallbackBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
        ])
        replacement = CallbackOnlyBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(control.transfer.transfer_id, 1, 0, 1, 8, True, chunks[1]),
        ])

        transport = CxnfsBulkPayloadTransport(
            control,
            first,
            retry_config=CxnfsBulkRetryConfig(read_retries=1),
            bulk_transport_reconnector=lambda: replacement,
        )
        client = CxnfsPythonClient(transport)
        destination = bytearray(b"x" * 20)

        bytes_read = client.read_into_buffer("/datasets/train/shard.00000.mds", 0, destination, 16)

        self.assertEqual(16, bytes_read)
        self.assertEqual(bytearray(b"abcdefghijklmnopxxxx"), destination)
        self.assertEqual(1, first.close_calls)
        self.assertEqual(2, len(control.bootstraps))
        self.assertEqual(2, len(control.requests))
        self.assertEqual(1, first.frame_sink_calls)
        self.assertEqual(1, replacement.frame_sink_calls)
        evidence = transport.read_path_evidence()
        self.assertEqual(1, evidence["read_operations"])
        self.assertEqual(1, evidence["direct_buffer_read_operations"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["failed_direct_buffer_read_operations"])
        self.assertEqual(1, evidence["failed_direct_destination_read_operations"])
        self.assertEqual(16, evidence["direct_destination_zeroed_bytes_on_failure"])
        self.assertEqual(1, evidence["read_retry_attempts"])
        self.assertEqual(1, evidence["read_retry_successes"])
        self.assertEqual(1, evidence["bulk_transport_reconnects"])

    def test_bulk_payload_transport_clears_direct_buffer_without_retrying_corrupted_payload(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        corrupted = CallbackOnlyBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, b"ABCDEFGH"),
        ])
        reconnects = []
        transport = CxnfsBulkPayloadTransport(
            control,
            corrupted,
            retry_config=CxnfsBulkRetryConfig(read_retries=1),
            bulk_transport_reconnector=lambda: reconnects.append("called"),
        )
        client = CxnfsPythonClient(transport)
        destination = bytearray(b"x" * 12)

        with self.assertRaisesRegex(ValueError, "chunk hash verification failed"):
            client.read_into_buffer("/datasets/train/shard.00000.mds", 0, destination, 8)

        self.assertEqual(bytearray(b"\x00" * 8 + b"xxxx"), destination)
        self.assertEqual([], reconnects)
        self.assertEqual(1, len(control.bootstraps))
        self.assertEqual(1, len(control.requests))
        evidence = transport.read_path_evidence()
        self.assertEqual(0, evidence["read_operations"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["failed_direct_buffer_read_operations"])
        self.assertEqual(1, evidence["failed_direct_destination_read_operations"])
        self.assertEqual(8, evidence["direct_destination_zeroed_bytes_on_failure"])
        self.assertEqual(0, evidence["read_retry_attempts"])
        self.assertEqual(0, evidence["read_retry_successes"])
        self.assertEqual(0, evidence["bulk_transport_reconnects"])

    def test_bulk_payload_transport_clears_direct_buffer_when_read_fails(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
        ])
        transport = CxnfsBulkPayloadTransport(control, bulk)
        client = CxnfsPythonClient(transport)
        destination = bytearray(b"x" * 20)

        with self.assertRaisesRegex(ValueError, "missing bulk frame"):
            client.read_into_buffer("/datasets/train/shard.00000.mds", 0, destination, 16)

        self.assertEqual(bytearray(b"\x00" * 16 + b"xxxx"), destination)
        evidence = transport.read_path_evidence()
        self.assertEqual(0, evidence["read_operations"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["failed_direct_buffer_read_operations"])
        self.assertEqual(1, evidence["failed_direct_destination_read_operations"])
        self.assertEqual(16, evidence["direct_destination_zeroed_bytes_on_failure"])
        self.assertTrue(evidence["direct_destination_failure_zeroing_enabled"])

    def test_quic_direct_buffer_read_failure_clears_destination_and_keeps_transport_evidence(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        control = FakeControlClient(plan)
        control.transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            2,
            2,
            128 * 1024 * 1024,
            False,
        )

        def frames_for_request(request):
            if request.offset == 0:
                return (BulkDataFrame(transfer_id, 0, 0, 0, 0, True, chunks[0]),)
            return (BulkDataFrame(transfer_id, 1, 0, 1, 8, True, b"ijklmnoP"),)

        quic = QuicBulkFrameTransport(FakeQuicBulkProtocol(frames_for_request), run_async)
        transport = CxnfsBulkPayloadTransport(control, quic)
        client = CxnfsPythonClient(transport)
        destination = bytearray(b"x" * 20)

        with self.assertRaisesRegex(ValueError, "hash verification"):
            client.read_into_buffer("/datasets/train/shard.00000.mds", 0, destination, 16)

        self.assertEqual(bytearray(b"\x00" * 16 + b"xxxx"), destination)
        evidence = transport.read_path_evidence()
        self.assertEqual(0, evidence["read_operations"])
        self.assertEqual(1, evidence["failed_read_operations"])
        self.assertEqual(1, evidence["failed_direct_buffer_read_operations"])
        self.assertEqual(1, evidence["failed_direct_destination_read_operations"])
        self.assertEqual(16, evidence["direct_destination_zeroed_bytes_on_failure"])
        quic_evidence = evidence["bulk_transport_read_evidence"]
        self.assertTrue(quic_evidence["closed"])
        self.assertEqual(1, quic_evidence["failed_read_operations"])
        self.assertEqual(2, quic_evidence["last_attempted_read_stream_count"])
        self.assertFalse(quic_evidence["read_stream_completion_verified"])
        self.assertFalse(quic_evidence["read_stream_eof_verified"])
        self.assertIn("hash verification", quic_evidence["last_read_error"])

    def test_quic_direct_buffer_failure_cancels_sibling_streams_before_zeroed_buffer_returns(self):
        chunks = [b"abcdefgh", b"ijklmnop"]
        transfer_id = UUID("00000000-0000-0000-0000-000000000010")
        plan = ReadPlan(
            b"\x09" * 32,
            16,
            ByteRange(0, 16),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 0, 8),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
            ),
        )
        transfer = TransferAcceptedResponse(
            transfer_id,
            TransferDirection.READ,
            2,
            2,
            128 * 1024 * 1024,
            False,
        )
        destination = bytearray(b"x" * 20)
        receiver = BulkReadBufferReceiver(transfer, plan, destination)
        protocol = FailOneDelayOneQuicBulkProtocol(transfer_id, chunks)

        async def exercise_failure():
            with self.assertRaisesRegex(ValueError, "hash verification"):
                await asyncio.wait_for(
                    _quic_read_frames_into(
                        protocol,
                        UUID("00000000-0000-0000-0000-000000000101"),
                        transfer,
                        "/datasets/train/shard.00000.mds",
                        plan,
                        receiver.accept,
                    ),
                    timeout=2.0,
                )
            self.assertTrue(protocol.delayed_read_started.is_set())
            self.assertTrue(protocol.delayed_read_cancelled.is_set())
            receiver.clear_destination()
            protocol.release_delayed_frame()
            await asyncio.sleep(0.05)

        asyncio.run(exercise_failure())

        self.assertFalse(protocol.delayed_frame_delivered.is_set())
        self.assertEqual(bytearray(b"\x00" * 16 + b"xxxx"), destination)

    def test_bulk_payload_transport_reads_into_cpu_tensor_memory(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))
        tensor = FakeWritableTensor(8)

        bytes_read = client.read_into_tensor("/datasets/train/shard.00000.mds", 0, tensor, 8)

        self.assertEqual(8, bytes_read)
        self.assertEqual(bytearray(b"abcdefgh"), tensor.data)
        self.assertEqual(control.session_id, bulk.sessions[0])

    def test_bulk_payload_transport_allocates_pinned_cpu_tensor_for_read_tensor(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))
        fake_torch = FakeTorchModule()

        with FakeTorchInstall(fake_torch):
            tensor = client.read_tensor(
                "/datasets/train/shard.00000.mds",
                0,
                8,
                "float32",
                (2,),
                target=TensorMemoryTarget.PYTORCH_PINNED_CPU,
            )

        self.assertEqual(bytearray(b"abcdefgh"), tensor.data)
        self.assertEqual("cpu", tensor.device)
        self.assertTrue(tensor.is_pinned())
        self.assertEqual([((2,), "float32", "cpu", True)], fake_torch.empty_calls)
        self.assertEqual(("/datasets/train/shard.00000.mds", 0, 8), control.requests[0])

    def test_bulk_payload_transport_stages_cuda_tensor_through_pinned_cpu(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))
        fake_torch = FakeTorchModule()

        with FakeTorchInstall(fake_torch):
            tensor = client.read_cuda_tensor(
                "/datasets/train/shard.00000.mds",
                0,
                8,
                "float32",
                (2,),
                device="cuda:0",
            )

        self.assertEqual(bytearray(b"abcdefgh"), tensor.data)
        self.assertEqual("cuda:0", tensor.device)
        self.assertTrue(tensor.is_cuda)
        self.assertTrue(tensor.copied_from.is_pinned())
        self.assertTrue(tensor.non_blocking_copy)
        self.assertEqual([((2,), "float32", "cpu", True)], fake_torch.empty_calls)
        self.assertEqual(control.session_id, bulk.sessions[0])

    def test_bulk_payload_transport_reads_into_caller_cuda_tensor_through_pinned_cpu(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        bulk = CallbackOnlyBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        transport = CxnfsBulkPayloadTransport(control, bulk)
        client = CxnfsPythonClient(transport)
        fake_torch = FakeTorchModule()
        destination = FakeTorchTensor(
            8,
            dtype=fake_torch.float32,
            device="cuda:0",
            pin_memory=False,
            shape=(2,),
        )

        with FakeTorchInstall(fake_torch):
            bytes_read = client.read_into_tensor(
                "/datasets/train/shard.00000.mds",
                0,
                destination,
                8,
                non_blocking=True,
            )

        self.assertEqual(8, bytes_read)
        self.assertEqual(bytearray(b"abcdefgh"), destination.data)
        self.assertTrue(destination.is_cuda)
        self.assertTrue(destination.copied_from.is_pinned())
        self.assertTrue(destination.non_blocking_copy)
        self.assertEqual([((2,), "float32", "cpu", True)], fake_torch.empty_calls)
        evidence = transport.read_path_evidence()
        self.assertEqual(1, evidence["direct_tensor_read_operations"])
        self.assertEqual(1, evidence["direct_destination_read_operations"])
        self.assertEqual(1, evidence["direct_destination_frame_sink_read_operations"])
        self.assertTrue(evidence["frame_sink_used_for_all_direct_destination_reads"])

    def test_bulk_payload_transport_rejects_partial_caller_cuda_tensor_read(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        bulk = CallbackOnlyBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        transport = CxnfsBulkPayloadTransport(control, bulk)
        client = CxnfsPythonClient(transport)
        fake_torch = FakeTorchModule()
        destination = FakeTorchTensor(
            8,
            dtype=fake_torch.float32,
            device="cuda:0",
            pin_memory=False,
            shape=(2,),
        )

        with FakeTorchInstall(fake_torch):
            with self.assertRaisesRegex(ValueError, "byte count"):
                client.read_into_tensor(
                    "/datasets/train/shard.00000.mds",
                    0,
                    destination,
                    4,
                    non_blocking=True,
                )

        self.assertEqual(bytearray(b"\x00" * 8), destination.data)
        self.assertEqual([], bulk.sessions)

    def test_bulk_payload_transport_sends_safetensors_output_with_payload_first_frames(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport(tuple())
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "model.safetensors").write_bytes(b"checkpoint tensor bytes")
            (root / "config.json").write_bytes(b"{\"hidden\": 16}")
            client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(control, bulk, CxnfsBulkReadConfig(initial_cords=2, max_cords=4)))

            result = client.send_safetensors_output(root, remote_root="/outputs/run-1")

        self.assertEqual(2, result.file_count)
        self.assertEqual(1, result.tensor_file_count)
        self.assertEqual(len(b"{\"hidden\": 16}") + len(b"checkpoint tensor bytes"), result.bytes_sent)
        self.assertEqual(1, len(control.write_bootstraps))
        self.assertEqual("/outputs/run-1", control.write_bootstraps[0][0])
        self.assertEqual(2, control.write_bootstraps[0][1])
        self.assertEqual(4, control.write_bootstraps[0][2])
        self.assertEqual([0, 1], [entry[1] for entry in control.accepted_entries])
        self.assertEqual([0, 1], [metadata[1] for metadata in control.accepted_metadata])
        self.assertEqual([0, 1], [commit[1] for commit in control.entry_commits])
        self.assertEqual(1, len(control.transfer_commits))
        self.assertEqual("/outputs/run-1", control.transfer_commits[0][1])
        self.assertTrue(all(commit[2].startswith("/outputs/run-1/") for commit in control.entry_commits))
        self.assertEqual(2, len(bulk.written_frames))
        self.assertEqual([0, 1], [frame.entry_sequence for frame in bulk.written_frames])
        for frame, metadata in zip(bulk.written_frames, control.accepted_metadata):
            self.assertEqual(cxnfs_chunk_hash(frame.payload), metadata[5][0])
        expected_transfer_hash = cxnfs_transfer_hash(
            [commit[3] for commit in control.entry_commits],
            [commit[4] for commit in control.entry_commits],
        )
        self.assertEqual(expected_transfer_hash, control.transfer_commits[0][2])

    def test_bulk_payload_transport_rejects_send_remote_root_traversal(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport(tuple())
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "model.safetensors").write_bytes(b"checkpoint tensor bytes")
            client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(control, bulk))

            with self.assertRaisesRegex(ValueError, "parent"):
                client.send_safetensors_output(root, remote_root="/outputs/../run-1")

        self.assertEqual([], control.write_bootstraps)
        self.assertEqual([], bulk.written_frames)

    def test_bulk_payload_transport_sends_safetensors_output_frames_with_admitted_cord_window(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        bulk = BlockingWriteBulkFrameTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "model.safetensors").write_bytes(b"0123456789")
            client = CxnfsDatasetClient(CxnfsBulkPayloadTransport(
                control,
                bulk,
                CxnfsBulkReadConfig(initial_cords=2, max_cords=4, write_frame_payload_bytes=4),
            ))

            result = client.send_safetensors_output(root, remote_root="/outputs/run-1")

        self.assertEqual(1, result.file_count)
        self.assertGreaterEqual(bulk.max_active_writes, 2)
        ordered = sorted(bulk.written_frames, key=lambda frame: frame.chunk_sequence)
        self.assertEqual([0, 1, 2], [frame.chunk_sequence for frame in ordered])
        self.assertEqual([0, 1, 2], [frame.cord_index for frame in ordered])
        self.assertEqual([b"0123", b"4567", b"89"], [frame.payload for frame in ordered])
        self.assertEqual(1, len(control.accepted_metadata))
        self.assertEqual(tuple(cxnfs_chunk_hash(frame.payload) for frame in ordered), control.accepted_metadata[0][5])

    def test_bulk_payload_transport_sends_safetensors_output_grows_default_write_window(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        control.write_transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000013"),
            TransferDirection.WRITE,
            1,
            4,
            128 * 1024 * 1024,
            True,
        )
        bulk = BlockingWriteBulkFrameTransport()
        transport = CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(initial_cords=1, max_cords=4, write_frame_payload_bytes=4),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "model.safetensors").write_bytes(b"0123456789")
            client = CxnfsDatasetClient(transport)

            result = client.send_safetensors_output(root, remote_root="/outputs/run-1")

        self.assertEqual(1, result.file_count)
        self.assertGreaterEqual(bulk.max_active_writes, 2)
        ordered = sorted(bulk.written_frames, key=lambda frame: frame.chunk_sequence)
        self.assertEqual([0, 1, 2], [frame.chunk_sequence for frame in ordered])
        self.assertEqual([0, 1, 2], [frame.cord_index for frame in ordered])
        evidence = transport.write_path_evidence()
        self.assertEqual(1, evidence["last_accepted_initial_cords"])
        self.assertEqual(4, evidence["last_accepted_max_cords"])
        self.assertEqual(3, evidence["max_observed_write_cords"])
        self.assertEqual(1, evidence["metadata_after_payload_entries"])

    def test_bulk_payload_transport_writes_tensor_memory_with_payload_first_frames(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport(tuple())
        transport = CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(initial_cords=2, max_cords=4, write_frame_payload_bytes=8),
        )
        client = CxnfsPythonClient(transport)
        source = bytearray(b"checkpoint tensor bytes")
        result = client.write_tensor("/outputs/run-1/model.safetensors", source, len(source))

        self.assertEqual(1, result.file_count)
        self.assertEqual(1, result.tensor_file_count)
        self.assertEqual(len(source), result.bytes_sent)
        self.assertEqual(1, len(control.write_bootstraps))
        self.assertEqual("/outputs/run-1", control.write_bootstraps[0][0])
        self.assertEqual(2, control.write_bootstraps[0][1])
        self.assertEqual(4, control.write_bootstraps[0][2])
        self.assertEqual(3, len(bulk.written_frames))
        ordered_frames = sorted(bulk.written_frames, key=lambda frame: frame.chunk_sequence)
        self.assertEqual([0, 1, 2], [frame.cord_index for frame in ordered_frames])
        self.assertEqual([0, 1, 2], [frame.chunk_sequence for frame in ordered_frames])
        self.assertEqual([0, 8, 16], [frame.entry_offset for frame in ordered_frames])
        self.assertEqual([bytes(source[:8]), bytes(source[8:16]), bytes(source[16:])], [
            frame.payload for frame in ordered_frames
        ])
        self.assertEqual(1, len(control.accepted_entries))
        accepted_entry = control.accepted_entries[0]
        self.assertEqual(control.write_transfer.transfer_id, accepted_entry[0])
        self.assertEqual(0, accepted_entry[1])
        self.assertEqual(TransferEntryAction.CREATE_OR_REPLACE, accepted_entry[2])
        self.assertEqual("/outputs/run-1/model.safetensors", accepted_entry[3])
        self.assertEqual(len(source), accepted_entry[5])
        self.assertEqual(TransferPayloadFormat.SAFETENSORS, accepted_entry[6])
        self.assertEqual(1, len(control.accepted_metadata))
        accepted_metadata = control.accepted_metadata[0]
        expected_chunk_hashes = tuple(cxnfs_chunk_hash(frame.payload) for frame in ordered_frames)
        self.assertEqual(expected_chunk_hashes, accepted_metadata[5])
        file_hash = cxnfs_file_hash(
            list(expected_chunk_hashes),
            [len(frame.payload) for frame in ordered_frames],
        )
        transfer_hash = cxnfs_transfer_hash((file_hash,), (len(source),))
        self.assertEqual((control.write_transfer.transfer_id, 0, "/outputs/run-1/model.safetensors", file_hash, len(source)), control.entry_commits[0])
        self.assertEqual((control.write_transfer.transfer_id, "/outputs/run-1", transfer_hash, len(source)), control.transfer_commits[0])
        evidence = transport.write_path_evidence()
        self.assertEqual("cxnfs.bulk-write-path-evidence.v1", evidence["format"])
        self.assertEqual(1, evidence["write_operations"])
        self.assertEqual(1, evidence["direct_tensor_write_operations"])
        self.assertEqual(0, evidence["directory_output_write_operations"])
        self.assertEqual(len(source), evidence["payload_bytes_written"])
        self.assertEqual(3, evidence["write_frames"])
        self.assertEqual(0, evidence["write_acks"])
        self.assertEqual(0, evidence["write_ack_frame_verified"])
        self.assertFalse(evidence["write_ack_frame_identity_verified"])
        self.assertEqual(1, evidence["metadata_after_payload_entries"])
        self.assertEqual(1, evidence["entries_committed"])
        self.assertEqual(1, evidence["transfers_committed"])
        self.assertEqual(2, evidence["last_accepted_initial_cords"])
        self.assertEqual(4, evidence["last_accepted_max_cords"])
        self.assertEqual(3, evidence["max_observed_write_cords"])

    def test_bulk_payload_transport_writes_tensor_frames_with_admitted_cord_window(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        bulk = EvidenceBlockingWriteBulkFrameTransport()
        transport = CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(initial_cords=2, max_cords=4, write_frame_payload_bytes=4),
        )
        client = CxnfsPythonClient(transport)
        source = bytearray(b"0123456789")
        result = client.write_tensor("/outputs/run-1/model.safetensors", source, len(source))

        self.assertEqual(1, result.file_count)
        self.assertGreaterEqual(bulk.max_active_writes, 2)
        ordered = sorted(bulk.written_frames, key=lambda frame: frame.chunk_sequence)
        self.assertEqual([0, 1, 2], [frame.chunk_sequence for frame in ordered])
        self.assertEqual([0, 1, 2], [frame.cord_index for frame in ordered])
        self.assertEqual([bytes(source[:4]), bytes(source[4:8]), bytes(source[8:])], [frame.payload for frame in ordered])
        self.assertEqual(tuple(cxnfs_chunk_hash(frame.payload) for frame in ordered), control.accepted_metadata[0][5])
        evidence = transport.write_path_evidence()
        self.assertEqual(3, evidence["write_frames"])
        self.assertEqual(3, evidence["write_acks"])
        self.assertEqual(3, evidence["write_ack_frame_verified"])
        self.assertTrue(evidence["write_ack_frame_identity_verified"])
        self.assertTrue(evidence["write_ack_stream_completion_verified"])
        self.assertTrue(evidence["write_ack_stream_eof_verified"])
        self.assertEqual(0, evidence["write_ack_metadata_verified"])
        self.assertEqual(3, evidence["write_ack_metadata_pending"])
        self.assertEqual(3, evidence["write_ack_memory"])
        self.assertEqual("test.bulk-write-transport-evidence.v1", evidence["bulk_transport_write_evidence"]["format"])
        self.assertEqual(3, evidence["bulk_transport_write_evidence"]["write_streams_created"])
        self.assertGreaterEqual(evidence["bulk_transport_write_evidence"]["max_active_writes"], 2)

    def test_bulk_payload_transport_writes_tensor_grows_default_write_window(self):
        plan = empty_plan()
        control = FakeControlClient(plan)
        control.write_transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000014"),
            TransferDirection.WRITE,
            1,
            4,
            128 * 1024 * 1024,
            True,
        )
        bulk = EvidenceBlockingWriteBulkFrameTransport()
        transport = CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(initial_cords=1, max_cords=4, write_frame_payload_bytes=4),
        )
        client = CxnfsPythonClient(transport)
        source = bytearray(b"0123456789")

        result = client.write_tensor("/outputs/run-1/model.safetensors", source, len(source))

        self.assertEqual(1, result.file_count)
        self.assertGreaterEqual(bulk.max_active_writes, 2)
        ordered = sorted(bulk.written_frames, key=lambda frame: frame.chunk_sequence)
        self.assertEqual([0, 1, 2], [frame.chunk_sequence for frame in ordered])
        self.assertEqual([0, 1, 2], [frame.cord_index for frame in ordered])
        evidence = transport.write_path_evidence()
        self.assertEqual(1, evidence["last_accepted_initial_cords"])
        self.assertEqual(4, evidence["last_accepted_max_cords"])
        self.assertEqual(3, evidence["max_observed_write_cords"])
        self.assertEqual(3, evidence["bulk_transport_write_evidence"]["write_streams_created"])
        self.assertGreaterEqual(evidence["bulk_transport_write_evidence"]["max_active_writes"], 2)

    def test_bulk_payload_transport_reconnects_and_retries_tensor_write_frame(self):
        control = FakeControlClient(empty_plan())
        first = FailingWriteBulkFrameTransport("cord closed before ack")
        replacement = AckingBulkFrameTransport()
        reconnects = []

        def reconnect():
            reconnects.append("called")
            return replacement

        transport = CxnfsBulkPayloadTransport(
            control,
            first,
            CxnfsBulkReadConfig(initial_cords=1, max_cords=4, write_frame_payload_bytes=64),
            retry_config=CxnfsBulkRetryConfig(write_retries=1),
            bulk_transport_reconnector=reconnect,
        )
        client = CxnfsPythonClient(transport)
        source = bytearray(b"checkpoint")

        result = client.write_tensor("/outputs/run-1/model.safetensors", source, len(source))

        self.assertEqual(1, result.file_count)
        self.assertEqual(["called"], reconnects)
        self.assertEqual(1, first.close_calls)
        self.assertEqual(0, len(first.written_frames))
        self.assertEqual(1, len(replacement.written_frames))
        self.assertEqual(bytes(source), replacement.written_frames[0].payload)
        self.assertEqual(1, len(control.accepted_metadata))
        self.assertEqual(1, len(control.entry_commits))
        self.assertEqual(1, len(control.transfer_commits))
        evidence = transport.write_path_evidence()
        self.assertEqual(1, evidence["write_frames"])
        self.assertEqual(1, evidence["write_acks"])
        self.assertEqual(1, evidence["write_retry_attempts"])
        self.assertEqual(1, evidence["write_retry_successes"])
        self.assertEqual(1, evidence["bulk_transport_write_reconnects"])
        self.assertEqual("", evidence["last_write_retry_error"])
        self.assertEqual(1, evidence["bulk_transport_write_evidence"]["successful_write_operations"])

    def test_bulk_payload_transport_does_not_retry_conflicting_write_ack(self):
        control = FakeControlClient(empty_plan())
        bulk = BadAckBulkFrameTransport()
        reconnects = []
        transport = CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(initial_cords=1, max_cords=4, write_frame_payload_bytes=64),
            retry_config=CxnfsBulkRetryConfig(write_retries=1),
            bulk_transport_reconnector=lambda: reconnects.append("called"),
        )
        client = CxnfsPythonClient(transport)

        with self.assertRaisesRegex(ValueError, "ack chunk"):
            client.write_tensor("/outputs/run-1/model.safetensors", bytearray(b"checkpoint"), 10)

        self.assertEqual([], reconnects)
        self.assertEqual([], control.accepted_entries)
        self.assertEqual([], control.accepted_metadata)
        self.assertEqual([], control.entry_commits)
        evidence = transport.write_path_evidence()
        self.assertEqual(0, evidence["write_operations"])
        self.assertEqual(0, evidence["write_retry_attempts"])
        self.assertEqual(0, evidence["bulk_transport_write_reconnects"])
        self.assertEqual(1, evidence["bulk_transport_write_evidence"]["failed_write_operations"])

    def test_bulk_payload_transport_closes_bulk_transport_when_concurrent_write_fails(self):
        control = FakeControlClient(empty_plan())
        bulk = FailingPeerBlockingBulkFrameTransport()
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(
            control,
            bulk,
            CxnfsBulkReadConfig(initial_cords=2, max_cords=4, write_frame_payload_bytes=4),
        ))
        source = bytearray(b"01234567")
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(client.write_tensor, "/outputs/run-1/model.safetensors", source, len(source))

            with self.assertRaisesRegex(OSError, "cord failed"):
                future.result(timeout=1.0)

            self.assertTrue(bulk.closed.wait(timeout=0.5))
            self.assertEqual([], control.accepted_entries)
            self.assertEqual([], control.accepted_metadata)
            self.assertEqual([], control.entry_commits)
            self.assertEqual([], control.transfer_commits)
        finally:
            bulk.close()
            executor.shutdown(wait=True)

    def test_bulk_payload_transport_rejects_mismatched_write_entry_ack(self):
        control = BadWriteAckControlClient(empty_plan(), "entry")
        bulk = FakeBulkFrameTransport(tuple())
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "write entry acknowledgement"):
            client.write_tensor("/outputs/model.safetensors", bytearray(b"checkpoint"), 10)

        self.assertEqual([], control.accepted_metadata)
        self.assertEqual([], control.entry_commits)
        self.assertEqual([], control.transfer_commits)

    def test_bulk_payload_transport_rejects_mismatched_write_metadata_ack(self):
        control = BadWriteAckControlClient(empty_plan(), "metadata")
        bulk = FakeBulkFrameTransport(tuple())
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "write metadata acknowledgement"):
            client.write_tensor("/outputs/model.safetensors", bytearray(b"checkpoint"), 10)

        self.assertEqual([], control.entry_commits)
        self.assertEqual([], control.transfer_commits)

    def test_bulk_payload_transport_rejects_mismatched_write_entry_commit_ack(self):
        control = BadWriteAckControlClient(empty_plan(), "entry_commit")
        bulk = FakeBulkFrameTransport(tuple())
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "write entry commit acknowledgement"):
            client.write_tensor("/outputs/model.safetensors", bytearray(b"checkpoint"), 10)

        self.assertEqual([], control.transfer_commits)

    def test_bulk_payload_transport_rejects_mismatched_write_transfer_commit_ack(self):
        control = BadWriteAckControlClient(empty_plan(), "transfer_commit")
        bulk = FakeBulkFrameTransport(tuple())
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "write transfer commit acknowledgement"):
            client.write_tensor("/outputs/model.safetensors", bytearray(b"checkpoint"), 10)

        self.assertEqual(1, len(control.transfer_commits))

    def test_bulk_payload_transport_rejects_direct_write_path_traversal(self):
        control = FakeClosableControlClient(empty_plan())
        bulk = FakeBulkFrameTransport(tuple())
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "parent"):
            client.write_tensor("/outputs/../model.safetensors", bytearray(b"tensor"), 6)

        self.assertEqual([], control.write_bootstraps)
        self.assertEqual([], bulk.written_frames)

    def test_bulk_payload_transport_rejects_direct_write_short_source(self):
        control = FakeClosableControlClient(empty_plan())
        bulk = FakeBulkFrameTransport(tuple())
        client = CxnfsPythonClient(CxnfsBulkPayloadTransport(control, bulk))

        with self.assertRaisesRegex(ValueError, "source size"):
            client.write_tensor("/outputs/model.safetensors", bytearray(b"short"), 64)

        self.assertEqual([], control.write_bootstraps)
        self.assertEqual([], bulk.written_frames)

    def test_transfer_hash_supports_large_entry_sizes(self):
        entry_hash = b"\x03" * 32
        transfer_hash = cxnfs_transfer_hash((entry_hash,), (5 * 1024 * 1024 * 1024,))

        self.assertEqual(32, len(transfer_hash))

    def test_length_prefixed_bulk_client_reads_mds_shard_through_socket_transport(self):
        chunks = [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        plan = ReadPlan(
            b"\x09" * 32,
            26,
            ByteRange(5, 15),
            (
                PlannedChunkRead(0, cxnfs_chunk_hash(chunks[0]), 0, 8, 5, 3),
                PlannedChunkRead(1, cxnfs_chunk_hash(chunks[1]), 8, 8, 0, 8),
                PlannedChunkRead(2, cxnfs_chunk_hash(chunks[2]), 16, 8, 0, 4),
            ),
        )
        control = FakeControlClient(plan)
        frames = [
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunks[0]),
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 1, 8, True, chunks[1]),
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 2, 16, True, chunks[2]),
        ]
        observed_requests = []
        left, right = socket.socketpair()
        left.settimeout(2.0)
        right.settimeout(2.0)

        def serve():
            observed_requests.append(decode_bulk_read_request(read_encoded_bulk_read_request(left)))
            for frame in frames:
                write_encoded_bulk_frame(left, encode_bulk_frame(frame))

        server_thread = threading.Thread(target=serve)
        try:
            server_thread.start()
            bulk_client = CxnfsLengthPrefixedBulkClient(control, LengthPrefixedBulkFrameTransport(right))

            payload = bulk_client.read_mds_shard("/datasets/train/shard.00000.mds", 5, 15)

            server_thread.join(timeout=2.0)
            self.assertFalse(server_thread.is_alive())
            self.assertEqual(b"fghijklmnopqrst", payload)
            self.assertEqual(1, len(observed_requests))
            self.assertEqual(control.session_id, observed_requests[0].session_id)
            self.assertEqual(control.transfer.transfer_id, observed_requests[0].transfer_id)
            self.assertEqual("/datasets/train/shard.00000.mds", observed_requests[0].path)
            self.assertEqual(5, observed_requests[0].offset)
            self.assertEqual(15, observed_requests[0].length)
        finally:
            left.close()
            right.close()

    def test_length_prefixed_bulk_client_writes_bulk_frame_to_socket_transport(self):
        control = FakeControlClient(empty_plan())
        frame = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000099"),
            3,
            2,
            7,
            4096,
            True,
            b"checkpoint chunk",
        )
        observed = []
        left, right = socket.socketpair()
        left.settimeout(2.0)
        right.settimeout(2.0)

        def serve():
            observed.append(decode_bulk_frame(read_encoded_bulk_frame(left)))
            write_encoded_bulk_write_ack(left, encode_bulk_write_ack(BulkWriteAck(
                frame.transfer_id,
                frame.entry_sequence,
                frame.chunk_sequence,
                len(frame.payload),
                False,
                "MEMORY",
            )))

        server_thread = threading.Thread(target=serve)
        try:
            server_thread.start()
            bulk_client = CxnfsLengthPrefixedBulkClient(control, LengthPrefixedBulkFrameTransport(right))

            ack = bulk_client.write_frame(frame)

            server_thread.join(timeout=2.0)
            self.assertFalse(server_thread.is_alive())
            self.assertEqual([frame], observed)
            self.assertEqual(frame.transfer_id, ack.transfer_id)
            self.assertEqual(frame.entry_sequence, ack.entry_sequence)
            self.assertEqual(frame.chunk_sequence, ack.chunk_sequence)
            self.assertEqual(len(frame.payload), ack.payload_length)
        finally:
            left.close()
            right.close()

    def test_pooled_length_prefixed_bulk_transport_dispatches_writes_across_connections(self):
        read_transport = FakeClosableBulkTransport()
        barrier = threading.Barrier(2)
        tracker = ActiveWriteTracker()
        writers = (
            PooledBlockingWriterTransport(barrier, tracker),
            PooledBlockingWriterTransport(barrier, tracker),
        )
        pool = PooledLengthPrefixedBulkFrameTransport(read_transport, writers)
        frames = (
            BulkDataFrame(
                UUID("00000000-0000-0000-0000-000000000081"),
                0,
                0,
                0,
                0,
                True,
                b"first",
            ),
            BulkDataFrame(
                UUID("00000000-0000-0000-0000-000000000081"),
                1,
                0,
                1,
                5,
                True,
                b"second",
            ),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            acks = tuple(executor.map(pool.write_frame, frames))

        self.assertGreaterEqual(tracker.max_active_writes, 2)
        self.assertEqual([0, 1], sorted(ack.chunk_sequence for ack in acks))
        written = sorted(
            (frame.chunk_sequence for writer in writers for frame in writer.written_frames)
        )
        self.assertEqual([0, 1], written)
        pool.close()
        self.assertTrue(read_transport.closed)
        self.assertTrue(all(writer.closed for writer in writers))

    def test_pooled_length_prefixed_bulk_transport_dispatches_reads_across_connections(self):
        chunks = (b"abcdefgh", b"ijklmnop", b"qrstuvwx", b"yz012345")
        plan = ReadPlan(
            b"\x09" * 32,
            32,
            ByteRange(0, 32),
            tuple(
                PlannedChunkRead(index, cxnfs_chunk_hash(chunk), index * 8, 8, 0, 8)
                for index, chunk in enumerate(chunks)
            ),
        )
        transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000083"),
            TransferDirection.READ,
            4,
            64,
            128 * 1024 * 1024,
            False,
        )
        session_id = UUID("00000000-0000-0000-0000-000000000101")
        barrier = threading.Barrier(4)
        tracker = ActiveReadTracker()
        read_transport = PooledBlockingReadTransport(chunks, barrier, tracker)
        writers = tuple(PooledBlockingReadTransport(chunks, barrier, tracker) for _ in range(3))
        pool = PooledLengthPrefixedBulkFrameTransport(read_transport, writers)

        frames = list(pool.read_frames(
            session_id,
            transfer,
            "/datasets/train/shard.00000.mds",
            plan,
        ))

        self.assertEqual(4, pool.last_read_transport_count)
        self.assertGreaterEqual(tracker.max_active_reads, 4)
        self.assertEqual([0, 1, 2, 3], sorted(frame.chunk_sequence for frame in frames))
        self.assertTrue(all(frame.transfer_id == transfer.transfer_id for frame in frames))
        self.assertEqual(1, len(read_transport.plans))
        self.assertTrue(all(len(writer.plans) == 1 for writer in writers))
        self.assertEqual(
            [0, 1, 2, 3],
            sorted(
                transport.plans[0].chunks[0].chunk_index
                for transport in (read_transport, *writers)
            ),
        )

    def test_pooled_length_prefixed_bulk_transport_unblocks_waiting_writer_when_closed(self):
        read_transport = FakeClosableBulkTransport()
        writer = HoldWriteTransport()
        pool = PooledLengthPrefixedBulkFrameTransport(read_transport, (writer,))
        first = BulkDataFrame(
            UUID("00000000-0000-0000-0000-000000000082"),
            0,
            0,
            0,
            0,
            True,
            b"first",
        )
        second = BulkDataFrame(
            first.transfer_id,
            0,
            0,
            1,
            len(first.payload),
            True,
            b"second",
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_send = executor.submit(pool.write_frame, first)
            self.assertTrue(writer.started.wait(timeout=2.0))
            second_send = executor.submit(pool.write_frame, second)

            pool.close()
            with self.assertRaisesRegex(OSError, "closed"):
                second_send.result(timeout=2.0)
            writer.release()
            self.assertEqual(first.chunk_sequence, first_send.result(timeout=2.0).chunk_sequence)

        self.assertTrue(read_transport.closed)
        self.assertTrue(writer.closed)

    def test_length_prefixed_bulk_client_connect_uses_initial_cords_for_write_pool(self):
        control = FakeClosableControlClient(empty_plan())
        created = []

        def fake_connect(host, port, **kwargs):
            transport = FakeClosableBulkTransport()
            created.append((host, port, kwargs, transport))
            return transport

        with patch.object(LengthPrefixedBulkFrameTransport, "connect", side_effect=fake_connect):
            client = CxnfsLengthPrefixedBulkClient.connect(
                control,
                "cxnfs.example.internal",
                4510,
                read_config=CxnfsBulkReadConfig(initial_cords=3, max_cords=4),
                close_control_client=True,
            )

        self.assertIsInstance(client._bulk_transport, PooledLengthPrefixedBulkFrameTransport)
        self.assertEqual(4, len(created))
        self.assertTrue(all(call[0] == "cxnfs.example.internal" and call[1] == 4510 for call in created))
        client.close()
        self.assertTrue(control.closed)
        self.assertTrue(all(call[3].closed for call in created))

    def test_length_prefixed_bulk_client_exposes_direct_tensor_write(self):
        control = FakeControlClient(empty_plan())
        bulk = FakeBulkFrameTransport(tuple())
        bulk_client = CxnfsLengthPrefixedBulkClient(
            control,
            bulk,
            read_config=CxnfsBulkReadConfig(initial_cords=2, max_cords=4),
        )
        source = bytearray(b"checkpoint")

        result = bulk_client.write_tensor("/outputs/model.safetensors", source, len(source))

        self.assertEqual(1, result.file_count)
        self.assertEqual(len(source), result.bytes_sent)
        self.assertEqual("/outputs", control.write_bootstraps[0][0])
        self.assertEqual(1, len(bulk.written_frames))
        self.assertEqual(bytes(source), bulk.written_frames[0].payload)
        self.assertEqual("/outputs/model.safetensors", control.entry_commits[0][2])

    def test_length_prefixed_bulk_client_exposes_allocated_tensor_read(self):
        chunk = b"abcdefgh"
        plan = single_chunk_plan(chunk)
        control = FakeControlClient(plan)
        bulk = FakeBulkFrameTransport([
            BulkDataFrame(control.transfer.transfer_id, 0, 0, 0, 0, True, chunk),
        ])
        bulk_client = CxnfsLengthPrefixedBulkClient(control, bulk)
        fake_torch = FakeTorchModule()

        with FakeTorchInstall(fake_torch):
            tensor = bulk_client.read_tensor(
                "/datasets/train/shard.00000.mds",
                0,
                8,
                "uint8",
                (8,),
                target=TensorMemoryTarget.PYTORCH_CPU,
            )

        self.assertEqual(bytearray(b"abcdefgh"), tensor.data)
        self.assertFalse(tensor.is_pinned())
        self.assertEqual([((8,), "uint8", "cpu", False)], fake_torch.empty_calls)

    def test_length_prefixed_bulk_client_can_own_control_close(self):
        control = FakeClosableControlClient(empty_plan())
        bulk = FakeClosableBulkTransport()
        client = CxnfsLengthPrefixedBulkClient(control, bulk, close_control_client=True)

        client.close()

        self.assertTrue(control.closed)
        self.assertTrue(bulk.closed)


class FakeControlClient:
    def __init__(self, plan):
        self.plan = plan
        self.requests = []
        self.bootstraps = []
        self.write_bootstraps = []
        self.accepted_entries = []
        self.accepted_metadata = []
        self.entry_commits = []
        self.transfer_commits = []
        self.transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000010"),
            TransferDirection.READ,
            1,
            64,
            128 * 1024 * 1024,
            False,
        )
        self.write_transfer = TransferAcceptedResponse(
            UUID("00000000-0000-0000-0000-000000000011"),
            TransferDirection.WRITE,
            2,
            4,
            128 * 1024 * 1024,
            True,
        )
        self.session_id = UUID("00000000-0000-0000-0000-000000000101")

    def bootstrap_read_transfer(
        self,
        path,
        *,
        payload_format,
        initial_cords,
        max_cords,
        provisional_container_size_bytes,
        expected_size_limit_bytes,
    ):
        self.bootstraps.append((
            path,
            initial_cords,
            max_cords,
            provisional_container_size_bytes,
            expected_size_limit_bytes,
        ))
        return self.transfer

    def bootstrap_write_transfer(
        self,
        path,
        *,
        payload_format,
        initial_cords,
        max_cords,
        provisional_container_size_bytes,
        expected_size_limit_bytes,
    ):
        self.write_bootstraps.append((
            path,
            initial_cords,
            max_cords,
            provisional_container_size_bytes,
            expected_size_limit_bytes,
        ))
        return self.write_transfer

    def accept_write_entry(
        self,
        transfer_id,
        entry_sequence,
        action,
        path,
        *,
        target_path="",
        size_hint_bytes=0,
        payload_format=None,
    ):
        self.accepted_entries.append((
            transfer_id,
            entry_sequence,
            action,
            path,
            target_path,
            size_hint_bytes,
            payload_format,
        ))
        return TransferEntryAcceptedResponse(transfer_id, entry_sequence, path)

    def accept_write_metadata(
        self,
        transfer_id,
        entry_sequence,
        metadata_sequence,
        final_metadata,
        bytes_covered,
        chunk_hashes,
    ):
        self.accepted_metadata.append((
            transfer_id,
            entry_sequence,
            metadata_sequence,
            final_metadata,
            bytes_covered,
            chunk_hashes,
        ))
        return TransferMetadataAcceptedResponse(
            transfer_id,
            entry_sequence,
            metadata_sequence,
            len(chunk_hashes),
            len(chunk_hashes),
            final_metadata,
        )

    def commit_write_entry(self, transfer_id, entry_sequence, path, content_hash, bytes_written):
        self.entry_commits.append((transfer_id, entry_sequence, path, content_hash, bytes_written))
        return TransferEntryCommittedResponse(transfer_id, entry_sequence, path, content_hash, bytes_written)

    def commit_write_transfer(self, transfer_id, path, content_hash, bytes_written):
        self.transfer_commits.append((transfer_id, path, content_hash, bytes_written))
        return TransferCommittedResponse(transfer_id, content_hash, bytes_written)

    def read_file_range(self, path, offset, length):
        self.requests.append((path, offset, length))
        return ReadFileRangeResponse(path, self.plan)


class FakeClosableControlClient(FakeControlClient):
    def __init__(self, plan):
        super().__init__(plan)
        self.closed = False

    def close(self):
        self.closed = True


class BadReadResponseControlClient(FakeControlClient):
    def __init__(self, plan, response_path):
        super().__init__(plan)
        self.response_path = response_path

    def read_file_range(self, path, offset, length):
        self.requests.append((path, offset, length))
        return ReadFileRangeResponse(self.response_path, self.plan)


class BadWriteAckControlClient(FakeControlClient):
    def __init__(self, plan, mode):
        super().__init__(plan)
        self.mode = mode

    def accept_write_entry(
        self,
        transfer_id,
        entry_sequence,
        action,
        path,
        *,
        target_path="",
        size_hint_bytes=0,
        payload_format=None,
    ):
        super().accept_write_entry(
            transfer_id,
            entry_sequence,
            action,
            path,
            target_path=target_path,
            size_hint_bytes=size_hint_bytes,
            payload_format=payload_format,
        )
        if self.mode == "entry":
            return TransferEntryAcceptedResponse(transfer_id, entry_sequence, path + ".wrong")
        return TransferEntryAcceptedResponse(transfer_id, entry_sequence, path)

    def accept_write_metadata(
        self,
        transfer_id,
        entry_sequence,
        metadata_sequence,
        final_metadata,
        bytes_covered,
        chunk_hashes,
    ):
        super().accept_write_metadata(
            transfer_id,
            entry_sequence,
            metadata_sequence,
            final_metadata,
            bytes_covered,
            chunk_hashes,
        )
        if self.mode == "metadata":
            return TransferMetadataAcceptedResponse(
                transfer_id,
                entry_sequence,
                metadata_sequence,
                max(0, len(chunk_hashes) - 1),
                len(chunk_hashes),
                final_metadata,
            )
        return TransferMetadataAcceptedResponse(
            transfer_id,
            entry_sequence,
            metadata_sequence,
            len(chunk_hashes),
            len(chunk_hashes),
            final_metadata,
        )

    def commit_write_entry(self, transfer_id, entry_sequence, path, content_hash, bytes_written):
        super().commit_write_entry(transfer_id, entry_sequence, path, content_hash, bytes_written)
        if self.mode == "entry_commit":
            return TransferEntryCommittedResponse(transfer_id, entry_sequence, path, b"\x01" * 32, bytes_written)
        return TransferEntryCommittedResponse(transfer_id, entry_sequence, path, content_hash, bytes_written)

    def commit_write_transfer(self, transfer_id, path, content_hash, bytes_written):
        super().commit_write_transfer(transfer_id, path, content_hash, bytes_written)
        if self.mode == "transfer_commit":
            return TransferCommittedResponse(transfer_id, b"\x02" * 32, bytes_written)
        return TransferCommittedResponse(transfer_id, content_hash, bytes_written)


class FakeBulkFrameTransport:
    def __init__(self, frames):
        self.frames = tuple(frames)
        self.sessions = []
        self.transfers = []
        self.paths = []
        self.plans = []
        self.written_frames = []
        self.close_calls = 0

    def read_frames(self, session_id, transfer, path, plan):
        self.sessions.append(session_id)
        self.transfers.append(transfer)
        self.paths.append(path)
        self.plans.append(plan)
        return self.frames

    def write_frame(self, frame):
        self.written_frames.append(frame)

    def close(self):
        self.close_calls += 1


class AckingBulkFrameTransport(FakeBulkFrameTransport):
    def __init__(self):
        super().__init__(tuple())
        self.successful_write_operations = 0
        self.failed_write_operations = 0

    def write_frame(self, frame):
        self.written_frames.append(frame)
        self.successful_write_operations += 1
        return BulkWriteAck(
            frame.transfer_id,
            frame.entry_sequence,
            frame.chunk_sequence,
            len(frame.payload),
            False,
            "MEMORY",
        )

    def write_path_evidence(self):
        return {
            "format": "test.acking-bulk-write-transport-evidence.v1",
            "successful_write_operations": self.successful_write_operations,
            "failed_write_operations": self.failed_write_operations,
        }


class BadAckBulkFrameTransport(AckingBulkFrameTransport):
    def write_frame(self, frame):
        self.written_frames.append(frame)
        self.failed_write_operations += 1
        return BulkWriteAck(
            frame.transfer_id,
            frame.entry_sequence,
            frame.chunk_sequence + 1,
            len(frame.payload),
            False,
            "MEMORY",
        )


class FailingWriteBulkFrameTransport(FakeBulkFrameTransport):
    def __init__(self, message):
        super().__init__(tuple())
        self.message = message

    def write_frame(self, frame):
        raise OSError(self.message)


class CallbackOnlyBulkFrameTransport(FakeBulkFrameTransport):
    def __init__(self, frames):
        super().__init__(frames)
        self.frame_sink_calls = 0
        self.read_frames_calls = 0

    def read_frames(self, session_id, transfer, path, plan):
        self.read_frames_calls += 1
        raise AssertionError("direct buffer reads should use the transport frame sink")

    def read_frames_into(self, session_id, transfer, path, plan, accept):
        self.frame_sink_calls += 1
        self.sessions.append(session_id)
        self.transfers.append(transfer)
        self.paths.append(path)
        self.plans.append(plan)
        for frame in self.frames:
            accept(frame)
        return len(self.frames)


class FailingReadBulkFrameTransport(FakeBulkFrameTransport):
    def __init__(self, message):
        super().__init__(tuple())
        self.message = message
        self.close_calls = 0

    def read_frames(self, session_id, transfer, path, plan):
        self.sessions.append(session_id)
        self.transfers.append(transfer)
        self.paths.append(path)
        self.plans.append(plan)
        raise OSError(self.message)

    def close(self):
        self.close_calls += 1


class BarrierFailingReadBulkFrameTransport(FailingReadBulkFrameTransport):
    def __init__(self, message, parties):
        super().__init__(message)
        self._barrier = threading.Barrier(parties)

    def read_frames(self, session_id, transfer, path, plan):
        self.sessions.append(session_id)
        self.transfers.append(transfer)
        self.paths.append(path)
        self.plans.append(plan)
        self._barrier.wait(timeout=2.0)
        raise OSError(self.message)


class FailingCallbackBulkFrameTransport(CallbackOnlyBulkFrameTransport):
    def __init__(self, frames):
        super().__init__(frames)
        self.close_calls = 0

    def read_frames_into(self, session_id, transfer, path, plan, accept):
        self.frame_sink_calls += 1
        self.sessions.append(session_id)
        self.transfers.append(transfer)
        self.paths.append(path)
        self.plans.append(plan)
        for frame in self.frames:
            accept(frame)
        raise OSError("cord closed during direct receive")

    def close(self):
        self.close_calls += 1


class BlockingWriteBulkFrameTransport(FakeBulkFrameTransport):
    def __init__(self):
        super().__init__(tuple())
        self._barrier = threading.Barrier(2)
        self._lock = threading.Lock()
        self._active_writes = 0
        self.max_active_writes = 0

    def write_frame(self, frame):
        with self._lock:
            self._active_writes += 1
            self.max_active_writes = max(self.max_active_writes, self._active_writes)
        try:
            if frame.chunk_sequence < 2:
                self._barrier.wait(timeout=2.0)
            with self._lock:
                self.written_frames.append(frame)
            return BulkWriteAck(
                frame.transfer_id,
                frame.entry_sequence,
                frame.chunk_sequence,
                len(frame.payload),
                False,
                "MEMORY",
            )
        finally:
            with self._lock:
                self._active_writes -= 1


class EvidenceBlockingWriteBulkFrameTransport(BlockingWriteBulkFrameTransport):
    def write_path_evidence(self):
        with self._lock:
            return {
                "format": "test.bulk-write-transport-evidence.v1",
                "write_streams_created": len(self.written_frames),
                "write_ack_stream_completion_verified": True,
                "write_ack_stream_eof_verified": True,
                "max_active_writes": self.max_active_writes,
            }


class FailingPeerBlockingBulkFrameTransport(FakeBulkFrameTransport):
    def __init__(self):
        super().__init__(tuple())
        self.second_started = threading.Event()
        self.closed = threading.Event()
        self.close_calls = 0

    def write_frame(self, frame):
        if frame.chunk_sequence == 0:
            if not self.second_started.wait(timeout=2.0):
                raise TimeoutError("second write frame did not start")
            raise OSError("cord failed")
        if frame.chunk_sequence == 1:
            self.second_started.set()
            if not self.closed.wait(timeout=5.0):
                raise TimeoutError("bulk transport was not closed")
            raise OSError("transport closed")
        self.written_frames.append(frame)
        return BulkWriteAck(
            frame.transfer_id,
            frame.entry_sequence,
            frame.chunk_sequence,
            len(frame.payload),
            False,
            "MEMORY",
        )

    def close(self):
        self.close_calls += 1
        self.closed.set()


class ActiveWriteTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_writes = 0
        self.max_active_writes = 0

    def enter(self):
        with self.lock:
            self.active_writes += 1
            self.max_active_writes = max(self.max_active_writes, self.active_writes)

    def exit(self):
        with self.lock:
            self.active_writes -= 1


class ActiveReadTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_reads = 0
        self.max_active_reads = 0

    def enter(self):
        with self.lock:
            self.active_reads += 1
            self.max_active_reads = max(self.max_active_reads, self.active_reads)

    def exit(self):
        with self.lock:
            self.active_reads -= 1


class PooledBlockingReadTransport(FakeBulkFrameTransport):
    def __init__(self, chunks, barrier, tracker):
        super().__init__(tuple())
        self.chunks = tuple(chunks)
        self.barrier = barrier
        self.tracker = tracker
        self.closed = False

    def read_frames(self, session_id, transfer, path, plan):
        self.sessions.append(session_id)
        self.transfers.append(transfer)
        self.paths.append(path)
        self.plans.append(plan)
        self.tracker.enter()
        try:
            self.barrier.wait(timeout=2.0)
            return tuple(
                BulkDataFrame(
                    transfer.transfer_id,
                    chunk.chunk_index % transfer.accepted_initial_cords,
                    0,
                    chunk.chunk_index,
                    chunk.file_offset,
                    True,
                    self.chunks[chunk.chunk_index],
                )
                for chunk in plan.chunks
            )
        finally:
            self.tracker.exit()

    def close(self):
        self.closed = True


class PooledBlockingWriterTransport(FakeBulkFrameTransport):
    def __init__(self, barrier, tracker):
        super().__init__(tuple())
        self.barrier = barrier
        self.tracker = tracker
        self.closed = False

    def write_frame(self, frame):
        self.tracker.enter()
        try:
            self.barrier.wait(timeout=2.0)
            self.written_frames.append(frame)
            return BulkWriteAck(
                frame.transfer_id,
                frame.entry_sequence,
                frame.chunk_sequence,
                len(frame.payload),
                False,
                "MEMORY",
            )
        finally:
            self.tracker.exit()

    def close(self):
        self.closed = True


class HoldWriteTransport(FakeBulkFrameTransport):
    def __init__(self):
        super().__init__(tuple())
        self.started = threading.Event()
        self.released = threading.Event()
        self.closed = False

    def write_frame(self, frame):
        self.started.set()
        if not self.released.wait(timeout=2.0):
            raise TimeoutError("test writer was not released")
        self.written_frames.append(frame)
        return BulkWriteAck(
            frame.transfer_id,
            frame.entry_sequence,
            frame.chunk_sequence,
            len(frame.payload),
            False,
            "MEMORY",
        )

    def release(self):
        self.released.set()

    def close(self):
        self.closed = True


class FakeWritableTensor:
    def __init__(self, nbytes):
        self.data = bytearray(nbytes)
        self.device = "cpu"
        self.is_cuda = False

    def is_pinned(self):
        return True


class FakeTorchDtype:
    def __init__(self, name, nbytes):
        self.name = name
        self.nbytes = nbytes


class FakeTorchTensor:
    def __init__(self, nbytes, *, dtype, device, pin_memory, shape=None):
        self.data = bytearray(nbytes)
        self.dtype = dtype
        self.device = device
        self._pin_memory = pin_memory
        self.is_cuda = str(device).startswith("cuda")
        self.shape = tuple(shape) if shape is not None else (nbytes // dtype.nbytes,)
        self.copied_from = None
        self.non_blocking_copy = False

    def is_pinned(self):
        return self._pin_memory

    def to(self, device, *, non_blocking=False):
        copied = FakeTorchTensor(
            len(self.data),
            dtype=self.dtype,
            device=device,
            pin_memory=False,
            shape=self.shape,
        )
        copied.data[:] = self.data
        copied.copied_from = self
        copied.non_blocking_copy = non_blocking
        return copied

    def copy_(self, other, *, non_blocking=False):
        self.data[:] = other.data
        self.copied_from = other
        self.non_blocking_copy = non_blocking
        return self


class FakeTorchCuda:
    @staticmethod
    def is_available():
        return True


class FakeTorchModule:
    float32 = FakeTorchDtype("float32", 4)
    uint8 = FakeTorchDtype("uint8", 1)
    cuda = FakeTorchCuda()

    def __init__(self):
        self.empty_calls = []

    def empty(self, shape, *, dtype, device, pin_memory=False):
        nbytes = dtype.nbytes
        for dimension in shape:
            nbytes *= dimension
        self.empty_calls.append((tuple(shape), dtype.name, device, pin_memory))
        return FakeTorchTensor(nbytes, dtype=dtype, device=device, pin_memory=pin_memory, shape=shape)


class FakeTorchInstall:
    _missing = object()

    def __init__(self, torch_module):
        self.torch_module = torch_module
        self.previous = self._missing

    def __enter__(self):
        self.previous = sys.modules.get("torch", self._missing)
        sys.modules["torch"] = self.torch_module
        return self.torch_module

    def __exit__(self, exc_type, exc, traceback):
        if self.previous is self._missing:
            sys.modules.pop("torch", None)
        else:
            sys.modules["torch"] = self.previous


class FakeClosableBulkTransport(FakeBulkFrameTransport):
    def __init__(self):
        super().__init__(tuple())
        self.closed = False

    def close(self):
        self.closed = True


def run_async(coroutine):
    return asyncio.run(coroutine)


class FakeQuicBulkProtocol:
    def __init__(self, read_frames_for_request, write_ack=None, trailing_write_acks=(), missing_write_ack=False):
        self.read_frames_for_request = read_frames_for_request
        self.write_ack = write_ack
        self.trailing_write_acks = tuple(trailing_write_acks)
        self.missing_write_ack = bool(missing_write_ack)
        self.requests = []
        self.writes = []

    async def create_stream(self, is_unidirectional=False):
        if is_unidirectional:
            raise AssertionError("CXNFS bulk QUIC uses bidirectional streams")
        reader = FakeQuicStreamReader()
        writer = FakeQuicStreamWriter(lambda payload: self._accept_writer_payload(payload, reader))
        return reader, writer

    def _accept_writer_payload(self, payload, reader):
        if len(payload) < 4:
            raise AssertionError("missing length prefix")
        length = struct.unpack(">i", payload[:4])[0]
        encoded = payload[4:4 + length]
        if len(encoded) != length:
            raise AssertionError("short length-prefixed payload")
        try:
            request = decode_bulk_read_request(encoded)
        except ValueError:
            frame = decode_bulk_frame(encoded)
            self.writes.append(frame)
            if self.missing_write_ack:
                return
            ack = self.write_ack or BulkWriteAck(
                frame.transfer_id,
                frame.entry_sequence,
                frame.chunk_sequence,
                len(frame.payload),
                True,
                "MEMORY",
            )
            reader.feed(_prefixed(encode_bulk_write_ack(ack)))
            for trailing_ack in self.trailing_write_acks:
                reader.feed(_prefixed(encode_bulk_write_ack(trailing_ack)))
            return

        self.requests.append(request)
        for frame in self.read_frames_for_request(request):
            reader.feed(_prefixed(encode_bulk_frame(frame)))


class FailOneDelayOneQuicBulkProtocol:
    def __init__(self, transfer_id, chunks):
        self.transfer_id = transfer_id
        self.chunks = chunks
        self.requests = []
        self.delayed_read_started = threading.Event()
        self.delayed_read_cancelled = threading.Event()
        self.delayed_frame_delivered = threading.Event()
        self._second_reader_waiting = None
        self._delayed_release = None

    async def create_stream(self, is_unidirectional=False):
        if is_unidirectional:
            raise AssertionError("CXNFS bulk QUIC uses bidirectional streams")
        if self._second_reader_waiting is None:
            self._second_reader_waiting = asyncio.Event()
            self._delayed_release = asyncio.Event()
        reader = ScriptedQuicStreamReader()
        writer = FakeQuicStreamWriter(lambda payload: self._accept_writer_payload(payload, reader))
        return reader, writer

    def release_delayed_frame(self):
        if self._delayed_release is not None:
            self._delayed_release.set()

    def _accept_writer_payload(self, payload, reader):
        if len(payload) < 4:
            raise AssertionError("missing length prefix")
        length = struct.unpack(">i", payload[:4])[0]
        encoded = payload[4:4 + length]
        if len(encoded) != length:
            raise AssertionError("short length-prefixed payload")
        request = decode_bulk_read_request(encoded)
        self.requests.append(request)
        if request.offset == 0:
            reader.configure(
                _prefixed(encode_bulk_frame(BulkDataFrame(
                    self.transfer_id,
                    0,
                    0,
                    0,
                    0,
                    True,
                    b"ABCDEFGH",
                ))),
                release=self._second_reader_waiting,
            )
            return
        reader.configure(
            _prefixed(encode_bulk_frame(BulkDataFrame(
                self.transfer_id,
                1,
                0,
                1,
                8,
                True,
                self.chunks[1],
            ))),
            release=self._delayed_release,
            notify_waiting=self._second_reader_waiting,
            read_started=self.delayed_read_started,
            cancelled=self.delayed_read_cancelled,
            delivered=self.delayed_frame_delivered,
        )


class BlockingWriteQuicBulkProtocol(FakeQuicBulkProtocol):
    def __init__(self):
        super().__init__(lambda _request: tuple())
        self.barrier = threading.Barrier(2)

    def _accept_writer_payload(self, payload, reader):
        self.barrier.wait(timeout=2.0)
        super()._accept_writer_payload(payload, reader)


class FakeQuicStreamReader:
    def __init__(self):
        self._buffer = bytearray()

    def feed(self, data):
        self._buffer.extend(data)

    async def readexactly(self, nbytes):
        if len(self._buffer) < nbytes:
            raise EOFError("fake QUIC stream ended")
        result = bytes(self._buffer[:nbytes])
        del self._buffer[:nbytes]
        return result


class ScriptedQuicStreamReader:
    def __init__(self):
        self._buffer = bytearray()
        self._payload = None
        self._release = None
        self._notify_waiting = None
        self._read_started = None
        self._cancelled = None
        self._delivered = None
        self._configured = asyncio.Event()
        self._payload_delivered = False

    def configure(
        self,
        payload,
        *,
        release=None,
        notify_waiting=None,
        read_started=None,
        cancelled=None,
        delivered=None,
    ):
        self._payload = payload
        self._release = release
        self._notify_waiting = notify_waiting
        self._read_started = read_started
        self._cancelled = cancelled
        self._delivered = delivered
        self._configured.set()

    async def readexactly(self, nbytes):
        while len(self._buffer) < nbytes:
            if self._payload_delivered:
                raise EOFError("fake QUIC stream ended")
            await self._configured.wait()
            if self._release is not None:
                if self._read_started is not None:
                    self._read_started.set()
                if self._notify_waiting is not None:
                    self._notify_waiting.set()
                try:
                    await self._release.wait()
                except asyncio.CancelledError:
                    if self._cancelled is not None:
                        self._cancelled.set()
                    raise
            self._buffer.extend(self._payload or b"")
            self._payload_delivered = True
            if self._delivered is not None:
                self._delivered.set()
        result = bytes(self._buffer[:nbytes])
        del self._buffer[:nbytes]
        return result


class FakeQuicStreamWriter:
    def __init__(self, on_eof):
        self._on_eof = on_eof
        self._buffer = bytearray()
        self._closed = False

    def write(self, data):
        if self._closed:
            raise EOFError("fake QUIC stream writer is closed")
        self._buffer.extend(data)

    async def drain(self):
        return None

    def write_eof(self):
        if self._closed:
            return
        self._closed = True
        self._on_eof(bytes(self._buffer))


def _prefixed(payload):
    return struct.pack(">i", len(payload)) + payload


def empty_plan():
    return ReadPlan(
        b"\x09" * 32,
        0,
        ByteRange(0, 0),
        tuple(),
    )


def single_chunk_plan(chunk):
    return ReadPlan(
        b"\x09" * 32,
        len(chunk),
        ByteRange(0, len(chunk)),
        (PlannedChunkRead(0, cxnfs_chunk_hash(chunk), 0, len(chunk), 0, len(chunk)),),
    )


if __name__ == "__main__":
    unittest.main()
