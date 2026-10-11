from __future__ import annotations

import importlib.util
import os
import shutil
import socket
import ssl
import subprocess
import time
import unittest
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from cxnfs import (
    BulkDataFrame,
    ClientCredentials,
    CxnfsBufferRead,
    CxnfsBulkReadConfig,
    CxnfsBulkRetryConfig,
    CxnfsControlError,
    CxnfsControlClient,
    CxnfsPythonClient,
    CxnfsQuicBulkClient,
    CxnfsTensorRead,
    CxnfsTrainerIntegrationPlan,
    CxnfsWeightedTorchDataset,
    DatasetConverter,
    DatasetPreparer,
    QuicBulkFrameTransport,
    ResponseStatus,
    TRAINER_CURSOR_FORMAT,
    TRAINER_STREAM_STATE_FORMAT,
    TensorMemoryTarget,
    TransferEntryAction,
    TransferPayloadFormat,
    build_trainer_distributed_runtime_smoke_payload,
    build_trainer_handoff_acceptance_payload,
    build_trainer_handoff_payload,
    build_trainer_integration_smoke_payload,
    build_trainer_loop_replay_payload,
    build_trainer_network_preflight_payload,
    build_trainer_readiness_bundle_payload,
    build_trainer_runtime_smoke_payload,
    cxnfs_chunk_hash,
    cxnfs_file_hash,
    cxnfs_transfer_hash,
    run_trainer_contract_benchmark_cli,
    run_trainer_contract_cli,
    run_trainer_handoff_cli,
    run_trainer_loop_replay_cli,
    run_trainer_pipeline_smoke_cli,
    run_trainer_surface_matrix_cli,
    run_trainer_stream_smoke_cli,
    run_trainer_workload_benchmark_cli,
    weighted_mds_sources_from_root,
    write_trainer_distributed_runtime_rank_states,
)
from cxnfs.client_cli import run_client_cli


def _has_aioquic() -> bool:
    try:
        import aioquic  # noqa: F401
    except Exception:
        return False
    return True


def _assert_direct_destination_cord_fanout(testcase, evidence, min_cords: int = 2) -> None:
    testcase.assertTrue(evidence["transport_frame_sink_observed"])
    testcase.assertTrue(evidence["frame_sink_used_for_all_direct_destination_reads"])
    testcase.assertTrue(evidence["direct_destination_frame_sink_cord_fanout_verified"])
    testcase.assertGreaterEqual(evidence["direct_destination_max_observed_cords"], min_cords)
    testcase.assertGreaterEqual(evidence["direct_destination_parallel_frame_sink_read_operations"], 1)
    testcase.assertGreaterEqual(evidence["direct_destination_parallel_frame_sink_frames"], min_cords)


def _assert_quic_write_ack_stream_proven(testcase, evidence) -> None:
    testcase.assertTrue(evidence["write_ack_frame_identity_verified"])
    testcase.assertTrue(evidence["write_ack_stream_completion_verified"])
    testcase.assertTrue(evidence["write_ack_stream_eof_verified"])


def _assert_aggregated_quic_write_ack_stream_proven(testcase, evidence) -> None:
    testcase.assertTrue(evidence["quic_write_transport_all_reports_write_ack_frame_identity_verified"])
    testcase.assertTrue(evidence["quic_write_transport_all_reports_write_ack_stream_completion_verified"])
    testcase.assertTrue(evidence["quic_write_transport_all_reports_write_ack_stream_eof_verified"])


class PythonJavaQuicIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not _has_aioquic():
            _skip_or_fail("aioquic is required for the Python-to-Java QUIC smoke test")

    def test_python_quic_bulk_client_reads_from_java_server(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = b"abcdefghijklmnopqrstuvwxyz" * 64
            (dataset / "shard.00000.mds").write_bytes(payload)
            config = root / "server.properties"
            config.write_text("auth.username=trainer\nauth.password=secret\n", encoding="utf-8")
            password = root / "server.pass"
            password.write_text("changeit\n", encoding="utf-8")
            key_store = root / "server.p12"
            cert = root / "server.pem"
            ready = root / "server.ready"
            stop = root / "server.stop"
            log = root / "server.log"
            _generate_key_store(key_store, password)
            _export_certificate(key_store, password, cert)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        self.assertEqual(
                            "TLS_AES_128_GCM_SHA256",
                            control._transport._socket.cipher()[0],
                        )
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=5.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=4,
                                max_cords=4,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=len(payload),
                            ),
                            close_control_client=True,
                        ) as client:
                            destination = bytearray(128)
                            bytes_read = client.read_into_buffer(
                                "/datasets/train/shard.00000.mds",
                                16,
                                destination,
                                len(destination),
                            )
                    except Exception:
                        control.close()
                        raise

                self.assertEqual(128, bytes_read)
                self.assertEqual(payload[16:144], bytes(destination))
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_client_batch_direct_reads_from_java_server(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = _pattern_bytes(2 * 1024 * 1024)
            (dataset / "large.mds").write_bytes(payload)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                first = bytearray(512 * 1024)
                second = bytearray(512 * 1024)
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=10.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=10.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=4,
                                max_cords=4,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=len(payload),
                            ),
                            close_control_client=True,
                        ) as client:
                            counts = client.read_into_buffers(
                                (
                                    CxnfsBufferRead("/datasets/train/large.mds", 0, first, len(first)),
                                    CxnfsBufferRead(
                                        "/datasets/train/large.mds",
                                        1024 * 1024,
                                        second,
                                        len(second),
                                    ),
                                ),
                                max_workers=2,
                            )
                            read_evidence = client.read_path_evidence()
                    except Exception:
                        control.close()
                        raise

                self.assertEqual((len(first), len(second)), counts)
                self.assertEqual(payload[:len(first)], bytes(first))
                self.assertEqual(payload[1024 * 1024:1024 * 1024 + len(second)], bytes(second))
                self.assertEqual(2, read_evidence["read_operations"])
                self.assertEqual(2, read_evidence["direct_buffer_read_operations"])
                self.assertEqual(2, read_evidence["direct_destination_read_operations"])
                self.assertTrue(read_evidence["transport_frame_sink_observed"])
                self.assertTrue(read_evidence["frame_sink_used_for_all_direct_destination_reads"])
                self.assertTrue(read_evidence["direct_destination_frame_sink_cord_fanout_verified"])
                self.assertGreaterEqual(read_evidence["direct_destination_max_observed_cords"], 4)
                self.assertEqual(4, read_evidence["last_accepted_initial_cords"])
                self.assertEqual(4, read_evidence["last_accepted_max_cords"])
                self.assertEqual(len(first) + len(second), read_evidence["payload_bytes_read"])
                bulk_evidence = read_evidence["bulk_transport_read_evidence"]
                self.assertEqual("cxnfs.quic-bulk-transport-evidence.v1", bulk_evidence["format"])
                self.assertEqual("TLSv1.3", bulk_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_evidence["cipher_suite"])
                self.assertEqual(2, bulk_evidence["successful_read_operations"])
                self.assertEqual(0, bulk_evidence["failed_read_operations"])
                self.assertGreaterEqual(bulk_evidence["read_streams_created"], 8)
                self.assertTrue(bulk_evidence["read_stream_completion_verified"])
                self.assertTrue(bulk_evidence["read_stream_eof_verified"])
                self.assertTrue(bulk_evidence["connection_reused_for_streams"])
                report = root / "batch-direct-read.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-batch-direct-read.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "tls_version": bulk_evidence["tls_version"],
                    "cipher_suite": bulk_evidence["cipher_suite"],
                    "batch_read_count": len(counts),
                    "batch_worker_count": 2,
                    "ordered_batch_read_verified": (
                        bytes(first) == payload[:len(first)]
                        and bytes(second) == payload[1024 * 1024:1024 * 1024 + len(second)]
                    ),
                    "bytes_read": sum(counts),
                    "read_operations": read_evidence["read_operations"],
                    "direct_buffer_read_operations": read_evidence["direct_buffer_read_operations"],
                    "direct_destination_read_operations": read_evidence["direct_destination_read_operations"],
                    "payload_bytes_read": read_evidence["payload_bytes_read"],
                    "transport_frame_sink_observed": read_evidence["transport_frame_sink_observed"],
                    "max_observed_cords": read_evidence["max_observed_cords"],
                    "direct_destination_max_observed_cords": read_evidence[
                        "direct_destination_max_observed_cords"
                    ],
                    "direct_destination_parallel_frame_sink_read_operations": read_evidence[
                        "direct_destination_parallel_frame_sink_read_operations"
                    ],
                    "direct_destination_frame_sink_cord_fanout_verified": read_evidence[
                        "direct_destination_frame_sink_cord_fanout_verified"
                    ],
                    "frame_sink_used_for_all_direct_destination_reads": read_evidence[
                        "frame_sink_used_for_all_direct_destination_reads"
                    ],
                    "last_accepted_initial_cords": read_evidence["last_accepted_initial_cords"],
                    "last_accepted_max_cords": read_evidence["last_accepted_max_cords"],
                    "observed_cords_within_admission": read_evidence["observed_cords_within_admission"],
                    "bulk_transport_read_evidence": bulk_evidence,
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-batch-direct-read.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_client_reads_large_mds_after_java_server_restart(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = _pattern_bytes(3 * 1024 * 1024)
            shard_path = "/datasets/train/restart-large.mds"
            (dataset / "restart-large.mds").write_bytes(payload)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            first_control_port = _free_tcp_port()
            first_transport_port = _free_udp_port()
            second_ready = root / "server-restart.ready"
            second_stop = root / "server-restart.stop"
            second_log = root / "server-restart.log"
            second_control_port = _free_tcp_port()
            second_transport_port = _free_udp_port()

            def read_two_large_ranges(control_port: int, transport_port: int, phase: str):
                first = bytearray(768 * 1024)
                second = bytearray(768 * 1024)
                tls_context = _server_verified_tls13_context(str(cert))
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=10.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        control_cipher = control._transport._socket.cipher()[0]
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=10.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=4,
                                max_cords=4,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=len(payload),
                            ),
                            close_control_client=True,
                        ) as client:
                            counts = client.read_into_buffers(
                                (
                                    CxnfsBufferRead(shard_path, 0, first, len(first)),
                                    CxnfsBufferRead(shard_path, 2 * 1024 * 1024, second, len(second)),
                                ),
                                max_workers=2,
                            )
                            read_evidence = client.read_path_evidence()
                    except Exception:
                        control.close()
                        raise

                expected_first = payload[:len(first)]
                expected_second = payload[2 * 1024 * 1024:2 * 1024 * 1024 + len(second)]
                expected_hash = cxnfs_file_hash(
                    (cxnfs_chunk_hash(expected_first), cxnfs_chunk_hash(expected_second)),
                    (len(expected_first), len(expected_second)),
                ).hex()
                actual_hash = cxnfs_file_hash(
                    (cxnfs_chunk_hash(first), cxnfs_chunk_hash(second)),
                    (len(first), len(second)),
                ).hex()
                return {
                    "phase": phase,
                    "counts": counts,
                    "first": bytes(first),
                    "second": bytes(second),
                    "expected_first": expected_first,
                    "expected_second": expected_second,
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                    "control_cipher_suite": control_cipher,
                    "read_evidence": read_evidence,
                    "bulk_evidence": read_evidence["bulk_transport_read_evidence"],
                }

            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                first_control_port,
                first_transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                first_result = read_two_large_ranges(first_control_port, first_transport_port, "before-restart")
            finally:
                _stop_server(process, stop)

            restarted = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                second_ready,
                second_stop,
                second_log,
                second_control_port,
                second_transport_port,
            )
            try:
                _wait_for_ready(second_ready, restarted, second_log)
                second_result = read_two_large_ranges(second_control_port, second_transport_port, "after-restart")
            finally:
                _stop_server(restarted, second_stop)

            for result in (first_result, second_result):
                read_evidence = result["read_evidence"]
                bulk_evidence = result["bulk_evidence"]
                self.assertEqual((768 * 1024, 768 * 1024), result["counts"])
                self.assertEqual(result["expected_first"], result["first"])
                self.assertEqual(result["expected_second"], result["second"])
                self.assertEqual(result["expected_hash"], result["actual_hash"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", result["control_cipher_suite"])
                self.assertEqual(2, read_evidence["read_operations"])
                self.assertEqual(2, read_evidence["direct_buffer_read_operations"])
                self.assertEqual(2, read_evidence["direct_destination_read_operations"])
                self.assertEqual(4, read_evidence["last_accepted_initial_cords"])
                self.assertEqual(4, read_evidence["last_accepted_max_cords"])
                self.assertGreaterEqual(read_evidence["max_observed_cords"], 4)
                self.assertTrue(read_evidence["transport_frame_sink_observed"])
                self.assertTrue(read_evidence["direct_destination_frame_sink_cord_fanout_verified"])
                self.assertEqual("TLSv1.3", bulk_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_evidence["cipher_suite"])
                self.assertEqual(2, bulk_evidence["successful_read_operations"])
                self.assertEqual(0, bulk_evidence["failed_read_operations"])
                self.assertTrue(bulk_evidence["read_stream_completion_verified"])
                self.assertTrue(bulk_evidence["read_stream_eof_verified"])
                self.assertTrue(bulk_evidence["connection_reused_for_streams"])

            report = root / "restart-read.json"
            report.write_text(json.dumps({
                "format": "cxnfs.quic-restart-read.v1",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "server_restart_observed": True,
                "same_data_root_reused": True,
                "auth_phases": 2,
                "read_phases": [
                    {
                        "phase": result["phase"],
                        "control_cipher_suite": result["control_cipher_suite"],
                        "tls_version": result["bulk_evidence"]["tls_version"],
                        "cipher_suite": result["bulk_evidence"]["cipher_suite"],
                        "byte_size": sum(result["counts"]),
                        "read_count": len(result["counts"]),
                        "ordered_batch_read_verified": (
                            result["first"] == result["expected_first"]
                            and result["second"] == result["expected_second"]
                        ),
                        "hash_verified": result["expected_hash"] == result["actual_hash"],
                        "expected_hash": result["expected_hash"],
                        "actual_hash": result["actual_hash"],
                        "max_observed_cords": result["read_evidence"]["max_observed_cords"],
                        "direct_destination_max_observed_cords": result["read_evidence"][
                            "direct_destination_max_observed_cords"
                        ],
                        "transport_frame_sink_observed": result["read_evidence"][
                            "transport_frame_sink_observed"
                        ],
                        "direct_destination_frame_sink_cord_fanout_verified": result["read_evidence"][
                            "direct_destination_frame_sink_cord_fanout_verified"
                        ],
                        "read_stream_completion_verified": result["bulk_evidence"][
                            "read_stream_completion_verified"
                        ],
                        "read_stream_eof_verified": result["bulk_evidence"]["read_stream_eof_verified"],
                        "bulk_transport_read_evidence": result["bulk_evidence"],
                    }
                    for result in (first_result, second_result)
                ],
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            _publish_report(report, "python-quic-restart-read.json")

    @unittest.skipUnless(
        importlib.util.find_spec("torch"),
        "torch is required for Python-to-Java QUIC direct tensor reads",
    )
    def test_python_quic_bulk_client_batch_direct_tensor_reads_from_java_server(self):
        import torch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = _pattern_bytes(2 * 1024 * 1024)
            (dataset / "large.mds").write_bytes(payload)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                first = torch.empty((512 * 1024,), dtype=torch.uint8, device="cpu")
                second = torch.empty((512 * 1024,), dtype=torch.uint8, device="cpu")
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=10.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=10.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=4,
                                max_cords=4,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=len(payload),
                            ),
                            close_control_client=True,
                        ) as client:
                            counts = client.read_into_tensors(
                                (
                                    CxnfsTensorRead(
                                        "/datasets/train/large.mds",
                                        0,
                                        first,
                                        first.numel(),
                                        TensorMemoryTarget.PYTORCH_CPU,
                                        "cpu",
                                        True,
                                    ),
                                    CxnfsTensorRead(
                                        "/datasets/train/large.mds",
                                        1024 * 1024,
                                        second,
                                        second.numel(),
                                        TensorMemoryTarget.PYTORCH_CPU,
                                        "cpu",
                                        True,
                                    ),
                                ),
                                max_workers=2,
                            )
                            read_evidence = client.read_path_evidence()
                    except Exception:
                        control.close()
                        raise

                first_bytes = first.cpu().numpy().tobytes()
                second_bytes = second.cpu().numpy().tobytes()
                self.assertEqual((first.numel(), second.numel()), counts)
                self.assertEqual(payload[:first.numel()], first_bytes)
                self.assertEqual(payload[1024 * 1024:1024 * 1024 + second.numel()], second_bytes)
                self.assertEqual(2, read_evidence["read_operations"])
                self.assertEqual(0, read_evidence["direct_buffer_read_operations"])
                self.assertEqual(2, read_evidence["direct_tensor_read_operations"])
                self.assertEqual(2, read_evidence["direct_destination_read_operations"])
                self.assertTrue(read_evidence["transport_frame_sink_observed"])
                self.assertTrue(read_evidence["frame_sink_used_for_all_direct_destination_reads"])
                self.assertTrue(read_evidence["direct_destination_frame_sink_cord_fanout_verified"])
                self.assertGreaterEqual(read_evidence["direct_destination_max_observed_cords"], 4)
                self.assertEqual(4, read_evidence["last_accepted_initial_cords"])
                self.assertEqual(4, read_evidence["last_accepted_max_cords"])
                self.assertEqual(first.numel() + second.numel(), read_evidence["payload_bytes_read"])
                bulk_evidence = read_evidence["bulk_transport_read_evidence"]
                self.assertEqual("cxnfs.quic-bulk-transport-evidence.v1", bulk_evidence["format"])
                self.assertEqual("TLSv1.3", bulk_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_evidence["cipher_suite"])
                self.assertEqual(2, bulk_evidence["successful_read_operations"])
                self.assertEqual(0, bulk_evidence["failed_read_operations"])
                self.assertGreaterEqual(bulk_evidence["read_streams_created"], 8)
                self.assertTrue(bulk_evidence["read_stream_completion_verified"])
                self.assertTrue(bulk_evidence["read_stream_eof_verified"])
                self.assertTrue(bulk_evidence["connection_reused_for_streams"])
                report = root / "batch-direct-tensor-read.json"
                tensor_memory_path = {
                    "format": "cxnfs.trainer-tensor-memory-path.v1",
                    "complete": True,
                    "target": "pytorch-cpu",
                    "dtype": "uint8",
                    "device": "cpu",
                    "python_byte_batch_avoided": True,
                    "network_fills_tensor_destination": True,
                    "device_copy_required": False,
                }
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-batch-direct-tensor-read.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "tls_version": bulk_evidence["tls_version"],
                    "cipher_suite": bulk_evidence["cipher_suite"],
                    "tensor_target": "pytorch-cpu",
                    "tensor_dtype": "uint8",
                    "tensor_device": "cpu",
                    "tensor_read_count": len(counts),
                    "tensor_worker_count": 2,
                    "ordered_tensor_read_verified": (
                        first_bytes == payload[:first.numel()]
                        and second_bytes == payload[1024 * 1024:1024 * 1024 + second.numel()]
                    ),
                    "bytes_read": sum(counts),
                    "read_operations": read_evidence["read_operations"],
                    "direct_buffer_read_operations": read_evidence["direct_buffer_read_operations"],
                    "direct_tensor_read_operations": read_evidence["direct_tensor_read_operations"],
                    "direct_destination_read_operations": read_evidence["direct_destination_read_operations"],
                    "payload_bytes_read": read_evidence["payload_bytes_read"],
                    "transport_frame_sink_observed": read_evidence["transport_frame_sink_observed"],
                    "max_observed_cords": read_evidence["max_observed_cords"],
                    "direct_destination_max_observed_cords": read_evidence[
                        "direct_destination_max_observed_cords"
                    ],
                    "direct_destination_parallel_frame_sink_read_operations": read_evidence[
                        "direct_destination_parallel_frame_sink_read_operations"
                    ],
                    "direct_destination_frame_sink_cord_fanout_verified": read_evidence[
                        "direct_destination_frame_sink_cord_fanout_verified"
                    ],
                    "frame_sink_used_for_all_direct_destination_reads": read_evidence[
                        "frame_sink_used_for_all_direct_destination_reads"
                    ],
                    "last_accepted_initial_cords": read_evidence["last_accepted_initial_cords"],
                    "last_accepted_max_cords": read_evidence["last_accepted_max_cords"],
                    "observed_cords_within_admission": read_evidence["observed_cords_within_admission"],
                    "tensor_memory_path": tensor_memory_path,
                    "bulk_transport_read_evidence": bulk_evidence,
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-batch-direct-tensor-read.json")
            finally:
                _stop_server(process, stop)

    def test_python_client_cli_lists_reads_and_sends_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = _pattern_bytes(768 * 1024)
            (dataset / "shard.00000.mds").write_bytes(payload)
            checkpoint = root / "checkpoint"
            checkpoint.mkdir()
            checkpoint_payload = _pattern_bytes(16 * 1024)
            (checkpoint / "model.safetensors").write_bytes(checkpoint_payload)
            direct_write_payload = _pattern_bytes(20 * 1024)
            direct_write_source = root / "direct-model.safetensors"
            direct_write_source.write_bytes(direct_write_payload)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            health = root / "server.health"
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
                health_file=health,
            )
            try:
                _wait_for_ready(ready, process, log)
                base_args = [
                    "--host", "localhost",
                    "--control-port", str(control_port),
                    "--transport-port", str(transport_port),
                    "--bulk-transport", "quic",
                    "--cafile", str(cert),
                    "--server-name", "localhost",
                    "--timeout", "10",
                    "--initial-cords", "4",
                    "--max-cords", "4",
                    "--write-frame-payload-bytes", "4096",
                ]

                list_out, list_err = io.StringIO(), io.StringIO()
                list_exit = run_client_cli(
                    [*base_args, "list", "/datasets/train"],
                    out=list_out,
                    err=list_err,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )
                self.assertEqual(0, list_exit, list_err.getvalue())
                self.assertNotIn("secret", list_out.getvalue())
                self.assertNotIn("secret", list_err.getvalue())
                list_payload = json.loads(list_out.getvalue())
                self.assertEqual("cxnfs.python-client-result.v1", list_payload["format"])
                self.assertEqual("list", list_payload["command"])
                self.assertEqual("quic", list_payload["bulk_transport"])
                self.assertEqual("/datasets/train", list_payload["listing"]["path"])
                self.assertIn("shard.00000.mds", {entry["name"] for entry in list_payload["listing"]["entries"]})

                read_output = root / "cli-read.bin"
                read_out, read_err = io.StringIO(), io.StringIO()
                read_exit = run_client_cli(
                    [
                        *base_args,
                        "read-mds",
                        "/datasets/train/shard.00000.mds",
                        "--offset", "1024",
                        "--length", str(512 * 1024),
                        "--output", str(read_output),
                    ],
                    out=read_out,
                    err=read_err,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )
                self.assertEqual(0, read_exit, read_err.getvalue())
                self.assertNotIn("secret", read_out.getvalue())
                self.assertNotIn("secret", read_err.getvalue())
                self.assertEqual(payload[1024:1024 + 512 * 1024], read_output.read_bytes())
                read_payload = json.loads(read_out.getvalue())
                self.assertEqual("read-mds", read_payload["command"])
                self.assertEqual(512 * 1024, read_payload["bytes_read"])
                self.assertEqual("QuicBulkFrameTransport", read_payload["read_path_evidence"]["bulk_transport_class"])
                self.assertEqual(4, read_payload["read_path_evidence"]["max_observed_cords"])
                self.assertEqual(
                    "TLS_AES_128_GCM_SHA256",
                    read_payload["read_path_evidence"]["bulk_transport_read_evidence"]["cipher_suite"],
                )

                send_out, send_err = io.StringIO(), io.StringIO()
                send_exit = run_client_cli(
                    [
                        *base_args,
                        "send-checkpoint",
                        str(checkpoint),
                        "--remote-root", "/outputs/cli-run",
                    ],
                    out=send_out,
                    err=send_err,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )
                self.assertEqual(0, send_exit, send_err.getvalue())
                self.assertNotIn("secret", send_out.getvalue())
                self.assertNotIn("secret", send_err.getvalue())
                send_payload = json.loads(send_out.getvalue())
                self.assertEqual("send-checkpoint", send_payload["command"])
                self.assertEqual(1, send_payload["result"]["file_count"])
                self.assertEqual(1, send_payload["result"]["tensor_file_count"])
                self.assertEqual(len(checkpoint_payload), send_payload["result"]["bytes_sent"])
                self.assertEqual(4, send_payload["write_path_evidence"]["max_observed_write_cords"])
                _assert_quic_write_ack_stream_proven(self, send_payload["write_path_evidence"])
                self.assertGreaterEqual(
                    send_payload["write_path_evidence"]["bulk_transport_write_evidence"]["max_active_write_streams"],
                    2,
                )
                _assert_quic_write_ack_stream_proven(
                    self,
                    send_payload["write_path_evidence"]["bulk_transport_write_evidence"],
                )
                self.assertEqual(
                    "TLS_AES_128_GCM_SHA256",
                    send_payload["write_path_evidence"]["bulk_transport_write_evidence"]["cipher_suite"],
                )

                direct_send_out, direct_send_err = io.StringIO(), io.StringIO()
                direct_send_exit = run_client_cli(
                    [
                        *base_args,
                        "send-file",
                        str(direct_write_source),
                        "--remote-path", "/outputs/cli-run/direct-model.safetensors",
                    ],
                    out=direct_send_out,
                    err=direct_send_err,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )
                self.assertEqual(0, direct_send_exit, direct_send_err.getvalue())
                self.assertNotIn("secret", direct_send_out.getvalue())
                self.assertNotIn("secret", direct_send_err.getvalue())
                direct_send_payload = json.loads(direct_send_out.getvalue())
                self.assertEqual("send-file", direct_send_payload["command"])
                self.assertEqual(len(direct_write_payload), direct_send_payload["result"]["bytes_sent"])
                self.assertEqual(1, direct_send_payload["write_path_evidence"]["direct_tensor_write_operations"])
                self.assertEqual(4, direct_send_payload["write_path_evidence"]["max_observed_write_cords"])
                _assert_quic_write_ack_stream_proven(self, direct_send_payload["write_path_evidence"])
                _assert_quic_write_ack_stream_proven(
                    self,
                    direct_send_payload["write_path_evidence"]["bulk_transport_write_evidence"],
                )
                self.assertEqual(
                    "TLS_AES_128_GCM_SHA256",
                    direct_send_payload["write_path_evidence"]["bulk_transport_write_evidence"]["cipher_suite"],
                )

                read_back_output = root / "cli-read-back.bin"
                read_back_out, read_back_err = io.StringIO(), io.StringIO()
                read_back_exit = run_client_cli(
                    [
                        *base_args,
                        "read-mds",
                        "/outputs/cli-run/model.safetensors",
                        "--offset", "0",
                        "--length", str(len(checkpoint_payload)),
                        "--output", str(read_back_output),
                    ],
                    out=read_back_out,
                    err=read_back_err,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )
                self.assertEqual(0, read_back_exit, read_back_err.getvalue())
                self.assertNotIn("secret", read_back_out.getvalue())
                self.assertNotIn("secret", read_back_err.getvalue())
                self.assertEqual(checkpoint_payload, read_back_output.read_bytes())

                direct_read_back_output = root / "cli-direct-read-back.bin"
                direct_read_back_out, direct_read_back_err = io.StringIO(), io.StringIO()
                direct_read_back_exit = run_client_cli(
                    [
                        *base_args,
                        "read-mds",
                        "/outputs/cli-run/direct-model.safetensors",
                        "--offset", "0",
                        "--length", str(len(direct_write_payload)),
                        "--output", str(direct_read_back_output),
                    ],
                    out=direct_read_back_out,
                    err=direct_read_back_err,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )
                self.assertEqual(0, direct_read_back_exit, direct_read_back_err.getvalue())
                self.assertNotIn("secret", direct_read_back_out.getvalue())
                self.assertNotIn("secret", direct_read_back_err.getvalue())
                self.assertEqual(direct_write_payload, direct_read_back_output.read_bytes())

                server_health = _wait_for_health_field_at_least(
                    health,
                    "bulk_total_streams",
                    3,
                    process,
                    log,
                )
                server_stream_count = _health_int(server_health, "bulk_total_streams")
                self.assertTrue(server_health.get("bulk_read_listener", "").startswith("quic-tls13-udp://"))
                self.assertEqual("tls_aes_128_gcm_sha256", server_health.get("bulk_cipher_suite"))
                self.assertGreaterEqual(server_stream_count, 3)

                report = root / "python-client-cli-quic.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.python-client-cli-quic-smoke.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "list_entries": len(list_payload["listing"]["entries"]),
                    "read_bytes": read_payload["bytes_read"],
                    "read_max_observed_cords": read_payload["read_path_evidence"]["max_observed_cords"],
                    "read_cipher_suite": read_payload["read_path_evidence"]["bulk_transport_read_evidence"]["cipher_suite"],
                    "checkpoint_bytes_sent": send_payload["result"]["bytes_sent"],
                    "write_max_observed_cords": send_payload["write_path_evidence"]["max_observed_write_cords"],
                    "write_max_active_streams": send_payload["write_path_evidence"]["bulk_transport_write_evidence"][
                        "max_active_write_streams"
                    ],
                    "write_cipher_suite": send_payload["write_path_evidence"]["bulk_transport_write_evidence"][
                        "cipher_suite"
                    ],
                    "write_ack_stream_completion_verified": send_payload["write_path_evidence"][
                        "write_ack_stream_completion_verified"
                    ],
                    "write_ack_stream_eof_verified": send_payload["write_path_evidence"][
                        "write_ack_stream_eof_verified"
                    ],
                    "direct_file_bytes_sent": direct_send_payload["result"]["bytes_sent"],
                    "direct_write_max_observed_cords": direct_send_payload["write_path_evidence"][
                        "max_observed_write_cords"
                    ],
                    "direct_write_cipher_suite": direct_send_payload["write_path_evidence"][
                        "bulk_transport_write_evidence"
                    ]["cipher_suite"],
                    "direct_write_ack_stream_completion_verified": direct_send_payload["write_path_evidence"][
                        "write_ack_stream_completion_verified"
                    ],
                    "direct_write_ack_stream_eof_verified": direct_send_payload["write_path_evidence"][
                        "write_ack_stream_eof_verified"
                    ],
                    "server_bulk_listener": server_health.get("bulk_read_listener"),
                    "server_bulk_cipher_suite": server_health.get("bulk_cipher_suite"),
                    "server_bulk_total_streams": server_stream_count,
                    "server_bulk_total_streams_observed": server_stream_count >= 3,
                    "direct_file_write_verified": direct_read_back_output.read_bytes() == direct_write_payload,
                    "read_back_verified": read_back_output.read_bytes() == checkpoint_payload,
                    "raw_password_visible": (
                        "secret" in list_out.getvalue()
                        or "secret" in list_err.getvalue()
                        or "secret" in read_out.getvalue()
                        or "secret" in read_err.getvalue()
                        or "secret" in send_out.getvalue()
                        or "secret" in send_err.getvalue()
                        or "secret" in direct_send_out.getvalue()
                        or "secret" in direct_send_err.getvalue()
                        or "secret" in read_back_out.getvalue()
                        or "secret" in read_back_err.getvalue()
                        or "secret" in direct_read_back_out.getvalue()
                        or "secret" in direct_read_back_err.getvalue()
                    ),
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-client-cli-quic-smoke.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_client_retries_interrupted_read_from_java_server(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = _pattern_bytes(2 * 1024 * 1024)
            (dataset / "shard.00000.mds").write_bytes(payload)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                read_config = CxnfsBulkReadConfig(
                    initial_cords=4,
                    max_cords=4,
                    provisional_container_size_bytes=64 * 1024,
                    expected_size_limit_bytes=len(payload),
                )

                opened_transports = []

                def connect_bulk_transport():
                    transport = QuicBulkFrameTransport.connect(
                        "localhost",
                        transport_port,
                        timeout=5.0,
                        server_hostname="localhost",
                        cafile=str(cert),
                    )
                    opened_transports.append(transport)
                    return transport

                tls_context = _server_verified_tls13_context(str(cert))
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        interrupted_transport = InterruptingReadBulkFrameTransport(connect_bulk_transport())
                        client = CxnfsQuicBulkClient(
                            control,
                            interrupted_transport,
                            read_config=read_config,
                            close_control_client=True,
                            retry_config=CxnfsBulkRetryConfig(read_retries=1),
                            bulk_transport_reconnector=connect_bulk_transport,
                        )
                        try:
                            destination = bytearray(len(payload))
                            bytes_read = client.read_into_buffer(
                                "/datasets/train/shard.00000.mds",
                                0,
                                destination,
                                len(destination),
                            )
                            read_evidence = client.read_path_evidence()
                        finally:
                            client.close()
                    except Exception:
                        control.close()
                        raise
                    finally:
                        for transport in opened_transports:
                            try:
                                transport.close()
                            except Exception:
                                pass

                self.assertEqual(len(payload), bytes_read)
                self.assertEqual(payload, bytes(destination))
                self.assertEqual(1, interrupted_transport.read_interruptions)
                self.assertEqual(1, read_evidence["failed_read_operations"])
                self.assertEqual(1, read_evidence["failed_direct_buffer_read_operations"])
                self.assertEqual(1, read_evidence["read_retry_attempts"])
                self.assertEqual(1, read_evidence["read_retry_successes"])
                self.assertEqual(1, read_evidence["bulk_transport_reconnects"])
                self.assertEqual(1, read_evidence["direct_buffer_read_operations"])
                self.assertGreater(read_evidence["direct_destination_zeroed_bytes_on_failure"], 0)
                self.assertGreater(read_evidence["max_observed_cords"], 1)
                bulk_evidence = read_evidence["bulk_transport_read_evidence"]
                self.assertEqual("TLSv1.3", bulk_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_evidence["cipher_suite"])
                self.assertTrue(bulk_evidence["read_stream_completion_verified"])
                self.assertTrue(bulk_evidence["read_stream_eof_verified"])
                report = root / "read-retry.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-read-retry.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "tls_version": bulk_evidence["tls_version"],
                    "cipher_suite": bulk_evidence["cipher_suite"],
                    "interruption_injected": True,
                    "payload_verified": bytes(destination) == payload,
                    "byte_size": len(payload),
                    "bytes_read": bytes_read,
                    "read_retry_limit": read_evidence["read_retry_limit"],
                    "read_retry_attempts": read_evidence["read_retry_attempts"],
                    "read_retry_successes": read_evidence["read_retry_successes"],
                    "bulk_transport_reconnects": read_evidence["bulk_transport_reconnects"],
                    "failed_read_operations": read_evidence["failed_read_operations"],
                    "failed_direct_buffer_read_operations": read_evidence["failed_direct_buffer_read_operations"],
                    "direct_destination_zeroed_bytes_on_failure": read_evidence[
                        "direct_destination_zeroed_bytes_on_failure"
                    ],
                    "direct_buffer_read_operations": read_evidence["direct_buffer_read_operations"],
                    "max_observed_cords": read_evidence["max_observed_cords"],
                    "transport_frame_sink_observed": read_evidence["direct_destination_frame_sink_read_operations"] > 0,
                    "frame_sink_used_for_all_direct_destination_reads": read_evidence[
                        "frame_sink_used_for_all_direct_destination_reads"
                    ],
                    "bulk_transport_read_evidence": bulk_evidence,
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-read-retry.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_client_reads_sixty_four_cords_from_java_server(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            dataset = data_root / "datasets" / "train"
            dataset.mkdir(parents=True)
            payload = _pattern_bytes(9 * 1024 * 1024)
            (dataset / "large.bin").write_bytes(payload)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=20.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=20.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=1,
                                max_cords=64,
                                provisional_container_size_bytes=512 * 1024,
                                expected_size_limit_bytes=len(payload),
                            ),
                            close_control_client=True,
                        ) as client:
                            destination = bytearray(len(payload))
                            bytes_read = client.read_into_buffer(
                                "/datasets/train/large.bin",
                                0,
                                destination,
                                len(destination),
                            )
                            read_evidence = client.read_path_evidence()
                    except Exception:
                        control.close()
                        raise

                self.assertEqual(len(payload), bytes_read)
                self.assertEqual(payload, bytes(destination))
                self.assertEqual(64, read_evidence["max_observed_cords"])
                self.assertEqual(64, read_evidence["direct_destination_max_observed_cords"])
                self.assertEqual(1, read_evidence["last_accepted_initial_cords"])
                self.assertEqual(64, read_evidence["last_accepted_max_cords"])
                self.assertTrue(read_evidence["observed_cords_within_admission"])
                self.assertTrue(read_evidence["direct_destination_frame_sink_cord_fanout_verified"])
                bulk_evidence = read_evidence["bulk_transport_read_evidence"]
                self.assertEqual(64, bulk_evidence["last_read_stream_count"])
                self.assertEqual("TLSv1.3", bulk_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_evidence["cipher_suite"])
                self.assertTrue(bulk_evidence["read_stream_completion_verified"])
                self.assertTrue(bulk_evidence["read_stream_eof_verified"])
                report = root / "high-cord-read.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-high-cord-read.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "tls_version": bulk_evidence["tls_version"],
                    "cipher_suite": bulk_evidence["cipher_suite"],
                    "byte_size": len(payload),
                    "bytes_read": bytes_read,
                    "max_observed_cords": read_evidence["max_observed_cords"],
                    "last_read_stream_count": bulk_evidence["last_read_stream_count"],
                    "transport_frame_sink_observed": read_evidence["transport_frame_sink_observed"],
                    "direct_destination_read_operations": read_evidence["direct_destination_read_operations"],
                    "direct_destination_parallel_frame_sink_read_operations": read_evidence[
                        "direct_destination_parallel_frame_sink_read_operations"
                    ],
                    "direct_destination_parallel_frame_sink_frames": read_evidence[
                        "direct_destination_parallel_frame_sink_frames"
                    ],
                    "direct_destination_max_observed_cords": read_evidence[
                        "direct_destination_max_observed_cords"
                    ],
                    "direct_destination_frame_sink_cord_fanout_verified": read_evidence[
                        "direct_destination_frame_sink_cord_fanout_verified"
                    ],
                    "frame_sink_used_for_all_direct_destination_reads": read_evidence[
                        "frame_sink_used_for_all_direct_destination_reads"
                    ],
                    "last_accepted_initial_cords": read_evidence["last_accepted_initial_cords"],
                    "last_accepted_max_cords": read_evidence["last_accepted_max_cords"],
                    "observed_cords_within_admission": read_evidence["observed_cords_within_admission"],
                    "bulk_transport_read_evidence": bulk_evidence,
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-high-cord-read.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_client_writes_tensor_with_64_cords_to_java_server(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            data_root.mkdir(parents=True)
            frame_bytes = 4096
            payload = bytearray((index % 251 for index in range(64 * frame_bytes)))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=5.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=1,
                                max_cords=64,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=len(payload),
                                write_frame_payload_bytes=frame_bytes,
                            ),
                            close_control_client=True,
                        ) as client:
                            result = client.write_tensor(
                                "/outputs/run-1/model.safetensors",
                                payload,
                                len(payload),
                            )
                            read_back = client.read_mds_shard(
                                "/outputs/run-1/model.safetensors",
                                0,
                                len(payload),
                            )
                            read_evidence = client.read_path_evidence()
                            write_evidence = client.write_path_evidence()
                    except Exception:
                        control.close()
                        raise

                self.assertEqual(1, result.file_count)
                self.assertEqual(1, result.tensor_file_count)
                self.assertEqual(len(payload), result.bytes_sent)
                self.assertEqual(bytes(payload), read_back)
                self.assertEqual("cxnfs.bulk-write-path-evidence.v1", write_evidence["format"])
                self.assertEqual(1, write_evidence["direct_tensor_write_operations"])
                self.assertEqual(len(payload), write_evidence["payload_bytes_written"])
                self.assertEqual(64, write_evidence["write_frames"])
                self.assertEqual(write_evidence["write_frames"], write_evidence["write_acks"])
                self.assertEqual(write_evidence["write_frames"], write_evidence["write_ack_frame_verified"])
                _assert_quic_write_ack_stream_proven(self, write_evidence)
                self.assertEqual(write_evidence["write_frames"], write_evidence["write_ack_metadata_pending"])
                self.assertEqual(1, write_evidence["metadata_after_payload_entries"])
                self.assertEqual(1, write_evidence["entries_committed"])
                self.assertEqual(1, write_evidence["transfers_committed"])
                self.assertEqual(1, write_evidence["last_accepted_initial_cords"])
                self.assertEqual(64, write_evidence["last_accepted_max_cords"])
                self.assertEqual(64, write_evidence["max_observed_write_cords"])
                bulk_write_evidence = write_evidence["bulk_transport_write_evidence"]
                self.assertEqual("cxnfs.quic-bulk-write-transport-evidence.v1", bulk_write_evidence["format"])
                self.assertEqual(1, bulk_write_evidence["connection_opened_count"])
                self.assertEqual(64, bulk_write_evidence["successful_write_operations"])
                self.assertEqual(0, bulk_write_evidence["failed_write_operations"])
                self.assertEqual(64, bulk_write_evidence["attempted_write_streams"])
                self.assertEqual(64, bulk_write_evidence["write_streams_created"])
                self.assertTrue(bulk_write_evidence["write_streams_all_succeeded"])
                self.assertEqual(64, bulk_write_evidence["write_acks_received"])
                self.assertEqual(64, bulk_write_evidence["write_acks_verified"])
                self.assertEqual(0, bulk_write_evidence["write_ack_verification_failures"])
                _assert_quic_write_ack_stream_proven(self, bulk_write_evidence)
                self.assertEqual(0, bulk_write_evidence["active_write_streams"])
                self.assertGreaterEqual(bulk_write_evidence["max_active_write_streams"], 2)
                self.assertTrue(bulk_write_evidence["concurrent_write_streams_observed"])
                self.assertEqual("TLSv1.3", bulk_write_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_write_evidence["cipher_suite"])
                self.assertTrue(bulk_write_evidence["connection_reused_for_streams"])
                self.assertEqual(64, read_evidence["max_observed_cords"])
                report = root / "high-cord-write.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-high-cord-write.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "tls_version": bulk_write_evidence["tls_version"],
                    "cipher_suite": bulk_write_evidence["cipher_suite"],
                    "bytes_written": len(payload),
                    "frame_bytes": frame_bytes,
                    "write_frames": write_evidence["write_frames"],
                    "write_acks": write_evidence["write_acks"],
                    "write_ack_frame_verified": write_evidence["write_ack_frame_verified"],
                    "write_ack_frame_identity_verified": write_evidence["write_ack_frame_identity_verified"],
                    "write_ack_stream_completion_verified": write_evidence["write_ack_stream_completion_verified"],
                    "write_ack_stream_eof_verified": write_evidence["write_ack_stream_eof_verified"],
                    "write_ack_metadata_pending": write_evidence["write_ack_metadata_pending"],
                    "metadata_after_payload_entries": write_evidence["metadata_after_payload_entries"],
                    "last_accepted_initial_cords": write_evidence["last_accepted_initial_cords"],
                    "last_accepted_max_cords": write_evidence["last_accepted_max_cords"],
                    "max_observed_write_cords": write_evidence["max_observed_write_cords"],
                    "bulk_transport_write_evidence": bulk_write_evidence,
                    "read_back_verified": bytes(payload) == read_back,
                    "read_back_max_observed_cords": read_evidence["max_observed_cords"],
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-high-cord-write.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_client_retries_interrupted_write_to_java_server(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            data_root.mkdir(parents=True)
            payload = _pattern_bytes(96 * 1024)
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                read_config = CxnfsBulkReadConfig(
                    initial_cords=4,
                    max_cords=4,
                    provisional_container_size_bytes=64 * 1024,
                    expected_size_limit_bytes=len(payload),
                )
                opened_transports = []

                def connect_bulk_transport():
                    transport = QuicBulkFrameTransport.connect(
                        "localhost",
                        transport_port,
                        timeout=5.0,
                        server_hostname="localhost",
                        cafile=str(cert),
                    )
                    opened_transports.append(transport)
                    return transport

                tls_context = _server_verified_tls13_context(str(cert))
                remote_root = "/outputs/write-retry"
                remote_path = f"{remote_root}/model.safetensors"
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        interrupted_transport = InterruptingWriteBulkFrameTransport(connect_bulk_transport())
                        client = CxnfsQuicBulkClient(
                            control,
                            interrupted_transport,
                            read_config=read_config,
                            close_control_client=True,
                            retry_config=CxnfsBulkRetryConfig(write_retries=1),
                            bulk_transport_reconnector=connect_bulk_transport,
                        )
                        try:
                            transfer = control.bootstrap_write_transfer(
                                remote_root,
                                payload_format=TransferPayloadFormat.SAFETENSORS,
                                initial_cords=read_config.initial_cords,
                                max_cords=read_config.max_cords,
                                provisional_container_size_bytes=read_config.provisional_container_size_bytes,
                                expected_size_limit_bytes=read_config.expected_size_limit_bytes,
                            )
                            frame = BulkDataFrame(
                                transfer.transfer_id,
                                0,
                                0,
                                0,
                                0,
                                True,
                                payload,
                            )
                            ack = client.write_frame(frame)
                            control.accept_write_entry(
                                transfer.transfer_id,
                                0,
                                TransferEntryAction.CREATE_OR_REPLACE,
                                remote_path,
                                size_hint_bytes=len(payload),
                                payload_format=TransferPayloadFormat.SAFETENSORS,
                            )
                            chunk_hash = cxnfs_chunk_hash(payload)
                            control.accept_write_metadata(
                                transfer.transfer_id,
                                0,
                                0,
                                True,
                                len(payload),
                                (chunk_hash,),
                            )
                            file_hash = cxnfs_file_hash((chunk_hash,), (len(payload),))
                            control.commit_write_entry(
                                transfer.transfer_id,
                                0,
                                remote_path,
                                file_hash,
                                len(payload),
                            )
                            transfer_hash = cxnfs_transfer_hash((file_hash,), (len(payload),))
                            control.commit_write_transfer(
                                transfer.transfer_id,
                                remote_root,
                                transfer_hash,
                                len(payload),
                            )
                            read_back = client.read_mds_shard(remote_path, 0, len(payload))
                            write_evidence = client.write_path_evidence()
                        finally:
                            client.close()
                    except Exception:
                        control.close()
                        raise
                    finally:
                        for transport in opened_transports:
                            try:
                                transport.close()
                            except Exception:
                                pass

                self.assertIsNotNone(ack)
                self.assertFalse(ack.metadata_verified)
                self.assertEqual(bytes(payload), read_back)
                self.assertEqual(1, interrupted_transport.write_interruptions)
                self.assertEqual(1, write_evidence["write_retry_limit"])
                self.assertEqual(1, write_evidence["write_retry_attempts"])
                self.assertEqual(1, write_evidence["write_retry_successes"])
                self.assertEqual(1, write_evidence["bulk_transport_write_reconnects"])
                self.assertEqual("", write_evidence["last_write_retry_error"])
                bulk_write_evidence = write_evidence["bulk_transport_write_evidence"]
                self.assertEqual(1, bulk_write_evidence["successful_write_operations"])
                self.assertEqual(0, bulk_write_evidence["failed_write_operations"])
                self.assertEqual(1, bulk_write_evidence["write_acks_verified"])
                self.assertEqual(0, bulk_write_evidence["write_ack_verification_failures"])
                _assert_quic_write_ack_stream_proven(self, bulk_write_evidence)
                self.assertEqual("TLSv1.3", bulk_write_evidence["tls_version"])
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_write_evidence["cipher_suite"])
                report = root / "write-retry.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-write-retry.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "tls_version": bulk_write_evidence["tls_version"],
                    "cipher_suite": bulk_write_evidence["cipher_suite"],
                    "interruption_injected": True,
                    "payload_verified": bytes(read_back) == payload,
                    "byte_size": len(payload),
                    "bytes_written": len(payload),
                    "write_retry_limit": write_evidence["write_retry_limit"],
                    "write_retry_attempts": write_evidence["write_retry_attempts"],
                    "write_retry_successes": write_evidence["write_retry_successes"],
                    "bulk_transport_write_reconnects": write_evidence["bulk_transport_write_reconnects"],
                    "last_write_retry_error": write_evidence["last_write_retry_error"],
                    "ack_metadata_verified": ack.metadata_verified,
                    "bulk_transport_write_evidence": bulk_write_evidence,
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-write-retry.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_write_accepts_idempotent_resend_before_metadata(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            data_root.mkdir(parents=True)
            payload = b"idempotent checkpoint frame"
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                remote_path = "/outputs/retry/model.safetensors"
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=5.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=2,
                                max_cords=2,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=1024,
                            ),
                            close_control_client=False,
                        ) as client:
                            transfer = control.bootstrap_write_transfer(
                                "/outputs/retry",
                                payload_format=TransferPayloadFormat.SAFETENSORS,
                                initial_cords=2,
                                max_cords=2,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=1024,
                            )
                            frame = BulkDataFrame(
                                transfer.transfer_id,
                                0,
                                0,
                                0,
                                0,
                                True,
                                payload,
                            )
                            first_ack = client.write_frame(frame)
                            duplicate_ack = client.write_frame(frame)
                            control.accept_write_entry(
                                transfer.transfer_id,
                                0,
                                TransferEntryAction.CREATE_OR_REPLACE,
                                remote_path,
                                size_hint_bytes=len(payload),
                                payload_format=TransferPayloadFormat.SAFETENSORS,
                            )
                            chunk_hash = cxnfs_chunk_hash(payload)
                            control.accept_write_metadata(
                                transfer.transfer_id,
                                0,
                                0,
                                True,
                                len(payload),
                                (chunk_hash,),
                            )
                            file_hash = cxnfs_file_hash((chunk_hash,), (len(payload),))
                            control.commit_write_entry(
                                transfer.transfer_id,
                                0,
                                remote_path,
                                file_hash,
                                len(payload),
                            )
                            transfer_hash = cxnfs_transfer_hash((file_hash,), (len(payload),))
                            control.commit_write_transfer(
                                transfer.transfer_id,
                                "/outputs/retry",
                                transfer_hash,
                                len(payload),
                            )
                            read_back = client.read_mds_shard(remote_path, 0, len(payload))
                            write_evidence = client.write_path_evidence()
                    except Exception:
                        control.close()
                        raise
                    finally:
                        control.close()

                self.assertIsNotNone(first_ack)
                self.assertIsNotNone(duplicate_ack)
                self.assertFalse(first_ack.metadata_verified)
                self.assertFalse(duplicate_ack.metadata_verified)
                self.assertEqual(bytes(payload), read_back)
                self.assertEqual(2, write_evidence["bulk_transport_write_evidence"]["successful_write_operations"])
                self.assertEqual(0, write_evidence["bulk_transport_write_evidence"]["failed_write_operations"])
                self.assertEqual(2, write_evidence["bulk_transport_write_evidence"]["write_acks_verified"])
                self.assertEqual(0, write_evidence["bulk_transport_write_evidence"]["write_ack_verification_failures"])
                _assert_quic_write_ack_stream_proven(self, write_evidence["bulk_transport_write_evidence"])
                report = root / "duplicate-write-retry.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-duplicate-write-retry.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "bytes_written": len(payload),
                    "duplicate_frame_sent": True,
                    "first_ack_metadata_verified": first_ack.metadata_verified,
                    "duplicate_ack_metadata_verified": duplicate_ack.metadata_verified,
                    "read_back_verified": bytes(payload) == read_back,
                    "bulk_transport_write_evidence": write_evidence["bulk_transport_write_evidence"],
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-duplicate-write-retry.json")
            finally:
                _stop_server(process, stop)

    def test_python_quic_bulk_write_rejects_bad_late_metadata_without_publish(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            data_root.mkdir(parents=True)
            payload = b"verified checkpoint bytes"
            bad_payload = b"different checkpoint bytes"
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                tls_context = _server_verified_tls13_context(str(cert))
                remote_path = "/outputs/rejected/model.safetensors"
                with ClientCredentials("trainer", "secret") as credentials:
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=tls_context,
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        with CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=5.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=2,
                                max_cords=2,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=1024,
                            ),
                            close_control_client=False,
                        ) as client:
                            transfer = control.bootstrap_write_transfer(
                                "/outputs/rejected",
                                payload_format=TransferPayloadFormat.SAFETENSORS,
                                initial_cords=2,
                                max_cords=2,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=1024,
                            )
                            ack = client.write_frame(BulkDataFrame(
                                transfer.transfer_id,
                                0,
                                0,
                                0,
                                0,
                                True,
                                payload,
                            ))
                            control.accept_write_entry(
                                transfer.transfer_id,
                                0,
                                TransferEntryAction.CREATE_OR_REPLACE,
                                remote_path,
                                size_hint_bytes=len(payload),
                                payload_format=TransferPayloadFormat.SAFETENSORS,
                            )
                            with self.assertRaises(CxnfsControlError) as metadata_error:
                                control.accept_write_metadata(
                                    transfer.transfer_id,
                                    0,
                                    0,
                                    True,
                                    len(payload),
                                    (cxnfs_chunk_hash(bad_payload),),
                                )
                            with self.assertRaises(CxnfsControlError) as commit_error:
                                control.commit_write_entry(
                                    transfer.transfer_id,
                                    0,
                                    remote_path,
                                    cxnfs_file_hash((cxnfs_chunk_hash(payload),), (len(payload),)),
                                    len(payload),
                                )
                            stat = control.stat_path(remote_path)
                    except Exception:
                        control.close()
                        raise
                    finally:
                        control.close()

                self.assertIsNotNone(ack)
                self.assertFalse(ack.metadata_verified)
                self.assertEqual(ResponseStatus.CONFLICT, metadata_error.exception.status)
                self.assertEqual(ResponseStatus.CONFLICT, commit_error.exception.status)
                self.assertIsNone(stat.metadata)
                report = root / "bad-late-metadata.json"
                report.write_text(json.dumps({
                    "format": "cxnfs.quic-bad-late-metadata.v1",
                    "transport": "cxnfs-quic",
                    "bulk_transport": "quic",
                    "ack_metadata_verified": ack.metadata_verified,
                    "metadata_rejected": True,
                    "metadata_error_status": metadata_error.exception.status.name,
                    "commit_rejected": True,
                    "commit_error_status": commit_error.exception.status.name,
                    "published": stat.metadata is not None,
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(report, "python-quic-bad-late-metadata.json")
            finally:
                _stop_server(process, stop)

    def test_trainer_stream_benchmark_reads_weighted_mds_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 4))
            _write_file_dataset(data_root, "summary", ("summary-0", "summary-1"))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                stdout = io.StringIO()
                stderr = io.StringIO()
                report = root / "trainer-stream-quic.json"

                exit_code = run_trainer_stream_smoke_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "10",
                        "--initial-cords",
                        "4",
                        "--max-cords",
                        "4",
                        "--provisional-container-size",
                        str(512 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--batch-size",
                        "3",
                        "--batches",
                        "2",
                        "--warmup-batches",
                        "1",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--trainer-prefetch-batches",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "6",
                        "--read-mode",
                        "direct-buffers",
                        "--objective",
                        "ai-training-25gbe",
                        "--baseline-mb-s",
                        "1.0",
                        "--output",
                        str(report),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=_server_verified_tls13_context,
                )

                self.assertEqual(0, exit_code, stderr.getvalue())
                self.assertEqual(stdout.getvalue(), report.read_text(encoding="utf-8"))
                payload = json.loads(stdout.getvalue())
                self.assertEqual("cxnfs.trainer-stream-smoke.v1", payload["format"])
                self.assertEqual("cxnfs-quic", payload["transport"])
                self.assertEqual("quic", payload["bulk_transport"])
                self.assertEqual("direct-buffers", payload["read_mode"])
                self.assertEqual("/stage0-mds", payload["remote_root"])
                self.assertEqual(2, payload["trainer_prefetch_batches"])
                self.assertTrue(payload["trainer_prefetch_enabled"])
                self.assertEqual("CxnfsTrainerPrefetchStream", payload["trainer_prefetch_stream_object"])
                self.assertEqual(1, payload["bulk_read_retries"])
                self.assertEqual(2, payload["batch_count"])
                self.assertEqual(6, payload["sample_count"])
                self.assertGreater(payload["byte_size"], 0)
                self.assertGreater(payload["bytes_per_second"], 0.0)
                self.assertTrue(payload["read_path_evidence"]["transport_frame_sink_supported"])
                _assert_direct_destination_cord_fanout(self, payload["read_path_evidence"])
                self.assertGreaterEqual(payload["read_path_evidence"]["direct_destination_read_operations"], 6)
                self.assertGreater(payload["read_path_evidence"]["max_observed_cords"], 1)
                self.assertEqual(
                    "cxnfs.trainer-read-path-reliability.v1",
                    payload["read_path_reliability"]["format"],
                )
                self.assertTrue(payload["read_path_reliability"]["complete"])
                self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
                self.assertIn("objective_passed", payload)
                _publish_report(report, "python-quic-trainer-stream-benchmark.json")
            finally:
                _stop_server(process, stop)

    @unittest.skipUnless(
        importlib.util.find_spec("torch"),
        "torch is required for trainer pipeline tensor integration",
    )
    def test_trainer_pipeline_smoke_reads_and_writes_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 4))
            _write_file_dataset(data_root, "summary", ("summary-0", "summary-1"))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                stdout = io.StringIO()
                stderr = io.StringIO()
                report = root / "trainer-pipeline-quic.json"

                exit_code = run_trainer_pipeline_smoke_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "10",
                        "--initial-cords",
                        "1",
                        "--max-cords",
                        "4",
                        "--provisional-container-size",
                        str(512 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--batch-size",
                        "3",
                        "--batches",
                        "2",
                        "--warmup-batches",
                        "1",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--pipeline-workers",
                        "4",
                        "--pipeline-prefetch-batches",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "6",
                        "--tensor-target",
                        "pytorch-cpu",
                        "--write-probe-bytes",
                        "4096",
                        "--write-probe-frame-bytes",
                        "1024",
                        "--write-probe-root",
                        "trainer-pipeline-write-probes",
                        "--output",
                        str(report),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )

                self.assertEqual(0, exit_code, stderr.getvalue())
                self.assertEqual(stdout.getvalue(), report.read_text(encoding="utf-8"))
                payload = json.loads(stdout.getvalue())
                self.assertEqual("cxnfs.trainer-pipeline-smoke.v1", payload["format"])
                self.assertTrue(payload["passed"])
                self.assertEqual("cxnfs-quic", payload["transport"])
                self.assertEqual("quic", payload["bulk_transport"])
                self.assertEqual("direct-tensors", payload["read_mode"])
                self.assertEqual("/stage0-mds", payload["remote_root"])
                self.assertEqual("CxnfsTrainingPipeline", payload["training_pipeline_object"])
                self.assertEqual(
                    "CxnfsTrainingPipeline.prefetch_trainer_stream",
                    payload["training_pipeline_context"],
                )
                self.assertEqual("CxnfsTrainerPipelineStream", payload["pipeline_descriptor_stream"])
                self.assertTrue(payload["pipeline_prefetch_stream_used"])
                self.assertEqual(2, payload["pipeline_prefetch_batches"])
                self.assertEqual("CxnfsPipelinePrefetchedTensorBatch", payload["pipeline_prefetch_result_object"])
                self.assertEqual(2, payload["batch_count"])
                self.assertEqual(6, payload["sample_count"])
                self.assertGreater(payload["byte_size"], 0)
                self.assertTrue(payload["read_path_evidence"]["training_pipeline_used"])
                self.assertTrue(payload["read_path_evidence"]["pipeline_descriptor_planning_used"])
                self.assertTrue(payload["read_path_evidence"]["pipeline_prefetch_stream_used"])
                self.assertEqual(2, payload["read_path_evidence"]["pipeline_prefetch_batches"])
                self.assertTrue(payload["read_path_evidence"]["pipeline_python_byte_batch_avoided"])
                self.assertTrue(payload["read_path_evidence"]["transport_frame_sink_supported"])
                _assert_direct_destination_cord_fanout(self, payload["read_path_evidence"])
                self.assertTrue(payload["read_path_evidence"]["quic_read_transport_observed"])
                self.assertTrue(payload["read_path_evidence"]["quic_read_transport_complete"])
                self.assertGreater(payload["read_path_evidence"]["quic_read_transport_read_streams_created"], 0)
                self.assertEqual(
                    "cxnfs.trainer-read-path-reliability.v1",
                    payload["read_path_reliability"]["format"],
                )
                self.assertTrue(payload["read_path_reliability"]["complete"])
                self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
                self.assertTrue(payload["tensor_memory_path"]["complete"])
                self.assertTrue(payload["remote_client_topology"]["per_source_client_observed"])
                self.assertTrue(payload["remote_client_lifecycle_closed"])

                write_probe = payload["write_probe"]
                self.assertTrue(write_probe["enabled"])
                self.assertTrue(write_probe["verified_read_back"])
                self.assertEqual(4096, write_probe["byte_size"])
                self.assertEqual(1024, write_probe["frame_bytes"])
                self.assertEqual(1, write_probe["file_count"])
                self.assertEqual(1, write_probe["tensor_file_count"])
                self.assertEqual(4096, write_probe["bytes_sent"])
                self.assertTrue(write_probe["path"].startswith("trainer-pipeline-write-probes/"))
                write_evidence = write_probe["write_path_evidence"]
                self.assertEqual("cxnfs.bulk-write-path-evidence.v1", write_evidence["format"])
                self.assertEqual(1, write_evidence["direct_tensor_write_operations"])
                self.assertEqual(4, write_evidence["max_observed_write_cords"])
                self.assertEqual(4, write_evidence["write_frames"])
                self.assertEqual(4, write_evidence["write_acks"])
                self.assertEqual(4, write_evidence["write_ack_frame_verified"])
                _assert_quic_write_ack_stream_proven(self, write_evidence)
                self.assertEqual(0, write_evidence["write_ack_metadata_verified"])
                self.assertEqual(4, write_evidence["write_ack_metadata_pending"])
                self.assertEqual(1, write_evidence["metadata_after_payload_entries"])
                self.assertEqual(1, write_evidence["entries_committed"])
                self.assertEqual(1, write_evidence["transfers_committed"])
                bulk_write = write_evidence["bulk_transport_write_evidence"]
                self.assertEqual("TLS_AES_128_GCM_SHA256", bulk_write["cipher_suite"])
                self.assertTrue(bulk_write["connection_reused_for_streams"])
                self.assertEqual(4, bulk_write["write_acks_verified"])
                self.assertEqual(0, bulk_write["write_ack_verification_failures"])
                _assert_quic_write_ack_stream_proven(self, bulk_write)
                self.assertGreaterEqual(bulk_write["max_active_write_streams"], 2)
                _publish_report(report, "python-quic-trainer-pipeline-smoke.json")
            finally:
                _stop_server(process, stop)

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for trainer DataLoader worker integration")
    def test_trainer_dataloader_workers_read_weighted_mds_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 4))
            _write_file_dataset(data_root, "summary", ("summary-0", "summary-1"))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                stdout = io.StringIO()
                stderr = io.StringIO()
                report = root / "trainer-workers-quic.json"

                exit_code = run_trainer_stream_smoke_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "5",
                        "--initial-cords",
                        "4",
                        "--max-cords",
                        "4",
                        "--provisional-container-size",
                        str(512 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--batch-size",
                        "3",
                        "--batches",
                        "2",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "6",
                        "--read-mode",
                        "direct-buffers",
                        "--trainer-batch-iterator",
                        "trainer-dataloader",
                        "--data-loader-workers",
                        "2",
                        "--output",
                        str(report),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_server_verified_tls13_context,
                )

                self.assertEqual(0, exit_code, stderr.getvalue())
                self.assertEqual(stdout.getvalue(), report.read_text(encoding="utf-8"))
                payload = json.loads(stdout.getvalue())
                self.assertEqual("cxnfs.trainer-stream-smoke.v1", payload["format"])
                self.assertEqual("cxnfs-quic", payload["transport"])
                self.assertEqual("quic", payload["bulk_transport"])
                self.assertEqual("trainer-dataloader", payload["trainer_batch_iterator"])
                self.assertEqual(2, payload["data_loader_workers"])
                self.assertEqual(2, payload["batch_count"])
                self.assertEqual(6, payload["sample_count"])
                worker_partition = payload["worker_partition_evidence"]
                self.assertTrue(worker_partition["worker_sharding_enabled"])
                self.assertTrue(worker_partition["complete"])
                self.assertEqual(2, worker_partition["data_loader_workers"])
                self.assertEqual(0, worker_partition["duplicate_source_sample_count"])
                read_evidence = payload["read_path_evidence"]
                self.assertTrue(read_evidence["worker_batch_read_evidence_observed"])
                self.assertEqual(2, read_evidence["worker_batch_read_evidence_count"])
                _assert_direct_destination_cord_fanout(self, read_evidence)
                self.assertGreaterEqual(read_evidence["direct_destination_read_operations"], 6)
                self.assertGreater(read_evidence["max_observed_cords"], 1)
                self.assertTrue(all(
                    report["produced_inside_torch_worker"]
                    for report in read_evidence["worker_process_read_path_evidence"]
                ))
                self.assertEqual(
                    {2},
                    {
                        report["worker_count"]
                        for report in read_evidence["worker_process_read_path_evidence"]
                    },
                )
                self.assertTrue(payload["read_path_reliability"]["complete"])
                self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
                _publish_report(report, "python-quic-trainer-workers.json")
            finally:
                _stop_server(process, stop)

    def test_trainer_stream_resume_cursor_replays_weighted_mds_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", ("chat-0", "chat-1", "chat-2", "chat-3"))
            _write_file_dataset(data_root, "summary", ("summary-0", "summary-1"))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                cursor = root / "trainer-resume-cursor.json"
                state = root / "trainer-resume-state.json"
                first_report = root / "trainer-resume-first.json"
                second_report = root / "trainer-resume-second.json"
                summary_report = root / "trainer-resume-quic.json"
                first_stdout = io.StringIO()
                first_stderr = io.StringIO()
                second_stdout = io.StringIO()
                second_stderr = io.StringIO()
                common_args = [
                    str(stage0),
                    "--weights",
                    "chat=2,summary=1",
                    "--remote-root",
                    "/stage0-mds",
                    "--host",
                    "localhost",
                    "--control-port",
                    str(control_port),
                    "--transport-port",
                    str(transport_port),
                    "--bulk-transport",
                    "quic",
                    "--server-hostname",
                    "localhost",
                    "--cafile",
                    str(cert),
                    "--timeout",
                    "5",
                    "--initial-cords",
                    "4",
                    "--max-cords",
                    "4",
                    "--provisional-container-size",
                    str(64 * 1024),
                    "--expected-size-limit",
                    str(512 * 1024),
                    "--batch-size",
                    "2",
                    "--batches",
                    "1",
                    "--rounds-per-read",
                    "2",
                    "--source-read-workers",
                    "2",
                    "--prefetch-chunks",
                    "2",
                    "--trainer-prefetch-batches",
                    "2",
                    "--bulk-read-retries",
                    "1",
                    "--samples-per-epoch",
                    "6",
                    "--read-mode",
                    "direct-buffers",
                ]

                first_exit = run_trainer_stream_smoke_cli(
                    [
                        *common_args,
                        "--write-resume-cursor",
                        str(cursor),
                        "--write-resume-state",
                        str(state),
                        "--output",
                        str(first_report),
                    ],
                    stdout=first_stdout,
                    stderr=first_stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )
                second_exit = run_trainer_stream_smoke_cli(
                    [
                        *common_args,
                        "--resume-state",
                        str(state),
                        "--output",
                        str(second_report),
                    ],
                    stdout=second_stdout,
                    stderr=second_stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )

                self.assertEqual(0, first_exit, first_stderr.getvalue())
                self.assertEqual(0, second_exit, second_stderr.getvalue())
                self.assertEqual(first_stdout.getvalue(), first_report.read_text(encoding="utf-8"))
                self.assertEqual(second_stdout.getvalue(), second_report.read_text(encoding="utf-8"))
                first_payload = json.loads(first_stdout.getvalue())
                second_payload = json.loads(second_stdout.getvalue())
                saved_cursor = json.loads(cursor.read_text(encoding="utf-8"))
                saved_state = json.loads(state.read_text(encoding="utf-8"))
                self.assertEqual(TRAINER_CURSOR_FORMAT, saved_cursor["format"])
                self.assertEqual(TRAINER_STREAM_STATE_FORMAT, saved_state["format"])
                self.assertEqual(first_payload["final_cursor"], saved_cursor)
                self.assertEqual(first_payload["stream_state"], saved_state)
                self.assertEqual(first_payload["final_cursor"], saved_state["cursor"])
                self.assertEqual(2, first_payload["final_cursor"]["emitted"])
                self.assertEqual(2, second_payload["initial_cursor"]["emitted"])
                self.assertEqual(4, second_payload["final_cursor"]["emitted"])
                self.assertEqual(first_payload["final_cursor"], second_payload["initial_cursor"])
                self.assertEqual("cxnfs-quic", first_payload["transport"])
                self.assertEqual("cxnfs-quic", second_payload["transport"])
                self.assertEqual("quic", first_payload["bulk_transport"])
                self.assertEqual("quic", second_payload["bulk_transport"])
                self.assertEqual("direct-buffers", first_payload["read_mode"])
                self.assertEqual("direct-buffers", second_payload["read_mode"])
                self.assertEqual(2, first_payload["trainer_prefetch_batches"])
                self.assertEqual(2, second_payload["trainer_prefetch_batches"])
                self.assertTrue(first_payload["trainer_prefetch_enabled"])
                self.assertTrue(second_payload["trainer_prefetch_enabled"])
                self.assertTrue(first_payload["stream_state_matches_final_cursor"])
                self.assertTrue(second_payload["stream_state_matches_final_cursor"])
                self.assertEqual(1, first_payload["bulk_read_retries"])
                self.assertEqual(1, second_payload["bulk_read_retries"])
                self.assertTrue(first_payload["read_path_evidence"]["transport_frame_sink_observed"])
                self.assertTrue(second_payload["read_path_evidence"]["transport_frame_sink_observed"])
                self.assertIn(
                    "direct_destination_frame_sink_cord_fanout_verified",
                    second_payload["read_path_evidence"],
                )
                self.assertTrue(first_payload["read_path_reliability"]["complete"])
                self.assertTrue(second_payload["read_path_reliability"]["complete"])
                self.assertEqual(0, second_payload["read_path_reliability"]["unrecovered_read_failures"])
                resumed_samples = [
                    (sample["source_name"], sample["sample_index"])
                    for sample in second_payload["batches"][0]["samples"]
                ]
                self.assertEqual([("summary", 0), ("chat", 2)], resumed_samples)

                summary_report.write_text(json.dumps({
                    "format": "cxnfs.quic-trainer-resume.v1",
                    "transport": second_payload["transport"],
                    "bulk_transport": second_payload["bulk_transport"],
                    "read_mode": second_payload["read_mode"],
                    "trainer_prefetch_batches": second_payload["trainer_prefetch_batches"],
                    "trainer_prefetch_enabled": second_payload["trainer_prefetch_enabled"],
                    "bulk_read_retries": second_payload["bulk_read_retries"],
                    "cursor_format": TRAINER_CURSOR_FORMAT,
                    "stream_state_format": TRAINER_STREAM_STATE_FORMAT,
                    "state_file_written": True,
                    "state_file_replayed": True,
                    "state_stream_state_matches_final_cursor": second_payload["stream_state_matches_final_cursor"],
                    "first_final_emitted": first_payload["final_cursor"]["emitted"],
                    "second_initial_emitted": second_payload["initial_cursor"]["emitted"],
                    "second_final_emitted": second_payload["final_cursor"]["emitted"],
                    "resumed_samples": [
                        {"source_name": source_name, "sample_index": sample_index}
                        for source_name, sample_index in resumed_samples
                    ],
                    "transport_frame_sink_observed": second_payload["read_path_evidence"][
                        "transport_frame_sink_observed"
                    ],
                    "frame_sink_used_for_all_direct_destination_reads": second_payload["read_path_evidence"][
                        "frame_sink_used_for_all_direct_destination_reads"
                    ],
                    "direct_destination_frame_sink_cord_fanout_verified": second_payload["read_path_evidence"][
                        "direct_destination_frame_sink_cord_fanout_verified"
                    ],
                    "direct_destination_max_observed_cords": second_payload["read_path_evidence"][
                        "direct_destination_max_observed_cords"
                    ],
                    "direct_destination_parallel_frame_sink_read_operations": second_payload["read_path_evidence"][
                        "direct_destination_parallel_frame_sink_read_operations"
                    ],
                    "direct_destination_parallel_frame_sink_frames": second_payload["read_path_evidence"][
                        "direct_destination_parallel_frame_sink_frames"
                    ],
                    "read_path_reliability_complete": second_payload["read_path_reliability"]["complete"],
                    "unrecovered_read_failures": second_payload["read_path_reliability"][
                        "unrecovered_read_failures"
                    ],
                }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(summary_report, "python-quic-trainer-resume.json")
            finally:
                _stop_server(process, stop)

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for direct tensor matrix integration")
    def test_trainer_surface_matrix_covers_live_java_quic_surfaces(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 2))
            _write_file_dataset(data_root, "summary", _large_values("summary", 1))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                stdout = io.StringIO()
                stderr = io.StringIO()
                report = root / "trainer-surface-matrix-quic.json"

                exit_code = run_trainer_surface_matrix_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "5",
                        "--initial-cords",
                        "4",
                        "--max-cords",
                        "4",
                        "--provisional-container-size",
                        str(64 * 1024),
                        "--expected-size-limit",
                        str(512 * 1024),
                        "--read-modes",
                        "direct-buffers,direct-tensors",
                        "--trainer-batch-iterators",
                        "native,trainer-dataloader",
                        "--data-loader-workers",
                        "0",
                        "--tensor-target",
                        "pytorch-cpu",
                        "--batch-size",
                        "3",
                        "--batches",
                        "1",
                        "--warmup-batches",
                        "1",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--trainer-prefetch-batches",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "3",
                        "--output",
                        str(report),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )

                self.assertEqual(0, exit_code, stderr.getvalue())
                self.assertEqual(stdout.getvalue(), report.read_text(encoding="utf-8"))
                payload = json.loads(stdout.getvalue())
                self.assertEqual("cxnfs.trainer-surface-matrix.v1", payload["format"])
                self.assertTrue(payload["passed"])
                self.assertFalse(payload["requires_mount"])
                self.assertFalse(payload["touches_trainer_code"])
                self.assertEqual("cxnfs-quic", payload["transport"])
                self.assertEqual("quic", payload["bulk_transport"])
                self.assertEqual("/stage0-mds", payload["remote_root"])
                self.assertEqual(2, payload["trainer_prefetch_batches"])
                self.assertEqual(1, payload["bulk_read_retries"])
                self.assertEqual(4, payload["case_count"])
                self.assertEqual(4, payload["passed_case_count"])
                self.assertEqual(0, payload["failed_case_count"])
                self.assertTrue(payload["read_path_reliability_complete"])
                self.assertEqual(0, payload["unrecovered_read_failures"])
                self.assertEqual(
                    "cxnfs.trainer-remote-client-topology-aggregate.v1",
                    payload["remote_client_topology"]["format"],
                )
                self.assertTrue(payload["remote_client_topology"]["per_source_client_complete"])
                self.assertEqual(4, payload["remote_client_topology"]["topology_report_count"])
                self.assertEqual(
                    {
                        "direct-buffers/native",
                        "direct-buffers/trainer-dataloader",
                        "direct-tensors/native",
                        "direct-tensors/trainer-dataloader",
                    },
                    {case["case_name"] for case in payload["cases"]},
                )
                for case in payload["cases"]:
                    self.assertTrue(case["passed"], case)
                    self.assertEqual(2, case["trainer_prefetch_batches"])
                    self.assertTrue(case["trainer_prefetch_enabled"])
                    self.assertEqual(1, case["bulk_read_retries"])
                    self.assertEqual(3, case["sample_count"])
                    self.assertEqual({"chat": 2, "summary": 1}, case["source_counts"])
                    self.assertTrue(case["read_path_reliability"]["complete"])
                    self.assertEqual(0, case["read_path_reliability"]["unrecovered_read_failures"])
                    self.assertTrue(case["remote_client_topology"]["per_source_client_observed"])
                    self.assertTrue(case["remote_client_topology"]["complete"])
                    self.assertTrue(case["memory_path_gate"]["complete"], case["memory_path_gate"])
                    _assert_direct_destination_cord_fanout(self, case["read_path_evidence"])
                    self.assertTrue(case["read_path_evidence"]["quic_read_transport_observed"])
                    self.assertTrue(case["read_path_evidence"]["quic_read_transport_complete"])
                    self.assertGreaterEqual(case["read_path_evidence"]["quic_read_transport_evidence_count"], 2)
                    self.assertGreater(case["read_path_evidence"]["quic_read_transport_read_streams_created"], 0)
                    self.assertGreater(case["read_path_evidence"]["quic_read_transport_connection_reuse_count"], 0)
                    self.assertGreater(case["read_path_evidence"]["max_observed_cords"], 1)
                    if case["read_mode"] == "direct-tensors":
                        self.assertEqual("pytorch-cpu", case["tensor_allocation"]["target"])
                        self.assertGreater(
                            case["read_path_evidence"]["direct_tensor_read_operations"],
                            0,
                        )
                    else:
                        self.assertGreater(
                            case["read_path_evidence"]["direct_buffer_read_operations"],
                            0,
                        )
                _publish_report(report, "python-quic-trainer-surface-matrix.json")
            finally:
                _stop_server(process, stop)

    def test_trainer_workload_benchmark_sweeps_weighted_mds_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 4))
            _write_file_dataset(data_root, "summary", ("summary-0", "summary-1"))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                stdout = io.StringIO()
                stderr = io.StringIO()
                report = root / "trainer-workload-quic.json"

                exit_code = run_trainer_workload_benchmark_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--workload-name",
                        "java-quic-weighted-mds",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "5",
                        "--cord-sweep",
                        "1,4",
                        "--provisional-container-size",
                        str(512 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--batch-size",
                        "3",
                        "--batches",
                        "2",
                        "--warmup-batches",
                        "1",
                        "--runs",
                        "2",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--trainer-prefetch-batches",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "6",
                        "--read-mode",
                        "direct-buffers",
                        "--objective",
                        "ai-training-25gbe",
                        "--baseline-mb-s",
                        "1.0",
                        "--write-probe-bytes",
                        "4096",
                        "--write-probe-frame-bytes",
                        "1024",
                        "--write-probe-root",
                        "/trainer-write-probes",
                        "--output",
                        str(report),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )

                self.assertEqual(0, exit_code, stderr.getvalue())
                self.assertEqual(stdout.getvalue(), report.read_text(encoding="utf-8"))
                payload = json.loads(stdout.getvalue())
                self.assertEqual("cxnfs.trainer-workload-benchmark.v1", payload["format"])
                self.assertEqual("java-quic-weighted-mds", payload["workload_name"])
                self.assertEqual("cxnfs-quic", payload["transport"])
                self.assertEqual("quic", payload["bulk_transport"])
                self.assertEqual("direct-buffers", payload["read_mode"])
                self.assertEqual("/stage0-mds", payload["remote_root"])
                self.assertEqual(2, payload["trainer_prefetch_batches"])
                self.assertTrue(payload["trainer_prefetch_enabled"])
                self.assertEqual(1, payload["bulk_read_retries"])
                self.assertEqual(1, payload["bulk_write_retries"])
                self.assertEqual(2, payload["runs_requested"])
                self.assertEqual(4, payload["run_count"])
                self.assertEqual(24, payload["sample_count"])
                self.assertGreater(payload["byte_size"], 0)
                self.assertGreater(payload["bytes_per_second"], 0.0)
                self.assertEqual(
                    "cxnfs.trainer-remote-client-topology-aggregate.v1",
                    payload["remote_client_topology"]["format"],
                )
                self.assertTrue(payload["remote_client_topology"]["per_source_client_complete"])
                self.assertEqual(payload["run_count"], payload["remote_client_topology"]["topology_report_count"])
                self.assertEqual(["1", "4", "1", "4"], [run["cord_label"] for run in payload["runs"]])
                self.assertTrue(payload["resume_cursor_evidence"]["complete"])
                self.assertTrue(payload["resume_cursor_evidence"]["all_runs_have_resume_cursors"])
                self.assertTrue(payload["resume_cursor_evidence"]["all_final_cursors_advance_by_sample_count"])
                self.assertTrue(all(run["transport"] == "cxnfs-quic" for run in payload["runs"]))
                self.assertTrue(all(run["trainer_prefetch_enabled"] for run in payload["runs"]))
                self.assertTrue(all(run["bulk_read_retries"] == 1 for run in payload["runs"]))
                self.assertTrue(all(run["bulk_write_retries"] == 1 for run in payload["runs"]))
                self.assertTrue(
                    all(run["remote_client_topology"]["per_source_client_observed"] for run in payload["runs"])
                )
                self.assertTrue(all("batches" not in run for run in payload["runs"]))
                self.assertTrue(payload["read_path_evidence"]["transport_frame_sink_supported"])
                _assert_direct_destination_cord_fanout(self, payload["read_path_evidence"])
                self.assertGreaterEqual(payload["read_path_evidence"]["direct_destination_read_operations"], 24)
                self.assertGreater(payload["read_path_evidence"]["max_observed_cords"], 1)
                self.assertTrue(payload["read_path_evidence"]["quic_read_transport_observed"])
                self.assertTrue(payload["read_path_evidence"]["quic_read_transport_complete"])
                self.assertEqual(8, payload["read_path_evidence"]["quic_read_transport_evidence_count"])
                self.assertGreater(payload["read_path_evidence"]["quic_read_transport_read_streams_created"], 0)
                self.assertGreater(payload["read_path_evidence"]["quic_read_transport_connection_reuse_count"], 0)
                self.assertEqual(
                    "cxnfs.trainer-read-path-reliability.v1",
                    payload["read_path_reliability"]["format"],
                )
                self.assertTrue(payload["read_path_reliability"]["complete"])
                self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
                self.assertTrue(payload["pressure_profile"]["read_path_reliability_complete"])
                self.assertEqual(0, payload["pressure_profile"]["unrecovered_read_failures"])
                self.assertTrue(payload["write_probe"]["enabled"])
                self.assertEqual(1024, payload["write_probe"]["frame_bytes"])
                self.assertTrue(payload["write_path_evidence"]["complete"])
                self.assertEqual(4, payload["write_path_evidence"]["run_count"])
                self.assertEqual(4, payload["write_path_evidence"]["verified_read_back_count"])
                self.assertTrue(payload["write_path_evidence"]["all_write_probes_verified"])
                self.assertEqual(4 * 4096, payload["write_path_evidence"]["payload_bytes_written"])
                self.assertTrue(payload["write_path_evidence"]["metadata_pending_observed"])
                self.assertEqual(2, payload["write_path_evidence"]["write_probe_cord_sweep_count"])
                self.assertEqual(["1", "4"], payload["write_path_evidence"]["write_probe_cord_labels"])
                self.assertEqual(4, payload["write_path_evidence"]["max_observed_write_cords"])
                self.assertEqual(1, payload["write_path_evidence"]["write_retry_limit"])
                self.assertEqual(0, payload["write_path_evidence"]["write_retry_attempts"])
                self.assertEqual(0, payload["write_path_evidence"]["write_retry_successes"])
                self.assertEqual(0, payload["write_path_evidence"]["bulk_transport_write_reconnects"])
                self.assertTrue(payload["write_path_evidence"]["write_retry_attempts_recovered"])
                self.assertTrue(payload["write_path_evidence"]["write_retry_reconnects_observed"])
                self.assertTrue(payload["write_path_evidence"]["quic_write_transport_complete"])
                self.assertEqual(4, payload["write_path_evidence"]["quic_write_transport_evidence_count"])
                self.assertEqual(4, payload["write_path_evidence"]["quic_write_transport_connection_reuse_count"])
                self.assertEqual(
                    4,
                    payload["write_path_evidence"]["quic_write_transport_expected_cipher_suite_count"],
                )
                self.assertTrue(
                    payload["write_path_evidence"]["quic_write_transport_all_reports_expected_cipher_suite"]
                )
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_successful_write_operations"])
                self.assertEqual(0, payload["write_path_evidence"]["quic_write_transport_failed_write_operations"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_write_streams_created"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_write_acks_received"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_write_acks_verified"])
                self.assertEqual(0, payload["write_path_evidence"]["quic_write_transport_write_ack_verification_failures"])
                _assert_aggregated_quic_write_ack_stream_proven(self, payload["write_path_evidence"])
                self.assertGreaterEqual(payload["write_path_evidence"]["quic_write_transport_max_active_write_streams"], 2)
                self.assertGreaterEqual(
                    payload["write_path_evidence"]["quic_write_transport_concurrent_stream_report_count"],
                    1,
                )
                self.assertTrue(payload["write_path_evidence"]["quic_write_transport_concurrent_streams_observed"])
                self.assertEqual(
                    4,
                    payload["write_path_evidence"]["quic_write_transport_write_stream_success_report_count"],
                )
                self.assertTrue(
                    payload["write_path_evidence"]["quic_write_transport_all_reports_write_streams_succeeded"]
                )
                self.assertTrue(payload["blockfs_gate"]["write_path_evidence_complete"])
                self.assertTrue(payload["blockfs_gate"]["write_path_gate"]["complete"])
                self.assertTrue(payload["blockfs_gate"]["read_path_gate"]["requires_quic_transport_evidence"])
                self.assertTrue(payload["blockfs_gate"]["read_path_gate"]["quic_read_transport_complete"])
                self.assertEqual(
                    8,
                    payload["blockfs_gate"]["read_path_gate"]["quic_read_transport_evidence_count"],
                )
                self.assertEqual(4, payload["blockfs_gate"]["write_path_gate"]["max_observed_write_cords"])
                self.assertTrue(payload["blockfs_gate"]["write_path_gate"]["requires_quic_transport_evidence"])
                self.assertTrue(payload["blockfs_gate"]["write_path_gate"]["quic_write_transport_complete"])
                _assert_aggregated_quic_write_ack_stream_proven(self, payload["blockfs_gate"]["write_path_gate"])
                self.assertTrue(
                    payload["blockfs_gate"]["write_path_gate"][
                        "quic_write_transport_concurrent_streams_observed"
                    ]
                )
                self.assertTrue(
                    payload["blockfs_gate"]["write_path_gate"][
                        "quic_write_transport_all_reports_write_streams_succeeded"
                    ]
                )
                self.assertGreaterEqual(
                    payload["blockfs_gate"]["write_path_gate"]["quic_write_transport_max_active_write_streams"],
                    2,
                )
                self.assertTrue(payload["network_path_summary"]["java_python_network_path_exercised"])
                self.assertTrue(payload["network_path_summary"]["remote_client_topology_complete"])
                self.assertTrue(payload["network_path_summary"]["remote_client_per_source_complete"])
                self.assertTrue(payload["network_path_summary"]["write_probe_completed"])
                self.assertEqual(2, payload["network_path_summary"]["cord_sweep_count"])
                self.assertFalse(payload["blockfs_gate"]["kernel_module_allowed"])
                self.assertIn("objective_passed_any", payload)
                _publish_report(report, "python-quic-trainer-workload-benchmark.json")
            finally:
                _stop_server(process, stop)

    @unittest.skipUnless(
        importlib.util.find_spec("torch"),
        "torch is required for QUIC trainer contract matrix and handoff integration",
    )
    def test_trainer_contract_benchmark_replays_contract_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 4))
            _write_file_dataset(data_root, "summary", ("summary-0", "summary-1"))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                contract = root / "trainer-contract-quic.json"
                contract_stdout = io.StringIO()
                contract_stderr = io.StringIO()
                contract_exit_code = run_trainer_contract_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--bulk-transport",
                        "quic",
                        "--read-mode",
                        "direct-tensors",
                        "--tensor-target",
                        "pytorch-cpu",
                        "--batch-size",
                        "3",
                        "--batches",
                        "2",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--trainer-prefetch-batches",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "6",
                        "--output",
                        str(contract),
                    ],
                    stdout=contract_stdout,
                    stderr=contract_stderr,
                )
                self.assertEqual(0, contract_exit_code, contract_stderr.getvalue())
                contract_payload = json.loads(contract.read_text(encoding="utf-8"))
                self.assertEqual("direct-tensors", contract_payload["read_contract"]["read_mode"])
                self.assertEqual("pytorch-cpu", contract_payload["read_contract"]["tensor_allocation"]["target"])
                self.assertEqual(2, contract_payload["read_contract"]["trainer_prefetch_batches"])
                self.assertEqual(1, contract_payload["network_contract"]["bulk_read_retries"])
                self.assertEqual(1, contract_payload["network_contract"]["bulk_write_retries"])
                _publish_report(contract, "python-quic-trainer-contract.json")

                matrix_stdout = io.StringIO()
                matrix_stderr = io.StringIO()
                matrix_report = root / "trainer-contract-surface-matrix-quic.json"
                matrix_exit_code = run_trainer_surface_matrix_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "5",
                        "--initial-cords",
                        "4",
                        "--max-cords",
                        "4",
                        "--provisional-container-size",
                        str(64 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--read-modes",
                        "direct-buffers,direct-tensors",
                        "--trainer-batch-iterators",
                        "native,trainer-dataloader",
                        "--data-loader-workers",
                        "0",
                        "--tensor-target",
                        "pytorch-cpu",
                        "--batch-size",
                        "3",
                        "--batches",
                        "1",
                        "--warmup-batches",
                        "1",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--trainer-prefetch-batches",
                        "2",
                        "--bulk-read-retries",
                        "1",
                        "--samples-per-epoch",
                        "6",
                        "--output",
                        str(matrix_report),
                    ],
                    stdout=matrix_stdout,
                    stderr=matrix_stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )
                self.assertEqual(0, matrix_exit_code, matrix_stderr.getvalue() + matrix_stdout.getvalue())
                matrix_payload = json.loads(matrix_stdout.getvalue())
                self.assertEqual("cxnfs.trainer-surface-matrix.v1", matrix_payload["format"])
                self.assertTrue(matrix_payload["passed"])
                self.assertEqual(2, matrix_payload["trainer_prefetch_batches"])
                self.assertEqual(1, matrix_payload["bulk_read_retries"])
                self.assertEqual(
                    "cxnfs.trainer-remote-client-topology-aggregate.v1",
                    matrix_payload["remote_client_topology"]["format"],
                )
                self.assertTrue(matrix_payload["remote_client_topology"]["per_source_client_complete"])
                self.assertEqual(4, matrix_payload["remote_client_topology"]["topology_report_count"])
                self.assertEqual(
                    contract_payload["workload_fingerprint"]["sha256"],
                    matrix_payload["workload_fingerprint"]["sha256"],
                )
                _publish_report(matrix_report, "python-quic-trainer-contract-surface-matrix.json")

                stdout = io.StringIO()
                stderr = io.StringIO()
                report = root / "trainer-contract-workload-quic.json"

                exit_code = run_trainer_contract_benchmark_cli(
                    [
                        str(contract),
                        "--workload-name",
                        "java-quic-contract-weighted-mds",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "10",
                        "--cord-sweep",
                        "1,4",
                        "--provisional-container-size",
                        str(512 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--warmup-batches",
                        "1",
                        "--runs",
                        "2",
                        "--objective",
                        "ai-training-25gbe",
                        "--baseline-mb-s",
                        "1.0",
                        "--write-probe-bytes",
                        "4096",
                        "--write-probe-frame-bytes",
                        "1024",
                        "--write-probe-root",
                        "/trainer-write-probes",
                        "--output",
                        str(report),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                    credential_prompt=lambda: ClientCredentials("trainer", "secret"),
                    tls_context_factory=lambda cafile: _server_verified_tls13_context(cafile),
                )

                self.assertEqual(0, exit_code, stderr.getvalue())
                self.assertEqual(stdout.getvalue(), report.read_text(encoding="utf-8"))
                payload = json.loads(stdout.getvalue())
                self.assertEqual("cxnfs.trainer-workload-benchmark.v1", payload["format"])
                self.assertEqual("java-quic-contract-weighted-mds", payload["workload_name"])
                self.assertTrue(payload["contract_applied"])
                self.assertEqual("cxnfs.trainer-contract.v1", payload["trainer_contract"]["format"])
                self.assertEqual(str(contract), payload["trainer_contract"]["path"])
                self.assertTrue(payload["trainer_contract"]["validated"])
                self.assertEqual("cxnfs-quic", payload["transport"])
                self.assertEqual("quic", payload["bulk_transport"])
                self.assertEqual("direct-tensors", payload["read_mode"])
                self.assertEqual("pytorch-cpu", payload["tensor_allocation"]["target"])
                self.assertEqual("/stage0-mds", payload["remote_root"])
                self.assertEqual(2, payload["trainer_prefetch_batches"])
                self.assertTrue(payload["trainer_prefetch_enabled"])
                self.assertEqual(1, payload["bulk_read_retries"])
                self.assertEqual(1, payload["bulk_write_retries"])
                self.assertEqual(4, payload["run_count"])
                self.assertEqual(24, payload["sample_count"])
                self.assertEqual(
                    "cxnfs.trainer-remote-client-topology-aggregate.v1",
                    payload["remote_client_topology"]["format"],
                )
                self.assertTrue(payload["remote_client_topology"]["per_source_client_complete"])
                self.assertEqual(payload["run_count"], payload["remote_client_topology"]["topology_report_count"])
                self.assertEqual(["1", "4", "1", "4"], [run["cord_label"] for run in payload["runs"]])
                self.assertTrue(all(run["trainer_prefetch_enabled"] for run in payload["runs"]))
                self.assertTrue(all(run["bulk_read_retries"] == 1 for run in payload["runs"]))
                self.assertTrue(all(run["bulk_write_retries"] == 1 for run in payload["runs"]))
                self.assertTrue(
                    all(run["remote_client_topology"]["per_source_client_observed"] for run in payload["runs"])
                )
                self.assertTrue(payload["resume_cursor_evidence"]["complete"])
                self.assertTrue(payload["resume_cursor_evidence"]["all_runs_have_resume_cursors"])
                self.assertTrue(payload["resume_cursor_evidence"]["all_final_cursors_advance_by_sample_count"])
                self.assertTrue(payload["read_path_evidence"]["transport_frame_sink_supported"])
                _assert_direct_destination_cord_fanout(self, payload["read_path_evidence"])
                self.assertGreaterEqual(payload["read_path_evidence"]["direct_destination_read_operations"], 24)
                self.assertGreater(payload["read_path_evidence"]["max_observed_cords"], 1)
                self.assertEqual(
                    "cxnfs.trainer-read-path-reliability.v1",
                    payload["read_path_reliability"]["format"],
                )
                self.assertTrue(payload["read_path_reliability"]["complete"])
                self.assertEqual(0, payload["read_path_reliability"]["unrecovered_read_failures"])
                self.assertTrue(payload["pressure_profile"]["read_path_reliability_complete"])
                self.assertEqual(0, payload["pressure_profile"]["unrecovered_read_failures"])
                self.assertTrue(payload["network_path_summary"]["java_python_network_path_exercised"])
                self.assertTrue(payload["network_path_summary"]["remote_client_topology_complete"])
                self.assertTrue(payload["network_path_summary"]["remote_client_per_source_complete"])
                self.assertTrue(payload["write_probe"]["enabled"])
                self.assertEqual(1024, payload["write_probe"]["frame_bytes"])
                self.assertTrue(payload["write_path_evidence"]["complete"])
                self.assertEqual(4, payload["write_path_evidence"]["run_count"])
                self.assertEqual(4, payload["write_path_evidence"]["verified_read_back_count"])
                self.assertTrue(payload["write_path_evidence"]["all_write_probes_verified"])
                self.assertEqual(4 * 4096, payload["write_path_evidence"]["payload_bytes_written"])
                self.assertEqual(2, payload["write_path_evidence"]["write_probe_cord_sweep_count"])
                self.assertEqual(["1", "4"], payload["write_path_evidence"]["write_probe_cord_labels"])
                self.assertEqual(4, payload["write_path_evidence"]["max_observed_write_cords"])
                self.assertEqual(1, payload["write_path_evidence"]["write_retry_limit"])
                self.assertEqual(0, payload["write_path_evidence"]["write_retry_attempts"])
                self.assertEqual(0, payload["write_path_evidence"]["write_retry_successes"])
                self.assertEqual(0, payload["write_path_evidence"]["bulk_transport_write_reconnects"])
                self.assertTrue(payload["write_path_evidence"]["write_retry_attempts_recovered"])
                self.assertTrue(payload["write_path_evidence"]["write_retry_reconnects_observed"])
                self.assertTrue(payload["write_path_evidence"]["quic_write_transport_complete"])
                self.assertEqual(4, payload["write_path_evidence"]["quic_write_transport_evidence_count"])
                self.assertEqual(4, payload["write_path_evidence"]["quic_write_transport_connection_reuse_count"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_successful_write_operations"])
                self.assertEqual(0, payload["write_path_evidence"]["quic_write_transport_failed_write_operations"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_write_streams_created"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_write_acks_received"])
                self.assertEqual(16, payload["write_path_evidence"]["quic_write_transport_write_acks_verified"])
                self.assertEqual(0, payload["write_path_evidence"]["quic_write_transport_write_ack_verification_failures"])
                _assert_aggregated_quic_write_ack_stream_proven(self, payload["write_path_evidence"])
                self.assertGreaterEqual(payload["write_path_evidence"]["quic_write_transport_max_active_write_streams"], 2)
                self.assertTrue(payload["write_path_evidence"]["quic_write_transport_concurrent_streams_observed"])
                self.assertEqual(
                    4,
                    payload["write_path_evidence"]["quic_write_transport_write_stream_success_report_count"],
                )
                self.assertTrue(
                    payload["write_path_evidence"]["quic_write_transport_all_reports_write_streams_succeeded"]
                )
                self.assertTrue(payload["blockfs_gate"]["write_path_evidence_complete"])
                self.assertTrue(payload["blockfs_gate"]["write_path_gate"]["complete"])
                self.assertEqual(4, payload["blockfs_gate"]["write_path_gate"]["max_observed_write_cords"])
                self.assertTrue(payload["blockfs_gate"]["write_path_gate"]["requires_quic_transport_evidence"])
                self.assertTrue(payload["blockfs_gate"]["write_path_gate"]["quic_write_transport_complete"])
                _assert_aggregated_quic_write_ack_stream_proven(self, payload["blockfs_gate"]["write_path_gate"])
                self.assertTrue(
                    payload["blockfs_gate"]["write_path_gate"][
                        "quic_write_transport_concurrent_streams_observed"
                    ]
                )
                self.assertTrue(
                    payload["blockfs_gate"]["write_path_gate"][
                        "quic_write_transport_all_reports_write_streams_succeeded"
                    ]
                )
                self.assertGreaterEqual(
                    payload["blockfs_gate"]["write_path_gate"]["quic_write_transport_max_active_write_streams"],
                    2,
                )
                self.assertTrue(payload["network_path_summary"]["write_probe_completed"])
                self.assertFalse(payload["blockfs_gate"]["kernel_module_allowed"])
                handoff_payload = build_trainer_handoff_payload(
                    contract_payload,
                    contract_path=contract,
                    workload_payload=payload,
                    workload_path=report,
                    matrix_payload=matrix_payload,
                    matrix_path=matrix_report,
                    batch_direct_read_payload=_complete_batch_direct_read_proof(),
                    batch_direct_read_path=root / "python-quic-batch-direct-read.json",
                    batch_direct_tensor_read_payload=_complete_batch_direct_tensor_read_proof(),
                    batch_direct_tensor_read_path=root / "python-quic-batch-direct-tensor-read.json",
                    require_low_level_direct_read_proofs=True,
                    require_low_level_direct_write_proofs=True,
                    require_remote=True,
                    require_quic=True,
                    require_workload=True,
                    require_matrix=True,
                    min_sources=2,
                    min_validation_samples=6,
                    min_validation_bytes=786000,
                    min_workload_runs=4,
                    min_workload_samples=24,
                    min_workload_bytes=3145728,
                    min_cord_sweep=2,
                )
                self.assertTrue(handoff_payload["ready_for_trainer_wiring"])
                self.assertTrue(handoff_payload["ready_for_remote_quic"])
                plan = CxnfsTrainerIntegrationPlan.from_handoff_payload(
                    handoff_payload,
                    require_remote_quic=True,
                )
                with ClientCredentials("trainer", "secret") as credentials:
                    network_preflight_payload = build_trainer_network_preflight_payload(
                        handoff_payload,
                        credentials,
                        require_remote_quic=True,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        read_length=1,
                        bulk_transport="quic",
                    )
                self.assertEqual("cxnfs.trainer-network-preflight.v1", network_preflight_payload["format"])
                self.assertTrue(network_preflight_payload["passed"])
                self.assertTrue(network_preflight_payload["ready_for_remote_quic"])
                self.assertEqual("cxnfs-quic", network_preflight_payload["transport"])
                self.assertEqual("quic", network_preflight_payload["bulk_transport"])
                self.assertEqual("ok", network_preflight_payload["bulk_probe"])
                self.assertEqual(1, network_preflight_payload["bulk_probe_bytes"])
                self.assertEqual("/stage0-mds", network_preflight_payload["probe_path"])
                self.assertEqual("/stage0-mds/chat/chat-000000.mds", network_preflight_payload["bulk_probe_path"])
                self.assertTrue(network_preflight_payload["opens_network_connections"])
                self.assertFalse(network_preflight_payload["consumes_batches"])
                self.assertTrue(network_preflight_payload["safe_to_run_before_trainer_workers"])
                self.assertTrue(network_preflight_payload["requires_credentials"])
                self.assertFalse(network_preflight_payload["raw_password_sent_over_control"])
                network_preflight_report = root / "trainer-network-preflight-quic.json"
                network_preflight_report.write_text(
                    json.dumps(network_preflight_payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                _publish_report(network_preflight_report, "python-quic-trainer-network-preflight.json")
                with ClientCredentials("trainer", "secret") as credentials:
                    with plan.open_remote_stream(
                        credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                    ) as stream:
                        direct_batch = stream.next_batch()
                        direct_stream_state = stream.state_dict()
                self.assertEqual(3, direct_batch.sample_count)
                self.assertEqual({"chat": 2, "summary": 1}, direct_batch.source_counts)
                self.assertEqual(TRAINER_CURSOR_FORMAT, direct_batch.resume_cursor.to_dict()["format"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", direct_stream_state["format"])
                with ClientCredentials("trainer", "secret") as credentials:
                    client_factory = plan.create_remote_client_factory(
                        credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                    )
                    smoke_payload = build_trainer_integration_smoke_payload(
                        handoff_payload,
                        batches=2,
                        require_remote_quic=True,
                        client_factory=client_factory,
                    )
                self.assertEqual("cxnfs.trainer-integration-smoke.v1", smoke_payload["format"])
                self.assertTrue(smoke_payload["passed"])
                self.assertTrue(smoke_payload["remote_client_required"])
                self.assertEqual("cxnfs-quic", smoke_payload["stream_report"]["transport"])
                self.assertEqual("quic", smoke_payload["stream_report"]["bulk_transport"])
                self.assertEqual("/stage0-mds", smoke_payload["stream_report"]["remote_root"])
                self.assertEqual(2, smoke_payload["batch_count"])
                self.assertEqual(6, smoke_payload["sample_count"])
                self.assertEqual(2, smoke_payload["trainer_prefetch_batches"])
                self.assertEqual(
                    "CxnfsTrainerIntegrationPlan.open_training_loop",
                    smoke_payload["training_loop_context"],
                )
                self.assertTrue(smoke_payload["training_loop_context_used"])
                self.assertEqual("CxnfsTrainerPrefetchStream", smoke_payload["training_loop_object"])
                self.assertEqual("CxnfsTrainerStream", smoke_payload["training_loop_base_stream_object"])
                self.assertEqual(2, smoke_payload["training_loop_prefetch_batches"])
                self.assertTrue(smoke_payload["training_loop_prefetch_applied"])
                self.assertTrue(smoke_payload["stream_opened"])
                self.assertTrue(smoke_payload["stream_kwargs_accepted"])
                self.assertTrue(smoke_payload["stream_state_matches_final_cursor"])
                self.assertTrue(smoke_payload["stream_report"]["remote_client_topology"]["per_source_client_observed"])
                self.assertTrue(smoke_payload["stream_report"]["remote_client_topology"]["complete"])
                _assert_direct_destination_cord_fanout(
                    self,
                    smoke_payload["stream_report"]["read_path_evidence"],
                )
                self.assertEqual(0, smoke_payload["read_path_reliability"]["unrecovered_read_failures"])
                smoke_report = root / "trainer-integration-smoke-quic.json"
                smoke_report.write_text(json.dumps(smoke_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(smoke_report, "python-quic-trainer-integration-smoke.json")
                with ClientCredentials("trainer", "secret") as credentials:
                    runtime_client_factory = plan.create_remote_client_factory(
                        credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                    )
                    try:
                        runtime_payload = build_trainer_runtime_smoke_payload(
                            handoff_payload,
                            batches=2,
                            rank=0,
                            world_size=1,
                            require_remote_quic=True,
                            client_factory=runtime_client_factory,
                        )
                    finally:
                        runtime_client_factory.close()
                self.assertEqual("cxnfs.trainer-runtime-smoke.v1", runtime_payload["format"])
                self.assertTrue(runtime_payload["passed"])
                self.assertTrue(runtime_payload["remote_client_required"])
                self.assertEqual("cxnfs.trainer-runtime-binding.v1", runtime_payload["runtime_binding_format"])
                self.assertEqual("CxnfsTrainerRuntimeBinding", runtime_payload["runtime_binding_object"])
                self.assertEqual("direct-tensors", runtime_payload["read_mode"])
                self.assertEqual("CxnfsTrainerRuntimeBinding.open_training_loop", runtime_payload["training_loop_context"])
                self.assertTrue(runtime_payload["pipeline_training_loop_required"])
                self.assertTrue(runtime_payload["pipeline_training_loop_verified"])
                self.assertEqual(
                    "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                    runtime_payload["pipeline_training_loop_context"],
                )
                self.assertTrue(runtime_payload["pipeline_training_loop_context_used"])
                self.assertEqual("CxnfsTrainerPipelineLoop", runtime_payload["pipeline_training_loop_object"])
                self.assertEqual({"index": 0, "count": 1}, runtime_payload["training_shard"])
                self.assertEqual("cxnfs-quic", runtime_payload["stream_report"]["transport"])
                self.assertEqual("quic", runtime_payload["stream_report"]["bulk_transport"])
                self.assertEqual("/stage0-mds", runtime_payload["stream_report"]["remote_root"])
                self.assertEqual(2, runtime_payload["batch_count"])
                self.assertEqual(6, runtime_payload["sample_count"])
                self.assertEqual(2, runtime_payload["trainer_prefetch_batches"])
                self.assertTrue(runtime_payload["client_factory_injected"])
                self.assertFalse(runtime_payload["credential_prompt_used"])
                self.assertTrue(runtime_payload["stream_report"]["remote_client_topology"]["per_source_client_observed"])
                self.assertTrue(runtime_payload["stream_report"]["remote_client_topology_after_close"]["all_open_clients_closed"])
                self.assertTrue(runtime_payload["stream_report"]["remote_client_lifecycle_closed"])
                _assert_direct_destination_cord_fanout(
                    self,
                    runtime_payload["stream_report"]["read_path_evidence"],
                )
                self.assertEqual(0, runtime_payload["read_path_reliability"]["unrecovered_read_failures"])
                runtime_report = root / "trainer-runtime-smoke-quic.json"
                runtime_report.write_text(json.dumps(runtime_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(runtime_report, "python-quic-trainer-runtime-smoke.json")
                with ClientCredentials("trainer", "secret") as credentials:
                    distributed_runtime_payload = build_trainer_distributed_runtime_smoke_payload(
                        handoff_payload,
                        world_size=2,
                        batches_per_rank=2,
                        require_remote_quic=True,
                        credentials=credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                    )
                distributed_rank_states_dir = root / "distributed-runtime-rank-states"
                distributed_state_artifacts = write_trainer_distributed_runtime_rank_states(
                    distributed_runtime_payload,
                    distributed_rank_states_dir,
                )
                self.assertEqual("cxnfs.trainer-distributed-runtime-smoke.v1", distributed_runtime_payload["format"])
                self.assertTrue(distributed_runtime_payload["passed"])
                self.assertTrue(distributed_runtime_payload["remote_client_required"])
                self.assertEqual("cxnfs.trainer-runtime-smoke.v1", distributed_runtime_payload["runtime_smoke_format"])
                self.assertEqual("cxnfs.trainer-runtime-binding.v1", distributed_runtime_payload["runtime_binding_format"])
                self.assertEqual(
                    "CxnfsTrainerRuntimeBinding.open_training_loop",
                    distributed_runtime_payload["training_loop_context"],
                )
                self.assertEqual(2, distributed_runtime_payload["world_size"])
                self.assertEqual([0, 1], distributed_runtime_payload["ranks"])
                self.assertEqual(2, distributed_runtime_payload["rank_count"])
                self.assertEqual(4, distributed_runtime_payload["total_batch_count"])
                self.assertEqual(12, distributed_runtime_payload["total_sample_count"])
                self.assertEqual("cxnfs-quic", distributed_runtime_payload["transport"])
                self.assertEqual("quic", distributed_runtime_payload["bulk_transport"])
                self.assertTrue(distributed_runtime_payload["all_rank_streams_opened"])
                self.assertTrue(distributed_runtime_payload["all_rank_stream_kwargs_accepted"])
                self.assertTrue(distributed_runtime_payload["all_rank_state_matches_final_cursor"])
                self.assertTrue(distributed_runtime_payload["all_rank_training_loop_context_used"])
                self.assertTrue(distributed_runtime_payload["all_rank_training_shards_correct"])
                self.assertTrue(distributed_runtime_payload["rank_sample_overlap"]["no_cross_rank_overlap"])
                self.assertTrue(distributed_runtime_payload["remote_client_topology_after_close"]["complete"])
                self.assertTrue(distributed_runtime_payload["remote_client_topology_after_close"]["all_open_clients_closed"])
                self.assertTrue(distributed_runtime_payload["all_rank_state_files_written"])
                self.assertEqual(2, distributed_runtime_payload["rank_state_file_count"])
                self.assertEqual(2, distributed_state_artifacts["rank_state_file_count"])
                for rank in (0, 1):
                    state_path = distributed_rank_states_dir / f"rank-{rank:05d}.trainer-stream-state.json"
                    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
                    self.assertEqual(TRAINER_STREAM_STATE_FORMAT, state_payload["format"])
                    self.assertEqual({"index": rank, "count": 2}, state_payload["cursor"]["shard"])
                self.assertEqual(0, distributed_runtime_payload["read_path_reliability"]["unrecovered_read_failures"])
                distributed_runtime_report = root / "trainer-distributed-runtime-smoke-quic.json"
                distributed_runtime_report.write_text(
                    json.dumps(distributed_runtime_payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                _publish_report(distributed_runtime_report, "python-quic-trainer-distributed-runtime-smoke.json")
                with ClientCredentials("trainer", "secret") as credentials:
                    loop_client_factory = plan.create_remote_client_factory(
                        credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                    )
                    try:
                        loop_payload = build_trainer_loop_replay_payload(
                            handoff_payload,
                            steps=2,
                            gradient_accumulation_steps=1,
                            require_remote_quic=True,
                            client_factory=loop_client_factory,
                        )
                    finally:
                        loop_client_factory.close()
                self.assertEqual("cxnfs.trainer-loop-replay.v1", loop_payload["format"])
                self.assertTrue(loop_payload["passed"])
                self.assertTrue(loop_payload["remote_client_required"])
                self.assertEqual("cxnfs-quic", loop_payload["stream_report"]["transport"])
                self.assertEqual("quic", loop_payload["stream_report"]["bulk_transport"])
                self.assertEqual("/stage0-mds", loop_payload["stream_report"]["remote_root"])
                self.assertEqual(2, loop_payload["step_count"])
                self.assertEqual(2, loop_payload["batch_count"])
                self.assertEqual(6, loop_payload["sample_count"])
                self.assertTrue(loop_payload["weighted_source_coverage_complete"])
                self.assertTrue(loop_payload["resume_replayed"])
                self.assertTrue(loop_payload["resume_continuity_verified"])
                self.assertEqual(0, loop_payload["first_initial_emitted"])
                self.assertEqual(3, loop_payload["first_final_emitted"])
                self.assertEqual(3, loop_payload["second_initial_emitted"])
                self.assertEqual(6, loop_payload["final_emitted"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", loop_payload["resume_state"]["format"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", loop_payload["final_state"]["format"])
                self.assertEqual(
                    "CxnfsTrainerIntegrationPlan.open_training_loop",
                    loop_payload["training_loop_context"],
                )
                self.assertTrue(loop_payload["training_loop_context_used"])
                self.assertEqual("CxnfsTrainerPrefetchStream", loop_payload["training_loop_object"])
                self.assertEqual("CxnfsTrainerStream", loop_payload["training_loop_base_stream_object"])
                self.assertEqual(2, loop_payload["training_loop_prefetch_batches"])
                self.assertTrue(loop_payload["training_loop_prefetch_applied"])
                self.assertTrue(loop_payload["stream_report"]["remote_client_topology"]["per_source_client_observed"])
                self.assertTrue(loop_payload["stream_report"]["remote_client_topology"]["complete"])
                _assert_direct_destination_cord_fanout(
                    self,
                    loop_payload["stream_report"]["read_path_evidence"],
                )
                self.assertEqual(0, loop_payload["stream_report"]["read_path_reliability"]["unrecovered_read_failures"])
                loop_report = root / "trainer-loop-replay-quic.json"
                loop_report.write_text(json.dumps(loop_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                _publish_report(loop_report, "python-quic-trainer-loop-replay.json")
                with ClientCredentials("trainer", "secret") as credentials:
                    readiness_payload = build_trainer_readiness_bundle_payload(
                        handoff_payload,
                        output_dir=root / "trainer-readiness-bundle",
                        integration_batches=1,
                        runtime_batches=1,
                        runtime_rank=0,
                        world_size=2,
                        batches_per_rank=1,
                        loop_steps=2,
                        trainer_prefetch_batches=2,
                        require_remote_quic=True,
                        credentials=credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                        bulk_client_factory=CxnfsQuicBulkClient.connect,
                    )
                self.assertEqual("cxnfs.trainer-readiness-bundle.v1", readiness_payload["format"])
                self.assertTrue(readiness_payload["passed"])
                self.assertTrue(readiness_payload["ready_for_trainer_wiring"])
                self.assertTrue(readiness_payload["ready_for_remote_quic"])
                self.assertTrue(readiness_payload["remote_client_required"])
                self.assertTrue(readiness_payload["low_level_direct_write_proofs_complete"])
                self.assertTrue(readiness_payload["write_probe_proof_complete"])
                self.assertEqual(
                    "TLS_AES_128_GCM_SHA256",
                    readiness_payload["write_probe_cipher_suite"],
                )
                self.assertTrue(readiness_payload["write_probe_quic_transport_complete"])
                self.assertTrue(readiness_payload["direct_quic_memory_path_verified"])
                self.assertTrue(readiness_payload["direct_python_streaming_verified"])
                self.assertTrue(readiness_payload["runtime_training_loop_verified"])
                self.assertTrue(readiness_payload["pipeline_training_loop_required"])
                self.assertTrue(readiness_payload["pipeline_training_loop_verified"])
                self.assertTrue(readiness_payload["distributed_runtime_verified"])
                self.assertTrue(readiness_payload["distributed_rank_states_written"])
                self.assertTrue(readiness_payload["loop_resume_verified"])
                self.assertFalse(readiness_payload["raw_password_sent_over_control"])
                self.assertEqual(5, readiness_payload["report_artifacts"]["artifact_count"])
                self.assertEqual(
                    "cxnfs.trainer-distributed-runtime-rank-states.v1",
                    readiness_payload["rank_state_artifacts"]["format"],
                )
                self.assertTrue(readiness_payload["rank_state_artifacts"]["all_rank_state_files_written"])
                readiness_report = root / "trainer-readiness-bundle-quic.json"
                readiness_report.write_text(
                    json.dumps(readiness_payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                _publish_report(readiness_report, "python-quic-trainer-readiness-bundle.json")
                with ClientCredentials("trainer", "secret") as credentials:
                    handoff_acceptance_payload = build_trainer_handoff_acceptance_payload(
                        handoff_payload,
                        handoff_path=root / "trainer-handoff-quic.json",
                        output_dir=root / "trainer-handoff-acceptance",
                        integration_batches=1,
                        runtime_batches=1,
                        runtime_rank=0,
                        world_size=2,
                        batches_per_rank=1,
                        loop_steps=2,
                        trainer_prefetch_batches=2,
                        require_remote_quic=True,
                        credentials=credentials,
                        host="localhost",
                        control_port=control_port,
                        transport_port=transport_port,
                        timeout=10.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                        initial_cords=4,
                        max_cords=4,
                        provisional_container_size=512 * 1024,
                        expected_size_limit=1024 * 1024,
                        bulk_read_retries=1,
                        bulk_write_retries=1,
                        bulk_transport="quic",
                        cafile=str(cert),
                        bulk_client_factory=CxnfsQuicBulkClient.connect,
                    )
                self.assertEqual("cxnfs.trainer-handoff-acceptance.v1", handoff_acceptance_payload["format"])
                self.assertTrue(handoff_acceptance_payload["passed"])
                self.assertTrue(handoff_acceptance_payload["ready_for_remote_quic"])
                self.assertTrue(handoff_acceptance_payload["direct_quic_memory_path_verified"])
                self.assertTrue(handoff_acceptance_payload["remote_client_required"])
                self.assertTrue(handoff_acceptance_payload["requires_credentials"])
                self.assertFalse(handoff_acceptance_payload["raw_password_sent_over_control"])
                self.assertTrue(handoff_acceptance_payload["direct_python_streaming_verified"])
                self.assertTrue(handoff_acceptance_payload["runtime_training_loop_verified"])
                self.assertTrue(handoff_acceptance_payload["pipeline_training_loop_required"])
                self.assertTrue(handoff_acceptance_payload["pipeline_training_loop_verified"])
                self.assertEqual(
                    "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                    handoff_acceptance_payload["pipeline_training_loop_context"],
                )
                self.assertTrue(handoff_acceptance_payload["distributed_runtime_verified"])
                self.assertTrue(handoff_acceptance_payload["loop_resume_verified"])
                self.assertTrue(handoff_acceptance_payload["cross_rank_overlap_free"])
                self.assertTrue(handoff_acceptance_payload["rank_states_written"])
                self.assertEqual(
                    "CxnfsTrainerRuntimeBinding.open_training_loop",
                    handoff_acceptance_payload["training_loop_context"],
                )
                handoff_acceptance_report = root / "trainer-handoff-acceptance-quic.json"
                handoff_acceptance_report.write_text(
                    json.dumps(handoff_acceptance_payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                _publish_report(handoff_acceptance_report, "python-quic-trainer-handoff-acceptance.json")
                _publish_report(report, "python-quic-trainer-contract-workload-benchmark.json")
            finally:
                _stop_server(process, stop)

    @unittest.skipUnless(importlib.util.find_spec("torch"), "torch is required for finite trainer DataLoader worker replay")
    def test_trainer_worker_loop_replay_reads_finite_mds_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", _large_values("chat", 6))
            _write_file_dataset(data_root, "summary", _large_values("summary", 4))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                contract = root / "trainer-worker-contract-quic.json"
                contract_stdout = io.StringIO()
                contract_stderr = io.StringIO()
                contract_exit_code = run_trainer_contract_cli(
                    [
                        str(stage0),
                        "--weights",
                        "chat=2,summary=1",
                        "--remote-root",
                        "/stage0-mds",
                        "--bulk-transport",
                        "quic",
                        "--read-mode",
                        "direct-buffers",
                        "--batch-size",
                        "2",
                        "--batches",
                        "4",
                        "--rounds-per-read",
                        "2",
                        "--source-read-workers",
                        "2",
                        "--prefetch-chunks",
                        "2",
                        "--trainer-prefetch-batches",
                        "0",
                        "--bulk-read-retries",
                        "1",
                        "--trainer-batch-iterator",
                        "trainer-dataloader",
                        "--data-loader-workers",
                        "2",
                        "--samples-per-epoch",
                        "8",
                        "--no-repeat",
                        "--output",
                        str(contract),
                    ],
                    stdout=contract_stdout,
                    stderr=contract_stderr,
                )
                self.assertEqual(0, contract_exit_code, contract_stderr.getvalue())
                contract_payload = json.loads(contract.read_text(encoding="utf-8"))
                self.assertEqual("trainer-dataloader", contract_payload["read_contract"]["trainer_batch_iterator"])
                self.assertEqual(2, contract_payload["read_contract"]["data_loader_workers"])
                self.assertFalse(contract_payload["read_contract"]["repeat"])
                self.assertEqual(8, contract_payload["read_contract"]["samples_per_epoch"])

                handoff = root / "trainer-worker-handoff-quic.json"
                batch_direct_read = root / "trainer-worker-batch-direct-read-quic.json"
                batch_direct_tensor_read = root / "trainer-worker-batch-direct-tensor-read-quic.json"
                batch_direct_read.write_text(
                    json.dumps(_complete_batch_direct_read_proof(), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                batch_direct_tensor_read.write_text(
                    json.dumps(_complete_batch_direct_tensor_read_proof(), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                handoff_stdout = io.StringIO()
                handoff_stderr = io.StringIO()
                handoff_exit_code = run_trainer_handoff_cli(
                    [
                        str(contract),
                        "--batch-direct-read-report",
                        str(batch_direct_read),
                        "--batch-direct-tensor-read-report",
                        str(batch_direct_tensor_read),
                        "--require-remote",
                        "--require-quic",
                        "--min-sources",
                        "2",
                        "--min-validation-samples",
                        "8",
                        "--min-validation-bytes",
                        "1",
                        "--output",
                        str(handoff),
                    ],
                    stdout=handoff_stdout,
                    stderr=handoff_stderr,
                )
                self.assertEqual(0, handoff_exit_code, handoff_stderr.getvalue())
                handoff_payload = json.loads(handoff.read_text(encoding="utf-8"))
                self.assertTrue(handoff_payload["ready_for_trainer_wiring"])
                self.assertTrue(handoff_payload["ready_for_remote_quic"])
                self.assertFalse(handoff_payload["requires_mount"])
                self.assertFalse(handoff_payload["touches_trainer_code"])
                stream_kwargs = handoff_payload["integration_plan"]["stream_kwargs"]
                self.assertEqual("trainer-dataloader", stream_kwargs["trainer_batch_iterator"])
                self.assertEqual(2, stream_kwargs["data_loader_workers"])
                self.assertFalse(stream_kwargs["repeat"])
                self.assertEqual(8, stream_kwargs["samples_per_epoch"])

                replay = root / "trainer-worker-loop-replay-quic.json"
                resume_state = root / "trainer-worker-loop-replay-resume-state.json"
                final_state = root / "trainer-worker-loop-replay-final-state.json"
                replay_stdout = io.StringIO()
                replay_stderr = io.StringIO()
                replay_exit_code = run_trainer_loop_replay_cli(
                    [
                        str(handoff),
                        "--steps",
                        "4",
                        "--resume-after-steps",
                        "2",
                        "--require-remote-quic",
                        "--host",
                        "localhost",
                        "--control-port",
                        str(control_port),
                        "--transport-port",
                        str(transport_port),
                        "--bulk-transport",
                        "quic",
                        "--server-hostname",
                        "localhost",
                        "--cafile",
                        str(cert),
                        "--timeout",
                        "10",
                        "--initial-cords",
                        "4",
                        "--max-cords",
                        "4",
                        "--provisional-container-size",
                        str(64 * 1024),
                        "--expected-size-limit",
                        str(1024 * 1024),
                        "--bulk-read-retries",
                        "1",
                        "--bulk-write-retries",
                        "1",
                        "--write-resume-state",
                        str(resume_state),
                        "--write-final-state",
                        str(final_state),
                        "--output",
                        str(replay),
                    ],
                    stdout=replay_stdout,
                    stderr=replay_stderr,
                    credential_prompt=_trainer_credentials,
                    tls_context_factory=_trainer_tls_context,
                )
                self.assertEqual(0, replay_exit_code, replay_stderr.getvalue() + replay_stdout.getvalue())
                payload = json.loads(replay.read_text(encoding="utf-8"))
                self.assertEqual("cxnfs.trainer-loop-replay.v1", payload["format"])
                self.assertTrue(payload["passed"])
                self.assertTrue(payload["remote_client_required"])
                self.assertEqual("cxnfs-quic", payload["stream_report"]["transport"])
                self.assertEqual("quic", payload["stream_report"]["bulk_transport"])
                self.assertEqual("/stage0-mds", payload["stream_report"]["remote_root"])
                self.assertEqual("direct-buffers", payload["read_mode"])
                self.assertEqual("trainer-dataloader", payload["trainer_batch_iterator"])
                self.assertEqual(2, payload["data_loader_workers"])
                self.assertFalse(payload["repeat"])
                self.assertEqual(8, payload["samples_per_epoch"])
                self.assertEqual(4, payload["step_count"])
                self.assertEqual(4, payload["batch_count"])
                self.assertEqual(8, payload["sample_count"])
                self.assertTrue(payload["weighted_source_coverage_complete"])
                self.assertTrue(payload["resume_replayed"])
                self.assertTrue(payload["resume_continuity_verified"])
                self.assertEqual(0, payload["first_initial_emitted"])
                self.assertEqual(4, payload["first_final_emitted"])
                self.assertEqual(4, payload["second_initial_emitted"])
                self.assertEqual(8, payload["final_emitted"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", payload["resume_state"]["format"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", payload["final_state"]["format"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", json.loads(resume_state.read_text(encoding="utf-8"))["format"])
                self.assertEqual("cxnfs.trainer-stream-state.v1", json.loads(final_state.read_text(encoding="utf-8"))["format"])
                self.assertTrue(payload["worker_partitioning_enabled"])
                self.assertEqual(2, payload["worker_cursor_count"])
                worker_partition = payload["worker_partition_evidence"]
                self.assertTrue(worker_partition["worker_sharding_enabled"])
                self.assertTrue(worker_partition["complete"])
                self.assertEqual(2, worker_partition["data_loader_workers"])
                self.assertEqual(0, worker_partition["duplicate_source_sample_count"])
                read_evidence = payload["stream_report"]["read_path_evidence"]
                self.assertTrue(read_evidence["worker_batch_read_evidence_observed"])
                self.assertGreaterEqual(read_evidence["worker_batch_read_evidence_count"], 2)
                self.assertTrue(
                    any(
                        report["produced_inside_torch_worker"]
                        for report in read_evidence["worker_process_read_path_evidence"]
                    )
                )
                _assert_direct_destination_cord_fanout(self, read_evidence)
                self.assertEqual(0, payload["stream_report"]["read_path_reliability"]["unrecovered_read_failures"])
                _publish_report(replay, "python-quic-trainer-worker-loop-replay.json")
            finally:
                _stop_server(process, stop)

    def test_direct_tensor_batches_read_weighted_mds_over_java_quic(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "served"
            stage0 = data_root / "stage0-mds"
            _write_file_dataset(data_root, "chat", ("chat-0", "chat-1"))
            _write_file_dataset(data_root, "summary", ("summary-0",))
            config, password, key_store, cert, ready, stop, log = _server_files(root)
            control_port = _free_tcp_port()
            transport_port = _free_udp_port()
            process = _start_java_quic_server(
                data_root,
                config,
                key_store,
                password,
                ready,
                stop,
                log,
                control_port,
                transport_port,
            )
            try:
                _wait_for_ready(ready, process, log)
                credentials = ClientCredentials("trainer", "secret")

                def client_factory(_name):
                    control = CxnfsControlClient.connect(
                        "localhost",
                        control_port,
                        timeout=5.0,
                        tls_context=_server_verified_tls13_context(str(cert)),
                        server_hostname="localhost",
                    )
                    try:
                        control.authenticate(credentials)
                        return CxnfsQuicBulkClient.connect(
                            control,
                            "localhost",
                            transport_port,
                            timeout=5.0,
                            server_hostname="localhost",
                            cafile=str(cert),
                            read_config=CxnfsBulkReadConfig(
                                initial_cords=4,
                                max_cords=4,
                                provisional_container_size_bytes=64 * 1024,
                                expected_size_limit_bytes=64 * 1024,
                            ),
                            close_control_client=True,
                        )
                    except Exception:
                        control.close()
                        raise

                sources = weighted_mds_sources_from_root(
                    stage0,
                    "chat=2,summary=1",
                    client_factory=client_factory,
                    remote_root="/stage0-mds",
                )
                dataset = CxnfsWeightedTorchDataset(
                    sources,
                    repeat=False,
                    rounds_per_read=2,
                    source_read_workers=2,
                    prefetch_chunks=2,
                )
                allocations = []

                def tensor_factory(source_name, source_index, sample_index, data_length):
                    tensor = FakeTensor(data_length)
                    allocations.append((source_name, source_index, sample_index, data_length))
                    return tensor

                try:
                    batches = list(dataset.iter_trainer_direct_tensor_batches(
                        3,
                        tensor_factory=tensor_factory,
                        include_paths=True,
                        non_blocking=False,
                    ))
                finally:
                    dataset.close()
                    credentials.close()

                self.assertEqual(1, len(batches))
                self.assertEqual(3, batches[0].sample_count)
                self.assertEqual({"chat": 2, "summary": 1}, batches[0].source_counts)
                self.assertEqual(
                    [b"chat-0", b"chat-1", b"summary-0"],
                    [bytes(sample.data) for sample in batches[0].samples],
                )
                self.assertEqual(3, len(allocations))
            finally:
                _stop_server(process, stop)


def _complete_batch_direct_read_proof():
    return {
        "format": "cxnfs.quic-batch-direct-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "batch_read_count": 2,
        "batch_worker_count": 2,
        "ordered_batch_read_verified": True,
        "direct_buffer_read_operations": 2,
        "direct_destination_read_operations": 2,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "max_observed_cords": 4,
        "direct_destination_max_observed_cords": 4,
    }


def _complete_batch_direct_tensor_read_proof():
    return {
        "format": "cxnfs.quic-batch-direct-tensor-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "tensor_target": "pytorch-cpu",
        "tensor_read_count": 2,
        "tensor_worker_count": 2,
        "ordered_tensor_read_verified": True,
        "direct_tensor_read_operations": 2,
        "network_fills_tensor_destination": True,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "max_observed_cords": 4,
        "direct_destination_max_observed_cords": 4,
    }


class FakeTensor:
    def __init__(self, nbytes: int) -> None:
        self.data = bytearray(nbytes)
        self.device = "cpu"
        self.is_cuda = False


class InterruptingReadBulkFrameTransport:
    def __init__(self, delegate) -> None:
        self.delegate = delegate
        self.read_interruptions = 0
        self.close_calls = 0

    def read_frames(self, session_id, transfer, path, plan):
        self._interrupt()

    def read_frames_into(self, session_id, transfer, path, plan, accept):
        self._interrupt()

    def write_frame(self, frame):
        return self.delegate.write_frame(frame)

    def close(self):
        self.close_calls += 1
        self.delegate.close()

    def _interrupt(self):
        self.read_interruptions += 1
        self.delegate.close()
        raise OSError("injected QUIC bulk read transport interruption")


class InterruptingWriteBulkFrameTransport:
    def __init__(self, delegate) -> None:
        self.delegate = delegate
        self.write_interruptions = 0
        self.close_calls = 0

    def read_frames(self, session_id, transfer, path, plan):
        return self.delegate.read_frames(session_id, transfer, path, plan)

    def read_frames_into(self, session_id, transfer, path, plan, accept):
        return self.delegate.read_frames_into(session_id, transfer, path, plan, accept)

    def write_frame(self, frame):
        if self.write_interruptions == 0:
            self.write_interruptions += 1
            self.delegate.close()
            raise OSError("injected QUIC bulk write transport interruption")
        return self.delegate.write_frame(frame)

    def close(self):
        self.close_calls += 1
        self.delegate.close()


def _generate_key_store(key_store: Path, password_file: Path) -> None:
    password = password_file.read_text(encoding="utf-8").strip()
    _run_checked(
        "keytool",
        "-genkeypair",
        "-alias",
        "cxnfs-test",
        "-keyalg",
        "RSA",
        "-storetype",
        "PKCS12",
        "-keystore",
        str(key_store),
        "-storepass",
        password,
        "-keypass",
        password,
        "-dname",
        "CN=localhost",
        "-ext",
        "SAN=dns:localhost,ip:127.0.0.1",
        "-validity",
        "1",
        "-noprompt",
    )


def _export_certificate(key_store: Path, password_file: Path, cert: Path) -> None:
    password = password_file.read_text(encoding="utf-8").strip()
    _run_checked(
        "keytool",
        "-exportcert",
        "-rfc",
        "-alias",
        "cxnfs-test",
        "-keystore",
        str(key_store),
        "-storepass",
        password,
        "-file",
        str(cert),
    )


def _require_java_classpath() -> str:
    if shutil.which("java") is None:
        _skip_or_fail("java is required")
    if shutil.which("keytool") is None:
        _skip_or_fail("keytool is required")
    repo_root = Path(__file__).resolve().parents[2]
    classes = repo_root / "target" / "classes"
    if not classes.is_dir():
        _skip_or_fail("target/classes is required; run mvn -q -DskipTests package first")
    runtime_classpath = repo_root / "target" / "cxnfs-runtime-classpath.txt"
    if not runtime_classpath.is_file():
        _skip_or_fail("target/cxnfs-runtime-classpath.txt is required; run mvn -q dependency:build-classpath -Dmdep.outputFile=target/cxnfs-runtime-classpath.txt first")
    return os.pathsep.join((str(classes), runtime_classpath.read_text(encoding="utf-8").strip()))


def _server_files(root: Path):
    config = root / "server.properties"
    config.write_text("auth.username=trainer\nauth.password=secret\n", encoding="utf-8")
    password = root / "server.pass"
    password.write_text("changeit\n", encoding="utf-8")
    key_store = root / "server.p12"
    cert = root / "server.pem"
    ready = root / "server.ready"
    stop = root / "server.stop"
    log = root / "server.log"
    _generate_key_store(key_store, password)
    _export_certificate(key_store, password, cert)
    return config, password, key_store, cert, ready, stop, log


def _start_java_quic_server(
    data_root: Path,
    config: Path,
    key_store: Path,
    password: Path,
    ready: Path,
    stop: Path,
    log: Path,
    control_port: int,
    transport_port: int,
    *,
    health_file: Path | None = None,
):
    return _start_server(
        _require_java_classpath(),
        data_root,
        config,
        key_store,
        password,
        ready,
        stop,
        log,
        control_port,
        transport_port,
        health_file=health_file,
    )


def _start_server(
    classpath: str,
    data_root: Path,
    config: Path,
    key_store: Path,
    password: Path,
    ready: Path,
    stop: Path,
    log: Path,
    control_port: int,
    transport_port: int,
    *,
    health_file: Path | None = None,
):
    output = log.open("wb")
    try:
        command = [
            "java",
            "-cp",
            classpath,
            "com.fortrow.cxnfs.Cxnfs",
            "serve",
            str(data_root),
            "--config",
            str(config),
            "--host",
            "127.0.0.1",
            "--control-port",
            str(control_port),
            "--transport-port",
            str(transport_port),
            "--bulk-transport",
            "quic",
            "--keystore",
            str(key_store),
            "--keystore-password-file",
            str(password),
            "--ready-file",
            str(ready),
            "--shutdown-file",
            str(stop),
            "--listen",
            "true",
        ]
        if health_file is not None:
            command.extend([
                "--health-file",
                str(health_file),
                "--health-interval-ms",
                "25",
            ])
        return subprocess.Popen(
            command,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
    finally:
        output.close()


def _stop_server(process, stop: Path) -> None:
    stop.touch()
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=5.0)


def _wait_for_ready(ready: Path, process, log: Path) -> None:
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if ready.is_file():
            return
        if process.poll() is not None:
            raise AssertionError(f"Java CXNFS server exited early:\n{log.read_text(encoding='utf-8', errors='replace')}")
        time.sleep(0.05)
    raise AssertionError(f"Java CXNFS server did not become ready:\n{log.read_text(encoding='utf-8', errors='replace')}")


def _parse_health_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in text.splitlines()[1:]:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        fields[key] = value
    return fields


def _health_int(fields: dict[str, str], key: str) -> int:
    try:
        return int(fields.get(key, "0"))
    except ValueError:
        return 0


def _wait_for_health_field_at_least(
    health: Path,
    field: str,
    minimum: int,
    process,
    log: Path,
    timeout: float = 8.0,
) -> dict[str, str]:
    deadline = time.monotonic() + timeout
    last_fields: dict[str, str] = {}
    while time.monotonic() < deadline:
        if health.is_file():
            last_fields = _parse_health_fields(health.read_text(encoding="utf-8", errors="replace"))
            if _health_int(last_fields, field) >= minimum:
                return last_fields
        if process.poll() is not None:
            raise AssertionError(f"Java CXNFS server exited before health advanced:\n{log.read_text(encoding='utf-8', errors='replace')}")
        time.sleep(0.05)
    raise AssertionError(
        f"Java CXNFS server health did not report {field}>={minimum}; "
        f"last={last_fields}; log:\n{log.read_text(encoding='utf-8', errors='replace')}"
    )


def _free_tcp_port() -> int:
    return _free_port(socket.SOCK_STREAM)


def _free_udp_port() -> int:
    return _free_port(socket.SOCK_DGRAM)


def _free_port(socket_type: int) -> int:
    try:
        with socket.socket(socket.AF_INET, socket_type) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])
    except PermissionError as exc:
        _skip_or_fail("local socket binds are not permitted in this environment", exc)


def _skip_or_fail(message: str, cause: BaseException | None = None):
    if _require_live_quic():
        raise AssertionError(message) from cause
    raise unittest.SkipTest(message)


def _require_live_quic() -> bool:
    return os.environ.get("CXNFS_REQUIRE_QUIC_INTEGRATION", "").lower() in {"1", "true", "yes"}


def _publish_report(source: Path, name: str) -> None:
    report_dir = os.environ.get("CXNFS_QUIC_INTEGRATION_REPORT_DIR")
    if not report_dir:
        return
    target_dir = Path(report_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target_dir / name)


def _large_values(prefix: str, count: int) -> tuple[str, ...]:
    payload = "x" * (192 * 1024)
    return tuple(f"{prefix}-{index}-{payload}" for index in range(count))


def _pattern_bytes(size: int) -> bytes:
    return bytes((index * 31 + 17) % 251 for index in range(size))


def _run_checked(*args: str) -> None:
    subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def _write_file_dataset(root: Path, name: str, values: tuple[str, ...]) -> None:
    raw = root / "raw" / name
    raw.mkdir(parents=True)
    for index, value in enumerate(values):
        (raw / f"{index:04d}.txt").write_text(value, encoding="utf-8")
    plan = DatasetPreparer(target_shard_size_bytes=4096).plan(raw, name, "files")
    DatasetConverter().execute(plan, root / "stage0-mds" / name)


def _server_verified_tls13_context(cafile: str) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=cafile)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    return context


def _trainer_credentials() -> ClientCredentials:
    return ClientCredentials("trainer", "secret")


def _trainer_tls_context(cafile: str | None):
    return _server_verified_tls13_context(cafile)


if __name__ == "__main__":
    unittest.main()
