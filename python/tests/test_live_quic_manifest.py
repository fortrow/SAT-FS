from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cxnfs.live_quic_manifest import build_live_quic_manifest, read_path_proven, write_manifest


class LiveQuicManifestTest(unittest.TestCase):
    def test_manifest_derives_trainer_and_remote_readiness_from_live_reports(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
                bootstrap_venv="/tmp/venv",
                system_site_packages=True,
                skip_maven=True,
            )

            self.assertTrue(manifest["ready_for_trainer_wiring"])
            self.assertTrue(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["ready_for_blockfs_candidate"])
            self.assertFalse(manifest["kernel_module_allowed"])
            self.assertTrue(manifest["readiness"]["trainer_contract_ready"])
            self.assertTrue(manifest["readiness"]["trainer_surface_matrix_ready"])
            self.assertTrue(manifest["readiness"]["trainer_workload_ready"])
            self.assertTrue(manifest["readiness"]["trainer_sustained_read_profile_ready"])
            self.assertTrue(manifest["readiness"]["trainer_integration_smoke_ready"])
            self.assertTrue(manifest["readiness"]["trainer_runtime_smoke_ready"])
            self.assertTrue(manifest["readiness"]["trainer_distributed_runtime_smoke_ready"])
            self.assertTrue(manifest["readiness"]["trainer_loop_replay_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["readiness"]["trainer_handoff_acceptance_ready"])
            self.assertTrue(manifest["readiness"]["trainer_network_preflight_ready"])
            self.assertTrue(manifest["readiness"]["trainer_worker_loop_replay_ready"])
            self.assertTrue(manifest["readiness"]["trainer_pipeline_smoke_ready"])
            self.assertTrue(manifest["readiness"]["trainer_distributed_plan_ready"])
            self.assertTrue(manifest["readiness"]["trainer_stream_benchmark_ready"])
            self.assertTrue(manifest["readiness"]["trainer_workers_ready"])
            self.assertTrue(manifest["readiness"]["trainer_resume_ready"])
            self.assertTrue(manifest["readiness"]["python_client_cli_quic_smoke_ready"])
            self.assertTrue(manifest["readiness"]["batch_direct_read_ready"])
            self.assertTrue(manifest["readiness"]["batch_direct_tensor_read_ready"])
            self.assertEqual(19, manifest["report_count"])

            reports = {entry["path"]: entry for entry in manifest["reports"]}
            stream = reports["python-quic-trainer-stream-benchmark.json"]
            pipeline = reports["python-quic-trainer-pipeline-smoke.json"]
            workers = reports["python-quic-trainer-workers.json"]
            resume = reports["python-quic-trainer-resume.json"]
            workload = reports["python-quic-trainer-contract-workload-benchmark.json"]
            matrix = reports["python-quic-trainer-contract-surface-matrix.json"]
            smoke = reports["python-quic-trainer-integration-smoke.json"]
            runtime_smoke = reports["python-quic-trainer-runtime-smoke.json"]
            distributed_runtime_smoke = reports["python-quic-trainer-distributed-runtime-smoke.json"]
            loop_replay = reports["python-quic-trainer-loop-replay.json"]
            readiness_bundle = reports["python-quic-trainer-readiness-bundle.json"]
            handoff_acceptance = reports["python-quic-trainer-handoff-acceptance.json"]
            network_preflight = reports["python-quic-trainer-network-preflight.json"]
            worker_loop_replay = reports["python-quic-trainer-worker-loop-replay.json"]
            distributed_plan = reports["python-quic-trainer-distributed-plan.json"]
            cli = reports["python-client-cli-quic-smoke.json"]
            batch = reports["python-quic-batch-direct-read.json"]
            tensor_batch = reports["python-quic-batch-direct-tensor-read.json"]
            self.assertTrue(stream["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(2, stream["trainer_prefetch_batches"])
            self.assertTrue(stream["trainer_prefetch_enabled"])
            self.assertEqual(1, stream["bulk_read_retries"])
            self.assertTrue(pipeline["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual("CxnfsTrainingPipeline", pipeline["training_pipeline_object"])
            self.assertEqual("CxnfsTrainingPipeline.prefetch_trainer_stream", pipeline["training_pipeline_context"])
            self.assertEqual("CxnfsTrainerPipelineStream", pipeline["pipeline_descriptor_stream"])
            self.assertTrue(pipeline["pipeline_prefetch_stream_used"])
            self.assertEqual(2, pipeline["pipeline_prefetch_batches"])
            self.assertEqual("CxnfsPipelinePrefetchedTensorBatch", pipeline["pipeline_prefetch_result_object"])
            self.assertTrue(pipeline["pipeline_python_byte_batch_avoided"])
            self.assertTrue(pipeline["pipeline_network_path_complete"])
            self.assertEqual("cxnfs.trainer-pipeline-network-path.v1", pipeline["pipeline_network_path_format"])
            self.assertTrue(pipeline["quic_read_transport_all_reports_expected_cipher_suite"])
            self.assertEqual(
                {"TLS_AES_128_GCM_SHA256": 2},
                pipeline["quic_read_transport_cipher_suite_counts"],
            )
            self.assertEqual(4, pipeline["max_observed_write_cords"])
            self.assertTrue(pipeline["verified_read_back"])
            self.assertTrue(workers["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual("trainer-dataloader", workers["trainer_batch_iterator"])
            self.assertEqual(2, workers["data_loader_workers"])
            self.assertTrue(workers["worker_batch_read_evidence_observed"])
            self.assertTrue(workers["worker_sharding_enabled"])
            self.assertEqual(0, workers["duplicate_source_sample_count"])
            self.assertFalse(resume["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(1, resume["direct_destination_max_observed_cords"])
            self.assertEqual("cxnfs.trainer-cursor.v1", resume["cursor_format"])
            self.assertEqual("cxnfs.trainer-stream-state.v1", resume["stream_state_format"])
            self.assertTrue(resume["state_file_written"])
            self.assertTrue(resume["state_file_replayed"])
            self.assertTrue(resume["state_stream_state_matches_final_cursor"])
            self.assertEqual(2, resume["first_final_emitted"])
            self.assertEqual(2, resume["second_initial_emitted"])
            self.assertEqual(4, resume["second_final_emitted"])
            self.assertEqual("cxnfs.trainer-sustained-read-profile.v1", workload["sustained_read_profile_format"])
            self.assertTrue(workload["sustained_read_profile_complete"])
            self.assertEqual(4, workload["sustained_read_run_count"])
            self.assertEqual(8, workload["sustained_read_total_batches"])
            self.assertEqual(2, workload["sustained_read_cord_sweep_count"])
            self.assertTrue(workload["sustained_read_quic_read_transport_complete"])
            self.assertTrue(workload["sustained_read_quic_read_transport_stream_eof_verified"])
            self.assertTrue(workload["sustained_read_quic_read_transport_all_reports_expected_cipher_suite"])
            self.assertEqual(
                {"TLS_AES_128_GCM_SHA256": 4},
                workload["sustained_read_quic_read_transport_cipher_suite_counts"],
            )
            self.assertEqual(0, workload["sustained_read_unrecovered_read_failures"])
            self.assertTrue(matrix["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(2, matrix["direct_destination_max_observed_cords"])
            self.assertTrue(smoke["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(2, smoke["direct_destination_max_observed_cords"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", smoke["training_loop_context"])
            self.assertTrue(smoke["training_loop_context_used"])
            self.assertEqual("CxnfsTrainerPrefetchStream", smoke["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", smoke["training_loop_base_stream_object"])
            self.assertEqual(2, smoke["training_loop_prefetch_batches"])
            self.assertTrue(smoke["training_loop_prefetch_applied"])
            self.assertEqual("cxnfs.trainer-runtime-binding.v1", runtime_smoke["runtime_binding_format"])
            self.assertEqual("CxnfsTrainerRuntimeBinding", runtime_smoke["runtime_binding_object"])
            self.assertEqual("CxnfsTrainerRuntimeBinding.open_training_loop", runtime_smoke["training_loop_context"])
            self.assertEqual("CxnfsTrainerPrefetchStream", runtime_smoke["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", runtime_smoke["training_loop_base_stream_object"])
            self.assertEqual(2, runtime_smoke["training_loop_prefetch_batches"])
            self.assertTrue(runtime_smoke["training_loop_prefetch_applied"])
            self.assertEqual("direct-tensors", runtime_smoke["read_mode"])
            self.assertTrue(runtime_smoke["pipeline_training_loop_required"])
            self.assertTrue(runtime_smoke["pipeline_training_loop_verified"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                runtime_smoke["pipeline_training_loop_context"],
            )
            self.assertTrue(runtime_smoke["pipeline_training_loop_context_used"])
            self.assertEqual("CxnfsTrainerPipelineLoop", runtime_smoke["pipeline_training_loop_object"])
            self.assertTrue(runtime_smoke["pipeline_network_path_verified"])
            self.assertEqual("cxnfs.trainer-distributed-runtime-smoke.v1", distributed_runtime_smoke["format"])
            self.assertEqual(2, distributed_runtime_smoke["world_size"])
            self.assertEqual(2, distributed_runtime_smoke["rank_count"])
            self.assertTrue(distributed_runtime_smoke["all_rank_training_shards_correct"])
            self.assertTrue(distributed_runtime_smoke["no_cross_rank_overlap"])
            self.assertTrue(distributed_runtime_smoke["all_open_clients_closed"])
            self.assertTrue(distributed_runtime_smoke["all_rank_state_files_written"])
            self.assertEqual(2, distributed_runtime_smoke["rank_state_file_count"])
            self.assertTrue(loop_replay["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual(2, loop_replay["direct_destination_max_observed_cords"])
            self.assertEqual("CxnfsTrainerIntegrationPlan.open_training_loop", loop_replay["training_loop_context"])
            self.assertTrue(loop_replay["training_loop_context_used"])
            self.assertEqual("CxnfsTrainerPrefetchStream", loop_replay["training_loop_object"])
            self.assertEqual("CxnfsTrainerStream", loop_replay["training_loop_base_stream_object"])
            self.assertEqual(2, loop_replay["training_loop_prefetch_batches"])
            self.assertTrue(loop_replay["training_loop_prefetch_applied"])
            self.assertEqual(2, loop_replay["step_count"])
            self.assertTrue(loop_replay["weighted_source_coverage_complete"])
            self.assertTrue(loop_replay["resume_replayed"])
            self.assertTrue(loop_replay["resume_continuity_verified"])
            self.assertEqual("cxnfs.trainer-readiness-bundle.v1", readiness_bundle["format"])
            self.assertTrue(readiness_bundle["requires_low_level_direct_read_proofs"])
            self.assertTrue(readiness_bundle["low_level_direct_read_proofs_complete"])
            self.assertTrue(readiness_bundle["batch_direct_read_proof_complete"])
            self.assertTrue(readiness_bundle["batch_direct_tensor_read_proof_complete"])
            self.assertTrue(readiness_bundle["requires_low_level_direct_write_proofs"])
            self.assertTrue(readiness_bundle["low_level_direct_write_proofs_complete"])
            self.assertTrue(readiness_bundle["write_probe_proof_complete"])
            self.assertEqual("TLS_AES_128_GCM_SHA256", readiness_bundle["write_probe_cipher_suite"])
            self.assertTrue(readiness_bundle["direct_quic_memory_path_verified"])
            self.assertTrue(readiness_bundle["direct_python_streaming_verified"])
            self.assertTrue(readiness_bundle["runtime_training_loop_verified"])
            self.assertTrue(readiness_bundle["pipeline_training_loop_required"])
            self.assertTrue(readiness_bundle["pipeline_training_loop_verified"])
            self.assertTrue(readiness_bundle["distributed_runtime_verified"])
            self.assertTrue(readiness_bundle["distributed_rank_states_written"])
            self.assertTrue(readiness_bundle["loop_resume_verified"])
            self.assertEqual(5, readiness_bundle["artifact_count"])
            self.assertEqual("cxnfs.trainer-handoff-acceptance.v1", handoff_acceptance["format"])
            self.assertTrue(handoff_acceptance["direct_quic_memory_path_verified"])
            self.assertTrue(handoff_acceptance["direct_python_streaming_verified"])
            self.assertTrue(handoff_acceptance["runtime_training_loop_verified"])
            self.assertTrue(handoff_acceptance["pipeline_training_loop_required"])
            self.assertTrue(handoff_acceptance["pipeline_training_loop_verified"])
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                handoff_acceptance["pipeline_training_loop_context"],
            )
            self.assertTrue(handoff_acceptance["distributed_runtime_verified"])
            self.assertTrue(handoff_acceptance["loop_resume_verified"])
            self.assertTrue(handoff_acceptance["cross_rank_overlap_free"])
            self.assertTrue(handoff_acceptance["rank_states_written"])
            self.assertFalse(handoff_acceptance["raw_password_sent_over_control"])
            self.assertEqual("cxnfs.trainer-loop-contract.v1", handoff_acceptance["trainer_loop_contract_format"])
            self.assertEqual(
                "cxnfs.trainer-loop-contract.v1",
                handoff_acceptance["trainer_loop_contract"]["format"],
            )
            self.assertEqual(
                "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                handoff_acceptance["trainer_loop_contract"]["entrypoint"][
                    "pipeline_training_loop_context"
                ],
            )
            self.assertEqual(
                "CxnfsTrainerPipelineLoop",
                handoff_acceptance["trainer_loop_contract"]["entrypoint"]["pipeline_loop_object"],
            )
            self.assertTrue(
                handoff_acceptance["trainer_loop_contract"]["authentication"][
                    "credential_prompt_required_for_remote"
                ]
            )
            self.assertFalse(
                handoff_acceptance["trainer_loop_contract"]["authentication"]["password_cli_argument"]
            )
            self.assertTrue(
                handoff_acceptance["trainer_loop_contract"]["remote_transport_contract"][
                    "per_source_client_default"
                ]
            )
            self.assertEqual("cxnfs.trainer-network-preflight.v1", network_preflight["format"])
            self.assertTrue(network_preflight["passed"])
            self.assertEqual("cxnfs-quic", network_preflight["transport"])
            self.assertEqual("quic", network_preflight["bulk_transport"])
            self.assertTrue(network_preflight["opens_network_connections"])
            self.assertFalse(network_preflight["consumes_batches"])
            self.assertEqual("ok", network_preflight["bulk_probe"])
            self.assertEqual(1, network_preflight["bulk_probe_bytes"])
            self.assertTrue(network_preflight["network_probe_is_handoff_aware"])
            self.assertEqual("trainer-dataloader", worker_loop_replay["trainer_batch_iterator"])
            self.assertEqual(2, worker_loop_replay["data_loader_workers"])
            self.assertEqual(8, worker_loop_replay["samples_per_epoch"])
            self.assertFalse(worker_loop_replay["repeat"])
            self.assertTrue(worker_loop_replay["worker_partitioning_enabled"])
            self.assertEqual(2, worker_loop_replay["worker_cursor_count"])
            self.assertEqual(4, worker_loop_replay["step_count"])
            self.assertEqual(8, worker_loop_replay["sample_count"])
            self.assertTrue(worker_loop_replay["worker_batch_read_evidence_observed"])
            self.assertTrue(worker_loop_replay["weighted_source_coverage_complete"])
            self.assertTrue(worker_loop_replay["resume_replayed"])
            self.assertTrue(worker_loop_replay["resume_continuity_verified"])
            self.assertEqual(2, distributed_plan["rank_count"])
            self.assertEqual(1, distributed_plan["selected_rank"])
            self.assertTrue(distributed_plan["low_level_direct_read_proofs_complete"])
            self.assertTrue(distributed_plan["batch_direct_read_proof_complete"])
            self.assertTrue(distributed_plan["batch_direct_tensor_read_proof_complete"])
            self.assertEqual("cxnfs.trainer-runtime-binding.v1", distributed_plan["runtime_binding_format"])
            self.assertEqual("CxnfsTrainerRuntimeBinding", distributed_plan["runtime_binding_object"])
            self.assertFalse(distributed_plan["opens_network_connections"])
            self.assertFalse(distributed_plan["consumes_batches"])
            self.assertEqual(4, cli["read_max_observed_cords"])
            self.assertEqual(4, cli["write_max_observed_cords"])
            self.assertEqual(4, cli["direct_write_max_observed_cords"])
            self.assertEqual("quic-tls13-udp://127.0.0.1:4510", cli["server_bulk_listener"])
            self.assertEqual("tls_aes_128_gcm_sha256", cli["server_bulk_cipher_suite"])
            self.assertEqual(16, cli["server_bulk_total_streams"])
            self.assertTrue(cli["server_bulk_total_streams_observed"])
            self.assertTrue(cli["read_back_verified"])
            self.assertTrue(cli["direct_file_write_verified"])
            self.assertFalse(cli["raw_password_visible"])
            self.assertEqual("cxnfs.quic-batch-direct-read.v1", batch["format"])
            self.assertTrue(batch["ordered_batch_read_verified"])
            self.assertEqual(2, batch["batch_read_count"])
            self.assertEqual(2, batch["batch_worker_count"])
            self.assertTrue(batch["direct_destination_frame_sink_cord_fanout_verified"])
            self.assertEqual("cxnfs.quic-batch-direct-tensor-read.v1", tensor_batch["format"])
            self.assertTrue(tensor_batch["ordered_tensor_read_verified"])
            self.assertEqual("pytorch-cpu", tensor_batch["tensor_target"])
            self.assertEqual(2, tensor_batch["tensor_read_count"])
            self.assertEqual(2, tensor_batch["tensor_worker_count"])
            self.assertEqual(2, tensor_batch["direct_tensor_read_operations"])
            self.assertTrue(tensor_batch["network_fills_tensor_destination"])

    def test_manifest_preserves_explicit_blockfs_decision_flags(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            write_json(
                report_dir / "python-quic-workload-decision.json",
                {
                    "format": "cxnfs.workload-decision.v1",
                    "ready_for_blockfs_candidate": True,
                    "kernel_module_allowed": True,
                },
            )

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertTrue(manifest["ready_for_trainer_wiring"])
            self.assertTrue(manifest["ready_for_remote_quic"])
            self.assertTrue(manifest["ready_for_blockfs_candidate"])
            self.assertTrue(manifest["kernel_module_allowed"])

    def test_high_cord_reports_can_prove_remote_quic_without_trainer_reports(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_high_cord_reports(report_dir, include_transport_frame_sink_observed=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertTrue(manifest["ready_for_remote_quic"])
            self.assertTrue(manifest["readiness"]["high_cord_remote_quic_ready"])
            self.assertTrue(manifest["readiness"]["batch_direct_read_ready"])
            self.assertTrue(manifest["readiness"]["batch_direct_tensor_read_ready"])
            self.assertFalse(manifest["readiness"]["trainer_contract_ready"])
            self.assertFalse(manifest["ready_for_blockfs_candidate"])
            self.assertFalse(manifest["kernel_module_allowed"])

    def test_read_path_proven_accepts_nested_cord_admission_evidence(self):
        payload = {
            "read_path_evidence": {
                "transport_frame_sink_observed": True,
                "frame_sink_used_for_all_direct_destination_reads": True,
                "direct_destination_frame_sink_cord_fanout_verified": True,
                "direct_destination_max_observed_cords": 2,
                "direct_destination_parallel_frame_sink_read_operations": 1,
                "quic_read_transport_stream_completion_verified": True,
                "quic_read_transport_stream_eof_verified": True,
                "sources": [
                    {"observed_cords_within_admission": True},
                    {"observed_cords_within_admission": True},
                ],
            }
        }

        self.assertTrue(read_path_proven(payload))

        payload["read_path_evidence"]["sources"][1]["observed_cords_within_admission"] = False
        self.assertFalse(read_path_proven(payload))

    def test_write_manifest_emits_json_and_line_report(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)

            manifest = write_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertEqual(manifest, json.loads((report_dir / "live-quic-check.json").read_text(encoding="utf-8")))
            text = (report_dir / "live-quic-check.report").read_text(encoding="utf-8")
            self.assertIn("cxnfs.live-quic-check.v1", text)
            self.assertIn("ready_for_trainer_wiring=true", text)
            self.assertIn("ready_for_remote_quic=true", text)
            self.assertIn("ready_for_blockfs_candidate=false", text)
            self.assertIn("kernel_module_allowed=false", text)
            self.assertIn("readiness.python_client_cli_quic_smoke_ready=true", text)
            self.assertIn("readiness.trainer_stream_benchmark_ready=true", text)
            self.assertIn("readiness.trainer_pipeline_smoke_ready=true", text)
            self.assertIn("readiness.trainer_workers_ready=true", text)
            self.assertIn("readiness.trainer_resume_ready=true", text)
            self.assertIn("readiness.trainer_readiness_bundle_ready=true", text)
            self.assertIn("readiness.trainer_network_preflight_ready=true", text)
            self.assertIn("readiness.trainer_worker_loop_replay_ready=true", text)
            self.assertIn("readiness.trainer_sustained_read_profile_ready=true", text)

    def test_raw_password_flag_prevents_trainer_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, raw_password_sent=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_contract_ready"])
            self.assertFalse(manifest["ready_for_remote_quic"])

    def test_cli_smoke_is_required_for_workload_derived_remote_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, omit_cli_smoke=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["python_client_cli_quic_smoke_ready"])

    def test_batch_direct_read_is_required_for_remote_quic_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, omit_batch_direct_read=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["batch_direct_read_ready"])
            self.assertTrue(manifest["readiness"]["python_client_cli_quic_smoke_ready"])

    def test_batch_direct_tensor_report_tracks_low_level_pytorch_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, omit_batch_direct_tensor_read=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertTrue(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertTrue(manifest["readiness"]["batch_direct_read_ready"])
            self.assertFalse(manifest["readiness"]["batch_direct_tensor_read_ready"])

    def test_trainer_stream_reports_are_required_for_trainer_and_remote_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, omit_trainer_stream_reports=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["trainer_stream_benchmark_ready"])
            self.assertFalse(manifest["readiness"]["trainer_workers_ready"])
            self.assertFalse(manifest["readiness"]["trainer_resume_ready"])

    def test_workload_sustained_read_profile_is_required_for_trainer_and_remote_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, sustained_read_profile_complete=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["trainer_workload_ready"])
            self.assertFalse(manifest["readiness"]["trainer_sustained_read_profile_ready"])

    def test_workload_sustained_read_profile_requires_quic_cipher_proof_for_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            workload_path = report_dir / "python-quic-trainer-contract-workload-benchmark.json"
            workload = json.loads(workload_path.read_text(encoding="utf-8"))
            profile = workload["sustained_read_profile"]
            profile["quic_read_transport_expected_cipher_suite_count"] = 0
            profile["quic_read_transport_all_reports_expected_cipher_suite"] = False
            profile["quic_read_transport_cipher_suite_counts"] = {}
            write_json(workload_path, workload)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["trainer_workload_ready"])
            self.assertFalse(manifest["readiness"]["trainer_sustained_read_profile_ready"])

    def test_pipeline_network_path_requires_quic_cipher_proof_for_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            pipeline_path = report_dir / "python-quic-trainer-pipeline-smoke.json"
            pipeline = json.loads(pipeline_path.read_text(encoding="utf-8"))
            network_path = pipeline["pipeline_network_path"]
            network_path["quic_read_transport_expected_cipher_suite_count"] = 0
            network_path["quic_read_transport_all_reports_expected_cipher_suite"] = False
            network_path["quic_read_transport_cipher_suite_counts"] = {}
            write_json(pipeline_path, pipeline)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["trainer_pipeline_smoke_ready"])

    def test_trainer_resume_requires_stream_state_replay(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, resume_state_file_replayed=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["trainer_resume_ready"])

    def test_cli_smoke_rejects_visible_password_evidence(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, cli_raw_password_visible=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["python_client_cli_quic_smoke_ready"])

    def test_cli_smoke_requires_direct_write_readback(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, cli_direct_file_write_verified=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["python_client_cli_quic_smoke_ready"])

    def test_cli_smoke_requires_server_bulk_stream_evidence(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, cli_server_bulk_total_streams_observed=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["python_client_cli_quic_smoke_ready"])

    def test_trainer_integration_smoke_requires_training_loop_adapter(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, smoke_training_loop_context_used=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_integration_smoke_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_loop_replay_is_required_for_trainer_wiring(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, loop_replay_passed=False)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_loop_replay_ready"])
            self.assertFalse(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_readiness_bundle_is_required_for_trainer_wiring(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, omit_trainer_readiness_bundle=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["readiness"]["trainer_loop_replay_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_network_preflight_is_required_for_trainer_wiring(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, omit_trainer_network_preflight=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_network_preflight_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertFalse(manifest["ready_for_remote_quic"])

    def test_trainer_readiness_bundle_must_preserve_direct_read_proofs(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            bundle_path = report_dir / "python-quic-trainer-readiness-bundle.json"
            bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
            bundle["low_level_direct_read_proofs_complete"] = False
            bundle["direct_quic_memory_path_verified"] = False
            bundle_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["readiness"]["batch_direct_read_ready"])
            self.assertTrue(manifest["readiness"]["batch_direct_tensor_read_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_runtime_smoke_must_preserve_pipeline_training_loop_proof(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            runtime_path = report_dir / "python-quic-trainer-runtime-smoke.json"
            runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            runtime["pipeline_training_loop_report"]["verified"] = False
            runtime_path.write_text(json.dumps(runtime, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_runtime_smoke_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_runtime_smoke_must_preserve_pipeline_network_path_proof(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            runtime_path = report_dir / "python-quic-trainer-runtime-smoke.json"
            runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            runtime["pipeline_network_path_verified"] = False
            runtime["pipeline_training_loop_report"]["pipeline_network_path_complete"] = False
            runtime["pipeline_training_loop_report"]["pipeline_network_path"]["complete"] = False
            runtime_path.write_text(json.dumps(runtime, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_runtime_smoke_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_readiness_bundle_must_preserve_pipeline_training_loop_proof(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            bundle_path = report_dir / "python-quic-trainer-readiness-bundle.json"
            bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
            bundle["pipeline_training_loop_verified"] = False
            bundle_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["readiness"]["trainer_runtime_smoke_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_handoff_acceptance_must_preserve_loop_contract(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            acceptance_path = report_dir / "python-quic-trainer-handoff-acceptance.json"
            acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
            acceptance["trainer_loop_contract"]["authentication"]["password_cli_argument"] = True
            acceptance_path.write_text(json.dumps(acceptance, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_handoff_acceptance_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_handoff_acceptance_must_preserve_pipeline_training_loop_proof(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            acceptance_path = report_dir / "python-quic-trainer-handoff-acceptance.json"
            acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
            acceptance["pipeline_training_loop_context"] = "CxnfsTrainerRuntimeBinding.open_training_loop"
            acceptance_path.write_text(json.dumps(acceptance, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_handoff_acceptance_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_handoff_acceptance_must_preserve_loop_pipeline_contract(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir)
            acceptance_path = report_dir / "python-quic-trainer-handoff-acceptance.json"
            acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
            acceptance["trainer_loop_contract"]["pipeline_contract"]["python_byte_batch_avoidance_supported"] = False
            acceptance_path.write_text(json.dumps(acceptance, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["readiness"]["trainer_handoff_acceptance_ready"])
            self.assertTrue(manifest["readiness"]["trainer_readiness_bundle_ready"])
            self.assertTrue(manifest["ready_for_remote_quic"])

    def test_trainer_worker_loop_replay_is_required_for_trainer_and_remote_readiness(self):
        with TemporaryDirectory() as directory:
            report_dir = Path(directory)
            write_live_trainer_reports(report_dir, worker_loop_repeat=True)

            manifest = build_live_quic_manifest(
                report_dir,
                test_target="python.tests.live",
                python_executable="/tmp/python",
            )

            self.assertFalse(manifest["ready_for_trainer_wiring"])
            self.assertFalse(manifest["ready_for_remote_quic"])
            self.assertFalse(manifest["readiness"]["trainer_worker_loop_replay_ready"])


def live_trainer_loop_contract_fixture(
    *,
    passed: bool,
    raw_password_sent: bool,
    loop_replay_passed: bool,
) -> dict[str, object]:
    return {
        "format": "cxnfs.trainer-loop-contract.v1",
        "passed": passed,
        "runtime_binding_format": "cxnfs.trainer-runtime-binding.v1",
        "stream_state_format": "cxnfs.trainer-stream-state.v1",
        "entrypoint": {
            "runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file",
            "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
            "pipeline_loop_object": "CxnfsTrainerPipelineLoop",
            "batch_object": "CxnfsTrainerBatch",
        },
        "authentication": {
            "remote_client_required": True,
            "requires_credentials": True,
            "credential_prompt_supported": True,
            "credential_prompt_required_for_remote": True,
            "password_cli_argument": False,
            "raw_password_sent_over_control": raw_password_sent,
        },
        "mount_free_contract": {
            "requires_mount": False,
            "touches_trainer_code": False,
            "opens_network_connections_during_preflight": False,
            "consumes_batches_during_preflight": False,
            "safe_to_run_before_trainer_workers": True,
        },
        "stream_contract": {
            "read_mode": "direct-buffers",
            "trainer_batch_iterator": "native",
            "trainer_prefetch_batches": 2,
            "direct_python_streaming_verified": passed,
            "runtime_training_loop_verified": True,
        },
        "pipeline_contract": {
            "training_pipeline_object": "CxnfsTrainingPipeline",
            "training_pipeline_context": "CxnfsTrainingPipeline.read_tensor_batch",
            "pipeline_stream_context": "CxnfsTrainingPipeline.prefetch_trainer_stream",
            "pipeline_batch_iterator": "CxnfsTrainerPipelineStream.next_batch",
            "pipeline_descriptor_iterator": (
                "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors"
            ),
            "pipeline_descriptor_stream": "CxnfsTrainerPipelineStream",
            "pipeline_loop_object": "CxnfsTrainerPipelineLoop",
            "pipeline_stream_state_format": "cxnfs.trainer-stream-state.v1",
            "pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch",
            "supports_pipeline_direct_tensor_batches": True,
            "supports_pipeline_gpu_transforms": True,
            "supports_pipeline_torch_compile": True,
            "pipeline_cpu_augmentation_default": "disabled-with-explicit-opt-in",
            "direct_tensor_read_mode": "direct-tensors",
            "direct_tensor_read_mode_selected": False,
            "selected_read_mode": "direct-buffers",
            "python_byte_batch_avoidance_required_for_direct_tensors": True,
            "python_byte_batch_avoidance_supported": True,
            "network_fills_tensor_destination_supported": False,
            "pinned_host_staging_supported": False,
            "tensor_allocation": None,
            "tensor_memory_path": None,
            "pipeline_network_path": dict(live_pipeline_network_path_fixture()),
            "pipeline_network_path_verified": True,
            "trainer_prefetch_batches": 2,
            "trainer_prefetch_enabled": True,
            "direct_tensor_transport_proof_complete": True,
            "direct_tensor_transport_cipher_suite": "TLS_AES_128_GCM_SHA256",
        },
        "batch_contract": {
            "fields": [
                "batch_index",
                "epoch",
                "samples",
                "sample_count",
                "byte_size",
                "source_counts",
                "resume_cursor",
            ],
            "sample_fields": [
                "source_name",
                "source_index",
                "sample_index",
                "data",
            ],
        },
        "distributed_contract": {
            "world_size": 2,
            "cross_rank_overlap_free": True,
            "rank_states_written": True,
        },
        "resume_contract": {
            "cursor_format": "cxnfs.trainer-cursor.v1",
            "state_format": "cxnfs.trainer-stream-state.v1",
            "loop_resume_verified": loop_replay_passed,
            "sample_count": 6,
        },
        "remote_transport_contract": {
            "bulk_transport": "quic",
            "max_cords": 64,
            "per_source_client_default": True,
            "ready_for_remote_quic": True,
            "direct_quic_memory_path_verified": True,
        },
    }


def live_pipeline_network_path_fixture() -> dict[str, object]:
    return {
        "format": "cxnfs.trainer-pipeline-network-path.v1",
        "complete": True,
        "missing": [],
        "quic_required": True,
        "quic_complete": True,
        "training_pipeline_object": "CxnfsTrainingPipeline",
        "training_pipeline_context": "CxnfsTrainingPipeline.prefetch_trainer_stream",
        "network_read_workers": 16,
        "max_cords": 64,
        "prefetch_batches": 2,
        "active_pipeline_prefetch_batches": 2,
        "max_inflight_bytes": 1073741824,
        "batch_count": 2,
        "requested_batches": 2,
        "sample_count": 6,
        "byte_size": 786432,
        "payload_bytes_read": 786432,
        "network_fanout_configured": True,
        "observed_parallel_direct_path": True,
        "direct_tensor_read_operations": 6,
        "direct_destination_read_operations": 6,
        "failed_direct_destination_read_operations": 0,
        "direct_destination_max_observed_cords": 2,
        "direct_destination_parallel_frame_sink_read_operations": 4,
        "direct_destination_parallel_frame_sink_frames": 8,
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "uses_caller_owned_destinations": True,
        "tensor_memory_path_complete": True,
        "python_byte_batch_avoided": True,
        "network_fills_tensor_destination": True,
        "requires_pinned_host_staging": False,
        "quic_read_transport_observed": True,
        "quic_read_transport_complete": True,
        "quic_read_transport_stream_completion_verified": True,
        "quic_read_transport_stream_eof_verified": True,
        "quic_read_transport_max_read_stream_count": 2,
        "quic_read_transport_expected_cipher_suite_count": 2,
        "quic_read_transport_all_reports_expected_cipher_suite": True,
        "quic_read_transport_cipher_suite_counts": {"TLS_AES_128_GCM_SHA256": 2},
    }


def write_live_trainer_reports(
    report_dir: Path,
    *,
    raw_password_sent: bool = False,
    omit_cli_smoke: bool = False,
    omit_trainer_stream_reports: bool = False,
    cli_raw_password_visible: bool = False,
    cli_direct_file_write_verified: bool = True,
    cli_server_bulk_total_streams_observed: bool = True,
    resume_state_file_replayed: bool = True,
    smoke_training_loop_context_used: bool = True,
    loop_replay_passed: bool = True,
    worker_loop_repeat: bool = False,
    omit_trainer_readiness_bundle: bool = False,
    omit_trainer_network_preflight: bool = False,
    omit_batch_direct_read: bool = False,
    omit_batch_direct_tensor_read: bool = False,
    sustained_read_profile_complete: bool = True,
) -> None:
    pipeline_network_path = live_pipeline_network_path_fixture()
    read_path = {
        "transport_frame_sink_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "direct_destination_max_observed_cords": 2,
        "direct_destination_parallel_frame_sink_read_operations": 4,
        "last_accepted_initial_cords": 2,
        "last_accepted_max_cords": 2,
        "observed_cords_within_admission": True,
        "quic_read_transport_stream_completion_verified": True,
        "quic_read_transport_stream_eof_verified": True,
    }
    read_reliability = {
        "format": "cxnfs.trainer-read-path-reliability.v1",
        "complete": True,
        "unrecovered_read_failures": 0,
    }
    write_path = {
        "format": "cxnfs.workload-write-path-evidence.v1",
        "complete": True,
        "quic_write_transport_complete": True,
        "write_ack_frame_identity_verified": True,
        "quic_write_transport_all_reports_write_ack_frame_identity_verified": True,
        "quic_write_transport_all_reports_write_ack_stream_completion_verified": True,
        "quic_write_transport_all_reports_write_ack_stream_eof_verified": True,
        "all_write_probes_verified": True,
    }
    sustained_read_profile = {
        "format": "cxnfs.trainer-sustained-read-profile.v1",
        "complete": sustained_read_profile_complete,
        "missing": [] if sustained_read_profile_complete else ["at least two workload runs are required"],
        "requires_quic_transport_evidence": True,
        "minimum_runs": 2,
        "minimum_batches": 2,
        "minimum_cord_sweep": 2,
        "minimum_observed_cords": 2,
        "run_count": 4 if sustained_read_profile_complete else 1,
        "total_batches": 8 if sustained_read_profile_complete else 1,
        "min_run_batches": 2 if sustained_read_profile_complete else 1,
        "sample_count": 24 if sustained_read_profile_complete else 3,
        "byte_size": 3145728 if sustained_read_profile_complete else 4096,
        "cord_sweep_count": 2,
        "java_python_network_path_exercised": True,
        "all_runs_have_read_path_evidence": sustained_read_profile_complete,
        "direct_destination_reads_observed": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "direct_destination_max_observed_cords": 2,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "direct_destination_parallel_frame_sink_read_operations": 4,
        "quic_read_transport_complete": True,
        "quic_read_transport_stream_eof_verified": True,
        "quic_read_transport_evidence_count": 4,
        "quic_read_transport_attempted_read_streams": 8,
        "quic_read_transport_read_streams_created": 8,
        "quic_read_transport_read_streams_eof_verified": 8,
        "quic_read_transport_read_stream_eof_report_count": 4,
        "quic_read_transport_expected_cipher_suite_count": 4,
        "quic_read_transport_all_reports_expected_cipher_suite": True,
        "quic_read_transport_cipher_suite_counts": {"TLS_AES_128_GCM_SHA256": 4},
        "read_path_reliability_complete": True,
        "unrecovered_read_failures": 0,
    }
    write_json(
        report_dir / "python-quic-trainer-contract.json",
        {
            "format": "cxnfs.trainer-contract.v1",
            "network_contract": {
                "bulk_transport": "quic",
                "bulk_transport_available": True,
                "max_cords": 64,
                "raw_password_sent_over_control": raw_password_sent,
                "transport": "cxnfs-quic",
            },
            "python_surface": {
                "requires_mount": False,
                "supports_trainer_stream": True,
                "supports_weighted_sources": True,
                "touches_trainer_code": False,
            },
            "validation": {"validated": True},
        },
    )
    if not omit_trainer_stream_reports:
        write_json(
            report_dir / "python-quic-trainer-stream-benchmark.json",
            {
                "format": "cxnfs.trainer-stream-smoke.v1",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_prefetch_enabled": True,
                "trainer_prefetch_batches": 2,
                "trainer_prefetch_stream_object": "CxnfsTrainerPrefetchStream",
                "bulk_read_retries": 1,
                "read_path_evidence": dict(read_path),
                "read_path_reliability": dict(read_reliability),
            },
        )
        write_json(
            report_dir / "python-quic-trainer-pipeline-smoke.json",
            {
                "format": "cxnfs.trainer-pipeline-smoke.v1",
                "passed": True,
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-tensors",
                "training_pipeline_object": "CxnfsTrainingPipeline",
                "training_pipeline_context": "CxnfsTrainingPipeline.prefetch_trainer_stream",
                "pipeline_descriptor_stream": "CxnfsTrainerPipelineStream",
                "pipeline_prefetch_stream_used": True,
                "pipeline_prefetch_batches": 2,
                "pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch",
                "pipeline_python_byte_batch_avoided": True,
                "pipeline_network_path": dict(pipeline_network_path),
                "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
                "pipeline_network_path_complete": True,
                "pipeline_network_read_workers": 16,
                "pipeline_max_inflight_bytes": 1073741824,
                "read_path_evidence": dict(read_path),
                "read_path_reliability": dict(read_reliability),
                "tensor_memory_path": {"complete": True},
                "remote_client_topology": {"per_source_client_observed": True},
                "remote_client_lifecycle_closed": True,
                "write_probe": {
                    "format": "cxnfs.trainer-pipeline-write-probe.v1",
                    "enabled": True,
                    "verified_read_back": True,
                    "byte_size": 4096,
                    "frame_bytes": 1024,
                    "write_path_evidence": {
                        "format": "cxnfs.bulk-write-path-evidence.v1",
                        "direct_tensor_write_operations": 1,
                        "max_observed_write_cords": 4,
                        "write_frames": 4,
                        "write_acks": 4,
                        "write_ack_frame_verified": 4,
                        "write_ack_frame_identity_verified": True,
                        "write_ack_stream_completion_verified": True,
                        "write_ack_stream_eof_verified": True,
                        "write_ack_metadata_pending": 4,
                        "metadata_after_payload_entries": 1,
                        "entries_committed": 1,
                        "transfers_committed": 1,
                        "bulk_transport_write_evidence": {
                            "format": "cxnfs.quic-bulk-write-transport-evidence.v1",
                            "cipher_suite": "TLS_AES_128_GCM_SHA256",
                            "connection_reused_for_streams": True,
                            "attempted_write_streams": 4,
                            "write_streams_created": 4,
                            "write_ack_streams_eof_verified": 4,
                            "write_acks_verified": 4,
                            "write_ack_verification_failures": 0,
                            "write_ack_frame_identity_verified": True,
                            "write_ack_stream_completion_verified": True,
                            "write_ack_stream_eof_verified": True,
                            "max_active_write_streams": 2,
                        },
                    },
                },
            },
        )
        write_json(
            report_dir / "python-quic-trainer-workers.json",
            {
                "format": "cxnfs.trainer-stream-smoke.v1",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_batch_iterator": "trainer-dataloader",
                "data_loader_workers": 2,
                "read_path_evidence": {
                    **read_path,
                    "worker_batch_read_evidence_observed": True,
                    "worker_batch_read_evidence_count": 2,
                    "worker_process_read_path_evidence": [
                        {
                            "produced_inside_torch_worker": True,
                            "worker_count": 2,
                        }
                    ],
                },
                "read_path_reliability": dict(read_reliability),
                "worker_partition_evidence": {
                    "worker_sharding_enabled": True,
                    "complete": True,
                    "data_loader_workers": 2,
                    "duplicate_source_sample_count": 0,
                },
            },
        )
        write_json(
            report_dir / "python-quic-trainer-resume.json",
            {
                "format": "cxnfs.quic-trainer-resume.v1",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-buffers",
                "trainer_prefetch_enabled": True,
                "trainer_prefetch_batches": 2,
                "bulk_read_retries": 1,
                "cursor_format": "cxnfs.trainer-cursor.v1",
                "stream_state_format": "cxnfs.trainer-stream-state.v1",
                "state_file_written": True,
                "state_file_replayed": resume_state_file_replayed,
                "state_stream_state_matches_final_cursor": resume_state_file_replayed,
                "first_final_emitted": 2,
                "second_initial_emitted": 2,
                "second_final_emitted": 4,
                "read_path_reliability_complete": True,
                "unrecovered_read_failures": 0,
                **{
                    **read_path,
                    "direct_destination_frame_sink_cord_fanout_verified": False,
                    "direct_destination_max_observed_cords": 1,
                    "direct_destination_parallel_frame_sink_read_operations": 0,
                    "direct_destination_parallel_frame_sink_frames": 0,
                },
            },
        )
    write_json(
        report_dir / "python-quic-trainer-contract-surface-matrix.json",
        {
            "format": "cxnfs.trainer-surface-matrix.v1",
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "passed": True,
            "read_path_reliability_complete": True,
            "unrecovered_read_failures": 0,
            "cases": [
                {"read_path_evidence": dict(read_path), "read_path_reliability": dict(read_reliability)},
                {"read_path_evidence": dict(read_path), "read_path_reliability": dict(read_reliability)},
            ],
        },
    )
    write_json(
        report_dir / "python-quic-trainer-contract-workload-benchmark.json",
        {
            "format": "cxnfs.trainer-workload-benchmark.v1",
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "contract_applied": True,
            "read_path_evidence": dict(read_path),
            "read_path_reliability": dict(read_reliability),
            "sustained_read_profile": dict(sustained_read_profile),
            "write_path_evidence": dict(write_path),
            "run_count": 4 if sustained_read_profile_complete else 1,
            "batches_per_run": 2 if sustained_read_profile_complete else 1,
            "sample_count": 24 if sustained_read_profile_complete else 3,
            "byte_size": 3145728 if sustained_read_profile_complete else 4096,
            "cord_sweep": [
                {"cord_index": 0, "initial_cords": 1, "max_cords": 1},
                {"cord_index": 1, "initial_cords": 4, "max_cords": 4},
            ],
            "network_path_summary": {
                "java_python_network_path_exercised": True,
                "cord_sweep_count": 2,
            },
            "blockfs_gate": {
                "read_path_evidence_complete": True,
                "sustained_read_profile_complete": sustained_read_profile_complete,
                "sustained_read_profile": dict(sustained_read_profile),
                "memory_path_evidence_complete": True,
                "write_path_evidence_complete": True,
                "read_path_gate": dict(read_path),
            },
            "pressure_profile": {
                "format": "cxnfs.trainer-workload-pressure.v1",
                "sustained_read_profile_complete": sustained_read_profile_complete,
                "sustained_read_run_count": sustained_read_profile["run_count"],
                "sustained_read_total_batches": sustained_read_profile["total_batches"],
                "sustained_read_total_samples": sustained_read_profile["sample_count"],
                "sustained_read_total_bytes": sustained_read_profile["byte_size"],
            },
        },
    )
    write_json(
        report_dir / "python-quic-trainer-integration-smoke.json",
        {
            "format": "cxnfs.trainer-integration-smoke.v1",
            "passed": True,
            "remote_client_required": True,
            "stream_opened": True,
            "stream_kwargs_accepted": True,
            "stream_state_matches_final_cursor": True,
            "trainer_prefetch_batches": 2,
            "training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop",
            "training_loop_context_used": smoke_training_loop_context_used,
            "training_loop_object": "CxnfsTrainerPrefetchStream",
            "training_loop_base_stream_object": "CxnfsTrainerStream",
            "training_loop_prefetch_batches": 2,
            "training_loop_prefetch_applied": smoke_training_loop_context_used,
            "trainer_batch_iterator": "native",
            "data_loader_workers": 0,
            "dataloader_worker_partitioning_enabled": False,
            "worker_partition_complete": True,
            "worker_batch_read_evidence_observed": False,
            "worker_batch_read_evidence_count": 0,
            "remote_client_warmup_complete": True,
            "remote_client_warmup_source_count": 2,
            "remote_client_warmup_warmed_source_count": 2,
            "remote_client_topology_complete": True,
            "remote_client_per_source_complete": True,
            "remote_client_lifecycle_closed": True,
            "stream_report": {
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_path_evidence": dict(read_path),
                "read_path_reliability": dict(read_reliability),
            },
        },
    )
    write_json(
        report_dir / "python-quic-trainer-runtime-smoke.json",
        {
            "format": "cxnfs.trainer-runtime-smoke.v1",
            "passed": True,
            "handoff_format": "cxnfs.trainer-handoff.v1",
            "plan_format": "cxnfs.trainer-integration-plan.v1",
            "runtime_binding_format": "cxnfs.trainer-runtime-binding.v1",
            "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
            "remote_client_required": True,
            "requires_credentials": True,
            "stream_opened": True,
            "stream_kwargs_accepted": True,
            "stream_state_matches_final_cursor": True,
            "raw_password_sent_over_control": raw_password_sent,
            "read_mode": "direct-tensors",
            "trainer_prefetch_batches": 2,
            "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "training_loop_context_used": True,
            "training_loop_object": "CxnfsTrainerPrefetchStream",
            "training_loop_base_stream_object": "CxnfsTrainerStream",
            "training_loop_prefetch_batches": 2,
            "training_loop_prefetch_applied": True,
            "pipeline_training_loop_required": True,
            "pipeline_training_loop_verified": True,
            "pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
            "pipeline_training_loop_context_used": True,
            "pipeline_training_loop_object": "CxnfsTrainerPipelineLoop",
            "pipeline_network_path_verified": True,
            "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
            "pipeline_training_loop_report": {
                "required": True,
                "verified": True,
                "context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                "context_used": True,
                "object": "CxnfsTrainerPipelineLoop",
                "batch_count": 2,
                "sample_count": 6,
                "byte_size": 786432,
                "stream_state_format": "cxnfs.trainer-stream-state.v1",
                "pipeline_network_path": dict(pipeline_network_path),
                "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
                "pipeline_network_path_complete": True,
                "pipeline_network_read_workers": 16,
                "pipeline_max_inflight_bytes": 1073741824,
            },
            "trainer_batch_iterator": "native",
            "data_loader_workers": 0,
            "dataloader_worker_partitioning_enabled": False,
            "worker_partition_complete": True,
            "worker_batch_read_evidence_observed": False,
            "worker_batch_read_evidence_count": 0,
            "remote_client_warmup_complete": True,
            "remote_client_warmup_source_count": 2,
            "remote_client_warmup_warmed_source_count": 2,
            "remote_client_topology_complete": True,
            "remote_client_per_source_complete": True,
            "remote_client_lifecycle_closed": True,
            "stream_report": {
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_mode": "direct-tensors",
                "read_path_evidence": dict(read_path),
                "read_path_reliability": dict(read_reliability),
                "remote_client_topology_after_close": {"all_open_clients_closed": True},
            },
        },
    )
    write_json(
        report_dir / "python-quic-trainer-distributed-runtime-smoke.json",
        {
            "format": "cxnfs.trainer-distributed-runtime-smoke.v1",
            "passed": True,
            "handoff_format": "cxnfs.trainer-handoff.v1",
            "plan_format": "cxnfs.trainer-integration-plan.v1",
            "distributed_plan_format": "cxnfs.trainer-distributed-plan.v1",
            "runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1",
            "runtime_binding_format": "cxnfs.trainer-runtime-binding.v1",
            "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
            "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
            "training_loop_context_used": True,
            "world_size": 2,
            "rank_count": 2,
            "ranks": [0, 1],
            "total_sample_count": 12,
            "remote_client_required": True,
            "raw_password_sent_over_control": raw_password_sent,
            "all_rank_streams_opened": True,
            "all_rank_stream_kwargs_accepted": True,
            "all_rank_state_matches_final_cursor": True,
            "all_rank_training_loop_context_used": True,
            "all_rank_training_shards_correct": True,
            "no_cross_rank_overlap": True,
            "all_open_clients_closed": True,
            "distributed_runtime_topology_complete": True,
            "observed_sources_complete": True,
            "rank_state_file_count": 2,
            "all_rank_state_files_written": True,
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "rank_sample_overlap": {
                "format": "cxnfs.trainer-distributed-runtime-overlap.v1",
                "no_cross_rank_overlap": True,
                "duplicate_observed_sample_count": 0,
                "unique_observed_samples": 12,
            },
            "remote_client_topology_after_close": {
                "format": "cxnfs.trainer-distributed-runtime-topology.v1",
                "complete": True,
                "all_open_clients_closed": True,
                "observed_sources_complete": True,
            },
            "rank_state_artifacts": {
                "format": "cxnfs.trainer-distributed-runtime-rank-states.v1",
                "rank_count": 2,
                "rank_state_file_count": 2,
                "all_rank_state_files_written": True,
            },
            "read_path_evidence": dict(read_path),
            "read_path_reliability": dict(read_reliability),
            "rank_smokes": [
                {
                    "format": "cxnfs.trainer-runtime-smoke.v1",
                    "stream_report": {
                        "transport": "cxnfs-quic",
                        "bulk_transport": "quic",
                        "read_path_evidence": dict(read_path),
                        "read_path_reliability": dict(read_reliability),
                    },
                }
            ],
        },
    )
    write_json(
        report_dir / "python-quic-trainer-loop-replay.json",
        {
            "format": "cxnfs.trainer-loop-replay.v1",
            "passed": loop_replay_passed,
            "handoff_format": "cxnfs.trainer-handoff.v1",
            "plan_format": "cxnfs.trainer-integration-plan.v1",
            "remote_client_required": True,
            "step_count": 2,
            "batch_count": 2,
            "sample_count": 6,
            "final_emitted": 6,
            "weighted_source_coverage_complete": True,
            "resume_replayed": True,
            "resume_continuity_verified": True,
            "trainer_prefetch_batches": 2,
            "training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop",
            "training_loop_context_used": True,
            "training_loop_object": "CxnfsTrainerPrefetchStream",
            "training_loop_base_stream_object": "CxnfsTrainerStream",
            "training_loop_prefetch_batches": 2,
            "training_loop_prefetch_applied": True,
            "stream_report": {
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_path_evidence": dict(read_path),
                "read_path_reliability": dict(read_reliability),
            },
        },
    )
    if not omit_trainer_readiness_bundle:
        bundle_passed = (
            not raw_password_sent
            and smoke_training_loop_context_used
            and loop_replay_passed
        )
        readiness_payload = {
            "format": "cxnfs.trainer-readiness-bundle.v1",
            "passed": bundle_passed,
            "handoff_format": "cxnfs.trainer-handoff.v1",
            "preflight_format": "cxnfs.trainer-integration-preflight.v1",
            "integration_smoke_format": "cxnfs.trainer-integration-smoke.v1",
            "runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1",
            "distributed_runtime_smoke_format": "cxnfs.trainer-distributed-runtime-smoke.v1",
            "loop_replay_format": "cxnfs.trainer-loop-replay.v1",
            "ready_for_trainer_wiring": not raw_password_sent,
            "ready_for_remote_quic": True,
            "ready_for_blockfs_candidate": False,
            "requires_low_level_direct_read_proofs": True,
            "low_level_direct_read_proofs_complete": True,
            "batch_direct_read_proof_complete": True,
            "batch_direct_tensor_read_proof_complete": True,
            "batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256",
            "batch_direct_tensor_read_cipher_suite": "TLS_AES_128_GCM_SHA256",
            "requires_low_level_direct_write_proofs": True,
            "low_level_direct_write_proofs_complete": True,
            "write_probe_proof_complete": True,
            "write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256",
            "write_probe_quic_transport_complete": True,
            "write_probe_max_observed_write_cords": 4,
            "direct_quic_memory_path_verified": True,
            "remote_client_required": True,
            "requires_credentials": True,
            "raw_password_sent_over_control": raw_password_sent,
            "direct_python_streaming_verified": bundle_passed,
            "runtime_training_loop_verified": True,
            "pipeline_training_loop_required": True,
            "pipeline_training_loop_verified": True,
            "pipeline_network_path_verified": True,
            "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
            "distributed_runtime_verified": True,
            "distributed_rank_states_written": True,
            "loop_resume_verified": loop_replay_passed,
            "world_size": 2,
            "runtime_rank": 0,
            "report_artifacts": {
                "format": "cxnfs.trainer-readiness-bundle-artifacts.v1",
                "artifact_count": 5,
            },
            "rank_state_artifacts": {
                "format": "cxnfs.trainer-distributed-runtime-rank-states.v1",
                "rank_count": 2,
                "rank_state_file_count": 2,
                "all_rank_state_files_written": True,
            },
            "reports": {
                    "preflight": {
                        "format": "cxnfs.trainer-integration-preflight.v1",
                        "passed": not raw_password_sent,
                        "ready_for_trainer_wiring": not raw_password_sent,
                        "ready_for_remote_quic": True,
                        "low_level_direct_read_proofs_complete": True,
                        "batch_direct_read_proof_complete": True,
                        "batch_direct_tensor_read_proof_complete": True,
                        "batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256",
                        "batch_direct_tensor_read_cipher_suite": "TLS_AES_128_GCM_SHA256",
                        "low_level_direct_write_proofs_complete": True,
                        "write_probe_proof_complete": True,
                        "write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256",
                        "write_probe_quic_transport_complete": True,
                        "write_probe_max_observed_write_cords": 4,
                        "raw_password_sent_over_control": raw_password_sent,
                    },
                    "integration_smoke": {
                        "format": "cxnfs.trainer-integration-smoke.v1",
                        "passed": smoke_training_loop_context_used,
                        "remote_client_required": True,
                        "stream_opened": True,
                        "stream_kwargs_accepted": True,
                        "stream_state_matches_final_cursor": True,
                        "trainer_prefetch_batches": 2,
                        "training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop",
                        "training_loop_context_used": smoke_training_loop_context_used,
                        "training_loop_object": "CxnfsTrainerPrefetchStream",
                        "training_loop_base_stream_object": "CxnfsTrainerStream",
                        "training_loop_prefetch_batches": 2,
                        "training_loop_prefetch_applied": smoke_training_loop_context_used,
                        "trainer_batch_iterator": "native",
                        "data_loader_workers": 0,
                        "dataloader_worker_partitioning_enabled": False,
                        "worker_partition_complete": True,
                        "worker_batch_read_evidence_observed": False,
                        "worker_batch_read_evidence_count": 0,
                        "remote_client_warmup_complete": True,
                        "remote_client_warmup_source_count": 2,
                        "remote_client_warmup_warmed_source_count": 2,
                        "remote_client_topology_complete": True,
                        "remote_client_per_source_complete": True,
                        "remote_client_lifecycle_closed": True,
                        "stream_report": {
                            "transport": "cxnfs-quic",
                            "bulk_transport": "quic",
                            "read_path_evidence": dict(read_path),
                            "read_path_reliability": dict(read_reliability),
                        },
                    },
                    "runtime_smoke": {
                        "format": "cxnfs.trainer-runtime-smoke.v1",
                        "passed": True,
                        "runtime_binding_format": "cxnfs.trainer-runtime-binding.v1",
                        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
                        "remote_client_required": True,
                        "stream_opened": True,
                        "stream_kwargs_accepted": True,
                        "stream_state_matches_final_cursor": True,
                        "raw_password_sent_over_control": raw_password_sent,
                        "read_mode": "direct-tensors",
                        "trainer_prefetch_batches": 2,
                        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
                        "training_loop_context_used": True,
                        "training_loop_object": "CxnfsTrainerPrefetchStream",
                        "training_loop_base_stream_object": "CxnfsTrainerStream",
                        "training_loop_prefetch_batches": 2,
                        "training_loop_prefetch_applied": True,
                        "pipeline_training_loop_required": True,
                        "pipeline_training_loop_verified": True,
                        "pipeline_training_loop_context": (
                            "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"
                        ),
                        "pipeline_training_loop_context_used": True,
                        "pipeline_training_loop_object": "CxnfsTrainerPipelineLoop",
                        "pipeline_network_path_verified": True,
                        "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
                        "pipeline_training_loop_report": {
                            "required": True,
                            "verified": True,
                            "context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                            "context_used": True,
                            "object": "CxnfsTrainerPipelineLoop",
                            "batch_count": 2,
                            "sample_count": 6,
                            "byte_size": 786432,
                            "stream_state_format": "cxnfs.trainer-stream-state.v1",
                            "pipeline_network_path": dict(pipeline_network_path),
                            "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
                            "pipeline_network_path_complete": True,
                            "pipeline_network_read_workers": 16,
                            "pipeline_max_inflight_bytes": 1073741824,
                        },
                        "trainer_batch_iterator": "native",
                        "data_loader_workers": 0,
                        "dataloader_worker_partitioning_enabled": False,
                        "worker_partition_complete": True,
                        "worker_batch_read_evidence_observed": False,
                        "worker_batch_read_evidence_count": 0,
                        "remote_client_warmup_complete": True,
                        "remote_client_warmup_source_count": 2,
                        "remote_client_warmup_warmed_source_count": 2,
                        "remote_client_topology_complete": True,
                        "remote_client_per_source_complete": True,
                        "remote_client_lifecycle_closed": True,
                        "stream_report": {
                            "transport": "cxnfs-quic",
                            "bulk_transport": "quic",
                            "read_mode": "direct-tensors",
                            "read_path_evidence": dict(read_path),
                            "read_path_reliability": dict(read_reliability),
                            "remote_client_topology_after_close": {
                                "all_open_clients_closed": True,
                            },
                        },
                    },
                    "distributed_runtime_smoke": {
                        "format": "cxnfs.trainer-distributed-runtime-smoke.v1",
                        "passed": True,
                        "runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1",
                        "runtime_binding_format": "cxnfs.trainer-runtime-binding.v1",
                        "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
                        "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
                        "training_loop_context_used": True,
                        "world_size": 2,
                        "rank_count": 2,
                        "total_sample_count": 12,
                        "remote_client_required": True,
                        "raw_password_sent_over_control": raw_password_sent,
                        "all_rank_streams_opened": True,
                        "all_rank_stream_kwargs_accepted": True,
                        "all_rank_state_matches_final_cursor": True,
                        "all_rank_training_loop_context_used": True,
                        "all_rank_training_shards_correct": True,
                        "no_cross_rank_overlap": True,
                        "all_open_clients_closed": True,
                        "rank_state_file_count": 2,
                        "all_rank_state_files_written": True,
                        "transport": "cxnfs-quic",
                        "bulk_transport": "quic",
                        "rank_sample_overlap": {
                            "no_cross_rank_overlap": True,
                            "duplicate_observed_sample_count": 0,
                        },
                        "remote_client_topology_after_close": {
                            "complete": True,
                            "all_open_clients_closed": True,
                        },
                        "rank_state_artifacts": {
                            "format": "cxnfs.trainer-distributed-runtime-rank-states.v1",
                            "rank_count": 2,
                            "rank_state_file_count": 2,
                            "all_rank_state_files_written": True,
                        },
                        "read_path_evidence": dict(read_path),
                        "read_path_reliability": dict(read_reliability),
                    },
                    "loop_replay": {
                        "format": "cxnfs.trainer-loop-replay.v1",
                        "passed": loop_replay_passed,
                        "handoff_format": "cxnfs.trainer-handoff.v1",
                        "plan_format": "cxnfs.trainer-integration-plan.v1",
                        "remote_client_required": True,
                        "step_count": 2,
                        "batch_count": 2,
                        "sample_count": 6,
                        "final_emitted": 6,
                        "weighted_source_coverage_complete": True,
                        "resume_replayed": True,
                        "resume_continuity_verified": loop_replay_passed,
                        "trainer_prefetch_batches": 2,
                        "training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop",
                        "training_loop_context_used": True,
                        "training_loop_object": "CxnfsTrainerPrefetchStream",
                        "training_loop_base_stream_object": "CxnfsTrainerStream",
                        "training_loop_prefetch_batches": 2,
                        "training_loop_prefetch_applied": True,
                        "stream_report": {
                            "transport": "cxnfs-quic",
                            "bulk_transport": "quic",
                            "read_path_evidence": dict(read_path),
                            "read_path_reliability": dict(read_reliability),
                        },
                    },
            },
        }
        write_json(report_dir / "python-quic-trainer-readiness-bundle.json", readiness_payload)
        write_json(
            report_dir / "python-quic-trainer-handoff-acceptance.json",
            {
                "format": "cxnfs.trainer-handoff-acceptance.v1",
                "passed": bundle_passed,
                "handoff_format": "cxnfs.trainer-handoff.v1",
                "readiness_bundle_format": "cxnfs.trainer-readiness-bundle.v1",
                "ready_for_trainer_wiring": not raw_password_sent,
                "ready_for_remote_quic": True,
                "requires_remote_quic": True,
                "direct_quic_memory_path_verified": True,
                "direct_python_streaming_verified": bundle_passed,
                "runtime_training_loop_verified": True,
                "pipeline_training_loop_required": True,
                "pipeline_training_loop_verified": True,
                "pipeline_network_path_verified": True,
                "pipeline_network_path_format": "cxnfs.trainer-pipeline-network-path.v1",
                "distributed_runtime_verified": True,
                "loop_resume_verified": loop_replay_passed,
                "cross_rank_overlap_free": True,
                "rank_states_written": True,
                "remote_client_required": True,
                "requires_credentials": True,
                "raw_password_sent_over_control": raw_password_sent,
                "requires_mount": False,
                "trainer_code_touched": False,
                "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
                "training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop",
                "pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop",
                "trainer_loop_contract_format": "cxnfs.trainer-loop-contract.v1",
                "trainer_loop_contract": live_trainer_loop_contract_fixture(
                    passed=bundle_passed,
                    raw_password_sent=raw_password_sent,
                    loop_replay_passed=loop_replay_passed,
                ),
                "trainer_handoff_surface": {
                    "format": "cxnfs.trainer-handoff-surface.v1",
                    "uses_mount": False,
                    "touches_trainer_code": False,
                    "password_cli_argument": False,
                },
                "reports": {
                    "readiness_bundle": readiness_payload,
                },
            },
        )
    if not omit_trainer_network_preflight:
        write_json(
            report_dir / "python-quic-trainer-network-preflight.json",
            {
                "format": "cxnfs.trainer-network-preflight.v1",
                "passed": not raw_password_sent,
                "handoff_format": "cxnfs.trainer-handoff.v1",
                "preflight_format": "cxnfs.trainer-integration-preflight.v1",
                "ready_for_trainer_wiring": not raw_password_sent,
                "ready_for_remote_quic": True,
                "remote_client_required": True,
                "requires_credentials": True,
                "raw_password_sent_over_control": raw_password_sent,
                "safe_to_run_before_trainer_workers": True,
                "opens_network_connections": True,
                "consumes_batches": False,
                "authenticated": True,
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "probe_path": "/stage0-mds",
                "probe_entries": 2,
                "bulk_probe": "ok",
                "bulk_probe_path": "/stage0-mds/chat/chat-000000.mds",
                "bulk_probe_bytes": 1,
                "read_length": 1,
                "network_probe_is_handoff_aware": True,
            },
        )
    write_json(
        report_dir / "python-quic-trainer-worker-loop-replay.json",
        {
            "format": "cxnfs.trainer-loop-replay.v1",
            "passed": True,
            "handoff_format": "cxnfs.trainer-handoff.v1",
            "plan_format": "cxnfs.trainer-integration-plan.v1",
            "remote_client_required": True,
            "read_mode": "direct-buffers",
            "trainer_batch_iterator": "trainer-dataloader",
            "data_loader_workers": 2,
            "samples_per_epoch": 8,
            "repeat": worker_loop_repeat,
            "worker_partitioning_enabled": True,
            "worker_cursor_count": 2,
            "worker_batch_read_evidence_observed": True,
            "worker_batch_read_evidence_count": 2,
            "step_count": 4,
            "batch_count": 4,
            "sample_count": 8,
            "final_emitted": 8,
            "weighted_source_coverage_complete": True,
            "resume_replayed": True,
            "resume_continuity_verified": True,
            "training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop",
            "training_loop_context_used": True,
            "training_loop_object": "CxnfsTrainerStream",
            "training_loop_base_stream_object": "CxnfsTrainerStream",
            "training_loop_prefetch_batches": 0,
            "training_loop_prefetch_applied": False,
            "worker_partition_evidence": {
                "format": "cxnfs.trainer-worker-partition.v1",
                "worker_sharding_enabled": True,
                "complete": True,
                "data_loader_workers": 2,
                "duplicate_source_sample_count": 0,
            },
            "stream_report": {
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "read_path_evidence": {
                    **read_path,
                    "worker_batch_read_evidence_observed": True,
                    "worker_batch_read_evidence_count": 2,
                    "worker_process_read_path_evidence": [
                        {
                            "produced_inside_torch_worker": True,
                            "worker_count": 2,
                        }
                    ],
                },
                "read_path_reliability": dict(read_reliability),
            },
        },
    )
    write_json(
        report_dir / "python-quic-trainer-distributed-plan.json",
        {
            "format": "cxnfs.trainer-distributed-plan.v1",
            "passed": True,
            "ready_for_trainer_wiring": True,
            "ready_for_remote_quic": True,
            "low_level_direct_read_proofs_complete": True,
            "batch_direct_read_proof_complete": True,
            "batch_direct_tensor_read_proof_complete": True,
            "low_level_direct_write_proofs_complete": True,
            "write_probe_proof_complete": True,
            "write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256",
            "remote_client_required": True,
            "requires_credentials": True,
            "opens_network_connections": False,
            "consumes_batches": False,
            "raw_password_sent_over_control": False,
            "world_size": 2,
            "rank_count": 2,
            "selected_rank": 1,
            "runtime_binding_format": "cxnfs.trainer-runtime-binding.v1",
            "runtime_binding_object": "CxnfsTrainerRuntimeBinding",
            "rank_plans": [
                {"rank": 0, "world_size": 2, "training_shard": {"index": 0, "count": 2}},
                {"rank": 1, "world_size": 2, "training_shard": {"index": 1, "count": 2}},
            ],
        },
    )
    if not omit_cli_smoke:
        write_json(
            report_dir / "python-client-cli-quic-smoke.json",
            {
                "format": "cxnfs.python-client-cli-quic-smoke.v1",
                "transport": "cxnfs-quic",
                "bulk_transport": "quic",
                "list_entries": 1,
                "read_bytes": 524288,
                "read_max_observed_cords": 4,
                "read_cipher_suite": "TLS_AES_128_GCM_SHA256",
                "checkpoint_bytes_sent": 16384,
                "write_max_observed_cords": 4,
                "write_max_active_streams": 2,
                "write_cipher_suite": "TLS_AES_128_GCM_SHA256",
                "direct_file_bytes_sent": 20480,
                "direct_write_max_observed_cords": 4,
                "direct_write_cipher_suite": "TLS_AES_128_GCM_SHA256",
                "server_bulk_listener": "quic-tls13-udp://127.0.0.1:4510",
                "server_bulk_cipher_suite": "tls_aes_128_gcm_sha256",
                "server_bulk_total_streams": 16 if cli_server_bulk_total_streams_observed else 0,
                "server_bulk_total_streams_observed": cli_server_bulk_total_streams_observed,
                "direct_file_write_verified": cli_direct_file_write_verified,
                "read_back_verified": True,
                "raw_password_visible": cli_raw_password_visible,
            },
        )
    if not omit_batch_direct_read:
        write_json(report_dir / "python-quic-batch-direct-read.json", batch_direct_read_report())
    if not omit_batch_direct_tensor_read:
        write_json(
            report_dir / "python-quic-batch-direct-tensor-read.json",
            batch_direct_tensor_read_report(),
        )


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def batch_direct_read_report() -> dict:
    return {
        "format": "cxnfs.quic-batch-direct-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "batch_read_count": 2,
        "batch_worker_count": 2,
        "ordered_batch_read_verified": True,
        "bytes_read": 1048576,
        "read_operations": 2,
        "direct_buffer_read_operations": 2,
        "direct_destination_read_operations": 2,
        "payload_bytes_read": 1048576,
        "transport_frame_sink_observed": True,
        "max_observed_cords": 4,
        "direct_destination_max_observed_cords": 4,
        "direct_destination_parallel_frame_sink_read_operations": 2,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "last_accepted_initial_cords": 4,
        "last_accepted_max_cords": 4,
        "observed_cords_within_admission": True,
        "bulk_transport_read_evidence": {
            "format": "cxnfs.quic-bulk-transport-evidence.v1",
            "tls_version": "TLSv1.3",
            "cipher_suite": "TLS_AES_128_GCM_SHA256",
            "successful_read_operations": 2,
            "failed_read_operations": 0,
            "attempted_read_streams": 8,
            "read_streams_created": 8,
            "read_streams_eof_verified": 8,
            "read_stream_completion_verified": True,
            "read_stream_eof_verified": True,
            "connection_reused_for_streams": True,
        },
    }


def batch_direct_tensor_read_report() -> dict:
    return {
        "format": "cxnfs.quic-batch-direct-tensor-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "tensor_target": "pytorch-cpu",
        "tensor_dtype": "uint8",
        "tensor_device": "cpu",
        "tensor_read_count": 2,
        "tensor_worker_count": 2,
        "ordered_tensor_read_verified": True,
        "bytes_read": 1048576,
        "read_operations": 2,
        "direct_buffer_read_operations": 0,
        "direct_tensor_read_operations": 2,
        "direct_destination_read_operations": 2,
        "payload_bytes_read": 1048576,
        "transport_frame_sink_observed": True,
        "max_observed_cords": 4,
        "direct_destination_max_observed_cords": 4,
        "direct_destination_parallel_frame_sink_read_operations": 2,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "last_accepted_initial_cords": 4,
        "last_accepted_max_cords": 4,
        "observed_cords_within_admission": True,
        "tensor_memory_path": {
            "format": "cxnfs.trainer-tensor-memory-path.v1",
            "complete": True,
            "target": "pytorch-cpu",
            "dtype": "uint8",
            "device": "cpu",
            "python_byte_batch_avoided": True,
            "network_fills_tensor_destination": True,
            "device_copy_required": False,
        },
        "bulk_transport_read_evidence": {
            "format": "cxnfs.quic-bulk-transport-evidence.v1",
            "tls_version": "TLSv1.3",
            "cipher_suite": "TLS_AES_128_GCM_SHA256",
            "successful_read_operations": 2,
            "failed_read_operations": 0,
            "attempted_read_streams": 8,
            "read_streams_created": 8,
            "read_streams_eof_verified": 8,
            "read_stream_completion_verified": True,
            "read_stream_eof_verified": True,
            "connection_reused_for_streams": True,
        },
    }


def write_high_cord_reports(
    report_dir: Path,
    *,
    include_transport_frame_sink_observed: bool = True,
    include_batch_direct_read: bool = True,
    include_batch_direct_tensor_read: bool = True,
) -> None:
    read_report = {
        "format": "cxnfs.quic-high-cord-read.v1",
        "transport": "cxnfs-quic",
        "bulk_transport": "quic",
        "tls_version": "TLSv1.3",
        "cipher_suite": "TLS_AES_128_GCM_SHA256",
        "byte_size": 9437184,
        "bytes_read": 9437184,
        "max_observed_cords": 64,
        "last_read_stream_count": 64,
        "direct_destination_read_operations": 1,
        "direct_destination_parallel_frame_sink_read_operations": 1,
        "direct_destination_parallel_frame_sink_frames": 72,
        "direct_destination_max_observed_cords": 64,
        "direct_destination_frame_sink_cord_fanout_verified": True,
        "frame_sink_used_for_all_direct_destination_reads": True,
        "last_accepted_initial_cords": 1,
        "last_accepted_max_cords": 64,
        "observed_cords_within_admission": True,
        "bulk_transport_read_evidence": {
            "format": "cxnfs.quic-bulk-transport-evidence.v1",
            "tls_version": "TLSv1.3",
            "cipher_suite": "TLS_AES_128_GCM_SHA256",
            "last_read_stream_count": 64,
            "attempted_read_streams": 64,
            "read_streams_created": 64,
            "read_streams_eof_verified": 64,
            "read_stream_completion_verified": True,
            "read_stream_eof_verified": True,
        },
    }
    if include_transport_frame_sink_observed:
        read_report["transport_frame_sink_observed"] = True

    write_json(report_dir / "python-quic-high-cord-read.json", read_report)
    write_json(
        report_dir / "python-quic-high-cord-write.json",
        {
            "format": "cxnfs.quic-high-cord-write.v1",
            "transport": "cxnfs-quic",
            "bulk_transport": "quic",
            "tls_version": "TLSv1.3",
            "cipher_suite": "TLS_AES_128_GCM_SHA256",
            "bytes_written": 262144,
            "write_frames": 64,
            "write_acks": 64,
            "write_ack_frame_verified": 64,
            "write_ack_frame_identity_verified": True,
            "write_ack_stream_completion_verified": True,
            "write_ack_stream_eof_verified": True,
            "max_observed_write_cords": 64,
            "read_back_verified": True,
            "bulk_transport_write_evidence": {
                "format": "cxnfs.quic-bulk-write-transport-evidence.v1",
                "tls_version": "TLSv1.3",
                "cipher_suite": "TLS_AES_128_GCM_SHA256",
                "write_streams_created": 64,
                "attempted_write_streams": 64,
                "write_ack_streams_eof_verified": 64,
                "successful_write_operations": 64,
                "write_acks_received": 64,
                "write_acks_verified": 64,
                "write_ack_verification_failures": 0,
                "write_ack_frame_identity_verified": True,
                "write_ack_stream_completion_verified": True,
                "write_ack_stream_eof_verified": True,
            },
        },
    )
    if include_batch_direct_read:
        write_json(report_dir / "python-quic-batch-direct-read.json", batch_direct_read_report())
    if include_batch_direct_tensor_read:
        write_json(
            report_dir / "python-quic-batch-direct-tensor-read.json",
            batch_direct_tensor_read_report(),
        )


if __name__ == "__main__":
    unittest.main()
