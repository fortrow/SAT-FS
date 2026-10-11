from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "python" / "pyproject.toml"
LIVE_QUIC_CHECK = ROOT / "packaging" / "live-quic-check.sh"
RELEASE_CHECK = ROOT / "packaging" / "release-check.sh"


class CxnfsPythonPackagingTest(unittest.TestCase):
    def test_python_console_scripts_are_packaged_without_password_flags(self):
        text = PYPROJECT.read_text(encoding="utf-8")

        self.assertIn("[project.scripts]", text)
        self.assertIn('cxnfs-client = "cxnfs.client_cli:main"', text)
        self.assertIn('cxnfs-health = "cxnfs.health:main"', text)
        self.assertIn(
            'cxnfs-trainer-acceptance-fixture = "cxnfs.torch_dataset:main_trainer_acceptance_fixture"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-integration-smoke = "cxnfs.torch_dataset:main_trainer_integration_smoke"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-loop-replay = "cxnfs.torch_dataset:main_trainer_loop_replay"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-pipeline-smoke = "cxnfs.torch_dataset:main_trainer_pipeline_smoke"',
            text,
        )
        self.assertIn('cxnfs-trainer-handoff = "cxnfs.torch_dataset:main_trainer_handoff"', text)
        self.assertIn(
            'cxnfs-trainer-network-preflight = "cxnfs.torch_dataset:main_trainer_network_preflight"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-handoff-acceptance = "cxnfs.torch_dataset:main_trainer_handoff_acceptance"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-distributed-plan = "cxnfs.torch_dataset:main_trainer_distributed_plan"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-runtime-smoke = "cxnfs.torch_dataset:main_trainer_runtime_smoke"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-distributed-runtime-smoke = "cxnfs.torch_dataset:main_trainer_distributed_runtime_smoke"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-readiness-bundle = "cxnfs.torch_dataset:main_trainer_readiness_bundle"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-loop-contract-check = "cxnfs.torch_dataset:main_trainer_loop_contract_check"',
            text,
        )
        self.assertIn(
            'cxnfs-trainer-workload-benchmark = "cxnfs.torch_dataset:main_trainer_workload_benchmark"',
            text,
        )
        self.assertNotIn("--password", text)

    def test_live_quic_required_handoff_checks_torch_before_running_suite(self):
        text = LIVE_QUIC_CHECK.read_text(encoding="utf-8")

        self.assertIn("check_handoff_python_prerequisites", text)
        self.assertIn('importlib.util.find_spec("torch")', text)
        self.assertIn("Live QUIC handoff generation requires torch", text)
        self.assertIn("--system-site-packages true", text)
        self.assertIn("--require-handoff false", text)
        self.assertIn("python-quic-trainer-distributed-plan.json", text)
        self.assertIn("python-quic-trainer-runtime-smoke.json", text)
        self.assertIn('"read_mode": "direct-tensors"', text)
        self.assertIn('"pipeline_training_loop_required": true', text)
        self.assertIn("python-quic-trainer-distributed-runtime-smoke.json", text)
        self.assertIn("python-quic-trainer-loop-replay.json", text)
        self.assertIn("python-quic-trainer-readiness-bundle.json", text)
        self.assertIn("python-quic-trainer-handoff-acceptance.json", text)
        self.assertIn("python-quic-trainer-network-preflight.json", text)
        self.assertIn("cxnfs.trainer-network-preflight.v1", text)
        self.assertIn("python-quic-batch-direct-read.json", text)
        self.assertIn("python-quic-batch-direct-tensor-read.json", text)
        self.assertIn("cxnfs.quic-batch-direct-tensor-read.v1", text)
        self.assertIn("--batch-direct-read-report \"$batch_direct_read\"", text)
        self.assertIn("--batch-direct-tensor-read-report \"$batch_direct_tensor_read\"", text)
        self.assertIn("--require-low-level-direct-read-proofs", text)
        self.assertIn("--require-low-level-direct-write-proofs", text)
        self.assertIn('"low_level_direct_read_proofs_complete": true', text)
        self.assertIn('"batch_direct_read_proof_complete": true', text)
        self.assertIn('"batch_direct_tensor_read_proof_complete": true', text)
        self.assertIn('"batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256"', text)
        self.assertIn('"low_level_direct_write_proofs_complete": true', text)
        self.assertIn('"write_probe_proof_complete": true', text)
        self.assertIn('"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"', text)
        self.assertIn('"direct_quic_memory_path_verified": true', text)
        self.assertIn("cxnfs.trainer-sustained-read-profile.v1", text)
        self.assertIn('"sustained_read_profile_complete": true', text)
        self.assertIn('"quic_read_transport_complete": true', text)
        self.assertIn('"quic_read_transport_stream_eof_verified": true', text)
        self.assertIn('"quic_read_transport_all_reports_expected_cipher_suite": true', text)
        self.assertIn('"TLS_AES_128_GCM_SHA256"', text)
        self.assertIn('"quic_read_transport_read_stream_eof_report_count": 4', text)
        self.assertIn('"read_stream_completion_verified": true', text)
        self.assertIn('"read_stream_eof_verified": true', text)
        self.assertIn("run_trainer_distributed_plan_cli", text)
        self.assertIn("cxnfs.trainer-runtime-binding.v1", text)
        self.assertIn("cxnfs.trainer-readiness-bundle.v1", text)
        self.assertIn("cxnfs.trainer-handoff-acceptance.v1", text)
        self.assertIn('"trainer_loop_contract_format": "cxnfs.trainer-loop-contract.v1"', text)
        self.assertIn('"credential_prompt_required_for_remote": true', text)
        self.assertIn('"opens_network_connections_during_preflight": false', text)
        self.assertIn('"consumes_batches_during_preflight": false', text)
        self.assertIn('"training_pipeline_object": "CxnfsTrainingPipeline"', text)
        self.assertIn('"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"', text)
        self.assertIn(
            '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"',
            text,
        )
        self.assertIn('"pipeline_loop_object": "CxnfsTrainerPipelineLoop"', text)
        self.assertIn('"python_byte_batch_avoidance_supported": true', text)
        self.assertIn('"per_source_client_default": true', text)

    def test_release_check_exercises_runtime_binding_smoke_cli(self):
        text = RELEASE_CHECK.read_text(encoding="utf-8")

        self.assertIn("run_trainer_runtime_smoke_cli", text)
        self.assertIn("run_trainer_network_preflight_cli", text)
        self.assertIn("run_trainer_distributed_runtime_smoke_cli", text)
        self.assertIn("run_trainer_acceptance_fixture_cli", text)
        self.assertIn("python-trainer-runtime-smoke.json", text)
        self.assertIn("python-trainer-distributed-runtime-smoke.json", text)
        self.assertIn("python-trainer-readiness-bundle.json", text)
        self.assertIn("python-trainer-acceptance-fixture.json", text)
        self.assertIn("cxnfs.trainer-runtime-smoke.v1", text)
        self.assertIn("cxnfs.trainer-distributed-runtime-smoke.v1", text)
        self.assertIn("cxnfs.trainer-readiness-bundle.v1", text)
        self.assertIn("cxnfs.trainer-acceptance-fixture.v1", text)
        self.assertIn("cxnfs.trainer-loop-contract.v1", text)
        self.assertIn("trainer-loop-contract.json", text)
        self.assertIn('"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"', text)
        self.assertIn(
            '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"',
            text,
        )
        self.assertIn('"pipeline_loop_object": "CxnfsTrainerPipelineLoop"', text)
        self.assertIn('"python_byte_batch_avoidance_supported": true', text)
        self.assertIn("--require-pipeline", text)
        self.assertIn("--require-direct-tensors", text)
        self.assertIn("--require-torch-compile", text)
        self.assertIn('"pipeline_contract_present": true', text)
        self.assertIn("cxnfs.trainer-distributed-runtime-rank-states.v1", text)
        self.assertIn("--write-rank-states-dir", text)
        self.assertIn("run_trainer_readiness_bundle_cli", text)
        self.assertIn("run_trainer_loop_contract_check_cli", text)
        self.assertIn("--output-dir", text)
        self.assertIn("CxnfsTrainerRuntimeBinding.open_training_loop", text)
        self.assertIn('require_file_not_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--password"', text)
        self.assertIn(
            'require_file_not_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--password"',
            text,
        )
        self.assertIn(
            'require_file_not_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--password"',
            text,
        )
        self.assertIn(
            'require_file_not_contains "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" "--password"',
            text,
        )
        self.assertIn(
            'require_file_not_contains "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" "--password"',
            text,
        )
        self.assertIn(
            'require_file_not_contains "$REPORT_DIR/python-trainer-network-preflight-help.txt" "--password"',
            text,
        )


if __name__ == "__main__":
    unittest.main()
