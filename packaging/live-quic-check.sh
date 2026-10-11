#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-python3}"
MVN_BIN="${MVN_BIN:-mvn}"
REPORT_DIR="${CXNFS_QUIC_REPORT_DIR:-$ROOT_DIR/target/cxnfs-live-quic-check}"
TEST_TARGET="python.tests.test_quic_java_integration.PythonJavaQuicIntegrationTest"
BOOTSTRAP_VENV=""
SYSTEM_SITE_PACKAGES=0
SKIP_MAVEN=0
DRY_RUN=0
GENERATE_HANDOFF=1
REQUIRE_HANDOFF=0

usage() {
	cat <<'EOF'
usage: packaging/live-quic-check.sh [options]

Runs the live Python-to-Java QUIC integration gate with skipped prerequisites
promoted to failures. By default it expects the selected Python interpreter to
already import aioquic.

Options:
  --report-dir <path>              output directory for published integration reports
  --python <path>                  Python interpreter to use
  --bootstrap-venv <path>          create/use a venv and install aioquic
  --system-site-packages true|false
                                   expose host site-packages inside the bootstrap venv
  --test <unittest-target>         test class or method to run
  --skip-maven true|false          skip Maven package/classpath preparation
  --generate-handoff true|false    generate workload/trainer handoff artifacts
                                   when the full live report set is present
  --require-handoff true|false     fail when handoff artifacts cannot be generated
  --dry-run true|false             print commands without running them
  --help                           show this help

Environment:
  CXNFS_PYTHONPATH_EXTRA            extra Python import roots, appended after python/src
  CXNFS_QUIC_REPORT_DIR             default report directory
  MVN_BIN                           Maven executable
  PYTHON_BIN                        default Python interpreter
EOF
}

bool_arg() {
	case "$1" in
		true|1|yes) printf '1' ;;
		false|0|no) printf '0' ;;
		*)
			printf 'expected boolean true or false, got: %s\n' "$1" >&2
			exit 2
			;;
	esac
}

need_value() {
	if [ "$#" -lt 2 ] || [[ "$2" == --* ]]; then
		printf '%s requires a value\n' "$1" >&2
		usage >&2
		exit 2
	fi
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--report-dir)
			need_value "$@"
			REPORT_DIR="$2"
			shift 2
			;;
		--python)
			need_value "$@"
			PYTHON_BIN="$2"
			shift 2
			;;
		--bootstrap-venv)
			need_value "$@"
			BOOTSTRAP_VENV="$2"
			shift 2
			;;
		--system-site-packages)
			need_value "$@"
			SYSTEM_SITE_PACKAGES="$(bool_arg "$2")"
			shift 2
			;;
		--test)
			need_value "$@"
			TEST_TARGET="$2"
			shift 2
			;;
		--skip-maven)
			need_value "$@"
			SKIP_MAVEN="$(bool_arg "$2")"
			shift 2
			;;
		--generate-handoff)
			need_value "$@"
			GENERATE_HANDOFF="$(bool_arg "$2")"
			shift 2
			;;
		--require-handoff)
			need_value "$@"
			REQUIRE_HANDOFF="$(bool_arg "$2")"
			shift 2
			;;
		--dry-run)
			need_value "$@"
			DRY_RUN="$(bool_arg "$2")"
			shift 2
			;;
		--help|-h)
			usage
			exit 0
			;;
		*)
			printf 'unknown option: %s\n' "$1" >&2
			usage >&2
			exit 2
			;;
	esac
done

run() {
	printf '+'
	for arg in "$@"; do
		printf ' %q' "$arg"
	done
	printf '\n'
	if [ "$DRY_RUN" = "0" ]; then
		"$@"
	fi
}

run_to_file() {
	local output_file="$1"
	shift
	printf '+'
	for arg in "$@"; do
		printf ' %q' "$arg"
	done
	printf ' > %q\n' "$output_file"
	if [ "$DRY_RUN" = "0" ]; then
		"$@" >"$output_file"
	fi
}

require_file_contains() {
	local input_file="$1"
	local expected="$2"
	printf '+ require %q contains %q\n' "$input_file" "$expected"
	if [ "$DRY_RUN" = "0" ]; then
		if ! grep -F -- "$expected" "$input_file" >/dev/null; then
			printf '%s missing expected text: %s\n' "$input_file" "$expected" >&2
			exit 1
		fi
	fi
}

generate_handoff_artifacts() {
	if [ "$GENERATE_HANDOFF" != "1" ]; then
		return
	fi

	local contract_report="$REPORT_DIR/python-quic-trainer-contract.json"
	local workload_report="$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json"
	local matrix_report="$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json"
	local pipeline_report="$REPORT_DIR/python-quic-trainer-pipeline-smoke.json"
	local workload_decision="$REPORT_DIR/python-quic-workload-decision.json"
	local decision_dir="$REPORT_DIR/quic-workload-decision"
	local blockfs_handoff="$decision_dir/blockfs-handoff.json"
	local runtime_smoke="$REPORT_DIR/python-quic-trainer-runtime-smoke.json"
	local distributed_runtime_smoke="$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json"
	local loop_replay="$REPORT_DIR/python-quic-trainer-loop-replay.json"
	local readiness_bundle="$REPORT_DIR/python-quic-trainer-readiness-bundle.json"
	local handoff_acceptance="$REPORT_DIR/python-quic-trainer-handoff-acceptance.json"
	local trainer_network_preflight="$REPORT_DIR/python-quic-trainer-network-preflight.json"
	local batch_direct_read="$REPORT_DIR/python-quic-batch-direct-read.json"
	local batch_direct_tensor_read="$REPORT_DIR/python-quic-batch-direct-tensor-read.json"
	local trainer_handoff="$REPORT_DIR/python-quic-trainer-handoff.json"
	local trainer_preflight="$REPORT_DIR/python-quic-trainer-preflight.json"
	local trainer_distributed_plan="$REPORT_DIR/python-quic-trainer-distributed-plan.json"
	local handoff_surface_check="$REPORT_DIR/python-quic-trainer-handoff-surface-check.json"
	local disk_evidence="$REPORT_DIR/workload-evidence/disk.report"
	local cache_evidence="$REPORT_DIR/workload-evidence/cache.json"
	local cpu_evidence="$REPORT_DIR/workload-evidence/cpu.report"
	local -a missing_reports=()

	if [ "$DRY_RUN" = "0" ]; then
		for required_report in "$contract_report" "$workload_report" "$matrix_report" "$pipeline_report" "$runtime_smoke" "$distributed_runtime_smoke" "$loop_replay" "$readiness_bundle" "$handoff_acceptance" "$trainer_network_preflight" "$batch_direct_read" "$batch_direct_tensor_read"; do
			if [ ! -f "$required_report" ]; then
				missing_reports+=("$required_report")
			fi
		done
	fi

	if [ "${#missing_reports[@]}" -gt 0 ]; then
		printf 'live QUIC handoff generation skipped; missing required report(s):'
		printf ' %s' "${missing_reports[@]}"
		printf '\n'
		if [ "$REQUIRE_HANDOFF" = "1" ]; then
			exit 1
		fi
		return
	fi

	printf '+ generate-live-quic-handoff-artifacts %q\n' "$REPORT_DIR"
	require_file_contains "$batch_direct_read" '"format": "cxnfs.quic-batch-direct-read.v1"'
	require_file_contains "$batch_direct_read" '"ordered_batch_read_verified": true'
	require_file_contains "$batch_direct_read" '"direct_buffer_read_operations": 2'
	require_file_contains "$batch_direct_read" '"frame_sink_used_for_all_direct_destination_reads": true'
	require_file_contains "$batch_direct_read" '"direct_destination_frame_sink_cord_fanout_verified": true'
	require_file_contains "$batch_direct_read" '"read_stream_completion_verified": true'
	require_file_contains "$batch_direct_read" '"read_stream_eof_verified": true'
	require_file_contains "$batch_direct_tensor_read" '"format": "cxnfs.quic-batch-direct-tensor-read.v1"'
	require_file_contains "$batch_direct_tensor_read" '"tensor_target": "pytorch-cpu"'
	require_file_contains "$batch_direct_tensor_read" '"ordered_tensor_read_verified": true'
	require_file_contains "$batch_direct_tensor_read" '"direct_tensor_read_operations": 2'
	require_file_contains "$batch_direct_tensor_read" '"network_fills_tensor_destination": true'
	require_file_contains "$batch_direct_tensor_read" '"frame_sink_used_for_all_direct_destination_reads": true'
	require_file_contains "$batch_direct_tensor_read" '"direct_destination_frame_sink_cord_fanout_verified": true'
	require_file_contains "$batch_direct_tensor_read" '"read_stream_completion_verified": true'
	require_file_contains "$batch_direct_tensor_read" '"read_stream_eof_verified": true'
	require_file_contains "$pipeline_report" '"format": "cxnfs.trainer-pipeline-smoke.v1"'
	require_file_contains "$pipeline_report" '"passed": true'
	require_file_contains "$pipeline_report" '"transport": "cxnfs-quic"'
	require_file_contains "$pipeline_report" '"bulk_transport": "quic"'
	require_file_contains "$pipeline_report" '"training_pipeline_object": "CxnfsTrainingPipeline"'
	require_file_contains "$pipeline_report" '"training_pipeline_context": "CxnfsTrainingPipeline.prefetch_trainer_stream"'
	require_file_contains "$pipeline_report" '"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"'
	require_file_contains "$pipeline_report" '"pipeline_prefetch_stream_used": true'
	require_file_contains "$pipeline_report" '"pipeline_prefetch_batches": 2'
	require_file_contains "$pipeline_report" '"pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch"'
	require_file_contains "$pipeline_report" '"format": "cxnfs.trainer-pipeline-network-path.v1"'
	require_file_contains "$pipeline_report" '"pipeline_network_path_complete": true'
	require_file_contains "$pipeline_report" '"network_fanout_configured": true'
	require_file_contains "$pipeline_report" '"observed_parallel_direct_path": true'
	require_file_contains "$pipeline_report" '"quic_complete": true'
	require_file_contains "$pipeline_report" '"pipeline_python_byte_batch_avoided": true'
	require_file_contains "$pipeline_report" '"format": "cxnfs.trainer-pipeline-write-probe.v1"'
	require_file_contains "$pipeline_report" '"verified_read_back": true'
	require_file_contains "$pipeline_report" '"format": "cxnfs.quic-bulk-write-transport-evidence.v1"'
	require_file_contains "$pipeline_report" '"cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$pipeline_report" '"write_ack_frame_identity_verified": true'
	require_file_contains "$pipeline_report" '"write_ack_stream_completion_verified": true'
	require_file_contains "$pipeline_report" '"write_ack_stream_eof_verified": true'
	require_file_contains "$runtime_smoke" '"format": "cxnfs.trainer-runtime-smoke.v1"'
	require_file_contains "$runtime_smoke" '"passed": true'
	require_file_contains "$runtime_smoke" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
	require_file_contains "$runtime_smoke" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
	require_file_contains "$runtime_smoke" '"read_mode": "direct-tensors"'
	require_file_contains "$runtime_smoke" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
	require_file_contains "$runtime_smoke" '"pipeline_training_loop_required": true'
	require_file_contains "$runtime_smoke" '"pipeline_training_loop_verified": true'
	require_file_contains "$runtime_smoke" '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"'
	require_file_contains "$runtime_smoke" '"pipeline_training_loop_context_used": true'
	require_file_contains "$runtime_smoke" '"pipeline_training_loop_object": "CxnfsTrainerPipelineLoop"'
	require_file_contains "$runtime_smoke" '"pipeline_network_path_verified": true'
	require_file_contains "$runtime_smoke" '"format": "cxnfs.trainer-pipeline-network-path.v1"'
	require_file_contains "$runtime_smoke" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
	require_file_contains "$runtime_smoke" '"remote_client_required": true'
	require_file_contains "$runtime_smoke" '"stream_opened": true'
	require_file_contains "$runtime_smoke" '"stream_kwargs_accepted": true'
	require_file_contains "$runtime_smoke" '"transport": "cxnfs-quic"'
	require_file_contains "$runtime_smoke" '"bulk_transport": "quic"'
	require_file_contains "$runtime_smoke" '"raw_password_sent_over_control": false'
	require_file_contains "$runtime_smoke" '"trainer_batch_iterator": "native"'
	require_file_contains "$runtime_smoke" '"data_loader_workers": 0'
	require_file_contains "$runtime_smoke" '"worker_partition_complete": true'
	require_file_contains "$runtime_smoke" '"remote_client_warmup_complete": true'
	require_file_contains "$runtime_smoke" '"remote_client_warmup_source_count": 2'
	require_file_contains "$runtime_smoke" '"remote_client_warmup_warmed_source_count": 2'
	require_file_contains "$runtime_smoke" '"remote_client_topology_complete": true'
	require_file_contains "$runtime_smoke" '"remote_client_per_source_complete": true'
	require_file_contains "$runtime_smoke" '"remote_client_lifecycle_closed": true'
	require_file_contains "$distributed_runtime_smoke" '"format": "cxnfs.trainer-distributed-runtime-smoke.v1"'
	require_file_contains "$distributed_runtime_smoke" '"passed": true'
	require_file_contains "$distributed_runtime_smoke" '"runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1"'
	require_file_contains "$distributed_runtime_smoke" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
	require_file_contains "$distributed_runtime_smoke" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
	require_file_contains "$distributed_runtime_smoke" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
	require_file_contains "$distributed_runtime_smoke" '"remote_client_required": true'
	require_file_contains "$distributed_runtime_smoke" '"world_size": 2'
	require_file_contains "$distributed_runtime_smoke" '"rank_count": 2'
	require_file_contains "$distributed_runtime_smoke" '"all_rank_streams_opened": true'
	require_file_contains "$distributed_runtime_smoke" '"all_rank_state_matches_final_cursor": true'
	require_file_contains "$distributed_runtime_smoke" '"all_rank_training_shards_correct": true'
	require_file_contains "$distributed_runtime_smoke" '"no_cross_rank_overlap": true'
	require_file_contains "$distributed_runtime_smoke" '"all_open_clients_closed": true'
	require_file_contains "$distributed_runtime_smoke" '"format": "cxnfs.trainer-distributed-runtime-rank-states.v1"'
	require_file_contains "$distributed_runtime_smoke" '"rank_state_file_count": 2'
	require_file_contains "$distributed_runtime_smoke" '"all_rank_state_files_written": true'
	require_file_contains "$distributed_runtime_smoke" '"transport": "cxnfs-quic"'
	require_file_contains "$distributed_runtime_smoke" '"bulk_transport": "quic"'
	require_file_contains "$distributed_runtime_smoke" '"raw_password_sent_over_control": false'
	require_file_contains "$loop_replay" '"format": "cxnfs.trainer-loop-replay.v1"'
	require_file_contains "$loop_replay" '"passed": true'
	require_file_contains "$loop_replay" '"handoff_format": "cxnfs.trainer-handoff.v1"'
	require_file_contains "$loop_replay" '"plan_format": "cxnfs.trainer-integration-plan.v1"'
	require_file_contains "$loop_replay" '"remote_client_required": true'
	require_file_contains "$loop_replay" '"transport": "cxnfs-quic"'
	require_file_contains "$loop_replay" '"bulk_transport": "quic"'
	require_file_contains "$loop_replay" '"training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop"'
	require_file_contains "$loop_replay" '"training_loop_context_used": true'
	require_file_contains "$loop_replay" '"weighted_source_coverage_complete": true'
	require_file_contains "$loop_replay" '"resume_replayed": true'
	require_file_contains "$loop_replay" '"resume_continuity_verified": true'
	require_file_contains "$loop_replay" '"unrecovered_read_failures": 0'
	require_file_contains "$readiness_bundle" '"format": "cxnfs.trainer-readiness-bundle.v1"'
	require_file_contains "$readiness_bundle" '"passed": true'
	require_file_contains "$readiness_bundle" '"ready_for_trainer_wiring": true'
	require_file_contains "$readiness_bundle" '"ready_for_remote_quic": true'
	require_file_contains "$readiness_bundle" '"requires_low_level_direct_read_proofs": true'
	require_file_contains "$readiness_bundle" '"low_level_direct_read_proofs_complete": true'
	require_file_contains "$readiness_bundle" '"batch_direct_read_proof_complete": true'
	require_file_contains "$readiness_bundle" '"batch_direct_tensor_read_proof_complete": true'
	require_file_contains "$readiness_bundle" '"batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$readiness_bundle" '"batch_direct_tensor_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$readiness_bundle" '"requires_low_level_direct_write_proofs": true'
	require_file_contains "$readiness_bundle" '"low_level_direct_write_proofs_complete": true'
	require_file_contains "$readiness_bundle" '"write_probe_proof_complete": true'
	require_file_contains "$readiness_bundle" '"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$readiness_bundle" '"write_probe_quic_transport_complete": true'
	require_file_contains "$readiness_bundle" '"direct_quic_memory_path_verified": true'
	require_file_contains "$readiness_bundle" '"remote_client_required": true'
	require_file_contains "$readiness_bundle" '"requires_credentials": true'
	require_file_contains "$readiness_bundle" '"direct_python_streaming_verified": true'
	require_file_contains "$readiness_bundle" '"runtime_training_loop_verified": true'
	require_file_contains "$readiness_bundle" '"pipeline_training_loop_verified": true'
	require_file_contains "$readiness_bundle" '"pipeline_network_path_verified": true'
	require_file_contains "$readiness_bundle" '"distributed_runtime_verified": true'
	require_file_contains "$readiness_bundle" '"distributed_rank_states_written": true'
	require_file_contains "$readiness_bundle" '"loop_resume_verified": true'
	require_file_contains "$readiness_bundle" '"raw_password_sent_over_control": false'
	require_file_contains "$readiness_bundle" '"integration_smoke_format": "cxnfs.trainer-integration-smoke.v1"'
	require_file_contains "$readiness_bundle" '"runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1"'
	require_file_contains "$readiness_bundle" '"distributed_runtime_smoke_format": "cxnfs.trainer-distributed-runtime-smoke.v1"'
	require_file_contains "$readiness_bundle" '"loop_replay_format": "cxnfs.trainer-loop-replay.v1"'
	require_file_contains "$readiness_bundle" '"artifact_count": 5'
	require_file_contains "$readiness_bundle" '"format": "cxnfs.trainer-distributed-runtime-rank-states.v1"'
	require_file_contains "$readiness_bundle" '"rank_state_file_count": 2'
	require_file_contains "$readiness_bundle" '"all_rank_state_files_written": true'
	require_file_contains "$handoff_acceptance" '"format": "cxnfs.trainer-handoff-acceptance.v1"'
	require_file_contains "$handoff_acceptance" '"passed": true'
	require_file_contains "$handoff_acceptance" '"handoff_format": "cxnfs.trainer-handoff.v1"'
	require_file_contains "$handoff_acceptance" '"readiness_bundle_format": "cxnfs.trainer-readiness-bundle.v1"'
	require_file_contains "$handoff_acceptance" '"ready_for_trainer_wiring": true'
	require_file_contains "$handoff_acceptance" '"ready_for_remote_quic": true'
	require_file_contains "$handoff_acceptance" '"requires_remote_quic": true'
	require_file_contains "$handoff_acceptance" '"direct_quic_memory_path_verified": true'
	require_file_contains "$handoff_acceptance" '"direct_python_streaming_verified": true'
	require_file_contains "$handoff_acceptance" '"runtime_training_loop_verified": true'
	require_file_contains "$handoff_acceptance" '"pipeline_training_loop_verified": true'
	require_file_contains "$handoff_acceptance" '"pipeline_network_path_verified": true'
	require_file_contains "$handoff_acceptance" '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"'
	require_file_contains "$handoff_acceptance" '"pipeline_training_loop_object": "CxnfsTrainerPipelineLoop"'
	require_file_contains "$handoff_acceptance" '"distributed_runtime_verified": true'
	require_file_contains "$handoff_acceptance" '"loop_resume_verified": true'
	require_file_contains "$handoff_acceptance" '"cross_rank_overlap_free": true'
	require_file_contains "$handoff_acceptance" '"rank_states_written": true'
	require_file_contains "$handoff_acceptance" '"remote_client_required": true'
	require_file_contains "$handoff_acceptance" '"requires_credentials": true'
	require_file_contains "$handoff_acceptance" '"raw_password_sent_over_control": false'
	require_file_contains "$handoff_acceptance" '"runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file"'
	require_file_contains "$handoff_acceptance" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
	require_file_contains "$handoff_acceptance" '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"'
	require_file_contains "$handoff_acceptance" '"pipeline_loop_object": "CxnfsTrainerPipelineLoop"'
	require_file_contains "$handoff_acceptance" '"trainer_loop_contract_format": "cxnfs.trainer-loop-contract.v1"'
	require_file_contains "$handoff_acceptance" '"format": "cxnfs.trainer-loop-contract.v1"'
	require_file_contains "$handoff_acceptance" '"credential_prompt_required_for_remote": true'
	require_file_contains "$handoff_acceptance" '"opens_network_connections_during_preflight": false'
	require_file_contains "$handoff_acceptance" '"consumes_batches_during_preflight": false'
	require_file_contains "$handoff_acceptance" '"training_pipeline_object": "CxnfsTrainingPipeline"'
	require_file_contains "$handoff_acceptance" '"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"'
	require_file_contains "$handoff_acceptance" '"python_byte_batch_avoidance_supported": true'
	require_file_contains "$handoff_acceptance" '"direct_python_streaming_verified": true'
	require_file_contains "$handoff_acceptance" '"runtime_training_loop_verified": true'
	require_file_contains "$handoff_acceptance" '"rank_states_written": true'
	require_file_contains "$handoff_acceptance" '"loop_resume_verified": true'
	require_file_contains "$handoff_acceptance" '"per_source_client_default": true'
	require_file_contains "$handoff_acceptance" '"password_cli_argument": false'
	require_file_contains "$trainer_network_preflight" '"format": "cxnfs.trainer-network-preflight.v1"'
	require_file_contains "$trainer_network_preflight" '"passed": true'
	require_file_contains "$trainer_network_preflight" '"handoff_format": "cxnfs.trainer-handoff.v1"'
	require_file_contains "$trainer_network_preflight" '"preflight_format": "cxnfs.trainer-integration-preflight.v1"'
	require_file_contains "$trainer_network_preflight" '"ready_for_trainer_wiring": true'
	require_file_contains "$trainer_network_preflight" '"ready_for_remote_quic": true'
	require_file_contains "$trainer_network_preflight" '"remote_client_required": true'
	require_file_contains "$trainer_network_preflight" '"requires_credentials": true'
	require_file_contains "$trainer_network_preflight" '"raw_password_sent_over_control": false'
	require_file_contains "$trainer_network_preflight" '"safe_to_run_before_trainer_workers": true'
	require_file_contains "$trainer_network_preflight" '"opens_network_connections": true'
	require_file_contains "$trainer_network_preflight" '"consumes_batches": false'
	require_file_contains "$trainer_network_preflight" '"authenticated": true'
	require_file_contains "$trainer_network_preflight" '"transport": "cxnfs-quic"'
	require_file_contains "$trainer_network_preflight" '"bulk_transport": "quic"'
	require_file_contains "$trainer_network_preflight" '"bulk_probe": "ok"'
	require_file_contains "$trainer_network_preflight" '"bulk_probe_bytes": 1'
	require_file_contains "$trainer_network_preflight" '"network_probe_is_handoff_aware": true'
	require_file_contains "$workload_report" '"sustained_read_profile": {'
	require_file_contains "$workload_report" '"format": "cxnfs.trainer-sustained-read-profile.v1"'
	require_file_contains "$workload_report" '"complete": true'
	require_file_contains "$workload_report" '"sustained_read_profile_complete": true'
	require_file_contains "$workload_report" '"requires_quic_transport_evidence": true'
	require_file_contains "$workload_report" '"run_count": 4'
	require_file_contains "$workload_report" '"total_batches": 8'
	require_file_contains "$workload_report" '"cord_sweep_count": 2'
	require_file_contains "$workload_report" '"all_runs_have_read_path_evidence": true'
	require_file_contains "$workload_report" '"direct_destination_reads_observed": true'
	require_file_contains "$workload_report" '"quic_read_transport_complete": true'
	require_file_contains "$workload_report" '"quic_read_transport_stream_eof_verified": true'
	require_file_contains "$workload_report" '"quic_read_transport_all_reports_expected_cipher_suite": true'
	require_file_contains "$workload_report" '"TLS_AES_128_GCM_SHA256"'
	require_file_contains "$workload_report" '"quic_read_transport_read_stream_eof_report_count": 4'
	require_file_contains "$workload_report" '"unrecovered_read_failures": 0'

	local -a decision_args=(
		env
		"PYTHONPATH=$PYTHON_RUN_PATH"
		"$PYTHON_BIN"
		-m cxnfs.workload_decision
		--input-workload-report "$workload_report"
		--input-matrix-report "$matrix_report"
		--output-dir "$decision_dir"
		--surface-contract "$contract_report"
		--surface-require-remote
		--surface-require-quic
		--surface-require-matrix
		--surface-min-sources 2
		--surface-min-validation-samples 6
		--surface-min-validation-bytes 786000
		--surface-min-workload-runs 4
		--surface-min-workload-samples 24
		--surface-min-workload-bytes 3145728
		--surface-min-cord-sweep 2
	)
	if [ "$DRY_RUN" = "1" ] || [ -f "$disk_evidence" ]; then
		decision_args+=(--server-disk-evidence "$disk_evidence")
	fi
	if [ "$DRY_RUN" = "1" ] || [ -f "$cache_evidence" ]; then
		decision_args+=(--server-cache-evidence "$cache_evidence")
	fi
	if [ "$DRY_RUN" = "1" ] || [ -f "$cpu_evidence" ]; then
		decision_args+=(--server-cpu-evidence "$cpu_evidence")
	fi

	run_to_file "$workload_decision" "${decision_args[@]}"
	require_file_contains "$workload_decision" '"format": "cxnfs.workload-decision.v1"'
	require_file_contains "$workload_decision" '"transport": "cxnfs-quic"'
	require_file_contains "$workload_decision" '"trainer_surface_check_passed": true'
	require_file_contains "$workload_decision" '"trainer_surface_matrix_passed": true'
	require_file_contains "$workload_decision" '"kernel_module_allowed": false'
	require_file_contains "$blockfs_handoff" '"format": "cxnfs.blockfs-handoff.v1"'
	require_file_contains "$blockfs_handoff" '"kernel_module_allowed": false'

	run_to_file "$trainer_handoff" \
		env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
		"from cxnfs.torch_dataset import run_trainer_handoff_cli; import sys; raise SystemExit(run_trainer_handoff_cli(sys.argv[1:]))" \
		"$contract_report" \
		--workload-report "$workload_report" \
		--matrix-report "$matrix_report" \
		--workload-decision "$workload_decision" \
		--blockfs-handoff "$blockfs_handoff" \
		--batch-direct-read-report "$batch_direct_read" \
		--batch-direct-tensor-read-report "$batch_direct_tensor_read" \
		--surface-check-json "$handoff_surface_check" \
		--require-low-level-direct-read-proofs \
		--require-low-level-direct-write-proofs \
		--require-remote \
		--require-quic \
		--require-workload \
		--require-matrix \
		--min-sources 2 \
		--min-validation-samples 6 \
		--min-validation-bytes 786000 \
		--min-workload-runs 4 \
		--min-workload-samples 24 \
		--min-workload-bytes 3145728 \
		--min-cord-sweep 2
	require_file_contains "$trainer_handoff" '"format": "cxnfs.trainer-handoff.v1"'
	require_file_contains "$trainer_handoff" '"ready_for_trainer_wiring": true'
	require_file_contains "$trainer_handoff" '"ready_for_remote_quic": true'
	require_file_contains "$trainer_handoff" '"ready_for_blockfs_candidate": false'
	require_file_contains "$trainer_handoff" '"python_client": "CxnfsWeightedTorchDataset"'
	require_file_contains "$trainer_handoff" '"stream_factory": "open_weighted_mds_trainer_stream"'
	require_file_contains "$trainer_handoff" '"remote_client_factory": "CxnfsTrainerIntegrationPlan.create_remote_client_factory"'
	require_file_contains "$trainer_handoff" '"raw_password_sent_over_control": false'
	require_file_contains "$trainer_handoff" '"format": "cxnfs.trainer-low-level-direct-read-proofs.v1"'
	require_file_contains "$trainer_handoff" '"batch_direct_read"'
	require_file_contains "$trainer_handoff" '"batch_direct_tensor_read"'
	require_file_contains "$trainer_handoff" '"complete": true'
	require_file_contains "$trainer_handoff" '"direct_buffer_read_operations": 2'
	require_file_contains "$trainer_handoff" '"direct_tensor_read_operations": 2'
	require_file_contains "$trainer_handoff" '"network_fills_tensor_destination": true'
	require_file_contains "$trainer_handoff" '"format": "cxnfs.trainer-low-level-direct-write-proofs.v1"'
	require_file_contains "$trainer_handoff" '"low_level_direct_write_proofs"'
	require_file_contains "$trainer_handoff" '"quic_write_transport_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$trainer_handoff" '"quic_write_transport_complete": true'
	require_file_contains "$trainer_handoff" '"write_ack_frame_identity_verified": true'
	require_file_contains "$trainer_handoff" '"quic_write_transport_all_reports_write_ack_frame_identity_verified": true'
	require_file_contains "$trainer_handoff" '"quic_write_transport_all_reports_write_ack_stream_completion_verified": true'
	require_file_contains "$trainer_handoff" '"quic_write_transport_all_reports_write_ack_stream_eof_verified": true'
	require_file_contains "$trainer_handoff" '"quic_write_transport_write_ack_verification_failures": 0'
	require_file_contains "$trainer_handoff" '"metadata_pending_observed": true'

	run_to_file "$trainer_preflight" \
		env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
		"from cxnfs.torch_dataset import run_trainer_preflight_cli; import sys; raise SystemExit(run_trainer_preflight_cli(sys.argv[1:]))" \
		"$trainer_handoff" \
		--require-remote-quic
	require_file_contains "$trainer_preflight" '"format": "cxnfs.trainer-integration-preflight.v1"'
	require_file_contains "$trainer_preflight" '"passed": true'
	require_file_contains "$trainer_preflight" '"ready_for_trainer_wiring": true'
	require_file_contains "$trainer_preflight" '"ready_for_remote_quic": true'
	require_file_contains "$trainer_preflight" '"remote_client_required": true'
	require_file_contains "$trainer_preflight" '"requires_credentials": true'
	require_file_contains "$trainer_preflight" '"opens_network_connections": false'
	require_file_contains "$trainer_preflight" '"consumes_batches": false'
	require_file_contains "$trainer_preflight" '"safe_to_run_before_trainer_workers": true'
	require_file_contains "$trainer_preflight" '"low_level_direct_read_proofs_complete": true'
	require_file_contains "$trainer_preflight" '"batch_direct_read_proof_complete": true'
	require_file_contains "$trainer_preflight" '"batch_direct_tensor_read_proof_complete": true'
	require_file_contains "$trainer_preflight" '"batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$trainer_preflight" '"batch_direct_tensor_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$trainer_preflight" '"low_level_direct_write_proofs_complete": true'
	require_file_contains "$trainer_preflight" '"write_probe_proof_complete": true'
	require_file_contains "$trainer_preflight" '"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$trainer_preflight" '"write_probe_quic_transport_complete": true'
	require_file_contains "$trainer_preflight" '"bulk_transport": "quic"'
	require_file_contains "$trainer_preflight" '"max_cords": 128'
	require_file_contains "$trainer_preflight" '"raw_password_sent_over_control": false'

	run_to_file "$trainer_distributed_plan" \
		env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
		"from cxnfs.torch_dataset import run_trainer_distributed_plan_cli; import sys; raise SystemExit(run_trainer_distributed_plan_cli(sys.argv[1:]))" \
		"$trainer_handoff" \
		--require-remote-quic \
		--world-size 2 \
		--rank 1
	require_file_contains "$trainer_distributed_plan" '"format": "cxnfs.trainer-distributed-plan.v1"'
	require_file_contains "$trainer_distributed_plan" '"passed": true'
	require_file_contains "$trainer_distributed_plan" '"ready_for_remote_quic": true'
	require_file_contains "$trainer_distributed_plan" '"low_level_direct_read_proofs_complete": true'
	require_file_contains "$trainer_distributed_plan" '"batch_direct_read_proof_complete": true'
	require_file_contains "$trainer_distributed_plan" '"batch_direct_tensor_read_proof_complete": true'
	require_file_contains "$trainer_distributed_plan" '"low_level_direct_write_proofs_complete": true'
	require_file_contains "$trainer_distributed_plan" '"write_probe_proof_complete": true'
	require_file_contains "$trainer_distributed_plan" '"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"'
	require_file_contains "$trainer_distributed_plan" '"remote_client_required": true'
	require_file_contains "$trainer_distributed_plan" '"requires_credentials": true'
	require_file_contains "$trainer_distributed_plan" '"opens_network_connections": false'
	require_file_contains "$trainer_distributed_plan" '"consumes_batches": false'
	require_file_contains "$trainer_distributed_plan" '"world_size": 2'
	require_file_contains "$trainer_distributed_plan" '"rank_count": 2'
	require_file_contains "$trainer_distributed_plan" '"selected_rank": 1'
	require_file_contains "$trainer_distributed_plan" '"training_shard": {'
	require_file_contains "$trainer_distributed_plan" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
	require_file_contains "$trainer_distributed_plan" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
	require_file_contains "$trainer_distributed_plan" '"raw_password_sent_over_control": false'
}

write_manifest() {
	local manifest_json="$REPORT_DIR/live-quic-check.json"
	if [ "$DRY_RUN" = "1" ]; then
		printf '+ write-live-quic-manifest %q\n' "$manifest_json"
		return
	fi

	env \
		PYTHONPATH="$PYTHON_RUN_PATH" \
		CXNFS_LIVE_QUIC_REPORT_DIR="$REPORT_DIR" \
		CXNFS_LIVE_QUIC_TEST_TARGET="$TEST_TARGET" \
		CXNFS_LIVE_QUIC_PYTHON_BIN="$PYTHON_BIN" \
		CXNFS_LIVE_QUIC_BOOTSTRAP_VENV="$BOOTSTRAP_VENV" \
		CXNFS_LIVE_QUIC_SYSTEM_SITE_PACKAGES="$SYSTEM_SITE_PACKAGES" \
		CXNFS_LIVE_QUIC_SKIP_MAVEN="$SKIP_MAVEN" \
		"$PYTHON_BIN" -m cxnfs.live_quic_manifest
}

check_handoff_python_prerequisites() {
	if [ "$DRY_RUN" = "1" ] || [ "$REQUIRE_HANDOFF" != "1" ]; then
		return
	fi
	if env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c 'import importlib.util, sys; sys.exit(0 if importlib.util.find_spec("torch") else 1)'; then
		return
	fi
	printf 'Live QUIC handoff generation requires torch in the selected Python interpreter.\n' >&2
	printf 'Re-run with --system-site-packages true when host Python has torch, install torch in the bootstrap venv, or use --require-handoff false for a transport-only proof.\n' >&2
	exit 1
}

if [ -n "$BOOTSTRAP_VENV" ]; then
	if [ "$SYSTEM_SITE_PACKAGES" = "1" ]; then
		run "$PYTHON_BIN" -m venv --system-site-packages "$BOOTSTRAP_VENV"
	else
		run "$PYTHON_BIN" -m venv "$BOOTSTRAP_VENV"
	fi
	PYTHON_BIN="$BOOTSTRAP_VENV/bin/python"
	run "$PYTHON_BIN" -m pip install "aioquic>=1.3.0"
fi

PYTHON_RUN_PATH="$ROOT_DIR/python/src"
if [ -n "${CXNFS_PYTHONPATH_EXTRA:-}" ]; then
	PYTHON_RUN_PATH="$PYTHON_RUN_PATH:$CXNFS_PYTHONPATH_EXTRA"
fi

if [ "$SKIP_MAVEN" != "1" ]; then
	run "$MVN_BIN" -q -DskipTests package
	run "$MVN_BIN" -q dependency:build-classpath "-Dmdep.outputFile=$ROOT_DIR/target/cxnfs-runtime-classpath.txt"
fi

if [ "$DRY_RUN" = "0" ]; then
	if ! env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c 'import importlib.util, sys; sys.exit(0 if importlib.util.find_spec("aioquic") else 1)'; then
		printf 'Python QUIC integration requires aioquic. Re-run with --bootstrap-venv <path> or install python[quic].\n' >&2
		exit 1
	fi
fi

check_handoff_python_prerequisites

run mkdir -p "$REPORT_DIR"
run env \
	CXNFS_REQUIRE_QUIC_INTEGRATION=1 \
	CXNFS_QUIC_INTEGRATION_REPORT_DIR="$REPORT_DIR" \
	PYTHONPATH="$PYTHON_RUN_PATH" \
	"$PYTHON_BIN" -m unittest -v "$TEST_TARGET"

generate_handoff_artifacts
write_manifest

if [ "$DRY_RUN" = "1" ]; then
	printf 'cxnfs live QUIC check dry-run complete\n'
else
	printf 'cxnfs live QUIC check passed\n'
fi
printf 'reports=%s\n' "$REPORT_DIR"
printf 'manifest=%s\n' "$REPORT_DIR/live-quic-check.json"
