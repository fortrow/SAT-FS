"""Manifest builder for the live Java/Python QUIC gate."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence


MANIFEST_FORMAT = "cxnfs.live-quic-check.v1"
READINESS_FORMAT = "cxnfs.live-quic-readiness.v1"
EXPECTED_CIPHER_SUITE = "TLS_AES_128_GCM_SHA256"
EXPECTED_HEALTH_CIPHER_SUITE = "tls_aes_128_gcm_sha256"
EXPECTED_TLS_VERSION = "TLSv1.3"
EXPECTED_PIPELINE_LOOP_CONTEXT = "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"
EXPECTED_PIPELINE_LOOP_OBJECT = "CxnfsTrainerPipelineLoop"
EXPECTED_STREAM_STATE_FORMAT = "cxnfs.trainer-stream-state.v1"
EXPECTED_SUSTAINED_READ_PROFILE_FORMAT = "cxnfs.trainer-sustained-read-profile.v1"

KNOWN_REPORT_PATHS = {
    "read_retry": "python-quic-read-retry.json",
    "write_retry": "python-quic-write-retry.json",
    "duplicate_write_retry": "python-quic-duplicate-write-retry.json",
    "bad_late_metadata": "python-quic-bad-late-metadata.json",
    "batch_direct_read": "python-quic-batch-direct-read.json",
    "batch_direct_tensor_read": "python-quic-batch-direct-tensor-read.json",
    "high_cord_read": "python-quic-high-cord-read.json",
    "high_cord_write": "python-quic-high-cord-write.json",
    "python_client_cli_quic_smoke": "python-client-cli-quic-smoke.json",
    "trainer_contract": "python-quic-trainer-contract.json",
    "trainer_stream_benchmark": "python-quic-trainer-stream-benchmark.json",
    "trainer_pipeline_smoke": "python-quic-trainer-pipeline-smoke.json",
    "trainer_workers": "python-quic-trainer-workers.json",
    "trainer_resume": "python-quic-trainer-resume.json",
    "trainer_surface_matrix": "python-quic-trainer-surface-matrix.json",
    "trainer_workload_benchmark": "python-quic-trainer-workload-benchmark.json",
    "trainer_contract_surface_matrix": "python-quic-trainer-contract-surface-matrix.json",
    "trainer_contract_workload_benchmark": "python-quic-trainer-contract-workload-benchmark.json",
    "workload_decision": "python-quic-workload-decision.json",
    "trainer_handoff": "python-quic-trainer-handoff.json",
    "trainer_preflight": "python-quic-trainer-preflight.json",
    "trainer_network_preflight": "python-quic-trainer-network-preflight.json",
    "trainer_distributed_plan": "python-quic-trainer-distributed-plan.json",
    "trainer_integration_smoke": "python-quic-trainer-integration-smoke.json",
    "trainer_runtime_smoke": "python-quic-trainer-runtime-smoke.json",
    "trainer_distributed_runtime_smoke": "python-quic-trainer-distributed-runtime-smoke.json",
    "trainer_loop_replay": "python-quic-trainer-loop-replay.json",
    "trainer_readiness_bundle": "python-quic-trainer-readiness-bundle.json",
    "trainer_handoff_acceptance": "python-quic-trainer-handoff-acceptance.json",
    "trainer_worker_loop_replay": "python-quic-trainer-worker-loop-replay.json",
    "blockfs_handoff": "quic-workload-decision/blockfs-handoff.json",
}

SUMMARY_KEYS = (
    "format",
    "transport",
    "bulk_transport",
    "tls_version",
    "cipher_suite",
    "passed",
    "ready_for_trainer_wiring",
    "ready_for_remote_quic",
    "requires_remote_quic",
    "ready_for_blockfs_candidate",
    "kernel_module_allowed",
    "requires_low_level_direct_read_proofs",
    "low_level_direct_read_proofs_complete",
    "batch_direct_read_proof_complete",
    "batch_direct_tensor_read_proof_complete",
    "batch_direct_read_cipher_suite",
    "batch_direct_tensor_read_cipher_suite",
    "requires_low_level_direct_write_proofs",
    "low_level_direct_write_proofs_complete",
    "write_probe_proof_complete",
    "write_probe_cipher_suite",
    "write_probe_quic_transport_complete",
    "write_probe_max_observed_write_cords",
    "direct_quic_memory_path_verified",
    "safe_to_run_before_trainer_workers",
    "opens_network_connections",
    "consumes_batches",
    "requires_credentials",
    "remote_client_required",
    "raw_password_sent_over_control",
    "read_mode",
    "rank",
    "rank_count",
    "selected_rank",
    "world_size",
    "runtime_binding_format",
    "runtime_binding_object",
    "trainer_loop_contract_format",
    "trainer_loop_contract",
    "source_count",
    "trainer_prefetch_enabled",
    "trainer_prefetch_batches",
    "training_pipeline_object",
    "training_pipeline_context",
    "pipeline_descriptor_stream",
    "pipeline_prefetch_stream_used",
    "pipeline_prefetch_batches",
    "pipeline_prefetch_result_object",
    "pipeline_network_path_format",
    "pipeline_network_path_complete",
    "pipeline_network_path_verified",
    "pipeline_network_read_workers",
    "pipeline_max_inflight_bytes",
    "pipeline_python_byte_batch_avoided",
    "training_loop_context",
    "training_loop_context_used",
    "training_loop_object",
    "training_loop_base_stream_object",
    "training_loop_prefetch_batches",
    "training_loop_prefetch_applied",
    "pipeline_training_loop_required",
    "pipeline_training_loop_verified",
    "pipeline_training_loop_context",
    "pipeline_training_loop_context_used",
    "pipeline_training_loop_object",
    "all_rank_streams_opened",
    "all_rank_stream_kwargs_accepted",
    "all_rank_state_matches_final_cursor",
    "all_rank_training_loop_context_used",
    "all_rank_training_shards_correct",
    "no_cross_rank_overlap",
    "all_open_clients_closed",
    "distributed_runtime_topology_complete",
    "observed_sources_complete",
    "rank_state_file_count",
    "all_rank_state_files_written",
    "direct_python_streaming_verified",
    "runtime_training_loop_verified",
    "distributed_runtime_verified",
    "distributed_rank_states_written",
    "rank_states_written",
    "loop_resume_verified",
    "cross_rank_overlap_free",
    "artifact_count",
    "integration_smoke_format",
    "runtime_smoke_format",
    "distributed_runtime_smoke_format",
    "loop_replay_format",
    "step_count",
    "batch_count",
    "sample_count",
    "gradient_accumulation_steps",
    "resume_replayed",
    "resume_continuity_verified",
    "weighted_source_coverage_complete",
    "final_emitted",
    "bulk_read_retries",
    "trainer_batch_iterator",
    "data_loader_workers",
    "samples_per_epoch",
    "repeat",
    "worker_partitioning_enabled",
    "worker_cursor_count",
    "worker_batch_read_evidence_observed",
    "worker_batch_read_evidence_count",
    "worker_sharding_enabled",
    "duplicate_source_sample_count",
    "cursor_format",
    "stream_state_format",
    "state_file_written",
    "state_file_replayed",
    "state_stream_state_matches_final_cursor",
    "first_final_emitted",
    "second_initial_emitted",
    "second_final_emitted",
    "read_path_reliability_complete",
    "unrecovered_read_failures",
    "sustained_read_profile_format",
    "sustained_read_profile_complete",
    "sustained_read_run_count",
    "sustained_read_total_batches",
    "sustained_read_min_run_batches",
    "sustained_read_total_samples",
    "sustained_read_total_bytes",
    "sustained_read_cord_sweep_count",
    "sustained_read_requires_quic_transport_evidence",
    "sustained_read_all_runs_have_read_path_evidence",
    "sustained_read_direct_destination_reads_observed",
    "sustained_read_quic_read_transport_complete",
    "sustained_read_quic_read_transport_stream_eof_verified",
    "sustained_read_quic_read_transport_expected_cipher_suite_count",
    "sustained_read_quic_read_transport_all_reports_expected_cipher_suite",
    "sustained_read_quic_read_transport_cipher_suite_counts",
    "sustained_read_quic_read_transport_read_partition_coverage_verified",
    "sustained_read_unrecovered_read_failures",
    "transport_frame_sink_observed",
    "frame_sink_used_for_all_direct_destination_reads",
    "direct_destination_frame_sink_cord_fanout_verified",
    "direct_destination_max_observed_cords",
    "direct_destination_parallel_frame_sink_read_operations",
    "last_accepted_initial_cords",
    "last_accepted_max_cords",
    "observed_cords_within_admission",
    "max_observed_write_cords",
    "verified_read_back",
    "write_frames",
    "write_acks",
    "read_max_observed_cords",
    "write_max_observed_cords",
    "read_back_verified",
    "batch_read_count",
    "batch_worker_count",
    "ordered_batch_read_verified",
    "bytes_read",
    "read_operations",
    "quic_read_transport_stream_eof_verified",
    "quic_read_transport_read_stream_eof_report_count",
    "quic_read_transport_expected_cipher_suite_count",
    "quic_read_transport_all_reports_expected_cipher_suite",
    "quic_read_transport_cipher_suite_counts",
    "quic_read_transport_read_partition_count",
    "quic_read_transport_read_partition_chunk_count",
    "quic_read_transport_read_partition_payload_bytes",
    "quic_read_transport_read_partition_coverage_verified",
    "direct_buffer_read_operations",
    "direct_tensor_read_operations",
    "direct_destination_read_operations",
    "payload_bytes_read",
    "tensor_read_count",
    "tensor_worker_count",
    "ordered_tensor_read_verified",
    "tensor_target",
    "tensor_dtype",
    "tensor_device",
    "python_byte_batch_avoided",
    "network_fills_tensor_destination",
    "device_copy_required",
    "raw_password_visible",
    "read_cipher_suite",
    "write_cipher_suite",
    "authenticated",
    "probe_path",
    "probe_entries",
    "bulk_probe",
    "bulk_probe_path",
    "bulk_probe_bytes",
    "read_length",
    "network_probe_is_handoff_aware",
    "direct_file_bytes_sent",
    "direct_write_max_observed_cords",
    "direct_write_cipher_suite",
    "direct_file_write_verified",
    "server_bulk_listener",
    "server_bulk_cipher_suite",
    "server_bulk_total_streams",
    "server_bulk_total_streams_observed",
)

SUMMARY_NESTS = (
    "bulk_transport_read_evidence",
    "bulk_transport_write_evidence",
    "read_path_evidence",
    "write_path_evidence",
    "tensor_memory_path",
    "worker_partition_evidence",
    "pipeline_network_path",
    "pipeline_training_loop_report",
    "report_artifacts",
    "rank_state_artifacts",
    "trainer_loop_contract",
)

SUSTAINED_READ_SUMMARY_KEYS = {
    "sustained_read_profile_format": "format",
    "sustained_read_profile_complete": "complete",
    "sustained_read_run_count": "run_count",
    "sustained_read_total_batches": "total_batches",
    "sustained_read_min_run_batches": "min_run_batches",
    "sustained_read_total_samples": "sample_count",
    "sustained_read_total_bytes": "byte_size",
    "sustained_read_cord_sweep_count": "cord_sweep_count",
    "sustained_read_requires_quic_transport_evidence": "requires_quic_transport_evidence",
    "sustained_read_all_runs_have_read_path_evidence": "all_runs_have_read_path_evidence",
    "sustained_read_direct_destination_reads_observed": "direct_destination_reads_observed",
    "sustained_read_quic_read_transport_complete": "quic_read_transport_complete",
    "sustained_read_quic_read_transport_stream_eof_verified": "quic_read_transport_stream_eof_verified",
    "sustained_read_quic_read_transport_expected_cipher_suite_count": (
        "quic_read_transport_expected_cipher_suite_count"
    ),
    "sustained_read_quic_read_transport_all_reports_expected_cipher_suite": (
        "quic_read_transport_all_reports_expected_cipher_suite"
    ),
    "sustained_read_quic_read_transport_cipher_suite_counts": "quic_read_transport_cipher_suite_counts",
    "sustained_read_quic_read_transport_read_partition_coverage_verified": (
        "quic_read_transport_read_partition_coverage_verified"
    ),
    "sustained_read_unrecovered_read_failures": "unrecovered_read_failures",
}


def lower_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "unknown"
    return str(value)


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return loaded if isinstance(loaded, dict) else None


def iter_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dicts(child)


def nested_summary_values(payload: Mapping[str, Any], key: str) -> list[Any]:
    values: list[Any] = []
    stream_report = payload.get("stream_report")
    if isinstance(stream_report, dict):
        evidence = stream_report.get("read_path_evidence")
        if isinstance(evidence, dict) and key in evidence:
            values.append(evidence[key])

    cases = payload.get("cases")
    if isinstance(cases, list):
        for case in cases:
            if not isinstance(case, dict):
                continue
            evidence = case.get("read_path_evidence")
            if isinstance(evidence, dict) and key in evidence:
                values.append(evidence[key])

    blockfs_gate = payload.get("blockfs_gate")
    if isinstance(blockfs_gate, dict):
        evidence = blockfs_gate.get("read_path_gate")
        if isinstance(evidence, dict) and key in evidence:
            values.append(evidence[key])

    write_probe = payload.get("write_probe")
    if isinstance(write_probe, dict):
        if key in write_probe:
            values.append(write_probe[key])
        write_path = write_probe.get("write_path_evidence")
        if isinstance(write_path, dict):
            if key in write_path:
                values.append(write_path[key])
            bulk_write = write_path.get("bulk_transport_write_evidence")
            if isinstance(bulk_write, dict) and key in bulk_write:
                values.append(bulk_write[key])
    return values


def summarize_nested_values(values: Sequence[Any]) -> Any:
    if all(isinstance(value, bool) for value in values):
        return all(values)
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
        return max(values)
    return values[0]


def sustained_read_profile_payload(payload: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not payload:
        return {}
    profile = payload.get("sustained_read_profile")
    if isinstance(profile, dict):
        return profile
    blockfs_gate = payload.get("blockfs_gate")
    if isinstance(blockfs_gate, dict):
        nested = blockfs_gate.get("sustained_read_profile")
        if isinstance(nested, dict):
            return nested
    return {}


def summary_value(payload: Mapping[str, Any] | None, key: str) -> Any:
    if not payload:
        return None
    sustained_key = SUSTAINED_READ_SUMMARY_KEYS.get(key)
    if sustained_key is not None:
        profile = sustained_read_profile_payload(payload)
        if sustained_key in profile:
            return profile[sustained_key]
    if key in payload:
        return payload[key]
    for nest in SUMMARY_NESTS:
        nested = payload.get(nest)
        if isinstance(nested, dict) and key in nested:
            return nested[key]
    values = nested_summary_values(payload, key)
    if values:
        return summarize_nested_values(values)
    return None


def summary_bool(payload: Mapping[str, Any] | None, key: str) -> bool:
    return bool(summary_value(payload, key))


def summary_int(payload: Mapping[str, Any] | None, key: str) -> int:
    value = summary_value(payload, key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def contains_key_value(payload: Mapping[str, Any] | None, key: str, expected: Any) -> bool:
    if not payload:
        return False
    return any(item.get(key) == expected for item in iter_dicts(payload))


def payload_uses_quic(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    transport = summary_value(payload, "transport")
    bulk_transport = summary_value(payload, "bulk_transport")
    return transport == "cxnfs-quic" and bulk_transport == "quic"


def payload_has_tls13_aes128(payload: Mapping[str, Any] | None) -> bool:
    return (
        contains_key_value(payload, "tls_version", EXPECTED_TLS_VERSION)
        and contains_key_value(payload, "cipher_suite", EXPECTED_CIPHER_SUITE)
    )


def read_path_proven(payload: Mapping[str, Any] | None) -> bool:
    return (
        read_path_frame_sink_observed(payload)
        and summary_bool(payload, "frame_sink_used_for_all_direct_destination_reads")
        and summary_bool(payload, "direct_destination_frame_sink_cord_fanout_verified")
        and summary_int(payload, "direct_destination_max_observed_cords") > 1
        and summary_int(payload, "direct_destination_parallel_frame_sink_read_operations") > 0
        and quic_read_stream_completion_proven(payload)
        and quic_read_stream_eof_proven(payload)
        and cords_within_admission(payload)
    )


def quic_read_stream_completion_proven(payload: Mapping[str, Any] | None) -> bool:
    return (
        summary_bool(payload, "quic_read_transport_stream_completion_verified")
        or summary_bool(payload, "read_stream_completion_verified")
    )


def quic_read_stream_eof_proven(payload: Mapping[str, Any] | None) -> bool:
    return (
        summary_bool(payload, "quic_read_transport_stream_eof_verified")
        or summary_bool(payload, "read_stream_eof_verified")
    )


def quic_read_expected_cipher_suite_proven(payload: Mapping[str, Any] | None) -> bool:
    expected_count = summary_int(payload, "quic_read_transport_expected_cipher_suite_count")
    evidence_count = summary_int(payload, "quic_read_transport_evidence_count")
    counts = summary_value(payload, "quic_read_transport_cipher_suite_counts")
    count_for_expected = 0
    if isinstance(counts, dict):
        count_for_expected = int(counts.get(EXPECTED_CIPHER_SUITE, 0) or 0)
    else:
        count_for_expected = expected_count
    return (
        summary_bool(payload, "quic_read_transport_all_reports_expected_cipher_suite")
        and expected_count > 0
        and count_for_expected == expected_count
        and (evidence_count == 0 or expected_count == evidence_count)
    )


def cords_within_admission(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    direct_value = summary_value(payload, "observed_cords_within_admission")
    if isinstance(direct_value, bool):
        return direct_value
    observed_values = [
        item.get("observed_cords_within_admission")
        for item in iter_dicts(payload)
        if "observed_cords_within_admission" in item
    ]
    if observed_values:
        return all(value is True for value in observed_values)
    accepted = summary_int(payload, "last_accepted_max_cords")
    observed = max(
        summary_int(payload, "direct_destination_max_observed_cords"),
        summary_int(payload, "max_observed_cords"),
    )
    if accepted > 0:
        return observed > 0 and observed <= accepted
    return 0 < observed <= 64


def read_path_frame_sink_observed(payload: Mapping[str, Any] | None) -> bool:
    return (
        summary_bool(payload, "transport_frame_sink_observed")
        or summary_int(payload, "direct_destination_parallel_frame_sink_read_operations") > 0
        or summary_int(payload, "direct_destination_parallel_frame_sink_frames") > 0
    )


def read_reliability_proven(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    reliability = payload.get("read_path_reliability")
    reliability_complete = (
        reliability.get("complete")
        if isinstance(reliability, dict) and "complete" in reliability
        else summary_value(payload, "read_path_reliability_complete")
    )
    return bool(reliability_complete) and summary_int(payload, "unrecovered_read_failures") == 0


def write_path_proven(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    write_path = payload.get("write_path_evidence")
    blockfs_gate = payload.get("blockfs_gate")
    blockfs_write_complete = (
        isinstance(blockfs_gate, dict) and bool(blockfs_gate.get("write_path_evidence_complete"))
    )
    return (
        (
            (isinstance(write_path, dict) and bool(write_path.get("complete")))
            or blockfs_write_complete
        )
        and contains_key_value(payload, "quic_write_transport_complete", True)
        and contains_key_value(payload, "write_ack_frame_identity_verified", True)
        and contains_key_value(payload, "quic_write_transport_all_reports_write_ack_frame_identity_verified", True)
        and contains_key_value(payload, "quic_write_transport_all_reports_write_ack_stream_completion_verified", True)
        and contains_key_value(payload, "quic_write_transport_all_reports_write_ack_stream_eof_verified", True)
    )


def sustained_read_profile_ready(workload: Mapping[str, Any] | None) -> bool:
    profile = sustained_read_profile_payload(workload)
    if not profile:
        return False
    return (
        profile.get("format") == EXPECTED_SUSTAINED_READ_PROFILE_FORMAT
        and profile.get("complete") is True
        and profile.get("requires_quic_transport_evidence") is True
        and mapping_int(profile, "run_count") >= 2
        and mapping_int(profile, "total_batches") >= 2
        and mapping_int(profile, "min_run_batches") > 0
        and mapping_int(profile, "sample_count") > 0
        and mapping_int(profile, "byte_size") > 0
        and mapping_int(profile, "cord_sweep_count") >= 2
        and profile.get("java_python_network_path_exercised") is True
        and profile.get("all_runs_have_read_path_evidence") is True
        and profile.get("direct_destination_reads_observed") is True
        and profile.get("frame_sink_used_for_all_direct_destination_reads") is True
        and profile.get("direct_destination_frame_sink_cord_fanout_verified") is True
        and mapping_int(profile, "direct_destination_max_observed_cords") > 1
        and mapping_int(profile, "direct_destination_parallel_frame_sink_read_operations") > 0
        and profile.get("quic_read_transport_complete") is True
        and profile.get("quic_read_transport_stream_eof_verified") is True
        and quic_read_expected_cipher_suite_proven(profile)
        and mapping_int(profile, "quic_read_transport_evidence_count") > 0
        and mapping_int(profile, "quic_read_transport_read_streams_created") > 0
        and mapping_int(profile, "quic_read_transport_read_stream_eof_report_count") > 0
        and profile.get("read_path_reliability_complete") is True
        and mapping_int(profile, "unrecovered_read_failures") == 0
    )


def child_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    child = payload.get(key)
    return child if isinstance(child, dict) else {}


def mapping_int(payload: Mapping[str, Any], key: str, default: int = 0) -> int:
    value = payload.get(key, default)
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def contract_ready(contract: Mapping[str, Any] | None) -> bool:
    if not contract:
        return False
    surface = child_mapping(contract, "python_surface") or contract
    network = child_mapping(contract, "network_contract") or contract
    validation = child_mapping(contract, "validation")
    return (
        contract.get("format") == "cxnfs.trainer-contract.v1"
        and surface.get("requires_mount") is False
        and surface.get("touches_trainer_code") is False
        and surface.get("supports_trainer_stream", True) is True
        and surface.get("supports_weighted_sources", True) is True
        and network.get("transport", "cxnfs-quic") == "cxnfs-quic"
        and network.get("bulk_transport", "quic") == "quic"
        and network.get("bulk_transport_available", True) is True
        and network.get("raw_password_sent_over_control") is False
        and mapping_int(network, "max_cords", 64) >= 64
        and validation.get("validated", True) is True
    )


def surface_matrix_ready(matrix: Mapping[str, Any] | None) -> bool:
    return (
        bool(matrix)
        and matrix.get("format") == "cxnfs.trainer-surface-matrix.v1"
        and matrix.get("passed") is True
        and payload_uses_quic(matrix)
        and read_path_proven(matrix)
        and read_reliability_proven(matrix)
    )


def workload_ready(workload: Mapping[str, Any] | None) -> bool:
    if not workload:
        return False
    blockfs_gate = workload.get("blockfs_gate")
    return (
        workload.get("format") == "cxnfs.trainer-workload-benchmark.v1"
        and workload.get("contract_applied") is True
        and payload_uses_quic(workload)
        and read_path_proven(workload)
        and read_reliability_proven(workload)
        and sustained_read_profile_ready(workload)
        and write_path_proven(workload)
        and isinstance(blockfs_gate, dict)
        and bool(blockfs_gate.get("read_path_evidence_complete"))
        and bool(blockfs_gate.get("sustained_read_profile_complete"))
        and bool(blockfs_gate.get("memory_path_evidence_complete"))
        and bool(blockfs_gate.get("write_path_evidence_complete"))
    )


def integration_smoke_ready(smoke: Mapping[str, Any] | None) -> bool:
    if not smoke:
        return False
    stream_report = smoke.get("stream_report")
    return (
        smoke.get("format") == "cxnfs.trainer-integration-smoke.v1"
        and smoke.get("passed") is True
        and smoke.get("remote_client_required") is True
        and smoke.get("stream_opened") is True
        and smoke.get("stream_kwargs_accepted") is True
        and smoke.get("stream_state_matches_final_cursor") is True
        and summary_value(smoke, "training_loop_context") == "CxnfsTrainerIntegrationPlan.open_training_loop"
        and summary_bool(smoke, "training_loop_context_used")
        and summary_value(smoke, "training_loop_object") == "CxnfsTrainerPrefetchStream"
        and summary_value(smoke, "training_loop_base_stream_object") == "CxnfsTrainerStream"
        and summary_int(smoke, "training_loop_prefetch_batches") > 0
        and summary_bool(smoke, "training_loop_prefetch_applied")
        and summary_value(smoke, "trainer_batch_iterator") in {"native", "trainer-dataloader"}
        and summary_bool(smoke, "remote_client_warmup_complete")
        and summary_bool(smoke, "remote_client_topology_complete")
        and summary_bool(smoke, "remote_client_per_source_complete")
        and smoke.get("remote_client_lifecycle_closed") is True
        and isinstance(stream_report, dict)
        and payload_uses_quic(stream_report)
        and read_path_proven(smoke)
        and read_reliability_proven(stream_report)
    )


def runtime_smoke_ready(smoke: Mapping[str, Any] | None) -> bool:
    if not smoke:
        return False
    stream_report = smoke.get("stream_report")
    after_close = child_mapping(stream_report if isinstance(stream_report, dict) else {}, "remote_client_topology_after_close")
    return (
        smoke.get("format") == "cxnfs.trainer-runtime-smoke.v1"
        and smoke.get("passed") is True
        and smoke.get("runtime_binding_format") == "cxnfs.trainer-runtime-binding.v1"
        and smoke.get("runtime_binding_object") == "CxnfsTrainerRuntimeBinding"
        and smoke.get("remote_client_required") is True
        and smoke.get("stream_opened") is True
        and smoke.get("stream_kwargs_accepted") is True
        and smoke.get("stream_state_matches_final_cursor") is True
        and smoke.get("raw_password_sent_over_control") is False
        and summary_value(smoke, "read_mode") == "direct-tensors"
        and summary_value(smoke, "training_loop_context") == "CxnfsTrainerRuntimeBinding.open_training_loop"
        and summary_bool(smoke, "training_loop_context_used")
        and summary_value(smoke, "training_loop_object") == "CxnfsTrainerPrefetchStream"
        and summary_value(smoke, "training_loop_base_stream_object") == "CxnfsTrainerStream"
        and summary_int(smoke, "training_loop_prefetch_batches") > 0
        and summary_bool(smoke, "training_loop_prefetch_applied")
        and summary_value(smoke, "trainer_batch_iterator") in {"native", "trainer-dataloader"}
        and summary_bool(smoke, "remote_client_warmup_complete")
        and summary_bool(smoke, "remote_client_topology_complete")
        and summary_bool(smoke, "remote_client_per_source_complete")
        and smoke.get("remote_client_lifecycle_closed") is True
        and isinstance(stream_report, dict)
        and payload_uses_quic(stream_report)
        and read_path_proven(smoke)
        and read_reliability_proven(stream_report)
        and after_close.get("all_open_clients_closed") is True
        and pipeline_training_loop_ready(smoke)
    )


def pipeline_training_loop_requirement_satisfied(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    if not summary_bool(payload, "pipeline_training_loop_required"):
        return True
    return summary_bool(payload, "pipeline_training_loop_verified")


def pipeline_network_path_ready(payload: Mapping[str, Any] | None, *, require_quic: bool) -> bool:
    if not payload:
        return False
    network_path = child_mapping(payload, "pipeline_network_path")
    return (
        network_path.get("format") == "cxnfs.trainer-pipeline-network-path.v1"
        and network_path.get("complete") is True
        and summary_int(network_path, "network_read_workers") > 1
        and summary_int(network_path, "max_cords") > 1
        and summary_int(network_path, "active_pipeline_prefetch_batches") > 0
        and summary_int(network_path, "max_inflight_bytes") > 0
        and summary_int(network_path, "batch_count") > 0
        and summary_int(network_path, "sample_count") > 0
        and summary_int(network_path, "byte_size") > 0
        and summary_int(network_path, "payload_bytes_read") > 0
        and summary_int(network_path, "direct_tensor_read_operations") > 0
        and summary_int(network_path, "direct_destination_read_operations") > 0
        and summary_int(network_path, "failed_direct_destination_read_operations") == 0
        and network_path.get("network_fanout_configured") is True
        and network_path.get("observed_parallel_direct_path") is True
        and network_path.get("tensor_memory_path_complete") is True
        and network_path.get("python_byte_batch_avoided") is True
        and (
            network_path.get("network_fills_tensor_destination") is True
            or network_path.get("requires_pinned_host_staging") is True
        )
        and (
            not require_quic
            or (
                network_path.get("quic_required") is True
                and network_path.get("quic_complete") is True
                and network_path.get("transport_frame_sink_observed") is True
                and network_path.get("frame_sink_used_for_all_direct_destination_reads") is True
                and network_path.get("direct_destination_frame_sink_cord_fanout_verified") is True
                and network_path.get("quic_read_transport_stream_completion_verified") is True
                and network_path.get("quic_read_transport_stream_eof_verified") is True
                and quic_read_expected_cipher_suite_proven(network_path)
                and summary_int(network_path, "quic_read_transport_max_read_stream_count") > 1
            )
        )
    )


def pipeline_training_loop_ready(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    report = child_mapping(payload, "pipeline_training_loop_report")
    return (
        summary_bool(payload, "pipeline_training_loop_required")
        and summary_bool(payload, "pipeline_training_loop_verified")
        and summary_value(payload, "pipeline_training_loop_context") == EXPECTED_PIPELINE_LOOP_CONTEXT
        and summary_bool(payload, "pipeline_training_loop_context_used")
        and summary_value(payload, "pipeline_training_loop_object") == EXPECTED_PIPELINE_LOOP_OBJECT
        and report.get("required") is True
        and report.get("verified") is True
        and report.get("context") == EXPECTED_PIPELINE_LOOP_CONTEXT
        and report.get("context_used") is True
        and report.get("object") == EXPECTED_PIPELINE_LOOP_OBJECT
        and summary_int(report, "batch_count") > 0
        and summary_int(report, "sample_count") > 0
        and summary_int(report, "byte_size") > 0
        and report.get("stream_state_format") == EXPECTED_STREAM_STATE_FORMAT
        and report.get("pipeline_network_path_complete") is True
        and pipeline_network_path_ready(report, require_quic=True)
    )


def distributed_runtime_smoke_ready(smoke: Mapping[str, Any] | None) -> bool:
    if not smoke:
        return False
    overlap = child_mapping(smoke, "rank_sample_overlap")
    topology = child_mapping(smoke, "remote_client_topology_after_close")
    reliability = child_mapping(smoke, "read_path_reliability")
    rank_states = child_mapping(smoke, "rank_state_artifacts")
    return (
        smoke.get("format") == "cxnfs.trainer-distributed-runtime-smoke.v1"
        and smoke.get("passed") is True
        and smoke.get("runtime_smoke_format") == "cxnfs.trainer-runtime-smoke.v1"
        and smoke.get("runtime_binding_format") == "cxnfs.trainer-runtime-binding.v1"
        and smoke.get("runtime_binding_object") == "CxnfsTrainerRuntimeBinding"
        and summary_value(smoke, "training_loop_context") == "CxnfsTrainerRuntimeBinding.open_training_loop"
        and summary_bool(smoke, "training_loop_context_used")
        and smoke.get("remote_client_required") is True
        and smoke.get("raw_password_sent_over_control") is False
        and summary_int(smoke, "world_size") > 1
        and summary_int(smoke, "rank_count") == summary_int(smoke, "world_size")
        and summary_int(smoke, "total_sample_count") > 0
        and smoke.get("all_rank_streams_opened") is True
        and smoke.get("all_rank_stream_kwargs_accepted") is True
        and smoke.get("all_rank_state_matches_final_cursor") is True
        and smoke.get("all_rank_training_loop_context_used") is True
        and smoke.get("all_rank_training_shards_correct") is True
        and smoke.get("all_rank_state_files_written") is True
        and summary_int(smoke, "rank_state_file_count") == summary_int(smoke, "world_size")
        and rank_states.get("format") == "cxnfs.trainer-distributed-runtime-rank-states.v1"
        and rank_states.get("all_rank_state_files_written") is True
        and overlap.get("no_cross_rank_overlap") is True
        and summary_int(overlap, "duplicate_observed_sample_count") == 0
        and topology.get("complete") is True
        and topology.get("all_open_clients_closed") is True
        and payload_uses_quic(smoke)
        and read_path_proven(smoke)
        and reliability.get("complete") is True
        and summary_int(reliability, "unrecovered_read_failures") == 0
    )


def trainer_loop_replay_ready(loop_replay: Mapping[str, Any] | None) -> bool:
    if not loop_replay:
        return False
    stream_report = loop_replay.get("stream_report")
    return (
        loop_replay.get("format") == "cxnfs.trainer-loop-replay.v1"
        and loop_replay.get("passed") is True
        and loop_replay.get("remote_client_required") is True
        and loop_replay.get("handoff_format") == "cxnfs.trainer-handoff.v1"
        and loop_replay.get("plan_format") == "cxnfs.trainer-integration-plan.v1"
        and summary_value(loop_replay, "training_loop_context") == "CxnfsTrainerIntegrationPlan.open_training_loop"
        and summary_bool(loop_replay, "training_loop_context_used")
        and summary_value(loop_replay, "training_loop_object") == "CxnfsTrainerPrefetchStream"
        and summary_value(loop_replay, "training_loop_base_stream_object") == "CxnfsTrainerStream"
        and summary_int(loop_replay, "training_loop_prefetch_batches") > 0
        and summary_bool(loop_replay, "training_loop_prefetch_applied")
        and summary_int(loop_replay, "step_count") > 0
        and summary_int(loop_replay, "batch_count") > 0
        and summary_int(loop_replay, "sample_count") > 0
        and summary_bool(loop_replay, "weighted_source_coverage_complete")
        and summary_bool(loop_replay, "resume_replayed")
        and summary_bool(loop_replay, "resume_continuity_verified")
        and summary_int(loop_replay, "final_emitted") >= summary_int(loop_replay, "sample_count")
        and isinstance(stream_report, dict)
        and payload_uses_quic(stream_report)
        and read_path_proven(loop_replay)
        and read_reliability_proven(stream_report)
    )


def trainer_readiness_bundle_ready(bundle: Mapping[str, Any] | None) -> bool:
    if not bundle:
        return False
    report_artifacts = child_mapping(bundle, "report_artifacts")
    rank_states = child_mapping(bundle, "rank_state_artifacts")
    reports = child_mapping(bundle, "reports")
    preflight = child_mapping(reports, "preflight")
    integration_smoke = child_mapping(reports, "integration_smoke")
    runtime_smoke = child_mapping(reports, "runtime_smoke")
    distributed_runtime_smoke = child_mapping(reports, "distributed_runtime_smoke")
    loop_replay = child_mapping(reports, "loop_replay")
    return (
        bundle.get("format") == "cxnfs.trainer-readiness-bundle.v1"
        and bundle.get("passed") is True
        and bundle.get("handoff_format") == "cxnfs.trainer-handoff.v1"
        and bundle.get("preflight_format") == "cxnfs.trainer-integration-preflight.v1"
        and bundle.get("integration_smoke_format") == "cxnfs.trainer-integration-smoke.v1"
        and bundle.get("runtime_smoke_format") == "cxnfs.trainer-runtime-smoke.v1"
        and bundle.get("distributed_runtime_smoke_format") == "cxnfs.trainer-distributed-runtime-smoke.v1"
        and bundle.get("loop_replay_format") == "cxnfs.trainer-loop-replay.v1"
        and bundle.get("ready_for_trainer_wiring") is True
        and bundle.get("ready_for_remote_quic") is True
        and bundle.get("requires_low_level_direct_read_proofs") is True
        and bundle.get("low_level_direct_read_proofs_complete") is True
        and bundle.get("batch_direct_read_proof_complete") is True
        and bundle.get("batch_direct_tensor_read_proof_complete") is True
        and bundle.get("batch_direct_read_cipher_suite") == EXPECTED_CIPHER_SUITE
        and bundle.get("batch_direct_tensor_read_cipher_suite") == EXPECTED_CIPHER_SUITE
        and bundle.get("requires_low_level_direct_write_proofs") is True
        and bundle.get("low_level_direct_write_proofs_complete") is True
        and bundle.get("write_probe_proof_complete") is True
        and bundle.get("write_probe_cipher_suite") == EXPECTED_CIPHER_SUITE
        and bundle.get("write_probe_quic_transport_complete") is True
        and summary_int(bundle, "write_probe_max_observed_write_cords") > 1
        and bundle.get("direct_quic_memory_path_verified") is True
        and bundle.get("remote_client_required") is True
        and bundle.get("requires_credentials") is True
        and bundle.get("raw_password_sent_over_control") is False
        and bundle.get("direct_python_streaming_verified") is True
        and bundle.get("runtime_training_loop_verified") is True
        and pipeline_training_loop_requirement_satisfied(bundle)
        and bundle.get("pipeline_network_path_verified") is True
        and bundle.get("distributed_runtime_verified") is True
        and bundle.get("distributed_rank_states_written") is True
        and bundle.get("loop_resume_verified") is True
        and report_artifacts.get("format") == "cxnfs.trainer-readiness-bundle-artifacts.v1"
        and summary_int(bundle, "artifact_count") >= 5
        and preflight.get("format") == "cxnfs.trainer-integration-preflight.v1"
        and preflight.get("passed") is True
        and preflight.get("ready_for_trainer_wiring") is True
        and preflight.get("ready_for_remote_quic") is True
        and preflight.get("low_level_direct_read_proofs_complete") is True
        and preflight.get("batch_direct_read_proof_complete") is True
        and preflight.get("batch_direct_tensor_read_proof_complete") is True
        and preflight.get("batch_direct_read_cipher_suite") == EXPECTED_CIPHER_SUITE
        and preflight.get("batch_direct_tensor_read_cipher_suite") == EXPECTED_CIPHER_SUITE
        and preflight.get("low_level_direct_write_proofs_complete") is True
        and preflight.get("write_probe_proof_complete") is True
        and preflight.get("write_probe_cipher_suite") == EXPECTED_CIPHER_SUITE
        and preflight.get("write_probe_quic_transport_complete") is True
        and summary_int(preflight, "write_probe_max_observed_write_cords") > 1
        and preflight.get("raw_password_sent_over_control") is False
        and integration_smoke_ready(integration_smoke)
        and runtime_smoke_ready(runtime_smoke)
        and distributed_runtime_smoke_ready(distributed_runtime_smoke)
        and trainer_loop_replay_ready(loop_replay)
        and rank_states.get("format") == "cxnfs.trainer-distributed-runtime-rank-states.v1"
        and rank_states.get("all_rank_state_files_written") is True
        and summary_int(bundle, "rank_state_file_count") == summary_int(bundle, "world_size")
    )


def trainer_loop_contract_ready(contract: Mapping[str, Any] | None) -> bool:
    if not contract:
        return False
    entrypoint = child_mapping(contract, "entrypoint")
    authentication = child_mapping(contract, "authentication")
    mount_free = child_mapping(contract, "mount_free_contract")
    stream = child_mapping(contract, "stream_contract")
    pipeline = child_mapping(contract, "pipeline_contract")
    batch = child_mapping(contract, "batch_contract")
    distributed = child_mapping(contract, "distributed_contract")
    resume = child_mapping(contract, "resume_contract")
    transport = child_mapping(contract, "remote_transport_contract")
    batch_fields = batch.get("fields")
    sample_fields = batch.get("sample_fields")
    return (
        contract.get("format") == "cxnfs.trainer-loop-contract.v1"
        and contract.get("passed") is True
        and contract.get("runtime_binding_format") == "cxnfs.trainer-runtime-binding.v1"
        and contract.get("stream_state_format") == "cxnfs.trainer-stream-state.v1"
        and entrypoint.get("runtime_binding_loader") == "CxnfsTrainerRuntimeBinding.from_file"
        and entrypoint.get("training_loop_context") == "CxnfsTrainerRuntimeBinding.open_training_loop"
        and entrypoint.get("pipeline_training_loop_context") == EXPECTED_PIPELINE_LOOP_CONTEXT
        and entrypoint.get("pipeline_loop_object") == EXPECTED_PIPELINE_LOOP_OBJECT
        and entrypoint.get("batch_object") == "CxnfsTrainerBatch"
        and authentication.get("remote_client_required") is True
        and authentication.get("requires_credentials") is True
        and authentication.get("credential_prompt_supported") is True
        and authentication.get("credential_prompt_required_for_remote") is True
        and authentication.get("password_cli_argument") is False
        and authentication.get("raw_password_sent_over_control") is False
        and mount_free.get("requires_mount") is False
        and mount_free.get("touches_trainer_code") is False
        and mount_free.get("opens_network_connections_during_preflight") is False
        and mount_free.get("consumes_batches_during_preflight") is False
        and mount_free.get("safe_to_run_before_trainer_workers") is True
        and stream.get("read_mode") in {"direct-buffers", "direct-tensors"}
        and stream.get("trainer_batch_iterator") in {"native", "trainer-dataloader"}
        and summary_int(stream, "trainer_prefetch_batches") > 0
        and stream.get("direct_python_streaming_verified") is True
        and stream.get("runtime_training_loop_verified") is True
        and pipeline.get("training_pipeline_object") == "CxnfsTrainingPipeline"
        and pipeline.get("training_pipeline_context") in {
            "CxnfsTrainingPipeline.read_tensor_batch",
            "CxnfsTrainingPipeline.read_into_tensors",
            "CxnfsTrainingPipeline.prefetch_trainer_stream",
        }
        and pipeline.get("pipeline_stream_context") == "CxnfsTrainingPipeline.prefetch_trainer_stream"
        and pipeline.get("pipeline_descriptor_iterator") == "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors"
        and pipeline.get("pipeline_descriptor_stream") == "CxnfsTrainerPipelineStream"
        and pipeline.get("pipeline_loop_object") == EXPECTED_PIPELINE_LOOP_OBJECT
        and pipeline.get("pipeline_stream_state_format") == EXPECTED_STREAM_STATE_FORMAT
        and pipeline.get("pipeline_prefetch_result_object") == "CxnfsPipelinePrefetchedTensorBatch"
        and pipeline.get("supports_pipeline_direct_tensor_batches") is True
        and pipeline.get("supports_pipeline_torch_compile") is True
        and pipeline.get("pipeline_cpu_augmentation_default") == "disabled-with-explicit-opt-in"
        and pipeline.get("python_byte_batch_avoidance_supported") is True
        and pipeline.get("pipeline_network_path_verified") is True
        and pipeline_network_path_ready(pipeline, require_quic=True)
        and isinstance(batch_fields, list)
        and {"sample_count", "byte_size", "source_counts", "resume_cursor"}.issubset(set(batch_fields))
        and isinstance(sample_fields, list)
        and {"source_name", "source_index", "sample_index", "data"}.issubset(set(sample_fields))
        and summary_int(distributed, "world_size") > 1
        and distributed.get("cross_rank_overlap_free") is True
        and distributed.get("rank_states_written") is True
        and resume.get("cursor_format") == "cxnfs.trainer-cursor.v1"
        and resume.get("state_format") == "cxnfs.trainer-stream-state.v1"
        and resume.get("loop_resume_verified") is True
        and summary_int(resume, "sample_count") > 0
        and transport.get("bulk_transport") == "quic"
        and summary_int(transport, "max_cords") >= 64
        and transport.get("per_source_client_default") is True
        and transport.get("ready_for_remote_quic") is True
        and transport.get("direct_quic_memory_path_verified") is True
    )


def trainer_handoff_acceptance_ready(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    reports = child_mapping(payload, "reports")
    readiness_bundle = child_mapping(reports, "readiness_bundle")
    surface = child_mapping(payload, "trainer_handoff_surface")
    loop_contract = child_mapping(payload, "trainer_loop_contract")
    return (
        payload.get("format") == "cxnfs.trainer-handoff-acceptance.v1"
        and payload.get("passed") is True
        and payload.get("handoff_format") == "cxnfs.trainer-handoff.v1"
        and payload.get("readiness_bundle_format") == "cxnfs.trainer-readiness-bundle.v1"
        and payload.get("ready_for_trainer_wiring") is True
        and payload.get("ready_for_remote_quic") is True
        and payload.get("requires_remote_quic") is True
        and payload.get("direct_quic_memory_path_verified") is True
        and payload.get("direct_python_streaming_verified") is True
        and payload.get("runtime_training_loop_verified") is True
        and pipeline_training_loop_requirement_satisfied(payload)
        and payload.get("pipeline_network_path_verified") is True
        and payload.get("distributed_runtime_verified") is True
        and payload.get("loop_resume_verified") is True
        and payload.get("cross_rank_overlap_free") is True
        and payload.get("rank_states_written") is True
        and payload.get("remote_client_required") is True
        and payload.get("requires_credentials") is True
        and payload.get("raw_password_sent_over_control") is False
        and payload.get("requires_mount") is False
        and payload.get("trainer_code_touched") is False
        and payload.get("runtime_binding_object") == "CxnfsTrainerRuntimeBinding"
        and payload.get("training_loop_context") == "CxnfsTrainerRuntimeBinding.open_training_loop"
        and (
            not summary_bool(payload, "pipeline_training_loop_required")
            or payload.get("pipeline_training_loop_context") == EXPECTED_PIPELINE_LOOP_CONTEXT
        )
        and payload.get("trainer_loop_contract_format") == "cxnfs.trainer-loop-contract.v1"
        and trainer_loop_contract_ready(loop_contract)
        and surface.get("uses_mount") is False
        and surface.get("touches_trainer_code") is False
        and surface.get("password_cli_argument") is False
        and trainer_readiness_bundle_ready(readiness_bundle)
    )


def trainer_worker_loop_replay_ready(loop_replay: Mapping[str, Any] | None) -> bool:
    if not loop_replay:
        return False
    stream_report = loop_replay.get("stream_report")
    if not isinstance(stream_report, dict):
        return False
    worker_partition = child_mapping(loop_replay, "worker_partition_evidence")
    data_loader_workers = summary_int(loop_replay, "data_loader_workers")
    samples_per_epoch = summary_int(loop_replay, "samples_per_epoch")
    return (
        loop_replay.get("format") == "cxnfs.trainer-loop-replay.v1"
        and loop_replay.get("passed") is True
        and loop_replay.get("remote_client_required") is True
        and loop_replay.get("handoff_format") == "cxnfs.trainer-handoff.v1"
        and loop_replay.get("plan_format") == "cxnfs.trainer-integration-plan.v1"
        and summary_value(loop_replay, "training_loop_context") == "CxnfsTrainerIntegrationPlan.open_training_loop"
        and summary_bool(loop_replay, "training_loop_context_used")
        and summary_value(loop_replay, "trainer_batch_iterator") == "trainer-dataloader"
        and data_loader_workers > 0
        and summary_value(loop_replay, "repeat") is False
        and samples_per_epoch > 0
        and summary_int(loop_replay, "step_count") > 0
        and summary_int(loop_replay, "batch_count") > 0
        and summary_int(loop_replay, "sample_count") == samples_per_epoch
        and summary_bool(loop_replay, "weighted_source_coverage_complete")
        and summary_bool(loop_replay, "resume_replayed")
        and summary_bool(loop_replay, "resume_continuity_verified")
        and summary_int(loop_replay, "final_emitted") == summary_int(loop_replay, "sample_count")
        and summary_bool(loop_replay, "worker_partitioning_enabled")
        and summary_int(loop_replay, "worker_cursor_count") >= data_loader_workers
        and summary_bool(loop_replay, "worker_batch_read_evidence_observed")
        and summary_int(loop_replay, "worker_batch_read_evidence_count") >= data_loader_workers
        and worker_partition.get("worker_sharding_enabled") is True
        and worker_partition.get("complete") is True
        and mapping_int(worker_partition, "duplicate_source_sample_count", 1) == 0
        and contains_key_value(loop_replay, "produced_inside_torch_worker", True)
        and payload_uses_quic(stream_report)
        and read_path_proven(loop_replay)
        and read_reliability_proven(stream_report)
    )


def trainer_stream_benchmark_ready(payload: Mapping[str, Any] | None) -> bool:
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.trainer-stream-smoke.v1"
        and payload.get("read_mode") in {"direct-buffers", "direct-tensors"}
        and payload.get("trainer_prefetch_enabled") is True
        and summary_int(payload, "trainer_prefetch_batches") > 0
        and summary_int(payload, "bulk_read_retries") >= 1
        and payload_uses_quic(payload)
        and read_path_proven(payload)
        and read_reliability_proven(payload)
    )


def trainer_pipeline_smoke_ready(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    write_probe = child_mapping(payload, "write_probe")
    write_path = child_mapping(write_probe, "write_path_evidence")
    bulk_write = child_mapping(write_path, "bulk_transport_write_evidence")
    return (
        payload.get("format") == "cxnfs.trainer-pipeline-smoke.v1"
        and payload.get("passed") is True
        and payload.get("read_mode") == "direct-tensors"
        and payload.get("training_pipeline_object") == "CxnfsTrainingPipeline"
        and payload.get("training_pipeline_context") == "CxnfsTrainingPipeline.prefetch_trainer_stream"
        and payload.get("pipeline_descriptor_stream") == "CxnfsTrainerPipelineStream"
        and summary_bool(payload, "pipeline_prefetch_stream_used")
        and summary_int(payload, "pipeline_prefetch_batches") > 0
        and payload.get("pipeline_prefetch_result_object") == "CxnfsPipelinePrefetchedTensorBatch"
        and summary_bool(payload, "pipeline_python_byte_batch_avoided")
        and summary_bool(payload, "pipeline_network_path_complete")
        and pipeline_network_path_ready(payload, require_quic=True)
        and payload_uses_quic(payload)
        and read_path_proven(payload)
        and read_reliability_proven(payload)
        and child_mapping(payload, "tensor_memory_path").get("complete") is True
        and child_mapping(payload, "remote_client_topology").get("per_source_client_observed") is True
        and payload.get("remote_client_lifecycle_closed") is True
        and write_probe.get("enabled") is True
        and write_probe.get("verified_read_back") is True
        and write_path.get("format") == "cxnfs.bulk-write-path-evidence.v1"
        and mapping_int(write_path, "direct_tensor_write_operations") > 0
        and mapping_int(write_path, "max_observed_write_cords") > 1
        and mapping_int(write_path, "write_frames") == mapping_int(write_path, "write_acks")
        and mapping_int(write_path, "write_ack_frame_verified") == mapping_int(write_path, "write_frames")
        and bool(write_path.get("write_ack_frame_identity_verified"))
        and mapping_int(write_path, "metadata_after_payload_entries") > 0
        and mapping_int(write_path, "entries_committed") > 0
        and mapping_int(write_path, "transfers_committed") > 0
        and bulk_write.get("format") == "cxnfs.quic-bulk-write-transport-evidence.v1"
        and bulk_write.get("cipher_suite") == EXPECTED_CIPHER_SUITE
        and bool(bulk_write.get("connection_reused_for_streams"))
        and mapping_int(bulk_write, "write_acks_verified") == mapping_int(write_path, "write_frames")
        and mapping_int(bulk_write, "write_ack_verification_failures") == 0
        and bool(bulk_write.get("write_ack_frame_identity_verified"))
        and bool(bulk_write.get("write_ack_stream_completion_verified"))
        and bool(bulk_write.get("write_ack_stream_eof_verified"))
        and mapping_int(bulk_write, "max_active_write_streams") > 1
    )


def trainer_distributed_plan_ready(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    rank_plans = payload.get("rank_plans")
    rank_count = int(payload.get("rank_count") or 0)
    return (
        payload.get("format") == "cxnfs.trainer-distributed-plan.v1"
        and payload.get("passed") is True
        and payload.get("ready_for_trainer_wiring") is True
        and payload.get("ready_for_remote_quic") is True
        and payload.get("low_level_direct_read_proofs_complete") is True
        and payload.get("batch_direct_read_proof_complete") is True
        and payload.get("batch_direct_tensor_read_proof_complete") is True
        and payload.get("low_level_direct_write_proofs_complete") is True
        and payload.get("write_probe_proof_complete") is True
        and payload.get("write_probe_cipher_suite") == EXPECTED_CIPHER_SUITE
        and payload.get("runtime_binding_format") == "cxnfs.trainer-runtime-binding.v1"
        and payload.get("runtime_binding_object") == "CxnfsTrainerRuntimeBinding"
        and not bool(payload.get("opens_network_connections"))
        and not bool(payload.get("consumes_batches"))
        and not bool(payload.get("raw_password_sent_over_control"))
        and rank_count > 0
        and isinstance(rank_plans, list)
        and len(rank_plans) == rank_count
        and all(
            isinstance(rank_plan, dict) and isinstance(rank_plan.get("training_shard"), dict)
            for rank_plan in rank_plans
        )
    )


def trainer_network_preflight_ready(payload: Mapping[str, Any] | None) -> bool:
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.trainer-network-preflight.v1"
        and payload.get("passed") is True
        and payload.get("handoff_format") == "cxnfs.trainer-handoff.v1"
        and payload.get("preflight_format") == "cxnfs.trainer-integration-preflight.v1"
        and payload.get("ready_for_trainer_wiring") is True
        and payload.get("ready_for_remote_quic") is True
        and payload.get("remote_client_required") is True
        and payload.get("requires_credentials") is True
        and payload.get("raw_password_sent_over_control") is False
        and payload.get("safe_to_run_before_trainer_workers") is True
        and payload.get("opens_network_connections") is True
        and payload.get("consumes_batches") is False
        and payload.get("authenticated") is True
        and payload_uses_quic(payload)
        and payload.get("bulk_probe") == "ok"
        and summary_int(payload, "bulk_probe_bytes") > 0
        and summary_int(payload, "read_length") > 0
        and payload.get("network_probe_is_handoff_aware") is True
    )


def trainer_workers_ready(payload: Mapping[str, Any] | None) -> bool:
    worker_partition = child_mapping(payload or {}, "worker_partition_evidence")
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.trainer-stream-smoke.v1"
        and payload.get("trainer_batch_iterator") == "trainer-dataloader"
        and summary_int(payload, "data_loader_workers") > 0
        and payload_uses_quic(payload)
        and read_path_proven(payload)
        and read_reliability_proven(payload)
        and summary_bool(payload, "worker_batch_read_evidence_observed")
        and summary_int(payload, "worker_batch_read_evidence_count") > 0
        and worker_partition.get("worker_sharding_enabled") is True
        and worker_partition.get("complete") is True
        and mapping_int(worker_partition, "duplicate_source_sample_count", 1) == 0
        and contains_key_value(payload, "produced_inside_torch_worker", True)
    )


def trainer_resume_ready(payload: Mapping[str, Any] | None) -> bool:
    first_final = summary_int(payload, "first_final_emitted")
    second_initial = summary_int(payload, "second_initial_emitted")
    second_final = summary_int(payload, "second_final_emitted")
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.quic-trainer-resume.v1"
        and payload.get("cursor_format") == "cxnfs.trainer-cursor.v1"
        and payload.get("stream_state_format") == "cxnfs.trainer-stream-state.v1"
        and payload.get("state_file_written") is True
        and payload.get("state_file_replayed") is True
        and payload.get("state_stream_state_matches_final_cursor") is True
        and payload.get("trainer_prefetch_enabled") is True
        and summary_int(payload, "trainer_prefetch_batches") > 0
        and summary_int(payload, "bulk_read_retries") >= 1
        and first_final > 0
        and second_initial == first_final
        and second_final > second_initial
        and payload_uses_quic(payload)
        and summary_bool(payload, "transport_frame_sink_observed")
        and summary_bool(payload, "frame_sink_used_for_all_direct_destination_reads")
        and summary_int(payload, "direct_destination_max_observed_cords") > 0
        and read_reliability_proven(payload)
    )


def high_cord_remote_quic_ready(read_report: Mapping[str, Any] | None, write_report: Mapping[str, Any] | None) -> bool:
    return (
        bool(read_report)
        and bool(write_report)
        and payload_uses_quic(read_report)
        and payload_uses_quic(write_report)
        and payload_has_tls13_aes128(read_report)
        and payload_has_tls13_aes128(write_report)
        and read_path_proven(read_report)
        and write_report.get("read_back_verified") is True
        and summary_int(write_report, "write_ack_frame_verified") == summary_int(write_report, "write_frames")
        and summary_bool(write_report, "write_ack_frame_identity_verified")
        and summary_bool(write_report, "write_ack_stream_completion_verified")
        and summary_bool(write_report, "write_ack_stream_eof_verified")
        and summary_int(write_report, "write_acks_verified") >= summary_int(write_report, "write_frames")
        and summary_int(write_report, "write_ack_verification_failures") == 0
        and summary_int(read_report, "direct_destination_max_observed_cords") >= 64
        and summary_int(write_report, "max_observed_write_cords") >= 64
    )


def python_client_cli_quic_smoke_ready(payload: Mapping[str, Any] | None) -> bool:
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.python-client-cli-quic-smoke.v1"
        and payload_uses_quic(payload)
        and payload.get("read_back_verified") is True
        and payload.get("raw_password_visible") is False
        and payload.get("read_cipher_suite") == EXPECTED_CIPHER_SUITE
        and payload.get("write_cipher_suite") == EXPECTED_CIPHER_SUITE
        and payload.get("direct_write_cipher_suite") == EXPECTED_CIPHER_SUITE
        and payload.get("direct_file_write_verified") is True
        and str(payload.get("server_bulk_listener") or "").startswith("quic-tls13-udp://")
        and payload.get("server_bulk_cipher_suite") == EXPECTED_HEALTH_CIPHER_SUITE
        and payload.get("server_bulk_total_streams_observed") is True
        and summary_int(payload, "server_bulk_total_streams") > 0
        and summary_int(payload, "read_max_observed_cords") > 1
        and summary_int(payload, "write_max_observed_cords") > 1
        and summary_int(payload, "direct_write_max_observed_cords") > 1
    )


def batch_direct_read_ready(payload: Mapping[str, Any] | None) -> bool:
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.quic-batch-direct-read.v1"
        and payload_uses_quic(payload)
        and payload_has_tls13_aes128(payload)
        and payload.get("ordered_batch_read_verified") is True
        and summary_int(payload, "batch_read_count") >= 2
        and summary_int(payload, "batch_worker_count") >= 2
        and summary_int(payload, "read_operations") >= 2
        and summary_int(payload, "direct_buffer_read_operations") >= 2
        and summary_int(payload, "direct_destination_read_operations") >= 2
        and summary_int(payload, "payload_bytes_read") > 0
        and read_path_proven(payload)
    )


def batch_direct_tensor_read_ready(payload: Mapping[str, Any] | None) -> bool:
    tensor_memory_path = child_mapping(payload or {}, "tensor_memory_path")
    return (
        bool(payload)
        and payload.get("format") == "cxnfs.quic-batch-direct-tensor-read.v1"
        and payload.get("tensor_target") == "pytorch-cpu"
        and payload.get("tensor_dtype") == "uint8"
        and payload.get("tensor_device") == "cpu"
        and payload_uses_quic(payload)
        and payload_has_tls13_aes128(payload)
        and payload.get("ordered_tensor_read_verified") is True
        and summary_int(payload, "tensor_read_count") >= 2
        and summary_int(payload, "tensor_worker_count") >= 2
        and summary_int(payload, "read_operations") >= 2
        and summary_int(payload, "direct_tensor_read_operations") >= 2
        and summary_int(payload, "direct_destination_read_operations") >= 2
        and summary_int(payload, "payload_bytes_read") > 0
        and tensor_memory_path.get("format") == "cxnfs.trainer-tensor-memory-path.v1"
        and tensor_memory_path.get("complete") is True
        and tensor_memory_path.get("python_byte_batch_avoided") is True
        and tensor_memory_path.get("network_fills_tensor_destination") is True
        and tensor_memory_path.get("device_copy_required") is False
        and read_path_proven(payload)
    )


def derive_readiness(payload_by_path: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    contract = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_contract"])
    matrix = (
        payload_by_path.get(KNOWN_REPORT_PATHS["trainer_contract_surface_matrix"])
        or payload_by_path.get(KNOWN_REPORT_PATHS["trainer_surface_matrix"])
    )
    workload = (
        payload_by_path.get(KNOWN_REPORT_PATHS["trainer_contract_workload_benchmark"])
        or payload_by_path.get(KNOWN_REPORT_PATHS["trainer_workload_benchmark"])
    )
    smoke = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_integration_smoke"])
    runtime_smoke = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_runtime_smoke"])
    distributed_runtime_smoke = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_distributed_runtime_smoke"])
    loop_replay = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_loop_replay"])
    readiness_bundle = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_readiness_bundle"])
    handoff_acceptance = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_handoff_acceptance"])
    network_preflight = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_network_preflight"])
    worker_loop_replay = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_worker_loop_replay"])
    trainer_stream = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_stream_benchmark"])
    trainer_pipeline = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_pipeline_smoke"])
    trainer_distributed_plan = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_distributed_plan"])
    trainer_workers = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_workers"])
    trainer_resume = payload_by_path.get(KNOWN_REPORT_PATHS["trainer_resume"])
    high_read = payload_by_path.get(KNOWN_REPORT_PATHS["high_cord_read"])
    high_write = payload_by_path.get(KNOWN_REPORT_PATHS["high_cord_write"])
    batch_read = payload_by_path.get(KNOWN_REPORT_PATHS["batch_direct_read"])
    batch_tensor_read = payload_by_path.get(KNOWN_REPORT_PATHS["batch_direct_tensor_read"])
    client_cli = payload_by_path.get(KNOWN_REPORT_PATHS["python_client_cli_quic_smoke"])

    trainer_ready = (
        contract_ready(contract)
        and trainer_stream_benchmark_ready(trainer_stream)
        and trainer_workers_ready(trainer_workers)
        and trainer_resume_ready(trainer_resume)
        and surface_matrix_ready(matrix)
        and workload_ready(workload)
        and integration_smoke_ready(smoke)
        and runtime_smoke_ready(runtime_smoke)
        and distributed_runtime_smoke_ready(distributed_runtime_smoke)
        and trainer_loop_replay_ready(loop_replay)
        and trainer_readiness_bundle_ready(readiness_bundle)
        and trainer_handoff_acceptance_ready(handoff_acceptance)
        and trainer_network_preflight_ready(network_preflight)
        and trainer_worker_loop_replay_ready(worker_loop_replay)
        and trainer_pipeline_smoke_ready(trainer_pipeline)
        and trainer_distributed_plan_ready(trainer_distributed_plan)
        and batch_direct_read_ready(batch_read)
        and batch_direct_tensor_read_ready(batch_tensor_read)
    )
    remote_ready = (
        batch_direct_read_ready(batch_read)
        and (
            (
                trainer_stream_benchmark_ready(trainer_stream)
                and trainer_pipeline_smoke_ready(trainer_pipeline)
                and trainer_workers_ready(trainer_workers)
                and trainer_resume_ready(trainer_resume)
                and trainer_worker_loop_replay_ready(worker_loop_replay)
                and trainer_network_preflight_ready(network_preflight)
                and workload_ready(workload)
                and python_client_cli_quic_smoke_ready(client_cli)
            )
            or high_cord_remote_quic_ready(high_read, high_write)
        )
    )
    explicit = explicit_readiness_flags(payload_by_path)
    blockfs_ready = explicit.get("ready_for_blockfs_candidate")
    if blockfs_ready is None and (trainer_ready or remote_ready):
        blockfs_ready = False
    kernel_allowed = explicit.get("kernel_module_allowed")
    if kernel_allowed is None and blockfs_ready is False:
        kernel_allowed = False
    def explicit_gate(key: str, fallback: Any) -> Any:
        value = explicit.get(key)
        return fallback if value is None else bool(value) and bool(fallback)

    return {
        "format": READINESS_FORMAT,
        "trainer_contract_ready": contract_ready(contract),
        "trainer_surface_matrix_ready": surface_matrix_ready(matrix),
        "trainer_workload_ready": workload_ready(workload),
        "trainer_sustained_read_profile_ready": sustained_read_profile_ready(workload),
        "trainer_integration_smoke_ready": integration_smoke_ready(smoke),
        "trainer_runtime_smoke_ready": runtime_smoke_ready(runtime_smoke),
        "trainer_distributed_runtime_smoke_ready": distributed_runtime_smoke_ready(distributed_runtime_smoke),
        "trainer_loop_replay_ready": trainer_loop_replay_ready(loop_replay),
        "trainer_readiness_bundle_ready": trainer_readiness_bundle_ready(readiness_bundle),
        "trainer_handoff_acceptance_ready": trainer_handoff_acceptance_ready(handoff_acceptance),
        "trainer_network_preflight_ready": trainer_network_preflight_ready(network_preflight),
        "trainer_worker_loop_replay_ready": trainer_worker_loop_replay_ready(worker_loop_replay),
        "trainer_pipeline_smoke_ready": trainer_pipeline_smoke_ready(trainer_pipeline),
        "trainer_distributed_plan_ready": trainer_distributed_plan_ready(trainer_distributed_plan),
        "trainer_stream_benchmark_ready": trainer_stream_benchmark_ready(trainer_stream),
        "trainer_workers_ready": trainer_workers_ready(trainer_workers),
        "trainer_resume_ready": trainer_resume_ready(trainer_resume),
        "python_client_cli_quic_smoke_ready": python_client_cli_quic_smoke_ready(client_cli),
        "batch_direct_read_ready": batch_direct_read_ready(batch_read),
        "batch_direct_tensor_read_ready": batch_direct_tensor_read_ready(batch_tensor_read),
        "high_cord_remote_quic_ready": high_cord_remote_quic_ready(high_read, high_write),
        "ready_for_trainer_wiring": explicit_gate("ready_for_trainer_wiring", trainer_ready),
        "ready_for_remote_quic": explicit_gate("ready_for_remote_quic", remote_ready),
        "ready_for_blockfs_candidate": blockfs_ready,
        "kernel_module_allowed": kernel_allowed,
    }


def explicit_readiness_flags(payload_by_path: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    flag_sources = (
        payload_by_path.get(KNOWN_REPORT_PATHS["trainer_handoff"]),
        payload_by_path.get(KNOWN_REPORT_PATHS["workload_decision"]),
        payload_by_path.get(KNOWN_REPORT_PATHS["blockfs_handoff"]),
    )
    flags: dict[str, Any] = {}
    for key in (
        "ready_for_trainer_wiring",
        "ready_for_remote_quic",
        "ready_for_blockfs_candidate",
        "kernel_module_allowed",
    ):
        flags[key] = next((payload[key] for payload in flag_sources if payload and key in payload), None)
    return flags


def summarize_report(path: Path, report_dir: Path, payload: Mapping[str, Any] | None) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "path": path.relative_to(report_dir).as_posix(),
        "bytes": path.stat().st_size,
    }
    if payload is None:
        return entry
    for key in SUMMARY_KEYS:
        value = summary_value(payload, key)
        if value is not None:
            entry[key] = value
    return entry


def discover_reports(report_dir: Path) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    manifest_json = report_dir / "live-quic-check.json"
    manifest_report = report_dir / "live-quic-check.report"
    skip_names = {manifest_json.name, manifest_report.name}
    reports: list[dict[str, Any]] = []
    payload_by_path: dict[str, dict[str, Any]] = {}
    for path in sorted(p for p in report_dir.rglob("*") if p.is_file()):
        if path.name in skip_names:
            continue
        relpath = path.relative_to(report_dir).as_posix()
        payload = load_json(path) if path.suffix == ".json" else None
        if payload is not None:
            payload_by_path[relpath] = payload
        reports.append(summarize_report(path, report_dir, payload))
    return reports, payload_by_path


def build_live_quic_manifest(
    report_dir: Path,
    *,
    test_target: str,
    python_executable: str,
    bootstrap_venv: str | None = None,
    system_site_packages: bool = False,
    skip_maven: bool = False,
) -> dict[str, Any]:
    report_dir = report_dir.resolve()
    reports, payload_by_path = discover_reports(report_dir)
    known_reports = {
        name: relpath in payload_by_path or (report_dir / relpath).is_file()
        for name, relpath in KNOWN_REPORT_PATHS.items()
    }
    readiness = derive_readiness(payload_by_path)
    return {
        "format": MANIFEST_FORMAT,
        "status": "passed",
        "test_target": test_target,
        "report_dir": str(report_dir),
        "python_executable": python_executable,
        "bootstrap_venv": bootstrap_venv or None,
        "system_site_packages": system_site_packages,
        "skip_maven": skip_maven,
        "report_count": len(reports),
        "known_reports": known_reports,
        "ready_for_trainer_wiring": readiness["ready_for_trainer_wiring"],
        "ready_for_remote_quic": readiness["ready_for_remote_quic"],
        "ready_for_blockfs_candidate": readiness["ready_for_blockfs_candidate"],
        "kernel_module_allowed": readiness["kernel_module_allowed"],
        "readiness": readiness,
        "reports": reports,
    }


def manifest_report_lines(manifest: Mapping[str, Any]) -> list[str]:
    lines = [
        MANIFEST_FORMAT,
        "status=passed",
        f"test_target={manifest['test_target']}",
        f"report_count={manifest['report_count']}",
        f"manifest={Path(str(manifest['report_dir'])) / 'live-quic-check.json'}",
    ]
    for key, value in sorted(manifest["known_reports"].items()):
        lines.append(f"known_report.{key}={lower_bool(value)}")
    for key in (
        "kernel_module_allowed",
        "ready_for_blockfs_candidate",
        "ready_for_remote_quic",
        "ready_for_trainer_wiring",
    ):
        lines.append(f"{key}={lower_bool(manifest.get(key))}")
    readiness = manifest.get("readiness")
    if isinstance(readiness, dict):
        for key, value in sorted(readiness.items()):
            lines.append(f"readiness.{key}={lower_bool(value)}")
    return lines


def write_manifest(
    report_dir: Path,
    *,
    test_target: str,
    python_executable: str,
    bootstrap_venv: str | None = None,
    system_site_packages: bool = False,
    skip_maven: bool = False,
) -> dict[str, Any]:
    manifest = build_live_quic_manifest(
        report_dir,
        test_target=test_target,
        python_executable=python_executable,
        bootstrap_venv=bootstrap_venv,
        system_site_packages=system_site_packages,
        skip_maven=skip_maven,
    )
    report_dir = report_dir.resolve()
    (report_dir / "live-quic-check.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (report_dir / "live-quic-check.report").write_text(
        "\n".join(manifest_report_lines(manifest)) + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    if argv:
        raise SystemExit("cxnfs.live_quic_manifest reads CXNFS_LIVE_QUIC_* environment variables")
    write_manifest(
        Path(os.environ["CXNFS_LIVE_QUIC_REPORT_DIR"]),
        test_target=os.environ["CXNFS_LIVE_QUIC_TEST_TARGET"],
        python_executable=os.environ["CXNFS_LIVE_QUIC_PYTHON_BIN"],
        bootstrap_venv=os.environ.get("CXNFS_LIVE_QUIC_BOOTSTRAP_VENV") or None,
        system_site_packages=os.environ.get("CXNFS_LIVE_QUIC_SYSTEM_SITE_PACKAGES") == "1",
        skip_maven=os.environ.get("CXNFS_LIVE_QUIC_SKIP_MAVEN") == "1",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
