#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"

REPORT_DIR="${CXNFS_RELEASE_REPORT_DIR:-$ROOT_DIR/target/cxnfs-release-check}"
BLOCKFS_BUILD_DIR="${CXNFS_BLOCKFS_BUILD_DIR:-$REPORT_DIR/blockfs-build}"
JAVA_BIN="${JAVA_BIN:-java}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
MVN_BIN="${MVN_BIN:-mvn}"
CMAKE_BIN="${CMAKE_BIN:-cmake}"
PYTHON_SRC_PATH="$ROOT_DIR/python/src"
PYTHON_RUN_PATH="$PYTHON_SRC_PATH"
if [ -n "${CXNFS_PYTHONPATH_EXTRA:-}" ]; then
	PYTHON_RUN_PATH="$PYTHON_RUN_PATH:$CXNFS_PYTHONPATH_EXTRA"
fi

DRY_RUN=0
SKIP_JAVA_TESTS=0
SKIP_PYTHON_TESTS=0
SKIP_QUIC_INTEGRATION=0
REQUIRE_QUIC_INTEGRATION=0
SKIP_BLOCKFS_SMOKE=0
SKIP_BLOCKFS_MODULE=0

usage() {
	cat <<'EOF'
usage: packaging/release-check.sh [options]

Runs the cross-stack CXNFS release gate:
  - Java Maven tests
  - Java Maven package
  - Java server health-file check and monitor metrics export
  - Java mount adapter handoff launch-plan and list-only smoke
  - Java mounted-path benchmark smoke
  - Java benchmark comparison report smoke
  - Java blockfs sidecar audit, extent-hint build, and namespace-sync reports
  - Java protocol fixture report with complete operation coverage required
  - Python unittest suite
  - Python-to-Java QUIC payload integration smoke
  - Python native benchmark CLI smoke
  - Python weighted trainer contract report smoke
  - Python trainer acceptance fixture smoke
  - Python contract-driven trainer workload benchmark smoke
  - Python weighted trainer-stream smoke
  - Python weighted trainer workload benchmark smoke
  - Python workload decision bundle smoke
  - Python mount handoff CLI smoke
  - Python protocol fixture report with complete operation coverage required
  - Linux kernel module build when running on Linux
  - Linux blockfs userspace tool smoke through CMake

Options:
  --report-dir <path>              report/build output directory
  --dry-run true|false             print commands without running them
  --skip-java-tests true|false     skip mvn test
  --skip-python-tests true|false   skip Python unit tests
  --skip-quic-integration true|false
                                     skip Python-to-Java QUIC payload integration
  --require-quic-integration true|false
                                     fail instead of skipping when QUIC prerequisites are unavailable
  --skip-blockfs-module true|false skip Linux kernel module build
  --skip-blockfs-smoke true|false  skip CMake blockfs tool smoke
  --help                           show this help

Environment:
  PYTHON_BIN                        Python interpreter for the CXNFS Python checks
  CXNFS_PYTHONPATH_EXTRA            extra Python import roots, appended after python/src
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
			BLOCKFS_BUILD_DIR="${CXNFS_BLOCKFS_BUILD_DIR:-$REPORT_DIR/blockfs-build}"
			shift 2
			;;
		--dry-run)
			need_value "$@"
			DRY_RUN="$(bool_arg "$2")"
			shift 2
			;;
		--skip-java-tests)
			need_value "$@"
			SKIP_JAVA_TESTS="$(bool_arg "$2")"
			shift 2
			;;
		--skip-python-tests)
			need_value "$@"
			SKIP_PYTHON_TESTS="$(bool_arg "$2")"
			shift 2
			;;
		--skip-quic-integration)
			need_value "$@"
			SKIP_QUIC_INTEGRATION="$(bool_arg "$2")"
			shift 2
			;;
		--require-quic-integration)
			need_value "$@"
			REQUIRE_QUIC_INTEGRATION="$(bool_arg "$2")"
			shift 2
			;;
		--skip-blockfs-smoke)
			need_value "$@"
			SKIP_BLOCKFS_SMOKE="$(bool_arg "$2")"
			shift 2
			;;
		--skip-blockfs-module)
			need_value "$@"
			SKIP_BLOCKFS_MODULE="$(bool_arg "$2")"
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
	printf ' %q' "$@"
	printf '\n'
	if [ "$DRY_RUN" != "1" ]; then
		"$@"
	fi
}

run_to_file() {
	local output_file="$1"
	shift
	printf '+'
	printf ' %q' "$@"
	printf ' > %q\n' "$output_file"
	if [ "$DRY_RUN" != "1" ]; then
		"$@" >"$output_file"
	fi
}

require_file_contains() {
	local input_file="$1"
	local expected="$2"
	printf '+ require %q contains %q\n' "$input_file" "$expected"
	if [ "$DRY_RUN" != "1" ]; then
		if ! grep -F -- "$expected" "$input_file" >/dev/null; then
			printf '%s missing expected text: %s\n' "$input_file" "$expected" >&2
			exit 1
		fi
	fi
}

require_trainer_direct_destination_fanout() {
	local input_file="$1"
	require_file_contains "$input_file" '"direct_destination_frame_sink_cord_fanout_verified": true'
	require_file_contains "$input_file" '"direct_destination_max_observed_cords":'
	require_file_contains "$input_file" '"direct_destination_parallel_frame_sink_read_operations":'
	require_file_contains "$input_file" '"direct_destination_parallel_frame_sink_frames":'
	require_file_contains "$input_file" '"last_accepted_initial_cords":'
	require_file_contains "$input_file" '"last_accepted_max_cords":'
	require_file_contains "$input_file" '"observed_cords_within_admission": true'
}

require_file_not_contains() {
	local input_file="$1"
	local unexpected="$2"
	printf '+ require %q not contains %q\n' "$input_file" "$unexpected"
	if [ "$DRY_RUN" != "1" ]; then
		if grep -F -- "$unexpected" "$input_file" >/dev/null; then
			printf '%s contains unexpected text: %s\n' "$input_file" "$unexpected" >&2
			exit 1
		fi
	fi
}

prepare_report_dir() {
	printf '+ mkdir -p %q\n' "$REPORT_DIR"
	if [ "$DRY_RUN" != "1" ]; then
		mkdir -p "$REPORT_DIR"
	fi
}

write_health_fixture() {
	local health_file="$REPORT_DIR/server.health.fixture"
	printf '+ write %q\n' "$health_file"
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	local sampled_at
	sampled_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	{
		printf 'cxnfs.server.health.v1\n'
		printf 'status=running\n'
		printf 'healthy=true\n'
		printf 'sampled_at=%s\n' "$sampled_at"
		printf 'process_id=0\n'
		printf 'served_directory=/srv/datasets\n'
		printf 'control_listener=tls13-tcp://127.0.0.1:4509\n'
		printf 'bulk_read_listener=tls13-bulk-stream://127.0.0.1:4510\n'
		printf 'cipher_suite=tls_aes_128_gcm_sha256\n'
		printf 'bulk_cipher_suite=tls_aes_128_gcm_sha256\n'
		printf 'control_running=true\n'
		printf 'bulk_running=true\n'
		printf 'control_active_connections=0\n'
		printf 'bulk_active_connections=0\n'
		printf 'bulk_total_connections=0\n'
		printf 'bulk_total_streams=0\n'
		printf 'control_last_accept_error=none\n'
		printf 'control_last_connection_error=none\n'
		printf 'bulk_last_accept_error=none\n'
		printf 'bulk_last_connection_error=none\n'
	} >"$health_file"
}

write_benchmark_fixture() {
	local benchmark_file="$REPORT_DIR/benchmark.fixture"
	printf '+ write %q\n' "$benchmark_file"
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	{
		printf 'cxnfs release benchmark fixture\n'
		printf '0123456789abcdef0123456789abcdef\n'
		printf 'training shard smoke bytes\n'
	} >"$benchmark_file"
}

write_weighted_mds_fixture() {
	local fixture_root="$REPORT_DIR/weighted-mds"
	printf '+ write %q\n' "$fixture_root"
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" - "$fixture_root" <<'PY'
from pathlib import Path
import sys

from cxnfs.prepare import DatasetConverter, DatasetPreparer

root = Path(sys.argv[1])
raw_root = root / "raw"
mds_root = root / "mds"
for name, values in {
    "chat": ("chat-0", "chat-1", "chat-2", "chat-3", "chat-4", "chat-5"),
    "summary": ("summary-0", "summary-1", "summary-2", "summary-3"),
}.items():
    source = raw_root / name
    source.mkdir(parents=True, exist_ok=True)
    for index, value in enumerate(values):
        (source / f"{index:04d}.txt").write_text(value, encoding="utf-8")
    plan = DatasetPreparer(target_shard_size_bytes=4096).plan(source, name, "files")
    DatasetConverter().execute(plan, mds_root / name)
PY
}

write_workload_evidence_proc_fixture() {
	local proc_root="$REPORT_DIR/workload-evidence-proc"
	printf '+ write %q\n' "$proc_root"
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	mkdir -p "$proc_root"
	{
		printf 'cpu 100 0 100 600 200 0 0 0 0 0\n'
	} >"$proc_root/stat"
	{
		printf '8 0 vda 10 0 1000 0 20 0 2000 0 0 1000 0\n'
	} >"$proc_root/diskstats"
	{
		printf 'MemTotal:       100000 kB\n'
		printf 'MemAvailable:   20000 kB\n'
		printf 'Cached:         15000 kB\n'
		printf 'SReclaimable:    5000 kB\n'
	} >"$proc_root/meminfo"
}

collect_workload_evidence_fixture() {
	local evidence_root="$REPORT_DIR/workload-evidence"
	write_workload_evidence_proc_fixture
	run_to_file "$REPORT_DIR/python-workload-evidence-help.txt" \
		env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.workload_evidence --help
	require_file_contains "$REPORT_DIR/python-workload-evidence-help.txt" "collect"
	run_to_file "$REPORT_DIR/python-workload-evidence.out" \
		env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.workload_evidence \
		collect \
		--output-dir "$evidence_root" \
		--proc-root "$REPORT_DIR/workload-evidence-proc" \
		--disk-device vda \
		--sample-seconds 0
	require_file_contains "$REPORT_DIR/python-workload-evidence.out" "cxnfs.workload-evidence.collect.v1"
	require_file_contains "$REPORT_DIR/python-workload-evidence.out" "status=collected"
	require_file_contains "$evidence_root/disk.report" "cxnfs.server.disk.v1"
	require_file_contains "$evidence_root/disk.report" "sample_start_unix_seconds="
	require_file_contains "$evidence_root/disk.report" "sample_end_unix_seconds="
	require_file_contains "$evidence_root/cache.json" "cxnfs.server.cache.v1"
	require_file_contains "$evidence_root/cache.json" '"sample_start_unix_seconds"'
	require_file_contains "$evidence_root/cache.json" '"sample_end_unix_seconds"'
	require_file_contains "$evidence_root/cpu.report" "cxnfs.server.cpu.v1"
	require_file_contains "$evidence_root/cpu.report" "sample_start_unix_seconds="
	require_file_contains "$evidence_root/cpu.report" "sample_end_unix_seconds="
}

check_python_quic_prerequisites() {
	printf '+ check Python QUIC optional dependency\n'
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	if env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c 'import importlib.util, sys; sys.exit(0 if importlib.util.find_spec("aioquic") else 1)'; then
		return
	fi
	if [ "$REQUIRE_QUIC_INTEGRATION" = "1" ]; then
		printf 'Python QUIC integration requires aioquic. Install with: %s -m pip install -e "%s/python[quic]"\n' "$PYTHON_BIN" "$ROOT_DIR" >&2
		printf 'If dependencies are staged outside the interpreter site-packages, set CXNFS_PYTHONPATH_EXTRA.\n' >&2
		exit 1
	fi
}

write_mount_handoff_fixture() {
	local handoff_file="$REPORT_DIR/cxnfs-mount.handoff"
	printf '+ write %q\n' "$handoff_file"
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	{
		printf 'cxnfs.mount.handoff.v1\n'
		printf 'host=cxnfs.example.internal\n'
		printf 'control_port=4509\n'
		printf 'transport_port=4510\n'
		printf 'requested_backend=macos_fuse_t\n'
		printf 'requested_capabilities=list,read\n'
		printf 'backend=macos_fuse_t\n'
		printf 'capabilities=list,read,write\n'
		printf 'adapter_kind=fuse\n'
		printf 'adapter_binary=cxnfs-fuse-t\n'
		printf 'adapter_argument_style=handoff-file\n'
		printf 'remote_root=/datasets/train\n'
		printf 'mount_point=/Volumes/CXNFS\n'
		printf 'initial_cords=8\n'
		printf 'max_cords=128\n'
		printf 'provisional_container_size_bytes=67108864\n'
		printf 'expected_size_limit_bytes=0\n'
		printf 'connect_timeout_ms=15000\n'
		printf 'read_timeout_ms=120000\n'
		printf 'truststore_path=\n'
		printf 'truststore_password_inline=false\n'
		printf 'auth_password_inline=false\n'
	} >"$handoff_file"
}

write_blockfs_metadata_fixture() {
	local metadata_root="$REPORT_DIR/blockfs-metadata-root"
	local hash_one="0100000000000000000000000000000000000000000000000000000000000000"
	local hash_file="0900000000000000000000000000000000000000000000000000000000000000"
	printf '+ write %q\n' "$metadata_root"
	if [ "$DRY_RUN" = "1" ]; then
		return
	fi
	mkdir -p "$metadata_root/namespace" "$metadata_root/manifest-index" "$metadata_root/payload-map"
	{
		printf 'version=1\n'
		printf 'generation=12\n'
		printf 'file.count=1\n'
		printf 'file.0.inode=2\n'
		printf 'file.0.path=datasets/train/sample.bin\n'
		printf 'file.0.size=4096\n'
		printf 'file.0.mode=0444\n'
	} >"$metadata_root/namespace/current.cxn"
	{
		printf 'version=1\n'
		printf 'generation=12\n'
		printf 'file.count=1\n'
		printf 'file.0.inode=2\n'
		printf 'file.0.path=datasets/train/sample.bin\n'
		printf 'file.0.size=4096\n'
		printf 'file.0.file-hash=%s\n' "$hash_file"
		printf 'file.0.chunk.count=1\n'
		printf 'file.0.chunk.0.file-offset=0\n'
		printf 'file.0.chunk.0.length=4096\n'
		printf 'file.0.chunk.0.hash=%s\n' "$hash_one"
	} >"$metadata_root/manifest-index/current.cxn"
	{
		printf 'version=1\n'
		printf 'generation=12\n'
		printf 'allocation.count=1\n'
		printf 'allocation.0.inode=2\n'
		printf 'allocation.0.file-offset=0\n'
		printf 'allocation.0.payload-block=1\n'
		printf 'allocation.0.payload-offset=0\n'
		printf 'allocation.0.payload-length=4096\n'
	} >"$metadata_root/payload-map/current.cxn"
}

cd "$ROOT_DIR"
prepare_report_dir

if [ "$SKIP_JAVA_TESTS" != "1" ]; then
	run "$MVN_BIN" -q test
fi
run "$MVN_BIN" -q -DskipTests package
if [ "$SKIP_QUIC_INTEGRATION" != "1" ]; then
	run "$MVN_BIN" -q dependency:build-classpath "-Dmdep.outputFile=$ROOT_DIR/target/cxnfs-runtime-classpath.txt"
fi
write_health_fixture
run "$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	server check-health "$REPORT_DIR/server.health.fixture" \
	--max-age-ms 60000 \
	--report "$REPORT_DIR/server-health-check.txt"
run_to_file "$REPORT_DIR/server-health-metrics.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	server health-metrics "$REPORT_DIR/server.health.fixture" \
	--max-age-ms 60000 \
	--report "$REPORT_DIR/server-health-metrics.prom"
require_file_contains "$REPORT_DIR/server-health-metrics.out" "cxnfs_server_health_check_passed 1"
require_file_contains "$REPORT_DIR/server-health-check.txt" "bulk_cipher_suite=tls_aes_128_gcm_sha256"
require_file_contains "$REPORT_DIR/server-health-check.txt" "bulk_total_connections=0"
require_file_contains "$REPORT_DIR/server-health-check.txt" "bulk_total_streams=0"
require_file_contains "$REPORT_DIR/server-health-metrics.prom" "cxnfs_server_health_info"
require_file_contains "$REPORT_DIR/server-health-metrics.prom" "bulk_cipher_suite=\"tls_aes_128_gcm_sha256\""
require_file_contains "$REPORT_DIR/server-health-metrics.prom" "cxnfs_server_bulk_total_connections 0"
require_file_contains "$REPORT_DIR/server-health-metrics.prom" "cxnfs_server_bulk_total_streams 0"
write_mount_handoff_fixture
run_to_file "$REPORT_DIR/mount-launch-plan.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	client mount-launch-plan "$REPORT_DIR/cxnfs-mount.handoff" \
	--adapter-binary /opt/cxnfs/bin/cxnfs-fuse-t \
	--report "$REPORT_DIR/mount-launch-plan.txt"
require_file_contains "$REPORT_DIR/mount-launch-plan.out" "cxnfs.mount.launch-plan.v1"
require_file_contains "$REPORT_DIR/mount-launch-plan.txt" "argv_1=--handoff"
require_file_contains "$REPORT_DIR/mount-launch-plan.txt" "auth_password_inline=false"
run_to_file "$REPORT_DIR/mount-list-help.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	client help
require_file_contains "$REPORT_DIR/mount-list-help.out" "cxnfs client mount-list <handoff-report>"
write_benchmark_fixture
run_to_file "$REPORT_DIR/java-benchmark-read.txt" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.CxnfsBenchmark \
	read "$REPORT_DIR/benchmark.fixture" \
	--label release-benchmark \
	--iterations 1 \
	--warmup 0 \
	--block-size 64
require_file_contains "$REPORT_DIR/java-benchmark-read.txt" "release-benchmark"
require_file_contains "$REPORT_DIR/java-benchmark-read.txt" "bytes="
run_to_file "$REPORT_DIR/java-benchmark-compare.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.CxnfsBenchmark \
	compare \
	--baseline "$REPORT_DIR/benchmark.fixture" \
	--candidate "$REPORT_DIR/benchmark.fixture" \
	--baseline-label release-baseline \
	--candidate-label release-candidate \
	--iterations 1 \
	--warmup 0 \
	--block-size 64 \
	--objective ai-training-25gbe \
	--fail-on-miss false \
	--report "$REPORT_DIR/java-benchmark-compare.txt"
require_file_contains "$REPORT_DIR/java-benchmark-compare.out" "candidate_vs_baseline"
require_file_contains "$REPORT_DIR/java-benchmark-compare.out" "objective=ai-training-25gbe"
require_file_contains "$REPORT_DIR/java-benchmark-compare.txt" "cxnfs.benchmark.compare.v1"
require_file_contains "$REPORT_DIR/java-benchmark-compare.txt" "objective=ai-training-25gbe"
require_file_contains "$REPORT_DIR/java-benchmark-compare.txt" "objective_baseline=legacy-mounted-share"
require_file_contains "$REPORT_DIR/java-benchmark-compare.txt" "objective_passed="
write_blockfs_metadata_fixture
run_to_file "$REPORT_DIR/blockfs-extent-hints.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	server build-blockfs-extent-hints "$REPORT_DIR/blockfs-metadata-root" \
	--require-complete true \
	--report "$REPORT_DIR/blockfs-extent-hints.txt"
require_file_contains "$REPORT_DIR/blockfs-extent-hints.out" "status=built"
require_file_contains "$REPORT_DIR/blockfs-extent-hints.txt" "cxnfs.blockfs.extent-hints.v1"
require_file_contains "$REPORT_DIR/blockfs-extent-hints.txt" "status=built"
run_to_file "$REPORT_DIR/blockfs-metadata-audit.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	server audit-blockfs-metadata "$REPORT_DIR/blockfs-metadata-root" \
	--report "$REPORT_DIR/blockfs-metadata-audit.txt"
require_file_contains "$REPORT_DIR/blockfs-metadata-audit.out" "status=clean"
require_file_contains "$REPORT_DIR/blockfs-metadata-audit.txt" "cxnfs.blockfs.metadata-audit.v1"
require_file_contains "$REPORT_DIR/blockfs-metadata-audit.txt" "status=clean"
run_to_file "$REPORT_DIR/blockfs-namespace-sync.out" \
	"$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	server sync-blockfs-namespace "$REPORT_DIR/blockfs-metadata-root" \
	--ctl /bin/echo \
	--device /dev/cxnfs_blockfs_ctl \
	--ctl-timeout-ms 5000 \
	--report "$REPORT_DIR/blockfs-namespace-sync.txt"
require_file_contains "$REPORT_DIR/blockfs-namespace-sync.out" "status=synced"
require_file_contains "$REPORT_DIR/blockfs-namespace-sync.txt" "cxnfs.blockfs.namespace-sync.v1"
require_file_contains "$REPORT_DIR/blockfs-namespace-sync.txt" "status=synced"
run "$JAVA_BIN" -cp "$ROOT_DIR/target/classes" com.fortrow.cxnfs.Cxnfs \
	protocol fixture-report "$ROOT_DIR/protocol-fixtures" \
	--report "$REPORT_DIR/java-protocol-fixtures.txt" \
	--fail-on-missing true

if [ "$SKIP_PYTHON_TESTS" != "1" ]; then
	run env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m unittest discover -s "$ROOT_DIR/python/tests"
fi
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-client = "cxnfs.client_cli:main"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-health = "cxnfs.health:main"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-acceptance-fixture = "cxnfs.torch_dataset:main_trainer_acceptance_fixture"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-handoff-acceptance = "cxnfs.torch_dataset:main_trainer_handoff_acceptance"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-preflight = "cxnfs.torch_dataset:main_trainer_preflight"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-network-preflight = "cxnfs.torch_dataset:main_trainer_network_preflight"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-distributed-plan = "cxnfs.torch_dataset:main_trainer_distributed_plan"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-integration-smoke = "cxnfs.torch_dataset:main_trainer_integration_smoke"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-runtime-smoke = "cxnfs.torch_dataset:main_trainer_runtime_smoke"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-distributed-runtime-smoke = "cxnfs.torch_dataset:main_trainer_distributed_runtime_smoke"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-readiness-bundle = "cxnfs.torch_dataset:main_trainer_readiness_bundle"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-loop-contract-check = "cxnfs.torch_dataset:main_trainer_loop_contract_check"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-loop-replay = "cxnfs.torch_dataset:main_trainer_loop_replay"'
require_file_contains "$ROOT_DIR/python/pyproject.toml" 'cxnfs-trainer-pipeline-smoke = "cxnfs.torch_dataset:main_trainer_pipeline_smoke"'
collect_workload_evidence_fixture
if [ "$SKIP_QUIC_INTEGRATION" != "1" ]; then
	check_python_quic_prerequisites
	run env PYTHONPATH="$PYTHON_RUN_PATH" CXNFS_REQUIRE_QUIC_INTEGRATION="$REQUIRE_QUIC_INTEGRATION" \
		CXNFS_QUIC_INTEGRATION_REPORT_DIR="$REPORT_DIR" \
		"$PYTHON_BIN" -m unittest -v python.tests.test_quic_java_integration
	if [ "$REQUIRE_QUIC_INTEGRATION" = "1" ]; then
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"format": "cxnfs.quic-high-cord-read.v1"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"bytes_read": 9437184'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"max_observed_cords": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"last_read_stream_count": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"read_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"read_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"direct_destination_frame_sink_cord_fanout_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"direct_destination_max_observed_cords": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"direct_destination_parallel_frame_sink_read_operations": 1'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"last_accepted_initial_cords": 1'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"last_accepted_max_cords": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-read.json" '"observed_cords_within_admission": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"format": "cxnfs.quic-batch-direct-read.v1"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"tls_version": "TLSv1.3"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"batch_read_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"batch_worker_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"ordered_batch_read_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"direct_buffer_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"direct_destination_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"direct_destination_frame_sink_cord_fanout_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"direct_destination_max_observed_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"last_accepted_initial_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"last_accepted_max_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"observed_cords_within_admission": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"successful_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"failed_read_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"read_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"read_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-read.json" '"connection_reused_for_streams": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"format": "cxnfs.quic-batch-direct-tensor-read.v1"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"tls_version": "TLSv1.3"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"tensor_target": "pytorch-cpu"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"tensor_dtype": "uint8"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"tensor_read_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"tensor_worker_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"ordered_tensor_read_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"direct_buffer_read_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"direct_tensor_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"direct_destination_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"format": "cxnfs.trainer-tensor-memory-path.v1"'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"python_byte_batch_avoided": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"network_fills_tensor_destination": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"device_copy_required": false'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"direct_destination_frame_sink_cord_fanout_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"direct_destination_max_observed_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"last_accepted_initial_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"last_accepted_max_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"observed_cords_within_admission": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"successful_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"failed_read_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"read_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"read_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" '"connection_reused_for_streams": true'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"format": "cxnfs.quic-read-retry.v1"'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"tls_version": "TLSv1.3"'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"interruption_injected": true'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"payload_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"read_retry_limit": 1'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"read_retry_attempts": 1'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"read_retry_successes": 1'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"bulk_transport_reconnects": 1'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"read_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"read_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"failed_read_operations": 1'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"failed_direct_buffer_read_operations": 1'
		require_file_contains "$REPORT_DIR/python-quic-read-retry.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"format": "cxnfs.python-client-cli-quic-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"read_max_observed_cords": 4'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"write_max_observed_cords": 4'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"read_back_verified": true'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"direct_file_write_verified": true'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"direct_write_max_observed_cords": 4'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"raw_password_visible": false'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"write_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"direct_write_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"server_bulk_listener": "quic-tls13-udp://'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"server_bulk_cipher_suite": "tls_aes_128_gcm_sha256"'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"server_bulk_total_streams":'
		require_file_contains "$REPORT_DIR/python-client-cli-quic-smoke.json" '"server_bulk_total_streams_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"format": "cxnfs.quic-high-cord-write.v1"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"bytes_written": 262144'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_frames": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_acks": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_frame_verified": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"max_observed_write_cords": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"bulk_transport_write_evidence"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"format": "cxnfs.quic-bulk-write-transport-evidence.v1"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"tls_version": "TLSv1.3"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"connection_opened_count": 1'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"successful_write_operations": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"failed_write_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_streams_created": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_acks_received": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_acks_verified": 64'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_verification_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"active_write_streams": 0'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"max_active_write_streams":'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"connection_reused_for_streams": true'
		require_file_contains "$REPORT_DIR/python-quic-high-cord-write.json" '"read_back_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"format": "cxnfs.quic-write-retry.v1"'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"interruption_injected": true'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"payload_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"write_retry_limit": 1'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"write_retry_attempts": 1'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"write_retry_successes": 1'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"bulk_transport_write_reconnects": 1'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"ack_metadata_verified": false'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"successful_write_operations": 1'
		require_file_contains "$REPORT_DIR/python-quic-write-retry.json" '"failed_write_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"format": "cxnfs.quic-duplicate-write-retry.v1"'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"duplicate_frame_sent": true'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"first_ack_metadata_verified": false'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"duplicate_ack_metadata_verified": false'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"read_back_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"successful_write_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-duplicate-write-retry.json" '"failed_write_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"format": "cxnfs.quic-bad-late-metadata.v1"'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"ack_metadata_verified": false'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"metadata_rejected": true'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"metadata_error_status": "CONFLICT"'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"commit_rejected": true'
		require_file_contains "$REPORT_DIR/python-quic-bad-late-metadata.json" '"published": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"read_mode": "direct-buffers"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"trainer_prefetch_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"trainer_prefetch_stream_object": "CxnfsTrainerPrefetchStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"read_path_evidence"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-stream-benchmark.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-stream-benchmark.json" '"max_observed_cords": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-pipeline-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"read_mode": "direct-tensors"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"training_pipeline_context": "CxnfsTrainingPipeline.prefetch_trainer_stream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"pipeline_prefetch_stream_used": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"pipeline_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"pipeline_descriptor_planning_used": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"pipeline_python_byte_batch_avoided": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-tensor-memory-path.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"quic_read_transport_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"quic_read_transport_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-pipeline-write-probe.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"verified_read_back": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"byte_size": 4096'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"frame_bytes": 1024'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"format": "cxnfs.bulk-write-path-evidence.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"direct_tensor_write_operations": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"max_observed_write_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_frames": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_acks": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_frame_verified": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_metadata_verified": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_metadata_pending": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"metadata_after_payload_entries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"entries_committed": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"transfers_committed": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"format": "cxnfs.quic-bulk-write-transport-evidence.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"connection_reused_for_streams": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_acks_verified": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_verification_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-pipeline-smoke.json" '"write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"format": "cxnfs.trainer-stream-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"trainer_batch_iterator": "trainer-dataloader"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"data_loader_workers": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"worker_batch_read_evidence_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"worker_batch_read_evidence_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"produced_inside_torch_worker": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"worker_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"worker_sharding_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"duplicate_source_sample_count": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-workers.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-workers.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"format": "cxnfs.quic-trainer-resume.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"trainer_prefetch_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"read_path_reliability_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"cursor_format": "cxnfs.trainer-cursor.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"state_file_written": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"state_file_replayed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"state_stream_state_matches_final_cursor": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"first_final_emitted": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"second_initial_emitted": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"second_final_emitted": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"direct_destination_frame_sink_cord_fanout_verified":'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"direct_destination_max_observed_cords":'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"direct_destination_parallel_frame_sink_read_operations":'
		require_file_contains "$REPORT_DIR/python-quic-trainer-resume.json" '"direct_destination_parallel_frame_sink_frames":'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"format": "cxnfs.trainer-surface-matrix.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"read_path_reliability_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"worker_partition_required": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"worker_partition_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"format": "cxnfs.trainer-worker-partition.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"case_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"case_name": "direct-buffers/native"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"case_name": "direct-buffers/trainer-dataloader"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"case_name": "direct-tensors/native"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"case_name": "direct-tensors/trainer-dataloader"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"target": "pytorch-cpu"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"format": "cxnfs.trainer-tensor-memory-path.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"tensor_memory_path_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"python_byte_batch_avoided": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"network_fills_tensor_destination": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"device_copy_required": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-surface-matrix.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"max_observed_cords": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"format": "cxnfs.trainer-remote-client-topology-aggregate.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"topology_report_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"per_source_client_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-surface-matrix.json" '"per_source_client_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-workload-benchmark.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"trainer_prefetch_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"bulk_write_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"run_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-remote-client-topology-aggregate.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"topology_report_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"all_reports_have_topology": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"per_source_client_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"remote_client_topology_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"workload_fingerprint"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"benchmark_provenance"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"resume_cursor_evidence"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"all_runs_have_resume_cursors": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"all_final_cursors_advance_by_sample_count": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"read_path_evidence"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"read_path_reliability_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-workload-benchmark.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"max_observed_cords": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"blockfs_gate"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"read_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"memory_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_probe_cord_sweep_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"max_observed_write_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_retry_limit": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_retry_attempts": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_retry_successes": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"bulk_transport_write_reconnects": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_retry_attempts_recovered": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_retry_reconnects_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_evidence_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_connection_reuse_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_successful_write_operations": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_failed_write_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_write_streams_created": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_write_acks_received": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_write_acks_verified": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_write_ack_verification_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_all_reports_write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_all_reports_write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_all_reports_write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"quic_write_transport_max_active_write_streams": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-workload-pressure.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"objective_present": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"java_python_network_path_exercised": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"decision_ready": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-workload-benchmark.json" '"kernel_module_allowed": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-workload-benchmark.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"contract_applied": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"trainer_prefetch_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"bulk_write_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"trainer_contract"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"workload_fingerprint"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"benchmark_provenance"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"resume_cursor_evidence"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"all_runs_have_resume_cursors": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"all_final_cursors_advance_by_sample_count": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"run_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-remote-client-topology-aggregate.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"topology_report_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"all_reports_have_topology": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"per_source_client_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"remote_client_topology_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"read_path_evidence"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"read_path_reliability_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"max_observed_cords": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"read_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"memory_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_probe_cord_sweep_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"max_observed_write_cords": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_retry_limit": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_retry_attempts": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_retry_successes": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"bulk_transport_write_reconnects": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_retry_attempts_recovered": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_retry_reconnects_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_evidence_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_connection_reuse_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_successful_write_operations": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_failed_write_operations": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_write_streams_created": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_write_acks_received": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_write_acks_verified": 16'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_write_ack_verification_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_all_reports_write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_all_reports_write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_all_reports_write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"quic_write_transport_max_active_write_streams": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-workload-pressure.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"objective_present": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"java_python_network_path_exercised": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" '"decision_ready": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"format": "cxnfs.trainer-integration-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"plan_format": "cxnfs.trainer-integration-plan.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"stream_factory": "open_weighted_mds_trainer_stream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"stream_opened": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"stream_kwargs_accepted": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_root": "/stage0-mds"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"format": "cxnfs.trainer-remote-client-topology.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"per_source_client_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"batch_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"sample_count": 6'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"training_loop_context_used": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"training_loop_object": "CxnfsTrainerPrefetchStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"training_loop_base_stream_object": "CxnfsTrainerStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"training_loop_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"training_loop_prefetch_applied": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"trainer_batch_iterator": "native"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"data_loader_workers": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"worker_partition_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_warmup_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_warmup_source_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_warmup_warmed_source_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_topology_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_per_source_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"remote_client_lifecycle_closed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"stream_state_matches_final_cursor": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-integration-smoke.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-integration-smoke.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"format": "cxnfs.trainer-runtime-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"read_mode": "direct-tensors"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"pipeline_training_loop_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"pipeline_training_loop_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"pipeline_training_loop_context_used": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"pipeline_training_loop_object": "CxnfsTrainerPipelineLoop"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"stream_opened": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"stream_kwargs_accepted": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"training_loop_object": "CxnfsTrainerPrefetchStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"training_loop_base_stream_object": "CxnfsTrainerStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"raw_password_sent_over_control": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"trainer_batch_iterator": "native"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"data_loader_workers": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"worker_partition_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_warmup_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_warmup_source_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_warmup_warmed_source_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_topology_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_per_source_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"remote_client_lifecycle_closed": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-runtime-smoke.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-runtime-smoke.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"format": "cxnfs.trainer-distributed-runtime-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"world_size": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"rank_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"all_rank_streams_opened": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"all_rank_stream_kwargs_accepted": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"all_rank_state_matches_final_cursor": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"all_rank_training_shards_correct": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"no_cross_rank_overlap": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"all_open_clients_closed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"format": "cxnfs.trainer-distributed-runtime-rank-states.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"rank_state_file_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"all_rank_state_files_written": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"raw_password_sent_over_control": false'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-runtime-smoke.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"plan_format": "cxnfs.trainer-integration-plan.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"remote_root": "/stage0-mds"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"step_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"batch_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"sample_count": 6'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"weighted_source_coverage_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"resume_replayed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"resume_continuity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"training_loop_context_used": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"training_loop_object": "CxnfsTrainerPrefetchStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"training_loop_base_stream_object": "CxnfsTrainerStream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"training_loop_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"training_loop_prefetch_applied": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"stream_report"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-loop-replay.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-loop-replay.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"format": "cxnfs.trainer-readiness-bundle.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"ready_for_trainer_wiring": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"ready_for_remote_quic": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"requires_low_level_direct_read_proofs": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"low_level_direct_read_proofs_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"batch_direct_read_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"batch_direct_tensor_read_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"batch_direct_tensor_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"requires_low_level_direct_write_proofs": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"low_level_direct_write_proofs_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"write_probe_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"write_probe_quic_transport_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"direct_quic_memory_path_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"requires_credentials": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"direct_python_streaming_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"runtime_training_loop_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"pipeline_training_loop_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"distributed_runtime_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"distributed_rank_states_written": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"loop_resume_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"raw_password_sent_over_control": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"preflight_format": "cxnfs.trainer-integration-preflight.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"integration_smoke_format": "cxnfs.trainer-integration-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"distributed_runtime_smoke_format": "cxnfs.trainer-distributed-runtime-smoke.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"loop_replay_format": "cxnfs.trainer-loop-replay.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"artifact_count": 5'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"format": "cxnfs.trainer-distributed-runtime-rank-states.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"rank_state_file_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-readiness-bundle.json" '"all_rank_state_files_written": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"plan_format": "cxnfs.trainer-integration-plan.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"remote_root": "/stage0-mds"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"read_mode": "direct-buffers"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"trainer_batch_iterator": "trainer-dataloader"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"data_loader_workers": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"samples_per_epoch": 8'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"repeat": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"step_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"batch_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"sample_count": 8'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"final_emitted": 8'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"weighted_source_coverage_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"resume_replayed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"resume_continuity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"worker_partitioning_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"worker_cursor_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"format": "cxnfs.trainer-worker-partition.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"worker_sharding_enabled": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"duplicate_source_sample_count": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"worker_batch_read_evidence_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"produced_inside_torch_worker": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"transport_frame_sink_observed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"frame_sink_used_for_all_direct_destination_reads": true'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json"
		require_file_contains "$REPORT_DIR/python-quic-trainer-worker-loop-replay.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"format": "cxnfs.trainer-contract.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"bulk_write_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"supports_dataloader_worker_partitioning": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract.json" '"format": "cxnfs.trainer-worker-partition.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"format": "cxnfs.trainer-surface-matrix.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"trainer_prefetch_batches": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"bulk_read_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"worker_partition_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"format": "cxnfs.trainer-remote-client-topology-aggregate.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"topology_report_count": 4'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"per_source_client_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" '"case_name": "direct-tensors/trainer-dataloader"'
		require_trainer_direct_destination_fanout "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json"
		run_to_file "$REPORT_DIR/python-quic-workload-decision.json" \
			env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.workload_decision \
			--input-workload-report "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" \
			--input-matrix-report "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" \
			--server-disk-evidence "$REPORT_DIR/workload-evidence/disk.report" \
			--server-cache-evidence "$REPORT_DIR/workload-evidence/cache.json" \
			--server-cpu-evidence "$REPORT_DIR/workload-evidence/cpu.report" \
			--output-dir "$REPORT_DIR/quic-workload-decision" \
			--surface-contract "$REPORT_DIR/python-quic-trainer-contract.json" \
			--surface-require-remote \
			--surface-require-quic \
			--surface-require-matrix \
			--surface-min-sources 2 \
			--surface-min-validation-samples 6 \
			--surface-min-validation-bytes 786000 \
			--surface-min-workload-runs 4 \
			--surface-min-workload-samples 24 \
			--surface-min-workload-bytes 3145728 \
			--surface-min-cord-sweep 2
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"format": "cxnfs.workload-decision.v1"'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"contract_applied": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"trainer_contract"'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"trainer_surface_matrix_passed": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"trainer_surface_check_passed": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"read_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"read_path_reliability_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"unrecovered_read_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"memory_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"write_path_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"server_evidence_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"server_evidence_sample_window_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"pressure_profile"'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"pressure_profile_complete": false'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"cord_plateau_observed":'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"kernel_module_allowed": false'
		require_file_contains "$REPORT_DIR/python-quic-workload-decision.json" '"blockfs_handoff"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/blockfs-handoff.json" '"format": "cxnfs.blockfs-handoff.v1"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/blockfs-handoff.json" '"status": "deferred"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/blockfs-handoff.json" '"kernel_module_allowed": false'
		require_file_contains "$REPORT_DIR/quic-workload-decision/blockfs-handoff.json" '"linux_blockfs_candidate": null'
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "trainer_surface_matrix_passed=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "trainer_surface_check_passed=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "blockfs_handoff=$REPORT_DIR/quic-workload-decision/blockfs-handoff.json"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "pressure_profile_complete=false"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "java_python_network_path_exercised=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "read_path_evidence_complete=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "read_path_reliability_complete=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "unrecovered_read_failures=0"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "memory_path_evidence_complete=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "write_path_evidence_complete=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/decision.report" "server_evidence_sample_window_complete=true"
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-workload-benchmark.json" '"server_evidence"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-workload-benchmark.json" '"sample_window_complete": true'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-matrix.json" '"transport": "cxnfs-quic"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-matrix.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-matrix.json" '"case_name": "direct-tensors/trainer-dataloader"'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-check.json" '"require_remote": true'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-check.json" '"require_quic": true'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-check.json" '"require_matrix": true'
		require_file_contains "$REPORT_DIR/quic-workload-decision/trainer-surface-check.json" '"workload_bulk_write_retries_match_contract"'
		run_to_file "$REPORT_DIR/python-quic-trainer-handoff.json" \
			env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
			"from cxnfs.torch_dataset import run_trainer_handoff_cli; import sys; raise SystemExit(run_trainer_handoff_cli(sys.argv[1:]))" \
			"$REPORT_DIR/python-quic-trainer-contract.json" \
			--workload-report "$REPORT_DIR/python-quic-trainer-contract-workload-benchmark.json" \
			--matrix-report "$REPORT_DIR/python-quic-trainer-contract-surface-matrix.json" \
			--workload-decision "$REPORT_DIR/python-quic-workload-decision.json" \
			--blockfs-handoff "$REPORT_DIR/quic-workload-decision/blockfs-handoff.json" \
			--batch-direct-read-report "$REPORT_DIR/python-quic-batch-direct-read.json" \
			--batch-direct-tensor-read-report "$REPORT_DIR/python-quic-batch-direct-tensor-read.json" \
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
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"format": "cxnfs.trainer-handoff.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"ready_for_trainer_wiring": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"ready_for_remote_quic": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"ready_for_blockfs_candidate": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"python_client": "CxnfsWeightedTorchDataset"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"integration_plan"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"format": "cxnfs.trainer-integration-plan.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"stream_factory": "open_weighted_mds_trainer_stream"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"training_pipeline_context": "CxnfsTrainingPipeline.read_tensor_batch"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"supports_pipeline_direct_tensor_batches": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"supports_pipeline_torch_compile": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"remote_client_factory": "CxnfsTrainerIntegrationPlan.create_remote_client_factory"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"raw_password_sent_over_control": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"remote_client_topology_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"remote_client_per_source_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"bulk_write_retries": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"format": "cxnfs.trainer-low-level-direct-read-proofs.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"batch_direct_read"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"batch_direct_tensor_read"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"direct_buffer_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"direct_tensor_read_operations": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"network_fills_tensor_destination": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"format": "cxnfs.trainer-low-level-direct-write-proofs.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"low_level_direct_write_proofs"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"quic_write_transport_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"quic_write_transport_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"quic_write_transport_all_reports_write_ack_frame_identity_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"quic_write_transport_all_reports_write_ack_stream_completion_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"quic_write_transport_all_reports_write_ack_stream_eof_verified": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"quic_write_transport_write_ack_verification_failures": 0'
		require_file_contains "$REPORT_DIR/python-quic-trainer-handoff.json" '"metadata_pending_observed": true'
		run_to_file "$REPORT_DIR/python-quic-trainer-preflight.json" \
			env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
			"from cxnfs.torch_dataset import run_trainer_preflight_cli; import sys; raise SystemExit(run_trainer_preflight_cli(sys.argv[1:]))" \
			"$REPORT_DIR/python-quic-trainer-handoff.json" \
			--require-remote-quic
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"format": "cxnfs.trainer-integration-preflight.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"ready_for_trainer_wiring": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"ready_for_remote_quic": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"requires_credentials": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"opens_network_connections": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"consumes_batches": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"safe_to_run_before_trainer_workers": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"low_level_direct_read_proofs_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"batch_direct_read_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"batch_direct_tensor_read_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"batch_direct_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"batch_direct_tensor_read_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"low_level_direct_write_proofs_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"write_probe_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"write_probe_quic_transport_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"max_cords": 128'
		require_file_contains "$REPORT_DIR/python-quic-trainer-preflight.json" '"raw_password_sent_over_control": false'
		run_to_file "$REPORT_DIR/python-quic-trainer-distributed-plan.json" \
			env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
			"from cxnfs.torch_dataset import run_trainer_distributed_plan_cli; import sys; raise SystemExit(run_trainer_distributed_plan_cli(sys.argv[1:]))" \
			"$REPORT_DIR/python-quic-trainer-handoff.json" \
			--require-remote-quic \
			--world-size 2 \
			--rank 1
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"format": "cxnfs.trainer-distributed-plan.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"low_level_direct_read_proofs_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"batch_direct_read_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"batch_direct_tensor_read_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"low_level_direct_write_proofs_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"write_probe_proof_complete": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"write_probe_cipher_suite": "TLS_AES_128_GCM_SHA256"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"passed": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"ready_for_remote_quic": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"remote_client_required": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"requires_credentials": true'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"opens_network_connections": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"consumes_batches": false'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"world_size": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"rank_count": 2'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"selected_rank": 1'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"training_shard": {'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"bulk_transport": "quic"'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"max_cords": 128'
		require_file_contains "$REPORT_DIR/python-quic-trainer-distributed-plan.json" '"raw_password_sent_over_control": false'
	fi
fi
run_to_file "$REPORT_DIR/python-client-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.client_cli --help
require_file_contains "$REPORT_DIR/python-client-help.txt" "read-mds"
require_file_contains "$REPORT_DIR/python-client-help.txt" "send-checkpoint"
require_file_contains "$REPORT_DIR/python-client-help.txt" "send-file"
require_file_contains "$REPORT_DIR/python-client-help.txt" "--bulk-transport"
require_file_not_contains "$REPORT_DIR/python-client-help.txt" "--password"
run_to_file "$REPORT_DIR/python-benchmark-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.benchmark --help
require_file_contains "$REPORT_DIR/python-benchmark-help.txt" "--objective"
require_file_contains "$REPORT_DIR/python-benchmark-help.txt" "--baseline-mb-s"
require_file_contains "$REPORT_DIR/python-benchmark-help.txt" "--fail-on-miss"
require_file_contains "$REPORT_DIR/python-benchmark-help.txt" "--bulk-transport"
write_weighted_mds_fixture
run_to_file "$REPORT_DIR/python-trainer-contract-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_contract_cli; import sys; raise SystemExit(run_trainer_contract_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--remote-root"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--read-mode"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--trainer-batch-iterator"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--data-loader-workers"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--world-size"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--bulk-read-retries"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--bulk-write-retries"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--tensor-target"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--no-repeat"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--min-workload-runs"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--min-workload-samples"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--min-workload-bytes"
require_file_contains "$REPORT_DIR/python-trainer-contract-help.txt" "--min-cord-sweep"
run_to_file "$REPORT_DIR/python-trainer-contract.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_contract_cli; import sys; raise SystemExit(run_trainer_contract_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--read-mode direct-buffers \
	--batch-size 2 \
	--batches 2 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 2 \
	--bulk-read-retries 1 \
	--trainer-batch-iterator native \
	--data-loader-workers 0 \
	--min-workload-runs 4 \
	--min-workload-samples 12 \
	--min-workload-bytes 84 \
	--min-cord-sweep 2 \
	--samples-per-epoch 3
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"format": "cxnfs.trainer-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"requires_mount": false'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"touches_trainer_code": false'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_direct_buffers": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_trainer_dataloader": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_dataloader_worker_partitioning": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_trainer_stream": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_training_pipeline": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_pipeline_direct_tensor_batches": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"supports_pipeline_torch_compile": true'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"pipeline_cpu_augmentation_default": "disabled-with-explicit-opt-in"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"dataloader_batch_object": "CxnfsTrainerBatch"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"raw_password_sent_over_control": false'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"max_cords": 128'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"trainer_batch_iterator": "native"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"data_loader_workers": 0'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"format": "cxnfs.trainer-worker-partition.v1"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"enabled": false'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"trainer_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"bulk_read_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"bulk_write_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"workload_requirements"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"min_workload_samples": 12'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"workload_fingerprint"'
require_file_contains "$REPORT_DIR/python-trainer-contract.json" '"validation"'
run_to_file "$REPORT_DIR/python-trainer-surface-check-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_surface_check_cli; import sys; raise SystemExit(run_trainer_surface_check_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-surface-check-help.txt" "--require-quic"
require_file_contains "$REPORT_DIR/python-trainer-surface-check-help.txt" "--workload-report"
require_file_contains "$REPORT_DIR/python-trainer-surface-check-help.txt" "--matrix-report"
require_file_contains "$REPORT_DIR/python-trainer-surface-check-help.txt" "--min-workload-samples"
require_file_contains "$REPORT_DIR/python-trainer-surface-check-help.txt" "--require-matrix"
run_to_file "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_contract_benchmark_cli; import sys; raise SystemExit(run_trainer_contract_benchmark_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--cord-sweep"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--server-disk-evidence"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--trainer-batch-iterator"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--bulk-read-retries"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--bulk-write-retries"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--tensor-target"
require_file_contains "$REPORT_DIR/python-trainer-contract-benchmark-help.txt" "--write-probe-frame-bytes"
run_to_file "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_contract_benchmark_cli; import sys; raise SystemExit(run_trainer_contract_benchmark_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-contract.json" \
	--warmup-batches 1 \
	--runs 2 \
	--cord-sweep 1,4 \
	--server-disk-evidence "$REPORT_DIR/workload-evidence/disk.report" \
	--server-cache-evidence "$REPORT_DIR/workload-evidence/cache.json" \
	--server-cpu-evidence "$REPORT_DIR/workload-evidence/cpu.report"
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-workload-benchmark.v1"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"contract_applied": true'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"trainer_contract"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"workload_fingerprint"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"trainer_batch_iterator": "native"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"trainer_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"bulk_read_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"bulk_write_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"read_path_reliability_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"unrecovered_read_failures": 0'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"resume_cursor_evidence"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"all_runs_have_resume_cursors": true'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"all_final_cursors_advance_by_sample_count": true'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"blockfs_workload_requirements"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"min_samples": 12'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"run_count": 4'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"memory_path_evidence_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"format": "cxnfs.trainer-workload-pressure.v1"'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"objective_present": false'
require_file_contains "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" '"decision_ready": false'
run_to_file "$REPORT_DIR/python-trainer-surface-check.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_surface_check_cli; import sys; raise SystemExit(run_trainer_surface_check_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-contract.json" \
	--workload-report "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" \
	--require-workload \
	--min-sources 2 \
	--min-validation-samples 3 \
	--min-validation-bytes 21 \
	--min-workload-runs 4 \
	--min-workload-samples 12 \
	--min-workload-bytes 84 \
	--min-cord-sweep 2
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"format": "cxnfs.trainer-surface-check.v1"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"workload_fingerprint_matches_contract"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"workload_trainer_batch_iterator_matches_contract"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"workload_data_loader_workers_match_contract"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"workload_bulk_read_retries_match_contract"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"workload_bulk_write_retries_match_contract"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"workload_memory_path"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check.json" '"min_workload_samples": 12'
run_to_file "$REPORT_DIR/python-trainer-surface-matrix-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_surface_matrix_cli; import sys; raise SystemExit(run_trainer_surface_matrix_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix-help.txt" "--read-modes"
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix-help.txt" "--trainer-batch-iterators"
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix-help.txt" "--bulk-read-retries"
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix-help.txt" "--world-size"
run_to_file "$REPORT_DIR/python-trainer-surface-matrix.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_surface_matrix_cli; import sys; raise SystemExit(run_trainer_surface_matrix_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--read-modes direct-buffers,direct-tensors \
	--trainer-batch-iterators native,trainer-dataloader \
	--data-loader-workers 0 \
	--tensor-target pytorch-cpu \
	--batch-size 3 \
	--batches 1 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 2 \
	--bulk-read-retries 1 \
	--samples-per-epoch 3
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"format": "cxnfs.trainer-surface-matrix.v1"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"trainer_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"bulk_read_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"training_shard"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"rank": 0'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"world_size": 1'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"read_path_reliability_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"worker_partition_required": false'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"worker_partition_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"unrecovered_read_failures": 0'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"format": "cxnfs.trainer-worker-partition.v1"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"case_name": "direct-buffers/native"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"case_name": "direct-buffers/trainer-dataloader"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"case_name": "direct-tensors/native"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"case_name": "direct-tensors/trainer-dataloader"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"memory_path_gate"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"format": "cxnfs.trainer-tensor-memory-path.v1"'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"tensor_memory_path_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"python_byte_batch_avoided": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"network_fills_tensor_destination": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-matrix.json" '"device_copy_required": false'
run_to_file "$REPORT_DIR/python-trainer-surface-check-matrix.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_surface_check_cli; import sys; raise SystemExit(run_trainer_surface_check_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-contract.json" \
	--workload-report "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" \
	--matrix-report "$REPORT_DIR/python-trainer-surface-matrix.json" \
	--require-workload \
	--require-matrix \
	--min-sources 2 \
	--min-validation-samples 3 \
	--min-validation-bytes 21 \
	--min-workload-runs 4 \
	--min-workload-samples 12 \
	--min-workload-bytes 84 \
	--min-cord-sweep 2
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"format": "cxnfs.trainer-surface-check.v1"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"require_matrix": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"matrix_direct_tensors_dataloader_present"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"matrix_fingerprint_matches_contract"'
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"memory_paths_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-surface-check-matrix.json" '"read_path_reliability_complete": true'
run_to_file "$REPORT_DIR/python-trainer-handoff-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_handoff_cli; import sys; raise SystemExit(run_trainer_handoff_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-handoff-help.txt" "--workload-report"
require_file_contains "$REPORT_DIR/python-trainer-handoff-help.txt" "--matrix-report"
require_file_contains "$REPORT_DIR/python-trainer-handoff-help.txt" "--workload-decision"
require_file_contains "$REPORT_DIR/python-trainer-handoff-help.txt" "--require-low-level-direct-write-proofs"
require_file_contains "$REPORT_DIR/python-trainer-handoff-help.txt" "--blockfs-handoff"
run_to_file "$REPORT_DIR/python-trainer-handoff.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_handoff_cli; import sys; raise SystemExit(run_trainer_handoff_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-contract.json" \
	--workload-report "$REPORT_DIR/python-trainer-contract-workload-benchmark.json" \
	--matrix-report "$REPORT_DIR/python-trainer-surface-matrix.json" \
	--require-workload \
	--require-matrix \
	--min-sources 2 \
	--min-validation-samples 3 \
	--min-validation-bytes 21 \
	--min-workload-runs 4 \
	--min-workload-samples 12 \
	--min-workload-bytes 84 \
	--min-cord-sweep 2 \
	--surface-check-json "$REPORT_DIR/python-trainer-handoff-surface-check.json"
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"ready_for_blockfs_candidate": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"requires_mount": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"touches_trainer_code": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"resume_cursor_format": "cxnfs.trainer-cursor.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"stream_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"integration_plan"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"format": "cxnfs.trainer-integration-plan.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"stream_factory": "open_weighted_mds_trainer_stream"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"supports_training_loop_prefetch": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"training_pipeline_context": "CxnfsTrainingPipeline.read_tensor_batch"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"supports_pipeline_direct_tensor_batches": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"supports_pipeline_gpu_transforms": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"supports_pipeline_torch_compile": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"supports_resume_cursor": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"supports_resume_state_file": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"resume_state_format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff.json" '"bulk_write_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-handoff-surface-check.json" '"format": "cxnfs.trainer-surface-check.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-surface-check.json" '"passed": true'
run_to_file "$REPORT_DIR/python-trainer-preflight-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_preflight_cli; import sys; raise SystemExit(run_trainer_preflight_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-preflight-help.txt" "--require-remote-quic"
require_file_contains "$REPORT_DIR/python-trainer-preflight-help.txt" "--output"
require_file_not_contains "$REPORT_DIR/python-trainer-preflight-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-network-preflight-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_network_preflight_cli; import sys; raise SystemExit(run_trainer_network_preflight_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-network-preflight-help.txt" "--bulk-transport"
require_file_contains "$REPORT_DIR/python-trainer-network-preflight-help.txt" "--read-path"
require_file_contains "$REPORT_DIR/python-trainer-network-preflight-help.txt" "--require-remote-quic"
require_file_not_contains "$REPORT_DIR/python-trainer-network-preflight-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-preflight.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_preflight_cli; import sys; raise SystemExit(run_trainer_preflight_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json"
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"format": "cxnfs.trainer-integration-preflight.v1"'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"requires_credentials": false'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"opens_network_connections": false'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"consumes_batches": false'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"safe_to_run_before_trainer_workers": true'
require_file_contains "$REPORT_DIR/python-trainer-preflight.json" '"raw_password_sent_over_control": false'
run_to_file "$REPORT_DIR/python-trainer-distributed-plan-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_distributed_plan_cli; import sys; raise SystemExit(run_trainer_distributed_plan_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan-help.txt" "--world-size"
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan-help.txt" "--require-remote-quic"
require_file_not_contains "$REPORT_DIR/python-trainer-distributed-plan-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-distributed-plan.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_distributed_plan_cli; import sys; raise SystemExit(run_trainer_distributed_plan_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--world-size 2 \
	--rank 1
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"format": "cxnfs.trainer-distributed-plan.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"requires_credentials": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"opens_network_connections": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"consumes_batches": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"world_size": 2'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"rank_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"selected_rank": 1'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"training_shard": {'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-plan.json" '"raw_password_sent_over_control": false'
run_to_file "$REPORT_DIR/python-trainer-integration-smoke-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_integration_smoke_cli; import sys; raise SystemExit(run_trainer_integration_smoke_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--batches"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--host"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--control-port"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--transport-port"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--bulk-transport"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--bulk-read-retries"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--bulk-write-retries"
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--shared-client"
require_file_not_contains "$REPORT_DIR/python-trainer-integration-smoke-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-integration-smoke.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_integration_smoke_cli; import sys; raise SystemExit(run_trainer_integration_smoke_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--batches 2
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"format": "cxnfs.trainer-integration-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"plan_format": "cxnfs.trainer-integration-plan.v1"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"stream_factory": "open_weighted_mds_trainer_stream"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"stream_opened": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"stream_kwargs_accepted": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"training_loop_context_used": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"training_loop_object": "CxnfsTrainerPrefetchStream"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"training_loop_base_stream_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"training_loop_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"training_loop_prefetch_applied": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"requires_mount": false'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"touches_trainer_code": false'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"stream_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"batch_object": "CxnfsTrainerBatch"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"trainer_batch_iterator": "native"'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"data_loader_workers": 0'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"worker_partition_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_warmup_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_warmup_source_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_warmup_warmed_source_count": 0'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_topology_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_per_source_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"remote_client_lifecycle_closed": true'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"batch_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-integration-smoke.json" '"stream_state_matches_final_cursor": true'
run_to_file "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_runtime_smoke_cli; import sys; raise SystemExit(run_trainer_runtime_smoke_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--batches"
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--world-size"
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--require-remote-quic"
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--trainer-prefetch-batches"
require_file_not_contains "$REPORT_DIR/python-trainer-runtime-smoke-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-runtime-smoke.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_runtime_smoke_cli; import sys; raise SystemExit(run_trainer_runtime_smoke_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--rank 0 \
	--world-size 1 \
	--batches 1 \
	--trainer-prefetch-batches 0
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"format": "cxnfs.trainer-runtime-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"plan_format": "cxnfs.trainer-integration-plan.v1"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"training_loop_context_used": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"training_loop_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"training_loop_base_stream_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"requires_credentials": false'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"stream_opened": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"stream_kwargs_accepted": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"trainer_batch_iterator": "native"'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"data_loader_workers": 0'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"worker_partition_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_warmup_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_warmup_source_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_warmup_warmed_source_count": 0'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_topology_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_per_source_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"remote_client_lifecycle_closed": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"batch_count": 1'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"stream_state_matches_final_cursor": true'
require_file_contains "$REPORT_DIR/python-trainer-runtime-smoke.json" '"raw_password_sent_over_control": false'
run_to_file "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_distributed_runtime_smoke_cli; import sys; raise SystemExit(run_trainer_distributed_runtime_smoke_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--world-size"
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--batches-per-rank"
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--require-remote-quic"
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--write-rank-states-dir"
require_file_not_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_distributed_runtime_smoke_cli; import sys; raise SystemExit(run_trainer_distributed_runtime_smoke_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--world-size 2 \
	--batches-per-rank 1 \
	--trainer-prefetch-batches 0 \
	--write-rank-states-dir "$REPORT_DIR/python-trainer-distributed-runtime-rank-states"
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"format": "cxnfs.trainer-distributed-runtime-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"distributed_plan_format": "cxnfs.trainer-distributed-plan.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"runtime_smoke_format": "cxnfs.trainer-runtime-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"runtime_binding_format": "cxnfs.trainer-runtime-binding.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"runtime_binding_object": "CxnfsTrainerRuntimeBinding"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"training_loop_context_used": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"world_size": 2'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"rank_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"requires_credentials": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"all_rank_streams_opened": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"all_rank_stream_kwargs_accepted": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"all_rank_state_matches_final_cursor": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"all_rank_training_shards_correct": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"no_cross_rank_overlap": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"format": "cxnfs.trainer-distributed-runtime-rank-states.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"rank_state_file_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"all_rank_state_files_written": true'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-smoke.json" '"raw_password_sent_over_control": false'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-rank-states/rank-00000.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-rank-states/rank-00000.trainer-stream-state.json" '"index": 0'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-rank-states/rank-00001.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-distributed-runtime-rank-states/rank-00001.trainer-stream-state.json" '"index": 1'
run_to_file "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_readiness_bundle_cli; import sys; raise SystemExit(run_trainer_readiness_bundle_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--output-dir"
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--runtime-rank"
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--world-size"
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--batches-per-rank"
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--loop-steps"
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--require-remote-quic"
require_file_not_contains "$REPORT_DIR/python-trainer-readiness-bundle-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-readiness-bundle.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_readiness_bundle_cli; import sys; raise SystemExit(run_trainer_readiness_bundle_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--output-dir "$REPORT_DIR/python-trainer-readiness-bundle" \
	--output "$REPORT_DIR/python-trainer-readiness-bundle.aggregate.json" \
	--integration-batches 1 \
	--runtime-batches 1 \
	--runtime-rank 0 \
	--world-size 2 \
	--batches-per-rank 1 \
	--loop-steps 2 \
	--trainer-prefetch-batches 0
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"format": "cxnfs.trainer-readiness-bundle.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"direct_python_streaming_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"runtime_training_loop_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"pipeline_training_loop_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"distributed_runtime_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"distributed_rank_states_written": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"loop_resume_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"raw_password_sent_over_control": false'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.json" '"artifact_count": 5'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle.aggregate.json" '"format": "cxnfs.trainer-readiness-bundle.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/trainer-preflight.json" '"format": "cxnfs.trainer-integration-preflight.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/trainer-integration-smoke.json" '"format": "cxnfs.trainer-integration-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/trainer-runtime-smoke.json" '"format": "cxnfs.trainer-runtime-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/trainer-distributed-runtime-smoke.json" '"format": "cxnfs.trainer-distributed-runtime-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/trainer-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/rank-states/rank-00000.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-readiness-bundle/rank-states/rank-00001.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
run_to_file "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_handoff_acceptance_cli; import sys; raise SystemExit(run_trainer_handoff_acceptance_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" "--output-dir"
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" "--runtime-rank"
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" "--world-size"
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" "--loop-steps"
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" "--require-remote-quic"
require_file_not_contains "$REPORT_DIR/python-trainer-handoff-acceptance-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-handoff-acceptance.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_handoff_acceptance_cli; import sys; raise SystemExit(run_trainer_handoff_acceptance_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--output-dir "$REPORT_DIR/python-trainer-handoff-acceptance" \
	--output "$REPORT_DIR/python-trainer-handoff-acceptance.aggregate.json" \
	--integration-batches 1 \
	--runtime-batches 1 \
	--runtime-rank 0 \
	--world-size 2 \
	--batches-per-rank 1 \
	--loop-steps 2 \
	--trainer-prefetch-batches 0
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"format": "cxnfs.trainer-handoff-acceptance.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"readiness_bundle_format": "cxnfs.trainer-readiness-bundle.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"remote_client_required": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"requires_credentials": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"credential_prompt_supported": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"raw_password_sent_over_control": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"direct_python_streaming_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"runtime_training_loop_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"pipeline_training_loop_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"pipeline_training_loop_object": "CxnfsTrainerPipelineLoop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"distributed_runtime_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"loop_resume_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"cross_rank_overlap_free": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"rank_states_written": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"trainer_loop_contract_format": "cxnfs.trainer-loop-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.json" '"password_cli_argument": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance.aggregate.json" '"format": "cxnfs.trainer-handoff-acceptance.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-handoff.json" '"format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-readiness-bundle.json" '"format": "cxnfs.trainer-readiness-bundle.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"format": "cxnfs.trainer-loop-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"pipeline_training_loop_context": "CxnfsTrainerRuntimeBinding.open_pipeline_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"pipeline_loop_object": "CxnfsTrainerPipelineLoop"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"password_cli_argument": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"requires_mount": false'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" '"python_byte_batch_avoidance_supported": true'
run_to_file "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_contract_check_cli; import sys; raise SystemExit(run_trainer_loop_contract_check_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" "--require-remote-quic"
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" "--require-pipeline"
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" "--require-direct-tensors"
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" "--require-torch-compile"
require_file_not_contains "$REPORT_DIR/python-trainer-loop-contract-check-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-loop-contract-check.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_contract_check_cli; import sys; raise SystemExit(run_trainer_loop_contract_check_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-contract.json" \
	--min-world-size 2 \
	--require-rank-states \
	--require-pipeline \
	--require-torch-compile
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check.json" '"format": "cxnfs.trainer-loop-contract-check.v1"'
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check.json" '"failed_checks": []'
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check.json" '"pipeline_contract_present": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-contract-check.json" '"pipeline_torch_compile_supported": true'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/trainer-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/rank-states/rank-00000.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-handoff-acceptance/rank-states/rank-00001.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
run_to_file "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_acceptance_fixture_cli; import sys; raise SystemExit(run_trainer_acceptance_fixture_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" "--output-dir"
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" "--trainer-batch-iterator"
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" "--loop-steps"
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" "--world-size"
require_file_not_contains "$REPORT_DIR/python-trainer-acceptance-fixture-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-acceptance-fixture.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_acceptance_fixture_cli; import sys; raise SystemExit(run_trainer_acceptance_fixture_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--output-dir "$REPORT_DIR/python-trainer-acceptance-fixture" \
	--output "$REPORT_DIR/python-trainer-acceptance-fixture.aggregate.json" \
	--batch-size 2 \
	--contract-batches 2 \
	--samples-per-epoch 6 \
	--trainer-batch-iterator trainer-dataloader \
	--data-loader-workers 0 \
	--world-size 2 \
	--loop-steps 2 \
	--trainer-prefetch-batches 1
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"format": "cxnfs.trainer-acceptance-fixture.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"trainer_code_touched": false'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"requires_mount": false'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"direct_python_streaming_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"runtime_training_loop_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"distributed_runtime_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"loop_resume_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"cross_rank_overlap_free": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"training_loop_context": "CxnfsTrainerRuntimeBinding.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"runtime_binding_loader": "CxnfsTrainerRuntimeBinding.from_file"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.json" '"trainer_loop_contract_format": "cxnfs.trainer-loop-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture.aggregate.json" '"format": "cxnfs.trainer-acceptance-fixture.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-contract.json" '"format": "cxnfs.trainer-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-handoff.json" '"format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-readiness-bundle.json" '"format": "cxnfs.trainer-readiness-bundle.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"format": "cxnfs.trainer-loop-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"trainer_batch_iterator": "trainer-dataloader"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"trainer_prefetch_batches": 1'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"rank_states_written": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"pipeline_descriptor_iterator": "CxnfsWeightedTorchDataset.iter_trainer_pipeline_tensor_batch_descriptors"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" '"python_byte_batch_avoidance_supported": true'
run_to_file "$REPORT_DIR/python-trainer-fixture-loop-contract-check.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_contract_check_cli; import sys; raise SystemExit(run_trainer_loop_contract_check_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-contract.json" \
	--min-world-size 2 \
	--require-rank-states \
	--require-prefetch \
	--require-pipeline \
	--require-torch-compile
require_file_contains "$REPORT_DIR/python-trainer-fixture-loop-contract-check.json" '"format": "cxnfs.trainer-loop-contract-check.v1"'
require_file_contains "$REPORT_DIR/python-trainer-fixture-loop-contract-check.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-fixture-loop-contract-check.json" '"trainer_prefetch_enabled": true'
require_file_contains "$REPORT_DIR/python-trainer-fixture-loop-contract-check.json" '"pipeline_contract_present": true'
require_file_contains "$REPORT_DIR/python-trainer-fixture-loop-contract-check.json" '"pipeline_torch_compile_supported": true'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/trainer-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/rank-states/rank-00000.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-acceptance-fixture/rank-states/rank-00001.trainer-stream-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
run_to_file "$REPORT_DIR/python-trainer-loop-replay-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_replay_cli; import sys; raise SystemExit(run_trainer_loop_replay_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-loop-replay-help.txt" "--steps"
require_file_contains "$REPORT_DIR/python-trainer-loop-replay-help.txt" "--gradient-accumulation-steps"
require_file_contains "$REPORT_DIR/python-trainer-loop-replay-help.txt" "--write-resume-state"
require_file_contains "$REPORT_DIR/python-trainer-loop-replay-help.txt" "--write-final-state"
require_file_not_contains "$REPORT_DIR/python-trainer-loop-replay-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-loop-replay.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_replay_cli; import sys; raise SystemExit(run_trainer_loop_replay_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-handoff.json" \
	--steps 2 \
	--write-resume-state "$REPORT_DIR/python-trainer-loop-replay-resume-state.json" \
	--write-final-state "$REPORT_DIR/python-trainer-loop-replay-final-state.json"
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"handoff_format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"training_loop_context": "CxnfsTrainerIntegrationPlan.open_training_loop"'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"training_loop_context_used": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"training_loop_object": "CxnfsTrainerPrefetchStream"'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"training_loop_base_stream_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"step_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"batch_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"sample_count": 3'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"resume_replayed": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"resume_continuity_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay.json" '"weighted_source_coverage_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay-resume-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-loop-replay-final-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
run_to_file "$REPORT_DIR/python-trainer-worker-contract.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_contract_cli; import sys; raise SystemExit(run_trainer_contract_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--read-mode direct-buffers \
	--batch-size 2 \
	--batches 4 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 0 \
	--bulk-read-retries 1 \
	--trainer-batch-iterator trainer-dataloader \
	--data-loader-workers 2 \
	--samples-per-epoch 8 \
	--no-repeat
require_file_contains "$REPORT_DIR/python-trainer-worker-contract.json" '"format": "cxnfs.trainer-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-worker-contract.json" '"trainer_batch_iterator": "trainer-dataloader"'
require_file_contains "$REPORT_DIR/python-trainer-worker-contract.json" '"data_loader_workers": 2'
require_file_contains "$REPORT_DIR/python-trainer-worker-contract.json" '"repeat": false'
run_to_file "$REPORT_DIR/python-trainer-worker-handoff.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_handoff_cli; import sys; raise SystemExit(run_trainer_handoff_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-worker-contract.json"
require_file_contains "$REPORT_DIR/python-trainer-worker-handoff.json" '"format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-worker-handoff.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-handoff.json" '"remote_client_required": false'
run_to_file "$REPORT_DIR/python-trainer-worker-loop-replay.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_replay_cli; import sys; raise SystemExit(run_trainer_loop_replay_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-worker-handoff.json" \
	--steps 4 \
	--resume-after-steps 2 \
	--write-resume-state "$REPORT_DIR/python-trainer-worker-loop-replay-resume-state.json" \
	--write-final-state "$REPORT_DIR/python-trainer-worker-loop-replay-final-state.json"
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"trainer_batch_iterator": "trainer-dataloader"'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"data_loader_workers": 2'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"batch_count": 4'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"sample_count": 8'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"repeat": false'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"resume_replayed": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"resume_continuity_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"worker_partitioning_enabled": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"worker_cursor_count": 2'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"format": "cxnfs.trainer-worker-partition.v1"'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"worker_batch_read_evidence_observed": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay.json" '"produced_inside_torch_worker": true'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay-resume-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-worker-loop-replay-final-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
run_to_file "$REPORT_DIR/python-trainer-tensor-contract.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_contract_cli; import sys; raise SystemExit(run_trainer_contract_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--read-mode direct-tensors \
	--tensor-target pytorch-cpu \
	--batch-size 2 \
	--batches 2 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 1 \
	--bulk-read-retries 1 \
	--trainer-batch-iterator native \
	--data-loader-workers 0 \
	--samples-per-epoch 3
require_file_contains "$REPORT_DIR/python-trainer-tensor-contract.json" '"format": "cxnfs.trainer-contract.v1"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-contract.json" '"read_mode": "direct-tensors"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-contract.json" '"target": "pytorch-cpu"'
run_to_file "$REPORT_DIR/python-trainer-tensor-handoff.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_handoff_cli; import sys; raise SystemExit(run_trainer_handoff_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-tensor-contract.json"
require_file_contains "$REPORT_DIR/python-trainer-tensor-handoff.json" '"format": "cxnfs.trainer-handoff.v1"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-handoff.json" '"ready_for_trainer_wiring": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-handoff.json" '"remote_client_required": false'
run_to_file "$REPORT_DIR/python-trainer-tensor-loop-replay.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_loop_replay_cli; import sys; raise SystemExit(run_trainer_loop_replay_cli(sys.argv[1:]))" \
	"$REPORT_DIR/python-trainer-tensor-handoff.json" \
	--steps 2 \
	--write-resume-state "$REPORT_DIR/python-trainer-tensor-loop-replay-resume-state.json" \
	--write-final-state "$REPORT_DIR/python-trainer-tensor-loop-replay-final-state.json"
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"format": "cxnfs.trainer-loop-replay.v1"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"read_mode": "direct-tensors"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"format": "cxnfs.trainer-tensor-memory-path.v1"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"complete": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"target": "pytorch-cpu"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"network_fills_tensor_destination": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"python_byte_batch_avoided": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"device_copy_required": false'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"direct_tensor_read_operations":'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"resume_replayed": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay.json" '"resume_continuity_verified": true'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay-resume-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-tensor-loop-replay-final-state.json" '"format": "cxnfs.trainer-stream-state.v1"'
run_to_file "$REPORT_DIR/python-trainer-stream-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_stream_smoke_cli; import sys; raise SystemExit(run_trainer_stream_smoke_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--remote-root"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--bulk-transport"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--trainer-batch-iterator"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--bulk-read-retries"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--tensor-target"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--resume-cursor"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--write-resume-cursor"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--resume-state"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--write-resume-state"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--no-repeat"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-stream-help.txt" "--world-size"
run_to_file "$REPORT_DIR/python-trainer-stream-smoke.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_stream_smoke_cli; import sys; raise SystemExit(run_trainer_stream_smoke_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--read-mode direct-buffers \
	--batch-size 2 \
	--batches 2 \
	--warmup-batches 1 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 2 \
	--bulk-read-retries 1 \
	--samples-per-epoch 3
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"format": "cxnfs.trainer-stream-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"transport": "local-mds"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"bulk_transport": "stream"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"read_mode": "direct-buffers"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"trainer_batch_iterator": "native"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"trainer_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"trainer_prefetch_enabled": true'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"bulk_read_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"training_shard"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"rank": 0'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"world_size": 1'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"unrecovered_read_failures": 0'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"stream_object": "CxnfsTrainerStream"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"stream_state_format": "cxnfs.trainer-stream-state.v1"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"stream_state_matches_final_cursor": true'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"format": "cxnfs.trainer-worker-partition.v1"'
require_file_contains "$REPORT_DIR/python-trainer-stream-smoke.json" '"worker_sharding_enabled": false'
run_to_file "$REPORT_DIR/python-trainer-pipeline-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_pipeline_smoke_cli; import sys; raise SystemExit(run_trainer_pipeline_smoke_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--remote-root"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--bulk-transport"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--pipeline-workers"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--pipeline-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--pipeline-max-inflight-bytes"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--tensor-target"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--write-probe-bytes"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--write-probe-frame-bytes"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--write-probe-root"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--world-size"
require_file_not_contains "$REPORT_DIR/python-trainer-pipeline-help.txt" "--password"
run_to_file "$REPORT_DIR/python-trainer-pipeline-smoke.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_pipeline_smoke_cli; import sys; raise SystemExit(run_trainer_pipeline_smoke_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--batch-size 2 \
	--batches 2 \
	--warmup-batches 1 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--pipeline-workers 2 \
	--pipeline-prefetch-batches 2 \
	--max-cords 4 \
	--samples-per-epoch 3 \
	--write-probe-bytes 4096 \
	--write-probe-frame-bytes 1024 \
	--tensor-target pytorch-cpu
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-pipeline-smoke.v1"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"passed": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"transport": "local-mds"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"bulk_transport": "stream"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"read_mode": "direct-tensors"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"training_pipeline_object": "CxnfsTrainingPipeline"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"training_pipeline_context": "CxnfsTrainingPipeline.prefetch_trainer_stream"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"pipeline_descriptor_stream": "CxnfsTrainerPipelineStream"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"pipeline_prefetch_stream_used": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"pipeline_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"pipeline_prefetch_result_object": "CxnfsPipelinePrefetchedTensorBatch"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"pipeline_descriptor_planning_used": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"pipeline_python_byte_batch_avoided": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"training_shard"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"rank": 0'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"world_size": 1'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-tensor-memory-path.v1"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"complete": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"unrecovered_read_failures": 0'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"format": "cxnfs.trainer-pipeline-write-probe.v1"'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"enabled": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"verified_read_back": true'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"byte_size": 4096'
require_file_contains "$REPORT_DIR/python-trainer-pipeline-smoke.json" '"frame_bytes": 1024'
run_to_file "$REPORT_DIR/python-trainer-workload-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_workload_benchmark_cli; import sys; raise SystemExit(run_trainer_workload_benchmark_cli(sys.argv[1:]))" \
	--help
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--cord-sweep"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--runs"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--server-disk-evidence"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--trainer-batch-iterator"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--trainer-prefetch-batches"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--bulk-read-retries"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--bulk-write-retries"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--tensor-target"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--write-probe-frame-bytes"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--rank"
require_file_contains "$REPORT_DIR/python-trainer-workload-help.txt" "--world-size"
run_to_file "$REPORT_DIR/python-trainer-workload-benchmark.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -c \
	"from cxnfs.torch_dataset import run_trainer_workload_benchmark_cli; import sys; raise SystemExit(run_trainer_workload_benchmark_cli(sys.argv[1:]))" \
	"$REPORT_DIR/weighted-mds/mds" \
	--weights chat=2,summary=1 \
	--bulk-transport stream \
	--read-mode direct-buffers \
	--batch-size 2 \
	--batches 2 \
	--warmup-batches 1 \
	--runs 2 \
	--cord-sweep 1,4 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 2 \
	--bulk-read-retries 1 \
	--samples-per-epoch 3 \
	--server-disk-evidence "$REPORT_DIR/workload-evidence/disk.report" \
	--server-cache-evidence "$REPORT_DIR/workload-evidence/cache.json" \
	--server-cpu-evidence "$REPORT_DIR/workload-evidence/cpu.report"
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-workload-benchmark.v1"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"run_count": 4'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"workload_fingerprint"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"trainer_batch_iterator": "native"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"trainer_prefetch_batches": 2'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"bulk_read_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"bulk_write_retries": 1'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"training_shard"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"rank": 0'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"world_size": 1'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-read-path-reliability.v1"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"read_path_reliability_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"unrecovered_read_failures": 0'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"benchmark_provenance"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-workload-provenance.v1"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"resume_cursor_evidence"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"all_runs_have_resume_cursors": true'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"all_final_cursors_advance_by_sample_count": true'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"cord_label": "4"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"server_evidence"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"complete": true'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"blockfs_gate"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"memory_path_evidence_complete": true'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"format": "cxnfs.trainer-workload-pressure.v1"'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"objective_present": false'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"decision_ready": false'
require_file_contains "$REPORT_DIR/python-trainer-workload-benchmark.json" '"kernel_module_allowed": false'
run_to_file "$REPORT_DIR/python-workload-decision-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.workload_decision --help
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--collect-local-server-evidence"
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--output-dir"
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--surface-min-workload-samples"
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--run-surface-matrix"
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--input-matrix-report"
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--surface-require-matrix"
require_file_contains "$REPORT_DIR/python-workload-decision-help.txt" "--blockfs-handoff-json"
run_to_file "$REPORT_DIR/python-workload-decision.json" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.workload_decision \
	--contract "$REPORT_DIR/python-trainer-contract.json" \
	--output-dir "$REPORT_DIR/workload-decision" \
	--run-surface-matrix \
	--collect-local-server-evidence \
	--proc-root "$REPORT_DIR/workload-evidence-proc" \
	--disk-device vda \
	--batch-size 2 \
	--batches 2 \
	--warmup-batches 1 \
	--runs 2 \
	--cord-sweep 1,4 \
	--rounds-per-read 2 \
	--source-read-workers 2 \
	--prefetch-chunks 2 \
	--trainer-prefetch-batches 2 \
	--bulk-read-retries 1 \
	--samples-per-epoch 3 \
	--surface-min-sources 2 \
	--surface-min-validation-samples 3 \
	--surface-min-validation-bytes 21 \
	--surface-min-workload-runs 4 \
	--surface-min-workload-samples 12 \
	--surface-min-workload-bytes 84 \
	--surface-min-cord-sweep 2 \
	--surface-require-matrix
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"format": "cxnfs.workload-decision.v1"'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"trainer_surface_verified": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"trainer_surface_matrix_passed": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"trainer_surface_check_passed": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"read_path_reliability_complete": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"unrecovered_read_failures": 0'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"memory_path_evidence_complete": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"server_evidence_complete": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"server_evidence_sample_window_complete": true'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"pressure_profile"'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"pressure_profile_complete": false'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"kernel_module_allowed": false'
require_file_contains "$REPORT_DIR/python-workload-decision.json" '"blockfs_handoff"'
require_file_contains "$REPORT_DIR/workload-decision/blockfs-handoff.json" '"format": "cxnfs.blockfs-handoff.v1"'
require_file_contains "$REPORT_DIR/workload-decision/blockfs-handoff.json" '"status": "deferred"'
require_file_contains "$REPORT_DIR/workload-decision/blockfs-handoff.json" '"kernel_module_allowed": false'
require_file_contains "$REPORT_DIR/workload-decision/blockfs-handoff.json" '"linux_blockfs_candidate": null'
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "cxnfs.workload-decision.v1"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "blockfs_handoff=$REPORT_DIR/workload-decision/blockfs-handoff.json"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "trainer_surface_verified=true"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "trainer_surface_matrix_passed=true"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "trainer_surface_check_passed=true"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "read_path_reliability_complete=true"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "unrecovered_read_failures=0"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "memory_path_evidence_complete=true"
require_file_contains "$REPORT_DIR/workload-decision/decision.report" "server_evidence_sample_window_complete=true"
require_file_contains "$REPORT_DIR/workload-decision/trainer-workload-benchmark.json" '"server_evidence"'
require_file_contains "$REPORT_DIR/workload-decision/trainer-surface-matrix.json" '"format": "cxnfs.trainer-surface-matrix.v1"'
require_file_contains "$REPORT_DIR/workload-decision/trainer-surface-matrix.json" '"case_name": "direct-tensors/trainer-dataloader"'
require_file_contains "$REPORT_DIR/workload-decision/trainer-surface-check.json" '"format": "cxnfs.trainer-surface-check.v1"'
require_file_contains "$REPORT_DIR/workload-decision/trainer-surface-check.json" '"passed": true'
require_file_contains "$REPORT_DIR/workload-decision/trainer-surface-check.json" '"require_matrix": true'
require_file_contains "$REPORT_DIR/workload-decision/server-evidence/disk.report" "cxnfs.server.disk.v1"
run_to_file "$REPORT_DIR/python-mount-help.txt" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.mount_handoff --help
require_file_contains "$REPORT_DIR/python-mount-help.txt" "negotiate"
require_file_contains "$REPORT_DIR/python-mount-help.txt" "launch-plan"
require_file_contains "$REPORT_DIR/python-mount-help.txt" "list"
run_to_file "$REPORT_DIR/python-mount-launch-plan.out" \
	env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.mount_handoff \
	launch-plan "$REPORT_DIR/cxnfs-mount.handoff" \
	--adapter-binary /opt/cxnfs/bin/cxnfs-fuse-t \
	--report "$REPORT_DIR/python-mount-launch-plan.txt"
require_file_contains "$REPORT_DIR/python-mount-launch-plan.out" "cxnfs.mount.launch-plan.v1"
require_file_contains "$REPORT_DIR/python-mount-launch-plan.txt" "argv_1=--handoff"
require_file_contains "$REPORT_DIR/python-mount-launch-plan.txt" "auth_password_inline=false"
run env PYTHONPATH="$PYTHON_RUN_PATH" "$PYTHON_BIN" -m cxnfs.protocol_report "$ROOT_DIR/protocol-fixtures" \
	--report "$REPORT_DIR/python-protocol-fixtures.txt" \
	--fail-on-missing true

if [ "$SKIP_BLOCKFS_SMOKE" != "1" ] || [ "$SKIP_BLOCKFS_MODULE" != "1" ]; then
	run "$CMAKE_BIN" -S "$ROOT_DIR/linux/cxnfs_blockfs" -B "$BLOCKFS_BUILD_DIR" -DCXNFS_DOWNLOAD_DEPS=OFF
fi
if [ "$SKIP_BLOCKFS_MODULE" != "1" ]; then
	if [ "$(uname -s)" = "Linux" ]; then
		run "$CMAKE_BIN" --build "$BLOCKFS_BUILD_DIR" --target module
	else
		printf 'cxnfs release check: blockfs module build skipped on %s\n' "$(uname -s)"
	fi
fi
if [ "$SKIP_BLOCKFS_SMOKE" != "1" ]; then
	run "$CMAKE_BIN" --build "$BLOCKFS_BUILD_DIR" --target tool-smoke
	require_file_contains "$BLOCKFS_BUILD_DIR/tool-smoke-reports/mkfs-force.report" "cxnfs.blockfs.mkfs.v1"
	require_file_contains "$BLOCKFS_BUILD_DIR/tool-smoke-reports/mkfs-force.report" "status=created"
	require_file_contains "$BLOCKFS_BUILD_DIR/tool-smoke-reports/fsck-force.report" "cxnfs.blockfs.fsck.v1"
	require_file_contains "$BLOCKFS_BUILD_DIR/tool-smoke-reports/fsck-force.report" "status=clean"
fi

if [ "$DRY_RUN" = "1" ]; then
	printf 'cxnfs release check dry run complete\n'
else
	printf 'cxnfs release check passed\n'
fi
printf 'reports=%s\n' "$REPORT_DIR"
