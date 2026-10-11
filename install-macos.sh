#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

JAR_PATH="${CXNFS_JAR:-}"
LIB_DIR="${CXNFS_LIB_DIR:-}"
JFLUX_CORE="${CXNFS_JFLUX_CORE:-}"
FUSE_T_ADAPTER="${CXNFS_FUSE_T_ADAPTER:-}"
PREFIX="${CXNFS_PREFIX:-/opt/cxnfs}"
BIN_DIR="${CXNFS_BIN_DIR:-/usr/local/bin}"
CONFIG_DIR="${CXNFS_CONFIG_DIR:-/etc/cxnfs}"
STATE_DIR="${CXNFS_STATE_DIR:-/var/db/cxnfs}"
CACHE_DIR="${CXNFS_CACHE_DIR:-/var/cache/cxnfs}"
RUN_DIR="${CXNFS_RUN_DIR:-/var/run/cxnfs}"
LOG_DIR="${CXNFS_LOG_DIR:-/var/log/cxnfs}"
DATASET_DIR="${CXNFS_DATASET_DIR:-/usr/local/srv/datasets}"
OUTPUT_DIR="${CXNFS_OUTPUT_DIR:-/usr/local/srv/cxnfs-outputs}"
DESTDIR="${DESTDIR:-}"

usage() {
  cat <<'EOF'
Usage: ./install-macos.sh [options]

Installs pre-built CXNFS jars and packaging assets for macOS.

Options:
  --jar <path>          Built CXNFS jar. Defaults to target/cxnfs-0.1.0.jar.
  --lib-dir <path>      Directory containing runtime dependency jars.
                        Defaults to target/runtime-libs.
  --jflux-core <path>   JFlux core jar. Defaults to ~/STRATO/jflux/target/jflux-1.0.6-core.jar
                        or ../jflux/target/jflux-1.0.6-core.jar.
  --fuse-t-adapter <path>
                        Native macOS FUSE-T adapter. Defaults to
                        target/native/cxnfs-fuse-t.
  --prefix <path>       Runtime install directory. Default: /opt/cxnfs.
  --bin-dir <path>      Launcher install directory. Default: /usr/local/bin.
  --config-dir <path>   Config directory. Default: /etc/cxnfs.
  --dataset-dir <path>  Example dataset jail. Default: /usr/local/srv/datasets.
  --output-dir <path>   Example output jail. Default: /usr/local/srv/cxnfs-outputs.
  --help                Show this help.

Set DESTDIR to stage into a packaging root without touching the live system.
EOF
}

die() {
  echo "install-macos.sh: $*" >&2
  exit 1
}

target_path() {
  printf '%s%s' "$DESTDIR" "$1"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --jar) JAR_PATH="${2:-}"; shift 2 ;;
    --lib-dir) LIB_DIR="${2:-}"; shift 2 ;;
    --jflux-core) JFLUX_CORE="${2:-}"; shift 2 ;;
    --fuse-t-adapter) FUSE_T_ADAPTER="${2:-}"; shift 2 ;;
    --prefix) PREFIX="${2:-}"; shift 2 ;;
    --bin-dir) BIN_DIR="${2:-}"; shift 2 ;;
    --config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
    --dataset-dir) DATASET_DIR="${2:-}"; shift 2 ;;
    --output-dir) OUTPUT_DIR="${2:-}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

if [ -z "$DESTDIR" ] && [ "$(id -u)" -ne 0 ]; then
  die "run as root, or set DESTDIR to stage into a package root"
fi

if [ -z "$JAR_PATH" ]; then
  JAR_PATH="$SCRIPT_DIR/target/cxnfs-0.1.0.jar"
fi
if [ -z "$LIB_DIR" ]; then
  LIB_DIR="$SCRIPT_DIR/target/runtime-libs"
fi
if [ -z "$FUSE_T_ADAPTER" ]; then
  FUSE_T_ADAPTER="$SCRIPT_DIR/target/native/cxnfs-fuse-t"
fi

[ -f "$JAR_PATH" ] || die "missing CXNFS jar: $JAR_PATH"
[ -d "$LIB_DIR" ] || die "missing runtime lib directory: $LIB_DIR"
[ -f "$FUSE_T_ADAPTER" ] || die "missing native FUSE-T adapter: $FUSE_T_ADAPTER"
[ -f "$SCRIPT_DIR/packaging/bin/cxnfs" ] || die "missing packaging/bin/cxnfs"

mkdir_p() {
  install -d -m "$1" "$(target_path "$2")"
}

install_file() {
  install -m "$1" "$2" "$(target_path "$3")"
}

copy_dependency_jars() {
  found=0
  for jar in "$LIB_DIR"/*.jar; do
    if [ -f "$jar" ]; then
      found=1
      install_file 0644 "$jar" "$PREFIX/lib/$(basename "$jar")"
    fi
  done
  [ "$found" -eq 1 ] || die "no dependency jars found in $LIB_DIR"
}

copy_jflux_core() {
  for jar in "$PREFIX"/lib/jflux-*-core.jar "$LIB_DIR"/jflux-*-core.jar; do
    if [ -f "$(target_path "$jar")" ] || [ -f "$jar" ]; then
      return
    fi
  done

  if [ -z "$JFLUX_CORE" ]; then
    for candidate in "$HOME/STRATO/jflux/target/jflux-1.0.6-core.jar" "$SCRIPT_DIR/../jflux/target/jflux-1.0.6-core.jar"; do
      if [ -f "$candidate" ]; then
        JFLUX_CORE="$candidate"
        break
      fi
    done
  fi

  [ -n "$JFLUX_CORE" ] || die "missing JFlux core jar; pass --jflux-core <path>"
  [ -f "$JFLUX_CORE" ] || die "missing JFlux core jar: $JFLUX_CORE"
  install_file 0644 "$JFLUX_CORE" "$PREFIX/lib/$(basename "$JFLUX_CORE")"
}

write_config() {
  config_path=$(target_path "$CONFIG_DIR/server.properties.example")
  tmp="$config_path.tmp.$$"
  umask 0077
  cat > "$tmp" <<EOF
# Copy to $CONFIG_DIR/server.properties, replace placeholders, then restrict
# ownership to root:wheel and mode 0640.
serve.root.0=/datasets=$DATASET_DIR
serve.root.1=/outputs=$OUTPUT_DIR
serve.host=0.0.0.0
serve.control-port=4509
serve.transport-port=4510
serve.bulk-transport=quic
serve.initial-cords=1
serve.max-cords=64
serve.adaptive-cords=true
serve.live-write-mode=positional
serve.payload-staging-memory-bytes=536870912
serve.payload-spill-dir=$CACHE_DIR/payload-staging
serve.repository-root=$STATE_DIR/repository
serve.keystore=$CONFIG_DIR/server.p12
serve.keystore-password-file=$CONFIG_DIR/server.p12.pass
serve.ready-file=$RUN_DIR/server.ready
serve.health-file=$RUN_DIR/server.health
serve.health-interval-ms=1000
serve.shutdown-file=$RUN_DIR/server.stop
serve.shutdown-poll-ms=250
serve.daemon-lock-file=$RUN_DIR/server.lock

auth.mechanisms=username_password
auth.user.trainer.password=REPLACE_WITH_LONG_RANDOM_PASSWORD
auth.user.eval.password=REPLACE_WITH_DIFFERENT_LONG_RANDOM_PASSWORD

# Optional per-user remote namespace jails. Omit to allow all served roots.
# auth.user.trainer.directories=/datasets,/outputs
# auth.user.eval.directories=/datasets
EOF
  install -m 0640 "$tmp" "$config_path"
  rm -f "$tmp"
  if [ ! -f "$(target_path "$CONFIG_DIR/server.properties")" ]; then
    install -m 0640 "$config_path" "$(target_path "$CONFIG_DIR/server.properties")"
  fi
}

mkdir_p 0755 "$PREFIX"
mkdir_p 0755 "$PREFIX/lib"
mkdir_p 0755 "$BIN_DIR"
mkdir_p 0750 "$CONFIG_DIR"
mkdir_p 0750 "$STATE_DIR/repository"
mkdir_p 0750 "$CACHE_DIR/payload-staging"
mkdir_p 0750 "$RUN_DIR"
mkdir_p 0750 "$LOG_DIR"
mkdir_p 0755 "$DATASET_DIR"
mkdir_p 0755 "$OUTPUT_DIR"

install_file 0644 "$JAR_PATH" "$PREFIX/cxnfs.jar"
copy_dependency_jars
copy_jflux_core
install_file 0755 "$SCRIPT_DIR/packaging/bin/cxnfs" "$BIN_DIR/cxnfs"
install_file 0755 "$FUSE_T_ADAPTER" "$BIN_DIR/cxnfs-fuse-t"
write_config

rm -f "$(target_path "$RUN_DIR/server.ready")" \
      "$(target_path "$RUN_DIR/server.health")" \
      "$(target_path "$RUN_DIR/server.stop")" \
      "$(target_path "$RUN_DIR/server.lock")"
touch "$(target_path "$LOG_DIR/server.out.log")" "$(target_path "$LOG_DIR/server.err.log")"

echo "Installed CXNFS macOS files."
echo "Jar: $PREFIX/cxnfs.jar"
echo "Launcher: $BIN_DIR/cxnfs"
echo "FUSE-T adapter: $BIN_DIR/cxnfs-fuse-t"
echo "Config example: $CONFIG_DIR/server.properties.example"
echo "Config: $CONFIG_DIR/server.properties"
echo "Runtime markers are prepared under $RUN_DIR and stale marker files were cleared."
