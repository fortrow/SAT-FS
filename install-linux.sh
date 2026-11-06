#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

JAR_PATH="${CXNFS_JAR:-}"
LIB_DIR="${CXNFS_LIB_DIR:-}"
JFLUX_CORE="${CXNFS_JFLUX_CORE:-}"
FUSE_ADAPTER="${CXNFS_FUSE_ADAPTER:-}"
PREFIX="${CXNFS_PREFIX:-/opt/cxnfs}"
BIN_DIR="${CXNFS_BIN_DIR:-/usr/local/bin}"
SBIN_DIR="${CXNFS_SBIN_DIR:-/sbin}"
TOOLS_DIR="${CXNFS_TOOLS_DIR:-}"
SKIP_JAR=0
CONFIG_DIR="${CXNFS_CONFIG_DIR:-/etc/cxnfs}"
SYSTEMD_DIR="${CXNFS_SYSTEMD_DIR:-/etc/systemd/system}"
STATE_DIR="${CXNFS_STATE_DIR:-/var/lib/cxnfs}"
CACHE_DIR="${CXNFS_CACHE_DIR:-/var/cache/cxnfs}"
RUN_DIR="${CXNFS_RUN_DIR:-/run/cxnfs}"
LOG_DIR="${CXNFS_LOG_DIR:-/var/log/cxnfs}"
DATASET_DIR="${CXNFS_DATASET_DIR:-/srv/datasets}"
OUTPUT_DIR="${CXNFS_OUTPUT_DIR:-/srv/cxnfs-outputs}"
SERVICE_USER="${CXNFS_SERVICE_USER:-cxnfs}"
DESTDIR="${DESTDIR:-}"

usage() {
  cat <<'EOF'
Usage: ./install-linux.sh [options]

Installs pre-built SAT-FS / CXNFS jars, tools, and packaging assets for Linux.

Options:
  --jar <path>          Built CXNFS jar. Defaults to target/cxnfs-0.1.0.jar.
  --lib-dir <path>      Directory containing runtime dependency jars.
                        Defaults to target/runtime-libs.
  --jflux-core <path>   JFlux core jar. Defaults to ~/STRATO/jflux/target/jflux-1.0.6-core.jar
                        or ../jflux/target/jflux-1.0.6-core.jar.
  --fuse-adapter <path> Native Linux FUSE adapter. Defaults to
                        target/native/cxnfs-fuse when present.
  --tools-dir <path>    Pre-built blockfs tools directory (mkfs.satfsblk, etc.).
  --skip-jar            Skip jar installation (for tools/systemd-only installs).
  --prefix <path>       Runtime install directory. Default: /opt/cxnfs.
  --bin-dir <path>      Launcher install directory. Default: /usr/local/bin.
  --sbin-dir <path>     Block filesystem tools directory. Default: /sbin.
  --config-dir <path>   Config directory. Default: /etc/cxnfs.
  --systemd-dir <path>  Systemd unit directory. Default: /etc/systemd/system.
  --dataset-dir <path>  Example dataset jail. Default: /srv/datasets.
  --output-dir <path>   Example output jail. Default: /srv/cxnfs-outputs.
  --help                Show this help.

Set DESTDIR to stage into a packaging root without touching the live system.
EOF
}

die() {
  echo "install-linux.sh: $*" >&2
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
    --fuse-adapter) FUSE_ADAPTER="${2:-}"; shift 2 ;;
    --prefix) PREFIX="${2:-}"; shift 2 ;;
    --bin-dir) BIN_DIR="${2:-}"; shift 2 ;;
    --sbin-dir) SBIN_DIR="${2:-}"; shift 2 ;;
    --tools-dir) TOOLS_DIR="${2:-}"; shift 2 ;;
    --skip-jar) SKIP_JAR=1; shift ;;
    --config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
    --systemd-dir) SYSTEMD_DIR="${2:-}"; shift 2 ;;
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
if [ -z "$FUSE_ADAPTER" ] && [ -f "$SCRIPT_DIR/target/native/cxnfs-fuse" ]; then
  FUSE_ADAPTER="$SCRIPT_DIR/target/native/cxnfs-fuse"
fi

if [ "$SKIP_JAR" -ne 1 ]; then
  if [ ! -d "$LIB_DIR" ] && command -v mvn >/dev/null 2>&1; then
    echo "install-linux.sh: staging runtime dependency jars into $LIB_DIR..."
    (cd "$SCRIPT_DIR" && mvn dependency:copy-dependencies -DoutputDirectory="$LIB_DIR" -DincludeScope=runtime -DskipTests >/dev/null 2>&1) || true
  fi
  [ -f "$JAR_PATH" ] || die "missing CXNFS jar: $JAR_PATH (run: mvn package -DskipTests, or pass --skip-jar for tools only)"
  [ -d "$LIB_DIR" ] || die "missing runtime lib directory: $LIB_DIR (run: mvn package -DskipTests, or pass --skip-jar for tools only)"
fi
[ -z "$FUSE_ADAPTER" ] || [ -f "$FUSE_ADAPTER" ] || die "missing native FUSE adapter: $FUSE_ADAPTER"
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

ensure_service_user() {
  if [ -n "$DESTDIR" ]; then
    return
  fi
  if id "$SERVICE_USER" >/dev/null 2>&1; then
    return
  fi
  if command -v systemd-sysusers >/dev/null 2>&1; then
    systemd-sysusers "$(target_path /etc/sysusers.d/cxnfs.conf)" >/dev/null 2>&1 || true
  fi
  if ! id "$SERVICE_USER" >/dev/null 2>&1 && command -v useradd >/dev/null 2>&1; then
    useradd --system --home-dir "$STATE_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
  fi
}

chown_if_present() {
  if [ -z "$DESTDIR" ] && id "$SERVICE_USER" >/dev/null 2>&1; then
    chown -R "$SERVICE_USER:$SERVICE_USER" "$@"
  fi
}

own_served_root_dirs_if_present() {
  if [ -n "$DESTDIR" ] || ! id "$SERVICE_USER" >/dev/null 2>&1; then
    return
  fi
  for dir in "$@"; do
    if [ -d "$dir" ]; then
      chown "$SERVICE_USER:$SERVICE_USER" "$dir"
      chmod u+rwx "$dir"
    fi
  done
}

secure_config_if_present() {
  if [ -z "$DESTDIR" ] && id "$SERVICE_USER" >/dev/null 2>&1; then
    chown root:"$SERVICE_USER" "$(target_path "$CONFIG_DIR")" \
      "$(target_path "$CONFIG_DIR/server.properties")" \
      "$(target_path "$CONFIG_DIR/server.properties.example")" \
      "$(target_path "$CONFIG_DIR/cxnfs.env")" 2>/dev/null || true
  fi
  chmod 0750 "$(target_path "$CONFIG_DIR")" 2>/dev/null || true
  chmod 0640 "$(target_path "$CONFIG_DIR/server.properties")" \
    "$(target_path "$CONFIG_DIR/server.properties.example")" \
    "$(target_path "$CONFIG_DIR/cxnfs.env")" 2>/dev/null || true
}

write_config() {
  config_path=$(target_path "$CONFIG_DIR/server.properties.example")
  tmp="$config_path.tmp.$$"
  umask 0077
  cat > "$tmp" <<EOF
# Copy to $CONFIG_DIR/server.properties, replace placeholders, then restrict
# ownership to root:$SERVICE_USER and mode 0640.
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
mkdir_p 0755 "$SBIN_DIR"
mkdir_p 0750 "$CONFIG_DIR"
mkdir_p 0750 "$STATE_DIR/repository"
mkdir_p 0750 "$CACHE_DIR/payload-staging"
mkdir_p 0750 "$RUN_DIR"
mkdir_p 0750 "$LOG_DIR"
mkdir_p 0755 "$DATASET_DIR"
mkdir_p 0755 "$OUTPUT_DIR"
mkdir_p 0755 "$SYSTEMD_DIR"
mkdir_p 0755 /etc/tmpfiles.d
mkdir_p 0755 /etc/sysusers.d

if [ "$SKIP_JAR" -ne 1 ]; then
  install_file 0644 "$JAR_PATH" "$PREFIX/cxnfs.jar"
  (cd "$(target_path "$PREFIX")" && ln -sf cxnfs.jar sat-fs.jar && ln -sf cxnfs.jar satfs.jar)
  copy_dependency_jars
  copy_jflux_core
fi

install_file 0755 "$SCRIPT_DIR/packaging/bin/cxnfs" "$BIN_DIR/cxnfs"
if [ -f "$SCRIPT_DIR/packaging/bin/sat-fs" ]; then
  install_file 0755 "$SCRIPT_DIR/packaging/bin/sat-fs" "$BIN_DIR/sat-fs"
  (cd "$(target_path "$BIN_DIR")" && ln -sf sat-fs satfs)
fi

find_blockfs_tool() {
  tool_name="$1"
  for cand in "$TOOLS_DIR/$tool_name" \
              "$SCRIPT_DIR/target/linux/$tool_name" \
              "$SCRIPT_DIR/linux/satfs_blockfs/tools/$tool_name" \
              "$SCRIPT_DIR/target/blockfs-build/tools/$tool_name" \
              "$SCRIPT_DIR/target/blockfs-tools/$tool_name"; do
    if [ -n "$cand" ] && [ -f "$cand" ]; then
      printf '%s' "$cand"
      return 0
    fi
  done
  return 1
}

if [ -z "$(find_blockfs_tool "mkfs.satfsblk" 2>/dev/null || true)" ] && [ -d "$SCRIPT_DIR/linux/satfs_blockfs/tools" ]; then
  if command -v make >/dev/null 2>&1 && command -v cc >/dev/null 2>&1; then
    make -C "$SCRIPT_DIR/linux/satfs_blockfs/tools" all || true
  fi
fi

MKFS_TOOL=$(find_blockfs_tool "mkfs.satfsblk" || find_blockfs_tool "mkfs.cxnfsblk" || true)
if [ -n "$MKFS_TOOL" ]; then
  install_file 0755 "$MKFS_TOOL" "$SBIN_DIR/mkfs.satfsblk"
  (cd "$(target_path "$SBIN_DIR")" && ln -sf mkfs.satfsblk mkfs.sat-fs && ln -sf mkfs.satfsblk mkfs.cxnfsblk)
  (cd "$(target_path "$BIN_DIR")" && ln -sf "$SBIN_DIR/mkfs.satfsblk" mkfs.satfsblk && ln -sf "$SBIN_DIR/mkfs.satfsblk" mkfs.sat-fs && ln -sf "$SBIN_DIR/mkfs.satfsblk" mkfs.cxnfsblk)
fi

FSCK_TOOL=$(find_blockfs_tool "fsck.satfsblk" || find_blockfs_tool "fsck.cxnfsblk" || true)
if [ -n "$FSCK_TOOL" ]; then
  install_file 0755 "$FSCK_TOOL" "$SBIN_DIR/fsck.satfsblk"
  (cd "$(target_path "$SBIN_DIR")" && ln -sf fsck.satfsblk fsck.sat-fs && ln -sf fsck.satfsblk fsck.cxnfsblk)
  (cd "$(target_path "$BIN_DIR")" && ln -sf "$SBIN_DIR/fsck.satfsblk" fsck.satfsblk && ln -sf "$SBIN_DIR/fsck.satfsblk" fsck.sat-fs && ln -sf "$SBIN_DIR/fsck.satfsblk" fsck.cxnfsblk)
fi

CTL_TOOL=$(find_blockfs_tool "satfs-blockfs-ctl" || find_blockfs_tool "cxnfs-blockfs-ctl" || true)
if [ -n "$CTL_TOOL" ]; then
  install_file 0755 "$CTL_TOOL" "$BIN_DIR/satfs-blockfs-ctl"
  (cd "$(target_path "$BIN_DIR")" && ln -sf satfs-blockfs-ctl cxnfs-blockfs-ctl)
fi

if [ -n "$FUSE_ADAPTER" ]; then
  install_file 0755 "$FUSE_ADAPTER" "$BIN_DIR/cxnfs-fuse"
fi
install_file 0644 "$SCRIPT_DIR/packaging/systemd/cxnfs.service" "$SYSTEMD_DIR/cxnfs.service"
install_file 0644 "$SCRIPT_DIR/packaging/systemd/cxnfs-maintenance.service" "$SYSTEMD_DIR/cxnfs-maintenance.service"
install_file 0644 "$SCRIPT_DIR/packaging/systemd/cxnfs-maintenance.timer" "$SYSTEMD_DIR/cxnfs-maintenance.timer"
install_file 0644 "$SCRIPT_DIR/packaging/systemd/cxnfs.tmpfiles" /etc/tmpfiles.d/cxnfs.conf
install_file 0644 "$SCRIPT_DIR/packaging/systemd/cxnfs.sysusers" /etc/sysusers.d/cxnfs.conf
if [ -f "$SCRIPT_DIR/packaging/systemd/satfs.service" ]; then
  install_file 0644 "$SCRIPT_DIR/packaging/systemd/satfs.service" "$SYSTEMD_DIR/satfs.service"
  install_file 0644 "$SCRIPT_DIR/packaging/systemd/satfs.tmpfiles" /etc/tmpfiles.d/satfs.conf
  install_file 0644 "$SCRIPT_DIR/packaging/systemd/satfs.sysusers" /etc/sysusers.d/satfs.conf
fi
install_file 0640 "$SCRIPT_DIR/packaging/systemd/cxnfs.env.example" "$CONFIG_DIR/cxnfs.env"
write_config

rm -f "$(target_path "$RUN_DIR/server.ready")" \
      "$(target_path "$RUN_DIR/server.health")" \
      "$(target_path "$RUN_DIR/server.stop")" \
      "$(target_path "$RUN_DIR/server.lock")"
touch "$(target_path "$LOG_DIR/server.out.log")" "$(target_path "$LOG_DIR/server.err.log")"

ensure_service_user
secure_config_if_present
chown_if_present "$(target_path "$STATE_DIR")" "$(target_path "$CACHE_DIR")" "$(target_path "$RUN_DIR")" "$(target_path "$LOG_DIR")"
own_served_root_dirs_if_present "$(target_path "$DATASET_DIR")" "$(target_path "$OUTPUT_DIR")"

if [ -z "$DESTDIR" ] && command -v systemd-tmpfiles >/dev/null 2>&1; then
  systemd-tmpfiles --create /etc/tmpfiles.d/cxnfs.conf >/dev/null 2>&1 || true
fi

echo "Installed SAT-FS / CXNFS Linux files."
if [ "$SKIP_JAR" -ne 1 ]; then
  echo "Jar: $PREFIX/cxnfs.jar (symlinks: sat-fs.jar, satfs.jar)"
fi
echo "Launcher: $BIN_DIR/cxnfs (and $BIN_DIR/sat-fs)"
if [ -n "$MKFS_TOOL" ]; then
  echo "BlockFS mkfs: $SBIN_DIR/mkfs.satfsblk (symlinks: mkfs.sat-fs, mkfs.cxnfsblk)"
fi
if [ -n "$FSCK_TOOL" ]; then
  echo "BlockFS fsck: $SBIN_DIR/fsck.satfsblk (symlinks: fsck.sat-fs, fsck.cxnfsblk)"
fi
if [ -n "$CTL_TOOL" ]; then
  echo "BlockFS control: $BIN_DIR/satfs-blockfs-ctl (symlink: cxnfs-blockfs-ctl)"
fi
if [ -n "$FUSE_ADAPTER" ]; then
  echo "FUSE adapter: $BIN_DIR/cxnfs-fuse"
else
  echo "FUSE adapter: not installed; build native/fuse-linux/build-linux.sh and rerun with --fuse-adapter"
fi
echo "Config example: $CONFIG_DIR/server.properties.example"
echo "Config: $CONFIG_DIR/server.properties"
echo "Runtime markers are prepared under $RUN_DIR and stale marker files were cleared."
