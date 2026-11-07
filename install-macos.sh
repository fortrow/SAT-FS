#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

JAR_PATH="${CXNFS_JAR:-}"
LIB_DIR="${CXNFS_LIB_DIR:-}"
JFLUX_CORE="${CXNFS_JFLUX_CORE:-}"
FUSE_T_ADAPTER="${CXNFS_FUSE_T_ADAPTER:-}"
FUSE_T_ROOT="${FUSE_T_ROOT:-/Library/Application Support/fuse-t}"
FUSE_T_INCLUDE_DIR="${FUSE_T_INCLUDE_DIR:-}"
FUSE_T_LIB_DIR="${FUSE_T_LIB_DIR:-}"
FUSE_T_VERSION="${FUSE_T_VERSION:-1.2.6}"
BUILD_FUSE_T=0
DOWNLOAD_FUSE_T=0
PREFIX="${CXNFS_PREFIX:-/opt/cxnfs}"
BIN_DIR="${CXNFS_BIN_DIR:-/usr/local/bin}"
TOOLS_DIR="${CXNFS_TOOLS_DIR:-}"
SKIP_JAR=0
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
  --jflux-core <path>   JFlux core jar. Defaults to lib/jflux-1.2.1-core.jar,
                        ~/STRATO/jflux/target/jflux-1.2.1-core.jar,
                        or ../jflux/target/jflux-1.2.1-core.jar.
  --fuse-t-adapter <path>
                        Native macOS FUSE-T adapter. Defaults to
                        target/native/cxnfs-fuse-t when present.
  --build-fuse-t        Build native FUSE-T adapter locally from source.
  --download-fuse-t     Download FUSE-T headers and libraries (Homebrew or
                        official release package) and build the adapter.
  --fuse-t-root <path>  FUSE-T root directory. Default:
                        /Library/Application Support/fuse-t.
  --fuse-t-include <path>
                        FUSE-T include directory (containing fuse/fuse.h).
  --fuse-t-lib <path>   FUSE-T library directory (containing libfuse-t*.dylib).
  --fuse-t-version <ver>
                        FUSE-T release version to download. Default: 1.2.6.
  --skip-jar            Skip jar installation.
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

download_fuse_t() {
  include_dir="${FUSE_T_INCLUDE_DIR:-$FUSE_T_ROOT/include}"
  lib_dir="${FUSE_T_LIB_DIR:-$FUSE_T_ROOT/lib}"

  if [ -f "$include_dir/fuse/fuse.h" ] || [ -f "$include_dir/fuse.h" ]; then
    if [ -f "$lib_dir/libfuse-t-1.2.6.dylib" ] || [ -f "$lib_dir/libfuse-t.dylib" ]; then
      echo "FUSE-T headers and libraries already found under $FUSE_T_ROOT"
      return 0
    fi
  fi

  echo "FUSE-T not found locally. Attempting installation / download..."
  if command -v brew >/dev/null 2>&1; then
    echo "Attempting to install FUSE-T via Homebrew..."
    brew install --cask fuse-t 2>/dev/null || brew install macos-fuse-t/homebrew-cask/fuse-t 2>/dev/null || true
    if [ -f "$include_dir/fuse/fuse.h" ] || [ -f "$include_dir/fuse.h" ]; then
      echo "Successfully installed FUSE-T via Homebrew."
      return 0
    fi
  fi

  PKG_URL="https://github.com/macos-fuse-t/fuse-t/releases/download/${FUSE_T_VERSION}/fuse-t-macos-installer-${FUSE_T_VERSION}.pkg"
  TMP_PKG="/tmp/fuse-t-installer-${FUSE_T_VERSION}.pkg"
  echo "Downloading FUSE-T ${FUSE_T_VERSION} from $PKG_URL..."
  if command -v curl >/dev/null 2>&1; then
    curl -fSL "$PKG_URL" -o "$TMP_PKG"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$TMP_PKG" "$PKG_URL"
  else
    die "neither curl nor wget available to download FUSE-T package"
  fi

  [ -f "$TMP_PKG" ] || die "failed to download FUSE-T package from $PKG_URL"

  if [ -z "$DESTDIR" ] && [ "$(id -u)" -eq 0 ] && command -v installer >/dev/null 2>&1; then
    echo "Installing FUSE-T package to system..."
    installer -pkg "$TMP_PKG" -target /
    rm -f "$TMP_PKG"
  else
    echo "Extracting FUSE-T package payload..."
    EXTRACT_DIR="${SCRIPT_DIR}/target/native/fuse-t-dist"
    rm -rf "$EXTRACT_DIR"
    mkdir -p "$EXTRACT_DIR"
    if command -v pkgutil >/dev/null 2>&1; then
      pkgutil --expand-full "$TMP_PKG" "$EXTRACT_DIR/pkg" 2>/dev/null || pkgutil --expand "$TMP_PKG" "$EXTRACT_DIR/pkg" 2>/dev/null || true
    fi
    found_root=$(find "$EXTRACT_DIR" -type d -name "fuse-t" 2>/dev/null | head -n 1 || true)
    if [ -n "$found_root" ] && [ -d "$found_root" ]; then
      FUSE_T_ROOT="$found_root"
      FUSE_T_INCLUDE_DIR="$found_root/include"
      FUSE_T_LIB_DIR="$found_root/lib"
      echo "FUSE-T payload staged under $FUSE_T_ROOT"
    fi
    rm -f "$TMP_PKG"
  fi
}

build_fuse_t() {
  if [ "$DOWNLOAD_FUSE_T" -eq 1 ]; then
    download_fuse_t
  fi

  include_dir="${FUSE_T_INCLUDE_DIR:-$FUSE_T_ROOT/include}"
  lib_dir="${FUSE_T_LIB_DIR:-$FUSE_T_ROOT/lib}"

  if [ ! -f "$include_dir/fuse/fuse.h" ] && [ ! -f "$include_dir/fuse.h" ]; then
    die "missing FUSE-T headers in $include_dir; pass --download-fuse-t or install with 'brew install --cask fuse-t'"
  fi
  if [ ! -f "$lib_dir/libfuse-t-1.2.6.dylib" ] && [ ! -f "$lib_dir/libfuse-t.dylib" ]; then
    die "missing FUSE-T library in $lib_dir; pass --download-fuse-t or install with 'brew install --cask fuse-t'"
  fi

  echo "Compiling native FUSE-T adapter..."
  FUSE_T_ROOT="$FUSE_T_ROOT" \
  FUSE_T_INCLUDE_DIR="$include_dir" \
  FUSE_T_LIB_DIR="$lib_dir" \
    "$SCRIPT_DIR/native/fuse-t/build-macos.sh" "$SCRIPT_DIR/target/native/cxnfs-fuse-t"
  FUSE_T_ADAPTER="$SCRIPT_DIR/target/native/cxnfs-fuse-t"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --jar) JAR_PATH="${2:-}"; shift 2 ;;
    --lib-dir) LIB_DIR="${2:-}"; shift 2 ;;
    --jflux-core) JFLUX_CORE="${2:-}"; shift 2 ;;
    --fuse-t-adapter) FUSE_T_ADAPTER="${2:-}"; shift 2 ;;
    --build-fuse-t) BUILD_FUSE_T=1; shift ;;
    --download-fuse-t) DOWNLOAD_FUSE_T=1; BUILD_FUSE_T=1; shift ;;
    --fuse-t-root) FUSE_T_ROOT="${2:-}"; shift 2 ;;
    --fuse-t-include|--fuse-t-include-dir) FUSE_T_INCLUDE_DIR="${2:-}"; shift 2 ;;
    --fuse-t-lib|--fuse-t-lib-dir) FUSE_T_LIB_DIR="${2:-}"; shift 2 ;;
    --fuse-t-version) FUSE_T_VERSION="${2:-}"; shift 2 ;;
    --skip-jar) SKIP_JAR=1; shift ;;
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
if [ "$SKIP_JAR" -ne 1 ]; then
  if [ ! -d "$LIB_DIR" ] && [ -d "$SCRIPT_DIR/lib" ]; then
    LIB_DIR="$SCRIPT_DIR/lib"
  fi
  [ -f "$JAR_PATH" ] || die "missing CXNFS jar: $JAR_PATH"
  [ -d "$LIB_DIR" ] || die "missing runtime lib directory: $LIB_DIR"
fi

if [ "$BUILD_FUSE_T" -eq 1 ]; then
  build_fuse_t
elif [ -z "$FUSE_T_ADAPTER" ]; then
  if [ -f "$SCRIPT_DIR/target/native/cxnfs-fuse-t" ]; then
    FUSE_T_ADAPTER="$SCRIPT_DIR/target/native/cxnfs-fuse-t"
  elif [ -f "$SCRIPT_DIR/native/fuse-t/build-macos.sh" ] && [ "$(uname -s 2>/dev/null || echo unknown)" = "Darwin" ]; then
    fuse_t_header="${FUSE_T_INCLUDE_DIR:-${FUSE_T_ROOT}/include}/fuse/fuse.h"
    fuse_t_lib="${FUSE_T_LIB_DIR:-${FUSE_T_ROOT}/lib}"
    if { [ -f "$fuse_t_header" ] || [ -f "${FUSE_T_INCLUDE_DIR:-${FUSE_T_ROOT}/include}/fuse.h" ]; } && \
       { [ -f "$fuse_t_lib/libfuse-t-1.2.6.dylib" ] || [ -f "$fuse_t_lib/libfuse-t.dylib" ]; }; then
      echo "FUSE-T detected. Building native adapter..."
      if FUSE_T_ROOT="$FUSE_T_ROOT" \
         FUSE_T_INCLUDE_DIR="${FUSE_T_INCLUDE_DIR:-$FUSE_T_ROOT/include}" \
         FUSE_T_LIB_DIR="${FUSE_T_LIB_DIR:-$FUSE_T_ROOT/lib}" \
         "$SCRIPT_DIR/native/fuse-t/build-macos.sh" "$SCRIPT_DIR/target/native/cxnfs-fuse-t" >/dev/null 2>&1; then
        FUSE_T_ADAPTER="$SCRIPT_DIR/target/native/cxnfs-fuse-t"
      fi
    fi
  fi
fi

[ -z "$FUSE_T_ADAPTER" ] || [ -f "$FUSE_T_ADAPTER" ] || die "missing native FUSE-T adapter: $FUSE_T_ADAPTER"
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
    for candidate in \
      "$SCRIPT_DIR/lib/jflux-1.2.1-core.jar" \
      "$SCRIPT_DIR/lib/jflux-v1.2.1-core.jar" \
      "$SCRIPT_DIR/packaging/lib/jflux-1.2.1-core.jar" \
      "$HOME/STRATO/jflux/target/jflux-1.2.1-core.jar" \
      "$SCRIPT_DIR/../jflux/target/jflux-1.2.1-core.jar" \
      "$SCRIPT_DIR/lib/jflux-1.0.6-core.jar" \
      "$HOME/STRATO/jflux/target/jflux-1.0.6-core.jar" \
      "$SCRIPT_DIR/../jflux/target/jflux-1.0.6-core.jar"; do
      if [ -f "$candidate" ]; then
        JFLUX_CORE="$candidate"
        break
      fi
    done
  fi

  [ -n "$JFLUX_CORE" ] || die "missing JFlux core jar; pass --jflux-core <path>"
  [ -f "$JFLUX_CORE" ] || die "missing JFlux core jar: $JFLUX_CORE"
  install_file 0644 "$JFLUX_CORE" "$PREFIX/lib/$(basename "$JFLUX_CORE")"

  if [ -d "$SCRIPT_DIR/lib" ]; then
    for extra_jar in "$SCRIPT_DIR"/lib/*.jar; do
      if [ -f "$extra_jar" ]; then
        base_name="$(basename "$extra_jar")"
        case "$base_name" in
          *source*.jar|*sources*.jar) ;;
          *)
            if [ "$base_name" != "$(basename "$JFLUX_CORE")" ] && [ ! -f "$(target_path "$PREFIX/lib/$base_name")" ]; then
              install_file 0644 "$extra_jar" "$PREFIX/lib/$base_name"
            fi
            ;;
        esac
      fi
    done
  fi
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

for extra_bin in /usr/bin /usr/sbin; do
  if [ "$extra_bin" != "$BIN_DIR" ]; then
    mkdir_p 0755 "$extra_bin" 2>/dev/null || true
    if [ -d "$(target_path "$extra_bin")" ]; then
      (cd "$(target_path "$extra_bin")" && ln -sf "$BIN_DIR/sat-fs" sat-fs 2>/dev/null && ln -sf "$BIN_DIR/sat-fs" satfs 2>/dev/null && ln -sf "$BIN_DIR/cxnfs" cxnfs 2>/dev/null) || true
    fi
  fi
done

if [ "$PREFIX" = "/opt/cxnfs" ] || [ "$(basename "$PREFIX")" = "cxnfs" ]; then
  mkdir_p 0755 "$(dirname "$PREFIX")" 2>/dev/null || true
  (cd "$(target_path "$(dirname "$PREFIX")")" && ln -sf "$(basename "$PREFIX")" sat-fs 2>/dev/null && ln -sf "$(basename "$PREFIX")" satfs 2>/dev/null) || true
fi
if [ -n "$FUSE_T_ADAPTER" ]; then
  install_file 0755 "$FUSE_T_ADAPTER" "$BIN_DIR/cxnfs-fuse-t"
  (cd "$(target_path "$BIN_DIR")" && ln -sf cxnfs-fuse-t satfs-fuse-t)
  for extra_bin in /usr/bin /usr/sbin; do
    if [ "$extra_bin" != "$BIN_DIR" ] && [ -d "$(target_path "$extra_bin")" ]; then
      (cd "$(target_path "$extra_bin")" && ln -sf "$BIN_DIR/cxnfs-fuse-t" cxnfs-fuse-t 2>/dev/null && ln -sf "$BIN_DIR/cxnfs-fuse-t" satfs-fuse-t 2>/dev/null) || true
    fi
  done
fi
write_config

rm -f "$(target_path "$RUN_DIR/server.ready")" \
      "$(target_path "$RUN_DIR/server.health")" \
      "$(target_path "$RUN_DIR/server.stop")" \
      "$(target_path "$RUN_DIR/server.lock")"
touch "$(target_path "$LOG_DIR/server.out.log")" "$(target_path "$LOG_DIR/server.err.log")"

echo "Installed CXNFS macOS files."
if [ "$SKIP_JAR" -ne 1 ]; then
  echo "Jar: $PREFIX/cxnfs.jar"
fi
echo "Launcher: $BIN_DIR/cxnfs"
if [ -n "$FUSE_T_ADAPTER" ]; then
  echo "FUSE-T adapter: $BIN_DIR/cxnfs-fuse-t"
fi
echo "Config example: $CONFIG_DIR/server.properties.example"
echo "Config: $CONFIG_DIR/server.properties"
echo "Runtime markers are prepared under $RUN_DIR and stale marker files were cleared."
