#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
OUTPUT="${1:-$ROOT_DIR/target/native/cxnfs-fuse-t}"
FUSE_T_ROOT="${FUSE_T_ROOT:-/Library/Application Support/fuse-t}"
INCLUDE_DIR="${FUSE_T_INCLUDE_DIR:-$FUSE_T_ROOT/include}"
LIB_DIR="${FUSE_T_LIB_DIR:-$FUSE_T_ROOT/lib}"

[ -f "$INCLUDE_DIR/fuse/fuse.h" ] || {
  echo "build-macos.sh: missing FUSE-T fuse headers under $INCLUDE_DIR/fuse" >&2
  exit 1
}
[ -f "$LIB_DIR/libfuse-t-1.2.6.dylib" ] || [ -f "$LIB_DIR/libfuse-t.dylib" ] || {
  echo "build-macos.sh: missing FUSE-T libfuse-t dylib under $LIB_DIR" >&2
  exit 1
}

mkdir -p "$(dirname -- "$OUTPUT")"
cc \
  -Wall \
  -Wextra \
  -Werror \
  -DFUSE_USE_VERSION=29 \
  -D_FILE_OFFSET_BITS=64 \
  -I"$INCLUDE_DIR" \
  "$SCRIPT_DIR/cxnfs_fuse_t.c" \
  -L"$LIB_DIR" \
  -Wl,-rpath,"$LIB_DIR" \
  -lfuse-t-1.2.6 \
  -o "$OUTPUT"
if command -v install_name_tool >/dev/null 2>&1; then
  install_name_tool \
    -change @rpath/libfuse-t.dylib @rpath/libfuse-t-1.2.6.dylib \
    "$OUTPUT"
fi
chmod 0755 "$OUTPUT"
printf '%s\n' "$OUTPUT"
