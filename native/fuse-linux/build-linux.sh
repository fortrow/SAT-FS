#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
OUTPUT="${1:-$ROOT_DIR/target/native/satfs-fuse}"

die() {
  echo "build-linux.sh: $*" >&2
  exit 1
}

command -v pkg-config >/dev/null 2>&1 || die "missing pkg-config; install pkg-config and libfuse-dev"
pkg-config --exists fuse || die "missing libfuse2 development files; on Debian/Ubuntu install: sudo apt-get install -y pkg-config libfuse-dev fuse"

mkdir -p "$(dirname -- "$OUTPUT")"

EXTRA_CFLAGS=""
if printf '%s\n' \
  '#define FUSE_USE_VERSION 29' \
  '#include <fuse.h>' \
  'int main(void) { struct fuse_operations ops; ops.copy_file_range = 0; return ops.copy_file_range != 0; }' \
  | cc -Wall -Wextra -Werror -D_FILE_OFFSET_BITS=64 $(pkg-config --cflags fuse) -x c - -c -o /tmp/cxnfs-fuse-copy-file-range-check.o >/dev/null 2>&1; then
  EXTRA_CFLAGS="$EXTRA_CFLAGS -DCXNFS_FUSE_HAS_COPY_FILE_RANGE=1"
fi
rm -f /tmp/cxnfs-fuse-copy-file-range-check.o

cc -Wall -Wextra -Werror -D_FILE_OFFSET_BITS=64 \
  $EXTRA_CFLAGS \
  $(pkg-config --cflags fuse) \
  "$SCRIPT_DIR/satfs_fuse.c" \
  $(pkg-config --libs fuse) \
  -lpthread \
  -o "$OUTPUT"

ln -sf "$(basename -- "$OUTPUT")" "$(dirname -- "$OUTPUT")/cxnfs-fuse"

echo "Built $OUTPUT (and cxnfs-fuse symlink)"
