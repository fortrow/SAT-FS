#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="${CXNFS_TOOLS_DIR:-$ROOT_DIR/tools}"
IMAGE="${CXNFS_IMAGE:-/tmp/cxnfs.img}"
META_ROOT="${CXNFS_METADATA_ROOT:-/var/lib/cxnfs/vm-smoke}"
MOUNTPOINT="${CXNFS_MOUNTPOINT:-/mnt/cxnfs-disk}"
IMAGE_B="${CXNFS_IMAGE_B:-/tmp/cxnfs-b.img}"
META_ROOT_B="${CXNFS_METADATA_ROOT_B:-/var/lib/cxnfs/vm-smoke-b}"
MOUNTPOINT_B="${CXNFS_MOUNTPOINT_B:-/mnt/cxnfs-disk-b}"
BLOCKS="${CXNFS_BLOCKS:-262144}"
LABEL="${CXNFS_LABEL:-vm-smoke}"
PAYLOAD="${CXNFS_PAYLOAD:-/tmp/cxnfs-payload.bin}"
INSTALL_DEPS="${CXNFS_INSTALL_DEPS:-0}"
USE_CMAKE="${CXNFS_USE_CMAKE:-1}"
CMAKE_BUILD_DIR="${CXNFS_CMAKE_BUILD_DIR:-$ROOT_DIR/build}"
BUILD_FROM_SOURCE="${CXNFS_BUILD_FROM_SOURCE:-1}"
BAD_HASH="0000000000000000000000000000000000000000000000000000000000000000"

LOOP_DEV=""
LOOP_DEV_B=""
MOUNTED=0
MOUNTED_B=0
LOADED=0
MKFS="$TOOLS_DIR/mkfs.cxnfsblk"
FSCK="$TOOLS_DIR/fsck.cxnfsblk"
CTL_PROBE="$TOOLS_DIR/ctl_probe"
CTL="$TOOLS_DIR/cxnfs-blockfs-ctl"

cleanup() {
	if [ "$MOUNTED_B" -eq 1 ]; then
		sudo umount "$MOUNTPOINT_B" || true
	fi
	if [ "$MOUNTED" -eq 1 ]; then
		sudo umount "$MOUNTPOINT" || true
	fi
	if [ -n "$LOOP_DEV_B" ]; then
		sudo losetup -d "$LOOP_DEV_B" || true
	fi
	if [ -n "$LOOP_DEV" ]; then
		sudo losetup -d "$LOOP_DEV" || true
	fi
	if [ "$LOADED" -eq 1 ]; then
		sudo rmmod cxnfs_blockfs || true
	fi
}
trap cleanup EXIT

if [ "$USE_CMAKE" = "1" ]; then
	if ! command -v cmake >/dev/null 2>&1 || ! command -v make >/dev/null 2>&1; then
		if [ "$INSTALL_DEPS" != "1" ]; then
			echo "cmake/make is missing. Set CXNFS_INSTALL_DEPS=1 or install cmake and build-essential." >&2
			exit 1
		fi
		sudo apt-get update
		sudo apt-get install -y cmake build-essential "linux-headers-$(uname -r)"
	fi
	cmake -S "$ROOT_DIR" -B "$CMAKE_BUILD_DIR" \
		-DCXNFS_DOWNLOAD_DEPS=ON
	cmake --build "$CMAKE_BUILD_DIR" --target prepare-build-env
	cmake --build "$CMAKE_BUILD_DIR" --target tools
	cmake --build "$CMAKE_BUILD_DIR" --target module
	MKFS="$CMAKE_BUILD_DIR/tools/mkfs.cxnfsblk"
	FSCK="$CMAKE_BUILD_DIR/tools/fsck.cxnfsblk"
	CTL_PROBE="$CMAKE_BUILD_DIR/tools/ctl_probe"
	CTL="$CMAKE_BUILD_DIR/tools/cxnfs-blockfs-ctl"
elif [ "$INSTALL_DEPS" = "1" ]; then
	sudo apt-get update
	sudo apt-get install -y build-essential cmake "linux-headers-$(uname -r)"
fi

if [ "$USE_CMAKE" != "1" ] && [ "$BUILD_FROM_SOURCE" = "1" ]; then
	make -C "$TOOLS_DIR"
	make -C "$ROOT_DIR"
	cc -std=c11 -Wall -Wextra -Werror -O2 \
		-o "$TOOLS_DIR/ctl_probe" "$TOOLS_DIR/ctl_probe.c"
fi

sudo "$MKFS" "$IMAGE" \
	--blocks "$BLOCKS" \
	--label "$LABEL" \
	--metadata-root "$META_ROOT" \
	--force
sudo "$FSCK" "$IMAGE" --metadata-root "$META_ROOT"
sudo "$MKFS" "$IMAGE_B" \
	--blocks "$BLOCKS" \
	--label "$LABEL-b" \
	--metadata-root "$META_ROOT_B" \
	--force
sudo "$FSCK" "$IMAGE_B" --metadata-root "$META_ROOT_B"
dd if=/dev/zero of="$PAYLOAD" bs=4096 count=1 status=none
printf 'cxnfs verified payload from blockfs\n' | dd of="$PAYLOAD" conv=notrunc status=none
PAYLOAD_HASH="$( { printf 'cxnfs.chunk.v1\0'; cat "$PAYLOAD"; } | sha256sum | awk '{print $1}' )"
sudo dd if="$PAYLOAD" of="$IMAGE" bs=4096 seek=1 conv=notrunc status=none

sudo insmod "$ROOT_DIR/cxnfs_blockfs.ko"
LOADED=1
test -e /dev/cxnfs_blockfs_ctl

sudo "$CTL_PROBE" /dev/cxnfs_blockfs_ctl
sudo "$CTL" stats

LOOP_DEV="$(sudo losetup --find --show "$IMAGE")"
LOOP_DEV_B="$(sudo losetup --find --show "$IMAGE_B")"
sudo mkdir -p "$MOUNTPOINT"
sudo mkdir -p "$MOUNTPOINT_B"
sudo mount -t cxnfsblk \
	-o "metadata_root=$META_ROOT,max_cords=64,readahead_pages=128,miss_wait_ms=3000" \
	"$LOOP_DEV" "$MOUNTPOINT"
MOUNTED=1
sudo mount -t cxnfsblk \
	-o "metadata_root=$META_ROOT_B,max_cords=8,readahead_pages=32,miss_wait_ms=0" \
	"$LOOP_DEV_B" "$MOUNTPOINT_B"
MOUNTED_B=1

sudo "$CTL" register-file \
	--metadata-root "$META_ROOT" \
	--inode 2 \
	--path datasets/train/sample.bin \
	--size 4096 \
	--mode 0444
sudo "$CTL" register-extent \
	--metadata-root "$META_ROOT" \
	--inode 2 \
	--file-offset 0 \
	--payload-block 1 \
	--payload-offset 0 \
	--payload-length 4096 \
	--hash "$BAD_HASH"
sudo "$CTL" stats

find "$MOUNTPOINT" -maxdepth 3 -print
find "$MOUNTPOINT_B" -maxdepth 3 -print
test -d "$MOUNTPOINT/datasets"
test -d "$MOUNTPOINT/datasets/train"
test -f "$MOUNTPOINT/datasets/train/sample.bin"
if [ -e "$MOUNTPOINT_B/datasets" ]; then
	echo "second mount saw first mount namespace entries" >&2
	exit 1
fi
if cmp "$PAYLOAD" "$MOUNTPOINT/datasets/train/sample.bin" >/dev/null 2>&1; then
	echo "read unexpectedly succeeded with a mismatched extent hash" >&2
	exit 1
fi
STATS_AFTER_BAD_HASH="$(sudo "$CTL" stats)"
printf '%s\n' "$STATS_AFTER_BAD_HASH"
if ! printf '%s\n' "$STATS_AFTER_BAD_HASH" | grep -Eq 'hash_mismatches=[1-9][0-9]*'; then
	echo "stats did not report a hash mismatch" >&2
	exit 1
fi
sudo "$CTL" register-extent \
	--metadata-root "$META_ROOT" \
	--inode 2 \
	--file-offset 0 \
	--payload-block 1 \
	--payload-offset 0 \
	--payload-length 4096 \
	--hash "$PAYLOAD_HASH"
cmp "$PAYLOAD" "$MOUNTPOINT/datasets/train/sample.bin"
sudo "$CTL" invalidate \
	--metadata-root "$META_ROOT" \
	--inode 2 \
	--offset 0 \
	--length 4096
MISS_REPORT="$(mktemp)"
cmp "$PAYLOAD" "$MOUNTPOINT/datasets/train/sample.bin" >/dev/null 2>&1 &
READ_PID=$!
MISS_SEEN=0
for _ in $(seq 1 50); do
	sudo "$CTL" next-miss --metadata-root "$META_ROOT" --nonblock >"$MISS_REPORT"
	if grep -q "miss_event event_id=" "$MISS_REPORT"; then
		MISS_SEEN=1
		break
	fi
	sleep 0.1
done
if [ "$MISS_SEEN" -ne 1 ]; then
	cat "$MISS_REPORT" >&2 || true
	echo "read did not publish a miss event before timeout" >&2
	exit 1
fi
cat "$MISS_REPORT"
if ! grep -Eq 'mount_id=[1-9][0-9]*' "$MISS_REPORT"; then
	echo "miss event did not include a mount id" >&2
	exit 1
fi
if ! grep -q "metadata_root=$META_ROOT" "$MISS_REPORT"; then
	echo "miss event did not include the mounted metadata root" >&2
	exit 1
fi
if ! grep -q 'max_cords=64' "$MISS_REPORT"; then
	echo "miss event did not include the mounted cord cap" >&2
	exit 1
fi
if ! grep -q 'readahead_pages=128' "$MISS_REPORT"; then
	echo "miss event did not include the mounted readahead policy" >&2
	exit 1
fi
if ! grep -q 'miss_wait_ms=3000' "$MISS_REPORT"; then
	echo "miss event did not include the mounted wait policy" >&2
	exit 1
fi
if ! grep -q 'verify_hashes=true' "$MISS_REPORT"; then
	echo "miss event did not include the mounted hash policy" >&2
	exit 1
fi
sudo "$CTL" register-extent \
	--metadata-root "$META_ROOT" \
	--inode 2 \
	--file-offset 0 \
	--payload-block 1 \
	--payload-offset 0 \
	--payload-length 4096 \
	--hash "$PAYLOAD_HASH"
wait "$READ_PID"
STATS_AFTER_WAIT="$(sudo "$CTL" stats)"
printf '%s\n' "$STATS_AFTER_WAIT"
if ! printf '%s\n' "$STATS_AFTER_WAIT" | grep -Eq 'miss_events_queued=[1-9][0-9]*'; then
	echo "stats did not report queued read-miss events" >&2
	exit 1
fi
if ! printf '%s\n' "$STATS_AFTER_WAIT" | grep -Eq 'miss_wait_successes=[1-9][0-9]*'; then
	echo "stats did not report a successful miss wait" >&2
	exit 1
fi
if ! printf '%s\n' "$STATS_AFTER_WAIT" | grep -Eq 'hash_verifications=[1-9][0-9]*'; then
	echo "stats did not report hash verification attempts" >&2
	exit 1
fi
if ! printf '%s\n' "$STATS_AFTER_WAIT" | grep -Eq 'readahead_miss_windows=[1-9][0-9]*'; then
	echo "stats did not report coalesced readahead miss windows" >&2
	exit 1
fi
sudo "$CTL" invalidate \
	--metadata-root "$META_ROOT" \
	--inode 2 \
	--offset 0 \
	--length 4096
if cmp "$PAYLOAD" "$MOUNTPOINT/datasets/train/sample.bin" >/dev/null 2>&1; then
	echo "read unexpectedly succeeded after extent invalidation" >&2
	exit 1
fi
sudo "$CTL" stats
echo "cxnfsblk smoke test passed"
