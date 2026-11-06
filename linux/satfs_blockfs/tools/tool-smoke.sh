#!/bin/sh
set -eu

tool_dir=${1:-.}
mkfs_tool="$tool_dir/mkfs.satfsblk"
[ -x "$mkfs_tool" ] || mkfs_tool="$tool_dir/mkfs.cxnfsblk"
fsck_tool="$tool_dir/fsck.satfsblk"
[ -x "$fsck_tool" ] || fsck_tool="$tool_dir/fsck.cxnfsblk"
ctl_tool="$tool_dir/satfs-blockfs-ctl"
[ -x "$ctl_tool" ] || ctl_tool="$tool_dir/cxnfs-blockfs-ctl"

if [ ! -x "$mkfs_tool" ]; then
	echo "missing mkfs tool: $mkfs_tool" >&2
	exit 2
fi
if [ ! -x "$fsck_tool" ]; then
	echo "missing fsck tool: $fsck_tool" >&2
	exit 2
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/cxnfs-blockfs-tools.XXXXXX")
cleanup() {
	rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

image="$tmp/cxnfs.img"
metadata_root="$tmp/metadata"
report_dir=${CXNFS_TOOL_SMOKE_REPORT_DIR:-"$tmp/reports"}

mkdir -p "$report_dir"

require_contains() {
	file=$1
	expected=$2
	if ! grep -F -- "$expected" "$file" >/dev/null; then
		echo "$file missing expected text: $expected" >&2
		exit 1
	fi
}

"$mkfs_tool" "$image" \
	--blocks 128 \
	--label smoke \
	--metadata-root "$metadata_root" \
	--report "$report_dir/mkfs-initial.report" >/dev/null
"$fsck_tool" "$image" \
	--metadata-root "$metadata_root" \
	--report "$report_dir/fsck-initial.report" \
	--quiet
require_contains "$report_dir/mkfs-initial.report" "cxnfs.blockfs.mkfs.v1"
require_contains "$report_dir/mkfs-initial.report" "status=created"
require_contains "$report_dir/fsck-initial.report" "cxnfs.blockfs.fsck.v1"
require_contains "$report_dir/fsck-initial.report" "status=clean"

if [ -x "$ctl_tool" ]; then
	mkdir -p "$metadata_root/namespace" "$metadata_root/extent-hints"
	cat >"$metadata_root/namespace/current.cxn" <<EOF
version=1
generation=7
file.count=1
file.0.inode=77
file.0.path=datasets/train/sample.bin
file.0.size=4096
file.0.mode=0444
EOF
	cat >"$metadata_root/extent-hints/current.cxn" <<EOF
version=1
generation=7
extent.count=1
extent.0.inode=77
extent.0.file-offset=0
extent.0.payload-block=1
extent.0.payload-offset=0
extent.0.payload-length=4096
extent.0.hash=0000000000000000000000000000000000000000000000000000000000000000
EOF
	"$ctl_tool" sync-sidecars \
		--metadata-root "$metadata_root" \
		--require-extent-hints \
		--dry-run \
		--report "$report_dir/sidecar-sync-dry-run.report" >/dev/null
	require_contains "$report_dir/sidecar-sync-dry-run.report" "cxnfs.blockfs.sidecar-sync.v1"
	require_contains "$report_dir/sidecar-sync-dry-run.report" "dry_run=true"
	require_contains "$report_dir/sidecar-sync-dry-run.report" "clear_before_publish=true"
	require_contains "$report_dir/sidecar-sync-dry-run.report" "registered_files=1"
	require_contains "$report_dir/sidecar-sync-dry-run.report" "registered_extents=1"
fi

if "$mkfs_tool" "$image" --blocks 128 >"$tmp/no-force.out" 2>"$tmp/no-force.err"; then
	echo "mkfs unexpectedly replaced an existing image without --force" >&2
	exit 1
fi

"$mkfs_tool" "$image" \
	--blocks 256 \
	--label smoke2 \
	--metadata-root "$metadata_root" \
	--report "$report_dir/mkfs-force.report" \
	--force >/dev/null
"$fsck_tool" "$image" \
	--metadata-root "$metadata_root" \
	--report "$report_dir/fsck-force.report" \
	--quiet
require_contains "$report_dir/mkfs-force.report" "blocks=256"
require_contains "$report_dir/fsck-force.report" "blocks=256"

broken_metadata_root="$tmp/broken-metadata"
cp -R "$metadata_root" "$broken_metadata_root"
printf "short" >"$broken_metadata_root/root.cxn"
if "$fsck_tool" "$image" --metadata-root "$broken_metadata_root" --quiet >"$tmp/broken-meta.out" 2>"$tmp/broken-meta.err"; then
	echo "fsck unexpectedly accepted a truncated metadata-root header" >&2
	exit 1
fi

truncated="$tmp/truncated.img"
dd if="$image" of="$truncated" bs=4096 count=1 >/dev/null 2>&1
if "$fsck_tool" "$truncated" --quiet >"$tmp/truncated.out" 2>"$tmp/truncated.err"; then
	echo "fsck unexpectedly accepted a truncated regular image" >&2
	exit 1
fi

bad_magic="$tmp/bad-magic.img"
cp "$image" "$bad_magic"
printf "BADMAGIC" | dd of="$bad_magic" bs=1 seek=0 conv=notrunc >/dev/null 2>&1
if "$fsck_tool" "$bad_magic" --quiet >"$tmp/bad-magic.out" 2>"$tmp/bad-magic.err"; then
	echo "fsck unexpectedly accepted a corrupt superblock magic" >&2
	exit 1
fi

not_dir="$tmp/not-a-directory"
bad_image="$tmp/bad-meta-root.img"
printf "not a directory\n" >"$not_dir"
if "$mkfs_tool" "$bad_image" \
	--blocks 128 \
	--metadata-root "$not_dir" >"$tmp/bad-root.out" 2>"$tmp/bad-root.err"; then
	echo "mkfs unexpectedly accepted a file as metadata root" >&2
	exit 1
fi
if [ -e "$bad_image" ]; then
	echo "mkfs created an image after metadata-root preflight failed" >&2
	exit 1
fi

printf "cxnfs blockfs tool smoke ok\n"
