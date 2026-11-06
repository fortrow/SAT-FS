#define _XOPEN_SOURCE 700

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../satfs_blockfs_disk.h"
#include "../satfs_blockfs_meta.h"
#include "satfs_tool_common.h"

struct fsck_options {
	const char *path;
	const char *metadata_root;
	const char *report_path;
	int quiet;
};

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s <image-or-device> [--metadata-root <path>] [--report <path>] [--quiet]\n",
		prog);
}

static int parse_args(int argc, char **argv, struct fsck_options *opts)
{
	int i;

	memset(opts, 0, sizeof(*opts));
	if (argc < 2)
		return -1;
	opts->path = argv[1];

	for (i = 2; i < argc; i++) {
		if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			opts->metadata_root = argv[++i];
		} else if (strcmp(argv[i], "--report") == 0 && i + 1 < argc) {
			opts->report_path = argv[++i];
		} else if (strcmp(argv[i], "--quiet") == 0) {
			opts->quiet = 1;
		} else {
			return -1;
		}
	}

	return 0;
}

static void label_string(const struct cxnfs_disk_super *super, char *out,
			 size_t out_size)
{
	size_t len = 0;

	while (len < CXNFS_DISK_LABEL_LEN && super->label[len] != '\0')
		len++;
	if (len >= out_size)
		len = out_size - 1;
	memcpy(out, super->label, len);
	out[len] = '\0';
}

static void uuid_hex(const __u8 uuid[CXNFS_DISK_UUID_LEN],
		     char out[CXNFS_DISK_UUID_LEN * 2 + 1])
{
	static const char hex[] = "0123456789abcdef";

	for (size_t i = 0; i < CXNFS_DISK_UUID_LEN; i++) {
		out[i * 2] = hex[(uuid[i] >> 4) & 0xf];
		out[i * 2 + 1] = hex[uuid[i] & 0xf];
	}
	out[CXNFS_DISK_UUID_LEN * 2] = '\0';
}

static int check_zone(const struct cxnfs_disk_zone *zone, uint64_t total_blocks,
		      const char *name)
{
	uint64_t start = cxnfs_from_le64(zone->start_block);
	uint64_t count = cxnfs_from_le64(zone->block_count);

	if (count == 0) {
		fprintf(stderr, "%s zone is empty\n", name);
		return -1;
	}
	if (start == 0) {
		fprintf(stderr, "%s zone starts at superblock\n", name);
		return -1;
	}
	if (start >= total_blocks || count > total_blocks - start) {
		fprintf(stderr, "%s zone is outside device bounds\n", name);
		return -1;
	}
	return 0;
}

static int check_super(const struct cxnfs_disk_super *super, uint64_t *blocks)
{
	uint32_t version = cxnfs_from_le32(super->version);
	uint32_t block_size = cxnfs_from_le32(super->block_size);
	uint64_t flags = cxnfs_from_le64(super->flags);
	uint64_t total_blocks = cxnfs_from_le64(super->total_blocks);
	uint64_t root_inode = cxnfs_from_le64(super->root_inode);
	uint64_t container_start =
		cxnfs_from_le64(super->container_cache_start_block);
	uint64_t container_count =
		cxnfs_from_le64(super->container_cache_block_count);
	uint32_t max_cords = cxnfs_from_le32(super->max_cords);
	uint32_t readahead_pages = cxnfs_from_le32(super->readahead_pages);
	uint32_t zone_count = cxnfs_from_le32(super->zone_count);
	uint64_t zone_start;
	uint64_t zone_count_blocks;

	if (memcmp(super->magic, SATFS_DISK_MAGIC, SATFS_DISK_MAGIC_LEN) != 0 &&
	    memcmp(super->magic, CXNFS_DISK_MAGIC, CXNFS_DISK_MAGIC_LEN) != 0) {
		fprintf(stderr, "bad SAT-FS magic\n");
		return -1;
	}
	if (version != CXNFS_DISK_VERSION) {
		fprintf(stderr, "unsupported version: %u\n", version);
		return -1;
	}
	if (block_size != CXNFS_DISK_BLOCK_SIZE) {
		fprintf(stderr, "bad block size: %u\n", block_size);
		return -1;
	}
	if (total_blocks < 128) {
		fprintf(stderr, "too few blocks: %llu\n",
			(unsigned long long)total_blocks);
		return -1;
	}
	if (!(flags & CXNFS_DISK_FLAG_OS_STRUCTURES_REQUIRED)) {
		fprintf(stderr, "missing OS-structures-required flag\n");
		return -1;
	}
	if (!(flags & CXNFS_DISK_FLAG_TRAILING_METADATA_ALLOWED)) {
		fprintf(stderr, "missing trailing-metadata flag\n");
		return -1;
	}
	if (root_inode != 1) {
		fprintf(stderr, "bad root inode: %llu\n",
			(unsigned long long)root_inode);
		return -1;
	}
	if (max_cords == 0 || max_cords > CXNFS_DISK_MAX_CORDS) {
		fprintf(stderr, "bad max cords: %u\n", max_cords);
		return -1;
	}
	if (readahead_pages == 0) {
		fprintf(stderr, "bad readahead pages: %u\n", readahead_pages);
		return -1;
	}
	if (zone_count != 1) {
		fprintf(stderr, "bad zone count: %u\n", zone_count);
		return -1;
	}
	if (cxnfs_from_le16(super->zones[0].kind) !=
	    CXNFS_ZONE_CONTAINER_CACHE) {
		fprintf(stderr, "first zone must be container cache\n");
		return -1;
	}
	if (check_zone(&super->zones[0], total_blocks, "container-cache") != 0)
		return -1;
	zone_start = cxnfs_from_le64(super->zones[0].start_block);
	zone_count_blocks = cxnfs_from_le64(super->zones[0].block_count);
	if (container_start != zone_start || container_count != zone_count_blocks) {
		fprintf(stderr, "container-cache fields do not match zone table\n");
		return -1;
	}

	*blocks = total_blocks;
	return 0;
}

static int image_size_bytes(uint64_t blocks, off_t *out)
{
	uint64_t bytes;

	if (blocks > UINT64_MAX / CXNFS_DISK_BLOCK_SIZE)
		return -1;
	bytes = blocks * (uint64_t)CXNFS_DISK_BLOCK_SIZE;
	if (bytes > (uint64_t)LLONG_MAX)
		return -1;
	*out = (off_t)bytes;
	return 0;
}

static int check_target_size(int fd, uint64_t blocks)
{
	struct stat st;
	off_t expected_size;

	if (fstat(fd, &st) != 0) {
		perror("fstat");
		return -1;
	}
	if (!S_ISREG(st.st_mode))
		return 0;
	if (image_size_bytes(blocks, &expected_size) != 0) {
		fprintf(stderr, "image size overflows host off_t\n");
		return -1;
	}
	if (st.st_size != expected_size) {
		fprintf(stderr,
			"regular image size mismatch: expected %lld bytes got %lld bytes\n",
			(long long)expected_size, (long long)st.st_size);
		return -1;
	}
	return 0;
}

static int require_dir(const char *root, const char *subdir)
{
	char path[PATH_MAX];
	struct stat st;

	if (snprintf(path, sizeof(path), "%s/%s", root, subdir) >=
	    (int)sizeof(path))
		return -1;
	if (stat(path, &st) != 0) {
		perror(path);
		return -1;
	}
	if (!S_ISDIR(st.st_mode)) {
		fprintf(stderr, "%s is not a directory\n", path);
		return -1;
	}
	return 0;
}

static int check_metadata_root(const char *root,
			       const struct cxnfs_disk_super *super)
{
	static const char *subdirs[] = {
		"auth", "namespace", "files", "manifests",
		"chunks", "cache", "journals", "allocation",
	};
	struct cxnfs_meta_root_header header;
	char path[PATH_MAX];
	struct stat st;
	int fd;

	if (stat(root, &st) != 0) {
		perror(root);
		return -1;
	}
	if (!S_ISDIR(st.st_mode)) {
		fprintf(stderr, "%s is not a directory\n", root);
		return -1;
	}
	for (size_t i = 0; i < sizeof(subdirs) / sizeof(subdirs[0]); i++) {
		if (require_dir(root, subdirs[i]) != 0)
			return -1;
	}

	if (snprintf(path, sizeof(path), "%s/root.sat", root) >=
	    (int)sizeof(path))
		return -1;
	if (stat(path, &st) != 0) {
		if (snprintf(path, sizeof(path), "%s/root.cxn", root) >=
		    (int)sizeof(path))
			return -1;
		if (stat(path, &st) != 0) {
			perror(path);
			return -1;
		}
	}
	if (!S_ISREG(st.st_mode)) {
		fprintf(stderr, "%s is not a regular metadata-root header\n",
			path);
		return -1;
	}
	if (st.st_size != (off_t)sizeof(header)) {
		fprintf(stderr,
			"%s metadata-root header size mismatch: expected %zu bytes got %lld bytes\n",
			path, sizeof(header), (long long)st.st_size);
		return -1;
	}
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		perror(path);
		return -1;
	}
	if (cxnfs_pread_full(fd, &header, sizeof(header), 0) != 0) {
		perror(path);
		close(fd);
		return -1;
	}
	close(fd);

	if (memcmp(header.magic, SATFS_META_ROOT_MAGIC, SATFS_META_MAGIC_LEN) != 0 &&
	    memcmp(header.magic, CXNFS_META_ROOT_MAGIC, CXNFS_META_MAGIC_LEN) != 0) {
		fprintf(stderr, "%s has bad metadata-root magic\n", path);
		return -1;
	}
	if (cxnfs_from_le32(header.version) != CXNFS_META_VERSION) {
		fprintf(stderr, "%s has unsupported metadata-root version\n", path);
		return -1;
	}
	if (cxnfs_from_le32(header.header_size) != sizeof(header)) {
		fprintf(stderr, "%s has unsupported metadata-root header size\n",
			path);
		return -1;
	}
	if (memcmp(header.fs_uuid, super->fs_uuid, CXNFS_DISK_UUID_LEN) != 0) {
		fprintf(stderr, "%s UUID does not match block image\n", path);
		return -1;
	}
	if (!(cxnfs_from_le64(header.flags) & CXNFS_META_FLAG_BOOTSTRAP_READY)) {
		fprintf(stderr, "%s is not bootstrap-ready\n", path);
		return -1;
	}
	if (header.max_cords != super->max_cords) {
		fprintf(stderr, "%s max cords does not match block image\n", path);
		return -1;
	}
	if (header.readahead_pages != super->readahead_pages) {
		fprintf(stderr, "%s readahead pages does not match block image\n",
			path);
		return -1;
	}

	return 0;
}

static int write_fsck_report(const struct fsck_options *opts,
			     const struct cxnfs_disk_super *super,
			     uint64_t blocks)
{
	char label[CXNFS_DISK_LABEL_LEN];
	char uuid[CXNFS_DISK_UUID_LEN * 2 + 1];
	int fd;

	if (!opts->report_path)
		return 0;

	label_string(super, label, sizeof(label));
	uuid_hex(super->fs_uuid, uuid);
	fd = open(opts->report_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	if (dprintf(fd,
		    "satfs.blockfs.fsck.v1\n"
		    "cxnfs.blockfs.fsck.v1\n"
		    "path=%s\n"
		    "blocks=%llu\n"
		    "block_size=%u\n"
		    "label=%s\n"
		    "fs_uuid=%s\n"
		    "metadata_root=%s\n"
		    "metadata_root_checked=%s\n"
		    "max_cords=%u\n"
		    "readahead_pages=%u\n"
		    "status=clean\n",
		    opts->path,
		    (unsigned long long)blocks,
		    CXNFS_DISK_BLOCK_SIZE,
		    label,
		    uuid,
		    opts->metadata_root ? opts->metadata_root : "",
		    opts->metadata_root ? "true" : "false",
		    cxnfs_from_le32(super->max_cords),
		    cxnfs_from_le32(super->readahead_pages)) < 0) {
		close(fd);
		return -1;
	}
	if (fsync(fd) != 0) {
		close(fd);
		return -1;
	}
	close(fd);
	return 0;
}

int main(int argc, char **argv)
{
	struct fsck_options opts;
	struct cxnfs_disk_super super;
	uint64_t blocks = 0;
	char label[CXNFS_DISK_LABEL_LEN];
	int fd;

	_Static_assert(sizeof(struct cxnfs_disk_super) == CXNFS_DISK_BLOCK_SIZE,
		       "CXNFS disk superblock must be exactly one block");

	if (parse_args(argc, argv, &opts) != 0) {
		usage(argv[0]);
		return 2;
	}

	fd = open(opts.path, O_RDONLY);
	if (fd < 0) {
		perror("open");
		return 1;
	}
	if (cxnfs_pread_full(fd, &super, sizeof(super), 0) != 0) {
		perror("read superblock");
		close(fd);
		return 1;
	}

	if (check_super(&super, &blocks) != 0) {
		close(fd);
		return 1;
	}
	if (check_target_size(fd, blocks) != 0) {
		close(fd);
		return 1;
	}
	close(fd);
	if (opts.metadata_root &&
	    check_metadata_root(opts.metadata_root, &super) != 0)
		return 1;
	if (write_fsck_report(&opts, &super, blocks) != 0) {
		perror("write report");
		return 1;
	}

	if (!opts.quiet) {
		label_string(&super, label, sizeof(label));
		printf("cxnfsblk ok: path=%s label=%s blocks=%llu block_size=%u max_cords=%u\n",
		       opts.path,
		       label,
		       (unsigned long long)blocks,
		       CXNFS_DISK_BLOCK_SIZE,
		       cxnfs_from_le32(super.max_cords));
		if (opts.metadata_root)
			printf("metadata root ok: %s\n", opts.metadata_root);
	}
	return 0;
}
