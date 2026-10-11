#define _XOPEN_SOURCE 700

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../cxnfs_blockfs_disk.h"
#include "../cxnfs_blockfs_meta.h"
#include "cxnfs_tool_common.h"

enum {
	MIN_BLOCKS = 128,
};

struct mkfs_options {
	const char *path;
	const char *label;
	const char *metadata_root;
	const char *report_path;
	uint64_t blocks;
	int force;
};

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s <image-or-device> --blocks <count> [--label <name>] [--metadata-root <path>] [--report <path>] [--force]\n",
		prog);
}

static int parse_u64(const char *text, uint64_t *out)
{
	char *end = NULL;
	unsigned long long value;

	errno = 0;
	value = strtoull(text, &end, 10);
	if (errno || end == text || *end != '\0')
		return -1;
	*out = (uint64_t)value;
	return 0;
}

static int parse_args(int argc, char **argv, struct mkfs_options *opts)
{
	int i;

	memset(opts, 0, sizeof(*opts));
	opts->label = "cxnfs";

	if (argc < 2)
		return -1;

	opts->path = argv[1];

	for (i = 2; i < argc; i++) {
		if (strcmp(argv[i], "--blocks") == 0 && i + 1 < argc) {
			if (parse_u64(argv[++i], &opts->blocks) != 0)
				return -1;
		} else if (strcmp(argv[i], "--label") == 0 && i + 1 < argc) {
			opts->label = argv[++i];
		} else if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			opts->metadata_root = argv[++i];
		} else if (strcmp(argv[i], "--report") == 0 && i + 1 < argc) {
			opts->report_path = argv[++i];
		} else if (strcmp(argv[i], "--force") == 0) {
			opts->force = 1;
		} else {
			return -1;
		}
	}

	if (opts->blocks < MIN_BLOCKS)
		return -1;
	if (strlen(opts->label) == 0 || strlen(opts->label) >= CXNFS_DISK_LABEL_LEN)
		return -1;
	for (const char *cursor = opts->label; *cursor; cursor++) {
		unsigned char c = (unsigned char)*cursor;

		if (c < 0x20 || c == 0x7f || c == '/')
			return -1;
	}
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

static void fill_uuid(__u8 uuid[CXNFS_DISK_UUID_LEN])
{
	FILE *random = fopen("/dev/urandom", "rb");

	if (random) {
		if (fread(uuid, 1, CXNFS_DISK_UUID_LEN, random) ==
		    CXNFS_DISK_UUID_LEN) {
			fclose(random);
			return;
		}
		fclose(random);
	}

	for (size_t i = 0; i < CXNFS_DISK_UUID_LEN; i++)
		uuid[i] = (uint8_t)(0xa5u ^ (uint8_t)i);
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

static void set_zone(struct cxnfs_disk_zone *zone, uint16_t kind,
		     uint64_t start_block, uint64_t block_count)
{
	zone->kind = cxnfs_to_le16(kind);
	zone->start_block = cxnfs_to_le64(start_block);
	zone->block_count = cxnfs_to_le64(block_count);
}

static int build_super(const struct mkfs_options *opts,
		       struct cxnfs_disk_super *super)
{
	uint64_t cursor = 1;

	memset(super, 0, sizeof(*super));
	memcpy(super->magic, CXNFS_DISK_MAGIC, CXNFS_DISK_MAGIC_LEN);
	super->version = cxnfs_to_le32(CXNFS_DISK_VERSION);
	super->block_size = cxnfs_to_le32(CXNFS_DISK_BLOCK_SIZE);
	fill_uuid(super->fs_uuid);
	memcpy(super->label, opts->label, strlen(opts->label));
	super->flags = cxnfs_to_le64(CXNFS_DISK_FLAG_OS_STRUCTURES_REQUIRED |
				     CXNFS_DISK_FLAG_TRAILING_METADATA_ALLOWED);
	super->total_blocks = cxnfs_to_le64(opts->blocks);
	super->root_inode = cxnfs_to_le64(1);
	super->max_cords = cxnfs_to_le32(CXNFS_DISK_MAX_CORDS);
	super->readahead_pages =
		cxnfs_to_le32(CXNFS_DISK_DEFAULT_READAHEAD_PAGES);

	if (opts->blocks <= cursor)
		return -1;

	super->container_cache_start_block = cxnfs_to_le64(cursor);
	super->container_cache_block_count = cxnfs_to_le64(opts->blocks - cursor);
	set_zone(&super->zones[0], CXNFS_ZONE_CONTAINER_CACHE, cursor,
		 opts->blocks - cursor);
	super->zone_count = cxnfs_to_le32(1);
	return 0;
}

static int mkdir_p(const char *path)
{
	char copy[PATH_MAX];
	char *cursor;

	if (strlen(path) >= sizeof(copy))
		return -1;
	strcpy(copy, path);

	for (cursor = copy + 1; *cursor; cursor++) {
		if (*cursor != '/')
			continue;
		*cursor = '\0';
		if (mkdir(copy, 0700) != 0 && errno != EEXIST)
			return -1;
		*cursor = '/';
	}

	if (mkdir(copy, 0700) != 0 && errno != EEXIST)
		return -1;
	return 0;
}

static int ensure_metadata_subdir(const char *root, const char *subdir)
{
	char path[PATH_MAX];

	if (snprintf(path, sizeof(path), "%s/%s", root, subdir) >=
	    (int)sizeof(path))
		return -1;
	return mkdir_p(path);
}

static int preflight_metadata_root(const struct mkfs_options *opts)
{
	char root_header_path[PATH_MAX];
	struct stat st;

	if (!opts->metadata_root)
		return 0;
	if (stat(opts->metadata_root, &st) == 0) {
		if (!S_ISDIR(st.st_mode)) {
			fprintf(stderr, "%s exists and is not a directory\n",
				opts->metadata_root);
			return -1;
		}
	} else if (errno != ENOENT) {
		perror(opts->metadata_root);
		return -1;
	}
	if (snprintf(root_header_path, sizeof(root_header_path), "%s/root.cxn",
		     opts->metadata_root) >= (int)sizeof(root_header_path))
		return -1;
	if (!opts->force && stat(root_header_path, &st) == 0) {
		fprintf(stderr, "%s already exists; use --force to replace it\n",
			root_header_path);
		return -1;
	}
	return 0;
}

static int write_metadata_root(const struct mkfs_options *opts,
			       const struct cxnfs_disk_super *super)
{
	static const char *subdirs[] = {
		"auth", "namespace", "files", "manifests",
		"chunks", "cache", "journals", "allocation",
	};
	struct cxnfs_meta_root_header header;
	char root_header_path[PATH_MAX];
	int fd;

	if (!opts->metadata_root)
		return 0;

	if (mkdir_p(opts->metadata_root) != 0)
		return -1;
	for (size_t i = 0; i < sizeof(subdirs) / sizeof(subdirs[0]); i++) {
		if (ensure_metadata_subdir(opts->metadata_root, subdirs[i]) != 0)
			return -1;
	}

	memset(&header, 0, sizeof(header));
	memcpy(header.magic, CXNFS_META_ROOT_MAGIC, CXNFS_META_MAGIC_LEN);
	header.version = cxnfs_to_le32(CXNFS_META_VERSION);
	header.header_size = cxnfs_to_le32(sizeof(header));
	memcpy(header.fs_uuid, super->fs_uuid, CXNFS_DISK_UUID_LEN);
	header.flags = cxnfs_to_le64(CXNFS_META_FLAG_BOOTSTRAP_READY);
	header.max_cords = super->max_cords;
	header.readahead_pages = super->readahead_pages;

	if (snprintf(root_header_path, sizeof(root_header_path), "%s/root.cxn",
		     opts->metadata_root) >= (int)sizeof(root_header_path))
		return -1;

	fd = open(root_header_path,
		  O_WRONLY | O_CREAT | (opts->force ? O_TRUNC : O_EXCL), 0600);
	if (fd < 0)
		return -1;
	if (cxnfs_pwrite_full(fd, &header, sizeof(header), 0) != 0) {
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

static int write_mkfs_report(const struct mkfs_options *opts,
			     const struct cxnfs_disk_super *super,
			     off_t image_bytes)
{
	char uuid[CXNFS_DISK_UUID_LEN * 2 + 1];
	int fd;

	if (!opts->report_path)
		return 0;

	uuid_hex(super->fs_uuid, uuid);
	fd = open(opts->report_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	if (dprintf(fd,
		    "cxnfs.blockfs.mkfs.v1\n"
		    "path=%s\n"
		    "blocks=%llu\n"
		    "block_size=%u\n"
		    "image_bytes=%lld\n"
		    "label=%s\n"
		    "fs_uuid=%s\n"
		    "metadata_root=%s\n"
		    "metadata_root_initialized=%s\n"
		    "max_cords=%u\n"
		    "readahead_pages=%u\n"
		    "status=created\n",
		    opts->path,
		    (unsigned long long)opts->blocks,
		    CXNFS_DISK_BLOCK_SIZE,
		    (long long)image_bytes,
		    opts->label,
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
	struct mkfs_options opts;
	struct cxnfs_disk_super super;
	struct stat target_stat;
	off_t image_bytes;
	int fd;
	int open_flags;
	int target_exists;

	_Static_assert(sizeof(struct cxnfs_disk_super) == CXNFS_DISK_BLOCK_SIZE,
		       "CXNFS disk superblock must be exactly one block");

	if (parse_args(argc, argv, &opts) != 0) {
		usage(argv[0]);
		return 2;
	}
	if (image_size_bytes(opts.blocks, &image_bytes) != 0) {
		fprintf(stderr, "image size overflows host off_t\n");
		return 2;
	}
	if (preflight_metadata_root(&opts) != 0)
		return 2;

	if (build_super(&opts, &super) != 0) {
		fprintf(stderr, "invalid CXNFS zone layout for %llu blocks\n",
			(unsigned long long)opts.blocks);
		return 2;
	}

	if (stat(opts.path, &target_stat) == 0) {
		target_exists = 1;
	} else if (errno == ENOENT) {
		target_exists = 0;
	} else {
		perror(opts.path);
		return 1;
	}
	if (target_exists && S_ISDIR(target_stat.st_mode)) {
		fprintf(stderr, "%s is a directory\n", opts.path);
		return 2;
	}
	if (target_exists && !opts.force) {
		fprintf(stderr, "%s already exists; use --force to format it\n",
			opts.path);
		return 2;
	}

	open_flags = O_RDWR;
	if (!target_exists)
		open_flags |= O_CREAT | O_EXCL;
	fd = open(opts.path, open_flags, 0600);
	if (fd < 0) {
		perror("open");
		return 1;
	}
	if (fstat(fd, &target_stat) != 0) {
		perror("fstat");
		close(fd);
		return 1;
	}
	if (!S_ISREG(target_stat.st_mode)
#ifdef S_ISBLK
	    && !S_ISBLK(target_stat.st_mode)
#endif
#ifdef S_ISCHR
	    && !S_ISCHR(target_stat.st_mode)
#endif
	) {
		fprintf(stderr, "%s is not a regular image or block device\n",
			opts.path);
		close(fd);
		return 2;
	}

	if (S_ISREG(target_stat.st_mode) && ftruncate(fd, image_bytes) != 0) {
		perror("ftruncate");
		close(fd);
		return 1;
	}

	if (cxnfs_pwrite_full(fd, &super, sizeof(super), 0) != 0) {
		perror("write superblock");
		close(fd);
		return 1;
	}

	if (fsync(fd) != 0) {
		perror("fsync");
		close(fd);
		return 1;
	}

	close(fd);
	if (write_metadata_root(&opts, &super) != 0) {
		perror("metadata root");
		return 1;
	}
	if (write_mkfs_report(&opts, &super, image_bytes) != 0) {
		perror("write report");
		return 1;
	}
	printf("created %s as cxnfsblk image: blocks=%llu block_size=%d label=%s\n",
	       opts.path, (unsigned long long)opts.blocks,
	       CXNFS_DISK_BLOCK_SIZE, opts.label);
	if (opts.metadata_root)
		printf("initialized metadata root: %s\n", opts.metadata_root);
	return 0;
}
