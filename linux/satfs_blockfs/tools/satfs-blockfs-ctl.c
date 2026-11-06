#define _GNU_SOURCE

#include <errno.h>
#include <ctype.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include "../satfs_blockfs_uapi.h"

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage:\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] stats\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] clear-namespace [--metadata-root path|--mount-id n]\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] next-miss [--metadata-root path|--mount-id n] [--nonblock]\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] register-file [--metadata-root path|--mount-id n] --inode n --path relative/path --size n [--mode octal]\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] register-extent [--metadata-root path|--mount-id n] --inode n --file-offset n --payload-block n --payload-offset n --payload-length n --hash hex32 [--allow-unsupported]\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] invalidate [--metadata-root path|--mount-id n] --inode n --offset n --length n [--allow-unsupported]\n"
		"  %s [--device /dev/cxnfs_blockfs_ctl] sync-sidecars --metadata-root path [--require-extent-hints] [--allow-unsupported] [--dry-run] [--no-clear] [--report path]\n",
		prog, prog, prog, prog, prog, prog, prog);
}

static int parse_u64(const char *text, uint64_t *out)
{
	char *end = NULL;
	unsigned long long value;

	errno = 0;
	value = strtoull(text, &end, 0);
	if (errno || end == text || *end != '\0')
		return -1;
	*out = (uint64_t)value;
	return 0;
}

static int parse_u32(const char *text, uint32_t *out)
{
	uint64_t value;

	if (parse_u64(text, &value) != 0 || value > UINT32_MAX)
		return -1;
	*out = (uint32_t)value;
	return 0;
}

static int hex_nibble(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

static int parse_hash32(const char *hex, uint8_t out[32])
{
	if (strlen(hex) != 64)
		return -1;
	for (size_t i = 0; i < 32; i++) {
		int hi = hex_nibble(hex[i * 2]);
		int lo = hex_nibble(hex[i * 2 + 1]);

		if (hi < 0 || lo < 0)
			return -1;
		out[i] = (uint8_t)((hi << 4) | lo);
	}
	return 0;
}

static int copy_uapi_path(char out[CXNFS_BLOCKFS_PATH_MAX], const char *value,
			  const char *name)
{
	size_t len = strlen(value);

	if (len == 0 || len >= CXNFS_BLOCKFS_PATH_MAX) {
		fprintf(stderr, "%s is empty or too long\n", name);
		return -1;
	}
	memcpy(out, value, len + 1);
	return 0;
}

static int valid_path_component(const char *start, size_t len)
{
	if (len == 0 || len > NAME_MAX)
		return 0;
	if (len == 1 && start[0] == '.')
		return 0;
	if (len == 2 && start[0] == '.' && start[1] == '.')
		return 0;
	for (size_t i = 0; i < len; i++) {
		unsigned char c = (unsigned char)start[i];

		if (c < 0x20 || c == 0x7f)
			return 0;
	}
	return 1;
}

static int valid_relative_path(const char *path)
{
	const char *component;
	size_t len;

	if (path == NULL)
		return 0;
	len = strnlen(path, CXNFS_BLOCKFS_PATH_MAX);
	if (len == 0 || len >= CXNFS_BLOCKFS_PATH_MAX)
		return 0;
	if (path[0] == '/' || path[len - 1] == '/')
		return 0;
	if (strchr(path, '\\') != NULL)
		return 0;

	component = path;
	for (size_t i = 0; i <= len; i++) {
		if (path[i] != '/' && path[i] != '\0')
			continue;
		if (!valid_path_component(component, (size_t)(&path[i] - component)))
			return 0;
		component = &path[i + 1];
	}
	return 1;
}

struct property_entry {
	char *key;
	char *value;
};

struct property_set {
	struct property_entry *entries;
	size_t count;
	size_t capacity;
};

static char *trim_ascii(char *text)
{
	char *end;

	while (*text && isspace((unsigned char)*text))
		text++;
	end = text + strlen(text);
	while (end > text && isspace((unsigned char)end[-1]))
		*--end = '\0';
	return text;
}

static char *unescape_property_value(const char *text)
{
	size_t len = strlen(text);
	char *out = malloc(len + 1);
	size_t written = 0;

	if (!out)
		return NULL;
	for (size_t i = 0; i < len; i++) {
		if (text[i] != '\\' || i + 1 >= len) {
			out[written++] = text[i];
			continue;
		}
		i++;
		switch (text[i]) {
		case 't':
			out[written++] = '\t';
			break;
		case 'n':
			out[written++] = '\n';
			break;
		case 'r':
			out[written++] = '\r';
			break;
		default:
			out[written++] = text[i];
			break;
		}
	}
	out[written] = '\0';
	return out;
}

static void free_properties(struct property_set *set)
{
	if (!set)
		return;
	for (size_t i = 0; i < set->count; i++) {
		free(set->entries[i].key);
		free(set->entries[i].value);
	}
	free(set->entries);
	memset(set, 0, sizeof(*set));
}

static int add_property(struct property_set *set, const char *key,
			const char *value)
{
	struct property_entry *entries;

	if (set->count == set->capacity) {
		size_t next_capacity = set->capacity ? set->capacity * 2 : 32;

		entries = realloc(set->entries,
				  next_capacity * sizeof(set->entries[0]));
		if (!entries)
			return -1;
		set->entries = entries;
		set->capacity = next_capacity;
	}
	set->entries[set->count].key = unescape_property_value(key);
	set->entries[set->count].value = unescape_property_value(value);
	if (!set->entries[set->count].key || !set->entries[set->count].value)
		return -1;
	set->count++;
	return 0;
}

static int load_properties(const char *path, struct property_set *set)
{
	FILE *file;
	char *line = NULL;
	size_t line_capacity = 0;
	ssize_t line_len;
	int ret = 0;

	memset(set, 0, sizeof(*set));
	file = fopen(path, "r");
	if (!file) {
		perror(path);
		return -1;
	}

	while ((line_len = getline(&line, &line_capacity, file)) >= 0) {
		char *cursor;
		char *separator;
		char *key;
		char *value;

		(void)line_len;
		cursor = trim_ascii(line);
		if (!*cursor || *cursor == '#' || *cursor == '!')
			continue;
		separator = strpbrk(cursor, "=:");
		if (!separator)
			separator = strpbrk(cursor, " \t");
		if (!separator) {
			ret = -1;
			break;
		}
		*separator = '\0';
		key = trim_ascii(cursor);
		value = trim_ascii(separator + 1);
		if (!*key || add_property(set, key, value) != 0) {
			ret = -1;
			break;
		}
	}

	if (ferror(file))
		ret = -1;
	free(line);
	fclose(file);
	if (ret)
		free_properties(set);
	return ret;
}

static const char *property_value(const struct property_set *set,
				  const char *key)
{
	for (size_t i = 0; i < set->count; i++) {
		if (strcmp(set->entries[i].key, key) == 0)
			return set->entries[i].value;
	}
	return NULL;
}

static int required_property(const struct property_set *set, const char *key,
			     const char **out)
{
	*out = property_value(set, key);
	if (!*out || !**out) {
		fprintf(stderr, "missing sidecar field: %s\n", key);
		return -1;
	}
	return 0;
}

static int property_u64(const struct property_set *set, const char *key,
			uint64_t *out)
{
	const char *value;

	if (required_property(set, key, &value) != 0)
		return -1;
	return parse_u64(value, out);
}

static int property_abi_u64(const struct property_set *set, const char *key,
			    __u64 *out)
{
	uint64_t value;

	if (property_u64(set, key, &value) != 0)
		return -1;
	*out = (__u64)value;
	return 0;
}

static int property_u32(const struct property_set *set, const char *key,
			uint32_t *out)
{
	const char *value;

	if (required_property(set, key, &value) != 0)
		return -1;
	return parse_u32(value, out);
}

static int property_octal_u32(const struct property_set *set, const char *key,
			      uint32_t *out)
{
	const char *value;
	char *end = NULL;
	unsigned long parsed;

	if (required_property(set, key, &value) != 0)
		return -1;
	errno = 0;
	parsed = strtoul(value, &end, 8);
	if (errno || end == value || *end != '\0' || parsed > 0777U)
		return -1;
	*out = (uint32_t)parsed;
	return 0;
}

static int path_join(char out[PATH_MAX], const char *root, const char *child)
{
	int written = snprintf(out, PATH_MAX, "%s/%s", root, child);

	return written > 0 && written < PATH_MAX ? 0 : -1;
}

static int open_ctl(const char *device)
{
	int fd = open(device, O_RDWR);

	if (fd < 0)
		perror(device);
	return fd;
}

static int ioctl_clear_namespace(int fd);
static int ioctl_clear_mount_namespace(
	int fd, const struct cxnfs_blockfs_ioc_clear_namespace *clear);

static int stats(const char *device)
{
	struct cxnfs_blockfs_ioc_stats_v2 stats_v2 = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
	};
	struct cxnfs_blockfs_ioc_stats stats = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
	};
	int fd = open_ctl(device);

	if (fd < 0)
		return 1;
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_STATS_V2, &stats_v2) == 0) {
		close(fd);
		printf("abi_version=%u registered_extents=%llu invalidations=%llu read_folio_calls=%llu readahead_calls=%llu verified_page_fills=%llu miss_events_queued=%llu miss_events_drained=%llu miss_events_pending=%llu miss_events_dropped=%llu miss_wait_successes=%llu miss_wait_timeouts=%llu miss_wait_interruptions=%llu extent_generation=%llu hash_verifications=%llu hash_mismatches=%llu hash_bytes_verified=%llu readahead_miss_windows=%llu readahead_miss_bytes=%llu\n",
		       stats_v2.abi_version,
		       (unsigned long long)stats_v2.registered_extents,
		       (unsigned long long)stats_v2.invalidations,
		       (unsigned long long)stats_v2.read_folio_calls,
		       (unsigned long long)stats_v2.readahead_calls,
		       (unsigned long long)stats_v2.verified_page_fills,
		       (unsigned long long)stats_v2.miss_events_queued,
		       (unsigned long long)stats_v2.miss_events_drained,
		       (unsigned long long)stats_v2.miss_events_pending,
		       (unsigned long long)stats_v2.miss_events_dropped,
		       (unsigned long long)stats_v2.miss_wait_successes,
		       (unsigned long long)stats_v2.miss_wait_timeouts,
		       (unsigned long long)stats_v2.miss_wait_interruptions,
		       (unsigned long long)stats_v2.extent_generation,
		       (unsigned long long)stats_v2.hash_verifications,
		       (unsigned long long)stats_v2.hash_mismatches,
		       (unsigned long long)stats_v2.hash_bytes_verified,
		       (unsigned long long)stats_v2.readahead_miss_windows,
		       (unsigned long long)stats_v2.readahead_miss_bytes);
		return 0;
	}
	if (errno != ENOTTY) {
		perror("CXNFS_BLOCKFS_IOC_STATS");
		close(fd);
		return 1;
	}
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_STATS, &stats) != 0) {
		perror("CXNFS_BLOCKFS_IOC_STATS");
		close(fd);
		return 1;
	}
	close(fd);

	printf("abi_version=%u registered_extents=%llu invalidations=%llu read_folio_calls=%llu readahead_calls=%llu verified_page_fills=%llu\n",
	       stats.abi_version,
	       (unsigned long long)stats.registered_extents,
	       (unsigned long long)stats.invalidations,
	       (unsigned long long)stats.read_folio_calls,
	       (unsigned long long)stats.readahead_calls,
	       (unsigned long long)stats.verified_page_fills);
	return 0;
}

static int clear_namespace(const char *device, int argc, char **argv)
{
	struct cxnfs_blockfs_ioc_clear_namespace clear = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
	};
	int fd = open_ctl(device);
	int targeted = 0;

	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			if (copy_uapi_path(clear.metadata_root, argv[++i],
					   "metadata-root") != 0)
				return 2;
			targeted = 1;
		} else if (strcmp(argv[i], "--mount-id") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			clear.mount_id = value;
			targeted = 1;
		} else {
			return 2;
		}
	}

	if (fd < 0)
		return 1;
	if (targeted ? ioctl_clear_mount_namespace(fd, &clear) != 0 :
		       ioctl_clear_namespace(fd) != 0) {
		close(fd);
		return 1;
	}
	close(fd);
	printf("cleared_namespace=true mount_id=%llu metadata_root=%s scope=%s\n",
	       (unsigned long long)clear.mount_id,
	       clear.metadata_root[0] ? clear.metadata_root : "-",
	       targeted ? "mount" : "global");
	return 0;
}

static const char *miss_reason_name(uint32_t reason)
{
	switch (reason) {
	case CXNFS_BLOCKFS_MISS_REASON_READ:
		return "read";
	case CXNFS_BLOCKFS_MISS_REASON_READAHEAD:
		return "readahead";
	case CXNFS_BLOCKFS_MISS_REASON_PAYLOAD_IO:
		return "payload-io";
	case CXNFS_BLOCKFS_MISS_REASON_HASH_MISMATCH:
		return "hash-mismatch";
	default:
		return "unknown";
	}
}

static int next_miss(const char *device, int argc, char **argv)
{
	struct cxnfs_blockfs_ioc_miss_event event = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
	};
	int targeted = 0;
	int fd;

	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "--nonblock") == 0) {
			event.flags |= CXNFS_BLOCKFS_MISS_EVENT_NONBLOCK;
		} else if (strcmp(argv[i], "--mount-id") == 0 && i + 1 < argc) {
			uint64_t value;

			if (targeted || parse_u64(argv[++i], &value) != 0 ||
			    value == 0)
				return 2;
			event.mount_id = value;
			targeted = 1;
		} else if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			if (targeted ||
			    copy_uapi_path(event.metadata_root, argv[++i],
					   "metadata-root") != 0)
				return 2;
			targeted = 1;
		} else {
			return 2;
		}
	}

	fd = open_ctl(device);
	if (fd < 0)
		return 1;
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_NEXT_MISS_EVENT, &event) != 0) {
		int saved_errno = errno;

		close(fd);
		if ((event.flags & CXNFS_BLOCKFS_MISS_EVENT_NONBLOCK) &&
		    saved_errno == EAGAIN) {
			printf("miss_event=none errno=%d\n", saved_errno);
			return 0;
		}
		errno = saved_errno;
		perror("CXNFS_BLOCKFS_IOC_NEXT_MISS_EVENT");
		return 1;
	}
	close(fd);

	printf("miss_event event_id=%llu reason=%s inode=%llu offset=%llu length=%llu dropped_events=%llu mount_id=%llu metadata_root=%s max_cords=%u readahead_pages=%u miss_wait_ms=%u verify_hashes=%s mount_flags=%u\n",
	       (unsigned long long)event.event_id,
	       miss_reason_name(event.reason),
	       (unsigned long long)event.inode_number,
	       (unsigned long long)event.offset,
	       (unsigned long long)event.length,
	       (unsigned long long)event.dropped_events,
	       (unsigned long long)event.mount_id,
	       event.metadata_root[0] ? event.metadata_root : "-",
	       event.max_cords,
	       event.readahead_pages,
	       event.miss_wait_ms,
	       (event.mount_flags & CXNFS_BLOCKFS_MOUNT_VERIFY_HASHES) ?
		       "true" : "false",
	       event.mount_flags);
	return 0;
}

static int register_file(const char *device, int argc, char **argv)
{
	struct cxnfs_blockfs_ioc_file file_record = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
		.mode = 0444,
	};
	int fd;

	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "--inode") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			file_record.inode_number = value;
		} else if (strcmp(argv[i], "--mount-id") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			file_record.mount_id = value;
		} else if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			if (copy_uapi_path(file_record.metadata_root, argv[++i],
					   "metadata-root") != 0)
				return 2;
		} else if (strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
			const char *path = argv[++i];
			size_t path_len;

			if (!valid_relative_path(path)) {
				fprintf(stderr, "invalid relative file path: %s\n", path);
				return 2;
			}
			path_len = strlen(path);
			memcpy(file_record.path, path, path_len + 1);
		} else if (strcmp(argv[i], "--size") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			file_record.size = value;
		} else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
			uint32_t value;

			if (parse_u32(argv[++i], &value) != 0)
				return 2;
			if (value > 0777U) {
				fprintf(stderr, "mode must fit permission bits\n");
				return 2;
			}
			file_record.mode = value;
		} else {
			return 2;
		}
	}

	if (!file_record.inode_number || !file_record.path[0])
		return 2;

	fd = open_ctl(device);
	if (fd < 0)
		return 1;
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_REGISTER_FILE, &file_record) != 0) {
		perror("CXNFS_BLOCKFS_IOC_REGISTER_FILE");
		close(fd);
		return 1;
	}
	close(fd);

	printf("registered_file mount_id=%llu metadata_root=%s inode=%llu path=%s size=%llu mode=%o\n",
	       (unsigned long long)file_record.mount_id,
	       file_record.metadata_root[0] ? file_record.metadata_root : "-",
	       (unsigned long long)file_record.inode_number,
	       file_record.path,
	       (unsigned long long)file_record.size,
	       file_record.mode);
	return 0;
}

static int register_extent(const char *device, int argc, char **argv)
{
	struct cxnfs_blockfs_ioc_extent extent = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
		.flags = CXNFS_BLOCKFS_EXTENT_VERIFIED |
			 CXNFS_BLOCKFS_EXTENT_CAN_READ_EARLY |
			 CXNFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING,
	};
	int allow_unsupported = 0;
	int fd;

	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "--inode") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			extent.inode_number = value;
		} else if (strcmp(argv[i], "--mount-id") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			extent.mount_id = value;
		} else if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			if (copy_uapi_path(extent.metadata_root, argv[++i],
					   "metadata-root") != 0)
				return 2;
		} else if (strcmp(argv[i], "--file-offset") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			extent.file_offset = value;
		} else if (strcmp(argv[i], "--payload-block") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			extent.payload_block = value;
		} else if (strcmp(argv[i], "--payload-offset") == 0 && i + 1 < argc) {
			uint32_t value;

			if (parse_u32(argv[++i], &value) != 0)
				return 2;
			extent.payload_offset = value;
		} else if (strcmp(argv[i], "--payload-length") == 0 && i + 1 < argc) {
			uint32_t value;

			if (parse_u32(argv[++i], &value) != 0)
				return 2;
			extent.payload_length = value;
		} else if (strcmp(argv[i], "--hash") == 0 && i + 1 < argc) {
			if (parse_hash32(argv[++i], extent.expected_hash) != 0)
				return 2;
		} else if (strcmp(argv[i], "--allow-unsupported") == 0) {
			allow_unsupported = 1;
		} else {
			return 2;
		}
	}

	if (!extent.inode_number || !extent.payload_length)
		return 2;

	fd = open_ctl(device);
	if (fd < 0)
		return 1;
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_REGISTER_EXTENT, &extent) != 0) {
		int saved_errno = errno;

		close(fd);
		if (allow_unsupported && saved_errno == EOPNOTSUPP) {
			printf("unsupported=register-extent errno=%d\n", saved_errno);
			return 0;
		}
		errno = saved_errno;
		perror("CXNFS_BLOCKFS_IOC_REGISTER_EXTENT");
		return 1;
	}
	close(fd);

	printf("registered_extent mount_id=%llu metadata_root=%s inode=%llu file_offset=%llu payload_block=%llu payload_offset=%u payload_length=%u\n",
	       (unsigned long long)extent.mount_id,
	       extent.metadata_root[0] ? extent.metadata_root : "-",
	       (unsigned long long)extent.inode_number,
	       (unsigned long long)extent.file_offset,
	       (unsigned long long)extent.payload_block,
	       extent.payload_offset,
	       extent.payload_length);
	return 0;
}

static int invalidate(const char *device, int argc, char **argv)
{
	struct cxnfs_blockfs_ioc_invalidate invalidate = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
	};
	int allow_unsupported = 0;
	int fd;

	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "--inode") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			invalidate.inode_number = value;
		} else if (strcmp(argv[i], "--mount-id") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			invalidate.mount_id = value;
		} else if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			if (copy_uapi_path(invalidate.metadata_root, argv[++i],
					   "metadata-root") != 0)
				return 2;
		} else if (strcmp(argv[i], "--offset") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			invalidate.offset = value;
		} else if (strcmp(argv[i], "--length") == 0 && i + 1 < argc) {
			uint64_t value;

			if (parse_u64(argv[++i], &value) != 0)
				return 2;
			invalidate.length = value;
		} else if (strcmp(argv[i], "--allow-unsupported") == 0) {
			allow_unsupported = 1;
		} else {
			return 2;
		}
	}

	if (!invalidate.inode_number)
		return 2;

	fd = open_ctl(device);
	if (fd < 0)
		return 1;
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_INVALIDATE, &invalidate) != 0) {
		int saved_errno = errno;

		close(fd);
		if (allow_unsupported && saved_errno == EOPNOTSUPP) {
			printf("unsupported=invalidate errno=%d\n", saved_errno);
			return 0;
		}
		errno = saved_errno;
		perror("CXNFS_BLOCKFS_IOC_INVALIDATE");
		return 1;
	}
	close(fd);

	printf("invalidated mount_id=%llu metadata_root=%s inode=%llu offset=%llu length=%llu\n",
	       (unsigned long long)invalidate.mount_id,
	       invalidate.metadata_root[0] ? invalidate.metadata_root : "-",
	       (unsigned long long)invalidate.inode_number,
	       (unsigned long long)invalidate.offset,
	       (unsigned long long)invalidate.length);
	return 0;
}

struct sidecar_sync_options {
	const char *metadata_root;
	const char *report_path;
	int allow_unsupported;
	int require_extent_hints;
	int dry_run;
	int no_clear;
};

struct sidecar_sync_result {
	uint64_t generation;
	uint64_t extent_generation;
	uint64_t file_count;
	uint64_t extent_count;
	uint64_t registered_files;
	uint64_t attempted_extents;
	uint64_t registered_extents;
	uint64_t unsupported_extents;
	int cleared_namespace;
};

static int parse_sync_sidecars_args(int argc, char **argv,
				    struct sidecar_sync_options *opts)
{
	memset(opts, 0, sizeof(*opts));
	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "--metadata-root") == 0 && i + 1 < argc) {
			opts->metadata_root = argv[++i];
		} else if (strcmp(argv[i], "--report") == 0 && i + 1 < argc) {
			opts->report_path = argv[++i];
		} else if (strcmp(argv[i], "--require-extent-hints") == 0) {
			opts->require_extent_hints = 1;
		} else if (strcmp(argv[i], "--allow-unsupported") == 0) {
			opts->allow_unsupported = 1;
		} else if (strcmp(argv[i], "--dry-run") == 0) {
			opts->dry_run = 1;
		} else if (strcmp(argv[i], "--no-clear") == 0) {
			opts->no_clear = 1;
		} else {
			return -1;
		}
	}
	return opts->metadata_root && *opts->metadata_root ? 0 : -1;
}

static int ioctl_clear_namespace(int fd)
{
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_CLEAR_NAMESPACE, 0) != 0) {
		perror("CXNFS_BLOCKFS_IOC_CLEAR_NAMESPACE");
		return -1;
	}
	return 0;
}

static int ioctl_clear_mount_namespace(
	int fd, const struct cxnfs_blockfs_ioc_clear_namespace *clear)
{
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_CLEAR_MOUNT_NAMESPACE, clear) != 0) {
		perror("CXNFS_BLOCKFS_IOC_CLEAR_MOUNT_NAMESPACE");
		return -1;
	}
	return 0;
}

static int ioctl_register_file_record(int fd,
				      const struct cxnfs_blockfs_ioc_file *file)
{
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_REGISTER_FILE, file) != 0) {
		perror("CXNFS_BLOCKFS_IOC_REGISTER_FILE");
		return -1;
	}
	return 0;
}

static int ioctl_register_extent_record(
	int fd, const struct cxnfs_blockfs_ioc_extent *extent,
	int allow_unsupported, uint64_t *unsupported_extents)
{
	if (ioctl(fd, CXNFS_BLOCKFS_IOC_REGISTER_EXTENT, extent) == 0)
		return 0;
	if (allow_unsupported && errno == EOPNOTSUPP) {
		(*unsupported_extents)++;
		return 0;
	}
	perror("CXNFS_BLOCKFS_IOC_REGISTER_EXTENT");
	return -1;
}

static int load_namespace_sidecar(const char *metadata_root,
				  struct property_set *namespace_props,
				  uint64_t *generation,
				  uint64_t *file_count)
{
	char path[PATH_MAX];
	uint64_t version;

	if (path_join(path, metadata_root, "namespace/current.sat") != 0)
		return -1;
	if (access(path, R_OK) != 0) {
		if (path_join(path, metadata_root, "namespace/current.cxn") != 0)
			return -1;
	}
	if (load_properties(path, namespace_props) != 0)
		return -1;
	if (property_u64(namespace_props, "version", &version) != 0 ||
	    version != 1) {
		fprintf(stderr, "unsupported namespace sidecar version\n");
		return -1;
	}
	if (property_u64(namespace_props, "generation", generation) != 0 ||
	    property_u64(namespace_props, "file.count", file_count) != 0)
		return -1;
	return 0;
}

static int load_extent_sidecar(const char *metadata_root,
			       struct property_set *extent_props,
			       uint64_t *generation,
			       uint64_t *extent_count,
			       int required)
{
	char path[PATH_MAX];
	uint64_t version;

	if (path_join(path, metadata_root, "extent-hints/current.sat") != 0)
		return -1;
	if (access(path, R_OK) != 0) {
		if (path_join(path, metadata_root, "extent-hints/current.cxn") != 0)
			return -1;
	}
	if (access(path, R_OK) != 0) {
		if (!required && errno == ENOENT) {
			memset(extent_props, 0, sizeof(*extent_props));
			*generation = 0;
			*extent_count = 0;
			return 0;
		}
		perror(path);
		return -1;
	}
	if (load_properties(path, extent_props) != 0)
		return -1;
	if (property_u64(extent_props, "version", &version) != 0 ||
	    version != 1) {
		fprintf(stderr, "unsupported extent-hints sidecar version\n");
		return -1;
	}
	if (property_u64(extent_props, "generation", generation) != 0 ||
	    property_u64(extent_props, "extent.count", extent_count) != 0)
		return -1;
	return 0;
}

static int publish_namespace_sidecar(int fd,
				     const char *metadata_root,
				     const struct property_set *props,
				     uint64_t file_count,
				     int dry_run,
				     uint64_t *registered_files)
{
	char key[128];

	for (uint64_t i = 0; i < file_count; i++) {
		struct cxnfs_blockfs_ioc_file file_record = {
			.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
			.mode = 0444,
		};
		const char *path;
		uint32_t mode;
		int written;

		written = snprintf(key, sizeof(key), "file.%llu.inode",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_abi_u64(props, key, &file_record.inode_number) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "file.%llu.path",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    required_property(props, key, &path) != 0)
			return -1;
		if (!valid_relative_path(path)) {
			fprintf(stderr, "invalid namespace path: %s\n", path);
			return -1;
		}
		strncpy(file_record.path, path, sizeof(file_record.path) - 1);
		written = snprintf(key, sizeof(key), "file.%llu.size",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_abi_u64(props, key, &file_record.size) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "file.%llu.mode",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_octal_u32(props, key, &mode) != 0)
			return -1;
		file_record.mode = mode;
		if (!file_record.inode_number)
			return -1;
		if (copy_uapi_path(file_record.metadata_root, metadata_root,
				   "metadata-root") != 0)
			return -1;
		if (!dry_run && ioctl_register_file_record(fd, &file_record) != 0)
			return -1;
		(*registered_files)++;
	}
	return 0;
}

static int publish_extent_sidecar(int fd,
				  const char *metadata_root,
				  const struct property_set *props,
				  uint64_t extent_count,
				  int dry_run,
				  int allow_unsupported,
				  uint64_t *registered_extents,
				  uint64_t *unsupported_extents)
{
	char key[128];

	for (uint64_t i = 0; i < extent_count; i++) {
		struct cxnfs_blockfs_ioc_extent extent = {
			.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
			.flags = CXNFS_BLOCKFS_EXTENT_VERIFIED |
				 CXNFS_BLOCKFS_EXTENT_CAN_READ_EARLY |
				 CXNFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING,
		};
		const char *hash;
		int written;

		written = snprintf(key, sizeof(key), "extent.%llu.inode",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_abi_u64(props, key, &extent.inode_number) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "extent.%llu.file-offset",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_abi_u64(props, key, &extent.file_offset) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "extent.%llu.payload-block",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_abi_u64(props, key, &extent.payload_block) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "extent.%llu.payload-offset",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_u32(props, key, &extent.payload_offset) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "extent.%llu.payload-length",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    property_u32(props, key, &extent.payload_length) != 0)
			return -1;
		written = snprintf(key, sizeof(key), "extent.%llu.hash",
				   (unsigned long long)i);
		if (written < 0 || written >= (int)sizeof(key) ||
		    required_property(props, key, &hash) != 0 ||
		    parse_hash32(hash, extent.expected_hash) != 0)
			return -1;
		if (!extent.inode_number || !extent.payload_length)
			return -1;
		if (copy_uapi_path(extent.metadata_root, metadata_root,
				   "metadata-root") != 0)
			return -1;
		if (dry_run) {
			(*registered_extents)++;
		} else {
			uint64_t unsupported_before = *unsupported_extents;

			if (ioctl_register_extent_record(fd, &extent,
							 allow_unsupported,
							 unsupported_extents) != 0)
				return -1;
			if (*unsupported_extents == unsupported_before)
				(*registered_extents)++;
		}
	}
	return 0;
}

static int write_sidecar_sync_report(
	const struct sidecar_sync_options *opts,
	const struct sidecar_sync_result *result)
{
	int fd;

	if (!opts->report_path)
		return 0;
	fd = open(opts->report_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	if (dprintf(fd,
		    "satfs.blockfs.sidecar-sync.v1\n"
		    "cxnfs.blockfs.sidecar-sync.v1\n"
		    "metadata_root=%s\n"
		    "generation=%llu\n"
		    "extent_generation=%llu\n"
		    "dry_run=%s\n"
		    "clear_before_publish=%s\n"
		    "cleared_namespace=%s\n"
		    "file_count=%llu\n"
		    "registered_files=%llu\n"
		    "extent_count=%llu\n"
		    "attempted_extents=%llu\n"
		    "registered_extents=%llu\n"
		    "unsupported_extents=%llu\n"
		    "status=synced\n",
		    opts->metadata_root,
		    (unsigned long long)result->generation,
		    (unsigned long long)result->extent_generation,
		    opts->dry_run ? "true" : "false",
		    opts->no_clear ? "false" : "true",
		    result->cleared_namespace ? "true" : "false",
		    (unsigned long long)result->file_count,
		    (unsigned long long)result->registered_files,
		    (unsigned long long)result->extent_count,
		    (unsigned long long)result->attempted_extents,
		    (unsigned long long)result->registered_extents,
		    (unsigned long long)result->unsupported_extents) < 0) {
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

static int sync_sidecars(const char *device, int argc, char **argv)
{
	struct sidecar_sync_options opts;
	struct sidecar_sync_result result = {0};
	struct property_set namespace_props = {0};
	struct property_set extent_props = {0};
	uint64_t file_count = 0;
	uint64_t extent_count = 0;
	int fd = -1;
	int ret = 1;

	if (parse_sync_sidecars_args(argc, argv, &opts) != 0)
		return 2;

	if (load_namespace_sidecar(opts.metadata_root, &namespace_props,
				  &result.generation, &file_count) != 0)
		goto out;
	if (load_extent_sidecar(opts.metadata_root, &extent_props,
				&result.extent_generation, &extent_count,
				opts.require_extent_hints) != 0)
		goto out;
	if (extent_count > 0 && result.extent_generation != result.generation) {
		fprintf(stderr,
			"sidecar generation mismatch: namespace=%llu extent-hints=%llu\n",
			(unsigned long long)result.generation,
			(unsigned long long)result.extent_generation);
		goto out;
	}

	if (!opts.dry_run) {
		fd = open_ctl(device);
		if (fd < 0)
			goto out;
		if (!opts.no_clear) {
			struct cxnfs_blockfs_ioc_clear_namespace clear = {
				.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
			};

			if (copy_uapi_path(clear.metadata_root, opts.metadata_root,
					   "metadata-root") != 0)
				goto out;
			if (ioctl_clear_mount_namespace(fd, &clear) != 0)
				goto out;
			result.cleared_namespace = 1;
		}
	}
	result.file_count = file_count;
	result.extent_count = extent_count;
	result.attempted_extents = extent_count;
	if (publish_namespace_sidecar(fd, opts.metadata_root, &namespace_props,
				      file_count,
				      opts.dry_run,
				      &result.registered_files) != 0)
		goto out;
	if (extent_count > 0 &&
	    publish_extent_sidecar(fd, opts.metadata_root, &extent_props,
				   extent_count, opts.dry_run,
				   opts.allow_unsupported,
				   &result.registered_extents,
				   &result.unsupported_extents) != 0)
		goto out;
	if (write_sidecar_sync_report(&opts, &result) != 0) {
		perror("write sidecar sync report");
		goto out;
	}

	printf("synced_sidecars metadata_root=%s generation=%llu dry_run=%s clear_before_publish=%s files=%llu registered_files=%llu extents=%llu registered_extents=%llu unsupported_extents=%llu\n",
	       opts.metadata_root,
	       (unsigned long long)result.generation,
	       opts.dry_run ? "true" : "false",
	       opts.no_clear ? "false" : "true",
	       (unsigned long long)result.file_count,
	       (unsigned long long)result.registered_files,
	       (unsigned long long)result.extent_count,
	       (unsigned long long)result.registered_extents,
	       (unsigned long long)result.unsupported_extents);
	ret = 0;

out:
	if (fd >= 0)
		close(fd);
	free_properties(&namespace_props);
	free_properties(&extent_props);
	return ret;
}

int main(int argc, char **argv)
{
	const char *prog = argv[0];
	const char *device = access("/dev/satfs_blockfs_ctl", F_OK) == 0 ? "/dev/satfs_blockfs_ctl" : "/dev/cxnfs_blockfs_ctl";
	const char *command;

	if (argc < 2) {
		usage(prog);
		return 2;
	}

	if (strcmp(argv[1], "--device") == 0) {
		if (argc < 4) {
			usage(prog);
			return 2;
		}
		device = argv[2];
		argv += 2;
		argc -= 2;
	}

	command = argv[1];
	if (strcmp(command, "stats") == 0) {
		if (argc != 2) {
			usage(prog);
			return 2;
		}
		return stats(device);
	}
	if (strcmp(command, "clear-namespace") == 0) {
		return clear_namespace(device, argc - 2, argv + 2);
	}
	if (strcmp(command, "next-miss") == 0)
		return next_miss(device, argc - 2, argv + 2);
	if (strcmp(command, "register-file") == 0)
		return register_file(device, argc - 2, argv + 2);
	if (strcmp(command, "register-extent") == 0)
		return register_extent(device, argc - 2, argv + 2);
	if (strcmp(command, "invalidate") == 0)
		return invalidate(device, argc - 2, argv + 2);
	if (strcmp(command, "sync-sidecars") == 0)
		return sync_sidecars(device, argc - 2, argv + 2);

	usage(prog);
	return 2;
}
