#ifndef CXNFS_TOOL_COMMON_H
#define CXNFS_TOOL_COMMON_H

#include <errno.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define CXNFS_BSWAP16(value) __builtin_bswap16(value)
#define CXNFS_BSWAP32(value) __builtin_bswap32(value)
#define CXNFS_BSWAP64(value) __builtin_bswap64(value)
#else
#define CXNFS_BSWAP16(value) (value)
#define CXNFS_BSWAP32(value) (value)
#define CXNFS_BSWAP64(value) (value)
#endif

static inline uint16_t cxnfs_to_le16(uint16_t value)
{
	return CXNFS_BSWAP16(value);
}

static inline uint32_t cxnfs_to_le32(uint32_t value)
{
	return CXNFS_BSWAP32(value);
}

static inline uint64_t cxnfs_to_le64(uint64_t value)
{
	return CXNFS_BSWAP64(value);
}

static inline uint16_t cxnfs_from_le16(uint16_t value)
{
	return CXNFS_BSWAP16(value);
}

static inline uint32_t cxnfs_from_le32(uint32_t value)
{
	return CXNFS_BSWAP32(value);
}

static inline uint64_t cxnfs_from_le64(uint64_t value)
{
	return CXNFS_BSWAP64(value);
}

static inline int cxnfs_pread_full(int fd, void *data, size_t size, off_t offset)
{
	uint8_t *cursor = data;

	while (size > 0) {
		ssize_t read_bytes = pread(fd, cursor, size, offset);

		if (read_bytes < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (read_bytes == 0)
			return -1;
		cursor += read_bytes;
		size -= (size_t)read_bytes;
		offset += read_bytes;
	}

	return 0;
}

static inline int cxnfs_pwrite_full(int fd, const void *data, size_t size,
				    off_t offset)
{
	const uint8_t *cursor = data;

	while (size > 0) {
		ssize_t written = pwrite(fd, cursor, size, offset);

		if (written < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (written == 0)
			return -1;
		cursor += written;
		size -= (size_t)written;
		offset += written;
	}

	return 0;
}

#endif
