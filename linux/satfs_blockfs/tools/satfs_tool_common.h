#ifndef SATFS_TOOL_COMMON_H
#define SATFS_TOOL_COMMON_H

#include <errno.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define SATFS_BSWAP16(value) __builtin_bswap16(value)
#define SATFS_BSWAP32(value) __builtin_bswap32(value)
#define SATFS_BSWAP64(value) __builtin_bswap64(value)
#else
#define SATFS_BSWAP16(value) (value)
#define SATFS_BSWAP32(value) (value)
#define SATFS_BSWAP64(value) (value)
#endif

#define CXNFS_BSWAP16 SATFS_BSWAP16
#define CXNFS_BSWAP32 SATFS_BSWAP32
#define CXNFS_BSWAP64 SATFS_BSWAP64

static inline uint16_t satfs_to_le16(uint16_t value)
{
	return SATFS_BSWAP16(value);
}

static inline uint32_t satfs_to_le32(uint32_t value)
{
	return SATFS_BSWAP32(value);
}

static inline uint64_t satfs_to_le64(uint64_t value)
{
	return SATFS_BSWAP64(value);
}

static inline uint16_t satfs_from_le16(uint16_t value)
{
	return SATFS_BSWAP16(value);
}

static inline uint32_t satfs_from_le32(uint32_t value)
{
	return SATFS_BSWAP32(value);
}

static inline uint64_t satfs_from_le64(uint64_t value)
{
	return SATFS_BSWAP64(value);
}

#define cxnfs_to_le16 satfs_to_le16
#define cxnfs_to_le32 satfs_to_le32
#define cxnfs_to_le64 satfs_to_le64
#define cxnfs_from_le16 satfs_from_le16
#define cxnfs_from_le32 satfs_from_le32
#define cxnfs_from_le64 satfs_from_le64

static inline int satfs_pread_full(int fd, void *data, size_t size, off_t offset)
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

		cursor += (size_t)read_bytes;
		size -= (size_t)read_bytes;
		offset += (off_t)read_bytes;
	}

	return 0;
}

static inline int satfs_pwrite_full(int fd, const void *data, size_t size, off_t offset)
{
	const uint8_t *cursor = data;

	while (size > 0) {
		ssize_t written_bytes = pwrite(fd, cursor, size, offset);

		if (written_bytes < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (written_bytes == 0)
			return -1;

		cursor += (size_t)written_bytes;
		size -= (size_t)written_bytes;
		offset += (off_t)written_bytes;
	}

	return 0;
}

#define cxnfs_pread_full satfs_pread_full
#define cxnfs_pwrite_full satfs_pwrite_full

#endif /* SATFS_TOOL_COMMON_H */
