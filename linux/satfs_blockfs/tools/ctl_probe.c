#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include "../satfs_blockfs_uapi.h"

int main(int argc, char **argv)
{
	const char *device = argc > 1 ? argv[1] : (access("/dev/satfs_blockfs_ctl", F_OK) == 0 ? "/dev/satfs_blockfs_ctl" : "/dev/cxnfs_blockfs_ctl");
	struct cxnfs_blockfs_ioc_stats stats = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
	};
	struct cxnfs_blockfs_ioc_extent extent = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
		.flags = CXNFS_BLOCKFS_EXTENT_VERIFIED |
			 CXNFS_BLOCKFS_EXTENT_CAN_READ_EARLY |
			 CXNFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING,
		.inode_number = 1,
		.file_offset = 0,
		.payload_block = 1,
		.payload_offset = 0,
		.payload_length = 4096,
	};
	struct cxnfs_blockfs_ioc_invalidate invalidate = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
		.inode_number = 1,
		.offset = 0,
		.length = 4096,
	};
	int fd = open(device, O_RDWR);
	int ret;

	if (fd < 0) {
		perror(device);
		return 1;
	}

	if (ioctl(fd, CXNFS_BLOCKFS_IOC_STATS, &stats) != 0) {
		perror("CXNFS_BLOCKFS_IOC_STATS");
		close(fd);
		return 1;
	}

	printf("stats: abi=%u registered_extents=%llu invalidations=%llu read_folio=%llu readahead=%llu verified_page_fills=%llu\n",
	       stats.abi_version,
	       (unsigned long long)stats.registered_extents,
	       (unsigned long long)stats.invalidations,
	       (unsigned long long)stats.read_folio_calls,
	       (unsigned long long)stats.readahead_calls,
	       (unsigned long long)stats.verified_page_fills);

	ret = ioctl(fd, CXNFS_BLOCKFS_IOC_REGISTER_EXTENT, &extent);
	if (ret != 0) {
		if (errno == ENODEV) {
			printf("register extent requires active mount selector\n");
			close(fd);
			return 0;
		}
		perror("CXNFS_BLOCKFS_IOC_REGISTER_EXTENT");
		close(fd);
		return 1;
	}

	printf("register extent accepted by verified extent index\n");
	ret = ioctl(fd, CXNFS_BLOCKFS_IOC_INVALIDATE, &invalidate);
	if (ret != 0) {
		perror("CXNFS_BLOCKFS_IOC_INVALIDATE");
		close(fd);
		return 1;
	}
	printf("invalidate accepted by verified extent index\n");
	close(fd);
	return 0;
}
