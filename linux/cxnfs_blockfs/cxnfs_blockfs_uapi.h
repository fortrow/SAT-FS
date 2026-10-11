#ifndef CXNFS_BLOCKFS_UAPI_H
#define CXNFS_BLOCKFS_UAPI_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define CXNFS_BLOCKFS_IOC_MAGIC 'C'
#define CXNFS_BLOCKFS_IOC_ABI_VERSION 2
#define CXNFS_BLOCKFS_PATH_MAX 256

#define CXNFS_BLOCKFS_EXTENT_VERIFIED (1U << 0)
#define CXNFS_BLOCKFS_EXTENT_CAN_READ_EARLY (1U << 1)
#define CXNFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING (1U << 2)

#define CXNFS_BLOCKFS_MISS_EVENT_NONBLOCK (1U << 0)

#define CXNFS_BLOCKFS_MISS_REASON_READ 1U
#define CXNFS_BLOCKFS_MISS_REASON_READAHEAD 2U
#define CXNFS_BLOCKFS_MISS_REASON_PAYLOAD_IO 3U
#define CXNFS_BLOCKFS_MISS_REASON_HASH_MISMATCH 4U

#define CXNFS_BLOCKFS_MOUNT_VERIFY_HASHES (1U << 0)

struct cxnfs_blockfs_ioc_extent {
	__u32 abi_version;
	__u32 flags;
	__u64 mount_id;
	__u64 inode_number;
	__u64 file_offset;
	__u64 payload_block;
	__u32 payload_offset;
	__u32 payload_length;
	__u8 expected_hash[32];
	char metadata_root[CXNFS_BLOCKFS_PATH_MAX];
};

struct cxnfs_blockfs_ioc_invalidate {
	__u32 abi_version;
	__u32 flags;
	__u64 mount_id;
	__u64 inode_number;
	__u64 offset;
	__u64 length;
	char metadata_root[CXNFS_BLOCKFS_PATH_MAX];
};

struct cxnfs_blockfs_ioc_stats {
	__u32 abi_version;
	__u32 reserved;
	__u64 registered_extents;
	__u64 invalidations;
	__u64 read_folio_calls;
	__u64 readahead_calls;
	__u64 verified_page_fills;
};

struct cxnfs_blockfs_ioc_stats_v2 {
	__u32 abi_version;
	__u32 reserved;
	__u64 registered_extents;
	__u64 invalidations;
	__u64 read_folio_calls;
	__u64 readahead_calls;
	__u64 verified_page_fills;
	__u64 miss_events_queued;
	__u64 miss_events_drained;
	__u64 miss_events_pending;
	__u64 miss_events_dropped;
	__u64 miss_wait_successes;
	__u64 miss_wait_timeouts;
	__u64 miss_wait_interruptions;
	__u64 extent_generation;
	__u64 hash_verifications;
	__u64 hash_mismatches;
	__u64 hash_bytes_verified;
	__u64 readahead_miss_windows;
	__u64 readahead_miss_bytes;
};

struct cxnfs_blockfs_ioc_file {
	__u32 abi_version;
	__u32 mode;
	__u64 mount_id;
	__u64 inode_number;
	__u64 size;
	char metadata_root[CXNFS_BLOCKFS_PATH_MAX];
	char path[CXNFS_BLOCKFS_PATH_MAX];
};

struct cxnfs_blockfs_ioc_clear_namespace {
	__u32 abi_version;
	__u32 reserved;
	__u64 mount_id;
	char metadata_root[CXNFS_BLOCKFS_PATH_MAX];
};

struct cxnfs_blockfs_ioc_miss_event {
	__u32 abi_version;
	__u32 flags;
	__u64 event_id;
	__u64 inode_number;
	__u64 offset;
	__u64 length;
	__u32 reason;
	__u32 reserved;
	__u64 dropped_events;
	__u64 mount_id;
	__u32 max_cords;
	__u32 readahead_pages;
	__u32 miss_wait_ms;
	__u32 mount_flags;
	char metadata_root[CXNFS_BLOCKFS_PATH_MAX];
};

#define CXNFS_BLOCKFS_IOC_REGISTER_EXTENT \
	_IOW(CXNFS_BLOCKFS_IOC_MAGIC, 1, struct cxnfs_blockfs_ioc_extent)
#define CXNFS_BLOCKFS_IOC_INVALIDATE \
	_IOW(CXNFS_BLOCKFS_IOC_MAGIC, 2, struct cxnfs_blockfs_ioc_invalidate)
#define CXNFS_BLOCKFS_IOC_STATS \
	_IOR(CXNFS_BLOCKFS_IOC_MAGIC, 3, struct cxnfs_blockfs_ioc_stats)
#define CXNFS_BLOCKFS_IOC_REGISTER_FILE \
	_IOW(CXNFS_BLOCKFS_IOC_MAGIC, 4, struct cxnfs_blockfs_ioc_file)
#define CXNFS_BLOCKFS_IOC_CLEAR_NAMESPACE \
	_IO(CXNFS_BLOCKFS_IOC_MAGIC, 5)
#define CXNFS_BLOCKFS_IOC_NEXT_MISS_EVENT \
	_IOWR(CXNFS_BLOCKFS_IOC_MAGIC, 6, struct cxnfs_blockfs_ioc_miss_event)
#define CXNFS_BLOCKFS_IOC_STATS_V2 \
	_IOR(CXNFS_BLOCKFS_IOC_MAGIC, 7, struct cxnfs_blockfs_ioc_stats_v2)
#define CXNFS_BLOCKFS_IOC_CLEAR_MOUNT_NAMESPACE \
	_IOW(CXNFS_BLOCKFS_IOC_MAGIC, 8, struct cxnfs_blockfs_ioc_clear_namespace)

#endif
