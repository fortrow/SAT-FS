#ifndef SATFS_BLOCKFS_UAPI_H
#define SATFS_BLOCKFS_UAPI_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define SATFS_BLOCKFS_IOC_MAGIC 'S'
#define SATFS_BLOCKFS_IOC_ABI_VERSION 2
#define SATFS_BLOCKFS_PATH_MAX 256

#define SATFS_BLOCKFS_EXTENT_VERIFIED (1U << 0)
#define SATFS_BLOCKFS_EXTENT_CAN_READ_EARLY (1U << 1)
#define SATFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING (1U << 2)

#define SATFS_BLOCKFS_MISS_EVENT_NONBLOCK (1U << 0)

#define SATFS_BLOCKFS_MISS_REASON_READ 1U
#define SATFS_BLOCKFS_MISS_REASON_READAHEAD 2U
#define SATFS_BLOCKFS_MISS_REASON_PAYLOAD_IO 3U
#define SATFS_BLOCKFS_MISS_REASON_HASH_MISMATCH 4U

#define SATFS_BLOCKFS_MOUNT_VERIFY_HASHES (1U << 0)

/* Legacy compatibility aliases */
#define CXNFS_BLOCKFS_IOC_MAGIC 'C'
#define CXNFS_BLOCKFS_IOC_ABI_VERSION SATFS_BLOCKFS_IOC_ABI_VERSION
#define CXNFS_BLOCKFS_PATH_MAX SATFS_BLOCKFS_PATH_MAX
#define CXNFS_BLOCKFS_EXTENT_VERIFIED SATFS_BLOCKFS_EXTENT_VERIFIED
#define CXNFS_BLOCKFS_EXTENT_CAN_READ_EARLY SATFS_BLOCKFS_EXTENT_CAN_READ_EARLY
#define CXNFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING SATFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING
#define CXNFS_BLOCKFS_MISS_EVENT_NONBLOCK SATFS_BLOCKFS_MISS_EVENT_NONBLOCK
#define CXNFS_BLOCKFS_MISS_REASON_READ SATFS_BLOCKFS_MISS_REASON_READ
#define CXNFS_BLOCKFS_MISS_REASON_READAHEAD SATFS_BLOCKFS_MISS_REASON_READAHEAD
#define CXNFS_BLOCKFS_MISS_REASON_PAYLOAD_IO SATFS_BLOCKFS_MISS_REASON_PAYLOAD_IO
#define CXNFS_BLOCKFS_MISS_REASON_HASH_MISMATCH SATFS_BLOCKFS_MISS_REASON_HASH_MISMATCH
#define CXNFS_BLOCKFS_MOUNT_VERIFY_HASHES SATFS_BLOCKFS_MOUNT_VERIFY_HASHES

struct satfs_blockfs_ioc_extent {
	__u32 abi_version;
	__u32 flags;
	__u64 mount_id;
	__u64 inode_number;
	__u64 file_offset;
	__u64 payload_block;
	__u32 payload_offset;
	__u32 payload_length;
	__u8 expected_hash[32];
	char metadata_root[SATFS_BLOCKFS_PATH_MAX];
};

#define cxnfs_blockfs_ioc_extent satfs_blockfs_ioc_extent

struct satfs_blockfs_ioc_invalidate {
	__u32 abi_version;
	__u32 flags;
	__u64 mount_id;
	__u64 inode_number;
	__u64 offset;
	__u64 length;
	char metadata_root[SATFS_BLOCKFS_PATH_MAX];
};

#define cxnfs_blockfs_ioc_invalidate satfs_blockfs_ioc_invalidate

struct satfs_blockfs_ioc_stats {
	__u32 abi_version;
	__u32 reserved;
	__u64 registered_extents;
	__u64 invalidations;
	__u64 read_folio_calls;
	__u64 readahead_calls;
	__u64 verified_page_fills;
};

#define cxnfs_blockfs_ioc_stats satfs_blockfs_ioc_stats

struct satfs_blockfs_ioc_stats_v2 {
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

#define cxnfs_blockfs_ioctl_stats_v2 satfs_blockfs_ioc_stats_v2
#define cxnfs_blockfs_ioc_stats_v2 satfs_blockfs_ioc_stats_v2

struct satfs_blockfs_ioc_file {
	__u32 abi_version;
	__u32 mode;
	__u64 mount_id;
	__u64 inode_number;
	__u64 size;
	char metadata_root[SATFS_BLOCKFS_PATH_MAX];
	char path[SATFS_BLOCKFS_PATH_MAX];
};

#define cxnfs_blockfs_ioc_file satfs_blockfs_ioc_file

struct satfs_blockfs_ioc_clear_namespace {
	__u32 abi_version;
	__u32 reserved;
	__u64 mount_id;
	char metadata_root[SATFS_BLOCKFS_PATH_MAX];
};

#define cxnfs_blockfs_ioc_clear_namespace satfs_blockfs_ioc_clear_namespace

struct satfs_blockfs_ioc_miss_event {
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
	char metadata_root[SATFS_BLOCKFS_PATH_MAX];
};

#define cxnfs_blockfs_ioc_miss_event satfs_blockfs_ioc_miss_event

/* SATFS IOCTL definitions (magic 'S') */
#define SATFS_BLOCKFS_IOC_REGISTER_EXTENT \
	_IOW(SATFS_BLOCKFS_IOC_MAGIC, 1, struct satfs_blockfs_ioc_extent)
#define SATFS_BLOCKFS_IOC_INVALIDATE \
	_IOW(SATFS_BLOCKFS_IOC_MAGIC, 2, struct satfs_blockfs_ioc_invalidate)
#define SATFS_BLOCKFS_IOC_STATS \
	_IOR(SATFS_BLOCKFS_IOC_MAGIC, 3, struct satfs_blockfs_ioc_stats)
#define SATFS_BLOCKFS_IOC_REGISTER_FILE \
	_IOW(SATFS_BLOCKFS_IOC_MAGIC, 4, struct satfs_blockfs_ioc_file)
#define SATFS_BLOCKFS_IOC_CLEAR_NAMESPACE \
	_IO(SATFS_BLOCKFS_IOC_MAGIC, 5)
#define SATFS_BLOCKFS_IOC_NEXT_MISS_EVENT \
	_IOWR(SATFS_BLOCKFS_IOC_MAGIC, 6, struct satfs_blockfs_ioc_miss_event)
#define SATFS_BLOCKFS_IOC_STATS_V2 \
	_IOR(SATFS_BLOCKFS_IOC_MAGIC, 7, struct satfs_blockfs_ioc_stats_v2)
#define SATFS_BLOCKFS_IOC_CLEAR_MOUNT_NAMESPACE \
	_IOW(SATFS_BLOCKFS_IOC_MAGIC, 8, struct satfs_blockfs_ioc_clear_namespace)

/* Legacy CXNFS IOCTL definitions (magic 'C') */
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

#endif /* SATFS_BLOCKFS_UAPI_H */
