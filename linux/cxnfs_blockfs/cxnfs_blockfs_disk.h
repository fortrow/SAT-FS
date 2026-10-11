#ifndef CXNFS_BLOCKFS_DISK_H
#define CXNFS_BLOCKFS_DISK_H

#ifdef __KERNEL__
#include <linux/types.h>
#else
#include <stdint.h>
typedef uint8_t __u8;
typedef uint16_t __le16;
typedef uint32_t __le32;
typedef uint64_t __le64;
#endif

#define CXNFS_DISK_MAGIC "CXNFSBFS"
#define CXNFS_DISK_MAGIC_LEN 8
#define CXNFS_DISK_VERSION 1
#define CXNFS_DISK_BLOCK_SIZE 4096
#define CXNFS_DISK_UUID_LEN 16
#define CXNFS_DISK_LABEL_LEN 64
#define CXNFS_DISK_MAX_CORDS 64
#define CXNFS_DISK_DEFAULT_READAHEAD_PAGES 128
#define CXNFS_DISK_FLAG_OS_STRUCTURES_REQUIRED (1ULL << 0)
#define CXNFS_DISK_FLAG_TRAILING_METADATA_ALLOWED (1ULL << 1)

enum cxnfs_disk_zone_kind {
	CXNFS_ZONE_FREE_MAP = 1,
	CXNFS_ZONE_LOCAL_EXTENT_INDEX = 2,
	CXNFS_ZONE_MANIFEST_INDEX = 3,
	CXNFS_ZONE_CONTAINER_CACHE = 4,
	CXNFS_ZONE_DATA_EXTENTS = 5,
	CXNFS_ZONE_JOURNAL = 6,
	CXNFS_ZONE_CACHE_INDEX = 7,
};

struct cxnfs_disk_zone {
	__le16 kind;
	__le16 flags;
	__le32 reserved;
	__le64 start_block;
	__le64 block_count;
};

struct cxnfs_disk_super {
	__u8 magic[CXNFS_DISK_MAGIC_LEN];
	__le32 version;
	__le32 block_size;
	__u8 fs_uuid[CXNFS_DISK_UUID_LEN];
	__u8 label[CXNFS_DISK_LABEL_LEN];
	__le64 flags;
	__le64 total_blocks;
	__le64 root_inode;
	/*
	 * In the default v1 layout, authoritative filesystem structures live in
	 * the OS metadata root. These block fields are reserved for optional
	 * mirrored/checkpointed structures or future offline recovery support.
	 */
	__le64 journal_start_block;
	__le64 journal_block_count;
	__le64 manifest_index_start_block;
	__le64 manifest_index_block_count;
	__le64 cache_index_start_block;
	__le64 cache_index_block_count;
	__le64 container_cache_start_block;
	__le64 container_cache_block_count;
	__le64 free_map_start_block;
	__le64 free_map_block_count;
	__le32 max_cords;
	__le32 readahead_pages;
	__le32 zone_count;
	__le32 checksum_crc32c;
	struct cxnfs_disk_zone zones[8];
	__u8 reserved[3688];
};

struct cxnfs_disk_extent {
	__le64 logical_offset;
	__le64 physical_block;
	__le32 block_count;
	__le32 flags;
	__u8 expected_hash[32];
};

struct cxnfs_disk_inode_record {
	__le64 inode_number;
	__le64 size;
	__le64 manifest_id;
	__le64 first_extent_record;
	__le32 mode;
	__le32 extent_count;
	__le64 mtime_ns;
	__le64 ctime_ns;
};

struct cxnfs_disk_journal_record {
	__le64 sequence;
	__le16 record_type;
	__le16 flags;
	__le32 payload_size;
	__le32 checksum_crc32c;
	__le32 reserved;
};

#endif
