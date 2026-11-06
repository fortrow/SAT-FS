#ifndef SATFS_BLOCKFS_DISK_H
#define SATFS_BLOCKFS_DISK_H

#ifdef __KERNEL__
#include <linux/types.h>
#else
#include <stdint.h>
typedef uint8_t __u8;
typedef uint16_t __le16;
typedef uint32_t __le32;
typedef uint64_t __le64;
#endif

#define SATFS_DISK_MAGIC "SATFSBFS"
#define SATFS_DISK_MAGIC_LEN 8
#define SATFS_DISK_VERSION 1
#define SATFS_DISK_BLOCK_SIZE 4096
#define SATFS_DISK_UUID_LEN 16
#define SATFS_DISK_LABEL_LEN 64
#define SATFS_DISK_MAX_CORDS 64
#define SATFS_DISK_DEFAULT_READAHEAD_PAGES 128
#define SATFS_DISK_FLAG_OS_STRUCTURES_REQUIRED (1ULL << 0)
#define SATFS_DISK_FLAG_TRAILING_METADATA_ALLOWED (1ULL << 1)

/* Legacy compatibility aliases */
#define CXNFS_DISK_MAGIC "CXNFSBFS"
#define CXNFS_DISK_MAGIC_LEN SATFS_DISK_MAGIC_LEN
#define CXNFS_DISK_VERSION SATFS_DISK_VERSION
#define CXNFS_DISK_BLOCK_SIZE SATFS_DISK_BLOCK_SIZE
#define CXNFS_DISK_UUID_LEN SATFS_DISK_UUID_LEN
#define CXNFS_DISK_LABEL_LEN SATFS_DISK_LABEL_LEN
#define CXNFS_DISK_MAX_CORDS SATFS_DISK_MAX_CORDS
#define CXNFS_DISK_DEFAULT_READAHEAD_PAGES SATFS_DISK_DEFAULT_READAHEAD_PAGES
#define CXNFS_DISK_FLAG_OS_STRUCTURES_REQUIRED SATFS_DISK_FLAG_OS_STRUCTURES_REQUIRED
#define CXNFS_DISK_FLAG_TRAILING_METADATA_ALLOWED SATFS_DISK_FLAG_TRAILING_METADATA_ALLOWED

enum satfs_disk_zone_kind {
	SATFS_ZONE_FREE_MAP = 1,
	SATFS_ZONE_LOCAL_EXTENT_INDEX = 2,
	SATFS_ZONE_MANIFEST_INDEX = 3,
	SATFS_ZONE_CONTAINER_CACHE = 4,
	SATFS_ZONE_DATA_EXTENTS = 5,
	SATFS_ZONE_JOURNAL = 6,
	SATFS_ZONE_CACHE_INDEX = 7,
};

#define cxnfs_disk_zone_kind satfs_disk_zone_kind
#define CXNFS_ZONE_FREE_MAP SATFS_ZONE_FREE_MAP
#define CXNFS_ZONE_LOCAL_EXTENT_INDEX SATFS_ZONE_LOCAL_EXTENT_INDEX
#define CXNFS_ZONE_MANIFEST_INDEX SATFS_ZONE_MANIFEST_INDEX
#define CXNFS_ZONE_CONTAINER_CACHE SATFS_ZONE_CONTAINER_CACHE
#define CXNFS_ZONE_DATA_EXTENTS SATFS_ZONE_DATA_EXTENTS
#define CXNFS_ZONE_JOURNAL SATFS_ZONE_JOURNAL
#define CXNFS_ZONE_CACHE_INDEX SATFS_ZONE_CACHE_INDEX

struct satfs_disk_zone {
	__le16 kind;
	__le16 flags;
	__le32 reserved;
	__le64 start_block;
	__le64 block_count;
};

#define cxnfs_disk_zone satfs_disk_zone

struct satfs_disk_super {
	__u8 magic[SATFS_DISK_MAGIC_LEN];
	__le32 version;
	__le32 block_size;
	__u8 fs_uuid[SATFS_DISK_UUID_LEN];
	__u8 label[SATFS_DISK_LABEL_LEN];
	__le64 flags;
	__le64 total_blocks;
	__le64 root_inode;
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
	struct satfs_disk_zone zones[8];
	__u8 reserved[3688];
};

#define cxnfs_disk_super satfs_disk_super

struct satfs_disk_extent {
	__le64 logical_offset;
	__le64 physical_block;
	__le32 block_count;
	__le32 flags;
	__u8 expected_hash[32];
};

#define cxnfs_disk_extent satfs_disk_extent

struct satfs_disk_inode_record {
	__le64 inode_number;
	__le64 size;
	__le64 manifest_id;
	__le64 first_extent_record;
	__le32 mode;
	__le32 extent_count;
	__le64 mtime_ns;
	__le64 ctime_ns;
};

#define cxnfs_disk_inode_record satfs_disk_inode_record

#endif /* SATFS_BLOCKFS_DISK_H */
