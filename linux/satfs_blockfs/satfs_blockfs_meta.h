#ifndef SATFS_BLOCKFS_META_H
#define SATFS_BLOCKFS_META_H

#include "satfs_blockfs_disk.h"

#define SATFS_META_ROOT_MAGIC "SATFSMRT"
#define SATFS_META_FILE_MAGIC "SATFSFIL"
#define SATFS_META_MAGIC_LEN 8
#define SATFS_META_VERSION 1

#define SATFS_META_FLAG_BOOTSTRAP_READY (1ULL << 0)
#define SATFS_META_FLAG_TRAILING_READY (1ULL << 1)
#define SATFS_META_FLAG_HASH_VERIFIED (1ULL << 2)

#define SATFS_META_EXTENT_VERIFIED (1U << 0)
#define SATFS_META_EXTENT_CAN_READ_EARLY (1U << 1)
#define SATFS_META_EXTENT_PUBLISH_NEEDS_TRAILING (1U << 2)

enum satfs_meta_structure_role {
	SATFS_META_ROLE_AUTHORIZATION_GRANT = 1,
	SATFS_META_ROLE_NAMESPACE_SNAPSHOT = 2,
	SATFS_META_ROLE_FILE_EXTENT_HINTS = 3,
	SATFS_META_ROLE_MANIFEST_INDEX = 4,
	SATFS_META_ROLE_CHUNK_HASH_INDEX = 5,
	SATFS_META_ROLE_CACHE_INDEX = 6,
	SATFS_META_ROLE_JOURNAL = 7,
	SATFS_META_ROLE_FREE_SPACE_MAP = 8,
};

/* Legacy compatibility aliases */
#define CXNFS_META_ROOT_MAGIC "CXNFSMRT"
#define CXNFS_META_FILE_MAGIC "CXNFSFIL"
#define CXNFS_META_MAGIC_LEN SATFS_META_MAGIC_LEN
#define CXNFS_META_VERSION SATFS_META_VERSION

#define CXNFS_META_FLAG_BOOTSTRAP_READY SATFS_META_FLAG_BOOTSTRAP_READY
#define CXNFS_META_FLAG_TRAILING_READY SATFS_META_FLAG_TRAILING_READY
#define CXNFS_META_FLAG_HASH_VERIFIED SATFS_META_FLAG_HASH_VERIFIED

#define CXNFS_META_EXTENT_VERIFIED SATFS_META_EXTENT_VERIFIED
#define CXNFS_META_EXTENT_CAN_READ_EARLY SATFS_META_EXTENT_CAN_READ_EARLY
#define CXNFS_META_EXTENT_PUBLISH_NEEDS_TRAILING SATFS_META_EXTENT_PUBLISH_NEEDS_TRAILING

#define cxnfs_meta_structure_role satfs_meta_structure_role
#define CXNFS_META_ROLE_AUTHORIZATION_GRANT SATFS_META_ROLE_AUTHORIZATION_GRANT
#define CXNFS_META_ROLE_NAMESPACE_SNAPSHOT SATFS_META_ROLE_NAMESPACE_SNAPSHOT
#define CXNFS_META_ROLE_FILE_EXTENT_HINTS SATFS_META_ROLE_FILE_EXTENT_HINTS
#define CXNFS_META_ROLE_MANIFEST_INDEX SATFS_META_ROLE_MANIFEST_INDEX
#define CXNFS_META_ROLE_CHUNK_HASH_INDEX SATFS_META_ROLE_CHUNK_HASH_INDEX
#define CXNFS_META_ROLE_CACHE_INDEX SATFS_META_ROLE_CACHE_INDEX
#define CXNFS_META_ROLE_JOURNAL SATFS_META_ROLE_JOURNAL
#define CXNFS_META_ROLE_FREE_SPACE_MAP SATFS_META_ROLE_FREE_SPACE_MAP

struct satfs_meta_root_header {
	__u8 magic[SATFS_META_MAGIC_LEN];
	__le32 version;
	__le32 header_size;
	__u8 fs_uuid[SATFS_DISK_UUID_LEN];
	__le64 flags;
	__le64 namespace_generation;
	__le64 bootstrap_generation;
	__le64 trailing_generation;
	__le32 max_cords;
	__le32 readahead_pages;
	__u8 reserved[96];
};

#define cxnfs_meta_root_header satfs_meta_root_header

struct satfs_meta_structure_ref {
	__le16 role;
	__le16 flags;
	__le32 path_length;
	__le64 generation;
	__le64 size_bytes;
	__u8 expected_hash[32];
};

#define cxnfs_meta_structure_ref satfs_meta_structure_ref

struct satfs_meta_file_header {
	__u8 magic[SATFS_META_MAGIC_LEN];
	__le32 version;
	__le32 header_size;
	__le64 inode_number;
	__le64 file_size;
	__le64 manifest_id;
	__le64 generation;
	__le32 extent_count;
	__le32 chunk_count;
	__u8 manifest_hash[32];
	__u8 reserved[96];
};

#define cxnfs_meta_file_header satfs_meta_file_header

struct satfs_meta_file_extent {
	__le64 file_offset;
	__le64 payload_block;
	__le32 payload_block_count;
	__le32 payload_offset;
	__le32 payload_length;
	__le32 flags;
	__u8 expected_hash[32];
};

#define cxnfs_meta_file_extent satfs_meta_file_extent

#endif /* SATFS_BLOCKFS_META_H */
