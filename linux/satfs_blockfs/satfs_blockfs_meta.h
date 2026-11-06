#ifndef SATFS_BLOCKFS_META_H
#define SATFS_BLOCKFS_META_H

#ifdef __KERNEL__
#include <linux/types.h>
#else
#include <stdint.h>
typedef uint8_t __u8;
typedef uint16_t __le16;
typedef uint32_t __le32;
typedef uint64_t __le64;
#endif

#define SATFS_META_ROOT_MAGIC "SATFSMRT"
#define SATFS_META_FILE_MAGIC "SATFSFIL"
#define SATFS_META_MAGIC_LEN 8
#define SATFS_META_VERSION 1

/* Legacy compatibility aliases */
#define CXNFS_META_ROOT_MAGIC "CXNFSMRT"
#define CXNFS_META_FILE_MAGIC "CXNFSFIL"
#define CXNFS_META_MAGIC_LEN SATFS_META_MAGIC_LEN
#define CXNFS_META_VERSION SATFS_META_VERSION

struct satfs_meta_header {
	__u8 magic[SATFS_META_MAGIC_LEN];
	__le32 version;
	__le32 header_bytes;
	__le64 generation;
	__le32 flags;
	__le32 checksum_crc32c;
};

#define cxnfs_meta_header satfs_meta_header

struct satfs_meta_root {
	struct satfs_meta_header header;
	__u8 fs_uuid[16];
	__u8 label[64];
	__le64 root_inode;
	__le64 file_count;
	__le64 total_blocks;
	__le32 block_size;
	__le32 reserved;
};

#define cxnfs_meta_root satfs_meta_root

struct satfs_meta_file {
	struct satfs_meta_header header;
	__le64 inode_number;
	__le64 size;
	__le64 manifest_id;
	__le32 mode;
	__le32 extent_count;
	__le64 mtime_ns;
	__le64 ctime_ns;
	__u8 expected_hash[32];
	__le32 path_len;
	__u8 reserved[28];
};

#define cxnfs_meta_file satfs_meta_file

#endif /* SATFS_BLOCKFS_META_H */
