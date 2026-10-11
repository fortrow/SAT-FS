#ifndef CXNFS_BLOCKFS_H
#define CXNFS_BLOCKFS_H

#include <linux/atomic.h>
#include <linux/list.h>
#include <linux/types.h>

struct super_block;

#define CXNFS_BLOCKFS_NAME "cxnfsblk"
#define CXNFS_BLOCKFS_SUPER_MAGIC 0x43584e42
#define CXNFS_BLOCKFS_DEFAULT_READAHEAD_PAGES 128
#define CXNFS_BLOCKFS_DEFAULT_MISS_WAIT_MS 5000
#define CXNFS_BLOCKFS_MAX_CORDS 64
#define CXNFS_BLOCKFS_METADATA_ROOT_MAX 256

struct cxnfs_blockfs_sb_info {
	struct list_head mount_node;
	struct super_block *sb;
	u64 mount_id;
	u32 block_size;
	u32 max_cords;
	u32 readahead_pages;
	u32 miss_wait_ms;
	bool verify_hashes;
	u64 flags;
	char *metadata_root;
	atomic64_t read_folio_calls;
	atomic64_t readahead_calls;
	atomic64_t registered_extents;
	atomic64_t invalidations;
	atomic64_t verified_page_fills;
	u64 manifest_index_start_block;
	u64 manifest_index_block_count;
	u64 cache_index_start_block;
	u64 cache_index_block_count;
	u64 container_cache_start_block;
	u64 container_cache_block_count;
	u64 free_map_start_block;
	u64 free_map_block_count;
};

#endif
