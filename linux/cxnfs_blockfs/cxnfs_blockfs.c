#include <crypto/hash.h>
#include <crypto/sha2.h>

#include <linux/blkdev.h>
#include <linux/buffer_head.h>
#include <linux/cred.h>
#include <linux/err.h>
#include <linux/fs.h>
#include <linux/fs_context.h>
#include <linux/init.h>
#include <linux/jiffies.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/limits.h>
#include <linux/math64.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pagemap.h>
#include <linux/highmem.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/wait.h>

#include "cxnfs_blockfs.h"
#include "cxnfs_blockfs_disk.h"
#include "cxnfs_blockfs_uapi.h"

static struct inode *cxnfs_blockfs_get_inode(struct super_block *sb,
					     unsigned long inode_number,
					     umode_t mode, loff_t size);
static int cxnfs_blockfs_check_abi(u32 abi_version);
static int cxnfs_blockfs_invalidate_cache_range(
	const struct cxnfs_blockfs_ioc_invalidate *invalidate,
	loff_t *start,
	loff_t *end);
static void cxnfs_blockfs_invalidate_page_cache(
	const struct cxnfs_blockfs_ioc_invalidate *invalidate);
static int cxnfs_blockfs_resolve_mount_selector(u64 requested_mount_id,
						const char *metadata_root,
						u64 *mount_id);

struct cxnfs_blockfs_mount_options {
	char metadata_root[CXNFS_BLOCKFS_METADATA_ROOT_MAX];
	u32 max_cords;
	u32 readahead_pages;
	u32 miss_wait_ms;
	bool miss_wait_ms_set;
	bool verify_hashes;
	bool verify_hashes_set;
};

struct cxnfs_blockfs_extent_entry {
	struct list_head node;
	struct cxnfs_blockfs_ioc_extent extent;
	bool hash_verified;
};

struct cxnfs_blockfs_file_entry {
	struct list_head node;
	u64 mount_id;
	u64 inode_number;
	u64 size;
	umode_t mode;
	bool synthetic;
	char path[CXNFS_BLOCKFS_PATH_MAX];
};

struct cxnfs_blockfs_miss_event_entry {
	struct list_head node;
	struct cxnfs_blockfs_ioc_miss_event event;
};

#define CXNFS_BLOCKFS_EXTENT_KNOWN_FLAGS \
	(CXNFS_BLOCKFS_EXTENT_VERIFIED | \
	 CXNFS_BLOCKFS_EXTENT_CAN_READ_EARLY | \
	 CXNFS_BLOCKFS_EXTENT_PUBLISH_NEEDS_TRAILING)
#define CXNFS_BLOCKFS_SYNTHETIC_INODE_START 1000000000ULL
#define CXNFS_BLOCKFS_MAX_MISS_EVENTS 4096U

static LIST_HEAD(cxnfs_blockfs_extent_index);
static LIST_HEAD(cxnfs_blockfs_file_index);
static LIST_HEAD(cxnfs_blockfs_mounts);
static LIST_HEAD(cxnfs_blockfs_miss_events);
static DEFINE_MUTEX(cxnfs_blockfs_extent_lock);
static DEFINE_MUTEX(cxnfs_blockfs_file_lock);
static DEFINE_MUTEX(cxnfs_blockfs_mount_lock);
static DEFINE_MUTEX(cxnfs_blockfs_miss_lock);
static DECLARE_WAIT_QUEUE_HEAD(cxnfs_blockfs_miss_wait);
static DECLARE_WAIT_QUEUE_HEAD(cxnfs_blockfs_extent_wait);
static atomic64_t cxnfs_blockfs_registered_extents = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_invalidations = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_read_folio_calls = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_readahead_calls = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_verified_page_fills = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_miss_events_queued = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_miss_events_drained = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_miss_wait_successes = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_miss_wait_timeouts = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_miss_wait_interruptions = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_hash_verifications = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_hash_mismatches = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_hash_bytes_verified = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_readahead_miss_windows = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_readahead_miss_bytes = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_next_synthetic_inode =
	ATOMIC64_INIT(CXNFS_BLOCKFS_SYNTHETIC_INODE_START);
static atomic64_t cxnfs_blockfs_next_miss_event_id = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_next_mount_id = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_dropped_miss_events = ATOMIC64_INIT(0);
static atomic64_t cxnfs_blockfs_extent_generation = ATOMIC64_INIT(0);
static u32 cxnfs_blockfs_miss_event_count;

static bool cxnfs_blockfs_inode_is_new(struct inode *inode)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(7, 0, 0)
	return inode_state_read_once(inode) & I_NEW;
#else
	return inode->i_state & I_NEW;
#endif
}

static bool cxnfs_blockfs_range_end(u64 start, u64 length, u64 *end)
{
	if (start > U64_MAX - length)
		return false;
	*end = start + length;
	return true;
}

static bool cxnfs_blockfs_ranges_overlap(u64 left_start, u64 left_length,
					 u64 right_start, u64 right_length)
{
	u64 left_end;
	u64 right_end;

	if (!left_length || !right_length)
		return false;
	if (!cxnfs_blockfs_range_end(left_start, left_length, &left_end))
		return false;
	if (!cxnfs_blockfs_range_end(right_start, right_length, &right_end))
		return false;
	return left_start < right_end && right_start < left_end;
}

static void cxnfs_blockfs_drop_oldest_miss_event_locked(void)
{
	struct cxnfs_blockfs_miss_event_entry *entry;

	if (list_empty(&cxnfs_blockfs_miss_events))
		return;
	entry = list_first_entry(&cxnfs_blockfs_miss_events,
				 struct cxnfs_blockfs_miss_event_entry, node);
	list_del(&entry->node);
	kfree(entry);
	if (cxnfs_blockfs_miss_event_count)
		cxnfs_blockfs_miss_event_count--;
	atomic64_inc(&cxnfs_blockfs_dropped_miss_events);
}

static void cxnfs_blockfs_record_miss_event(struct inode *inode, u64 offset,
					    u64 length, u32 reason)
{
	struct cxnfs_blockfs_miss_event_entry *entry;
	struct cxnfs_blockfs_sb_info *sbi = NULL;
	u32 mount_flags = 0;

	if (!length)
		return;
	entry = kzalloc(sizeof(*entry), GFP_NOFS);
	if (!entry) {
		atomic64_inc(&cxnfs_blockfs_dropped_miss_events);
		return;
	}
	if (inode && inode->i_sb)
		sbi = inode->i_sb->s_fs_info;
	if (sbi && sbi->verify_hashes)
		mount_flags |= CXNFS_BLOCKFS_MOUNT_VERIFY_HASHES;

	entry->event.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION;
	entry->event.event_id =
		(u64)atomic64_inc_return(&cxnfs_blockfs_next_miss_event_id);
	entry->event.inode_number = inode->i_ino;
	entry->event.offset = offset;
	entry->event.length = length;
	entry->event.reason = reason;
	if (sbi) {
		entry->event.mount_id = sbi->mount_id;
		entry->event.max_cords = sbi->max_cords;
		entry->event.readahead_pages = sbi->readahead_pages;
		entry->event.miss_wait_ms = sbi->miss_wait_ms;
		entry->event.mount_flags = mount_flags;
		if (sbi->metadata_root)
			strscpy(entry->event.metadata_root, sbi->metadata_root,
				sizeof(entry->event.metadata_root));
	}

	mutex_lock(&cxnfs_blockfs_miss_lock);
	while (cxnfs_blockfs_miss_event_count >= CXNFS_BLOCKFS_MAX_MISS_EVENTS)
		cxnfs_blockfs_drop_oldest_miss_event_locked();
	entry->event.dropped_events =
		(u64)atomic64_read(&cxnfs_blockfs_dropped_miss_events);
	list_add_tail(&entry->node, &cxnfs_blockfs_miss_events);
	cxnfs_blockfs_miss_event_count++;
	atomic64_inc(&cxnfs_blockfs_miss_events_queued);
	mutex_unlock(&cxnfs_blockfs_miss_lock);
	wake_up_interruptible(&cxnfs_blockfs_miss_wait);
}

static int cxnfs_blockfs_next_miss_event(
	struct cxnfs_blockfs_ioc_miss_event *event)
{
	struct cxnfs_blockfs_miss_event_entry *entry;
	bool nonblock = event->flags & CXNFS_BLOCKFS_MISS_EVENT_NONBLOCK;
	u32 flags = event->flags;
	u64 requested_mount_id = event->mount_id;
	char requested_metadata_root[CXNFS_BLOCKFS_PATH_MAX];
	int ret;

	if (cxnfs_blockfs_check_abi(event->abi_version))
		return -EINVAL;
	if (flags & ~CXNFS_BLOCKFS_MISS_EVENT_NONBLOCK)
		return -EINVAL;
	strscpy(requested_metadata_root, event->metadata_root,
		sizeof(requested_metadata_root));

	for (;;) {
		u64 seen_event_id;

		mutex_lock(&cxnfs_blockfs_miss_lock);
		list_for_each_entry(entry, &cxnfs_blockfs_miss_events, node) {
			if (requested_mount_id &&
			    entry->event.mount_id != requested_mount_id)
				continue;
			if (requested_metadata_root[0] &&
			    strcmp(entry->event.metadata_root,
				   requested_metadata_root) != 0)
				continue;
			list_del(&entry->node);
			if (cxnfs_blockfs_miss_event_count)
				cxnfs_blockfs_miss_event_count--;
			*event = entry->event;
			event->flags = flags;
			event->dropped_events =
				(u64)atomic64_read(&cxnfs_blockfs_dropped_miss_events);
			kfree(entry);
			atomic64_inc(&cxnfs_blockfs_miss_events_drained);
			mutex_unlock(&cxnfs_blockfs_miss_lock);
			return 0;
		}
		seen_event_id =
			(u64)atomic64_read(&cxnfs_blockfs_next_miss_event_id);
		mutex_unlock(&cxnfs_blockfs_miss_lock);
		if (nonblock)
			return -EAGAIN;
		ret = wait_event_interruptible(
			cxnfs_blockfs_miss_wait,
			(u64)atomic64_read(&cxnfs_blockfs_next_miss_event_id) !=
				seen_event_id);
		if (ret)
			return ret;
	}
}

static void cxnfs_blockfs_clear_miss_events(void)
{
	struct cxnfs_blockfs_miss_event_entry *entry;
	struct cxnfs_blockfs_miss_event_entry *tmp;

	mutex_lock(&cxnfs_blockfs_miss_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_miss_events, node) {
		list_del(&entry->node);
		kfree(entry);
	}
	cxnfs_blockfs_miss_event_count = 0;
	mutex_unlock(&cxnfs_blockfs_miss_lock);
}

static void cxnfs_blockfs_register_mount(struct super_block *sb,
					 struct cxnfs_blockfs_sb_info *sbi)
{
	INIT_LIST_HEAD(&sbi->mount_node);
	sbi->sb = sb;
	mutex_lock(&cxnfs_blockfs_mount_lock);
	list_add_tail(&sbi->mount_node, &cxnfs_blockfs_mounts);
	mutex_unlock(&cxnfs_blockfs_mount_lock);
}

static void cxnfs_blockfs_unregister_mount(struct cxnfs_blockfs_sb_info *sbi)
{
	mutex_lock(&cxnfs_blockfs_mount_lock);
	if (!list_empty(&sbi->mount_node))
		list_del_init(&sbi->mount_node);
	mutex_unlock(&cxnfs_blockfs_mount_lock);
	sbi->sb = NULL;
}

static bool cxnfs_blockfs_sbi_matches_selector(
	const struct cxnfs_blockfs_sb_info *sbi, u64 requested_mount_id,
	const char *metadata_root)
{
	bool has_metadata_root = metadata_root && metadata_root[0];

	if (requested_mount_id && sbi->mount_id != requested_mount_id)
		return false;
	if (has_metadata_root &&
	    (!sbi->metadata_root || strcmp(sbi->metadata_root, metadata_root) != 0))
		return false;
	return true;
}

static int cxnfs_blockfs_resolve_mount_selector(u64 requested_mount_id,
						const char *metadata_root,
						u64 *mount_id)
{
	struct cxnfs_blockfs_sb_info *sbi;
	u64 resolved_mount_id = 0;
	u32 matches = 0;

	mutex_lock(&cxnfs_blockfs_mount_lock);
	list_for_each_entry(sbi, &cxnfs_blockfs_mounts, mount_node) {
		if (!cxnfs_blockfs_sbi_matches_selector(sbi, requested_mount_id,
							metadata_root))
			continue;
		resolved_mount_id = sbi->mount_id;
		matches++;
	}
	mutex_unlock(&cxnfs_blockfs_mount_lock);

	if (!matches)
		return -ENODEV;
	if (matches > 1)
		return -EXDEV;
	*mount_id = resolved_mount_id;
	return 0;
}

static void cxnfs_blockfs_remove_extent_entry(
	struct cxnfs_blockfs_extent_entry *entry)
{
	list_del(&entry->node);
	kfree(entry);
	atomic64_dec(&cxnfs_blockfs_registered_extents);
}

static void cxnfs_blockfs_wake_extent_waiters(void)
{
	atomic64_inc(&cxnfs_blockfs_extent_generation);
	wake_up_all(&cxnfs_blockfs_extent_wait);
}

static int cxnfs_blockfs_validate_extent(
	const struct cxnfs_blockfs_ioc_extent *extent)
{
	u64 unused_end;

	if (cxnfs_blockfs_check_abi(extent->abi_version))
		return -EINVAL;
	if (!extent->inode_number || !extent->payload_length)
		return -EINVAL;
	if (!(extent->flags & CXNFS_BLOCKFS_EXTENT_VERIFIED))
		return -EINVAL;
	if (extent->flags & ~CXNFS_BLOCKFS_EXTENT_KNOWN_FLAGS)
		return -EINVAL;
	if (extent->payload_offset >= CXNFS_DISK_BLOCK_SIZE)
		return -EINVAL;
	if (!cxnfs_blockfs_range_end(extent->file_offset,
				     extent->payload_length, &unused_end))
		return -EINVAL;
	return 0;
}

static bool cxnfs_blockfs_same_extent(
	const struct cxnfs_blockfs_ioc_extent *left,
	const struct cxnfs_blockfs_ioc_extent *right)
{
	return left->mount_id == right->mount_id &&
	       left->inode_number == right->inode_number &&
	       left->file_offset == right->file_offset &&
	       left->payload_block == right->payload_block &&
	       left->payload_offset == right->payload_offset &&
	       left->payload_length == right->payload_length &&
	       memcmp(left->expected_hash, right->expected_hash,
		      sizeof(left->expected_hash)) == 0;
}

static int cxnfs_blockfs_register_extent(
	const struct cxnfs_blockfs_ioc_extent *extent)
{
	struct cxnfs_blockfs_extent_entry *entry;
	struct cxnfs_blockfs_extent_entry *cursor;
	struct cxnfs_blockfs_extent_entry *tmp;
	u64 mount_id;
	struct cxnfs_blockfs_ioc_invalidate invalidate = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
		.inode_number = extent->inode_number,
		.offset = extent->file_offset,
		.length = extent->payload_length,
	};
	int ret;

	ret = cxnfs_blockfs_resolve_mount_selector(extent->mount_id,
						   extent->metadata_root,
						   &mount_id);
	if (ret)
		return ret;
	entry = kmalloc(sizeof(*entry), GFP_KERNEL);
	if (!entry)
		return -ENOMEM;
	entry->extent = *extent;
	entry->extent.mount_id = mount_id;
	entry->extent.metadata_root[0] = '\0';
	entry->hash_verified = false;
	invalidate.mount_id = mount_id;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry_safe(cursor, tmp, &cxnfs_blockfs_extent_index, node) {
		if (cursor->extent.mount_id != mount_id)
			continue;
		if (cursor->extent.inode_number != extent->inode_number)
			continue;
		if (!cxnfs_blockfs_ranges_overlap(cursor->extent.file_offset,
						  cursor->extent.payload_length,
						  extent->file_offset,
						  extent->payload_length))
			continue;
		cxnfs_blockfs_remove_extent_entry(cursor);
	}
	list_add_tail(&entry->node, &cxnfs_blockfs_extent_index);
	atomic64_inc(&cxnfs_blockfs_registered_extents);
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	cxnfs_blockfs_invalidate_page_cache(&invalidate);
	cxnfs_blockfs_wake_extent_waiters();
	return 0;
}

static int cxnfs_blockfs_invalidate_extents(
	const struct cxnfs_blockfs_ioc_invalidate *invalidate)
{
	struct cxnfs_blockfs_extent_entry *entry;
	struct cxnfs_blockfs_extent_entry *tmp;
	struct cxnfs_blockfs_ioc_invalidate resolved = *invalidate;
	u64 unused_end;
	loff_t cache_start;
	loff_t cache_end;
	u64 mount_id;
	int ret;

	if (cxnfs_blockfs_check_abi(invalidate->abi_version))
		return -EINVAL;
	if (!invalidate->inode_number)
		return -EINVAL;
	ret = cxnfs_blockfs_resolve_mount_selector(invalidate->mount_id,
						   invalidate->metadata_root,
						   &mount_id);
	if (ret)
		return ret;
	resolved.mount_id = mount_id;
	resolved.metadata_root[0] = '\0';
	if (invalidate->length &&
	    !cxnfs_blockfs_range_end(invalidate->offset, invalidate->length,
				     &unused_end))
		return -EINVAL;
	if (cxnfs_blockfs_invalidate_cache_range(&resolved, &cache_start,
						 &cache_end))
		return -EINVAL;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_extent_index, node) {
		if (entry->extent.mount_id != mount_id)
			continue;
		if (entry->extent.inode_number != invalidate->inode_number)
			continue;
		if (invalidate->length &&
		    !cxnfs_blockfs_ranges_overlap(entry->extent.file_offset,
						  entry->extent.payload_length,
						  invalidate->offset,
						  invalidate->length))
			continue;
		cxnfs_blockfs_remove_extent_entry(entry);
	}
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	cxnfs_blockfs_invalidate_page_cache(&resolved);
	atomic64_inc(&cxnfs_blockfs_invalidations);
	cxnfs_blockfs_wake_extent_waiters();
	return 0;
}

static void cxnfs_blockfs_clear_extent_index(void)
{
	struct cxnfs_blockfs_extent_entry *entry;
	struct cxnfs_blockfs_extent_entry *tmp;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_extent_index, node)
		cxnfs_blockfs_remove_extent_entry(entry);
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	cxnfs_blockfs_wake_extent_waiters();
}

static void cxnfs_blockfs_clear_mount_extent_index(u64 mount_id)
{
	struct cxnfs_blockfs_extent_entry *entry;
	struct cxnfs_blockfs_extent_entry *tmp;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_extent_index, node) {
		if (entry->extent.mount_id == mount_id)
			cxnfs_blockfs_remove_extent_entry(entry);
	}
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	cxnfs_blockfs_wake_extent_waiters();
}

static int cxnfs_blockfs_invalidate_cache_range(
	const struct cxnfs_blockfs_ioc_invalidate *invalidate,
	loff_t *start,
	loff_t *end)
{
	u64 range_end;

	if (!invalidate->length) {
		*start = 0;
		*end = LLONG_MAX;
		return 0;
	}
	if (invalidate->offset > LLONG_MAX)
		return -EINVAL;
	if (!cxnfs_blockfs_range_end(invalidate->offset, invalidate->length,
				     &range_end))
		return -EINVAL;

	*start = (loff_t)invalidate->offset;
	if (range_end - 1 > LLONG_MAX)
		*end = LLONG_MAX;
	else
		*end = (loff_t)(range_end - 1);
	return 0;
}

static void cxnfs_blockfs_invalidate_page_cache(
	const struct cxnfs_blockfs_ioc_invalidate *invalidate)
{
	struct cxnfs_blockfs_sb_info *sbi;
	loff_t start;
	loff_t end;

	if (cxnfs_blockfs_invalidate_cache_range(invalidate, &start, &end))
		return;

	mutex_lock(&cxnfs_blockfs_mount_lock);
	list_for_each_entry(sbi, &cxnfs_blockfs_mounts, mount_node) {
		struct inode *inode;

		if (invalidate->mount_id && sbi->mount_id != invalidate->mount_id)
			continue;
		if (invalidate->inode_number > ULONG_MAX)
			continue;
		inode = ilookup(sbi->sb, (unsigned long)invalidate->inode_number);
		if (!inode)
			continue;
		truncate_inode_pages_range(inode->i_mapping, start, end);
		iput(inode);
	}
	mutex_unlock(&cxnfs_blockfs_mount_lock);
}

static void cxnfs_blockfs_refresh_file_cache(u64 mount_id, u64 inode_number,
					     umode_t mode, u64 size)
{
	struct cxnfs_blockfs_sb_info *sbi;
	struct cxnfs_blockfs_ioc_invalidate invalidate = {
		.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
		.mount_id = mount_id,
		.inode_number = inode_number,
		.offset = 0,
		.length = 0,
	};

	if (inode_number > ULONG_MAX || size > LLONG_MAX)
		return;

	mutex_lock(&cxnfs_blockfs_mount_lock);
	list_for_each_entry(sbi, &cxnfs_blockfs_mounts, mount_node) {
		struct inode *inode;

		if (sbi->mount_id != mount_id)
			continue;
		inode = ilookup(sbi->sb, (unsigned long)inode_number);
		if (!inode)
			continue;
		inode_lock(inode);
		inode->i_mode = mode;
		i_size_write(inode, (loff_t)size);
		inode_unlock(inode);
		iput(inode);
	}
	mutex_unlock(&cxnfs_blockfs_mount_lock);
	cxnfs_blockfs_invalidate_page_cache(&invalidate);
}

static bool cxnfs_blockfs_valid_path_component(const char *start, size_t len)
{
	if (!len || len > NAME_MAX)
		return false;
	if (len == 1 && start[0] == '.')
		return false;
	if (len == 2 && start[0] == '.' && start[1] == '.')
		return false;
	for (size_t i = 0; i < len; i++) {
		unsigned char c = start[i];

		if (c < 0x20 || c == 0x7f)
			return false;
	}
	return true;
}

static bool cxnfs_blockfs_valid_relative_path(const char *path)
{
	const char *component;
	size_t len;

	len = strnlen(path, CXNFS_BLOCKFS_PATH_MAX);
	if (!len || len >= CXNFS_BLOCKFS_PATH_MAX)
		return false;
	if (path[0] == '/' || path[len - 1] == '/')
		return false;
	if (strchr(path, '\\'))
		return false;

	component = path;
	for (size_t i = 0; i <= len; i++) {
		if (path[i] != '/' && path[i] != '\0')
			continue;
		if (!cxnfs_blockfs_valid_path_component(component,
						       &path[i] - component))
			return false;
		component = &path[i + 1];
	}
	return true;
}

static struct cxnfs_blockfs_file_entry *cxnfs_blockfs_alloc_file_entry(
	u64 mount_id, u64 inode_number, u64 size, umode_t mode,
	const char *path, bool synthetic)
{
	struct cxnfs_blockfs_file_entry *entry;

	entry = kmalloc(sizeof(*entry), GFP_KERNEL);
	if (!entry)
		return NULL;
	entry->mount_id = mount_id;
	entry->inode_number = inode_number;
	entry->size = size;
	entry->mode = mode;
	entry->synthetic = synthetic;
	strscpy(entry->path, path, sizeof(entry->path));
	return entry;
}

static void cxnfs_blockfs_remove_file_entry(
	struct cxnfs_blockfs_file_entry *entry)
{
	list_del(&entry->node);
	kfree(entry);
}

static struct cxnfs_blockfs_file_entry *
cxnfs_blockfs_find_file_entry_by_path_locked(u64 mount_id, const char *path)
{
	struct cxnfs_blockfs_file_entry *entry;

	list_for_each_entry(entry, &cxnfs_blockfs_file_index, node) {
		if (entry->mount_id == mount_id && strcmp(entry->path, path) == 0)
			return entry;
	}
	return NULL;
}

static struct cxnfs_blockfs_file_entry *
cxnfs_blockfs_find_file_entry_by_inode_locked(u64 mount_id, u64 inode_number)
{
	struct cxnfs_blockfs_file_entry *entry;

	list_for_each_entry(entry, &cxnfs_blockfs_file_index, node) {
		if (entry->mount_id == mount_id &&
		    entry->inode_number == inode_number)
			return entry;
	}
	return NULL;
}

static int cxnfs_blockfs_ensure_parent_directories_locked(u64 mount_id,
							  const char *path)
{
	char dir_path[CXNFS_BLOCKFS_PATH_MAX];
	size_t len = strlen(path);

	for (size_t i = 0; i < len; i++) {
		struct cxnfs_blockfs_file_entry *existing;
		struct cxnfs_blockfs_file_entry *entry;
		u64 inode_number;

		if (path[i] != '/')
			continue;
		if (i >= sizeof(dir_path))
			return -ENAMETOOLONG;
		memcpy(dir_path, path, i);
		dir_path[i] = '\0';

		existing =
			cxnfs_blockfs_find_file_entry_by_path_locked(mount_id,
								     dir_path);
		if (existing) {
			if (!S_ISDIR(existing->mode))
				return -ENOTDIR;
			continue;
		}

		inode_number =
			(u64)atomic64_inc_return(&cxnfs_blockfs_next_synthetic_inode);
		entry = cxnfs_blockfs_alloc_file_entry(mount_id, inode_number, 0,
						       S_IFDIR | 0755,
						       dir_path, true);
		if (!entry)
			return -ENOMEM;
		list_add_tail(&entry->node, &cxnfs_blockfs_file_index);
	}
	return 0;
}

static int cxnfs_blockfs_register_file(
	const struct cxnfs_blockfs_ioc_file *file)
{
	struct cxnfs_blockfs_file_entry *entry;
	struct cxnfs_blockfs_file_entry *cursor;
	struct cxnfs_blockfs_file_entry *tmp;
	u64 mount_id;
	int ret = 0;

	if (cxnfs_blockfs_check_abi(file->abi_version))
		return -EINVAL;
	if (!file->inode_number)
		return -EINVAL;
	if (file->size > LLONG_MAX)
		return -EINVAL;
	if (!cxnfs_blockfs_valid_relative_path(file->path))
		return -EINVAL;
	if ((file->mode & ~0777U) != 0)
		return -EINVAL;
	ret = cxnfs_blockfs_resolve_mount_selector(file->mount_id,
						   file->metadata_root,
						   &mount_id);
	if (ret)
		return ret;

	entry = cxnfs_blockfs_alloc_file_entry(
		mount_id, file->inode_number, file->size,
		S_IFREG | (file->mode ? file->mode : 0444), file->path, false);
	if (!entry)
		return -ENOMEM;

	mutex_lock(&cxnfs_blockfs_file_lock);
	ret = cxnfs_blockfs_ensure_parent_directories_locked(mount_id,
							    entry->path);
	if (ret)
		goto out_unlock;

	cursor = cxnfs_blockfs_find_file_entry_by_path_locked(mount_id,
							     entry->path);
	if (cursor && S_ISDIR(cursor->mode)) {
		ret = -EISDIR;
		goto out_unlock;
	}
	cursor = cxnfs_blockfs_find_file_entry_by_inode_locked(mount_id,
							      entry->inode_number);
	if (cursor && S_ISDIR(cursor->mode)) {
		ret = -EEXIST;
		goto out_unlock;
	}

	list_for_each_entry_safe(cursor, tmp, &cxnfs_blockfs_file_index, node) {
		if (cursor->mount_id != mount_id)
			continue;
		if (S_ISDIR(cursor->mode))
			continue;
		if (cursor->inode_number != entry->inode_number &&
		    strcmp(cursor->path, entry->path) != 0)
			continue;
		cxnfs_blockfs_remove_file_entry(cursor);
	}
	list_add_tail(&entry->node, &cxnfs_blockfs_file_index);
	entry = NULL;

out_unlock:
	mutex_unlock(&cxnfs_blockfs_file_lock);
	if (entry)
		kfree(entry);
	if (ret)
		return ret;
	cxnfs_blockfs_refresh_file_cache(mount_id, file->inode_number,
					 S_IFREG | (file->mode ? file->mode : 0444),
					 file->size);
	return 0;
}

static void cxnfs_blockfs_clear_file_index(void)
{
	struct cxnfs_blockfs_file_entry *entry;
	struct cxnfs_blockfs_file_entry *tmp;

	mutex_lock(&cxnfs_blockfs_file_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_file_index, node) {
		cxnfs_blockfs_remove_file_entry(entry);
	}
	mutex_unlock(&cxnfs_blockfs_file_lock);
}

static void cxnfs_blockfs_clear_mount_file_index(u64 mount_id)
{
	struct cxnfs_blockfs_file_entry *entry;
	struct cxnfs_blockfs_file_entry *tmp;

	mutex_lock(&cxnfs_blockfs_file_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_file_index, node) {
		if (entry->mount_id == mount_id)
			cxnfs_blockfs_remove_file_entry(entry);
	}
	mutex_unlock(&cxnfs_blockfs_file_lock);
}

static void cxnfs_blockfs_invalidate_registered_file_pages_for_mount(u64 mount_id)
{
	struct cxnfs_blockfs_file_entry *entry;

	mutex_lock(&cxnfs_blockfs_file_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_file_index, node) {
		struct cxnfs_blockfs_ioc_invalidate invalidate = {
			.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
			.mount_id = entry->mount_id,
			.inode_number = entry->inode_number,
			.offset = 0,
			.length = 0,
		};

		if ((mount_id == 0 || entry->mount_id == mount_id) &&
		    S_ISREG(entry->mode))
			cxnfs_blockfs_invalidate_page_cache(&invalidate);
	}
	mutex_unlock(&cxnfs_blockfs_file_lock);
}

static void cxnfs_blockfs_invalidate_registered_file_pages(void)
{
	cxnfs_blockfs_invalidate_registered_file_pages_for_mount(0);
}

static void cxnfs_blockfs_shrink_mount_dentries_for_mount(u64 mount_id)
{
	struct cxnfs_blockfs_sb_info *sbi;

	mutex_lock(&cxnfs_blockfs_mount_lock);
	list_for_each_entry(sbi, &cxnfs_blockfs_mounts, mount_node) {
		if (mount_id && sbi->mount_id != mount_id)
			continue;
		if (sbi->sb)
			shrink_dcache_sb(sbi->sb);
	}
	mutex_unlock(&cxnfs_blockfs_mount_lock);
}

static void cxnfs_blockfs_shrink_mount_dentries(void)
{
	cxnfs_blockfs_shrink_mount_dentries_for_mount(0);
}

static void cxnfs_blockfs_clear_miss_events_for_mount(u64 mount_id)
{
	struct cxnfs_blockfs_miss_event_entry *entry;
	struct cxnfs_blockfs_miss_event_entry *tmp;

	mutex_lock(&cxnfs_blockfs_miss_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_miss_events, node) {
		if (entry->event.mount_id != mount_id)
			continue;
		list_del(&entry->node);
		kfree(entry);
		if (cxnfs_blockfs_miss_event_count)
			cxnfs_blockfs_miss_event_count--;
	}
	mutex_unlock(&cxnfs_blockfs_miss_lock);
}

static void cxnfs_blockfs_clear_mount_namespace(u64 mount_id)
{
	cxnfs_blockfs_invalidate_registered_file_pages_for_mount(mount_id);
	cxnfs_blockfs_clear_mount_extent_index(mount_id);
	cxnfs_blockfs_clear_mount_file_index(mount_id);
	cxnfs_blockfs_clear_miss_events_for_mount(mount_id);
	cxnfs_blockfs_shrink_mount_dentries_for_mount(mount_id);
	atomic64_inc(&cxnfs_blockfs_invalidations);
}

static void cxnfs_blockfs_clear_live_namespace(void)
{
	cxnfs_blockfs_invalidate_registered_file_pages();
	cxnfs_blockfs_clear_extent_index();
	cxnfs_blockfs_clear_file_index();
	cxnfs_blockfs_clear_miss_events();
	cxnfs_blockfs_shrink_mount_dentries();
	atomic64_inc(&cxnfs_blockfs_invalidations);
}

static bool cxnfs_blockfs_copy_entry_by_path(
	u64 mount_id, const char *path, struct cxnfs_blockfs_file_entry *out)
{
	struct cxnfs_blockfs_file_entry *entry;
	bool found = false;

	mutex_lock(&cxnfs_blockfs_file_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_file_index, node) {
		if (entry->mount_id != mount_id || strcmp(entry->path, path) != 0)
			continue;
		*out = *entry;
		found = true;
		break;
	}
	mutex_unlock(&cxnfs_blockfs_file_lock);
	return found;
}

static bool cxnfs_blockfs_copy_dir_path(struct inode *dir,
					char out[CXNFS_BLOCKFS_PATH_MAX])
{
	struct cxnfs_blockfs_sb_info *sbi = dir->i_sb->s_fs_info;
	struct cxnfs_blockfs_file_entry *entry;
	bool found = false;

	if (dir->i_sb->s_root && d_inode(dir->i_sb->s_root) == dir) {
		out[0] = '\0';
		return true;
	}

	mutex_lock(&cxnfs_blockfs_file_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_file_index, node) {
		if (entry->mount_id != sbi->mount_id ||
		    entry->inode_number != dir->i_ino || !S_ISDIR(entry->mode))
			continue;
		strscpy(out, entry->path, CXNFS_BLOCKFS_PATH_MAX);
		found = true;
		break;
	}
	mutex_unlock(&cxnfs_blockfs_file_lock);
	return found;
}

static bool cxnfs_blockfs_build_child_path(
	const char *parent, const struct qstr *name,
	char out[CXNFS_BLOCKFS_PATH_MAX])
{
	size_t parent_len = strlen(parent);
	size_t child_len = name->len;
	size_t needed;

	if (!cxnfs_blockfs_valid_path_component((const char *)name->name,
						child_len))
		return false;
	needed = parent_len ? parent_len + 1 + child_len : child_len;
	if (needed >= CXNFS_BLOCKFS_PATH_MAX)
		return false;
	if (parent_len) {
		memcpy(out, parent, parent_len);
		out[parent_len] = '/';
		memcpy(out + parent_len + 1, name->name, child_len);
	} else {
		memcpy(out, name->name, child_len);
	}
	out[needed] = '\0';
	return true;
}

static bool cxnfs_blockfs_immediate_child(const char *parent,
					  const char *path,
					  const char **name,
					  size_t *name_len)
{
	size_t parent_len = strlen(parent);
	const char *child;
	const char *slash;

	if (parent_len) {
		if (strncmp(path, parent, parent_len) != 0 ||
		    path[parent_len] != '/')
			return false;
		child = path + parent_len + 1;
	} else {
		child = path;
	}
	if (!*child)
		return false;
	slash = strchr(child, '/');
	if (slash)
		return false;
	*name = child;
	*name_len = strlen(child);
	return true;
}

static bool cxnfs_blockfs_find_extent(u64 mount_id, u64 inode_number,
				      u64 file_offset,
				      struct cxnfs_blockfs_ioc_extent *out,
				      bool *hash_verified,
				      u64 *extent_offset, size_t *available)
{
	struct cxnfs_blockfs_extent_entry *entry;
	bool found = false;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_extent_index, node) {
		u64 extent_end;

		if (entry->extent.mount_id != mount_id)
			continue;
		if (entry->extent.inode_number != inode_number)
			continue;
		if (!cxnfs_blockfs_range_end(entry->extent.file_offset,
					     entry->extent.payload_length,
					     &extent_end))
			continue;
		if (file_offset < entry->extent.file_offset ||
		    file_offset >= extent_end)
			continue;
		*out = entry->extent;
		*hash_verified = entry->hash_verified;
		*extent_offset = file_offset - entry->extent.file_offset;
		*available = min_t(u64, extent_end - file_offset, SIZE_MAX);
		found = true;
		break;
	}
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	return found;
}

static void cxnfs_blockfs_mark_extent_hash_verified(
	const struct cxnfs_blockfs_ioc_extent *extent)
{
	struct cxnfs_blockfs_extent_entry *entry;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_extent_index, node) {
		if (!cxnfs_blockfs_same_extent(&entry->extent, extent))
			continue;
		entry->hash_verified = true;
		break;
	}
	mutex_unlock(&cxnfs_blockfs_extent_lock);
}

static void cxnfs_blockfs_remove_failed_extent(
	const struct cxnfs_blockfs_ioc_extent *extent)
{
	struct cxnfs_blockfs_extent_entry *entry;
	struct cxnfs_blockfs_extent_entry *tmp;
	bool removed = false;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry_safe(entry, tmp, &cxnfs_blockfs_extent_index, node) {
		if (!cxnfs_blockfs_same_extent(&entry->extent, extent))
			continue;
		cxnfs_blockfs_remove_extent_entry(entry);
		removed = true;
	}
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	if (removed) {
		atomic64_inc(&cxnfs_blockfs_invalidations);
		cxnfs_blockfs_wake_extent_waiters();
	}
}

static bool cxnfs_blockfs_has_extent(u64 mount_id, u64 inode_number,
				     u64 file_offset)
{
	struct cxnfs_blockfs_extent_entry *entry;
	bool found = false;

	mutex_lock(&cxnfs_blockfs_extent_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_extent_index, node) {
		u64 extent_end;

		if (entry->extent.mount_id != mount_id)
			continue;
		if (entry->extent.inode_number != inode_number)
			continue;
		if (!cxnfs_blockfs_range_end(entry->extent.file_offset,
					     entry->extent.payload_length,
					     &extent_end))
			continue;
		if (file_offset >= entry->extent.file_offset &&
		    file_offset < extent_end) {
			found = true;
			break;
		}
	}
	mutex_unlock(&cxnfs_blockfs_extent_lock);
	return found;
}

static bool cxnfs_blockfs_find_missing_range(u64 mount_id, u64 inode_number,
					     u64 start, u64 length,
					     u64 *missing_start,
					     u64 *missing_length)
{
	u64 end;
	u64 cursor = start;

	if (!length || !cxnfs_blockfs_range_end(start, length, &end))
		return false;

	while (cursor < end) {
		struct cxnfs_blockfs_extent_entry *entry;
		bool covered = false;
		u64 covered_end = cursor;
		u64 next_extent_start = end;

		mutex_lock(&cxnfs_blockfs_extent_lock);
		list_for_each_entry(entry, &cxnfs_blockfs_extent_index, node) {
			u64 extent_end;

			if (entry->extent.mount_id != mount_id)
				continue;
			if (entry->extent.inode_number != inode_number)
				continue;
			if (!cxnfs_blockfs_range_end(entry->extent.file_offset,
						     entry->extent.payload_length,
						     &extent_end))
				continue;
			if (entry->extent.file_offset <= cursor &&
			    cursor < extent_end) {
				covered = true;
				covered_end = extent_end;
				break;
			}
			if (entry->extent.file_offset > cursor &&
			    entry->extent.file_offset < next_extent_start)
				next_extent_start = entry->extent.file_offset;
		}
		mutex_unlock(&cxnfs_blockfs_extent_lock);

		if (covered) {
			cursor = min(covered_end, end);
			continue;
		}

		*missing_start = cursor;
		*missing_length = next_extent_start - cursor;
		return true;
	}
	return false;
}

static int cxnfs_blockfs_wait_for_extent(struct inode *inode, u64 file_offset)
{
	struct cxnfs_blockfs_sb_info *sbi = inode->i_sb->s_fs_info;
	unsigned long timeout;
	unsigned long deadline;

	if (!sbi || !sbi->miss_wait_ms)
		return -EIO;
	timeout = msecs_to_jiffies(sbi->miss_wait_ms);
	if (!timeout)
		return -ETIMEDOUT;
	deadline = jiffies + timeout;

	for (;;) {
		u64 seen_generation;
		unsigned long remaining;
		long waited;

		if (cxnfs_blockfs_has_extent(sbi->mount_id, inode->i_ino,
					     file_offset)) {
			atomic64_inc(&cxnfs_blockfs_miss_wait_successes);
			return 0;
		}
		if (time_after_eq(jiffies, deadline)) {
			atomic64_inc(&cxnfs_blockfs_miss_wait_timeouts);
			return -ETIMEDOUT;
		}
		seen_generation =
			(u64)atomic64_read(&cxnfs_blockfs_extent_generation);
		if (cxnfs_blockfs_has_extent(sbi->mount_id, inode->i_ino,
					     file_offset)) {
			atomic64_inc(&cxnfs_blockfs_miss_wait_successes);
			return 0;
		}
		remaining = deadline - jiffies;
		waited = wait_event_interruptible_timeout(
			cxnfs_blockfs_extent_wait,
			(u64)atomic64_read(&cxnfs_blockfs_extent_generation) !=
				seen_generation,
			remaining);
		if (waited < 0) {
			atomic64_inc(&cxnfs_blockfs_miss_wait_interruptions);
			return (int)waited;
		}
		if (waited == 0) {
			if (cxnfs_blockfs_has_extent(sbi->mount_id,
						     inode->i_ino,
						     file_offset)) {
				atomic64_inc(&cxnfs_blockfs_miss_wait_successes);
				return 0;
			}
			atomic64_inc(&cxnfs_blockfs_miss_wait_timeouts);
			return -ETIMEDOUT;
		}
	}
}

static int cxnfs_blockfs_copy_payload(struct super_block *sb,
				      struct folio *folio,
				      size_t folio_offset,
				      const struct cxnfs_blockfs_ioc_extent *extent,
				      u64 extent_offset,
				      size_t bytes)
{
	while (bytes) {
		u64 payload_byte;
		u64 block_number;
		u32 block_offset;
		size_t chunk_bytes;
		struct buffer_head *bh;
		void *dst;

		if (extent->payload_block > U64_MAX / CXNFS_DISK_BLOCK_SIZE)
			return -EIO;
		payload_byte = extent->payload_block * CXNFS_DISK_BLOCK_SIZE +
			       extent->payload_offset + extent_offset;
		block_number = div_u64_rem(payload_byte, CXNFS_DISK_BLOCK_SIZE,
					   &block_offset);
		chunk_bytes = min_t(size_t, bytes,
				    CXNFS_DISK_BLOCK_SIZE - block_offset);

		bh = sb_bread(sb, block_number);
		if (!bh)
			return -EIO;
		dst = kmap_local_folio(folio, folio_offset);
		memcpy(dst, bh->b_data + block_offset, chunk_bytes);
		kunmap_local(dst);
		brelse(bh);

		bytes -= chunk_bytes;
		folio_offset += chunk_bytes;
		extent_offset += chunk_bytes;
	}
	return 0;
}

static int cxnfs_blockfs_hash_extent(struct super_block *sb,
				     const struct cxnfs_blockfs_ioc_extent *extent,
				     u8 out[SHA256_DIGEST_SIZE])
{
	static const u8 chunk_domain[] = "cxnfs.chunk.v1";
	static const u8 zero = 0;
	struct crypto_shash *tfm;
	struct shash_desc *desc;
	u64 extent_offset = 0;
	u64 bytes = extent->payload_length;
	size_t desc_size;
	int ret;

	tfm = crypto_alloc_shash("sha256", 0, 0);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);
	desc_size = sizeof(*desc) + crypto_shash_descsize(tfm);
	desc = kzalloc(desc_size, GFP_KERNEL);
	if (!desc) {
		crypto_free_shash(tfm);
		return -ENOMEM;
	}

	desc->tfm = tfm;
	ret = crypto_shash_init(desc);
	if (ret)
		goto out;
	ret = crypto_shash_update(desc, chunk_domain, sizeof(chunk_domain) - 1);
	if (ret)
		goto out;
	ret = crypto_shash_update(desc, &zero, 1);
	if (ret)
		goto out;

	while (bytes) {
		u64 payload_byte;
		u64 block_number;
		u32 block_offset;
		size_t chunk_bytes;
		struct buffer_head *bh;

		if (extent->payload_block > U64_MAX / CXNFS_DISK_BLOCK_SIZE) {
			ret = -EIO;
			goto out;
		}
		payload_byte = extent->payload_block * CXNFS_DISK_BLOCK_SIZE +
			       extent->payload_offset + extent_offset;
		block_number = div_u64_rem(payload_byte, CXNFS_DISK_BLOCK_SIZE,
					   &block_offset);
		chunk_bytes = min_t(u64, bytes,
				    CXNFS_DISK_BLOCK_SIZE - block_offset);

		bh = sb_bread(sb, block_number);
		if (!bh) {
			ret = -EIO;
			goto out;
		}
		ret = crypto_shash_update(desc, bh->b_data + block_offset,
					  chunk_bytes);
		brelse(bh);
		if (ret)
			goto out;

		bytes -= chunk_bytes;
		extent_offset += chunk_bytes;
	}

	ret = crypto_shash_final(desc, out);

out:
	kfree(desc);
	crypto_free_shash(tfm);
	return ret;
}

static int cxnfs_blockfs_verify_extent_hash(
	struct super_block *sb,
	const struct cxnfs_blockfs_ioc_extent *extent,
	bool hash_verified)
{
	struct cxnfs_blockfs_sb_info *sbi = sb->s_fs_info;
	u8 actual[SHA256_DIGEST_SIZE];
	int ret;

	if (!sbi || !sbi->verify_hashes || hash_verified)
		return 0;

	ret = cxnfs_blockfs_hash_extent(sb, extent, actual);
	if (ret)
		return ret;
	atomic64_inc(&cxnfs_blockfs_hash_verifications);
	atomic64_add(extent->payload_length,
		     &cxnfs_blockfs_hash_bytes_verified);
	if (memcmp(actual, extent->expected_hash, sizeof(actual)) != 0) {
		atomic64_inc(&cxnfs_blockfs_hash_mismatches);
		cxnfs_blockfs_remove_failed_extent(extent);
		return -EILSEQ;
	}
	cxnfs_blockfs_mark_extent_hash_verified(extent);
	return 0;
}

static int cxnfs_blockfs_fill_verified_folio(struct inode *inode,
					     struct folio *folio,
					     u32 miss_reason,
					     bool record_missing_extent)
{
	struct super_block *sb = folio->mapping->host->i_sb;
	struct cxnfs_blockfs_sb_info *sbi = sb->s_fs_info;
	u64 file_offset = folio_pos(folio);
	size_t folio_bytes = folio_size(folio);
	loff_t file_size = i_size_read(inode);
	size_t bytes_to_fill;
	size_t filled = 0;

	if (file_offset >= (u64)file_size) {
		folio_zero_segment(folio, 0, folio_bytes);
		folio_mark_uptodate(folio);
		atomic64_inc(&sbi->verified_page_fills);
		atomic64_inc(&cxnfs_blockfs_verified_page_fills);
		return 0;
	}

	bytes_to_fill = min_t(u64, folio_bytes, (u64)file_size - file_offset);
	while (filled < bytes_to_fill) {
		struct cxnfs_blockfs_ioc_extent extent;
		bool hash_verified;
		u64 extent_offset;
		size_t available;
		size_t copy_bytes;
		int ret;

		if (!cxnfs_blockfs_find_extent(sbi->mount_id, inode->i_ino,
					       file_offset + filled, &extent,
					       &hash_verified, &extent_offset,
					       &available)) {
			int wait_ret;

			if (record_missing_extent)
				cxnfs_blockfs_record_miss_event(
					inode, file_offset + filled,
					bytes_to_fill - filled, miss_reason);
			if (miss_reason != CXNFS_BLOCKFS_MISS_REASON_READ)
				return -EIO;
			wait_ret = cxnfs_blockfs_wait_for_extent(
				inode, file_offset + filled);
			if (wait_ret == 0)
				continue;
			return wait_ret;
		}
		ret = cxnfs_blockfs_verify_extent_hash(sb, &extent,
						       hash_verified);
		if (ret) {
			cxnfs_blockfs_record_miss_event(
				inode, file_offset + filled,
				bytes_to_fill - filled,
				ret == -EILSEQ ?
					CXNFS_BLOCKFS_MISS_REASON_HASH_MISMATCH :
					CXNFS_BLOCKFS_MISS_REASON_PAYLOAD_IO);
			return ret;
		}
		copy_bytes = min(bytes_to_fill - filled, available);
		ret = cxnfs_blockfs_copy_payload(sb, folio, filled, &extent,
						 extent_offset, copy_bytes);
		if (ret) {
			cxnfs_blockfs_record_miss_event(
				inode, file_offset + filled, copy_bytes,
				CXNFS_BLOCKFS_MISS_REASON_PAYLOAD_IO);
			return ret;
		}
		filled += copy_bytes;
	}
	if (bytes_to_fill < folio_bytes)
		folio_zero_segment(folio, bytes_to_fill, folio_bytes);
	folio_mark_uptodate(folio);
	atomic64_inc(&sbi->verified_page_fills);
	atomic64_inc(&cxnfs_blockfs_verified_page_fills);
	return 0;
}

static int cxnfs_blockfs_read_folio(struct file *file, struct folio *folio)
{
	struct inode *inode = folio->mapping->host;
	struct cxnfs_blockfs_sb_info *sbi = inode->i_sb->s_fs_info;
	int ret;

	atomic64_inc(&sbi->read_folio_calls);
	atomic64_inc(&cxnfs_blockfs_read_folio_calls);
	ret = cxnfs_blockfs_fill_verified_folio(
		inode, folio, CXNFS_BLOCKFS_MISS_REASON_READ, true);
	folio_unlock(folio);
	return ret;
}

static void cxnfs_blockfs_record_readahead_window(struct inode *inode,
						  u64 offset, u64 length)
{
	if (!length)
		return;
	cxnfs_blockfs_record_miss_event(
		inode, offset, length, CXNFS_BLOCKFS_MISS_REASON_READAHEAD);
	atomic64_inc(&cxnfs_blockfs_readahead_miss_windows);
	atomic64_add(length, &cxnfs_blockfs_readahead_miss_bytes);
}

static void cxnfs_blockfs_extend_readahead_window(struct inode *inode,
						  u64 *window_start,
						  u64 *window_length,
						  bool *has_window,
						  u64 offset,
						  u64 length,
						  u64 max_window_bytes)
{
	u64 window_end;
	u64 range_end;

	if (!length)
		return;
	if (!*has_window) {
		*window_start = offset;
		*window_length = length;
		*has_window = true;
		return;
	}
	if (!cxnfs_blockfs_range_end(*window_start, *window_length,
				     &window_end) ||
	    !cxnfs_blockfs_range_end(offset, length, &range_end) ||
	    offset != window_end ||
	    *window_length >= max_window_bytes ||
	    length > max_window_bytes - *window_length) {
		cxnfs_blockfs_record_readahead_window(inode, *window_start,
						      *window_length);
		*window_start = offset;
		*window_length = length;
		return;
	}
	*window_length += length;
}

static void cxnfs_blockfs_readahead(struct readahead_control *rac)
{
	struct super_block *sb = rac->mapping->host->i_sb;
	struct cxnfs_blockfs_sb_info *sbi = sb->s_fs_info;
	struct inode *inode = rac->mapping->host;
	struct folio *folio;
	loff_t file_size = i_size_read(inode);
	u64 window_start = 0;
	u64 window_length = 0;
	u64 max_window_bytes = (u64)max_t(u32, sbi->readahead_pages, 1) *
			       PAGE_SIZE;
	bool has_window = false;

	atomic64_inc(&sbi->readahead_calls);
	atomic64_inc(&cxnfs_blockfs_readahead_calls);
	while ((folio = readahead_folio(rac)) != NULL) {
		u64 file_offset = folio_pos(folio);
		size_t folio_bytes = folio_size(folio);
		size_t bytes_to_check = 0;
		u64 missing_start;
		u64 missing_length;

		if (file_offset < (u64)file_size)
			bytes_to_check = min_t(u64, folio_bytes,
					       (u64)file_size - file_offset);
		if (bytes_to_check &&
		    cxnfs_blockfs_find_missing_range(sbi->mount_id,
						     inode->i_ino, file_offset,
						     bytes_to_check,
						     &missing_start,
						     &missing_length)) {
			cxnfs_blockfs_extend_readahead_window(
				inode, &window_start, &window_length, &has_window,
				missing_start, missing_length, max_window_bytes);
		}
		cxnfs_blockfs_fill_verified_folio(
			inode, folio, CXNFS_BLOCKFS_MISS_REASON_READAHEAD,
			false);
		folio_unlock(folio);
	}
	if (has_window)
		cxnfs_blockfs_record_readahead_window(inode, window_start,
						      window_length);
}

static const struct address_space_operations cxnfs_blockfs_aops = {
	.read_folio = cxnfs_blockfs_read_folio,
	.readahead = cxnfs_blockfs_readahead,
};

static const struct file_operations cxnfs_blockfs_file_ops = {
	.llseek = generic_file_llseek,
	.read_iter = generic_file_read_iter,
	.mmap = generic_file_mmap,
};

static struct dentry *cxnfs_blockfs_lookup(struct inode *dir,
					   struct dentry *dentry,
					   unsigned int flags)
{
	struct cxnfs_blockfs_sb_info *sbi = dir->i_sb->s_fs_info;
	struct cxnfs_blockfs_file_entry entry;
	char dir_path[CXNFS_BLOCKFS_PATH_MAX];
	char child_path[CXNFS_BLOCKFS_PATH_MAX];
	struct inode *inode = NULL;

	if (!cxnfs_blockfs_copy_dir_path(dir, dir_path))
		return ERR_PTR(-ENOENT);
	if (!cxnfs_blockfs_build_child_path(dir_path, &dentry->d_name,
					    child_path))
		return ERR_PTR(-ENAMETOOLONG);
	if (cxnfs_blockfs_copy_entry_by_path(sbi->mount_id, child_path,
					     &entry)) {
		inode = cxnfs_blockfs_get_inode(dir->i_sb, entry.inode_number,
						entry.mode, entry.size);
		if (!inode)
			return ERR_PTR(-ENOMEM);
	}
	d_add(dentry, inode);
	return NULL;
}

static int cxnfs_blockfs_iterate_shared(struct file *file,
					struct dir_context *ctx)
{
	struct cxnfs_blockfs_sb_info *sbi = file_inode(file)->i_sb->s_fs_info;
	struct cxnfs_blockfs_file_entry *entry;
	char dir_path[CXNFS_BLOCKFS_PATH_MAX];
	loff_t index = 0;

	if (ctx->pos < 2 && !dir_emit_dots(file, ctx))
		return 0;
	if (!cxnfs_blockfs_copy_dir_path(file_inode(file), dir_path))
		return -ENOENT;

	mutex_lock(&cxnfs_blockfs_file_lock);
	list_for_each_entry(entry, &cxnfs_blockfs_file_index, node) {
		const char *name;
		size_t name_len;
		unsigned int type = S_ISDIR(entry->mode) ? DT_DIR : DT_REG;

		if (entry->mount_id != sbi->mount_id)
			continue;
		if (!cxnfs_blockfs_immediate_child(dir_path, entry->path, &name,
						   &name_len))
			continue;
		if (index++ < ctx->pos - 2)
			continue;
		if (!dir_emit(ctx, name, name_len, entry->inode_number, type))
			break;
		ctx->pos++;
	}
	mutex_unlock(&cxnfs_blockfs_file_lock);
	return 0;
}

static const struct inode_operations cxnfs_blockfs_dir_inode_ops = {
	.lookup = cxnfs_blockfs_lookup,
};

static const struct file_operations cxnfs_blockfs_dir_ops = {
	.owner = THIS_MODULE,
	.iterate_shared = cxnfs_blockfs_iterate_shared,
	.llseek = generic_file_llseek,
};

static const struct super_operations cxnfs_blockfs_super_ops = {
	.statfs = simple_statfs,
};

static struct inode *cxnfs_blockfs_get_inode(struct super_block *sb,
					     unsigned long inode_number,
					     umode_t mode, loff_t size)
{
	struct inode *inode;
	bool new_hashed_inode = false;

	if (inode_number) {
		inode = iget_locked(sb, inode_number);
		if (!inode)
			return NULL;
		if (!cxnfs_blockfs_inode_is_new(inode)) {
			i_size_write(inode, size);
			return inode;
		}
		new_hashed_inode = true;
	} else {
		inode = new_inode(sb);
		if (!inode)
			return NULL;
	}

	inode->i_ino = inode_number ? inode_number : get_next_ino();
	inode->i_mode = mode;
	inode->i_uid = current_fsuid();
	inode->i_gid = current_fsgid();
	i_size_write(inode, size);

	switch (mode & S_IFMT) {
	case S_IFDIR:
		inode->i_op = &cxnfs_blockfs_dir_inode_ops;
		inode->i_fop = &cxnfs_blockfs_dir_ops;
		inc_nlink(inode);
		break;
	case S_IFREG:
		inode->i_mapping->a_ops = &cxnfs_blockfs_aops;
		inode->i_fop = &cxnfs_blockfs_file_ops;
		break;
	default:
		break;
	}

	if (new_hashed_inode)
		unlock_new_inode(inode);
	return inode;
}

static int cxnfs_blockfs_validate_super(struct cxnfs_disk_super *disk,
					struct super_block *sb)
{
	u32 block_size = le32_to_cpu(disk->block_size);
	u32 version = le32_to_cpu(disk->version);

	if (memcmp(disk->magic, CXNFS_DISK_MAGIC, CXNFS_DISK_MAGIC_LEN) != 0)
		return -EINVAL;
	if (version != CXNFS_DISK_VERSION)
		return -EINVAL;
	if (block_size != CXNFS_DISK_BLOCK_SIZE)
		return -EINVAL;
	if (le64_to_cpu(disk->total_blocks) == 0)
		return -EINVAL;
	if (!(le64_to_cpu(disk->flags) & CXNFS_DISK_FLAG_OS_STRUCTURES_REQUIRED))
		return -EINVAL;
	if (!sb_set_blocksize(sb, block_size))
		return -EINVAL;

	return 0;
}

static void cxnfs_blockfs_load_sbi(struct cxnfs_blockfs_sb_info *sbi,
				   const struct cxnfs_disk_super *disk,
				   const struct cxnfs_blockfs_mount_options *opts)
{
	u32 max_cords = le32_to_cpu(disk->max_cords);
	u32 readahead_pages = le32_to_cpu(disk->readahead_pages);

	sbi->mount_id = (u64)atomic64_inc_return(&cxnfs_blockfs_next_mount_id);
	sbi->block_size = le32_to_cpu(disk->block_size);
	sbi->flags = le64_to_cpu(disk->flags);
	sbi->metadata_root = kstrdup(opts->metadata_root, GFP_KERNEL);
	atomic64_set(&sbi->read_folio_calls, 0);
	atomic64_set(&sbi->readahead_calls, 0);
	atomic64_set(&sbi->registered_extents, 0);
	atomic64_set(&sbi->invalidations, 0);
	atomic64_set(&sbi->verified_page_fills, 0);
	sbi->max_cords = max_cords ? min(max_cords, (u32)CXNFS_BLOCKFS_MAX_CORDS)
				   : CXNFS_BLOCKFS_MAX_CORDS;
	sbi->readahead_pages = readahead_pages ? readahead_pages
					       : CXNFS_BLOCKFS_DEFAULT_READAHEAD_PAGES;
	sbi->miss_wait_ms = CXNFS_BLOCKFS_DEFAULT_MISS_WAIT_MS;
	sbi->verify_hashes = true;
	if (opts->max_cords)
		sbi->max_cords = min(opts->max_cords, (u32)CXNFS_BLOCKFS_MAX_CORDS);
	if (opts->readahead_pages)
		sbi->readahead_pages = opts->readahead_pages;
	if (opts->miss_wait_ms_set)
		sbi->miss_wait_ms = opts->miss_wait_ms;
	if (opts->verify_hashes_set)
		sbi->verify_hashes = opts->verify_hashes;
	sbi->manifest_index_start_block =
		le64_to_cpu(disk->manifest_index_start_block);
	sbi->manifest_index_block_count =
		le64_to_cpu(disk->manifest_index_block_count);
	sbi->cache_index_start_block =
		le64_to_cpu(disk->cache_index_start_block);
	sbi->cache_index_block_count =
		le64_to_cpu(disk->cache_index_block_count);
	sbi->container_cache_start_block =
		le64_to_cpu(disk->container_cache_start_block);
	sbi->container_cache_block_count =
		le64_to_cpu(disk->container_cache_block_count);
	sbi->free_map_start_block = le64_to_cpu(disk->free_map_start_block);
	sbi->free_map_block_count = le64_to_cpu(disk->free_map_block_count);
}

static int cxnfs_blockfs_parse_u32_option(const char *value, u32 *out)
{
	int ret;

	ret = kstrtou32(value, 10, out);
	if (ret)
		return ret;
	if (*out == 0)
		return -EINVAL;
	return 0;
}

static int cxnfs_blockfs_parse_u32_option_allow_zero(const char *value,
						     u32 *out)
{
	return kstrtou32(value, 10, out);
}

static int cxnfs_blockfs_parse_bool_option(const char *value, bool *out)
{
	if (strcmp(value, "1") == 0 || strcmp(value, "true") == 0 ||
	    strcmp(value, "yes") == 0 || strcmp(value, "on") == 0) {
		*out = true;
		return 0;
	}
	if (strcmp(value, "0") == 0 || strcmp(value, "false") == 0 ||
	    strcmp(value, "no") == 0 || strcmp(value, "off") == 0) {
		*out = false;
		return 0;
	}
	return -EINVAL;
}

static int cxnfs_blockfs_parse_options(const char *data,
				       struct cxnfs_blockfs_mount_options *opts)
{
	char *raw;
	char *cursor;
	char *token;
	int ret = 0;

	memset(opts, 0, sizeof(*opts));
	if (!data)
		return -EINVAL;

	raw = kstrdup(data, GFP_KERNEL);
	if (!raw)
		return -ENOMEM;

	cursor = raw;
	while ((token = strsep(&cursor, ",")) != NULL) {
		if (!*token)
			continue;
		if (strncmp(token, "metadata_root=", 14) == 0) {
			const char *value = token + 14;

			if (!*value || strlen(value) >= sizeof(opts->metadata_root)) {
				ret = -EINVAL;
				break;
			}
			strscpy(opts->metadata_root, value,
				sizeof(opts->metadata_root));
		} else if (strncmp(token, "max_cords=", 10) == 0) {
			ret = cxnfs_blockfs_parse_u32_option(token + 10,
							     &opts->max_cords);
			if (ret)
				break;
			if (opts->max_cords > CXNFS_BLOCKFS_MAX_CORDS) {
				ret = -EINVAL;
				break;
			}
		} else if (strncmp(token, "readahead_pages=", 16) == 0) {
			ret = cxnfs_blockfs_parse_u32_option(token + 16,
							     &opts->readahead_pages);
			if (ret)
				break;
		} else if (strncmp(token, "miss_wait_ms=", 13) == 0) {
			ret = cxnfs_blockfs_parse_u32_option_allow_zero(
				token + 13, &opts->miss_wait_ms);
			if (ret)
				break;
			opts->miss_wait_ms_set = true;
		} else if (strncmp(token, "verify_hashes=", 14) == 0) {
			ret = cxnfs_blockfs_parse_bool_option(
				token + 14, &opts->verify_hashes);
			if (ret)
				break;
			opts->verify_hashes_set = true;
		} else {
			ret = -EINVAL;
			break;
		}
	}

	if (!ret && !opts->metadata_root[0])
		ret = -EINVAL;

	kfree(raw);
	return ret;
}

static int cxnfs_blockfs_fill_super(struct super_block *sb,
				    struct fs_context *fc)
{
	struct cxnfs_blockfs_mount_options *opts = fc->fs_private;
	struct buffer_head *bh = NULL;
	struct cxnfs_blockfs_sb_info *sbi = NULL;
	struct cxnfs_disk_super *disk;
	struct inode *root_inode;
	int ret;

	BUILD_BUG_ON(sizeof(struct cxnfs_disk_super) != CXNFS_DISK_BLOCK_SIZE);

	if (!opts || !opts->metadata_root[0])
		return -EINVAL;

	if (!sb_set_blocksize(sb, CXNFS_DISK_BLOCK_SIZE))
		return -EINVAL;

	bh = sb_bread(sb, 0);
	if (!bh)
		return -EIO;

	disk = (struct cxnfs_disk_super *)bh->b_data;
	ret = cxnfs_blockfs_validate_super(disk, sb);
	if (ret)
		goto out_brelse;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi) {
		ret = -ENOMEM;
		goto out_brelse;
	}

	cxnfs_blockfs_load_sbi(sbi, disk, opts);
	if (!sbi->metadata_root) {
		ret = -ENOMEM;
		goto out_free_sbi;
	}

	sb->s_magic = CXNFS_BLOCKFS_SUPER_MAGIC;
	sb->s_op = &cxnfs_blockfs_super_ops;
	sb->s_fs_info = sbi;

	root_inode = cxnfs_blockfs_get_inode(sb, le64_to_cpu(disk->root_inode),
					     S_IFDIR | 0755, 0);
	if (!root_inode) {
		ret = -ENOMEM;
		goto out_free_sbi;
	}

	sb->s_root = d_make_root(root_inode);
	if (!sb->s_root) {
		ret = -ENOMEM;
		goto out_free_sbi;
	}

	cxnfs_blockfs_register_mount(sb, sbi);
	brelse(bh);
	return 0;

out_free_sbi:
	kfree(sbi->metadata_root);
	kfree(sbi);
	sb->s_fs_info = NULL;
out_brelse:
	brelse(bh);
	return ret;
}

static int cxnfs_blockfs_parse_monolithic(struct fs_context *fc, void *data)
{
	return cxnfs_blockfs_parse_options((const char *)data, fc->fs_private);
}

static int cxnfs_blockfs_get_tree(struct fs_context *fc)
{
	return get_tree_bdev(fc, cxnfs_blockfs_fill_super);
}

static void cxnfs_blockfs_free_context(struct fs_context *fc)
{
	kfree(fc->fs_private);
}

static const struct fs_context_operations cxnfs_blockfs_context_ops = {
	.free = cxnfs_blockfs_free_context,
	.parse_monolithic = cxnfs_blockfs_parse_monolithic,
	.get_tree = cxnfs_blockfs_get_tree,
};

static int cxnfs_blockfs_init_fs_context(struct fs_context *fc)
{
	struct cxnfs_blockfs_mount_options *opts;

	opts = kzalloc(sizeof(*opts), GFP_KERNEL);
	if (!opts)
		return -ENOMEM;

	fc->fs_private = opts;
	fc->ops = &cxnfs_blockfs_context_ops;
	return 0;
}

static void cxnfs_blockfs_kill_sb(struct super_block *sb)
{
	struct cxnfs_blockfs_sb_info *sbi = sb->s_fs_info;

	if (sbi) {
		cxnfs_blockfs_clear_mount_namespace(sbi->mount_id);
		cxnfs_blockfs_unregister_mount(sbi);
	}
	kill_block_super(sb);
	if (sbi) {
		kfree(sbi->metadata_root);
		kfree(sbi);
	}
}

static int cxnfs_blockfs_check_abi(u32 abi_version)
{
	return abi_version == CXNFS_BLOCKFS_IOC_ABI_VERSION ? 0 : -EINVAL;
}

static long cxnfs_blockfs_ctl_ioctl(struct file *file, unsigned int cmd,
				    unsigned long arg)
{
	switch (cmd) {
	case CXNFS_BLOCKFS_IOC_REGISTER_EXTENT: {
		struct cxnfs_blockfs_ioc_extent *extent;
		int ret;

		extent = memdup_user((void __user *)arg, sizeof(*extent));
		if (IS_ERR(extent))
			return PTR_ERR(extent);
		extent->metadata_root[CXNFS_BLOCKFS_PATH_MAX - 1] = '\0';
		ret = cxnfs_blockfs_validate_extent(extent);
		if (!ret)
			ret = cxnfs_blockfs_register_extent(extent);
		kfree(extent);
		return ret;
	}
	case CXNFS_BLOCKFS_IOC_INVALIDATE: {
		struct cxnfs_blockfs_ioc_invalidate *invalidate;
		int ret;

		invalidate = memdup_user((void __user *)arg, sizeof(*invalidate));
		if (IS_ERR(invalidate))
			return PTR_ERR(invalidate);
		invalidate->metadata_root[CXNFS_BLOCKFS_PATH_MAX - 1] = '\0';
		ret = cxnfs_blockfs_check_abi(invalidate->abi_version);
		if (!ret)
			ret = cxnfs_blockfs_invalidate_extents(invalidate);
		kfree(invalidate);
		return ret;
	}
	case CXNFS_BLOCKFS_IOC_STATS: {
		struct cxnfs_blockfs_ioc_stats stats = {
			.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
			.registered_extents =
				atomic64_read(&cxnfs_blockfs_registered_extents),
			.invalidations =
				atomic64_read(&cxnfs_blockfs_invalidations),
			.read_folio_calls =
				atomic64_read(&cxnfs_blockfs_read_folio_calls),
			.readahead_calls =
				atomic64_read(&cxnfs_blockfs_readahead_calls),
			.verified_page_fills =
				atomic64_read(&cxnfs_blockfs_verified_page_fills),
		};

		if (copy_to_user((void __user *)arg, &stats, sizeof(stats)))
			return -EFAULT;
		return 0;
	}
	case CXNFS_BLOCKFS_IOC_STATS_V2: {
		struct cxnfs_blockfs_ioc_stats_v2 stats = {
			.abi_version = CXNFS_BLOCKFS_IOC_ABI_VERSION,
			.registered_extents =
				atomic64_read(&cxnfs_blockfs_registered_extents),
			.invalidations =
				atomic64_read(&cxnfs_blockfs_invalidations),
			.read_folio_calls =
				atomic64_read(&cxnfs_blockfs_read_folio_calls),
			.readahead_calls =
				atomic64_read(&cxnfs_blockfs_readahead_calls),
			.verified_page_fills =
				atomic64_read(&cxnfs_blockfs_verified_page_fills),
			.miss_events_queued =
				atomic64_read(&cxnfs_blockfs_miss_events_queued),
			.miss_events_drained =
				atomic64_read(&cxnfs_blockfs_miss_events_drained),
			.miss_events_pending =
				(u64)READ_ONCE(cxnfs_blockfs_miss_event_count),
			.miss_events_dropped =
				atomic64_read(&cxnfs_blockfs_dropped_miss_events),
			.miss_wait_successes =
				atomic64_read(&cxnfs_blockfs_miss_wait_successes),
			.miss_wait_timeouts =
				atomic64_read(&cxnfs_blockfs_miss_wait_timeouts),
			.miss_wait_interruptions =
				atomic64_read(&cxnfs_blockfs_miss_wait_interruptions),
			.extent_generation =
				atomic64_read(&cxnfs_blockfs_extent_generation),
			.hash_verifications =
				atomic64_read(&cxnfs_blockfs_hash_verifications),
			.hash_mismatches =
				atomic64_read(&cxnfs_blockfs_hash_mismatches),
			.hash_bytes_verified =
				atomic64_read(&cxnfs_blockfs_hash_bytes_verified),
			.readahead_miss_windows =
				atomic64_read(&cxnfs_blockfs_readahead_miss_windows),
			.readahead_miss_bytes =
				atomic64_read(&cxnfs_blockfs_readahead_miss_bytes),
		};

		if (copy_to_user((void __user *)arg, &stats, sizeof(stats)))
			return -EFAULT;
		return 0;
	}
	case CXNFS_BLOCKFS_IOC_REGISTER_FILE: {
		struct cxnfs_blockfs_ioc_file *file_record;
		int ret;

		file_record = memdup_user((void __user *)arg,
					  sizeof(*file_record));
		if (IS_ERR(file_record))
			return PTR_ERR(file_record);
		file_record->metadata_root[CXNFS_BLOCKFS_PATH_MAX - 1] = '\0';
		file_record->path[CXNFS_BLOCKFS_PATH_MAX - 1] = '\0';
		ret = cxnfs_blockfs_register_file(file_record);
		kfree(file_record);
		return ret;
	}
	case CXNFS_BLOCKFS_IOC_CLEAR_NAMESPACE:
		cxnfs_blockfs_clear_live_namespace();
		return 0;
	case CXNFS_BLOCKFS_IOC_CLEAR_MOUNT_NAMESPACE: {
		struct cxnfs_blockfs_ioc_clear_namespace *clear;
		u64 mount_id;
		int ret;

		clear = memdup_user((void __user *)arg, sizeof(*clear));
		if (IS_ERR(clear))
			return PTR_ERR(clear);
		clear->metadata_root[CXNFS_BLOCKFS_PATH_MAX - 1] = '\0';
		ret = cxnfs_blockfs_check_abi(clear->abi_version);
		if (ret) {
			kfree(clear);
			return ret;
		}
		ret = cxnfs_blockfs_resolve_mount_selector(clear->mount_id,
							   clear->metadata_root,
							   &mount_id);
		if (ret) {
			kfree(clear);
			return ret;
		}
		kfree(clear);
		cxnfs_blockfs_clear_mount_namespace(mount_id);
		return 0;
	}
	case CXNFS_BLOCKFS_IOC_NEXT_MISS_EVENT: {
		struct cxnfs_blockfs_ioc_miss_event *event;
		int ret;

		event = memdup_user((void __user *)arg, sizeof(*event));
		if (IS_ERR(event))
			return PTR_ERR(event);
		event->metadata_root[CXNFS_BLOCKFS_PATH_MAX - 1] = '\0';
		ret = cxnfs_blockfs_next_miss_event(event);
		if (ret) {
			kfree(event);
			return ret;
		}
		if (copy_to_user((void __user *)arg, event, sizeof(*event))) {
			kfree(event);
			return -EFAULT;
		}
		kfree(event);
		return 0;
	}
	default:
		return -ENOTTY;
	}
}

static const struct file_operations cxnfs_blockfs_ctl_fops = {
	.owner = THIS_MODULE,
	.unlocked_ioctl = cxnfs_blockfs_ctl_ioctl,
	.llseek = noop_llseek,
};

static struct miscdevice cxnfs_blockfs_miscdev = {
	.minor = MISC_DYNAMIC_MINOR,
	.name = "cxnfs_blockfs_ctl",
	.fops = &cxnfs_blockfs_ctl_fops,
	.mode = 0600,
};

static struct file_system_type cxnfs_blockfs_type = {
	.owner = THIS_MODULE,
	.name = CXNFS_BLOCKFS_NAME,
	.init_fs_context = cxnfs_blockfs_init_fs_context,
	.kill_sb = cxnfs_blockfs_kill_sb,
	.fs_flags = FS_REQUIRES_DEV,
};

static int __init cxnfs_blockfs_init(void)
{
	int ret;

	ret = register_filesystem(&cxnfs_blockfs_type);
	if (ret)
		return ret;

	ret = misc_register(&cxnfs_blockfs_miscdev);
	if (ret) {
		unregister_filesystem(&cxnfs_blockfs_type);
		return ret;
	}

	return 0;
}

static void __exit cxnfs_blockfs_exit(void)
{
	misc_deregister(&cxnfs_blockfs_miscdev);
	unregister_filesystem(&cxnfs_blockfs_type);
	cxnfs_blockfs_clear_extent_index();
	cxnfs_blockfs_clear_file_index();
}

module_init(cxnfs_blockfs_init);
module_exit(cxnfs_blockfs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Fortrow");
MODULE_DESCRIPTION("CXNFS Linux host block filesystem");
