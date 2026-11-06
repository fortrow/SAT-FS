#ifndef SATFS_BLOCKFS_H
#define SATFS_BLOCKFS_H

#include "satfs_blockfs_disk.h"
#include "satfs_blockfs_meta.h"
#include "satfs_blockfs_uapi.h"

#define SATFS_BLOCKFS_NAME "satfsblk"
#define SATFS_BLOCKFS_SUPER_MAGIC 0x53415442 /* "SATB" */
#define SATFS_BLOCKFS_CTL_DEVICE_NAME "satfs_blockfs_ctl"

/* Legacy aliases */
#define CXNFS_BLOCKFS_NAME "cxnfsblk"
#define CXNFS_BLOCKFS_SUPER_MAGIC 0x43584e42 /* "CXNB" */
#define CXNFS_BLOCKFS_CTL_DEVICE_NAME "cxnfs_blockfs_ctl"

#define SATFS_BLOCKFS_MOUNT_OPT_VERIFY_HASHES (1U << 0)
#define SATFS_BLOCKFS_MOUNT_OPT_TRACE (1U << 1)
#define CXNFS_BLOCKFS_MOUNT_OPT_VERIFY_HASHES SATFS_BLOCKFS_MOUNT_OPT_VERIFY_HASHES
#define CXNFS_BLOCKFS_MOUNT_OPT_TRACE SATFS_BLOCKFS_MOUNT_OPT_TRACE

#endif /* SATFS_BLOCKFS_H */
