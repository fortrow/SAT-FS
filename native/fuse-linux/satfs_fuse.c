#define CXNFS_FUSE_PROGRAM_NAME "satfs-fuse"
#define CXNFS_FUSE_HEADER <fuse.h>
#define CXNFS_FUSE_REQUIRE_ROOT 0
#define CXNFS_FUSE_LINUX 1
#define CXNFS_FUSE_TEMP_TEMPLATE "/tmp/satfs-fuse-write-XXXXXX"
#define CXNFS_FUSE_MOUNT_OPTIONS "allow_other,default_permissions,entry_timeout=0,attr_timeout=0,negative_timeout=0"
#define CXNFS_FUSE_FALLBACK_MOUNT_OPTIONS "default_permissions,entry_timeout=0,attr_timeout=0,negative_timeout=0"

#include "../fuse-t/cxnfs_fuse_t.c"
