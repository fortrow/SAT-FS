CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -Werror -O2
UNAME_S := $(shell uname -s)
TOOLS := mkfs.cxnfsblk fsck.cxnfsblk
ifeq ($(UNAME_S),Linux)
TOOLS += ctl_probe cxnfs-blockfs-ctl
endif

.PHONY: all smoke clean

all: $(TOOLS)

smoke: $(TOOLS)
	sh ./tool-smoke.sh .

mkfs.cxnfsblk: mkfs.cxnfsblk.c cxnfs_tool_common.h ../cxnfs_blockfs_disk.h ../cxnfs_blockfs_meta.h
	$(CC) $(CFLAGS) -o $@ mkfs.cxnfsblk.c

fsck.cxnfsblk: fsck.cxnfsblk.c cxnfs_tool_common.h ../cxnfs_blockfs_disk.h ../cxnfs_blockfs_meta.h
	$(CC) $(CFLAGS) -o $@ fsck.cxnfsblk.c

ctl_probe: ctl_probe.c ../cxnfs_blockfs_uapi.h
	$(CC) $(CFLAGS) -o $@ ctl_probe.c

cxnfs-blockfs-ctl: cxnfs-blockfs-ctl.c ../cxnfs_blockfs_uapi.h
	$(CC) $(CFLAGS) -o $@ cxnfs-blockfs-ctl.c

clean:
	rm -f mkfs.cxnfsblk fsck.cxnfsblk ctl_probe cxnfs-blockfs-ctl
