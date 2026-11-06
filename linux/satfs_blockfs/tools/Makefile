CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -Werror -O2
UNAME_S := $(shell uname -s)
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
SBINDIR ?= /sbin

TOOLS := mkfs.satfsblk fsck.satfsblk
ifeq ($(UNAME_S),Linux)
TOOLS += ctl_probe satfs-blockfs-ctl
endif

.PHONY: all smoke install clean

all: $(TOOLS)
	ln -sf mkfs.satfsblk mkfs.cxnfsblk
	ln -sf fsck.satfsblk fsck.cxnfsblk
ifeq ($(UNAME_S),Linux)
	ln -sf satfs-blockfs-ctl cxnfs-blockfs-ctl
endif

smoke: all
	sh ./tool-smoke.sh .

mkfs.satfsblk: mkfs.satfsblk.c satfs_tool_common.h ../satfs_blockfs_disk.h ../satfs_blockfs_meta.h
	$(CC) $(CFLAGS) -o $@ mkfs.satfsblk.c

fsck.satfsblk: fsck.satfsblk.c satfs_tool_common.h ../satfs_blockfs_disk.h ../satfs_blockfs_meta.h
	$(CC) $(CFLAGS) -o $@ fsck.satfsblk.c

ctl_probe: ctl_probe.c ../satfs_blockfs_uapi.h
	$(CC) $(CFLAGS) -o $@ ctl_probe.c

satfs-blockfs-ctl: satfs-blockfs-ctl.c ../satfs_blockfs_uapi.h
	$(CC) $(CFLAGS) -o $@ satfs-blockfs-ctl.c

install: all
	install -d -m 0755 $(DESTDIR)$(SBINDIR)
	install -m 0755 mkfs.satfsblk $(DESTDIR)$(SBINDIR)/mkfs.satfsblk
	ln -sf mkfs.satfsblk $(DESTDIR)$(SBINDIR)/mkfs.sat-fs
	ln -sf mkfs.satfsblk $(DESTDIR)$(SBINDIR)/mkfs.cxnfsblk
	install -m 0755 fsck.satfsblk $(DESTDIR)$(SBINDIR)/fsck.satfsblk
	ln -sf fsck.satfsblk $(DESTDIR)$(SBINDIR)/fsck.sat-fs
	ln -sf fsck.satfsblk $(DESTDIR)$(SBINDIR)/fsck.cxnfsblk
ifeq ($(UNAME_S),Linux)
	install -d -m 0755 $(DESTDIR)$(BINDIR)
	install -m 0755 satfs-blockfs-ctl $(DESTDIR)$(BINDIR)/satfs-blockfs-ctl
	ln -sf satfs-blockfs-ctl $(DESTDIR)$(BINDIR)/cxnfs-blockfs-ctl
endif

clean:
	rm -f mkfs.satfsblk fsck.satfsblk ctl_probe satfs-blockfs-ctl
	rm -f mkfs.cxnfsblk fsck.cxnfsblk cxnfs-blockfs-ctl
