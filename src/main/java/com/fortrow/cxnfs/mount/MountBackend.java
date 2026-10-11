package com.fortrow.cxnfs.mount;

public enum MountBackend {
    LINUX_FUSE,
    MACOS_FUSE_T,
    MACOS_MACFUSE,
    WINDOWS_WINFSP,
    WINDOWS_PROJFS,
    CLI_LIST_ONLY
}
