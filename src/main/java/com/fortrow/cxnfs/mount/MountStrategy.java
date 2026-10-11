package com.fortrow.cxnfs.mount;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public record MountStrategy(
        MountBackend backend,
        Set<MountCapability> capabilities
) {
    public static MountStrategy linuxFuse() {
        return new MountStrategy(MountBackend.LINUX_FUSE, EnumSet.allOf(MountCapability.class));
    }

    public static MountStrategy macFuseT() {
        return new MountStrategy(MountBackend.MACOS_FUSE_T, EnumSet.allOf(MountCapability.class));
    }

    public static MountStrategy macFuse() {
        return new MountStrategy(MountBackend.MACOS_MACFUSE, EnumSet.allOf(MountCapability.class));
    }

    public static MountStrategy windowsWinFsp() {
        return new MountStrategy(MountBackend.WINDOWS_WINFSP, EnumSet.allOf(MountCapability.class));
    }

    public static MountStrategy windowsProjectionListRead() {
        return new MountStrategy(MountBackend.WINDOWS_PROJFS, EnumSet.of(MountCapability.LIST, MountCapability.READ));
    }

    public static MountStrategy listOnlyFallback() {
        return new MountStrategy(MountBackend.CLI_LIST_ONLY, EnumSet.of(MountCapability.LIST));
    }

    public static List<MountStrategy> portableUserSpaceDefaults() {
        return List.of(
                linuxFuse(),
                macFuseT(),
                windowsWinFsp(),
                listOnlyFallback()
        );
    }

    public MountStrategy {
        if (backend == null) {
            throw new IllegalArgumentException("backend is required");
        }
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
        if (capabilities.isEmpty()) {
            throw new IllegalArgumentException("mount strategy requires at least one capability");
        }
    }
}
