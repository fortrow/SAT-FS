package com.fortrow.cxnfs.sys;

import java.util.Locale;

public final class PlatformUtils {
    public enum OpSys {
        WINDOWS,
        MAC_OSX,
        LINUX,
        UNKNOWN
    }

    private PlatformUtils() {}

    public static OpSys getOsType(boolean fallback) {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            return OpSys.WINDOWS;
        }
        if (os.contains("mac") || os.contains("darwin") || os.contains("osx")) {
            return OpSys.MAC_OSX;
        }
        if (os.contains("linux")) {
            return OpSys.LINUX;
        }
        return fallback ? OpSys.UNKNOWN : OpSys.UNKNOWN;
    }
}
