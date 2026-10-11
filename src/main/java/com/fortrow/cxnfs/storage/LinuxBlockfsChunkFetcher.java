package com.fortrow.cxnfs.storage;

import java.io.IOException;

@FunctionalInterface
public interface LinuxBlockfsChunkFetcher {
    byte[] fetchChunk(LinuxBlockfsManifestEntry entry, LinuxBlockfsManifestChunk chunk)
            throws IOException, InterruptedException;
}
