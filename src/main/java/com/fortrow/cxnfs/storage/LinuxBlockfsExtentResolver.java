package com.fortrow.cxnfs.storage;

@FunctionalInterface
public interface LinuxBlockfsExtentResolver {
    LinuxBlockfsExtent resolve(EarlyReadStoragePlan plan, EarlyReadPayloadExtent extent);
}
