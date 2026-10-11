package com.fortrow.cxnfs.storage;

public enum FilesystemStructureRole {
    AUTHORIZATION_GRANT,
    NAMESPACE_SNAPSHOT,
    FILE_EXTENT_HINTS,
    MANIFEST_INDEX,
    CHUNK_HASH_INDEX,
    CACHE_INDEX,
    JOURNAL,
    FREE_SPACE_MAP
}
