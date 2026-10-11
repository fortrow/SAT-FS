package com.fortrow.cxnfs.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class LinuxBlockfsMetadataAuditor {
    public LinuxBlockfsMetadataAuditResult audit(
            LinuxBlockfsNamespaceSnapshot namespace,
            Optional<LinuxBlockfsManifestIndexSnapshot> manifestIndex,
            Optional<LinuxBlockfsExtentHintsSnapshot> extentHints
    ) {
        if (namespace == null) {
            throw new IllegalArgumentException("namespace is required");
        }
        manifestIndex = manifestIndex == null ? Optional.empty() : manifestIndex;
        extentHints = extentHints == null ? Optional.empty() : extentHints;

        Map<Long, LinuxBlockfsFile> filesByInode = filesByInode(namespace);
        Map<ChunkKey, LinuxBlockfsManifestChunk> chunksByKey = Map.of();
        int manifestFiles = 0;
        int manifestChunks = 0;
        if (manifestIndex.isPresent()) {
            LinuxBlockfsManifestIndexSnapshot manifest = manifestIndex.orElseThrow();
            requireSameGeneration("manifest index", manifest.generation(), namespace.generation());
            chunksByKey = auditManifest(namespace, manifest);
            manifestFiles = manifest.entries().size();
            manifestChunks = manifest.chunkCount();
        }

        int hintCount = 0;
        if (extentHints.isPresent()) {
            LinuxBlockfsExtentHintsSnapshot hints = extentHints.orElseThrow();
            requireSameGeneration("extent hints", hints.generation(), namespace.generation());
            if (manifestIndex.isEmpty()) {
                throw new IllegalStateException("extent hints require manifest index audit");
            }
            auditExtentHints(filesByInode, chunksByKey, hints);
            hintCount = hints.extents().size();
        }

        return new LinuxBlockfsMetadataAuditResult(
                namespace.files().size(),
                manifestFiles,
                manifestChunks,
                hintCount
        );
    }

    private static Map<Long, LinuxBlockfsFile> filesByInode(LinuxBlockfsNamespaceSnapshot namespace) {
        Map<Long, LinuxBlockfsFile> byInode = new HashMap<>();
        for (LinuxBlockfsFile file : namespace.files()) {
            byInode.put(file.inodeNumber(), file);
        }
        return byInode;
    }

    private static Map<ChunkKey, LinuxBlockfsManifestChunk> auditManifest(
            LinuxBlockfsNamespaceSnapshot namespace,
            LinuxBlockfsManifestIndexSnapshot manifest
    ) {
        Map<Long, LinuxBlockfsFile> namespaceByInode = filesByInode(namespace);
        Map<Long, LinuxBlockfsManifestEntry> manifestByInode = new HashMap<>();
        Map<ChunkKey, LinuxBlockfsManifestChunk> chunksByKey = new HashMap<>();

        for (LinuxBlockfsManifestEntry entry : manifest.entries()) {
            LinuxBlockfsFile namespaceFile = namespaceByInode.get(entry.inodeNumber());
            if (namespaceFile == null) {
                throw new IllegalStateException("manifest entry is not in namespace: " + entry.path());
            }
            if (!namespaceFile.path().equals(entry.path())) {
                throw new IllegalStateException("manifest path mismatch for inode: " + entry.inodeNumber());
            }
            if (namespaceFile.size() != entry.size()) {
                throw new IllegalStateException("manifest size mismatch for inode: " + entry.inodeNumber());
            }
            manifestByInode.put(entry.inodeNumber(), entry);
            for (LinuxBlockfsManifestChunk chunk : entry.chunks()) {
                chunksByKey.put(new ChunkKey(entry.inodeNumber(), chunk.fileOffset(), chunk.length()), chunk);
            }
        }
        for (LinuxBlockfsFile file : namespace.files()) {
            if (!manifestByInode.containsKey(file.inodeNumber())) {
                throw new IllegalStateException("namespace file missing from manifest index: " + file.path());
            }
        }
        return chunksByKey;
    }

    private static void auditExtentHints(
            Map<Long, LinuxBlockfsFile> filesByInode,
            Map<ChunkKey, LinuxBlockfsManifestChunk> chunksByKey,
            LinuxBlockfsExtentHintsSnapshot hints
    ) {
        for (LinuxBlockfsExtent extent : hints.extents()) {
            if (!filesByInode.containsKey(extent.inodeNumber())) {
                throw new IllegalStateException("extent hint inode is not in namespace: " + extent.inodeNumber());
            }
            ChunkKey key = new ChunkKey(extent.inodeNumber(), extent.fileOffset(), extent.payloadLength());
            LinuxBlockfsManifestChunk chunk = chunksByKey.get(key);
            if (chunk == null) {
                throw new IllegalStateException("extent hint does not match a manifest chunk");
            }
            if (!chunk.chunkHash().hex().equals(extent.expectedHashHex())) {
                throw new IllegalStateException("extent hint hash mismatch for inode: " + extent.inodeNumber());
            }
        }
    }

    private static void requireSameGeneration(String name, long generation, long namespaceGeneration) {
        if (generation != namespaceGeneration) {
            throw new IllegalStateException(name + " generation "
                    + generation
                    + " does not match namespace generation "
                    + namespaceGeneration);
        }
    }

    private record ChunkKey(long inodeNumber, long fileOffset, int length) {
    }
}
