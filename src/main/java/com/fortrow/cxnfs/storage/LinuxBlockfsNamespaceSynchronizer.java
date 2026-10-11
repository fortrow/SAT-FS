package com.fortrow.cxnfs.storage;

import java.io.IOException;
import java.nio.file.Path;

public final class LinuxBlockfsNamespaceSynchronizer {
    private final LinuxBlockfsNamespaceStore store;
    private final LinuxBlockfsManifestIndexStore manifestIndexStore;
    private final LinuxBlockfsExtentHintStore extentHintStore;
    private final LinuxBlockfsExtentPublisher publisher;
    private final LinuxBlockfsMetadataAuditor auditor;

    public LinuxBlockfsNamespaceSynchronizer(
            LinuxBlockfsNamespaceStore store,
            LinuxBlockfsExtentPublisher publisher
    ) {
        this(store, new LinuxBlockfsManifestIndexStore(), new LinuxBlockfsExtentHintStore(), publisher);
    }

    public LinuxBlockfsNamespaceSynchronizer(
            LinuxBlockfsNamespaceStore store,
            LinuxBlockfsManifestIndexStore manifestIndexStore,
            LinuxBlockfsExtentHintStore extentHintStore,
            LinuxBlockfsExtentPublisher publisher
    ) {
        if (store == null) {
            throw new IllegalArgumentException("store is required");
        }
        if (manifestIndexStore == null) {
            throw new IllegalArgumentException("manifestIndexStore is required");
        }
        if (extentHintStore == null) {
            throw new IllegalArgumentException("extentHintStore is required");
        }
        if (publisher == null) {
            throw new IllegalArgumentException("publisher is required");
        }
        this.store = store;
        this.manifestIndexStore = manifestIndexStore;
        this.extentHintStore = extentHintStore;
        this.publisher = publisher;
        this.auditor = new LinuxBlockfsMetadataAuditor();
    }

    public LinuxBlockfsNamespaceSyncResult sync(Path metadataRoot)
            throws IOException, InterruptedException {
        return sync(metadataRoot, false);
    }

    public LinuxBlockfsNamespaceSyncResult sync(Path metadataRoot, boolean allowUnsupportedExtents)
            throws IOException, InterruptedException {
        LinuxBlockfsNamespaceSnapshot snapshot = store.load(metadataRoot);
        var manifestIndex = manifestIndexStore.loadIfExists(metadataRoot);
        var extentHints = extentHintStore.loadIfExists(metadataRoot);
        LinuxBlockfsMetadataAuditResult audit = auditor.audit(snapshot, manifestIndex, extentHints);
        LinuxBlockfsMountSelector selector = LinuxBlockfsMountSelector.metadataRoot(metadataRoot);
        int publishedFiles = publisher.publishNamespace(selector, snapshot, true);
        LinuxBlockfsPublishResult publishResult = new LinuxBlockfsPublishResult(0, 0, 0, 0);
        if (extentHints.isPresent()) {
            LinuxBlockfsExtentHintsSnapshot hints = extentHints.orElseThrow();
            publishResult = publisher.publishExtentHints(selector, hints, allowUnsupportedExtents);
        }
        return new LinuxBlockfsNamespaceSyncResult(
                metadataRoot,
                snapshot.generation(),
                publishedFiles,
                audit.manifestFiles(),
                audit.manifestChunks(),
                audit.extentHints(),
                publishResult.attemptedExtents(),
                publishResult.acceptedExtents(),
                publishResult.unsupportedExtents(),
                publishResult.acceptedBytes()
        );
    }
}
