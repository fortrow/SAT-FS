package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.chunk.Chunker;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.PlannedChunkConsumer;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.store.CxnfsStore;
import com.fortrow.cxnfs.store.PathDeleteResult;
import com.fortrow.cxnfs.store.PathRenameResult;
import com.fortrow.cxnfs.transport.VerifiedEntryPublisher;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public final class LiveDirectoryRefresher {
    public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMillis(500);

    private final LiveSourceIndex sourceIndex;
    private final long refreshIntervalNanos;
    private final Object lock = new Object();
    private long lastRefreshNanos = Long.MIN_VALUE;
    private boolean backgroundRefreshRunning;

    public LiveDirectoryRefresher(List<ServedRoot> roots, CxnfsStore repository) {
        this(roots, repository, DEFAULT_REFRESH_INTERVAL);
    }

    LiveDirectoryRefresher(List<ServedRoot> roots, CxnfsStore repository, Duration refreshInterval) {
        this(roots, GearHashChunker.defaults(), refreshInterval, LiveWriteMode.DEFAULT);
    }

    public LiveDirectoryRefresher(List<ServedRoot> roots, Chunker chunker) {
        this(roots, chunker, DEFAULT_REFRESH_INTERVAL);
    }

    LiveDirectoryRefresher(List<ServedRoot> roots, Chunker chunker, Duration refreshInterval) {
        this(roots, chunker, refreshInterval, LiveWriteMode.DEFAULT);
    }

    LiveDirectoryRefresher(
            List<ServedRoot> roots,
            Chunker chunker,
            Duration refreshInterval,
            LiveWriteMode liveWriteMode
    ) {
        if (roots == null || roots.isEmpty()) {
            throw new IllegalArgumentException("roots are required");
        }
        if (chunker == null) {
            throw new IllegalArgumentException("chunker is required");
        }
        if (refreshInterval == null || refreshInterval.isNegative()) {
            throw new IllegalArgumentException("refreshInterval must be non-negative");
        }
        this.sourceIndex = new LiveSourceIndex(roots, chunker, liveWriteMode);
        this.refreshIntervalNanos = refreshInterval.toNanos();
    }

    public DirectoryLoadResult refreshNow() throws IOException {
        synchronized (lock) {
            DirectoryLoadResult result = sourceIndex.refreshNow();
            lastRefreshNanos = System.nanoTime();
            return result;
        }
    }

    public void refreshIfDue() throws IOException {
        long now = System.nanoTime();
        synchronized (lock) {
            if (lastRefreshNanos != Long.MIN_VALUE
                    && refreshIntervalNanos > 0
                    && now - lastRefreshNanos < refreshIntervalNanos) {
                return;
            }
            if (refreshIntervalNanos == 0) {
                DirectoryLoadResult ignored = sourceIndex.refreshNow();
                lastRefreshNanos = System.nanoTime();
                return;
            }
            if (backgroundRefreshRunning) {
                return;
            }
            backgroundRefreshRunning = true;
            lastRefreshNanos = now;
            Thread refreshThread = new Thread(this::refreshInBackground, "cxnfs-live-directory-refresh");
            refreshThread.setDaemon(true);
            refreshThread.start();
        }
    }

    private void refreshInBackground() {
        try {
            DirectoryLoadResult ignored = sourceIndex.refreshNow();
        } catch (IOException | RuntimeException ignored) {
            // A later foreground operation will schedule another refresh attempt.
        } finally {
            synchronized (lock) {
                backgroundRefreshRunning = false;
                lastRefreshNanos = System.nanoTime();
            }
        }
    }

    public boolean isCovered(String path) {
        return sourceIndex.isCovered(path);
    }

    public Optional<Path> localPathFor(String namespacePath) {
        return sourceIndex.localPathFor(namespacePath);
    }

    public DirectoryListing list(String path) {
        return sourceIndex.list(path);
    }

    public Optional<FileMetadata> stat(String path) {
        return sourceIndex.stat(path);
    }

    public Optional<FileManifest> manifest(String path) throws IOException {
        return sourceIndex.manifest(path);
    }

    public Optional<FileManifest> manifest(ContentHash fileHash) {
        return sourceIndex.manifest(fileHash);
    }

    public ReadPlan readPlan(String path, long offset, long length) throws IOException {
        return sourceIndex.readPlan(path, offset, length);
    }

    public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        return sourceIndex.readPlan(fileHash, offset, length);
    }

    public byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException {
        return sourceIndex.readRange(fileHash, offset, length);
    }

    public byte[] reconstruct(ContentHash fileHash) throws IOException {
        return sourceIndex.reconstruct(fileHash);
    }

    public void readVerifiedPlanChunks(ReadPlan plan, PlannedChunkConsumer consumer) throws IOException {
        sourceIndex.readVerifiedPlanChunks(plan, consumer);
    }

    public PathDeleteResult deletePath(String path, boolean recursive) throws IOException {
        return sourceIndex.deletePath(path, recursive);
    }

    public PathRenameResult renamePath(String sourcePath, String targetPath) throws IOException {
        return sourceIndex.renamePath(sourcePath, targetPath);
    }

    public VerifiedEntryPublisher threadedPublisher(String namespacePath) throws IOException {
        return threadedPublisher(namespacePath, -1L);
    }

    public VerifiedEntryPublisher threadedPublisher(String namespacePath, long sizeHintBytes) throws IOException {
        return sourceIndex.threadedPublisher(namespacePath, sizeHintBytes);
    }
}
