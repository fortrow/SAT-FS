package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.transport.CordPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class LinuxBlockfsReadMissService {
    public static final int DEFAULT_CORD_COUNT = 1;
    public static final int MAX_CORD_COUNT = CordPoolConfig.ABSOLUTE_MAX_CORDS;

    private final LinuxBlockfsControlCommand control;
    private final LinuxBlockfsReadMissPlanner planner;
    private final LinuxBlockfsPayloadCacheWriter payloadWriter;
    private final LinuxBlockfsChunkFetcher fetcher;
    private final int cordCount;
    private final LinuxBlockfsMountSelector expectedSelector;

    public LinuxBlockfsReadMissService(
            LinuxBlockfsControlCommand control,
            LinuxBlockfsReadMissPlanner planner,
            LinuxBlockfsPayloadCacheWriter payloadWriter,
            LinuxBlockfsChunkFetcher fetcher
    ) {
        this(control, planner, payloadWriter, fetcher, DEFAULT_CORD_COUNT);
    }

    public LinuxBlockfsReadMissService(
            LinuxBlockfsControlCommand control,
            LinuxBlockfsReadMissPlanner planner,
            LinuxBlockfsPayloadCacheWriter payloadWriter,
            LinuxBlockfsChunkFetcher fetcher,
            int cordCount
    ) {
        this(control, planner, payloadWriter, fetcher, cordCount, LinuxBlockfsMountSelector.unselected());
    }

    public LinuxBlockfsReadMissService(
            LinuxBlockfsControlCommand control,
            LinuxBlockfsReadMissPlanner planner,
            LinuxBlockfsPayloadCacheWriter payloadWriter,
            LinuxBlockfsChunkFetcher fetcher,
            int cordCount,
            LinuxBlockfsMountSelector expectedSelector
    ) {
        if (control == null) {
            throw new IllegalArgumentException("control is required");
        }
        if (planner == null) {
            throw new IllegalArgumentException("planner is required");
        }
        if (payloadWriter == null) {
            throw new IllegalArgumentException("payloadWriter is required");
        }
        if (fetcher == null) {
            throw new IllegalArgumentException("fetcher is required");
        }
        if (cordCount <= 0 || cordCount > MAX_CORD_COUNT) {
            throw new IllegalArgumentException("cordCount must be between 1 and " + MAX_CORD_COUNT);
        }
        if (expectedSelector == null) {
            throw new IllegalArgumentException("expectedSelector is required");
        }
        this.control = control;
        this.planner = planner;
        this.payloadWriter = payloadWriter;
        this.fetcher = fetcher;
        this.cordCount = cordCount;
        this.expectedSelector = expectedSelector;
    }

    public int cordCount() {
        return cordCount;
    }

    public Optional<LinuxBlockfsReadMissResolution> resolveNext(
            boolean nonblocking,
            boolean allowUnsupportedExtents
    ) throws IOException, InterruptedException {
        Optional<LinuxBlockfsMissEvent> event = control.nextMissEvent(expectedSelector, nonblocking);
        if (event.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(resolve(event.orElseThrow(), allowUnsupportedExtents));
    }

    public List<LinuxBlockfsReadMissResolution> drainAvailable(
            int maxEvents,
            boolean allowUnsupportedExtents
    ) throws IOException, InterruptedException {
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be positive");
        }
        List<LinuxBlockfsReadMissResolution> resolved = new ArrayList<>();
        while (resolved.size() < maxEvents) {
            Optional<LinuxBlockfsReadMissResolution> next = resolveNext(true, allowUnsupportedExtents);
            if (next.isEmpty()) {
                break;
            }
            resolved.add(next.orElseThrow());
        }
        return List.copyOf(resolved);
    }

    public LinuxBlockfsReadMissResolution resolve(
            LinuxBlockfsMissEvent event,
            boolean allowUnsupportedExtents
    ) throws IOException, InterruptedException {
        validateEventScope(event);
        LinuxBlockfsReadMissPlan plan = planner.plan(event);
        LinuxBlockfsMountSelector selector = selectorFor(event);
        int published = 0;
        int unsupported = 0;
        long bytesWritten = 0;
        List<ResolvedChunk> resolvedChunks = resolveChunks(plan);

        for (ResolvedChunk resolved : resolvedChunks) {
            bytesWritten = Math.addExact(bytesWritten, resolved.bytesWritten());
            LinuxBlockfsControlResult result = control.registerExtent(
                    selector,
                    resolved.extent(),
                    allowUnsupportedExtents
            );
            if (result.accepted()) {
                published++;
            } else if (result.unsupported()) {
                unsupported++;
            }
        }

        return new LinuxBlockfsReadMissResolution(
                event,
                plan.chunks().size(),
                resolvedChunks.size(),
                published,
                unsupported,
                bytesWritten
        );
    }

    private List<ResolvedChunk> resolveChunks(LinuxBlockfsReadMissPlan plan)
            throws IOException, InterruptedException {
        if (plan.chunks().isEmpty()) {
            return List.of();
        }
        int workerCount = Math.min(cordCount, plan.chunks().size());
        Thread.Builder.OfVirtual threadBuilder = Thread.ofVirtual()
                .name("cxnfs-cxbfs-read-miss-cord-", 0);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount, threadBuilder.factory());
        try {
            List<Future<ResolvedChunk>> futures = new ArrayList<>(plan.chunks().size());
            for (LinuxBlockfsReadMissChunk chunk : plan.chunks()) {
                futures.add(executor.submit(() -> resolveChunk(chunk)));
            }
            List<ResolvedChunk> resolved = new ArrayList<>(futures.size());
            for (Future<ResolvedChunk> future : futures) {
                resolved.add(await(future));
            }
            return List.copyOf(resolved);
        } finally {
            executor.shutdownNow();
        }
    }

    private ResolvedChunk resolveChunk(LinuxBlockfsReadMissChunk chunk)
            throws IOException, InterruptedException {
        byte[] data = fetcher.fetchChunk(chunk.entry(), chunk.manifestChunk());
        long bytesWritten = payloadWriter.writeVerified(chunk, data);
        return new ResolvedChunk(chunk.toExtent(), bytesWritten);
    }

    private void validateEventScope(LinuxBlockfsMissEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event is required");
        }
        if (!expectedSelector.selected()) {
            return;
        }
        if (!expectedSelector.metadataRoot().isBlank()) {
            String eventRoot = normalizedMetadataRoot(event.metadataRoot());
            if (!eventRoot.isBlank() && !eventRoot.equals(expectedSelector.metadataRoot())) {
                throw new IllegalStateException("miss event metadata root "
                        + eventRoot
                        + " does not match resolver metadata root "
                        + expectedSelector.metadataRoot());
            }
        }
        if (expectedSelector.mountId() > 0 && event.mountId() > 0
                && event.mountId() != expectedSelector.mountId()) {
            throw new IllegalStateException("miss event mount id "
                    + event.mountId()
                    + " does not match resolver mount id "
                    + expectedSelector.mountId());
        }
    }

    private LinuxBlockfsMountSelector selectorFor(LinuxBlockfsMissEvent event) {
        String eventRoot = normalizedMetadataRoot(event.metadataRoot());
        if (!eventRoot.isBlank()) {
            return LinuxBlockfsMountSelector.metadataRoot(eventRoot);
        }
        if (event.mountId() > 0 && expectedSelector.metadataRoot().isBlank()) {
            return LinuxBlockfsMountSelector.mountId(event.mountId());
        }
        if (expectedSelector.selected()) {
            return expectedSelector;
        }
        return LinuxBlockfsMountSelector.unselected();
    }

    private static String normalizedMetadataRoot(String metadataRoot) {
        if (metadataRoot == null || metadataRoot.isBlank() || "-".equals(metadataRoot)) {
            return "";
        }
        return LinuxBlockfsMountSelector.metadataRoot(metadataRoot).metadataRoot();
    }

    private static ResolvedChunk await(Future<ResolvedChunk> future)
            throws IOException, InterruptedException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("failed to resolve blockfs read miss chunk", cause);
        }
    }

    private record ResolvedChunk(LinuxBlockfsExtent extent, long bytesWritten) {
    }
}
