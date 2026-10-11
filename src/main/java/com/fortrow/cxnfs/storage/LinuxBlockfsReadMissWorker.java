package com.fortrow.cxnfs.storage;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class LinuxBlockfsReadMissWorker {
    public static final int DEFAULT_MAX_EVENTS_PER_DRAIN = 64;
    public static final Duration DEFAULT_IDLE_POLL_INTERVAL = Duration.ofMillis(10);

    private final LinuxBlockfsReadMissService service;
    private final int maxEventsPerDrain;
    private final Duration idlePollInterval;
    private final boolean allowUnsupportedExtents;
    private final AtomicLong drainCycles = new AtomicLong();
    private final AtomicLong eventsResolved = new AtomicLong();
    private final AtomicLong plannedChunks = new AtomicLong();
    private final AtomicLong fetchedChunks = new AtomicLong();
    private final AtomicLong publishedExtents = new AtomicLong();
    private final AtomicLong unsupportedExtents = new AtomicLong();
    private final AtomicLong payloadBytesWritten = new AtomicLong();
    private final AtomicReference<Instant> lastDrainAt = new AtomicReference<>();
    private final AtomicReference<String> lastError = new AtomicReference<>();
    private final Object lifecycleLock = new Object();
    private Thread thread;

    public LinuxBlockfsReadMissWorker(
            LinuxBlockfsReadMissService service,
            int maxEventsPerDrain,
            Duration idlePollInterval,
            boolean allowUnsupportedExtents
    ) {
        if (service == null) {
            throw new IllegalArgumentException("service is required");
        }
        if (maxEventsPerDrain <= 0) {
            throw new IllegalArgumentException("maxEventsPerDrain must be positive");
        }
        if (idlePollInterval == null || idlePollInterval.isZero() || idlePollInterval.isNegative()) {
            throw new IllegalArgumentException("idlePollInterval must be positive");
        }
        this.service = service;
        this.maxEventsPerDrain = maxEventsPerDrain;
        this.idlePollInterval = idlePollInterval;
        this.allowUnsupportedExtents = allowUnsupportedExtents;
    }

    public void start() {
        synchronized (lifecycleLock) {
            if (thread != null && thread.isAlive()) {
                throw new IllegalStateException("read miss worker is already running");
            }
            thread = Thread.ofVirtual()
                    .name("cxnfs-cxbfs-read-miss-worker")
                    .start(this::runLoop);
        }
    }

    public void stop() throws InterruptedException {
        Thread worker;
        synchronized (lifecycleLock) {
            worker = thread;
            thread = null;
        }
        if (worker == null) {
            return;
        }
        worker.interrupt();
        worker.join();
    }

    public int maxEventsPerDrain() {
        return maxEventsPerDrain;
    }

    public int cordCount() {
        return service.cordCount();
    }

    public Duration idlePollInterval() {
        return idlePollInterval;
    }

    public boolean allowUnsupportedExtents() {
        return allowUnsupportedExtents;
    }

    public boolean running() {
        Thread worker = thread;
        return worker != null && worker.isAlive();
    }

    public LinuxBlockfsReadMissWorkerSnapshot snapshot() {
        return new LinuxBlockfsReadMissWorkerSnapshot(
                running(),
                drainCycles.get(),
                eventsResolved.get(),
                plannedChunks.get(),
                fetchedChunks.get(),
                publishedExtents.get(),
                unsupportedExtents.get(),
                payloadBytesWritten.get(),
                Optional.ofNullable(lastDrainAt.get()),
                Optional.ofNullable(lastError.get())
        );
    }

    public List<LinuxBlockfsReadMissResolution> drainOnce()
            throws IOException, InterruptedException {
        List<LinuxBlockfsReadMissResolution> resolutions =
                service.drainAvailable(maxEventsPerDrain, allowUnsupportedExtents);
        recordDrain(resolutions);
        return resolutions;
    }

    private void runLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<LinuxBlockfsReadMissResolution> resolutions = drainOnce();
                if (resolutions.isEmpty()) {
                    Thread.sleep(idlePollInterval.toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException | RuntimeException e) {
                lastError.set(e.getClass().getSimpleName() + ": " + e.getMessage());
                try {
                    Thread.sleep(idlePollInterval.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void recordDrain(List<LinuxBlockfsReadMissResolution> resolutions) {
        drainCycles.incrementAndGet();
        lastDrainAt.set(Instant.now());
        lastError.set(null);
        for (LinuxBlockfsReadMissResolution resolution : resolutions) {
            eventsResolved.incrementAndGet();
            plannedChunks.addAndGet(resolution.plannedChunks());
            fetchedChunks.addAndGet(resolution.fetchedChunks());
            publishedExtents.addAndGet(resolution.publishedExtents());
            unsupportedExtents.addAndGet(resolution.unsupportedExtents());
            payloadBytesWritten.addAndGet(resolution.bytesWritten());
        }
    }
}
