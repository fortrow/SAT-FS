package com.fortrow.cxnfs.storage;

import java.io.IOException;

public final class LinuxBlockfsExtentPublisher {
    private final LinuxBlockfsControlCommand control;
    private final LinuxBlockfsExtentResolver resolver;

    public LinuxBlockfsExtentPublisher(
            LinuxBlockfsControlCommand control,
            LinuxBlockfsExtentResolver resolver
    ) {
        if (control == null) {
            throw new IllegalArgumentException("control is required");
        }
        if (resolver == null) {
            throw new IllegalArgumentException("resolver is required");
        }
        this.control = control;
        this.resolver = resolver;
    }

    public LinuxBlockfsPublishResult publishVerifiedExtents(
            EarlyReadStoragePlan plan,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        return publishVerifiedExtents(LinuxBlockfsMountSelector.unselected(), plan, allowUnsupported);
    }

    public LinuxBlockfsPublishResult publishVerifiedExtents(
            LinuxBlockfsMountSelector selector,
            EarlyReadStoragePlan plan,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        if (!plan.finalPublishRequiresTrailingStructures()) {
            throw new IllegalArgumentException("blockfs publish must be gated by trailing metadata verification");
        }

        int attempted = 0;
        int accepted = 0;
        int unsupported = 0;
        long acceptedBytes = 0;
        for (EarlyReadPayloadExtent payloadExtent : plan.payloadExtents()) {
            LinuxBlockfsExtent extent = resolver.resolve(plan, payloadExtent);
            if (extent == null) {
                throw new IllegalStateException("extent resolver returned null for chunk "
                        + payloadExtent.chunkIndex());
            }
            attempted++;
            LinuxBlockfsControlResult result = control.registerExtent(selector, extent, allowUnsupported);
            if (result.accepted()) {
                accepted++;
                acceptedBytes = Math.addExact(acceptedBytes, extent.payloadLength());
            } else if (result.unsupported()) {
                unsupported++;
            }
        }
        return new LinuxBlockfsPublishResult(attempted, accepted, unsupported, acceptedBytes);
    }

    public LinuxBlockfsControlResult publishFile(LinuxBlockfsFile file)
            throws IOException, InterruptedException {
        return control.registerFile(file);
    }

    public LinuxBlockfsControlResult publishFile(
            LinuxBlockfsMountSelector selector,
            LinuxBlockfsFile file
    ) throws IOException, InterruptedException {
        return control.registerFile(selector, file);
    }

    public int publishNamespace(LinuxBlockfsNamespaceSnapshot snapshot)
            throws IOException, InterruptedException {
        return publishNamespace(snapshot, false);
    }

    public int publishNamespace(LinuxBlockfsNamespaceSnapshot snapshot, boolean clearBeforePublish)
            throws IOException, InterruptedException {
        return publishNamespace(LinuxBlockfsMountSelector.unselected(), snapshot, clearBeforePublish);
    }

    public int publishNamespace(
            LinuxBlockfsMountSelector selector,
            LinuxBlockfsNamespaceSnapshot snapshot,
            boolean clearBeforePublish
    ) throws IOException, InterruptedException {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot is required");
        }
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        if (clearBeforePublish) {
            control.clearNamespace(selector);
        }
        int published = 0;
        for (LinuxBlockfsFile file : snapshot.files()) {
            control.registerFile(selector, file);
            published++;
        }
        return published;
    }

    public LinuxBlockfsPublishResult publishExtentHints(
            LinuxBlockfsExtentHintsSnapshot snapshot,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        return publishExtentHints(LinuxBlockfsMountSelector.unselected(), snapshot, allowUnsupported);
    }

    public LinuxBlockfsPublishResult publishExtentHints(
            LinuxBlockfsMountSelector selector,
            LinuxBlockfsExtentHintsSnapshot snapshot,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot is required");
        }
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        int attempted = 0;
        int accepted = 0;
        int unsupported = 0;
        long acceptedBytes = 0;
        for (LinuxBlockfsExtent extent : snapshot.extents()) {
            attempted++;
            LinuxBlockfsControlResult result = control.registerExtent(selector, extent, allowUnsupported);
            if (result.accepted()) {
                accepted++;
                acceptedBytes = Math.addExact(acceptedBytes, extent.payloadLength());
            } else if (result.unsupported()) {
                unsupported++;
            }
        }
        return new LinuxBlockfsPublishResult(attempted, accepted, unsupported, acceptedBytes);
    }

    public LinuxBlockfsControlResult invalidate(long inodeNumber, long offset, long length)
            throws IOException, InterruptedException {
        return control.invalidate(new LinuxBlockfsInvalidation(inodeNumber, offset, length), false);
    }

    public LinuxBlockfsControlResult invalidate(
            LinuxBlockfsMountSelector selector,
            long inodeNumber,
            long offset,
            long length
    ) throws IOException, InterruptedException {
        return control.invalidate(selector, new LinuxBlockfsInvalidation(inodeNumber, offset, length), false);
    }
}
