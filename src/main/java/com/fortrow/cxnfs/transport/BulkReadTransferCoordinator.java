package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.server.CxnFileServer;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public final class BulkReadTransferCoordinator {
    private final CxnFileServer server;
    private final Function<UUID, Optional<AdmittedReadTransfer>> transferLookup;
    private final BulkReadDeliveryService deliveryService;

    public BulkReadTransferCoordinator(
            CxnFileServer server,
            Function<UUID, Optional<AdmittedReadTransfer>> transferLookup
    ) {
        if (server == null) {
            throw new IllegalArgumentException("server is required");
        }
        if (transferLookup == null) {
            throw new IllegalArgumentException("transferLookup is required");
        }
        this.server = server;
        this.transferLookup = transferLookup;
        this.deliveryService = new BulkReadDeliveryService(server);
    }

    public BulkReadDeliveryResult deliver(
            UUID sessionId,
            UUID transferId,
            String path,
            long offset,
            long length,
            BulkFrameSink sink
    ) throws IOException {
        AdmittedReadTransfer transfer = transferLookup.apply(transferId)
                .orElseThrow(() -> new IllegalStateException("read transfer is not admitted: " + transferId));
        requireSessionMatchesAdmission(transfer, sessionId);
        requirePathMatchesAdmission(transfer, path);
        ReadPlan plan = server.readPlan(path, offset, length);
        return deliveryService.deliver(transfer, 0, plan, sink);
    }

    public BulkReadDeliveryResult deliver(
            UUID sessionId,
            UUID transferId,
            String path,
            long offset,
            long length,
            int activeCords,
            BulkFrameSink sink
    ) throws IOException {
        AdmittedReadTransfer transfer = transferLookup.apply(transferId)
                .orElseThrow(() -> new IllegalStateException("read transfer is not admitted: " + transferId));
        requireSessionMatchesAdmission(transfer, sessionId);
        requirePathMatchesAdmission(transfer, path);
        ReadPlan plan = server.readPlan(path, offset, length);
        return deliveryService.deliver(transfer, 0, activeCords, plan, sink);
    }

    private static void requireSessionMatchesAdmission(AdmittedReadTransfer transfer, UUID sessionId) {
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (!transfer.sessionId().equals(sessionId)) {
            throw new IllegalArgumentException("bulk read session does not match admitted control session");
        }
    }

    private static void requirePathMatchesAdmission(AdmittedReadTransfer transfer, String path) {
        if (!transfer.path().equals(normalizePath(path))) {
            throw new IllegalArgumentException("read path does not match admitted transfer path: " + path);
        }
    }

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }
}
