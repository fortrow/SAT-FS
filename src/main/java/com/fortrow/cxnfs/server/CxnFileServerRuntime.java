package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.transport.BulkReadSessionHandler;
import com.fortrow.cxnfs.transport.BulkReadTransferCoordinator;
import com.fortrow.cxnfs.transport.BulkTransferSessionHandler;

public final class CxnFileServerRuntime {
    private final CxnFileServer server;
    private final ControlSessionDispatcher controlDispatcher;
    private final BulkReadTransferCoordinator bulkReadTransferCoordinator;
    private final BulkReadSessionHandler bulkReadSessionHandler;
    private final BulkTransferSessionHandler bulkTransferSessionHandler;

    public CxnFileServerRuntime(CxnFileServer server) {
        if (server == null) {
            throw new IllegalArgumentException("server is required");
        }
        this.server = server;
        this.controlDispatcher = new ControlSessionDispatcher(server);
        this.bulkReadTransferCoordinator = new BulkReadTransferCoordinator(
                server,
                controlDispatcher::admittedReadTransfer
        );
        this.bulkReadSessionHandler = new BulkReadSessionHandler(bulkReadTransferCoordinator);
        this.bulkTransferSessionHandler = new BulkTransferSessionHandler(
                bulkReadSessionHandler,
                controlDispatcher::stageWritePayload
        );
    }

    public CxnFileServer server() {
        return server;
    }

    public ControlSessionDispatcher controlDispatcher() {
        return controlDispatcher;
    }

    public BulkReadTransferCoordinator bulkReadTransferCoordinator() {
        return bulkReadTransferCoordinator;
    }

    public BulkReadSessionHandler bulkReadSessionHandler() {
        return bulkReadSessionHandler;
    }

    public BulkTransferSessionHandler bulkTransferSessionHandler() {
        return bulkTransferSessionHandler;
    }
}
