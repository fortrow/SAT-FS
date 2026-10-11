package com.fortrow.cxnfs.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.OptionalLong;

public interface BulkServerListener extends AutoCloseable {
    void start();

    InetSocketAddress boundAddress();

    boolean running();

    int activeConnectionCount();

    default OptionalLong totalConnectionCount() {
        return OptionalLong.empty();
    }

    default OptionalLong totalBulkStreamCount() {
        return OptionalLong.empty();
    }

    Optional<IOException> lastAcceptError();

    Optional<IOException> lastConnectionError();

    void awaitShutdown() throws InterruptedException;

    @Override
    void close() throws IOException;
}
