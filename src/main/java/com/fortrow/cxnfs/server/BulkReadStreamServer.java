package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.transport.BulkReadSessionHandler;
import com.fortrow.cxnfs.transport.BulkTransferSessionHandler;
import com.fortrow.cxnfs.transport.CordPoolConfig;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class BulkReadStreamServer implements BulkServerListener {
    private static final int STREAM_LISTEN_BACKLOG = CordPoolConfig.ABSOLUTE_MAX_CORDS + 32;

    private final ServerSocket serverSocket;
    private final BulkConnectionHandler handler;
    private final ExecutorService connectionExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<IOException> lastAcceptError = new AtomicReference<>();
    private final AtomicReference<IOException> lastConnectionError = new AtomicReference<>();
    private final AtomicInteger totalConnections = new AtomicInteger();
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private Thread acceptThread;

    private BulkReadStreamServer(
            ServerSocket serverSocket,
            BulkConnectionHandler handler,
            ExecutorService connectionExecutor
    ) {
        this.serverSocket = serverSocket;
        this.handler = handler;
        this.connectionExecutor = connectionExecutor;
    }

    public static BulkReadStreamServer bind(
            CxnFileServerRuntime runtime,
            ServerSocketFactory socketFactory
    ) throws IOException {
        if (runtime == null) {
            throw new IllegalArgumentException("runtime is required");
        }
        return bind(
                runtime.server().config().containerTransferBinding().address(),
                runtime.bulkTransferSessionHandler(),
                socketFactory
        );
    }

    public static BulkReadStreamServer bind(
            InetSocketAddress address,
            BulkReadSessionHandler handler,
            ServerSocketFactory socketFactory
    ) throws IOException {
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler is required");
        }
        if (socketFactory == null) {
            throw new IllegalArgumentException("socketFactory is required");
        }
        ServerSocket serverSocket = socketFactory.createServerSocket();
        serverSocket.bind(address, STREAM_LISTEN_BACKLOG);
        return new BulkReadStreamServer(
                serverSocket,
                (input, output) -> {
                    handler.drainUntilEof(input, output);
                    return null;
                },
                Executors.newCachedThreadPool(daemonThreadFactory("cxnfs-bulk-read-stream-connection-"))
        );
    }

    public static BulkReadStreamServer bind(
            InetSocketAddress address,
            BulkTransferSessionHandler handler,
            ServerSocketFactory socketFactory
    ) throws IOException {
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler is required");
        }
        if (socketFactory == null) {
            throw new IllegalArgumentException("socketFactory is required");
        }
        ServerSocket serverSocket = socketFactory.createServerSocket();
        serverSocket.bind(address, STREAM_LISTEN_BACKLOG);
        return new BulkReadStreamServer(
                serverSocket,
                (input, output) -> {
                    handler.drainUntilEof(input, output);
                    return null;
                },
                Executors.newCachedThreadPool(daemonThreadFactory("cxnfs-bulk-read-stream-connection-"))
        );
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        acceptThread = daemonThreadFactory("cxnfs-bulk-read-stream-accept-").newThread(this::acceptLoop);
        acceptThread.start();
    }

    @Override
    public InetSocketAddress boundAddress() {
        return new InetSocketAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());
    }

    @Override
    public Optional<IOException> lastAcceptError() {
        return Optional.ofNullable(lastAcceptError.get());
    }

    @Override
    public Optional<IOException> lastConnectionError() {
        return Optional.ofNullable(lastConnectionError.get());
    }

    @Override
    public boolean running() {
        return running.get() && !serverSocket.isClosed();
    }

    @Override
    public int activeConnectionCount() {
        return connections.size();
    }

    @Override
    public OptionalLong totalConnectionCount() {
        return OptionalLong.of(totalConnections.get());
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        Thread thread = acceptThread;
        if (thread != null) {
            thread.join();
        }
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        IOException failure = null;
        for (Socket connection : connections) {
            try {
                connection.close();
            } catch (IOException e) {
                failure = e;
            }
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            failure = e;
        }
        connectionExecutor.shutdownNow();
        if (failure != null) {
            throw failure;
        }
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                enableTcpNoDelay(socket);
                totalConnections.incrementAndGet();
                connections.add(socket);
                connectionExecutor.submit(() -> handleConnection(socket));
            } catch (SocketException e) {
                if (running.get()) {
                    lastAcceptError.set(e);
                }
                break;
            } catch (IOException e) {
                lastAcceptError.set(e);
                break;
            }
        }
    }

    private static void enableTcpNoDelay(Socket socket) {
        try {
            socket.setTcpNoDelay(true);
        } catch (SocketException ignored) {
            // TCP_NODELAY is an optimization; accepted sockets must remain usable if the platform rejects it.
        }
    }

    private void handleConnection(Socket socket) {
        try (Socket ignored = socket) {
            startTlsHandshakeIfNeeded(socket);
            handler.handleUntilEof(socket.getInputStream(), socket.getOutputStream());
        } catch (IOException e) {
            recordConnectionError(socket, e);
        } catch (RuntimeException e) {
            recordConnectionError(socket, new IOException("bulk connection handler failed", e));
        } finally {
            connections.remove(socket);
        }
    }

    private static void startTlsHandshakeIfNeeded(Socket socket) throws IOException {
        if (socket instanceof SSLSocket sslSocket) {
            sslSocket.startHandshake();
        }
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        return new ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger sequence =
                    new java.util.concurrent.atomic.AtomicInteger();

            @Override
            public Thread newThread(Runnable task) {
                Thread thread = new Thread(task, prefix + sequence.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        };
    }

    private void recordConnectionError(Socket socket, IOException error) {
        if (running.get()) {
            lastConnectionError.set(error);
        }
    }

    @FunctionalInterface
    private interface BulkConnectionHandler {
        Object handleUntilEof(java.io.InputStream input, java.io.OutputStream output) throws IOException;
    }
}
