package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.protocol.ControlFrameStream;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ControlTcpServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ControlFrameHandler frameHandler;
    private final ExecutorService connectionExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<IOException> lastAcceptError = new AtomicReference<>();
    private final AtomicReference<IOException> lastConnectionError = new AtomicReference<>();
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private Thread acceptThread;

    private ControlTcpServer(
            ServerSocket serverSocket,
            ControlFrameHandler frameHandler,
            ExecutorService connectionExecutor
    ) {
        this.serverSocket = serverSocket;
        this.frameHandler = frameHandler;
        this.connectionExecutor = connectionExecutor;
    }

    public static ControlTcpServer bind(CxnFileServer server, ServerSocketFactory socketFactory) throws IOException {
        if (server == null) {
            throw new IllegalArgumentException("server is required");
        }
        return bind(new CxnFileServerRuntime(server), socketFactory);
    }

    public static ControlTcpServer bind(CxnFileServerRuntime runtime, ServerSocketFactory socketFactory) throws IOException {
        if (runtime == null) {
            throw new IllegalArgumentException("runtime is required");
        }
        if (socketFactory == null) {
            throw new IllegalArgumentException("socketFactory is required");
        }
        ServerSocket serverSocket = socketFactory.createServerSocket();
        serverSocket.bind(runtime.server().config().controlBinding().address());
        ControlFrameHandler frameHandler = new ControlFrameHandler(runtime.controlDispatcher());
        return new ControlTcpServer(serverSocket, frameHandler, Executors.newCachedThreadPool(daemonThreadFactory(
                "cxnfs-control-connection-"
        )));
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        acceptThread = daemonThreadFactory("cxnfs-control-accept-").newThread(this::acceptLoop);
        acceptThread.start();
    }

    public InetSocketAddress boundAddress() {
        return new InetSocketAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());
    }

    public Optional<IOException> lastAcceptError() {
        return Optional.ofNullable(lastAcceptError.get());
    }

    public Optional<IOException> lastConnectionError() {
        return Optional.ofNullable(lastConnectionError.get());
    }

    public boolean running() {
        return running.get() && !serverSocket.isClosed();
    }

    public int activeConnectionCount() {
        return connections.size();
    }

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
            while (running.get() && !socket.isClosed()) {
                Optional<byte[]> frame = ControlFrameStream.read(socket.getInputStream());
                if (frame.isEmpty()) {
                    return;
                }
                byte[] response = frameHandler.handle(frame.orElseThrow());
                ControlFrameStream.write(socket.getOutputStream(), response);
            }
        } catch (IOException e) {
            recordConnectionError(socket, e);
        } catch (RuntimeException e) {
            recordConnectionError(socket, new IOException("control connection handler failed", e));
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
}
