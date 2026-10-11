package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.transport.BulkTransferSessionHandler;
import com.fortrow.cxnfs.transport.CordPoolConfig;
import com.fortrow.cxnfs.transport.QuicBulkProtocol;
import com.fortrow.cxnfs.transport.QuicStreamSendBufferTuner;
import com.fortrow.cxnfs.transport.TlsCipherSuite;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.server.ApplicationProtocolConnection;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.core.server.ServerConnectionConfig;
import tech.kwik.core.server.ServerConnector;
import tech.kwik.core.log.NullLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class QuicBulkTransferServer implements BulkServerListener {
    public static final TlsCipherSuite CIPHER_SUITE = TlsCipherSuite.TLS_AES_128_GCM_SHA256;
    private static final long BIDIRECTIONAL_STREAM_BUFFER_BYTES = 16L * 1024L * 1024L;
    private static final long CONNECTION_BUFFER_BYTES = 512L * 1024L * 1024L;
    private static final int STREAM_SEND_BUFFER_BYTES = 16 * 1024 * 1024;
    private static final int WRITE_ACK_STREAM_RESERVE = 1;

    private final DatagramSocket socket;
    private final ServerConnector connector;
    private final ExecutorService streamExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeStreams = new AtomicInteger();
    private final AtomicInteger maxActiveStreams = new AtomicInteger();
    private final AtomicInteger totalStreams = new AtomicInteger();
    private final AtomicInteger streamSendBuffersTuned = new AtomicInteger();
    private final AtomicInteger streamSendBufferTuneFailures = new AtomicInteger();
    private final AtomicReference<IOException> lastStreamError = new AtomicReference<>();

    private QuicBulkTransferServer(
            DatagramSocket socket,
            ServerConnector connector,
            ExecutorService streamExecutor
    ) {
        this.socket = socket;
        this.connector = connector;
        this.streamExecutor = streamExecutor;
    }

    public static QuicBulkTransferServer bind(
            CxnFileServerRuntime runtime,
            KeyStore keyStore,
            char[] keyStorePassword
    ) throws IOException {
        if (runtime == null) {
            throw new IllegalArgumentException("runtime is required");
        }
        InetSocketAddress address = runtime.server().config().containerTransferBinding().address();
        CordPoolConfig cordPool = runtime.server().config().containerTransferBinding().cordPool()
                .orElse(CordPoolConfig.xetLikeDefaults());
        return bind(address, runtime.bulkTransferSessionHandler(), keyStore, keyStorePassword, cordPool.maxCords());
    }

    public static QuicBulkTransferServer bind(
            InetSocketAddress address,
            BulkTransferSessionHandler handler,
            KeyStore keyStore,
            char[] keyStorePassword,
            int maxCords
    ) throws IOException {
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler is required");
        }
        if (keyStore == null) {
            throw new IllegalArgumentException("keyStore is required");
        }
        if (maxCords <= 0 || maxCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("maxCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        char[] password = keyStorePassword == null ? new char[0] : keyStorePassword.clone();
        DatagramSocket socket = new DatagramSocket(null);
        boolean socketOwned = true;
        try {
            socket.bind(address);
            ExecutorService streamExecutor = Executors.newVirtualThreadPerTaskExecutor();
            QuicApplicationFactory protocolFactory = new QuicApplicationFactory(handler, streamExecutor, maxCords);
            ServerConnector connector = ServerConnector.builder()
                    .withSocket(socket)
                    .withKeyStore(keyStore, keyAlias(keyStore), password)
                    .withConfiguration(ServerConnectionConfig.builder()
                            .maxOpenPeerInitiatedBidirectionalStreams(maxConcurrentBidirectionalStreams(maxCords))
                            .maxOpenPeerInitiatedUnidirectionalStreams(0)
                            .maxBidirectionalStreamBufferSize(BIDIRECTIONAL_STREAM_BUFFER_BYTES)
                            .maxUnidirectionalStreamBufferSize(0)
                            .maxConnectionBufferSize(CONNECTION_BUFFER_BYTES)
                            .build())
                    .withLogger(new NullLogger())
                    .build();
            connector.registerApplicationProtocol(QuicBulkProtocol.APPLICATION_PROTOCOL, protocolFactory);
            QuicBulkTransferServer server = new QuicBulkTransferServer(socket, connector, streamExecutor);
            protocolFactory.attach(server);
            socketOwned = false;
            return server;
        } catch (CertificateException | KeyStoreException e) {
            throw new IOException("failed to prepare QUIC bulk listener", e);
        } finally {
            java.util.Arrays.fill(password, '\0');
            if (socketOwned) {
                socket.close();
            }
        }
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        connector.start();
    }

    @Override
    public InetSocketAddress boundAddress() {
        return new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort());
    }

    @Override
    public boolean running() {
        return running.get() && !socket.isClosed();
    }

    public int activeStreamCount() {
        return activeStreams.get();
    }

    public int maxActiveStreamCount() {
        return maxActiveStreams.get();
    }

    public int totalStreamCount() {
        return totalStreams.get();
    }

    public int streamSendBuffersTuned() {
        return streamSendBuffersTuned.get();
    }

    public int streamSendBufferTuneFailures() {
        return streamSendBufferTuneFailures.get();
    }

    @Override
    public OptionalLong totalBulkStreamCount() {
        return OptionalLong.of(totalStreams.get());
    }

    @Override
    public int activeConnectionCount() {
        return activeStreams.get();
    }

    @Override
    public Optional<IOException> lastAcceptError() {
        return Optional.empty();
    }

    @Override
    public Optional<IOException> lastConnectionError() {
        return lastStreamError();
    }

    public Optional<IOException> lastStreamError() {
        return Optional.ofNullable(lastStreamError.get());
    }

    public TlsCipherSuite cipherSuite() {
        return CIPHER_SUITE;
    }

    @Override
    public void awaitShutdown() {
    }

    @Override
    public void close() {
        running.set(false);
        connector.close();
        socket.close();
        streamExecutor.shutdownNow();
    }

    private void handleStream(BulkTransferSessionHandler handler, QuicStream stream) {
        int streamNumber = totalStreams.incrementAndGet();
        int active = activeStreams.incrementAndGet();
        maxActiveStreams.accumulateAndGet(active, Math::max);
        tuneStreamSendBuffer(stream);
        try (
                InputStream input = stream.getInputStream();
                OutputStream output = stream.getOutputStream()
        ) {
            handler.drainUntilEof(input, output);
            try {
                output.flush();
            } catch (IOException e) {
                if (!benignStreamClose(e)) {
                    throw e;
                }
            }
        } catch (IOException e) {
            if (running.get() && !benignStreamClose(e)) {
                lastStreamError.set(e);
                logStreamFailure(streamNumber, e);
            }
        } catch (RuntimeException e) {
            if (running.get()) {
                IOException wrapped = new IOException("QUIC bulk stream handler failed", e);
                lastStreamError.set(wrapped);
                logStreamFailure(streamNumber, wrapped);
            }
        } finally {
            activeStreams.decrementAndGet();
        }
    }

    private void tuneStreamSendBuffer(QuicStream stream) {
        if (QuicStreamSendBufferTuner.tune(stream, STREAM_SEND_BUFFER_BYTES)) {
            streamSendBuffersTuned.incrementAndGet();
        } else {
            streamSendBufferTuneFailures.incrementAndGet();
        }
    }

    private static void logStreamFailure(int streamNumber, IOException failure) {
        System.err.println("cxnfs-quic: stream failed stream=" + streamNumber
                + " error=" + failure.getClass().getSimpleName()
                + ": " + failure.getMessage());
        failure.printStackTrace(System.err);
    }

    private static boolean benignStreamClose(IOException failure) {
        String message = failure.getMessage();
        return message != null
                && (message.contains("output stream is reset")
                || message.contains("Connection closed")
                || message.contains("stream is closed"));
    }

    private static String keyAlias(KeyStore keyStore) throws KeyStoreException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return alias;
            }
        }
        throw new IllegalArgumentException("QUIC bulk listener keystore has no private key entry");
    }

    private static final class QuicApplicationFactory implements ApplicationProtocolConnectionFactory {
        private final BulkTransferSessionHandler handler;
        private final ExecutorService streamExecutor;
        private final int maxCords;
        private QuicBulkTransferServer server;

        private QuicApplicationFactory(
                BulkTransferSessionHandler handler,
                ExecutorService streamExecutor,
                int maxCords
        ) {
            this.handler = handler;
            this.streamExecutor = streamExecutor;
            this.maxCords = maxCords;
        }

        private void attach(QuicBulkTransferServer server) {
            this.server = server;
        }

        @Override
        public ApplicationProtocolConnection createConnection(String protocol, QuicConnection connection) {
            return new ApplicationProtocolConnection() {
                @Override
                public void acceptPeerInitiatedStream(QuicStream stream) {
                    streamExecutor.submit(() -> server.handleStream(handler, stream));
                }
            };
        }

        @Override
        public int maxConcurrentPeerInitiatedBidirectionalStreams() {
            return maxConcurrentBidirectionalStreams(maxCords);
        }

        @Override
        public long maxTotalPeerInitiatedBidirectionalStreams() {
            return Long.MAX_VALUE;
        }

        @Override
        public int minBidirectionalStreamReceiverBufferSize() {
            return 64 * 1024;
        }

        @Override
        public long maxBidirectionalStreamReceiverBufferSize() {
            return BIDIRECTIONAL_STREAM_BUFFER_BYTES;
        }

        @Override
        public int maxConcurrentPeerInitiatedUnidirectionalStreams() {
            return 0;
        }

        @Override
        public long maxTotalPeerInitiatedUnidirectionalStreams() {
            return 0;
        }
    }

    private static int maxConcurrentBidirectionalStreams(int maxCords) {
        return Math.min(CordPoolConfig.ABSOLUTE_MAX_CORDS + WRITE_ACK_STREAM_RESERVE, maxCords + WRITE_ACK_STREAM_RESERVE);
    }
}
