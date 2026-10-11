package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.transport.TlsCipherSuite;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public final class ControlTlsServerSocketFactory extends ServerSocketFactory {
    private static final String[] TLS_PROTOCOLS = {"TLSv1.3"};

    private final SSLServerSocketFactory delegate;
    private final TlsCipherSuite cipherSuite;

    public ControlTlsServerSocketFactory(SSLContext context) {
        this(context, TlsCipherSuite.TLS_AES_128_GCM_SHA256);
    }

    public ControlTlsServerSocketFactory(SSLContext context, TlsCipherSuite cipherSuite) {
        if (context == null) {
            throw new IllegalArgumentException("context is required");
        }
        if (cipherSuite == null) {
            throw new IllegalArgumentException("cipherSuite is required");
        }
        this.delegate = context.getServerSocketFactory();
        this.cipherSuite = cipherSuite;
    }

    @Override
    public ServerSocket createServerSocket() throws IOException {
        return configure(delegate.createServerSocket());
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        return configure(delegate.createServerSocket(port));
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog) throws IOException {
        return configure(delegate.createServerSocket(port, backlog));
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
        return configure(delegate.createServerSocket(port, backlog, ifAddress));
    }

    private ServerSocket configure(ServerSocket socket) throws IOException {
        if (!(socket instanceof SSLServerSocket sslSocket)) {
            socket.close();
            throw new IOException("TLS server socket factory did not create an SSL server socket");
        }
        sslSocket.setEnabledProtocols(TLS_PROTOCOLS);
        sslSocket.setEnabledCipherSuites(new String[]{cipherSuite.javaName()});
        sslSocket.setNeedClientAuth(false);
        return sslSocket;
    }
}
