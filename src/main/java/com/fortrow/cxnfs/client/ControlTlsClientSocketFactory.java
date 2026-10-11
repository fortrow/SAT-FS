package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.transport.TlsCipherSuite;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public final class ControlTlsClientSocketFactory extends SocketFactory {
    private static final String[] TLS_PROTOCOLS = {"TLSv1.3"};

    private final SSLSocketFactory delegate;
    private final TlsCipherSuite cipherSuite;
    private final boolean endpointIdentificationEnabled;

    public ControlTlsClientSocketFactory(SSLContext context) {
        this(context, TlsCipherSuite.TLS_AES_128_GCM_SHA256, true);
    }

    public ControlTlsClientSocketFactory(
            SSLContext context,
            TlsCipherSuite cipherSuite,
            boolean endpointIdentificationEnabled
    ) {
        if (context == null) {
            throw new IllegalArgumentException("context is required");
        }
        if (cipherSuite == null) {
            throw new IllegalArgumentException("cipherSuite is required");
        }
        this.delegate = context.getSocketFactory();
        this.cipherSuite = cipherSuite;
        this.endpointIdentificationEnabled = endpointIdentificationEnabled;
    }

    @Override
    public Socket createSocket() throws IOException {
        return configure(delegate.createSocket());
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return configure(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        return configure(delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return configure(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return configure(delegate.createSocket(address, port, localAddress, localPort));
    }

    public Socket createConnectedSocket(
            InetSocketAddress address,
            int connectTimeoutMillis,
            int readTimeoutMillis
    ) throws IOException {
        Socket rawSocket = new Socket();
        SSLSocket sslSocket = null;
        boolean wrapped = false;
        try {
            rawSocket.connect(address, connectTimeoutMillis);
            rawSocket.setTcpNoDelay(true);
            rawSocket.setSoTimeout(readTimeoutMillis);
            sslSocket = (SSLSocket) configure(delegate.createSocket(
                    rawSocket,
                    address.getHostString(),
                    address.getPort(),
                    true
            ));
            sslSocket.setSoTimeout(readTimeoutMillis);
            sslSocket.startHandshake();
            wrapped = true;
            return sslSocket;
        } finally {
            if (!wrapped) {
                if (sslSocket != null) {
                    sslSocket.close();
                } else {
                    rawSocket.close();
                }
            }
        }
    }

    private Socket configure(Socket socket) throws IOException {
        if (!(socket instanceof SSLSocket sslSocket)) {
            socket.close();
            throw new IOException("TLS client socket factory did not create an SSL socket");
        }
        sslSocket.setEnabledProtocols(TLS_PROTOCOLS);
        sslSocket.setEnabledCipherSuites(new String[]{cipherSuite.javaName()});
        if (endpointIdentificationEnabled) {
            SSLParameters parameters = sslSocket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslSocket.setSSLParameters(parameters);
        }
        return sslSocket;
    }
}
