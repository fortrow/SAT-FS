package com.fortrow.cxnfs.transport;

public enum TlsCipherSuite {
    TLS_AES_128_GCM_SHA256("tls_aes_128_gcm_sha256", "TLS_AES_128_GCM_SHA256");

    private final String protocolName;
    private final String javaName;

    TlsCipherSuite(String protocolName, String javaName) {
        this.protocolName = protocolName;
        this.javaName = javaName;
    }

    public String protocolName() {
        return protocolName;
    }

    public String javaName() {
        return javaName;
    }
}
