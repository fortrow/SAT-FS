package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.transport.TransportSecurityConfig;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

public final class ControlTlsContextFactory {
    public SSLContext create(TransportSecurityConfig security, char[] keyStorePassword) throws IOException {
        if (security == null) {
            throw new IllegalArgumentException("security is required");
        }
        Path keyStorePath = security.keyStore()
                .orElseThrow(() -> new IllegalArgumentException("control TLS requires a keystore"));
        char[] password = keyStorePassword == null ? new char[0] : keyStorePassword.clone();
        try {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType(keyStorePath));
            try (InputStream input = Files.newInputStream(keyStorePath)) {
                keyStore.load(input, password);
            }
            requirePrivateKeyEntry(keyStore, keyStorePath);
            KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(keyStore, password);
            SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(keyManagers.getKeyManagers(), null, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to load control TLS keystore: " + keyStorePath, e);
        } finally {
            java.util.Arrays.fill(password, '\0');
        }
    }

    private static void requirePrivateKeyEntry(KeyStore keyStore, Path keyStorePath)
            throws GeneralSecurityException, IOException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            Certificate[] chain = keyStore.getCertificateChain(alias);
            if (chain == null || chain.length == 0) {
                throw new IOException("control TLS keystore private key has no certificate chain: "
                        + keyStorePath + " alias=" + alias);
            }
            return;
        }
        throw new IOException("control TLS keystore has no private key entry: " + keyStorePath);
    }

    private static String keyStoreType(Path path) {
        String name = path.getFileName() == null ? "" : path.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx")) {
            return "PKCS12";
        }
        if (name.endsWith(".jks")) {
            return "JKS";
        }
        return KeyStore.getDefaultType();
    }
}
