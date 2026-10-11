package com.fortrow.cxnfs.client;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;

public final class ClientTlsContextFactory {
    public SSLContext createDefault() throws IOException {
        try {
            SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(null, null, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to initialize default client TLS context", e);
        }
    }

    public SSLContext createInsecureTrustAll() throws IOException {
        try {
            TrustManager[] trustManagers = {insecureTrustAllManager()};
            SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(null, trustManagers, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to initialize insecure client TLS context", e);
        }
    }

    public SSLContext create(Path trustStorePath, char[] trustStorePassword) throws IOException {
        if (trustStorePath == null) {
            return createDefault();
        }
        char[] password = trustStorePassword == null ? new char[0] : trustStorePassword.clone();
        try {
            KeyStore trustStore = KeyStore.getInstance(keyStoreType(trustStorePath));
            try (InputStream input = Files.newInputStream(trustStorePath)) {
                trustStore.load(input, password);
            }
            TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(null, trustManagers.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to load client TLS truststore: " + trustStorePath, e);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    public static X509TrustManager insecureTrustAllManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }

    private static String keyStoreType(Path path) {
        String name = path.getFileName() == null
                ? ""
                : path.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx")) {
            return "PKCS12";
        }
        if (name.endsWith(".jks")) {
            return "JKS";
        }
        return KeyStore.getDefaultType();
    }
}
