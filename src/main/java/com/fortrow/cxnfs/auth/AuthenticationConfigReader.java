package com.fortrow.cxnfs.auth;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class AuthenticationConfigReader {
    public AuthenticationPolicy read(Path configPath) throws IOException {
        if (configPath == null) {
            throw new IllegalArgumentException("configPath is required");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(configPath)) {
            properties.load(input);
        }
        return policy(properties);
    }

    AuthenticationPolicy policy(Properties properties) {
        Map<String, String> passwords = new LinkedHashMap<>();
        Map<String, List<String>> directoriesByUsername = new LinkedHashMap<>();
        List<String> bearerTokens = new ArrayList<>();

        String singleUsername = trimToNull(properties.getProperty("auth.username"));
        String singlePassword = trimToNull(properties.getProperty("auth.password"));
        if (singleUsername != null || singlePassword != null) {
            if (singleUsername == null || singlePassword == null) {
                throw new IllegalArgumentException("auth.username and auth.password must be configured together");
            }
            putPassword(passwords, singleUsername, singlePassword);
        }

        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("auth.user.") && name.endsWith(".password")) {
                String username = trimToNull(name.substring(
                        "auth.user.".length(),
                        name.length() - ".password".length()
                ));
                putPassword(passwords, username, properties.getProperty(name));
            } else if (name.startsWith("auth.user.") && name.endsWith(".directories")) {
                String username = trimToNull(name.substring(
                        "auth.user.".length(),
                        name.length() - ".directories".length()
                ));
                putDirectories(directoriesByUsername, username, properties.getProperty(name));
            } else if (name.equals("auth.bearer-token") || name.startsWith("auth.bearer-token.")) {
                String token = trimToNull(properties.getProperty(name));
                if (token != null) {
                    bearerTokens.add(token);
                }
            }
        }

        List<AuthMechanism> mechanisms = mechanisms(properties, passwords, bearerTokens);
        return AuthenticationPolicy.fromConfig(mechanisms, passwords, bearerTokens, directoriesByUsername);
    }

    private static List<AuthMechanism> mechanisms(
            Properties properties,
            Map<String, String> passwords,
            List<String> bearerTokens
    ) {
        String configured = trimToNull(properties.getProperty("auth.mechanisms"));
        if (configured == null) {
            List<AuthMechanism> derived = new ArrayList<>();
            if (!passwords.isEmpty()) {
                derived.add(AuthMechanism.USERNAME_PASSWORD);
            }
            if (!bearerTokens.isEmpty()) {
                derived.add(AuthMechanism.BEARER_TOKEN);
            }
            return derived;
        }

        List<AuthMechanism> parsed = new ArrayList<>();
        for (String token : configured.split(",")) {
            AuthMechanism mechanism = AuthMechanism.parse(token.trim());
            if (!parsed.contains(mechanism)) {
                parsed.add(mechanism);
            }
        }
        return parsed;
    }

    private static void putPassword(Map<String, String> passwords, String username, String password) {
        String normalizedUsername = trimToNull(username);
        String normalizedPassword = trimToNull(password);
        if (normalizedUsername == null) {
            throw new IllegalArgumentException("configured usernames must not be blank");
        }
        if (normalizedPassword == null) {
            throw new IllegalArgumentException("configured passwords must not be blank");
        }
        if (passwords.putIfAbsent(normalizedUsername, normalizedPassword) != null) {
            throw new IllegalArgumentException("duplicate configured username: " + normalizedUsername);
        }
    }

    private static void putDirectories(Map<String, List<String>> directoriesByUsername, String username, String value) {
        String normalizedUsername = trimToNull(username);
        if (normalizedUsername == null) {
            throw new IllegalArgumentException("configured usernames must not be blank");
        }
        String configured = trimToNull(value);
        if (configured == null) {
            throw new IllegalArgumentException("configured user directories must not be blank: " + normalizedUsername);
        }
        List<String> directories = new ArrayList<>();
        for (String token : configured.split(",")) {
            String directory = trimToNull(token);
            if (directory == null) {
                throw new IllegalArgumentException("configured user directories must not contain blank entries: "
                        + normalizedUsername);
            }
            directories.add(directory);
        }
        directoriesByUsername.put(normalizedUsername, List.copyOf(directories));
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
