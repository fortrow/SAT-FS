package com.fortrow.cxnfs.auth;

import com.fortrow.cxnfs.namespace.NamespacePath;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record AuthenticationPolicy(
        List<AuthMechanism> acceptedMechanisms,
        Map<String, String> passwordByUsername,
        List<String> bearerTokens,
        Map<String, List<String>> directoriesByUsername
) {
    public AuthenticationPolicy(List<AuthMechanism> acceptedMechanisms) {
        this(acceptedMechanisms, Map.of(), List.of(), Map.of());
    }

    public static AuthenticationPolicy requireUsernamePassword(Map<String, String> passwordByUsername) {
        return new AuthenticationPolicy(List.of(AuthMechanism.USERNAME_PASSWORD), passwordByUsername, List.of(), Map.of());
    }

    public static AuthenticationPolicy requireUsernamePassword(
            Map<String, String> passwordByUsername,
            Map<String, List<String>> directoriesByUsername
    ) {
        return new AuthenticationPolicy(
                List.of(AuthMechanism.USERNAME_PASSWORD),
                passwordByUsername,
                List.of(),
                directoriesByUsername
        );
    }

    public static AuthenticationPolicy fromConfig(
            List<AuthMechanism> acceptedMechanisms,
            Map<String, String> passwordByUsername,
            List<String> bearerTokens
    ) {
        return new AuthenticationPolicy(acceptedMechanisms, passwordByUsername, bearerTokens, Map.of());
    }

    public static AuthenticationPolicy fromConfig(
            List<AuthMechanism> acceptedMechanisms,
            Map<String, String> passwordByUsername,
            List<String> bearerTokens,
            Map<String, List<String>> directoriesByUsername
    ) {
        return new AuthenticationPolicy(acceptedMechanisms, passwordByUsername, bearerTokens, directoriesByUsername);
    }

    public AuthenticationPolicy {
        acceptedMechanisms = acceptedMechanisms == null ? List.of() : List.copyOf(acceptedMechanisms);
        passwordByUsername = passwordByUsername == null ? Map.of() : Map.copyOf(passwordByUsername);
        bearerTokens = bearerTokens == null ? List.of() : List.copyOf(bearerTokens);
        directoriesByUsername = normalizeDirectories(directoriesByUsername);
        if (acceptedMechanisms.isEmpty()) {
            throw new IllegalArgumentException("CXNFS requires at least one authentication mechanism");
        }
        if (acceptedMechanisms.contains(AuthMechanism.USERNAME_PASSWORD) && passwordByUsername.isEmpty()) {
            throw new IllegalArgumentException("username/password authentication requires at least one configured user");
        }
        if (acceptedMechanisms.contains(AuthMechanism.BEARER_TOKEN) && bearerTokens.isEmpty()) {
            throw new IllegalArgumentException("bearer token authentication requires at least one configured token");
        }
        if (passwordByUsername.keySet().stream().anyMatch(username -> username == null || username.isBlank())) {
            throw new IllegalArgumentException("configured usernames must not be blank");
        }
        if (passwordByUsername.values().stream().anyMatch(password -> password == null || password.isBlank())) {
            throw new IllegalArgumentException("configured passwords must not be blank");
        }
        if (bearerTokens.stream().anyMatch(token -> token == null || token.isBlank())) {
            throw new IllegalArgumentException("configured bearer tokens must not be blank");
        }
        for (String username : directoriesByUsername.keySet()) {
            if (!passwordByUsername.containsKey(username)) {
                throw new IllegalArgumentException("directories configured for unknown user: " + username);
            }
        }
    }

    public boolean accepts(AuthMechanism mechanism) {
        return acceptedMechanisms.contains(mechanism);
    }

    public boolean authenticateUsernamePasswordProof(
            String username,
            byte[] nonce,
            int iterations,
            byte[] proof
    ) {
        return accepts(AuthMechanism.USERNAME_PASSWORD)
                && PasswordAuthProof.verify(passwordByUsername.get(username), nonce, iterations, proof);
    }

    public boolean authenticateBearerToken(String token) {
        return accepts(AuthMechanism.BEARER_TOKEN) && bearerTokens.contains(token);
    }

    public boolean canAccessPath(String username, String path) {
        if (!passwordByUsername.containsKey(username)) {
            return false;
        }
        List<String> directories = directoriesByUsername.get(username);
        if (directories == null) {
            return true;
        }
        String normalizedPath = NamespacePath.normalize(path);
        return directories.stream().anyMatch(directory -> isUnderDirectory(directory, normalizedPath));
    }

    public boolean canTraversePath(String username, String path) {
        if (!passwordByUsername.containsKey(username)) {
            return false;
        }
        List<String> directories = directoriesByUsername.get(username);
        if (directories == null) {
            return true;
        }
        String normalizedPath = NamespacePath.normalizeDirectory(path);
        return directories.stream().anyMatch(directory ->
                isUnderDirectory(directory, normalizedPath) || isAncestor(normalizedPath, directory));
    }

    private static Map<String, List<String>> normalizeDirectories(Map<String, List<String>> directoriesByUsername) {
        if (directoriesByUsername == null || directoriesByUsername.isEmpty()) {
            return Map.of();
        }
        Map<String, List<String>> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : directoriesByUsername.entrySet()) {
            String username = entry.getKey();
            if (username == null || username.isBlank()) {
                throw new IllegalArgumentException("configured usernames must not be blank");
            }
            List<String> directories = entry.getValue() == null ? List.of() : entry.getValue().stream()
                    .map(NamespacePath::normalizeDirectory)
                    .distinct()
                    .toList();
            if (directories.isEmpty()) {
                throw new IllegalArgumentException("configured user directories must not be blank: " + username);
            }
            normalized.put(username, List.copyOf(directories));
        }
        return Map.copyOf(normalized);
    }

    private static boolean isUnderDirectory(String directory, String path) {
        return directory.equals("/") || path.equals(directory) || path.startsWith(directory + "/");
    }

    private static boolean isAncestor(String ancestor, String path) {
        return ancestor.equals("/") || path.equals(ancestor) || path.startsWith(ancestor + "/");
    }
}
