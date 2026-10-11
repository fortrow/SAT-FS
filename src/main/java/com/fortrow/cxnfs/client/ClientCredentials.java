package com.fortrow.cxnfs.client;

import java.util.Arrays;

public final class ClientCredentials implements AutoCloseable {
    private final String username;
    private final char[] password;

    public ClientCredentials(String username, char[] password) {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username is required");
        }
        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("password is required");
        }
        this.username = username;
        this.password = password.clone();
    }

    public String username() {
        return username;
    }

    public char[] password() {
        return password.clone();
    }

    @Override
    public void close() {
        Arrays.fill(password, '\0');
    }

    @Override
    public String toString() {
        return "ClientCredentials[username=" + username + ", password=<hidden>]";
    }
}
