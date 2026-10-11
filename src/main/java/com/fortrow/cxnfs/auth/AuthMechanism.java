package com.fortrow.cxnfs.auth;

import java.util.Locale;

public enum AuthMechanism {
    USERNAME_PASSWORD("username_password"),
    BEARER_TOKEN("bearer_token");

    private final String cliName;

    AuthMechanism(String cliName) {
        this.cliName = cliName;
    }

    public String cliName() {
        return cliName;
    }

    public static AuthMechanism parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("auth mechanism is required");
        }
        String normalized = value.toLowerCase(Locale.ROOT).replace("-", "_");
        for (AuthMechanism mechanism : values()) {
            if (mechanism.cliName.equals(normalized) || mechanism.name().toLowerCase(Locale.ROOT).equals(normalized)) {
                return mechanism;
            }
        }
        throw new IllegalArgumentException("unknown auth mechanism: " + value);
    }
}
