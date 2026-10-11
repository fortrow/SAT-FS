package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;

public record AuthStart(
        AuthMechanism mechanism,
        String principal
) implements ControlPayload {
    public AuthStart {
        if (mechanism == null) {
            throw new IllegalArgumentException("mechanism is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.AUTH_START;
    }
}
