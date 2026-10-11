package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;

import java.util.List;
import java.util.UUID;

public record AuthAccepted(
        UUID sessionId,
        List<AuthMechanism> mechanisms
) implements ControlPayload {
    public AuthAccepted {
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        mechanisms = mechanisms == null ? List.of() : List.copyOf(mechanisms);
        if (mechanisms.isEmpty()) {
            throw new IllegalArgumentException("at least one accepted mechanism is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.AUTH_ACCEPTED;
    }
}
