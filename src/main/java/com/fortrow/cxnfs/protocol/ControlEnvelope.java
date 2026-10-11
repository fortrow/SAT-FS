package com.fortrow.cxnfs.protocol;

import java.util.Optional;
import java.util.UUID;

public record ControlEnvelope<T extends ControlPayload>(
        ProtocolVersion version,
        UUID messageId,
        Optional<UUID> sessionId,
        T payload
) {
    public static <T extends ControlPayload> ControlEnvelope<T> unauthenticated(T payload) {
        return new ControlEnvelope<>(ProtocolVersion.CURRENT, UUID.randomUUID(), Optional.empty(), payload);
    }

    public static <T extends ControlPayload> ControlEnvelope<T> authenticated(UUID sessionId, T payload) {
        return new ControlEnvelope<>(ProtocolVersion.CURRENT, UUID.randomUUID(), Optional.of(sessionId), payload);
    }

    public ControlEnvelope {
        if (version == null) {
            throw new IllegalArgumentException("version is required");
        }
        if (messageId == null) {
            throw new IllegalArgumentException("messageId is required");
        }
        sessionId = sessionId == null ? Optional.empty() : sessionId;
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
    }

    public ControlOperation operation() {
        return payload.operation();
    }
}
