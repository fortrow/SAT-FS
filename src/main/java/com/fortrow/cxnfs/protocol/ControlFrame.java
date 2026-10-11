package com.fortrow.cxnfs.protocol;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

public record ControlFrame(
        ProtocolVersion version,
        UUID messageId,
        Optional<UUID> sessionId,
        ControlOperation operation,
        byte[] payload
) {
    public static ControlFrame fromEnvelope(ControlEnvelope<?> envelope, byte[] payload) {
        return new ControlFrame(
                envelope.version(),
                envelope.messageId(),
                envelope.sessionId(),
                envelope.operation(),
                payload
        );
    }

    public ControlFrame {
        if (version == null) {
            throw new IllegalArgumentException("version is required");
        }
        if (messageId == null) {
            throw new IllegalArgumentException("messageId is required");
        }
        sessionId = sessionId == null ? Optional.empty() : sessionId;
        if (operation == null) {
            throw new IllegalArgumentException("operation is required");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        payload = Arrays.copyOf(payload, payload.length);
    }

    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }
}
