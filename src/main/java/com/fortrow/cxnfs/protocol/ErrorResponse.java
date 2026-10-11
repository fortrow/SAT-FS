package com.fortrow.cxnfs.protocol;

public record ErrorResponse(
        ResponseStatus status,
        String message
) implements ControlPayload {
    public ErrorResponse {
        if (status == null || status == ResponseStatus.OK) {
            throw new IllegalArgumentException("error status must be non-OK");
        }
        message = message == null ? "" : message;
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.ERROR;
    }
}
