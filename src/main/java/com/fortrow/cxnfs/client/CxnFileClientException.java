package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.protocol.ResponseStatus;

import java.io.IOException;

public final class CxnFileClientException extends IOException {
    private final ResponseStatus status;

    public CxnFileClientException(ResponseStatus status, String message) {
        super(message == null || message.isBlank()
                ? "control request failed with status " + status
                : message);
        if (status == null || status == ResponseStatus.OK) {
            throw new IllegalArgumentException("non-OK status is required");
        }
        this.status = status;
    }

    public ResponseStatus status() {
        return status;
    }
}
