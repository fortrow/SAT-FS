package com.fortrow.cxnfs.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BulkReadSessionHandler {
    private final BulkReadTransferCoordinator coordinator;

    public BulkReadSessionHandler(BulkReadTransferCoordinator coordinator) {
        if (coordinator == null) {
            throw new IllegalArgumentException("coordinator is required");
        }
        this.coordinator = coordinator;
    }

    public Optional<BulkReadDeliveryResult> handleOnce(InputStream input, OutputStream output) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        Optional<byte[]> requestBytes = BulkReadRequestStream.read(input);
        if (requestBytes.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(handleEncodedRequest(requestBytes.orElseThrow(), output));
    }

    public BulkReadDeliveryResult handleEncodedRequest(byte[] requestBytes, OutputStream output) throws IOException {
        if (requestBytes == null) {
            throw new IllegalArgumentException("requestBytes is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }

        BulkReadRequest request = BulkReadRequestCodec.decode(requestBytes);
        BulkReadDeliveryResult result = coordinator.deliver(
                request.sessionId(),
                request.transferId(),
                request.path(),
                request.offset(),
                request.length(),
                request.activeCords(),
                new BulkFrameEncodingSink(new BulkFrameStreamSink(output))
        );
        return result;
    }

    public List<BulkReadDeliveryResult> handleUntilEof(InputStream input, OutputStream output) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }

        List<BulkReadDeliveryResult> results = new ArrayList<>();
        while (true) {
            Optional<BulkReadDeliveryResult> result = handleOnce(input, output);
            if (result.isEmpty()) {
                return List.copyOf(results);
            }
            results.add(result.orElseThrow());
        }
    }

    public void drainUntilEof(InputStream input, OutputStream output) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }

        while (handleOnce(input, output).isPresent()) {
            // Drain the protected bulk stream without retaining per-request results.
        }
    }
}
