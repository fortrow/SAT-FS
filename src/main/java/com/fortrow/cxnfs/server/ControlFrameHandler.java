package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlFrame;
import com.fortrow.cxnfs.protocol.ControlFrameCodec;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.ControlPayloadCodec;

import java.io.IOException;

public final class ControlFrameHandler {
    private final ControlSessionDispatcher dispatcher;

    public ControlFrameHandler(ControlSessionDispatcher dispatcher) {
        if (dispatcher == null) {
            throw new IllegalArgumentException("dispatcher is required");
        }
        this.dispatcher = dispatcher;
    }

    public byte[] handle(byte[] frameBytes) throws IOException {
        ControlFrame frame = ControlFrameCodec.decode(frameBytes);
        ControlPayload payload = ControlPayloadCodec.decode(frame.operation(), frame.payload());
        ControlEnvelope<ControlPayload> request = new ControlEnvelope<>(
                frame.version(),
                frame.messageId(),
                frame.sessionId(),
                payload
        );
        ControlEnvelope<ControlPayload> response = dispatcher.handle(request);
        byte[] responsePayload = ControlPayloadCodec.encode(response.payload());
        return ControlFrameCodec.encode(ControlFrame.fromEnvelope(response, responsePayload));
    }
}
