package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.format.BinaryFormatException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ControlFrameCodecTest {
    @Test
    void encodesAndDecodesControlFrameHeaderAndPayload() throws Exception {
        UUID messageId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();
        byte[] payload = "payload bytes".getBytes(StandardCharsets.UTF_8);
        ControlFrame frame = new ControlFrame(
                ProtocolVersion.CURRENT,
                messageId,
                Optional.of(sessionId),
                ControlOperation.LIST_DIRECTORY,
                payload
        );

        ControlFrame decoded = ControlFrameCodec.decode(ControlFrameCodec.encode(frame));

        assertEquals(ProtocolVersion.CURRENT, decoded.version());
        assertEquals(messageId, decoded.messageId());
        assertEquals(Optional.of(sessionId), decoded.sessionId());
        assertEquals(ControlOperation.LIST_DIRECTORY, decoded.operation());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void keepsOperationCodesStable() {
        assertEquals(1, ControlOperation.AUTH_START.code());
        assertEquals(2, ControlOperation.AUTH_ACCEPTED.code());
        assertEquals(3, ControlOperation.AUTH_CHALLENGE.code());
        assertEquals(4, ControlOperation.AUTH_PROOF.code());
        assertEquals(10, ControlOperation.LIST_DIRECTORY.code());
        assertEquals(12, ControlOperation.LIST_DIRECTORY_RESULT.code());
        assertEquals(13, ControlOperation.STAT_PATH_RESULT.code());
        assertEquals(42, ControlOperation.READ_FILE_RANGE.code());
        assertEquals(43, ControlOperation.READ_FILE_RANGE_RESULT.code());
        assertEquals(50, ControlOperation.READ_CONTAINER_RANGE.code());
        assertEquals(100, ControlOperation.ERROR.code());
    }

    @Test
    void rejectsBadControlFrameMagic() throws Exception {
        ControlFrame frame = new ControlFrame(
                ProtocolVersion.CURRENT,
                UUID.randomUUID(),
                Optional.empty(),
                ControlOperation.AUTH_START,
                new byte[0]
        );
        byte[] encoded = ControlFrameCodec.encode(frame);
        encoded[0] = 'X';

        assertThrows(BinaryFormatException.class, () -> ControlFrameCodec.decode(encoded));
    }
}
