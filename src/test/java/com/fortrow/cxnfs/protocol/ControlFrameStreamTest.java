package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.format.BinaryFormatException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ControlFrameStreamTest {
    @Test
    void readsAndWritesLengthPrefixedFrames() throws Exception {
        byte[] frame = "encoded-control-frame".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        ControlFrameStream.write(output, frame);

        Optional<byte[]> decoded = ControlFrameStream.read(new ByteArrayInputStream(output.toByteArray()));

        assertTrue(decoded.isPresent());
        assertArrayEquals(frame, decoded.orElseThrow());
    }

    @Test
    void treatsCleanEndOfStreamAsEmpty() throws Exception {
        assertTrue(ControlFrameStream.read(new ByteArrayInputStream(new byte[0])).isEmpty());
    }

    @Test
    void rejectsInvalidFrameLength() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(output)) {
            out.writeInt(ControlFrameStream.MAX_ENCODED_FRAME_BYTES + 1);
        }

        assertThrows(
                BinaryFormatException.class,
                () -> ControlFrameStream.read(new ByteArrayInputStream(output.toByteArray()))
        );
    }
}
