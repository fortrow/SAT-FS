package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.format.BinaryFormatException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class BulkFrameStreamTest {
    @Test
    void writesAndReadsLengthPrefixedBulkFrames() throws Exception {
        BulkDataFrame frame = new BulkDataFrame(
                UUID.randomUUID(),
                3,
                2,
                5,
                4096,
                true,
                new byte[]{1, 2, 3, 4}
        );
        byte[] encoded = BulkDataFrameCodec.encode(frame);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        BulkFrameStream.write(output, encoded);

        Optional<byte[]> decoded = BulkFrameStream.read(new ByteArrayInputStream(output.toByteArray()));
        assertArrayEquals(encoded, decoded.orElseThrow());
    }

    @Test
    void returnsEmptyAtEndOfStream() throws Exception {
        assertEquals(Optional.empty(), BulkFrameStream.read(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    void rejectsInvalidEncodedFrameLengths() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        new DataOutputStream(output).writeInt(BulkFrameStream.MAX_ENCODED_FRAME_BYTES + 1);

        assertThrows(
                BinaryFormatException.class,
                () -> BulkFrameStream.read(new ByteArrayInputStream(output.toByteArray()))
        );
        assertThrows(BinaryFormatException.class, () -> BulkFrameStream.write(new ByteArrayOutputStream(), new byte[0]));
    }
}
