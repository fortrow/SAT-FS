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

final class BulkReadRequestCodecTest {
    @Test
    void encodesAndDecodesBulkReadRequests() throws Exception {
        BulkReadRequest request = new BulkReadRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "datasets/train/shard.00000.mds",
                128,
                4096,
                16
        );

        BulkReadRequest decoded = BulkReadRequestCodec.decode(BulkReadRequestCodec.encode(request));

        assertEquals(request.sessionId(), decoded.sessionId());
        assertEquals(request.transferId(), decoded.transferId());
        assertEquals("/datasets/train/shard.00000.mds", decoded.path());
        assertEquals(128, decoded.offset());
        assertEquals(4096, decoded.length());
        assertEquals(16, decoded.activeCords());
    }

    @Test
    void rejectsTraversalSegmentsBeforeBulkReadsAreQueued() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BulkReadRequest(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        "/datasets/train/../shard.00000.mds",
                        128,
                        4096,
                        16
                )
        );
    }

    @Test
    void writesAndReadsLengthPrefixedBulkReadRequests() throws Exception {
        BulkReadRequest request = new BulkReadRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "/datasets/train/shard.00000.mds",
                0,
                8192,
                8
        );
        byte[] encoded = BulkReadRequestCodec.encode(request);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        BulkReadRequestStream.write(output, encoded);

        assertEquals(encoded.length + Integer.BYTES, output.size());
        assertArrayEquals(encoded, BulkReadRequestStream.read(new ByteArrayInputStream(output.toByteArray())).orElseThrow());
        assertEquals(Optional.empty(), BulkReadRequestStream.read(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    void rejectsInvalidRequestLengthsAndFields() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        new DataOutputStream(output).writeInt(BulkReadRequestStream.MAX_ENCODED_REQUEST_BYTES + 1);

        assertThrows(
                BinaryFormatException.class,
                () -> BulkReadRequestStream.read(new ByteArrayInputStream(output.toByteArray()))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new BulkReadRequest(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        "/datasets/train/shard.00000.mds",
                        0,
                        8,
                        CordPoolConfig.ABSOLUTE_MAX_CORDS + 1
                )
        );
        assertThrows(
                BinaryFormatException.class,
                () -> BulkReadRequestStream.write(new ByteArrayOutputStream(), new byte[0])
        );
    }
}
