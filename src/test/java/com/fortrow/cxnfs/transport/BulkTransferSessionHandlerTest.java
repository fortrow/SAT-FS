package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.server.CxnFileServerConfig;
import com.fortrow.cxnfs.store.CxnfsRepository;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BulkTransferSessionHandlerTest {
    @Test
    void writesAckAfterStagingBulkWriteFrame() throws Exception {
        BulkDataFrame frame = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000456"),
                3,
                2,
                7,
                4096,
                true,
                "checkpoint chunk".getBytes(StandardCharsets.UTF_8)
        );
        ByteArrayOutputStream inputBytes = new ByteArrayOutputStream();
        BulkFrameStream.write(inputBytes, BulkDataFrameCodec.encode(frame));
        ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        BulkTransferSessionHandler handler = new BulkTransferSessionHandler(
                readHandler(),
                value -> new StagedPayload(
                        value.key(),
                        value.cordIndex(),
                        value.entryOffset(),
                        value.payloadLength(),
                        value.finalFrameForChunk(),
                        StagedPayloadStorage.MEMORY,
                        Optional.empty(),
                        false
                )
        );

        List<BulkTransferDeliveryResult> results = handler.handleUntilEof(
                new ByteArrayInputStream(inputBytes.toByteArray()),
                outputBytes
        );

        assertEquals(1, results.size());
        BulkWriteDeliveryResult result = results.getFirst().write().orElseThrow();
        assertEquals(frame.key(), result.key());
        assertEquals(frame.payloadLength(), result.payloadLength());
        assertEquals(StagedPayloadStorage.MEMORY, result.storage());
        BulkWriteAck ack = BulkWriteAckCodec.decode(BulkWriteAckStream.read(
                new ByteArrayInputStream(outputBytes.toByteArray())
        ).orElseThrow());
        assertEquals(frame.key(), ack.key());
        assertEquals(frame.payloadLength(), ack.payloadLength());
        assertEquals(StagedPayloadStorage.MEMORY, ack.storage());
    }

    @Test
    void drainsBulkWriteFramesWithoutRetainingDeliveryResults() throws Exception {
        BulkDataFrame first = frame(1, "first chunk");
        BulkDataFrame second = frame(2, "second chunk");
        ByteArrayOutputStream inputBytes = new ByteArrayOutputStream();
        BulkFrameStream.write(inputBytes, BulkDataFrameCodec.encode(first));
        BulkFrameStream.write(inputBytes, BulkDataFrameCodec.encode(second));
        ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        AtomicInteger stagedFrames = new AtomicInteger();
        BulkTransferSessionHandler handler = new BulkTransferSessionHandler(
                readHandler(),
                value -> {
                    stagedFrames.incrementAndGet();
                    return new StagedPayload(
                            value.key(),
                            value.cordIndex(),
                            value.entryOffset(),
                            value.payloadLength(),
                            value.finalFrameForChunk(),
                            StagedPayloadStorage.MEMORY,
                            Optional.empty(),
                            false
                    );
                }
        );

        handler.drainUntilEof(new ByteArrayInputStream(inputBytes.toByteArray()), outputBytes);

        assertEquals(2, stagedFrames.get());
        ByteArrayInputStream acks = new ByteArrayInputStream(outputBytes.toByteArray());
        assertEquals(first.key(), BulkWriteAckCodec.decode(BulkWriteAckStream.read(acks).orElseThrow()).key());
        assertEquals(second.key(), BulkWriteAckCodec.decode(BulkWriteAckStream.read(acks).orElseThrow()).key());
        assertTrue(BulkWriteAckStream.read(acks).isEmpty());
    }

    private static BulkReadSessionHandler readHandler() {
        CxnFileServer server = new CxnFileServer(config(), new CxnfsRepository(data -> List.of()));
        return new BulkReadSessionHandler(new BulkReadTransferCoordinator(server, ignored -> Optional.empty()));
    }

    private static BulkDataFrame frame(long chunkSequence, String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        return new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000456"),
                (int) chunkSequence,
                2,
                chunkSequence,
                chunkSequence * 4096,
                true,
                bytes
        );
    }

    private static CxnFileServerConfig config() {
        Path keyStore = Path.of("cxnfs-test-server.p12");
        return new CxnFileServerConfig(
                Path.of("."),
                AuthenticationPolicy.requireUsernamePassword(Map.of("trainer", "secret")),
                new TransportBinding(
                        TransportRole.FILE_SERVER_CONTROL,
                        new InetSocketAddress("127.0.0.1", 4509),
                        TransportSecurityConfig.tls13ForTcp(keyStore)
                ),
                new TransportBinding(
                        TransportRole.CONTAINER_TRANSFER,
                        new InetSocketAddress("127.0.0.1", 4510),
                        TransportSecurityConfig.quicTls13ForUdp(keyStore)
                )
        );
    }
}
