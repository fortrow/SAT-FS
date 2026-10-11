package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.client.ClientAuthenticator;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.server.ControlSessionDispatcher;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.server.CxnFileServerConfig;
import com.fortrow.cxnfs.store.CxnfsRepository;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BulkReadSessionHandlerTest {
    @Test
    void handlesOneReadRequestAndWritesBulkFramesToTheOutputStream() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted controlSession = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        bootstrapRead(dispatcher, controlSession, transferId, fixture.manifest());
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        BulkReadRequest request = new BulkReadRequest(
                controlSession.sessionId(),
                transferId,
                "/datasets/train/shard.00000.mds",
                5,
                15,
                4
        );
        BulkReadRequestStream.write(requestBytes, BulkReadRequestCodec.encode(request));
        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();

        BulkReadSessionHandler handler = new BulkReadSessionHandler(
                new BulkReadTransferCoordinator(fixture.server(), dispatcher::admittedReadTransfer)
        );
        BulkReadDeliveryResult result = handler
                .handleOnce(new ByteArrayInputStream(requestBytes.toByteArray()), responseBytes)
                .orElseThrow();

        assertEquals(transferId, result.transferId());
        assertEquals(4, result.activeCords());
        assertEquals(3, result.frameCount());
        ByteArrayInputStream responseInput = new ByteArrayInputStream(responseBytes.toByteArray());
        BulkDataFrame first = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        BulkDataFrame second = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        BulkDataFrame third = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        assertTrue(BulkFrameStream.read(responseInput).isEmpty());
        assertEquals(0, first.cordIndex());
        assertEquals(1, second.cordIndex());
        assertEquals(2, third.cordIndex());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), first.payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), second.payload());
        assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), third.payload());
    }

    @Test
    void returnsEmptyWhenNoRequestIsAvailable() throws Exception {
        BulkReadSessionHandler handler = new BulkReadSessionHandler(
                new BulkReadTransferCoordinator(fixture().server(), transferId -> java.util.Optional.empty())
        );

        assertTrue(handler.handleOnce(new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream()).isEmpty());
    }

    @Test
    void handlesRepeatedReadRequestsUntilEndOfStream() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted controlSession = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        bootstrapRead(dispatcher, controlSession, transferId, fixture.manifest());
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        BulkReadRequest first = new BulkReadRequest(
                controlSession.sessionId(),
                transferId,
                "/datasets/train/shard.00000.mds",
                0,
                8,
                2
        );
        BulkReadRequest second = new BulkReadRequest(
                controlSession.sessionId(),
                transferId,
                "/datasets/train/shard.00000.mds",
                8,
                8,
                2
        );
        BulkReadRequestStream.write(requestBytes, BulkReadRequestCodec.encode(first));
        BulkReadRequestStream.write(requestBytes, BulkReadRequestCodec.encode(second));
        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
        BulkReadSessionHandler handler = new BulkReadSessionHandler(
                new BulkReadTransferCoordinator(fixture.server(), dispatcher::admittedReadTransfer)
        );

        List<BulkReadDeliveryResult> results = handler.handleUntilEof(
                new ByteArrayInputStream(requestBytes.toByteArray()),
                responseBytes
        );

        assertEquals(2, results.size());
        assertEquals(1, results.get(0).frameCount());
        assertEquals(1, results.get(1).frameCount());
        ByteArrayInputStream responseInput = new ByteArrayInputStream(responseBytes.toByteArray());
        BulkDataFrame firstFrame = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        BulkDataFrame secondFrame = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        assertTrue(BulkFrameStream.read(responseInput).isEmpty());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), firstFrame.payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), secondFrame.payload());
    }

    private static void bootstrapRead(
            ControlSessionDispatcher dispatcher,
            AuthAccepted accepted,
            UUID transferId,
            FileManifest manifest
    ) {
        dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.READ,
                        "/datasets/train/shard.00000.mds",
                        TransferPayloadFormat.MDS_SHARDS,
                        1,
                        64,
                        64 * 1024 * 1024,
                        manifest.size()
                )
        ));
    }

    private static AuthAccepted authenticate(ControlSessionDispatcher dispatcher) {
        ClientAuthenticator authenticator = new ClientAuthenticator();
        try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
            ControlPayload challenge = dispatcher
                    .handle(ControlEnvelope.unauthenticated(authenticator.start(credentials)))
                    .payload();
            return (AuthAccepted) dispatcher
                    .handle(ControlEnvelope.unauthenticated(authenticator.answer((AuthChallenge) challenge, credentials)))
                    .payload();
        }
    }

    private static Fixture fixture() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(BulkReadSessionHandlerTest::fixedEightByteChunks);
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        return new Fixture(new CxnFileServer(config(), repository), manifest);
    }

    private static List<Chunk> fixedEightByteChunks(byte[] data) {
        List<Chunk> chunks = new ArrayList<>();
        for (int offset = 0; offset < data.length; offset += 8) {
            byte[] chunk = Arrays.copyOfRange(data, offset, Math.min(data.length, offset + 8));
            chunks.add(new Chunk(Hashing.chunk(chunk), offset, chunk));
        }
        return chunks;
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

    private record Fixture(CxnFileServer server, FileManifest manifest) {
    }
}
