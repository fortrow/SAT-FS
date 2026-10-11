package com.fortrow.cxnfs.server;

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
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkFrameStream;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkReadRequestCodec;
import com.fortrow.cxnfs.transport.BulkReadRequestStream;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class CxnFileServerRuntimeTest {
    @Test
    void sharesControlAdmissionsWithBulkReadSessions() throws Exception {
        Fixture fixture = fixture();
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());
        AuthAccepted accepted = authenticate(runtime.controlDispatcher());
        UUID transferId = UUID.randomUUID();
        bootstrapRead(runtime.controlDispatcher(), accepted, transferId, fixture.manifest());
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        BulkReadRequestStream.write(
                requestBytes,
                BulkReadRequestCodec.encode(new BulkReadRequest(
                        accepted.sessionId(),
                        transferId,
                        "/datasets/train/shard.00000.mds",
                        0,
                        16,
                        2
                ))
        );
        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();

        runtime.bulkReadSessionHandler().handleOnce(
                new ByteArrayInputStream(requestBytes.toByteArray()),
                responseBytes
        );

        ByteArrayInputStream responseInput = new ByteArrayInputStream(responseBytes.toByteArray());
        BulkDataFrame first = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        BulkDataFrame second = BulkDataFrameCodec.decode(BulkFrameStream.read(responseInput).orElseThrow());
        assertEquals(transferId, first.transferId());
        assertEquals(transferId, second.transferId());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), first.payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), second.payload());
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

    private static Fixture fixture() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnFileServerRuntimeTest::fixedEightByteChunks);
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        return new Fixture(new CxnFileServer(config(), repository), manifest);
    }

    private static java.util.List<Chunk> fixedEightByteChunks(byte[] data) {
        java.util.List<Chunk> chunks = new ArrayList<>();
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
                        new InetSocketAddress("127.0.0.1", 0),
                        TransportSecurityConfig.tls13ForTcp(keyStore)
                ),
                new TransportBinding(
                        TransportRole.CONTAINER_TRANSFER,
                        new InetSocketAddress("127.0.0.1", 0),
                        TransportSecurityConfig.quicTls13ForUdp(keyStore)
                )
        );
    }

    private record Fixture(CxnFileServer server, FileManifest manifest) {
    }
}
