package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.client.ClientAuthenticator;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlFrame;
import com.fortrow.cxnfs.protocol.ControlFrameCodec;
import com.fortrow.cxnfs.protocol.ControlFrameStream;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.ControlPayloadCodec;
import com.fortrow.cxnfs.protocol.ReadFileRangeRequest;
import com.fortrow.cxnfs.protocol.ReadFileRangeResponse;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import org.junit.jupiter.api.Test;

import javax.net.ServerSocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ControlTcpServerTest {
    @Test
    void servesLengthPrefixedControlFramesOverLoopback() throws Exception {
        Fixture fixture = fixture();
        try (ControlTcpServer server = ControlTcpServer.bind(fixture.server(), ServerSocketFactory.getDefault())) {
            server.start();

            try (Socket socket = new Socket()) {
                socket.connect(server.boundAddress());
                socket.setSoTimeout(5_000);
                assertTrue(server.running());
                assertEquals(1, waitForActiveConnections(server));

                ClientAuthenticator authenticator = new ClientAuthenticator();
                try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
                    DecodedFrame challenge = exchange(
                            socket,
                            ControlEnvelope.unauthenticated(authenticator.start(credentials))
                    );
                    DecodedFrame auth = exchange(
                            socket,
                            ControlEnvelope.unauthenticated(authenticator.answer((AuthChallenge) challenge.payload(), credentials))
                    );
                    AuthAccepted accepted = (AuthAccepted) auth.payload();
                    DecodedFrame range = exchange(
                            socket,
                            ControlEnvelope.authenticated(
                                    accepted.sessionId(),
                                    new ReadFileRangeRequest("/datasets/train/shard.mds", 13, 256)
                            )
                    );

                    assertTrue(challenge.frame().sessionId().isEmpty());
                    assertEquals(Optional.of(accepted.sessionId()), auth.frame().sessionId());
                    assertEquals(Optional.of(accepted.sessionId()), range.frame().sessionId());
                    ReadFileRangeResponse response = (ReadFileRangeResponse) range.payload();
                    assertEquals(fixture.manifest().fileHash(), response.plan().fileHash());
                    assertEquals(13, response.plan().requestedRange().offset());
                    assertEquals(256, response.plan().requestedRange().length());
                }
                assertTrue(server.lastAcceptError().isEmpty());
                assertTrue(server.lastConnectionError().isEmpty());
            }
        }
    }

    @Test
    void recordsRuntimeControlHandlerFailures() throws Exception {
        Fixture fixture = fixture();
        try (ControlTcpServer server = ControlTcpServer.bind(fixture.server(), ServerSocketFactory.getDefault())) {
            server.start();

            try (Socket socket = new Socket()) {
                socket.connect(server.boundAddress());
                socket.setSoTimeout(5_000);

                ControlFrameStream.write(socket.getOutputStream(), invalidOperationFrame());
                IOException error = waitForConnectionError(server);

                assertTrue(error.getMessage().contains("control connection handler failed"));
                assertTrue(error.getCause() instanceof IllegalArgumentException);
            }
        }
    }

    private static DecodedFrame exchange(Socket socket, ControlEnvelope<? extends ControlPayload> envelope)
            throws Exception {
        byte[] payload = ControlPayloadCodec.encode(envelope.payload());
        byte[] frame = ControlFrameCodec.encode(ControlFrame.fromEnvelope(envelope, payload));
        ControlFrameStream.write(socket.getOutputStream(), frame);
        byte[] responseBytes = ControlFrameStream.read(socket.getInputStream()).orElseThrow();
        ControlFrame responseFrame = ControlFrameCodec.decode(responseBytes);
        ControlPayload responsePayload = ControlPayloadCodec.decode(responseFrame.operation(), responseFrame.payload());
        return new DecodedFrame(responseFrame, responsePayload);
    }

    private static IOException waitForConnectionError(ControlTcpServer server) throws InterruptedException {
        for (int attempt = 0; attempt < 50; attempt++) {
            Optional<IOException> error = server.lastConnectionError();
            if (error.isPresent()) {
                return error.orElseThrow();
            }
            Thread.sleep(20);
        }
        throw new AssertionError("server did not record the control connection error");
    }

    private static int waitForActiveConnections(ControlTcpServer server) throws InterruptedException {
        for (int attempt = 0; attempt < 50; attempt++) {
            int connections = server.activeConnectionCount();
            if (connections > 0) {
                return connections;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("server did not record the active control connection");
    }

    private static byte[] invalidOperationFrame() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.write("CXNFSPRO".getBytes(StandardCharsets.US_ASCII));
            out.writeInt(1);
            out.writeInt(0);
            out.writeInt(1);
            UUID messageId = UUID.fromString("00000000-0000-0000-0000-00000000ffff");
            out.writeLong(messageId.getMostSignificantBits());
            out.writeLong(messageId.getLeastSignificantBits());
            out.writeBoolean(false);
            out.writeInt(999_999);
            out.writeInt(0);
        }
        return bytes.toByteArray();
    }

    private static Fixture fixture() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        byte[] data = "tcp control shard data\n".repeat(4096).getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.mds", data);
        return new Fixture(new CxnFileServer(config(), repository), manifest);
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

    private record DecodedFrame(ControlFrame frame, ControlPayload payload) {
    }
}
