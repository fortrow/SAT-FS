package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.client.ClientAuthenticator;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlFrame;
import com.fortrow.cxnfs.protocol.ControlFrameCodec;
import com.fortrow.cxnfs.protocol.ControlFrameStream;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.ControlPayloadCodec;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.storage.LinuxBlockfsFile;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestEntry;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceStore;
import com.fortrow.cxnfs.storage.MountedBlockfsStore;
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
import org.junit.jupiter.api.io.TempDir;

import javax.net.ServerSocketFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ControlAndBulkRuntimeIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void controlAdmissionOverLoopbackFeedsTheBulkReadStream() throws Exception {
        Fixture fixture = fixture();
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             BulkReadStreamServer bulk = BulkReadStreamServer.bind(
                     new InetSocketAddress("127.0.0.1", 0),
                     runtime.bulkReadSessionHandler(),
                     ServerSocketFactory.getDefault()
             )) {
            control.start();
            bulk.start();

            ControlBootstrap accepted = bootstrapReadOverControl(control.boundAddress(), fixture.manifest());

            try (Socket bulkSocket = new Socket()) {
                bulkSocket.connect(bulk.boundAddress());
                bulkSocket.setSoTimeout(5_000);
                BulkReadRequestStream.write(
                        bulkSocket.getOutputStream(),
                        BulkReadRequestCodec.encode(new BulkReadRequest(
                                accepted.sessionId(),
                                accepted.transfer().transferId(),
                                "/datasets/train/shard.00000.mds",
                                5,
                                15,
                                accepted.transfer().acceptedInitialCords()
                        ))
                );

                BulkDataFrame first = BulkDataFrameCodec.decode(BulkFrameStream.read(bulkSocket.getInputStream()).orElseThrow());
                BulkDataFrame second = BulkDataFrameCodec.decode(BulkFrameStream.read(bulkSocket.getInputStream()).orElseThrow());
                BulkDataFrame third = BulkDataFrameCodec.decode(BulkFrameStream.read(bulkSocket.getInputStream()).orElseThrow());
                assertEquals(accepted.transfer().transferId(), first.transferId());
                assertEquals(accepted.transfer().transferId(), second.transferId());
                assertEquals(accepted.transfer().transferId(), third.transferId());
                assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), first.payload());
                assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), second.payload());
                assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), third.payload());
            }
        }
    }

    @Test
    void mountedBlockfsControlAdmissionFeedsTheBulkReadStream() throws Exception {
        Fixture fixture = mountedBlockfsFixture();
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             BulkReadStreamServer bulk = BulkReadStreamServer.bind(
                     new InetSocketAddress("127.0.0.1", 0),
                     runtime.bulkReadSessionHandler(),
                     ServerSocketFactory.getDefault()
             )) {
            control.start();
            bulk.start();

            ControlBootstrap accepted = bootstrapReadOverControl(control.boundAddress(), fixture.manifest());

            try (Socket bulkSocket = new Socket()) {
                bulkSocket.connect(bulk.boundAddress());
                bulkSocket.setSoTimeout(5_000);
                BulkReadRequestStream.write(
                        bulkSocket.getOutputStream(),
                        BulkReadRequestCodec.encode(new BulkReadRequest(
                                accepted.sessionId(),
                                accepted.transfer().transferId(),
                                "/datasets/train/shard.00000.mds",
                                3,
                                18,
                                3
                        ))
                );

                BulkDataFrame first = BulkDataFrameCodec.decode(BulkFrameStream.read(bulkSocket.getInputStream()).orElseThrow());
                BulkDataFrame second = BulkDataFrameCodec.decode(BulkFrameStream.read(bulkSocket.getInputStream()).orElseThrow());
                BulkDataFrame third = BulkDataFrameCodec.decode(BulkFrameStream.read(bulkSocket.getInputStream()).orElseThrow());
                assertEquals(0, first.cordIndex());
                assertEquals(1, second.cordIndex());
                assertEquals(2, third.cordIndex());
                assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), first.payload());
                assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), second.payload());
                assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), third.payload());
            }
        }
    }

    private static ControlBootstrap bootstrapReadOverControl(InetSocketAddress controlAddress, FileManifest manifest)
            throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(controlAddress);
            socket.setSoTimeout(5_000);
            ClientAuthenticator authenticator = new ClientAuthenticator();
            try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
                DecodedFrame challenge = exchange(socket, ControlEnvelope.unauthenticated(authenticator.start(credentials)));
                DecodedFrame auth = exchange(
                        socket,
                        ControlEnvelope.unauthenticated(authenticator.answer((AuthChallenge) challenge.payload(), credentials))
                );
                AuthAccepted accepted = (AuthAccepted) auth.payload();
                DecodedFrame transfer = exchange(
                        socket,
                        ControlEnvelope.authenticated(
                                accepted.sessionId(),
                                new TransferBootstrapRequest(
                                        java.util.UUID.randomUUID(),
                                        TransferDirection.READ,
                                        "/datasets/train/shard.00000.mds",
                                        TransferPayloadFormat.MDS_SHARDS,
                                        1,
                                        64,
                                        64 * 1024 * 1024,
                                        manifest.size()
                                )
                        )
                );
                return new ControlBootstrap(accepted.sessionId(), (TransferAcceptedResponse) transfer.payload());
            }
        }
    }

    private static DecodedFrame exchange(Socket socket, ControlEnvelope<? extends ControlPayload> envelope)
            throws Exception {
        byte[] payload = ControlPayloadCodec.encode(envelope.payload());
        byte[] frame = ControlFrameCodec.encode(ControlFrame.fromEnvelope(envelope, payload));
        ControlFrameStream.write(socket.getOutputStream(), frame);
        ControlFrame responseFrame = ControlFrameCodec.decode(ControlFrameStream.read(socket.getInputStream()).orElseThrow());
        ControlPayload responsePayload = ControlPayloadCodec.decode(responseFrame.operation(), responseFrame.payload());
        return new DecodedFrame(responseFrame, responsePayload);
    }

    private static Fixture fixture() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(ControlAndBulkRuntimeIntegrationTest::fixedEightByteChunks);
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        return new Fixture(new CxnFileServer(config(), repository), manifest);
    }

    private Fixture mountedBlockfsFixture() throws Exception {
        Path mountRoot = tempDir.resolve("mount");
        Path metadataRoot = tempDir.resolve("metadata");
        Path mountedFile = mountRoot.resolve("datasets/train/shard.00000.mds");
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        Files.createDirectories(mountedFile.getParent());
        Files.write(mountedFile, data);

        List<Chunk> chunks = fixedEightByteChunks(data);
        FileManifest manifest = new FileManifest(
                "/datasets/train/shard.00000.mds",
                Hashing.file(
                        chunks.stream().map(Chunk::hash).toList(),
                        chunks.stream().map(Chunk::size).toList()
                ),
                data.length,
                chunks.stream()
                        .map(chunk -> new ChunkRef(chunk.hash(), chunk.size()))
                        .toList()
        );
        new LinuxBlockfsNamespaceStore().write(
                metadataRoot,
                new LinuxBlockfsNamespaceSnapshot(
                        11,
                        List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/shard.00000.mds", data.length))
                )
        );
        new LinuxBlockfsManifestIndexStore().write(
                metadataRoot,
                new LinuxBlockfsManifestIndexSnapshot(
                        11,
                        List.of(LinuxBlockfsManifestEntry.fromManifest(2, manifest))
                )
        );
        return new Fixture(new CxnFileServer(
                config(),
                MountedBlockfsStore.load(mountRoot, metadataRoot)
        ), manifest);
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

    private record DecodedFrame(ControlFrame frame, ControlPayload payload) {
    }

    private record ControlBootstrap(UUID sessionId, TransferAcceptedResponse transfer) {
    }
}
