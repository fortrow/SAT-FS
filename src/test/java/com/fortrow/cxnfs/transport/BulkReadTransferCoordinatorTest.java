package com.fortrow.cxnfs.transport;

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
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.server.ControlSessionDispatcher;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.server.CxnFileServerConfig;
import com.fortrow.cxnfs.storage.LinuxBlockfsFile;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestEntry;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsManifestIndexStore;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceSnapshot;
import com.fortrow.cxnfs.storage.LinuxBlockfsNamespaceStore;
import com.fortrow.cxnfs.storage.MountedBlockfsStore;
import com.fortrow.cxnfs.store.CxnfsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class BulkReadTransferCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void deliversOnlyAfterControlAdmissionForTheSamePath() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted controlSession = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        TransferAcceptedResponse admission = bootstrapRead(dispatcher, controlSession, transferId, fixture.manifest());
        List<BulkDataFrame> frames = new ArrayList<>();

        BulkReadDeliveryResult result = new BulkReadTransferCoordinator(fixture.server(), dispatcher::admittedReadTransfer)
                .deliver(
                        controlSession.sessionId(),
                        transferId,
                        "/datasets/train/shard.00000.mds",
                        5,
                        15,
                        new BulkFrameEncodingSink((cordIndex, frameBytes) ->
                                frames.add(BulkDataFrameCodec.decode(frameBytes)))
                );

        assertEquals(admission.transferId(), result.transferId());
        assertEquals(0, result.entrySequence());
        assertEquals(1, result.activeCords());
        assertEquals(3, result.frameCount());
        assertEquals(24, result.payloadBytes());
        assertEquals(3, frames.size());
        assertEquals(0, frames.get(0).cordIndex());
        assertEquals(0, frames.get(1).cordIndex());
        assertEquals(0, frames.get(2).cordIndex());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), frames.get(0).payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), frames.get(1).payload());
        assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), frames.get(2).payload());
    }

    @Test
    void canDeliverAdmittedReadsIntoALengthPrefixedBulkStream() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted controlSession = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        bootstrapRead(dispatcher, controlSession, transferId, fixture.manifest());
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        new BulkReadTransferCoordinator(fixture.server(), dispatcher::admittedReadTransfer)
                .deliver(
                        controlSession.sessionId(),
                        transferId,
                        "/datasets/train/shard.00000.mds",
                        0,
                        16,
                        new BulkFrameEncodingSink(new BulkFrameStreamSink(output))
                );

        ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
        BulkDataFrame first = BulkDataFrameCodec.decode(BulkFrameStream.read(input).orElseThrow());
        BulkDataFrame second = BulkDataFrameCodec.decode(BulkFrameStream.read(input).orElseThrow());
        assertEquals(Optional.empty(), BulkFrameStream.read(input));
        assertEquals(transferId, first.transferId());
        assertEquals(transferId, second.transferId());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), first.payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), second.payload());
    }

    @Test
    void deliversMountedBlockfsReadPlansThroughAdmittedBulkPath() throws Exception {
        MountedBulkFixture mounted = mountedBlockfsFixture("abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8));
        CxnFileServer server = new CxnFileServer(
                config(),
                MountedBlockfsStore.load(mounted.mountRoot(), mounted.metadataRoot())
        );
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(server);
        AuthAccepted controlSession = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        bootstrapRead(dispatcher, controlSession, transferId, mounted.manifest());
        List<BulkDataFrame> frames = new ArrayList<>();

        BulkReadDeliveryResult result = new BulkReadTransferCoordinator(server, dispatcher::admittedReadTransfer)
                .deliver(
                        controlSession.sessionId(),
                        transferId,
                        "/datasets/train/shard.00000.mds",
                        3,
                        18,
                        3,
                        new BulkFrameEncodingSink((cordIndex, frameBytes) ->
                                frames.add(BulkDataFrameCodec.decode(frameBytes)))
                );

        assertEquals(transferId, result.transferId());
        assertEquals(3, result.activeCords());
        assertEquals(3, result.frameCount());
        assertEquals(24, result.payloadBytes());
        assertEquals(0, frames.get(0).cordIndex());
        assertEquals(1, frames.get(1).cordIndex());
        assertEquals(2, frames.get(2).cordIndex());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), frames.get(0).payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), frames.get(1).payload());
        assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), frames.get(2).payload());
    }

    @Test
    void rejectsUnadmittedTransfersAndPathMismatches() throws Exception {
        Fixture fixture = fixture();
        ControlSessionDispatcher dispatcher = new ControlSessionDispatcher(fixture.server());
        AuthAccepted controlSession = authenticate(dispatcher);
        UUID transferId = UUID.randomUUID();
        bootstrapRead(dispatcher, controlSession, transferId, fixture.manifest());
        BulkReadTransferCoordinator coordinator =
                new BulkReadTransferCoordinator(fixture.server(), dispatcher::admittedReadTransfer);

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.deliver(controlSession.sessionId(), UUID.randomUUID(), "/datasets/train/shard.00000.mds", 0, 8, frame -> {
                })
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.deliver(controlSession.sessionId(), transferId, "/datasets/train/other.mds", 0, 8, frame -> {
                })
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.deliver(UUID.randomUUID(), transferId, "/datasets/train/shard.00000.mds", 0, 8, frame -> {
                })
        );
    }

    private static TransferAcceptedResponse bootstrapRead(
            ControlSessionDispatcher dispatcher,
            AuthAccepted accepted,
            UUID transferId,
            FileManifest manifest
    ) {
        return (TransferAcceptedResponse) dispatcher
                .handle(ControlEnvelope.authenticated(
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
                ))
                .payload();
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
        CxnfsRepository repository = new CxnfsRepository(BulkReadTransferCoordinatorTest::fixedEightByteChunks);
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        return new Fixture(new CxnFileServer(config(), repository), manifest);
    }

    private MountedBulkFixture mountedBlockfsFixture(byte[] data) throws Exception {
        Path mountRoot = tempDir.resolve("mount");
        Path metadataRoot = tempDir.resolve("metadata");
        Path mountedFile = mountRoot.resolve("datasets/train/shard.00000.mds");
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
                        7,
                        List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/shard.00000.mds", data.length))
                )
        );
        new LinuxBlockfsManifestIndexStore().write(
                metadataRoot,
                new LinuxBlockfsManifestIndexSnapshot(
                        7,
                        List.of(LinuxBlockfsManifestEntry.fromManifest(2, manifest))
                )
        );
        return new MountedBulkFixture(mountRoot, metadataRoot, manifest);
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

    private record MountedBulkFixture(Path mountRoot, Path metadataRoot, FileManifest manifest) {
    }
}
