package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.server.CxnFileServerConfig;
import com.fortrow.cxnfs.store.CxnfsRepository;
import org.junit.jupiter.api.Test;

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
import static org.junit.jupiter.api.Assertions.assertThrows;

final class BulkReadDeliveryServiceTest {
    @Test
    void deliversEncodedFullChunkFramesAcrossTheActiveCordSet() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(BulkReadDeliveryServiceTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 5, 15);
        AdmittedReadTransfer transfer = transfer(UUID.randomUUID(), 1, 4, data.length);
        List<EncodedFrame> encodedFrames = new ArrayList<>();

        BulkReadDeliveryResult result = new BulkReadDeliveryService(server)
                .deliver(
                        transfer,
                        7,
                        4,
                        plan,
                        new BulkFrameEncodingSink((cordIndex, frameBytes) ->
                                encodedFrames.add(new EncodedFrame(cordIndex, frameBytes)))
                );

        assertEquals(transfer.transferId(), result.transferId());
        assertEquals(7, result.entrySequence());
        assertEquals(4, result.activeCords());
        assertEquals(3, result.frameCount());
        assertEquals(24, result.payloadBytes());
        assertEquals(3, encodedFrames.size());

        BulkDataFrame first = BulkDataFrameCodec.decode(encodedFrames.get(0).frameBytes());
        BulkDataFrame second = BulkDataFrameCodec.decode(encodedFrames.get(1).frameBytes());
        BulkDataFrame third = BulkDataFrameCodec.decode(encodedFrames.get(2).frameBytes());
        assertEquals(0, encodedFrames.get(0).cordIndex());
        assertEquals(1, encodedFrames.get(1).cordIndex());
        assertEquals(2, encodedFrames.get(2).cordIndex());
        assertEquals(transfer.transferId(), first.transferId());
        assertEquals(7, first.entrySequence());
        assertEquals(0, first.chunkSequence());
        assertEquals(1, second.chunkSequence());
        assertEquals(2, third.chunkSequence());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), first.payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), second.payload());
        assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), third.payload());
    }

    @Test
    void rejectsActiveCordCountsOutsideTheAdmittedRange() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(BulkReadDeliveryServiceTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 0, 16);
        AdmittedReadTransfer transfer = transfer(UUID.randomUUID(), 1, 2, data.length);
        BulkReadDeliveryService service = new BulkReadDeliveryService(server);

        assertThrows(
                IllegalArgumentException.class,
                () -> service.deliver(transfer, 0, 3, plan, frame -> {
                })
        );
    }

    @Test
    void rejectsReadPlansAboveTheAdmittedSizeLimit() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(BulkReadDeliveryServiceTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 0, 16);
        AdmittedReadTransfer transfer = transfer(UUID.randomUUID(), 1, 4, 8);
        BulkReadDeliveryService service = new BulkReadDeliveryService(server);

        assertThrows(
                java.io.IOException.class,
                () -> service.deliver(transfer, 0, plan, frame -> {
                })
        );
    }

    @Test
    void deliversThroughStreamingProducerPath() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(BulkReadDeliveryServiceTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 0, 8);
        AdmittedReadTransfer transfer = transfer(UUID.randomUUID(), 1, 4, data.length);
        StreamingOnlyProducer producer = new StreamingOnlyProducer(server);
        List<BulkDataFrame> deliveredFrames = new ArrayList<>();

        BulkReadDeliveryResult result = new BulkReadDeliveryService(producer)
                .deliver(transfer, 3, 2, plan, deliveredFrames::add);

        assertEquals(1, producer.streamCalls);
        assertEquals(1, result.frameCount());
        assertEquals(8, result.payloadBytes());
        assertEquals(2, result.activeCords());
        assertEquals(1, deliveredFrames.size());
        assertEquals(transfer.transferId(), deliveredFrames.getFirst().transferId());
        assertEquals(3, deliveredFrames.getFirst().entrySequence());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), deliveredFrames.getFirst().payload());
    }

    private static AdmittedReadTransfer transfer(UUID transferId, int acceptedInitialCords, int acceptedMaxCords, long sizeLimit) {
        return new AdmittedReadTransfer(
                transferId,
                UUID.randomUUID(),
                "trainer",
                "/datasets/train/shard.00000.mds",
                TransferPayloadFormat.MDS_SHARDS,
                acceptedInitialCords,
                acceptedMaxCords,
                sizeLimit
        );
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

    private record EncodedFrame(int cordIndex, byte[] frameBytes) {
    }

    private static final class StreamingOnlyProducer extends ReadPlanBulkFrameProducer {
        private int streamCalls;

        private StreamingOnlyProducer(CxnFileServer server) {
            super(server);
        }

        @Override
        public List<BulkDataFrame> produce(
                AdmittedReadTransfer transfer,
                int entrySequence,
                int activeCords,
                ReadPlan plan
        ) {
            throw new AssertionError("delivery must use the streaming producer path");
        }

        @Override
        public BulkReadFrameProductionResult produceTo(
                AdmittedReadTransfer transfer,
                int entrySequence,
                int activeCords,
                ReadPlan plan,
                BulkFrameSink sink
        ) throws java.io.IOException {
            streamCalls++;
            byte[] payload = "abcdefgh".getBytes(StandardCharsets.UTF_8);
            sink.send(new BulkDataFrame(
                    transfer.transferId(),
                    0,
                    entrySequence,
                    0,
                    0,
                    true,
                    payload
            ));
            return new BulkReadFrameProductionResult(1, payload.length);
        }
    }
}
