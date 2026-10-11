package com.fortrow.cxnfs.transport;

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

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ReadPlanBulkFrameProducerTest {
    @Test
    void producesFullTouchedChunkFramesSoReceiversCanVerifyBeforeSlicing() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(ReadPlanBulkFrameProducerTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 5, 15);
        UUID transferId = UUID.randomUUID();

        List<BulkDataFrame> frames = new ReadPlanBulkFrameProducer(server)
                .produce(transferId, 0, 2, plan);

        assertEquals(3, frames.size());
        assertEquals(0, frames.get(0).cordIndex());
        assertEquals(1, frames.get(1).cordIndex());
        assertEquals(0, frames.get(2).cordIndex());
        assertEquals(0, frames.get(0).chunkSequence());
        assertEquals(1, frames.get(1).chunkSequence());
        assertEquals(2, frames.get(2).chunkSequence());
        assertEquals(0, frames.get(0).entryOffset());
        assertEquals(8, frames.get(1).entryOffset());
        assertEquals(16, frames.get(2).entryOffset());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), frames.get(0).payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), frames.get(1).payload());
        assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), frames.get(2).payload());
    }

    @Test
    void streamsFullTouchedChunkFramesToSink() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(ReadPlanBulkFrameProducerTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 5, 15);
        UUID transferId = UUID.randomUUID();
        List<BulkDataFrame> streamedFrames = new ArrayList<>();

        BulkReadFrameProductionResult result = new ReadPlanBulkFrameProducer(server)
                .produceTo(transferId, 0, 2, plan, streamedFrames::add);

        assertEquals(3, result.frameCount());
        assertEquals(24, result.payloadBytes());
        assertEquals(3, streamedFrames.size());
        assertEquals(0, streamedFrames.get(0).cordIndex());
        assertEquals(1, streamedFrames.get(1).cordIndex());
        assertEquals(0, streamedFrames.get(2).cordIndex());
        assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), streamedFrames.get(0).payload());
        assertArrayEquals("ijklmnop".getBytes(StandardCharsets.UTF_8), streamedFrames.get(1).payload());
        assertArrayEquals("qrstuvwx".getBytes(StandardCharsets.UTF_8), streamedFrames.get(2).payload());
    }

    @Test
    void producesFramesFromAnAdmittedReadTransfer() throws Exception {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(ReadPlanBulkFrameProducerTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServer server = new CxnFileServer(config(), repository);
        ReadPlan plan = server.readPlan(manifest.fileHash(), 0, 16);
        UUID transferId = UUID.randomUUID();
        AdmittedReadTransfer transfer = new AdmittedReadTransfer(
                transferId,
                UUID.randomUUID(),
                "trainer",
                "/datasets/train/shard.00000.mds",
                TransferPayloadFormat.MDS_SHARDS,
                2,
                64,
                data.length
        );

        List<BulkDataFrame> frames = new ReadPlanBulkFrameProducer(server)
                .produce(transfer, 0, plan);

        assertEquals(2, frames.size());
        assertEquals(transferId, frames.get(0).transferId());
        assertEquals(0, frames.get(0).cordIndex());
        assertEquals(1, frames.get(1).cordIndex());
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
}
