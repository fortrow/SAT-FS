package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.auth.PasswordAuthProof;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.dataset.DatasetSendPlan;
import com.fortrow.cxnfs.dataset.DatasetSendResult;
import com.fortrow.cxnfs.dataset.SafetensorsOutputSender;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ByteRange;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.AuthProof;
import com.fortrow.cxnfs.protocol.AuthStart;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlFrame;
import com.fortrow.cxnfs.protocol.ControlFrameCodec;
import com.fortrow.cxnfs.protocol.ControlFrameStream;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.ControlPayloadCodec;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.PutFileRequest;
import com.fortrow.cxnfs.protocol.PutFileResponse;
import com.fortrow.cxnfs.protocol.ReadFileRangeResponse;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryAction;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.server.BulkReadStreamServer;
import com.fortrow.cxnfs.server.ControlTcpServer;
import com.fortrow.cxnfs.server.ControlTlsContextFactory;
import com.fortrow.cxnfs.server.ControlTlsServerSocketFactory;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.server.CxnFileServerConfig;
import com.fortrow.cxnfs.server.CxnFileServerRuntime;
import com.fortrow.cxnfs.server.QuicBulkTransferServer;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameKey;
import com.fortrow.cxnfs.transport.BulkFrameStream;
import com.fortrow.cxnfs.transport.BulkWriteAck;
import com.fortrow.cxnfs.transport.BulkWriteAckCodec;
import com.fortrow.cxnfs.transport.BulkWriteAckStream;
import com.fortrow.cxnfs.transport.StagedPayloadStorage;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CxnFileClientTest {
    @TempDir
    Path tempDir;

    @Test
    void readsByNamespacePathThroughServerFacade() throws Exception {
        byte[] data = "server facade path read\n".repeat(4096).getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest manifest = repository.put("/datasets/train/shard.mds", data);
        CxnFileClient client = new CxnFileClient(new CxnFileServer(config(), repository));

        byte[] range = client.getRange("/datasets/train/shard.mds", 7, 128);

        assertEquals(manifest, client.manifest("/datasets/train/shard.mds").orElseThrow());
        assertEquals(manifest.fileHash(), client.readPlan("/datasets/train/shard.mds", 7, 128).fileHash());
        assertArrayEquals(Arrays.copyOfRange(data, 7, 135), range);
    }

    @Test
    void networkClientAuthenticatesListsAndReadsBulkRangesOverLoopback() throws Exception {
        byte[] data = "network client bulk range read\n".repeat(4096).getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest manifest = repository.put("/datasets/train/shard.mds", data);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(new CxnFileServer(config(0, 0), repository));

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             BulkReadStreamServer bulk = BulkReadStreamServer.bind(runtime, ServerSocketFactory.getDefault())) {
            control.start();
            bulk.start();

            try (CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                    control.boundAddress(),
                    bulk.boundAddress(),
                    SocketFactory.getDefault()
            );
                 ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
                client.authenticate(credentials);

                byte[] range = client.readRange("/datasets/train/shard.mds", 9, 257);
                ByteArrayOutputStream streamedRange = new ByteArrayOutputStream();
                long streamedBytes = client.readRangeTo(
                        "/datasets/train/shard.mds",
                        17,
                        333,
                        TransferPayloadFormat.CONTAINER,
                        streamedRange
                );
                PutFileResponse putPreflight = client.preparePutFile(new PutFileRequest(
                        "/outputs/copy.mds",
                        manifest.size(),
                        manifest.fileHash(),
                        manifest.chunks()
                ));
                TransferAcceptedResponse writeAdmission = client.bootstrapWrite(
                        "/outputs/checkpoints",
                        TransferPayloadFormat.SAFETENSORS
                );
                byte[] writePayload = "checkpoint payload over bulk stream".getBytes(StandardCharsets.UTF_8);
                ContentHash writeChunkHash = Hashing.chunk(writePayload);
                ContentHash writeFileHash = Hashing.file(List.of(writeChunkHash), List.of(writePayload.length));
                ContentHash writeTransferHash = Hashing.transfer(List.of(writeFileHash), List.of((long) writePayload.length));
                TransferEntryAcceptedResponse entryAccepted = client.acceptWriteEntry(new TransferEntryUpdate(
                        writeAdmission.transferId(),
                        0,
                        TransferEntryAction.CREATE_OR_REPLACE,
                        "/outputs/checkpoints/model.safetensors",
                        "",
                        writePayload.length,
                        TransferPayloadFormat.SAFETENSORS
                ));
                client.sendWriteFrame(new BulkDataFrame(
                        writeAdmission.transferId(),
                        0,
                        0,
                        0,
                        0,
                        true,
                        writePayload
                ));
                TransferMetadataAcceptedResponse metadataAccepted = client.acceptWriteMetadata(new TransferMetadataUpdate(
                        writeAdmission.transferId(),
                        0,
                        0,
                        true,
                        writePayload.length,
                        List.of(writeChunkHash)
                ));
                TransferEntryCommittedResponse entryCommitted = client.commitWriteEntry(new TransferEntryCommitRequest(
                        writeAdmission.transferId(),
                        0,
                        "/outputs/checkpoints/model.safetensors",
                        writeFileHash,
                        writePayload.length
                ));
                TransferCommittedResponse transferCommitted = client.commitWriteTransfer(new TransferCommitRequest(
                        writeAdmission.transferId(),
                        "/outputs/checkpoints",
                        writeTransferHash,
                        writePayload.length
                ));

                assertEquals("shard.mds", client.list("/datasets/train").entries().getFirst().name());
                assertEquals(manifest.fileHash(), client.readPlan("/datasets/train/shard.mds", 9, 257).fileHash());
                assertEquals(manifest, client.manifest(manifest.fileHash()).orElseThrow());
                assertEquals(manifest.fileHash(), client.reconstructionPlan(manifest.fileHash()).fileHash());
                assertEquals(data.length, client.stat("/datasets/train/shard.mds").orElseThrow().size());
                assertEquals(333, streamedBytes);
                assertArrayEquals(Arrays.copyOfRange(data, 17, 350), streamedRange.toByteArray());
                assertEquals("/outputs/copy.mds", putPreflight.manifest().path());
                assertTrue(putPreflight.missingChunks().isEmpty());
                assertTrue(client.stat("/outputs/copy.mds").isEmpty());
                assertEquals(TransferDirection.WRITE, writeAdmission.direction());
                assertTrue(writeAdmission.writePayloadAdmissionGranted());
                assertEquals("/outputs/checkpoints/model.safetensors", entryAccepted.path());
                assertEquals(1, metadataAccepted.chunkHashesAccepted());
                assertEquals(writeFileHash, entryCommitted.contentHash());
                assertEquals(writeTransferHash, transferCommitted.contentHash());
                assertEquals(writeFileHash, client.stat("/outputs/checkpoints/model.safetensors")
                        .orElseThrow()
                        .contentHash()
                        .orElseThrow());
                assertArrayEquals(writePayload, client.readRange(
                        "/outputs/checkpoints/model.safetensors",
                        0,
                        writePayload.length,
                        TransferPayloadFormat.CONTAINER
                ));
                RenamePathResponse rename = client.renamePath(
                        "/outputs/checkpoints/model.safetensors",
                        "/outputs/checkpoints/model-renamed.safetensors"
                );
                DeletePathResponse delete = client.deletePath(
                        "/outputs/checkpoints/model-renamed.safetensors",
                        false
                );
                assertEquals("/outputs/checkpoints/model-renamed.safetensors", rename.targetPath());
                assertEquals(writeFileHash, rename.metadata().contentHash().orElseThrow());
                assertEquals(1, delete.deletedEntries());
                assertTrue(client.stat("/outputs/checkpoints/model-renamed.safetensors").isEmpty());
                assertArrayEquals(Arrays.copyOfRange(data, 9, 266), range);
            }
        }
    }

    @Test
    void networkClientReadsAndWritesThroughTlsControlAndQuicBulkRuntime() throws Exception {
        Path keyStorePath = tempDir.resolve("network-quic.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        byte[] data = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(CxnFileClientTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/shard.mds", data);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(new CxnFileServer(
                config(keyStorePath, 0, 0),
                repository
        ));
        CxnFileNetworkClientConfig clientConfig = new CxnFileNetworkClientConfig(
                4,
                8,
                64 * 1024,
                manifest.size(),
                5_000,
                5_000
        );
        SSLContext serverContext = new ControlTlsContextFactory().create(
                runtime.server().config().controlBinding().security(),
                password
        );
        SSLContext clientContext = new ClientTlsContextFactory().create(keyStorePath, password);

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, new ControlTlsServerSocketFactory(serverContext));
             QuicBulkTransferServer bulk = QuicBulkTransferServer.bind(runtime, keyStore, password)) {
            control.start();
            bulk.start();

            try (QuicBulkTransferClientChannel bulkChannel = QuicBulkTransferClientChannel.connect(
                    new InetSocketAddress("localhost", bulk.boundAddress().getPort()),
                    clientConfig,
                    java.util.Optional.of(keyStore),
                    false
            );
                 CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                         new InetSocketAddress("localhost", control.boundAddress().getPort()),
                         new ControlTlsClientSocketFactory(clientContext),
                         clientConfig,
                         bulkChannel
                 );
                 ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
                client.authenticate(credentials);

                byte[] range = client.readRange("/datasets/train/shard.mds", 8, 40, TransferPayloadFormat.MDS_SHARDS);
                int readCordStreams = bulkChannel.lastReadStreamCount();
                Map<String, Object> readEvidence = bulkChannel.readPathEvidence();
                TransferAcceptedResponse writeAdmission = client.bootstrapWrite(
                        "/outputs/checkpoints",
                        TransferPayloadFormat.SAFETENSORS
                );
                byte[] writePayload = "quic checkpoint payload with late metadata".getBytes(StandardCharsets.UTF_8);
                ContentHash writeChunkHash = Hashing.chunk(writePayload);
                ContentHash writeFileHash = Hashing.file(List.of(writeChunkHash), List.of(writePayload.length));
                ContentHash writeTransferHash = Hashing.transfer(List.of(writeFileHash), List.of((long) writePayload.length));
                TransferEntryAcceptedResponse entryAccepted = client.acceptWriteEntry(new TransferEntryUpdate(
                        writeAdmission.transferId(),
                        0,
                        TransferEntryAction.CREATE_OR_REPLACE,
                        "/outputs/checkpoints/model.safetensors",
                        "",
                        writePayload.length,
                        TransferPayloadFormat.SAFETENSORS
                ));
                client.sendWriteFrame(new BulkDataFrame(
                        writeAdmission.transferId(),
                        0,
                        0,
                        0,
                        0,
                        true,
                        writePayload
                ));
                assertTrue(client.stat("/outputs/checkpoints/model.safetensors").isEmpty());

                TransferMetadataAcceptedResponse metadataAccepted = client.acceptWriteMetadata(new TransferMetadataUpdate(
                        writeAdmission.transferId(),
                        0,
                        0,
                        true,
                        writePayload.length,
                        List.of(writeChunkHash)
                ));
                assertTrue(client.stat("/outputs/checkpoints/model.safetensors").isEmpty());

                TransferEntryCommittedResponse entryCommitted = client.commitWriteEntry(new TransferEntryCommitRequest(
                        writeAdmission.transferId(),
                        0,
                        "/outputs/checkpoints/model.safetensors",
                        writeFileHash,
                        writePayload.length
                ));
                TransferCommittedResponse transferCommitted = client.commitWriteTransfer(new TransferCommitRequest(
                        writeAdmission.transferId(),
                        "/outputs/checkpoints",
                        writeTransferHash,
                        writePayload.length
                ));

                assertArrayEquals(Arrays.copyOfRange(data, 8, 48), range);
                assertEquals(5, readCordStreams);
                assertTrue(bulkChannel.readStreamCompletionVerified());
                assertTrue(bulkChannel.readStreamEofVerified());
                assertEquals("cxnfs.quic-bulk-transport-evidence.v1", readEvidence.get("format"));
                assertEquals(5, readEvidence.get("last_read_stream_count"));
                assertEquals(5, readEvidence.get("last_attempted_read_stream_count"));
                assertEquals(5, readEvidence.get("read_streams_eof_verified"));
                assertEquals(true, readEvidence.get("read_stream_completion_verified"));
                assertEquals(true, readEvidence.get("read_stream_eof_verified"));
                assertEquals(5, readEvidence.get("read_partition_count"));
                assertEquals(5, readEvidence.get("read_partition_chunk_count"));
                assertEquals(40L, readEvidence.get("read_partition_payload_bytes"));
                assertEquals(40L, readEvidence.get("read_partition_requested_bytes"));
                assertEquals(true, readEvidence.get("read_partition_covers_plan"));
                assertTrue(bulk.totalStreamCount() >= 6);
                assertTrue(bulk.lastStreamError().isEmpty());
                assertEquals(TransferDirection.WRITE, writeAdmission.direction());
                assertTrue(writeAdmission.writePayloadAdmissionGranted());
                assertEquals("/outputs/checkpoints/model.safetensors", entryAccepted.path());
                assertEquals(1, metadataAccepted.chunkHashesAccepted());
                assertEquals(1, metadataAccepted.stagedPayloadsVerified());
                assertEquals(1, bulkChannel.writeAckReceivedCount());
                assertEquals(1, bulkChannel.writeAckVerifiedCount());
                assertEquals(0, bulkChannel.writeAckVerificationFailureCount());
                assertEquals(1, bulkChannel.writeAckMetadataPendingCount());
                assertTrue(bulkChannel.writeAckFrameIdentityVerified());
                assertTrue(bulkChannel.writeAckStreamCompletionVerified());
                assertFalse(bulkChannel.writeAckStreamEofVerified());
                Map<String, Object> writeEvidence = bulkChannel.writePathEvidence();
                assertEquals("cxnfs.quic-bulk-write-transport-evidence.v1", writeEvidence.get("format"));
                assertEquals(1, writeEvidence.get("write_acks_received"));
                assertEquals(1, writeEvidence.get("write_acks_verified"));
                assertEquals(0, writeEvidence.get("write_ack_streams_eof_verified"));
                assertEquals(0, writeEvidence.get("write_ack_verification_failures"));
                assertEquals(true, writeEvidence.get("write_ack_frame_identity_verified"));
                assertEquals(true, writeEvidence.get("write_ack_stream_completion_verified"));
                assertEquals(false, writeEvidence.get("write_ack_stream_eof_verified"));
                assertEquals(writeFileHash, entryCommitted.contentHash());
                assertEquals(writeTransferHash, transferCommitted.contentHash());
                assertArrayEquals(writePayload, client.readRange(
                        "/outputs/checkpoints/model.safetensors",
                        0,
                        writePayload.length,
                        TransferPayloadFormat.CONTAINER
                ));
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void quicWriteAckStreamRejectsTrailingAcknowledgement() throws Exception {
        BulkDataFrame first = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000501"),
                0,
                0,
                0,
                0,
                true,
                "checkpoint-frame-0".getBytes(StandardCharsets.UTF_8)
        );
        BulkDataFrame second = new BulkDataFrame(
                first.transferId(),
                1,
                0,
                1,
                first.payloadLength(),
                true,
                "checkpoint-frame-1".getBytes(StandardCharsets.UTF_8)
        );
        ByteArrayInputStream singleAck = new ByteArrayInputStream(ackBytes(first));
        BulkWriteAckStream.read(singleAck).orElseThrow();

        QuicBulkTransferClientChannel.requireQuicWriteAckStreamComplete(singleAck);

        ByteArrayInputStream trailingAck = new ByteArrayInputStream(ackBytes(first, second));
        BulkWriteAckStream.read(trailingAck).orElseThrow();

        IOException failure = assertThrows(
                IOException.class,
                () -> QuicBulkTransferClientChannel.requireQuicWriteAckStreamComplete(trailingAck)
        );
        assertTrue(failure.getMessage().contains("extra write ack"));
    }

    @Test
    void quicReadStreamRejectsTrailingFrameAfterExpectedPayload() throws Exception {
        BulkDataFrame first = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000503"),
                0,
                0,
                0,
                0,
                true,
                "dataset-frame-0".getBytes(StandardCharsets.UTF_8)
        );
        BulkDataFrame second = new BulkDataFrame(
                first.transferId(),
                1,
                0,
                1,
                first.payloadLength(),
                true,
                "dataset-frame-1".getBytes(StandardCharsets.UTF_8)
        );
        ByteArrayInputStream singleFrame = new ByteArrayInputStream(bulkFrames(first));
        BulkFrameStream.read(singleFrame).orElseThrow();

        QuicBulkTransferClientChannel.requireQuicReadStreamComplete(singleFrame);

        ByteArrayInputStream trailingFrame = new ByteArrayInputStream(bulkFrames(first, second));
        BulkFrameStream.read(trailingFrame).orElseThrow();

        IOException failure = assertThrows(
                IOException.class,
                () -> QuicBulkTransferClientChannel.requireQuicReadStreamComplete(trailingFrame)
        );
        assertTrue(failure.getMessage().contains("extra frame"));
    }

    @Test
    void quicWriteAckValidationRejectsMismatchedFrameIdentity() throws Exception {
        BulkDataFrame frame = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000502"),
                0,
                0,
                7,
                4096,
                true,
                "checkpoint-frame".getBytes(StandardCharsets.UTF_8)
        );
        BulkWriteAck wrongKey = new BulkWriteAck(
                new BulkDataFrameKey(frame.transferId(), frame.entrySequence(), frame.chunkSequence() + 1),
                frame.payloadLength(),
                false,
                StagedPayloadStorage.MEMORY
        );
        BulkWriteAck wrongLength = new BulkWriteAck(
                frame.key(),
                frame.payloadLength() + 1,
                false,
                StagedPayloadStorage.MEMORY
        );

        IOException keyFailure = assertThrows(
                IOException.class,
                () -> QuicBulkTransferClientChannel.requireQuicWriteAckMatchesFrame(frame, wrongKey)
        );
        IOException lengthFailure = assertThrows(
                IOException.class,
                () -> QuicBulkTransferClientChannel.requireQuicWriteAckMatchesFrame(frame, wrongLength)
        );

        assertTrue(keyFailure.getMessage().contains("key does not match"));
        assertTrue(lengthFailure.getMessage().contains("length does not match"));
    }

    @Test
    void networkClientSendsSafetensorsDirectoryThroughRuntimeWritePath() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(new CxnFileServer(config(0, 0), repository));
        Path checkpoint = tempDir.resolve("checkpoint");
        Path weights = checkpoint.resolve("weights");
        Files.createDirectories(weights);
        byte[] configBytes = "{\"dtype\":\"bf16\"}".getBytes(StandardCharsets.UTF_8);
        byte[] tensorBytes = "checkpoint tensor payload over authenticated bulk write".getBytes(StandardCharsets.UTF_8);
        Files.write(checkpoint.resolve("config.json"), configBytes);
        Files.write(weights.resolve("model.safetensors"), tensorBytes);

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             BulkReadStreamServer bulk = BulkReadStreamServer.bind(runtime, ServerSocketFactory.getDefault())) {
            control.start();
            bulk.start();

            try (CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                    control.boundAddress(),
                    bulk.boundAddress(),
                    SocketFactory.getDefault()
            );
                 ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
                client.authenticate(credentials);

                DatasetSendResult result = new SafetensorsOutputSender().send(
                        DatasetSendPlan.safetensors(checkpoint),
                        "/outputs/run-42",
                        client
                );

                assertEquals("/outputs/run-42", result.remoteRoot());
                assertEquals(2, result.fileCount());
                assertEquals(1, result.tensorFileCount());
                assertEquals(configBytes.length + tensorBytes.length, result.bytesSent());
                assertArrayEquals(configBytes, client.readRange(
                        "/outputs/run-42/config.json",
                        0,
                        configBytes.length,
                        TransferPayloadFormat.CONTAINER
                ));
                assertArrayEquals(tensorBytes, client.readRange(
                        "/outputs/run-42/weights/model.safetensors",
                        0,
                        tensorBytes.length,
                        TransferPayloadFormat.CONTAINER
                ));
                assertEquals(tensorBytes.length, client.stat("/outputs/run-42/weights/model.safetensors")
                        .orElseThrow()
                        .size());
            }
        }
    }

    @Test
    void networkClientSerializesConcurrentReadsOnSharedStreams() throws Exception {
        byte[] data = "concurrent network client read\n".repeat(4096).getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        repository.put("/datasets/train/shard.mds", data);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(new CxnFileServer(config(0, 0), repository));

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             BulkReadStreamServer bulk = BulkReadStreamServer.bind(runtime, ServerSocketFactory.getDefault())) {
            control.start();
            bulk.start();

            try (CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                    control.boundAddress(),
                    bulk.boundAddress(),
                    SocketFactory.getDefault()
            );
                 ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
                client.authenticate(credentials);
                ExecutorService executor = Executors.newFixedThreadPool(2);
                try {
                    Future<byte[]> first = executor.submit(() ->
                            client.readRange("/datasets/train/shard.mds", 0, 257));
                    Future<byte[]> second = executor.submit(() ->
                            client.readRange("/datasets/train/shard.mds", 257, 333));

                    assertArrayEquals(Arrays.copyOfRange(data, 0, 257), first.get(5, TimeUnit.SECONDS));
                    assertArrayEquals(Arrays.copyOfRange(data, 257, 590), second.get(5, TimeUnit.SECONDS));
                } finally {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    void networkClientRejectsControlReadPlanForDifferentRangeBeforeBulkRequest() throws Exception {
        UUID sessionId = UUID.fromString("00000000-0000-0000-0000-000000000401");
        TransferAcceptedResponse transfer = new TransferAcceptedResponse(
                UUID.fromString("00000000-0000-0000-0000-000000000402"),
                TransferDirection.READ,
                1,
                64,
                128L * 1024 * 1024,
                false
        );
        ReadPlan wrongPlan = new ReadPlan(
                Hashing.chunk("file-hash-placeholder".getBytes(StandardCharsets.UTF_8)),
                128,
                new ByteRange(0, 7),
                List.of()
        );
        ByteArrayOutputStream bulkOutput = new ByteArrayOutputStream();
        CxnFileNetworkClient client = new CxnFileNetworkClient(
                new ByteArrayInputStream(controlResponses(
                        sessionId,
                        transfer,
                        new ReadFileRangeResponse("/datasets/train/shard.mds", wrongPlan)
                )),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(new byte[0]),
                bulkOutput
        );
        authenticateForWriteTest(client, sessionId);

        IOException failure = assertThrows(IOException.class, () -> client.readRangeTo(
                "/datasets/train/shard.mds",
                5,
                7,
                TransferPayloadFormat.MDS_SHARDS,
                new ByteArrayOutputStream()
        ));

        assertEquals("control read plan range does not match requested read", failure.getMessage());
        assertEquals(0, bulkOutput.size());
    }

    @Test
    void networkClientRejectsMismatchedPublicReadPlanResponse() throws Exception {
        UUID sessionId = UUID.fromString("00000000-0000-0000-0000-000000000405");
        ReadPlan wrongPlan = new ReadPlan(
                Hashing.chunk("file-hash-placeholder".getBytes(StandardCharsets.UTF_8)),
                128,
                new ByteRange(5, 7),
                List.of()
        );
        CxnFileNetworkClient client = new CxnFileNetworkClient(
                new ByteArrayInputStream(controlResponses(
                        sessionId,
                        new ReadFileRangeResponse("/datasets/other/shard.mds", wrongPlan)
                )),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream()
        );
        authenticateForWriteTest(client, sessionId);

        IOException failure = assertThrows(IOException.class, () ->
                client.readPlan("/datasets/train/shard.mds", 5, 7));

        assertEquals("control read response path does not match requested read", failure.getMessage());
    }

    @Test
    void networkClientAcceptsControlReadPlanClampedAtEof() throws Exception {
        UUID sessionId = UUID.fromString("00000000-0000-0000-0000-000000000409");
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000410");
        TransferAcceptedResponse transfer = new TransferAcceptedResponse(
                transferId,
                TransferDirection.READ,
                1,
                64,
                128L * 1024 * 1024,
                false
        );
        byte[] payload = "abcd".getBytes(StandardCharsets.UTF_8);
        ReadPlan eofPlan = new ReadPlan(
                Hashing.chunk("file-hash-placeholder".getBytes(StandardCharsets.UTF_8)),
                payload.length,
                new ByteRange(0, payload.length),
                List.of(new PlannedChunkRead(0, Hashing.chunk(payload), 0, payload.length, 0, payload.length))
        );
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CxnFileNetworkClient client = new CxnFileNetworkClient(
                new ByteArrayInputStream(controlResponses(
                        sessionId,
                        transfer,
                        new ReadFileRangeResponse("/datasets/train/shard.mds", eofPlan)
                )),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(bulkFrames(new BulkDataFrame(
                        transferId,
                        0,
                        0,
                        0,
                        0,
                        true,
                        payload
                ))),
                new ByteArrayOutputStream()
        );
        authenticateForWriteTest(client, sessionId);

        long bytesRead = client.readRangeTo(
                "/datasets/train/shard.mds",
                0,
                8,
                TransferPayloadFormat.MDS_SHARDS,
                output
        );

        assertEquals(payload.length, bytesRead);
        assertArrayEquals(payload, output.toByteArray());
    }

    @Test
    void networkClientRejectsBulkReadThatCompletesShortOfRequestedLength() throws Exception {
        UUID sessionId = UUID.fromString("00000000-0000-0000-0000-000000000403");
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000404");
        TransferAcceptedResponse transfer = new TransferAcceptedResponse(
                transferId,
                TransferDirection.READ,
                1,
                64,
                128L * 1024 * 1024,
                false
        );
        byte[] payload = "abcd".getBytes(StandardCharsets.UTF_8);
        ReadPlan shortPlan = new ReadPlan(
                Hashing.chunk("file-hash-placeholder".getBytes(StandardCharsets.UTF_8)),
                128,
                new ByteRange(0, 8),
                List.of(new PlannedChunkRead(0, Hashing.chunk(payload), 0, payload.length, 0, payload.length))
        );
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CxnFileNetworkClient client = new CxnFileNetworkClient(
                new ByteArrayInputStream(controlResponses(
                        sessionId,
                        transfer,
                        new ReadFileRangeResponse("/datasets/train/shard.mds", shortPlan)
                )),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(bulkFrames(new BulkDataFrame(
                        transferId,
                        0,
                        0,
                        0,
                        0,
                        true,
                        payload
                ))),
                new ByteArrayOutputStream()
        );
        authenticateForWriteTest(client, sessionId);

        IOException failure = assertThrows(IOException.class, () -> client.readRangeTo(
                "/datasets/train/shard.mds",
                0,
                8,
                TransferPayloadFormat.MDS_SHARDS,
                output
        ));

        assertEquals("bulk read completed with 4 bytes for requested length 8", failure.getMessage());
        assertArrayEquals(payload, output.toByteArray());
    }

    @Test
    void networkClientRejectsTransferAdmissionsThatDoNotMatchRequestedDirection() throws Exception {
        UUID sessionId = UUID.fromString("00000000-0000-0000-0000-000000000406");
        TransferAcceptedResponse writeTransferForReadRequest = new TransferAcceptedResponse(
                UUID.fromString("00000000-0000-0000-0000-000000000407"),
                TransferDirection.WRITE,
                1,
                64,
                128L * 1024 * 1024,
                true
        );
        TransferAcceptedResponse writeTransferWithoutPayloadAdmission = new TransferAcceptedResponse(
                UUID.fromString("00000000-0000-0000-0000-000000000408"),
                TransferDirection.WRITE,
                1,
                64,
                128L * 1024 * 1024,
                false
        );
        CxnFileNetworkClient client = new CxnFileNetworkClient(
                new ByteArrayInputStream(controlResponses(
                        sessionId,
                        writeTransferForReadRequest,
                        writeTransferWithoutPayloadAdmission
                )),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream()
        );
        authenticateForWriteTest(client, sessionId);

        IOException readFailure = assertThrows(IOException.class, () ->
                client.bootstrapRead("/datasets/train/shard.mds"));
        IOException writeFailure = assertThrows(IOException.class, () ->
                client.bootstrapWrite("/outputs/run-1"));

        assertEquals("control read transfer admission does not match requested read", readFailure.getMessage());
        assertEquals("control write transfer admission does not match requested write", writeFailure.getMessage());
    }

    @Test
    void promptsUsernameAndPasswordThroughHiddenTerminalPath() throws Exception {
        FakeTerminal terminal = new FakeTerminal("trainer", "secret".toCharArray());

        try (ClientCredentials credentials = ClientCredentialPrompt.prompt(terminal)) {
            assertEquals("trainer", credentials.username());
            assertEquals("ClientCredentials[username=trainer, password=<hidden>]", credentials.toString());
            assertArrayEquals("secret".toCharArray(), credentials.password());
        }
        assertEquals("CXNFS username: ", terminal.linePrompt);
        assertEquals("CXNFS password: ", terminal.passwordPrompt);
        assertArrayEquals("secret".toCharArray(), terminal.password);
    }

    @Test
    void networkClientAppliesBoundedSocketTimeouts() throws Exception {
        RecordingSocket controlSocket = new RecordingSocket();
        RecordingSocket bulkSocket = new RecordingSocket();
        RecordingSocketFactory socketFactory = new RecordingSocketFactory(controlSocket, bulkSocket);
        InetSocketAddress controlAddress = InetSocketAddress.createUnresolved("control.example.internal", 4509);
        InetSocketAddress bulkAddress = InetSocketAddress.createUnresolved("bulk.example.internal", 4510);
        CxnFileNetworkClientConfig clientConfig = new CxnFileNetworkClientConfig(
                2,
                8,
                1024 * 1024,
                1024 * 1024L,
                1_234,
                5_678
        );

        try (CxnFileNetworkClient ignored = CxnFileNetworkClient.connect(
                controlAddress,
                bulkAddress,
                socketFactory,
                socketFactory,
                clientConfig
        )) {
            assertEquals(controlAddress, controlSocket.connectedAddress);
            assertEquals(bulkAddress, bulkSocket.connectedAddress);
            assertEquals(1_234, controlSocket.connectTimeoutMillis);
            assertEquals(1_234, bulkSocket.connectTimeoutMillis);
            assertEquals(5_678, controlSocket.soTimeoutMillis);
            assertEquals(5_678, bulkSocket.soTimeoutMillis);
            assertTrue(controlSocket.tcpNoDelay);
            assertTrue(bulkSocket.tcpNoDelay);
        }

        assertTrue(controlSocket.closed);
        assertTrue(bulkSocket.closed);
    }

    @Test
    void networkClientReusesLazyBulkWriteSocketForSequentialFrames() throws Exception {
        BulkDataFrame firstFrame = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000201"),
                0,
                0,
                0,
                0,
                true,
                "first".getBytes(StandardCharsets.UTF_8)
        );
        BulkDataFrame secondFrame = new BulkDataFrame(
                firstFrame.transferId(),
                1,
                0,
                1,
                firstFrame.payloadLength(),
                true,
                "second".getBytes(StandardCharsets.UTF_8)
        );
        RecordingSocket controlSocket = new RecordingSocket();
        RecordingSocket readBulkSocket = new RecordingSocket();
        RecordingSocket writeSocket = new RecordingSocket(ackBytes(firstFrame, secondFrame));
        RecordingSocketFactory socketFactory = new RecordingSocketFactory(controlSocket, readBulkSocket, writeSocket);

        try (CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                InetSocketAddress.createUnresolved("control.example.internal", 4509),
                InetSocketAddress.createUnresolved("bulk.example.internal", 4510),
                socketFactory,
                socketFactory,
                new CxnFileNetworkClientConfig(2, 4, 1024, 0, 250, 500)
        )) {
            authenticateForWriteTest(client);

            client.sendWriteFrame(firstFrame);
            client.sendWriteFrame(secondFrame);

            assertEquals(0, socketFactory.remainingSockets());
            assertTrue(writeSocket.outputBytes().length > 0);
        }

        assertTrue(controlSocket.closed);
        assertTrue(readBulkSocket.closed);
        assertTrue(writeSocket.closed);
    }

    @Test
    void networkClientUsesMultipleLazyBulkWriteSocketsForConcurrentFrames() throws Exception {
        BulkDataFrame firstFrame = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000202"),
                0,
                0,
                0,
                0,
                true,
                "first".getBytes(StandardCharsets.UTF_8)
        );
        BulkDataFrame secondFrame = new BulkDataFrame(
                firstFrame.transferId(),
                1,
                0,
                1,
                firstFrame.payloadLength(),
                true,
                "second".getBytes(StandardCharsets.UTF_8)
        );
        RecordingSocket controlSocket = new RecordingSocket();
        RecordingSocket readBulkSocket = new RecordingSocket();
        BlockingAckRecordingSocket firstWriteSocket = new BlockingAckRecordingSocket(ackBytes(firstFrame));
        RecordingSocket secondWriteSocket = new RecordingSocket(ackBytes(secondFrame));
        RecordingSocketFactory socketFactory = new RecordingSocketFactory(
                controlSocket,
                readBulkSocket,
                firstWriteSocket,
                secondWriteSocket
        );

        try (CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                InetSocketAddress.createUnresolved("control.example.internal", 4509),
                InetSocketAddress.createUnresolved("bulk.example.internal", 4510),
                socketFactory,
                socketFactory,
                new CxnFileNetworkClientConfig(2, 4, 1024, 0, 250, 500)
        )) {
            authenticateForWriteTest(client);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> firstSend = executor.submit(() -> {
                    client.sendWriteFrame(firstFrame);
                    return null;
                });
                assertTrue(firstWriteSocket.awaitAckReadStarted());
                Future<?> secondSend = executor.submit(() -> {
                    client.sendWriteFrame(secondFrame);
                    return null;
                });

                secondSend.get(5, TimeUnit.SECONDS);
                firstWriteSocket.releaseAck();
                firstSend.get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }

            assertEquals(0, socketFactory.remainingSockets());
            assertTrue(firstWriteSocket.outputBytes().length > 0);
            assertTrue(secondWriteSocket.outputBytes().length > 0);
        }
    }

    @Test
    void networkClientUnblocksPendingBulkWriteLeaseWhenClosed() throws Exception {
        BulkDataFrame firstFrame = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000203"),
                0,
                0,
                0,
                0,
                true,
                "first".getBytes(StandardCharsets.UTF_8)
        );
        BulkDataFrame secondFrame = new BulkDataFrame(
                firstFrame.transferId(),
                0,
                0,
                1,
                firstFrame.payloadLength(),
                true,
                "second".getBytes(StandardCharsets.UTF_8)
        );
        RecordingSocket controlSocket = new RecordingSocket();
        RecordingSocket readBulkSocket = new RecordingSocket();
        BlockingAckRecordingSocket writeSocket = new BlockingAckRecordingSocket(ackBytes(firstFrame));
        RecordingSocketFactory socketFactory = new RecordingSocketFactory(controlSocket, readBulkSocket, writeSocket);

        CxnFileNetworkClient client = CxnFileNetworkClient.connect(
                InetSocketAddress.createUnresolved("control.example.internal", 4509),
                InetSocketAddress.createUnresolved("bulk.example.internal", 4510),
                socketFactory,
                socketFactory,
                new CxnFileNetworkClientConfig(1, 4, 1024, 0, 5000, 5000)
        );
        authenticateForWriteTest(client);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstSend = executor.submit(() -> {
                client.sendWriteFrame(firstFrame);
                return null;
            });
            assertTrue(writeSocket.awaitAckReadStarted());
            Future<?> secondSend = executor.submit(() -> {
                client.sendWriteFrame(secondFrame);
                return null;
            });

            client.close();
            java.util.concurrent.ExecutionException failure = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> secondSend.get(5, TimeUnit.SECONDS)
            );
            assertTrue(failure.getCause() instanceof IOException);
            String closeMsg = failure.getCause().getMessage() == null ? "" : failure.getCause().getMessage().toLowerCase(java.util.Locale.ROOT);
            assertTrue(closeMsg.contains("close") || closeMsg.contains("broken") || closeMsg.contains("reset") || closeMsg.contains("time"));
            writeSocket.releaseAck();
            firstSend.get(5, TimeUnit.SECONDS);
        } finally {
            writeSocket.releaseAck();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void networkClientClosesControlSocketWhenBulkConnectFails() {
        RecordingSocket controlSocket = new RecordingSocket();
        RecordingSocketFactory controlSocketFactory = new RecordingSocketFactory(controlSocket);
        IOException failure = assertThrows(IOException.class, () -> CxnFileNetworkClient.connect(
                InetSocketAddress.createUnresolved("control.example.internal", 4509),
                InetSocketAddress.createUnresolved("bulk.example.internal", 4510),
                controlSocketFactory,
                new FailingSocketFactory("bulk unavailable"),
                new CxnFileNetworkClientConfig(1, 1, 1024, 0, 250, 500)
        ));

        assertTrue(failure.getMessage().contains("failed to connect stream bulk endpoint bulk.example.internal:4510"));
        assertTrue(failure.getMessage().contains("bulk unavailable"));
        assertTrue(controlSocket.closed);
    }

    @Test
    void networkClientCloseIsIdempotentAndRejectsLaterOperations() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        CxnFileNetworkClient client = new CxnFileNetworkClient(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream(),
                CxnFileNetworkClientConfig.defaults(),
                closeCount::incrementAndGet
        );
        authenticateForWriteTest(client);

        client.close();
        client.close();

        assertEquals(1, closeCount.get());
        IOException readFailure = assertThrows(
                IOException.class,
                () -> client.readRange("/datasets/train/shard.mds", 0, 1)
        );
        assertEquals("network client is closed", readFailure.getMessage());
        IOException writeFailure = assertThrows(
                IOException.class,
                () -> client.sendWriteFrame(new BulkDataFrame(
                        UUID.fromString("00000000-0000-0000-0000-000000000204"),
                        0,
                        0,
                        0,
                        0,
                        true,
                        "payload".getBytes(StandardCharsets.UTF_8)
                ))
        );
        assertEquals("network client is closed", writeFailure.getMessage());
    }

    @Test
    void networkClientRejectsMismatchedWriteEntryAcknowledgement() throws Exception {
        UUID session = UUID.fromString("00000000-0000-0000-0000-000000000311");
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000312");
        TransferEntryUpdate request = new TransferEntryUpdate(
                transferId,
                0,
                TransferEntryAction.CREATE_OR_REPLACE,
                "/outputs/run-1/model.safetensors",
                "",
                1024,
                TransferPayloadFormat.SAFETENSORS
        );
        CxnFileNetworkClient client = networkClientWithControlResponses(
                session,
                new TransferEntryAcceptedResponse(transferId, 0, "/outputs/run-2/model.safetensors")
        );
        authenticateForWriteTest(client, session);

        IOException failure = assertThrows(IOException.class, () -> client.acceptWriteEntry(request));

        assertTrue(failure.getMessage().contains("write entry acknowledgement"));
    }

    @Test
    void networkClientRejectsMismatchedWriteMetadataAcknowledgement() throws Exception {
        UUID session = UUID.fromString("00000000-0000-0000-0000-000000000321");
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000322");
        ContentHash chunkHash = Hashing.chunk("chunk".getBytes(StandardCharsets.UTF_8));
        TransferMetadataUpdate request = new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                1024,
                List.of(chunkHash)
        );
        CxnFileNetworkClient client = networkClientWithControlResponses(
                session,
                new TransferMetadataAcceptedResponse(transferId, 0, 0, 2, 1, true)
        );
        authenticateForWriteTest(client, session);

        IOException failure = assertThrows(IOException.class, () -> client.acceptWriteMetadata(request));

        assertTrue(failure.getMessage().contains("write metadata acknowledgement"));
    }

    @Test
    void networkClientRejectsMismatchedWriteEntryCommitAcknowledgement() throws Exception {
        UUID session = UUID.fromString("00000000-0000-0000-0000-000000000331");
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000332");
        ContentHash expectedHash = Hashing.chunk("expected".getBytes(StandardCharsets.UTF_8));
        ContentHash wrongHash = Hashing.chunk("wrong".getBytes(StandardCharsets.UTF_8));
        TransferEntryCommitRequest request = new TransferEntryCommitRequest(
                transferId,
                0,
                "/outputs/run-1/model.safetensors",
                expectedHash,
                1024
        );
        CxnFileNetworkClient client = networkClientWithControlResponses(
                session,
                new TransferEntryCommittedResponse(
                        transferId,
                        0,
                        "/outputs/run-1/model.safetensors",
                        wrongHash,
                        1024
                )
        );
        authenticateForWriteTest(client, session);

        IOException failure = assertThrows(IOException.class, () -> client.commitWriteEntry(request));

        assertTrue(failure.getMessage().contains("write entry commit acknowledgement"));
    }

    @Test
    void networkClientRejectsMismatchedWriteTransferCommitAcknowledgement() throws Exception {
        UUID session = UUID.fromString("00000000-0000-0000-0000-000000000341");
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000342");
        ContentHash expectedHash = Hashing.transfer(
                List.of(Hashing.chunk("expected".getBytes(StandardCharsets.UTF_8))),
                List.of(1024L)
        );
        ContentHash wrongHash = Hashing.transfer(
                List.of(Hashing.chunk("wrong".getBytes(StandardCharsets.UTF_8))),
                List.of(1024L)
        );
        TransferCommitRequest request = new TransferCommitRequest(
                transferId,
                "/outputs/run-1",
                expectedHash,
                1024
        );
        CxnFileNetworkClient client = networkClientWithControlResponses(
                session,
                new TransferCommittedResponse(transferId, wrongHash, 1024)
        );
        authenticateForWriteTest(client, session);

        IOException failure = assertThrows(IOException.class, () -> client.commitWriteTransfer(request));

        assertTrue(failure.getMessage().contains("write transfer commit acknowledgement"));
    }

    @Test
    void clientAuthenticatorSendsUsernameFirstAndPasswordProofSecond() {
        ClientAuthenticator authenticator = new ClientAuthenticator();
        byte[] nonce = PasswordAuthProof.newNonce();
        AuthChallenge challenge = new AuthChallenge(
                UUID.randomUUID(),
                AuthMechanism.USERNAME_PASSWORD,
                "trainer",
                nonce,
                PasswordAuthProof.PROOF_ALGORITHM,
                2
        );

        try (ClientCredentials credentials = new ClientCredentials("trainer", "secret".toCharArray())) {
            AuthStart start = authenticator.start(credentials);
            AuthProof proof = authenticator.answer(challenge, credentials);

            assertEquals(new AuthStart(AuthMechanism.USERNAME_PASSWORD, "trainer"), start);
            assertEquals("trainer", proof.principal());
            assertTrue(AuthenticationPolicy.requireUsernamePassword(Map.of("trainer", "secret"))
                    .authenticateUsernamePasswordProof("trainer", challenge.nonce(), challenge.iterations(), proof.proof()));
        }
    }

    private static CxnFileServerConfig config() {
        return config(4509, 4510);
    }

    private static CxnFileServerConfig config(int controlPort, int containerPort) {
        return config(Path.of("cxnfs-test-server.p12"), controlPort, containerPort);
    }

    private static CxnFileServerConfig config(Path keyStore, int controlPort, int containerPort) {
        return new CxnFileServerConfig(
                Path.of("."),
                AuthenticationPolicy.requireUsernamePassword(Map.of("trainer", "secret")),
                new TransportBinding(
                        TransportRole.FILE_SERVER_CONTROL,
                        new InetSocketAddress("127.0.0.1", controlPort),
                        TransportSecurityConfig.tls13ForTcp(keyStore)
                ),
                new TransportBinding(
                        TransportRole.CONTAINER_TRANSFER,
                        new InetSocketAddress("127.0.0.1", containerPort),
                        TransportSecurityConfig.quicTls13ForUdp(keyStore)
                )
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

    private static void generateKeyStore(Path keyStore, char[] password) throws Exception {
        Process process = new ProcessBuilder(
                "keytool",
                "-genkeypair",
                "-alias",
                "cxnfs-test",
                "-keyalg",
                "RSA",
                "-storetype",
                "PKCS12",
                "-keystore",
                keyStore.toString(),
                "-storepass",
                new String(password),
                "-keypass",
                new String(password),
                "-dname",
                "CN=localhost",
                "-validity",
                "1",
                "-noprompt"
        ).redirectErrorStream(true).start();
        String output;
        try (InputStream input = process.getInputStream()) {
            output = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
    }

    private static KeyStore loadKeyStore(Path keyStorePath, char[] password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(keyStorePath)) {
            keyStore.load(input, password);
        }
        return keyStore;
    }

    private static void authenticateForWriteTest(CxnFileNetworkClient client) throws Exception {
        authenticateForWriteTest(client, UUID.fromString("00000000-0000-0000-0000-000000000301"));
    }

    private static void authenticateForWriteTest(CxnFileNetworkClient client, UUID session) throws Exception {
        java.lang.reflect.Field sessionId = CxnFileNetworkClient.class.getDeclaredField("sessionId");
        sessionId.setAccessible(true);
        sessionId.set(client, session);
    }

    private static CxnFileNetworkClient networkClientWithControlResponses(
            UUID sessionId,
            ControlPayload... payloads
    ) throws IOException {
        return new CxnFileNetworkClient(
                new ByteArrayInputStream(controlResponses(sessionId, payloads)),
                new ByteArrayOutputStream(),
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream()
        );
    }

    private static byte[] controlResponses(UUID sessionId, ControlPayload... payloads) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (ControlPayload payload : payloads) {
            ControlEnvelope<? extends ControlPayload> envelope = ControlEnvelope.authenticated(sessionId, payload);
            byte[] payloadBytes = ControlPayloadCodec.encode(envelope.payload());
            ControlFrame frame = ControlFrame.fromEnvelope(envelope, payloadBytes);
            ControlFrameStream.write(output, ControlFrameCodec.encode(frame));
        }
        return output.toByteArray();
    }

    private static byte[] bulkFrames(BulkDataFrame... frames) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (BulkDataFrame frame : frames) {
            BulkFrameStream.write(output, com.fortrow.cxnfs.transport.BulkDataFrameCodec.encode(frame));
        }
        return output.toByteArray();
    }

    private static byte[] ackBytes(BulkDataFrame... frames) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (BulkDataFrame frame : frames) {
            BulkWriteAckStream.write(output, BulkWriteAckCodec.encode(new BulkWriteAck(
                    frame.key(),
                    frame.payloadLength(),
                    false,
                    StagedPayloadStorage.MEMORY
            )));
        }
        return output.toByteArray();
    }

    private static final class FakeTerminal implements ClientCredentialPrompt.Terminal {
        private final String username;
        private final char[] password;
        private String linePrompt;
        private String passwordPrompt;

        private FakeTerminal(String username, char[] password) {
            this.username = username;
            this.password = password.clone();
        }

        @Override
        public String readLine(String prompt) {
            linePrompt = prompt;
            return username;
        }

        @Override
        public char[] readPassword(String prompt) {
            passwordPrompt = prompt;
            return password.clone();
        }
    }

    private static class RecordingSocket extends Socket {
        private final java.io.InputStream input;
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private SocketAddress connectedAddress;
        private int connectTimeoutMillis;
        private int soTimeoutMillis;
        private boolean tcpNoDelay;
        private boolean closed;

        private RecordingSocket() {
            this(new byte[0]);
        }

        private RecordingSocket(byte[] inputBytes) {
            this.input = new ByteArrayInputStream(inputBytes);
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) {
            this.connectedAddress = endpoint;
            this.connectTimeoutMillis = timeout;
        }

        @Override
        public void setTcpNoDelay(boolean on) {
            this.tcpNoDelay = on;
        }

        @Override
        public void setSoTimeout(int timeout) {
            this.soTimeoutMillis = timeout;
        }

        @Override
        public java.io.InputStream getInputStream() {
            return input;
        }

        @Override
        public ByteArrayOutputStream getOutputStream() {
            return output;
        }

        @Override
        public synchronized void close() {
            this.closed = true;
        }

        byte[] outputBytes() {
            return output.toByteArray();
        }
    }

    private static final class BlockingAckRecordingSocket extends RecordingSocket {
        private final CountDownLatch ackReadStarted = new CountDownLatch(1);
        private final CountDownLatch releaseAck = new CountDownLatch(1);
        private final byte[] ackBytes;

        private BlockingAckRecordingSocket(byte[] ackBytes) {
            this.ackBytes = ackBytes.clone();
        }

        @Override
        public java.io.InputStream getInputStream() {
            return new java.io.InputStream() {
                private final ByteArrayInputStream delegate = new ByteArrayInputStream(ackBytes);
                private boolean waiting = true;

                @Override
                public int read() throws IOException {
                    waitForReleaseOnce();
                    return delegate.read();
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    waitForReleaseOnce();
                    return delegate.read(bytes, offset, length);
                }

                private void waitForReleaseOnce() throws IOException {
                    if (!waiting) {
                        return;
                    }
                    waiting = false;
                    ackReadStarted.countDown();
                    try {
                        if (!releaseAck.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("timed out waiting to release test ack");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted waiting to release test ack", e);
                    }
                }
            };
        }

        private boolean awaitAckReadStarted() throws InterruptedException {
            return ackReadStarted.await(5, TimeUnit.SECONDS);
        }

        private void releaseAck() {
            releaseAck.countDown();
        }
    }

    private static final class RecordingSocketFactory extends SocketFactory {
        private final Queue<Socket> sockets;

        private RecordingSocketFactory(Socket... sockets) {
            this.sockets = new ArrayDeque<>(List.of(sockets));
        }

        @Override
        public Socket createSocket() throws IOException {
            Socket socket = sockets.poll();
            if (socket == null) {
                throw new IOException("no test socket available");
            }
            return socket;
        }

        private int remainingSockets() {
            return sockets.size();
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            throw new IOException("client must create unconnected sockets for timeout control");
        }

        @Override
        public Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort)
                throws IOException {
            throw new IOException("client must create unconnected sockets for timeout control");
        }

        @Override
        public Socket createSocket(java.net.InetAddress host, int port) throws IOException {
            throw new IOException("client must create unconnected sockets for timeout control");
        }

        @Override
        public Socket createSocket(
                java.net.InetAddress address,
                int port,
                java.net.InetAddress localAddress,
                int localPort
        ) throws IOException {
            throw new IOException("client must create unconnected sockets for timeout control");
        }
    }

    private static final class FailingSocketFactory extends SocketFactory {
        private final String message;

        private FailingSocketFactory(String message) {
            this.message = message;
        }

        @Override
        public Socket createSocket() throws IOException {
            throw new IOException(message);
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            throw new IOException(message);
        }

        @Override
        public Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort)
                throws IOException {
            throw new IOException(message);
        }

        @Override
        public Socket createSocket(java.net.InetAddress host, int port) throws IOException {
            throw new IOException(message);
        }

        @Override
        public Socket createSocket(
                java.net.InetAddress address,
                int port,
                java.net.InetAddress localAddress,
                int localPort
        ) throws IOException {
            throw new IOException(message);
        }
    }
}
