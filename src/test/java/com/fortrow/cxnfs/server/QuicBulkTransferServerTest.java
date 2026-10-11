package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import com.fortrow.cxnfs.client.ClientAuthenticator;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.client.QuicBulkTransferClientChannel;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkReadSessionHandler;
import com.fortrow.cxnfs.transport.BulkReadTransferCoordinator;
import com.fortrow.cxnfs.transport.BulkTransferSessionHandler;
import com.fortrow.cxnfs.transport.BulkWriteAck;
import com.fortrow.cxnfs.transport.CordPoolConfig;
import com.fortrow.cxnfs.transport.StagedPayload;
import com.fortrow.cxnfs.transport.StagedPayloadStorage;
import com.fortrow.cxnfs.transport.TlsCipherSuite;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.kwik.agent15.TlsConstants;
import tech.kwik.agent15.engine.ServerMessageSender;
import tech.kwik.agent15.engine.TlsServerEngine;
import tech.kwik.agent15.engine.TlsServerEngineFactory;
import tech.kwik.agent15.engine.TlsStatusEventHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class QuicBulkTransferServerTest {
    @TempDir
    Path tempDir;

    @Test
    void exposesLockedCipherSuiteAndAgent15ServerEngineSupportsOnlyThatSuite() throws Exception {
        Path keyStorePath = tempDir.resolve("server-cipher.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        TlsServerEngineFactory factory = new TlsServerEngineFactory(keyStore, "cxnfs-test", password);
        try {
            TlsServerEngine engine = factory.createServerEngine(
                    noOpProxy(ServerMessageSender.class),
                    noOpProxy(TlsStatusEventHandler.class)
            );

            assertEquals(TlsCipherSuite.TLS_AES_128_GCM_SHA256, QuicBulkTransferServer.CIPHER_SUITE);
            assertEquals(
                    Set.of(TlsConstants.CipherSuite.TLS_AES_128_GCM_SHA256),
                    supportedCiphers(engine)
            );
        } finally {
            factory.dispose();
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void servesAdmittedBulkReadRequestsOverQuicStreams() throws Exception {
        Path keyStorePath = tempDir.resolve("server.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        Fixture fixture = fixture(keyStorePath);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());
        AuthAccepted accepted = authenticate(runtime.controlDispatcher());
        UUID transferId = UUID.randomUUID();
        TransferAcceptedResponse transfer = bootstrapRead(
                runtime.controlDispatcher(),
                accepted,
                transferId,
                fixture.manifest()
        );
        ReadPlan plan = fixture.server().readPlan("/datasets/train/shard.00000.mds", 0, 16);

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(runtime, keyStore, password)) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(2, 4, 64 * 1024, fixture.manifest().size(), 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                long bytesRead = client.read(
                        new BulkReadRequest(
                                accepted.sessionId(),
                                transferId,
                                "/datasets/train/shard.00000.mds",
                                0,
                                16,
                                transfer.acceptedInitialCords()
                        ),
                        transfer,
                        plan,
                        output
                );

                assertEquals(16, bytesRead);
                assertArrayEquals("abcdefghijklmnop".getBytes(StandardCharsets.UTF_8), output.toByteArray());
                assertEquals(2, client.lastReadStreamCount());
                assertEquals(2, client.attemptedReadStreamCount());
                assertEquals(2, client.readStreamCount());
                assertEquals(1, client.successfulReadOperationCount());
                assertEquals(0, client.failedReadOperationCount());
                assertTrue(client.connectionReusedForStreams());
                assertTrue(server.totalStreamCount() >= 2);
                assertTrue(server.lastStreamError().isEmpty());
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void fansBulkReadAcrossAcceptedQuicCordStreams() throws Exception {
        Path keyStorePath = tempDir.resolve("server-cords.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        Fixture fixture = fixture(keyStorePath);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());
        AuthAccepted accepted = authenticate(runtime.controlDispatcher());
        UUID transferId = UUID.randomUUID();
        TransferAcceptedResponse transfer = bootstrapRead(
                runtime.controlDispatcher(),
                accepted,
                transferId,
                fixture.manifest()
        );
        ReadPlan plan = fixture.server().readPlan("/datasets/train/shard.00000.mds", 0, 26);

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(runtime, keyStore, password)) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(4, 4, 64 * 1024, fixture.manifest().size(), 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                long bytesRead = client.read(
                        new BulkReadRequest(
                                accepted.sessionId(),
                                transferId,
                                "/datasets/train/shard.00000.mds",
                                0,
                                26,
                                transfer.acceptedInitialCords()
                        ),
                        transfer,
                        plan,
                        output
                );

                assertEquals(26, bytesRead);
                assertArrayEquals("abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8), output.toByteArray());
                assertEquals(4, client.lastReadStreamCount());
                assertEquals(4, client.attemptedReadStreamCount());
                assertEquals(4, client.readStreamCount());
                assertEquals(1, client.successfulReadOperationCount());
                assertEquals(0, client.failedReadOperationCount());
                Map<String, Object> evidence = client.readPathEvidence();
                assertEquals(4, evidence.get("read_partition_count"));
                assertEquals(4, evidence.get("read_partition_chunk_count"));
                assertEquals(26L, evidence.get("read_partition_payload_bytes"));
                assertEquals(26L, evidence.get("read_partition_requested_bytes"));
                assertEquals(true, evidence.get("read_partition_covers_plan"));
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> partitions = (List<Map<String, Object>>) evidence.get("read_partitions");
                assertEquals(4, partitions.size());
                assertEquals(List.of(0L, 8L, 16L, 24L), partitions.stream().map(item -> item.get("offset")).toList());
                assertEquals(List.of(8L, 8L, 8L, 2L), partitions.stream().map(item -> item.get("length")).toList());
                assertEquals(List.of(1, 1, 1, 1), partitions.stream().map(item -> item.get("chunk_count")).toList());
                assertTrue(client.connectionReusedForStreams());
                assertTrue(server.totalStreamCount() >= 4);
                assertTrue(server.maxActiveStreamCount() >= 2);
                assertTrue(server.lastStreamError().isEmpty());
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void clampsQuicReadRequestCordsToAdmittedMaximum() throws Exception {
        Path keyStorePath = tempDir.resolve("server-clamped-cords.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        Fixture fixture = fixture(keyStorePath, CordPoolConfig.fixed(2));
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());
        AuthAccepted accepted = authenticate(runtime.controlDispatcher());
        UUID transferId = UUID.randomUUID();
        TransferAcceptedResponse transfer = bootstrapRead(
                runtime.controlDispatcher(),
                accepted,
                transferId,
                fixture.manifest()
        );
        ReadPlan plan = fixture.server().readPlan("/datasets/train/shard.00000.mds", 0, 26);

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(runtime, keyStore, password)) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(8, 8, 64 * 1024, fixture.manifest().size(), 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                long bytesRead = client.read(
                        new BulkReadRequest(
                                accepted.sessionId(),
                                transferId,
                                "/datasets/train/shard.00000.mds",
                                0,
                                26,
                                8
                        ),
                        transfer,
                        plan,
                        output
                );

                assertEquals(2, transfer.acceptedMaxCords());
                assertEquals(26, bytesRead);
                assertArrayEquals("abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8), output.toByteArray());
                assertEquals(2, client.lastReadStreamCount());
                assertEquals(2, client.attemptedReadStreamCount());
                assertEquals(2, client.readStreamCount());
                assertEquals(1, client.successfulReadOperationCount());
                assertEquals(0, client.failedReadOperationCount());
                assertTrue(server.totalStreamCount() >= 2);
                assertTrue(server.lastStreamError().isEmpty());
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void closesQuicClientChannelAfterFailedReadStream() throws Exception {
        Path keyStorePath = tempDir.resolve("server-failed-read.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        Fixture fixture = fixture(keyStorePath);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(fixture.server());
        AuthAccepted accepted = authenticate(runtime.controlDispatcher());
        UUID transferId = UUID.randomUUID();
        TransferAcceptedResponse unadmittedTransfer = new TransferAcceptedResponse(
                transferId,
                TransferDirection.READ,
                2,
                4,
                64 * 1024 * 1024,
                false
        );
        ReadPlan plan = fixture.server().readPlan("/datasets/train/shard.00000.mds", 0, 16);

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(runtime, keyStore, password)) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(2, 4, 64 * 1024, fixture.manifest().size(), 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                IOException failedRead = assertThrows(IOException.class, () -> client.read(
                        new BulkReadRequest(
                                accepted.sessionId(),
                                transferId,
                                "/datasets/train/shard.00000.mds",
                                0,
                                16,
                                2
                        ),
                        unadmittedTransfer,
                        plan,
                        new ByteArrayOutputStream()
                ));
                assertTrue(failedRead.getMessage().contains("QUIC bulk stream ended")
                        || failedRead.getMessage().contains("Connection closed"));
                assertTrue(client.closed());
                assertEquals(2, client.attemptedReadStreamCount());
                assertEquals(0, client.readStreamCount());
                assertEquals(0, client.successfulReadOperationCount());
                assertEquals(1, client.failedReadOperationCount());
                assertTrue(server.lastStreamError().isPresent());

                IOException closedRead = assertThrows(IOException.class, () -> client.read(
                        new BulkReadRequest(
                                accepted.sessionId(),
                                transferId,
                                "/datasets/train/shard.00000.mds",
                                0,
                                16,
                                2
                        ),
                        unadmittedTransfer,
                        plan,
                        new ByteArrayOutputStream()
                ));
                assertTrue(closedRead.getMessage().contains("closed"));
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void acknowledgesBulkWriteFramesOverQuicStreams() throws Exception {
        Path keyStorePath = tempDir.resolve("server-write.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        BulkDataFrame frame = new BulkDataFrame(
                UUID.fromString("00000000-0000-0000-0000-000000000456"),
                1,
                0,
                7,
                4096,
                true,
                "checkpoint chunk".getBytes(StandardCharsets.UTF_8)
        );
        BulkTransferSessionHandler handler = new BulkTransferSessionHandler(
                readHandler(keyStorePath),
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

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                keyStore,
                password,
                4
        )) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(2, 4, 64 * 1024, 0, 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                BulkWriteAck ack = client.sendWriteFrame(BulkDataFrameCodec.encode(frame));

                assertEquals(frame.key(), ack.key());
                assertEquals(frame.payloadLength(), ack.payloadLength());
                assertEquals(StagedPayloadStorage.MEMORY, ack.storage());
                assertEquals(1, client.attemptedWriteStreamCount());
                assertEquals(1, client.writeStreamCount());
                assertEquals(1, client.successfulWriteOperationCount());
                assertEquals(0, client.failedWriteOperationCount());
                assertEquals(1, client.writeAckReceivedCount());
                assertEquals(1, client.writeAckVerifiedCount());
                assertEquals(0, client.writeAckVerificationFailureCount());
                assertEquals(0, client.writeAckMetadataVerifiedCount());
                assertEquals(1, client.writeAckMetadataPendingCount());
                assertEquals(1, client.writeAckMemoryCount());
                assertEquals(0, client.writeAckSpillCount());
                assertTrue(client.writeAckFrameIdentityVerified());
                assertTrue(client.writeAckStreamCompletionVerified());
                assertFalse(client.writeAckStreamEofVerified());
                Map<String, Object> evidence = client.writePathEvidence();
                assertEquals("cxnfs.quic-bulk-write-transport-evidence.v1", evidence.get("format"));
                assertEquals(1, evidence.get("write_acks_received"));
                assertEquals(1, evidence.get("write_acks_verified"));
                assertEquals(0, evidence.get("write_ack_verification_failures"));
                assertEquals(true, evidence.get("write_ack_frame_identity_verified"));
                assertEquals(true, evidence.get("write_ack_stream_completion_verified"));
                assertEquals(false, evidence.get("write_ack_stream_eof_verified"));
                assertEquals(1, evidence.get("write_ack_subscribe_streams_created"));
                assertEquals(1, evidence.get("write_ack_subscribe_streams_ready"));
                assertEquals(true, evidence.get("write_ack_metadata_pending_observed"));
                assertTrue(server.lastStreamError().isEmpty());
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void acknowledgesMultipleBulkWriteFramesOnOneQuicStream() throws Exception {
        Path keyStorePath = tempDir.resolve("server-batched-write.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000654");
        List<BulkDataFrame> frames = List.of(
                writeFrame(transferId, 0, 0, "checkpoint chunk 0"),
                writeFrame(transferId, 1, 1, "checkpoint chunk 1"),
                writeFrame(transferId, 2, 2, "checkpoint chunk 2"),
                writeFrame(transferId, 3, 3, "checkpoint chunk 3")
        );
        BulkTransferSessionHandler handler = new BulkTransferSessionHandler(
                readHandler(keyStorePath),
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

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                keyStore,
                password,
                4
        )) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(4, 4, 64 * 1024, 0, 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                List<byte[]> encodedFrames = new ArrayList<>();
                for (BulkDataFrame frame : frames) {
                    encodedFrames.add(BulkDataFrameCodec.encode(frame));
                }
                List<BulkWriteAck> acks = client.sendWriteFrames(encodedFrames);

                assertEquals(frames.size(), acks.size());
                for (int index = 0; index < frames.size(); index++) {
                    BulkDataFrame frame = frames.get(index);
                    BulkWriteAck ack = acks.get(index);
                    assertEquals(frame.key(), ack.key());
                    assertEquals(frame.payloadLength(), ack.payloadLength());
                    assertEquals(StagedPayloadStorage.MEMORY, ack.storage());
                }
                assertEquals(1, client.attemptedWriteStreamCount());
                assertEquals(frames.size(), client.attemptedWriteFrameCount());
                assertEquals(1, client.writeStreamCount());
                assertEquals(1, client.successfulWriteOperationCount());
                assertEquals(0, client.failedWriteOperationCount());
                assertEquals(frames.size(), client.writeAckReceivedCount());
                assertEquals(frames.size(), client.writeAckVerifiedCount());
                assertEquals(0, client.writeAckVerificationFailureCount());
                assertTrue(client.writeAckFrameIdentityVerified());
                assertTrue(client.writeAckStreamCompletionVerified());
                assertFalse(client.writeAckStreamEofVerified());
                assertTrue(server.lastStreamError().isEmpty());
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Test
    void reusesSingleQuicConnectionForConcurrentBulkWriteCordStreams() throws Exception {
        Path keyStorePath = tempDir.resolve("server-concurrent-write.p12");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000789");
        List<BulkDataFrame> frames = List.of(
                writeFrame(transferId, 0, 0, "checkpoint chunk 0"),
                writeFrame(transferId, 1, 1, "checkpoint chunk 1"),
                writeFrame(transferId, 2, 2, "checkpoint chunk 2"),
                writeFrame(transferId, 3, 3, "checkpoint chunk 3")
        );
        CountDownLatch arrived = new CountDownLatch(frames.size());
        BulkTransferSessionHandler handler = new BulkTransferSessionHandler(
                readHandler(keyStorePath),
                value -> {
                    arrived.countDown();
                    try {
                        if (!arrived.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("timed out waiting for concurrent QUIC write streams");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted waiting for concurrent QUIC write streams", e);
                    }
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

        try (QuicBulkTransferServer server = QuicBulkTransferServer.bind(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                keyStore,
                password,
                4
        )) {
            server.start();
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", server.boundAddress().getPort());
            try (QuicBulkTransferClientChannel client = QuicBulkTransferClientChannel.connect(
                    clientAddress,
                    new CxnFileNetworkClientConfig(4, 4, 64 * 1024, 0, 5_000, 5_000),
                    Optional.of(keyStore),
                    false
            )) {
                ExecutorService executor = Executors.newFixedThreadPool(frames.size());
                try {
                    List<Future<BulkWriteAck>> futures = new ArrayList<>();
                    for (BulkDataFrame frame : frames) {
                        futures.add(executor.submit(() -> client.sendWriteFrame(BulkDataFrameCodec.encode(frame))));
                    }
                    for (int index = 0; index < frames.size(); index++) {
                        BulkDataFrame frame = frames.get(index);
                        BulkWriteAck ack = futures.get(index).get(5, TimeUnit.SECONDS);
                        assertEquals(frame.key(), ack.key());
                        assertEquals(frame.payloadLength(), ack.payloadLength());
                        assertEquals(StagedPayloadStorage.MEMORY, ack.storage());
                    }
                } finally {
                    executor.shutdownNow();
                }

                assertEquals(frames.size(), client.attemptedWriteStreamCount());
                assertEquals(frames.size(), client.writeStreamCount());
                assertEquals(frames.size(), client.successfulWriteOperationCount());
                assertEquals(0, client.failedWriteOperationCount());
                assertEquals(frames.size(), client.writeAckReceivedCount());
                assertEquals(frames.size(), client.writeAckVerifiedCount());
                assertEquals(0, client.writeAckVerificationFailureCount());
                assertEquals(0, client.writeAckMetadataVerifiedCount());
                assertEquals(frames.size(), client.writeAckMetadataPendingCount());
                assertEquals(frames.size(), client.writeAckMemoryCount());
                assertEquals(0, client.writeAckSpillCount());
                assertTrue(client.writeAckFrameIdentityVerified());
                assertTrue(client.writeAckStreamCompletionVerified());
                assertFalse(client.writeAckStreamEofVerified());
                assertEquals(0, client.activeWriteStreamCount());
                assertTrue(client.maxActiveWriteStreamCount() >= frames.size());
                assertTrue(client.connectionReusedForStreams());
                Map<String, Object> evidence = client.writePathEvidence();
                assertEquals(frames.size(), evidence.get("attempted_write_streams"));
                assertEquals(frames.size(), evidence.get("write_streams_created"));
                assertEquals(frames.size(), evidence.get("write_acks_received"));
                assertEquals(frames.size(), evidence.get("write_acks_verified"));
                assertEquals(0, evidence.get("write_ack_verification_failures"));
                assertEquals(true, evidence.get("write_ack_frame_identity_verified"));
                assertEquals(true, evidence.get("write_ack_stream_completion_verified"));
                assertEquals(false, evidence.get("write_ack_stream_eof_verified"));
                assertEquals(1, evidence.get("write_ack_subscribe_streams_created"));
                assertEquals(1, evidence.get("write_ack_subscribe_streams_ready"));
                assertEquals(true, evidence.get("concurrent_write_streams_observed"));
                assertTrue(server.totalStreamCount() >= frames.size());
                assertTrue(server.maxActiveStreamCount() >= frames.size());
                assertTrue(server.lastStreamError().isEmpty());
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static <T> T noOpProxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (proxy, method, args) -> method.getReturnType().equals(boolean.class) ? false : null
        ));
    }

    @SuppressWarnings("unchecked")
    private static Set<TlsConstants.CipherSuite> supportedCiphers(TlsServerEngine engine) throws Exception {
        Field field = engine.getClass().getDeclaredField("supportedCiphers");
        field.setAccessible(true);
        return Set.copyOf((Set<TlsConstants.CipherSuite>) field.get(engine));
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

    private static TransferAcceptedResponse bootstrapRead(
            ControlSessionDispatcher dispatcher,
            AuthAccepted accepted,
            UUID transferId,
            FileManifest manifest
    ) {
        return (TransferAcceptedResponse) dispatcher.handle(ControlEnvelope.authenticated(
                accepted.sessionId(),
                new TransferBootstrapRequest(
                        transferId,
                        TransferDirection.READ,
                        "/datasets/train/shard.00000.mds",
                        TransferPayloadFormat.MDS_SHARDS,
                        2,
                        64,
                        64 * 1024 * 1024,
                        manifest.size()
                )
        )).payload();
    }

    private static Fixture fixture(Path keyStore) throws Exception {
        return fixture(keyStore, CordPoolConfig.xetLikeDefaults());
    }

    private static Fixture fixture(Path keyStore, CordPoolConfig cordPool) throws Exception {
        CxnfsRepository repository = new CxnfsRepository(QuicBulkTransferServerTest::fixedEightByteChunks);
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/datasets/train/shard.00000.mds", data);
        return new Fixture(new CxnFileServer(config(keyStore, cordPool), repository), manifest);
    }

    private static BulkReadSessionHandler readHandler(Path keyStore) {
        CxnFileServer server = new CxnFileServer(config(keyStore), new CxnfsRepository(data -> java.util.List.of()));
        return new BulkReadSessionHandler(new BulkReadTransferCoordinator(server, ignored -> Optional.empty()));
    }

    private static java.util.List<Chunk> fixedEightByteChunks(byte[] data) {
        java.util.List<Chunk> chunks = new ArrayList<>();
        for (int offset = 0; offset < data.length; offset += 8) {
            byte[] chunk = Arrays.copyOfRange(data, offset, Math.min(data.length, offset + 8));
            chunks.add(new Chunk(Hashing.chunk(chunk), offset, chunk));
        }
        return chunks;
    }

    private static BulkDataFrame writeFrame(UUID transferId, int cordIndex, long chunkSequence, String payload) {
        return new BulkDataFrame(
                transferId,
                cordIndex,
                0,
                chunkSequence,
                chunkSequence * 4096,
                true,
                payload.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static CxnFileServerConfig config(Path keyStore) {
        return config(keyStore, CordPoolConfig.xetLikeDefaults());
    }

    private static CxnFileServerConfig config(Path keyStore, CordPoolConfig cordPool) {
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
                        TransportSecurityConfig.quicTls13ForUdp(keyStore),
                        Optional.of(cordPool)
                )
        );
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
        try (InputStream input = java.nio.file.Files.newInputStream(keyStorePath)) {
            keyStore.load(input, password);
        }
        return keyStore;
    }

    private record Fixture(CxnFileServer server, FileManifest manifest) {
    }
}
