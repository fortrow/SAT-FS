package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.client.BulkTransferClientChannel;
import com.fortrow.cxnfs.client.ClientCli;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.client.CxnFileNetworkClient;
import com.fortrow.cxnfs.client.QuicBulkTransferClientChannel;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.server.BulkReadStreamServer;
import com.fortrow.cxnfs.server.ControlTcpServer;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.server.CxnFileServerConfig;
import com.fortrow.cxnfs.server.CxnFileServerRuntime;
import com.fortrow.cxnfs.server.QuicBulkTransferServer;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeSequentialReadBenchmarkIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void nativeReadBenchmarkExercisesAuthenticatedControlAndBulkRuntime() throws Exception {
        byte[] data = "native benchmark loopback mds shard\n".repeat(2048).getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(new CxnFileServer(config(), repository));

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             BulkReadStreamServer bulk = BulkReadStreamServer.bind(runtime, ServerSocketFactory.getDefault())) {
            control.start();
            bulk.start();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BenchmarkCli cli = new BenchmarkCli(
                    new SequentialReadBenchmark(),
                    new NativeSequentialReadBenchmark(),
                    () -> new ClientCredentials("trainer", "secret".toCharArray()),
                    options -> new NetworkClientConnection(CxnFileNetworkClient.connect(
                            control.boundAddress(),
                            bulk.boundAddress(),
                            SocketFactory.getDefault(),
                            SocketFactory.getDefault(),
                            options.readConfig()
                    ))
            );

            int exit = cli.run(
                    new String[]{
                            "native-read",
                            "/datasets/train/shard.00000.mds",
                            "--iterations", "2",
                            "--warmup", "1",
                            "--block-size", "4096",
                            "--initial-cords", "2",
                            "--max-cords", "4"
                    },
                    new PrintStream(out),
                    System.err
            );

            assertEquals(0, exit);
            String output = out.toString();
            assertTrue(output.contains("cxnfs-native"));
            assertTrue(output.contains("bytes=" + (data.length * 2L)));
            assertTrue(output.contains("initial_cords=2"));
            assertTrue(output.contains("max_cords=4"));
        }
    }

    @Test
    @Timeout(20)
    void nativeReadBenchmarkExercisesQuicBulkRuntime() throws Exception {
        Path keyStorePath = tempDir.resolve("server.p12");
        Path report = tempDir.resolve("quic-native-report.txt");
        char[] password = "changeit".toCharArray();
        generateKeyStore(keyStorePath, password);
        KeyStore keyStore = loadKeyStore(keyStorePath, password);
        byte[] data = "abcdefghijklmnopqrstuvwxyz".repeat(8).getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(NativeSequentialReadBenchmarkIntegrationTest::fixedEightByteChunks);
        repository.put("/datasets/train/shard.00000.mds", data);
        CxnFileServerRuntime runtime = new CxnFileServerRuntime(new CxnFileServer(config(keyStorePath), repository));

        try (ControlTcpServer control = ControlTcpServer.bind(runtime, ServerSocketFactory.getDefault());
             QuicBulkTransferServer bulk = QuicBulkTransferServer.bind(runtime, keyStore, password)) {
            control.start();
            bulk.start();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BenchmarkCli cli = new BenchmarkCli(
                    new SequentialReadBenchmark(),
                    new NativeSequentialReadBenchmark(),
                    () -> new ClientCredentials("trainer", "secret".toCharArray()),
                    options -> {
                        assertEquals("quic", options.bulkTransport());
                        assertEquals(8, options.readConfig().initialCords());
                        assertEquals(64, options.readConfig().maxCords());
                        BulkTransferClientChannel bulkChannel = QuicBulkTransferClientChannel.connect(
                                new InetSocketAddress("localhost", bulk.boundAddress().getPort()),
                                options.readConfig(),
                                Optional.of(keyStore),
                                false
                        );
                        return new NetworkClientConnection(CxnFileNetworkClient.connect(
                                control.boundAddress(),
                                SocketFactory.getDefault(),
                                options.readConfig(),
                                bulkChannel
                        ));
                    }
            );

            int exit = cli.run(
                    new String[]{
                            "native-read",
                            "/datasets/train/shard.00000.mds",
                            "--iterations", "1",
                            "--warmup", "1",
                            "--block-size", "64",
                            "--bulk-transport", "quic",
                            "--initial-cords", "8",
                            "--max-cords", "64",
                            "--report", report.toString()
                    },
                    new PrintStream(out),
                    System.err
            );

            assertEquals(0, exit);
            String output = out.toString();
            assertTrue(output.contains("cxnfs-native"));
            assertTrue(output.contains("bytes=" + data.length));
            assertTrue(output.contains("bulk_transport=quic"));
            assertTrue(output.contains("initial_cords=8"));
            assertTrue(output.contains("max_cords=64"));
            String reportText = java.nio.file.Files.readString(report);
            assertTrue(reportText.contains("cxnfs.benchmark.native-read.v1"));
            assertTrue(reportText.contains("bulk_transport=quic"));
            assertTrue(reportText.contains("payload_format=mds_shards"));
            assertTrue(reportText.contains("bytes=" + data.length));
            assertTrue(bulk.lastStreamError().isEmpty());
        } finally {
            java.util.Arrays.fill(password, '\0');
        }
    }

    private static CxnFileServerConfig config() {
        Path keyStore = Path.of("cxnfs-test-server.p12");
        return config(keyStore);
    }

    private static CxnFileServerConfig config(Path keyStore) {
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

    private static java.util.List<Chunk> fixedEightByteChunks(byte[] data) {
        java.util.List<Chunk> chunks = new ArrayList<>();
        for (int offset = 0; offset < data.length; offset += 8) {
            byte[] chunk = java.util.Arrays.copyOfRange(data, offset, Math.min(data.length, offset + 8));
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
        try (InputStream input = java.nio.file.Files.newInputStream(keyStorePath)) {
            keyStore.load(input, password);
        }
        return keyStore;
    }

    private record NetworkClientConnection(CxnFileNetworkClient client) implements ClientCli.ClientConnection {
        @Override
        public AuthAccepted authenticate(ClientCredentials credentials) throws IOException {
            return client.authenticate(credentials);
        }

        @Override
        public DirectoryListing list(String path) throws IOException {
            return client.list(path);
        }

        @Override
        public Optional<FileMetadata> stat(String path) throws IOException {
            return client.stat(path);
        }

        @Override
        public Optional<FileManifest> manifest(ContentHash fileHash) throws IOException {
            return client.manifest(fileHash);
        }

        @Override
        public ReconstructionPlan reconstructionPlan(ContentHash fileHash) throws IOException {
            return client.reconstructionPlan(fileHash);
        }

        @Override
        public MountStrategy negotiateMount(MountBackend backend, Set<MountCapability> requestedCapabilities)
                throws IOException {
            return client.negotiateMount(backend, requestedCapabilities);
        }

        @Override
        public byte[] readRange(String path, long offset, long length, TransferPayloadFormat payloadFormat)
                throws IOException {
            return client.readRange(path, offset, length, payloadFormat);
        }

        @Override
        public long readRangeTo(
                String path,
                long offset,
                long length,
                TransferPayloadFormat payloadFormat,
                OutputStream output
        ) throws IOException {
            return client.readRangeTo(path, offset, length, payloadFormat, output);
        }

        @Override
        public RenamePathResponse renamePath(String sourcePath, String targetPath) throws IOException {
            return client.renamePath(sourcePath, targetPath);
        }

        @Override
        public DeletePathResponse deletePath(String path, boolean recursive) throws IOException {
            return client.deletePath(path, recursive);
        }

        @Override
        public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat)
                throws IOException {
            return client.bootstrapWrite(rootPath, payloadFormat);
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }
}
