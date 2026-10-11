package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.mount.MountAdapterHandoff;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClientCliTest {
    @TempDir
    Path tempDir;

    @Test
    void listsDirectoryUsingHiddenPromptAndNetworkConnector() {
        FakeTerminal terminal = new FakeTerminal("trainer", "secret".toCharArray());
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> ClientCredentialPrompt.prompt(terminal),
                options -> {
                    connection.options = options;
                    return connection;
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "list",
                        "/datasets/train",
                        "--host",
                        "cxnfs.example.internal",
                        "--control-port",
                        "4444",
                        "--transport-port",
                        "4445",
                        "--bulk-transport",
                        "quic",
                        "--initial-cords",
                        "8",
                        "--max-cords",
                        "64",
                        "--connect-timeout-ms",
                        "1234",
                        "--read-timeout-ms",
                        "5678"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals("CXNFS username: ", terminal.linePrompt);
        assertEquals("CXNFS password: ", terminal.passwordPrompt);
        assertArrayEquals(new char[6], terminal.password);
        assertEquals("trainer", connection.username);
        assertArrayEquals("secret".toCharArray(), connection.password);
        assertTrue(connection.closed);
        assertEquals("cxnfs.example.internal", connection.options.host());
        assertEquals(4444, connection.options.controlPort());
        assertEquals(4445, connection.options.transportPort());
        assertEquals("quic", connection.options.bulkTransport());
        assertEquals(8, connection.options.readConfig().initialCords());
        assertEquals(64, connection.options.readConfig().maxCords());
        assertEquals(1234, connection.options.readConfig().connectTimeoutMillis());
        assertEquals(5678, connection.options.readConfig().readTimeoutMillis());
        String output = out.toString();
        assertTrue(output.contains("cxnfs client list"));
        assertTrue(output.contains("path=/datasets/train"));
        assertTrue(output.contains("entries=1"));
        assertTrue(output.contains("regular_file\t42\t-\t/datasets/train/shard.mds"));
        assertFalse(output.contains("secret"));
    }

    @Test
    void healthProbesAuthenticatedControlAndOptionalBulkRead() {
        FakeConnection connection = new FakeConnection();
        connection.readBytes = new byte[]{1, 2, 3};
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Path report = tempDir.resolve("reports/health.txt");

        int exit = cli.run(
                new String[]{
                        "health",
                        "--path",
                        "/datasets",
                        "--read-path",
                        "/datasets/train/shard.mds",
                        "--read-length",
                        "3",
                        "--host",
                        "cxnfs.example.internal",
                        "--control-port",
                        "4444",
                        "--transport-port",
                        "4445",
                        "--report",
                        report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals("/datasets", connection.listPath);
        assertEquals("/datasets/train/shard.mds", connection.readPath);
        assertEquals(0, connection.readOffset);
        assertEquals(3, connection.readLength);
        assertEquals(TransferPayloadFormat.CONTAINER, connection.payloadFormat);
        assertTrue(connection.streamingReadUsed);
        assertTrue(connection.closed);
        String output = out.toString();
        assertTrue(output.contains("cxnfs client health"));
        assertTrue(output.contains("host=cxnfs.example.internal"));
        assertTrue(output.contains("control_port=4444"));
        assertTrue(output.contains("transport_port=4445"));
        assertTrue(output.contains("authenticated=true"));
        assertTrue(output.contains("session_id="));
        assertTrue(output.contains("probe_path=/datasets"));
        assertTrue(output.contains("probe_entries=1"));
        assertTrue(output.contains("bulk_probe=ok"));
        assertTrue(output.contains("bulk_probe_path=/datasets/train/shard.mds"));
        assertTrue(output.contains("bulk_probe_bytes=3"));
        assertTrue(output.contains("status=ready"));
        assertTrue(output.contains("report=" + report.toAbsolutePath().normalize()));
        assertFalse(output.contains("secret"));
        String reportText = assertDoesNotThrow(() -> Files.readString(report));
        assertTrue(reportText.contains("cxnfs.client.health.v1"));
        assertTrue(reportText.contains("host=cxnfs.example.internal"));
        assertTrue(reportText.contains("control_port=4444"));
        assertTrue(reportText.contains("transport_port=4445"));
        assertTrue(reportText.contains("bulk_probe=ok"));
        assertTrue(reportText.contains("bulk_probe_bytes=3"));
        assertTrue(reportText.contains("status=ready"));
        assertFalse(reportText.contains("secret"));
    }

    @Test
    void healthRejectsBulkLengthWithoutBulkPathBeforePrompting() {
        ClientCli cli = new ClientCli(
                () -> {
                    throw new AssertionError("credential prompt must not run");
                },
                options -> {
                    throw new AssertionError("connector must not run");
                }
        );
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"health", "--read-length", "1"},
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("--read-length requires --read-path"));
    }

    @Test
    void promptsForCredentialsBeforeOpeningNetworkConnection() {
        List<String> events = new ArrayList<>();
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> {
                    events.add("prompt");
                    return new ClientCredentials("trainer", "secret".toCharArray());
                },
                options -> {
                    events.add("connect");
                    assertEquals(List.of("prompt", "connect"), events);
                    return connection;
                }
        );

        int exit = cli.run(
                new String[]{"stat", "/datasets/train/shard.mds"},
                new PrintStream(new ByteArrayOutputStream()),
                System.err
        );

        assertEquals(0, exit);
        assertEquals(List.of("prompt", "connect"), events);
        assertTrue(connection.closed);
    }

    @Test
    void negotiatesMountStrategyThroughAuthenticatedClient() {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "mount-negotiate",
                        "macos-fuse-t",
                        "--capabilities",
                        "list,read"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals(MountBackend.MACOS_FUSE_T, connection.mountBackend);
        assertEquals(Set.of(MountCapability.LIST, MountCapability.READ), connection.mountCapabilities);
        String output = out.toString();
        assertTrue(output.contains("cxnfs client mount-negotiate"));
        assertTrue(output.contains("requested_backend=macos_fuse_t"));
        assertTrue(output.contains("requested_capabilities=list,read"));
        assertTrue(output.contains("backend=macos_fuse_t"));
        assertTrue(output.contains("capabilities=list,read,write"));
    }

    @Test
    void writesMountAdapterHandoffAfterAuthenticatedNegotiation() throws Exception {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Path handoff = tempDir.resolve("mount/cxnfs-mount.handoff");

        int exit = cli.run(
                new String[]{
                        "mount-negotiate",
                        "macos-fuse-t",
                        "--capabilities",
                        "list,read,write",
                        "--remote-root",
                        "/datasets/train",
                        "--mount-point",
                        "/Volumes/CXNFS",
                        "--handoff-report",
                        handoff.toString(),
                        "--host",
                        "cxnfs.example.internal",
                        "--control-port",
                        "4444",
                        "--transport-port",
                        "4445",
                        "--initial-cords",
                        "8",
                        "--max-cords",
                        "64",
                        "--connect-timeout-ms",
                        "1234",
                        "--read-timeout-ms",
                        "5678"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("handoff_report=" + handoff.toAbsolutePath().normalize()));
        assertTrue(output.contains("handoff_format=cxnfs.mount.handoff.v1"));
        assertTrue(output.contains("adapter_binary=cxnfs-fuse-t"));
        String report = Files.readString(handoff);
        assertTrue(report.contains("cxnfs.mount.handoff.v1"));
        assertTrue(report.contains("host=cxnfs.example.internal"));
        assertTrue(report.contains("control_port=4444"));
        assertTrue(report.contains("transport_port=4445"));
        assertTrue(report.contains("requested_backend=macos_fuse_t"));
        assertTrue(report.contains("requested_capabilities=list,read,write"));
        assertTrue(report.contains("backend=macos_fuse_t"));
        assertTrue(report.contains("capabilities=list,read,write"));
        assertTrue(report.contains("adapter_kind=fuse"));
        assertTrue(report.contains("adapter_binary=cxnfs-fuse-t"));
        assertTrue(report.contains("remote_root=/datasets/train"));
        assertTrue(report.contains("mount_point=/Volumes/CXNFS"));
        assertTrue(report.contains("initial_cords=8"));
        assertTrue(report.contains("max_cords=64"));
        assertTrue(report.contains("connect_timeout_ms=1234"));
        assertTrue(report.contains("read_timeout_ms=5678"));
        assertTrue(report.contains("truststore_password_inline=false"));
        assertTrue(report.contains("auth_password_inline=false"));
        assertFalse(report.contains("secret"));
    }

    @Test
    void mountHandoffDefaultsToAggregateRootForAllServedRoots() throws Exception {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Path handoff = tempDir.resolve("mount/cxnfs-all-roots.handoff");

        int exit = cli.run(
                new String[]{
                        "mount-negotiate",
                        "windows-winfsp",
                        "--capabilities",
                        "list,read",
                        "--mount-point",
                        "X:",
                        "--handoff-report",
                        handoff.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals(MountBackend.WINDOWS_WINFSP, connection.mountBackend);
        String report = Files.readString(handoff);
        assertTrue(report.contains("cxnfs.mount.handoff.v1"));
        assertTrue(report.contains("remote_root=/"));
        assertTrue(report.contains("mount_point=X:"));
        assertFalse(report.contains("secret"));
    }

    @Test
    void rejectsMountHandoffReportWithoutMountPointBeforePrompting() {
        ClientCli cli = new ClientCli(
                () -> {
                    throw new AssertionError("credentials should not be prompted");
                },
                options -> new FakeConnection()
        );
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "mount-negotiate",
                        "linux-fuse",
                        "--handoff-report",
                        tempDir.resolve("missing-mount.handoff").toString()
                },
                System.out,
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("--handoff-report requires --mount-point"));
    }

    @Test
    void readsMountHandoffAndWritesLaunchPlanWithoutPrompting() throws Exception {
        Path handoff = tempDir.resolve("mount/cxnfs-mount.handoff");
        Path launchPlan = tempDir.resolve("mount/cxnfs-launch.plan");
        Files.createDirectories(handoff.getParent());
        Files.writeString(
                handoff,
                new MountAdapterHandoff(
                        "cxnfs.example.internal",
                        4444,
                        4445,
                        MountBackend.MACOS_FUSE_T,
                        Set.of(MountCapability.LIST, MountCapability.READ),
                        MountStrategy.macFuseT(),
                        "/datasets/train",
                        "/Volumes/CXNFS",
                        new CxnFileNetworkClientConfig(8, 64, 64 * 1024 * 1024, 0, 1234, 5678),
                        Optional.empty()
                ).toReportText()
        );
        ClientCli cli = new ClientCli(
                () -> {
                    throw new AssertionError("credentials should not be prompted");
                },
                options -> {
                    throw new AssertionError("network should not be opened");
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "mount-launch-plan",
                        handoff.toString(),
                        "--adapter-binary",
                        "/opt/cxnfs/bin/cxnfs-fuse-t",
                        "--report",
                        launchPlan.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("cxnfs.mount.launch-plan.v1"));
        assertTrue(output.contains("handoff_report=" + handoff.toAbsolutePath().normalize()));
        assertTrue(output.contains("adapter_binary=/opt/cxnfs/bin/cxnfs-fuse-t"));
        assertTrue(output.contains("adapter_kind=fuse"));
        assertTrue(output.contains("remote_root=/datasets/train"));
        assertTrue(output.contains("mount_point=/Volumes/CXNFS"));
        assertTrue(output.contains("argv_count=3"));
        assertTrue(output.contains("argv_0=/opt/cxnfs/bin/cxnfs-fuse-t"));
        assertTrue(output.contains("argv_1=--handoff"));
        assertTrue(output.contains("auth_password_inline=false"));
        assertTrue(output.contains("truststore_password_inline=false"));
        assertFalse(output.contains("secret"));
        assertEquals(output, Files.readString(launchPlan));
    }

    @Test
    void listsDirectoryThroughMountHandoffAdapterPath() throws Exception {
        Path handoff = tempDir.resolve("mount/cxnfs-mount-list.handoff");
        Files.createDirectories(handoff.getParent());
        Files.writeString(
                handoff,
                new MountAdapterHandoff(
                        "cxnfs.example.internal",
                        4444,
                        4445,
                        MountBackend.MACOS_FUSE_T,
                        Set.of(MountCapability.LIST),
                        MountStrategy.macFuseT(),
                        "/datasets/train",
                        "/Volumes/CXNFS",
                        new CxnFileNetworkClientConfig(8, 64, 64 * 1024 * 1024, 0, 1234, 5678),
                        Optional.empty()
                ).toReportText()
        );
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> {
                    connection.options = options;
                    return connection;
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "mount-list",
                        handoff.toString(),
                        "--path",
                        "/datasets/train"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals("trainer", connection.username);
        assertArrayEquals("secret".toCharArray(), connection.password);
        assertEquals("cxnfs.example.internal", connection.options.host());
        assertEquals(4444, connection.options.controlPort());
        assertEquals(4445, connection.options.transportPort());
        assertEquals(8, connection.options.readConfig().initialCords());
        assertEquals(64, connection.options.readConfig().maxCords());
        assertEquals("/datasets/train", connection.listPath);
        String output = out.toString();
        assertTrue(output.contains("cxnfs.mount.list.v1"));
        assertTrue(output.contains("handoff_report=" + handoff.toAbsolutePath().normalize()));
        assertTrue(output.contains("backend=macos_fuse_t"));
        assertTrue(output.contains("remote_root=/datasets/train"));
        assertTrue(output.contains("mount_point=/Volumes/CXNFS"));
        assertTrue(output.contains("path=/datasets/train"));
        assertTrue(output.contains("regular_file\t42\t-\t/datasets/train/shard.mds"));
        assertFalse(output.contains("secret"));
    }

    @Test
    void fetchesManifestByFileHashThroughAuthenticatedClient() {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"manifest", connection.fileHash.hex()},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals(connection.fileHash, connection.manifestRequestHash);
        String output = out.toString();
        assertTrue(output.contains("cxnfs client manifest"));
        assertTrue(output.contains("found=true"));
        assertTrue(output.contains("path=/datasets/train/shard.mds"));
        assertTrue(output.contains("chunks=1"));
    }

    @Test
    void fetchesReconstructionPlanByFileHashThroughAuthenticatedClient() {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"reconstruction-plan", connection.fileHash.hex()},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals(connection.fileHash, connection.reconstructionRequestHash);
        String output = out.toString();
        assertTrue(output.contains("cxnfs client reconstruction-plan"));
        assertTrue(output.contains("file_size=42"));
        assertTrue(output.contains("terms=1"));
        assertTrue(output.contains("term.0=" + connection.containerHash + ":0:1"));
    }

    @Test
    void readsRangeToOutputFileUsingContainerPayloadFormat() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.readBytes = "range payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        Path outputFile = tempDir.resolve("range.bin");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "read",
                        "/datasets/train/shard.mds",
                        "--offset",
                        "9",
                        "--length",
                        "13",
                        "--output",
                        outputFile.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals("/datasets/train/shard.mds", connection.readPath);
        assertEquals(9, connection.readOffset);
        assertEquals(13, connection.readLength);
        assertEquals(TransferPayloadFormat.CONTAINER, connection.payloadFormat);
        assertTrue(connection.streamingReadUsed);
        assertArrayEquals(connection.readBytes, Files.readAllBytes(outputFile));
        String output = out.toString();
        assertTrue(output.contains("cxnfs client read"));
        assertTrue(output.contains("returned_bytes=13"));
        assertTrue(output.contains("output=" + outputFile));
    }

    @Test
    void bootstrapsWriteTransferThroughAuthenticatedClient() {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{
                        "write-bootstrap",
                        "/outputs/run-1",
                        "--format",
                        "safetensors",
                        "--initial-cords",
                        "16",
                        "--max-cords",
                        "64"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals("/outputs/run-1", connection.writeBootstrapRoot);
        assertEquals(TransferPayloadFormat.SAFETENSORS, connection.writeBootstrapPayloadFormat);
        String output = out.toString();
        assertTrue(output.contains("cxnfs client write-bootstrap"));
        assertTrue(output.contains("root_path=/outputs/run-1"));
        assertTrue(output.contains("direction=write"));
        assertTrue(output.contains("payload_format=safetensors"));
        assertTrue(output.contains("write_payload_admission_granted=true"));
    }

    @Test
    void renamesAndDeletesThroughAuthenticatedClient() {
        FakeConnection connection = new FakeConnection();
        ClientCli cli = new ClientCli(
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> connection
        );
        ByteArrayOutputStream renameOut = new ByteArrayOutputStream();
        ByteArrayOutputStream deleteOut = new ByteArrayOutputStream();

        int renameExit = cli.run(
                new String[]{"rename", "/datasets/train/old.mds", "/datasets/train/new.mds"},
                new PrintStream(renameOut),
                System.err
        );
        int deleteExit = cli.run(
                new String[]{"delete", "/datasets/train/new.mds", "--recursive", "false"},
                new PrintStream(deleteOut),
                System.err
        );

        assertEquals(0, renameExit);
        assertEquals(0, deleteExit);
        assertEquals("/datasets/train/old.mds", connection.renameSourcePath);
        assertEquals("/datasets/train/new.mds", connection.renameTargetPath);
        assertEquals("/datasets/train/new.mds", connection.deletePath);
        assertFalse(connection.deleteRecursive);
        assertTrue(renameOut.toString().contains("cxnfs client rename"));
        assertTrue(renameOut.toString().contains("target_path=/datasets/train/new.mds"));
        assertTrue(deleteOut.toString().contains("cxnfs client delete"));
        assertTrue(deleteOut.toString().contains("deleted_entries=1"));
    }

    @Test
    void rejectsVisiblePasswordOptionsBeforeConnecting() {
        ClientCli cli = new ClientCli(
                () -> {
                    throw new AssertionError("credential prompt must not run");
                },
                options -> {
                    throw new AssertionError("connector must not run");
                }
        );
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = cli.run(
                new String[]{"list", "/datasets/train", "--password", "secret"},
                System.out,
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("local hidden prompt"));
    }

    private static final class FakeConnection implements ClientCli.ClientConnection {
        private final ContentHash chunkHash = Hashing.chunk("training sample payload".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        private final ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(42));
        private final ContentHash containerHash = Hashing.container("container".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        private ClientCli.ClientOptions options;
        private String username;
        private char[] password;
        private boolean closed;
        private byte[] readBytes = new byte[0];
        private String listPath;
        private String readPath;
        private long readOffset;
        private long readLength;
        private TransferPayloadFormat payloadFormat;
        private boolean streamingReadUsed;
        private MountBackend mountBackend;
        private Set<MountCapability> mountCapabilities;
        private ContentHash manifestRequestHash;
        private ContentHash reconstructionRequestHash;
        private String writeBootstrapRoot;
        private TransferPayloadFormat writeBootstrapPayloadFormat;
        private String renameSourcePath;
        private String renameTargetPath;
        private String deletePath;
        private boolean deleteRecursive;

        @Override
        public AuthAccepted authenticate(ClientCredentials credentials) {
            username = credentials.username();
            password = credentials.password();
            return new AuthAccepted(UUID.randomUUID(), List.of(AuthMechanism.USERNAME_PASSWORD));
        }

        @Override
        public DirectoryListing list(String path) {
            listPath = path;
            return new DirectoryListing(
                    path,
                    List.of(new FileMetadata(
                            "/datasets/train/shard.mds",
                            "shard.mds",
                            FileType.REGULAR_FILE,
                            42,
                            Optional.empty(),
                            Instant.EPOCH
                    ))
            );
        }

        @Override
        public Optional<FileMetadata> stat(String path) {
            return Optional.of(new FileMetadata(
                    path,
                    Path.of(path).getFileName().toString(),
                    FileType.REGULAR_FILE,
                    42,
                    Optional.empty(),
                    Instant.EPOCH
            ));
        }

        @Override
        public Optional<FileManifest> manifest(ContentHash fileHash) {
            manifestRequestHash = fileHash;
            return Optional.of(new FileManifest(
                    "/datasets/train/shard.mds",
                    fileHash,
                    42,
                    List.of(new ChunkRef(chunkHash, 42))
            ));
        }

        @Override
        public ReconstructionPlan reconstructionPlan(ContentHash fileHash) {
            reconstructionRequestHash = fileHash;
            return new ReconstructionPlan(
                    fileHash,
                    42,
                    List.of(new ReconstructionTerm(containerHash, 0, 1))
            );
        }

        @Override
        public MountStrategy negotiateMount(MountBackend backend, Set<MountCapability> requestedCapabilities) {
            this.mountBackend = backend;
            this.mountCapabilities = Set.copyOf(requestedCapabilities);
            return switch (backend) {
                case MACOS_FUSE_T -> MountStrategy.macFuseT();
                case CLI_LIST_ONLY -> MountStrategy.listOnlyFallback();
                case LINUX_FUSE -> MountStrategy.linuxFuse();
                case MACOS_MACFUSE -> MountStrategy.macFuse();
                case WINDOWS_WINFSP -> MountStrategy.windowsWinFsp();
                case WINDOWS_PROJFS -> MountStrategy.windowsProjectionListRead();
            };
        }

        @Override
        public byte[] readRange(String path, long offset, long length, TransferPayloadFormat payloadFormat)
                throws IOException {
            this.readPath = path;
            this.readOffset = offset;
            this.readLength = length;
            this.payloadFormat = payloadFormat;
            if (readBytes.length != length) {
                throw new IOException("test fixture readBytes must match requested length");
            }
            return readBytes.clone();
        }

        @Override
        public long readRangeTo(
                String path,
                long offset,
                long length,
                TransferPayloadFormat payloadFormat,
                OutputStream output
        ) throws IOException {
            streamingReadUsed = true;
            byte[] data = readRange(path, offset, length, payloadFormat);
            output.write(data);
            return data.length;
        }

        @Override
        public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) {
            this.writeBootstrapRoot = rootPath;
            this.writeBootstrapPayloadFormat = payloadFormat;
            return new TransferAcceptedResponse(
                    UUID.randomUUID(),
                    TransferDirection.WRITE,
                    16,
                    64,
                    128L * 1024 * 1024,
                    true
            );
        }

        @Override
        public RenamePathResponse renamePath(String sourcePath, String targetPath) {
            this.renameSourcePath = sourcePath;
            this.renameTargetPath = targetPath;
            return new RenamePathResponse(
                    sourcePath,
                    targetPath,
                    new FileMetadata(
                            targetPath,
                            Path.of(targetPath).getFileName().toString(),
                            FileType.REGULAR_FILE,
                            42,
                            Optional.of(fileHash),
                            Instant.EPOCH
                    )
            );
        }

        @Override
        public DeletePathResponse deletePath(String path, boolean recursive) {
            this.deletePath = path;
            this.deleteRecursive = recursive;
            return new DeletePathResponse(path, 1);
        }

        @Override
        public void close() {
            closed = true;
        }
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
            return password;
        }
    }
}
