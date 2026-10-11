package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.mount.MountAdapterHandoff;
import com.fortrow.cxnfs.mount.MountAdapterLaunchPlan;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.io.Console;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class ClientCli {
    private static final int DEFAULT_CONTROL_PORT = 4509;
    private static final int DEFAULT_TRANSPORT_PORT = 4510;
    private static final Set<String> COMMON_OPTIONS = Set.of(
            "--host",
            "--control-port",
            "--transport-port",
            "--bulk-transport",
            "--truststore",
            "--truststore-password-file",
            "--truststore-password-stdin",
            "--initial-cords",
            "--max-cords",
            "--provisional-container-size",
            "--expected-size-limit",
            "--connect-timeout-ms",
            "--read-timeout-ms"
    );
    private static final Set<String> READ_OPTIONS = Set.of("--offset", "--length", "--output");
    private static final Set<String> HEALTH_OPTIONS = Set.of("--path", "--read-path", "--read-length", "--report");
    private static final Set<String> DELETE_OPTIONS = Set.of("--recursive");
    private static final Set<String> WRITE_BOOTSTRAP_OPTIONS = Set.of("--format");
    private static final Set<String> MOUNT_OPTIONS = Set.of(
            "--capabilities",
            "--remote-root",
            "--mount-point",
            "--handoff-report"
    );
    private static final Set<String> MOUNT_LAUNCH_PLAN_OPTIONS = Set.of("--adapter-binary", "--report");
    private static final Set<String> MOUNT_LIST_OPTIONS = Set.of("--path");
    private static final Set<String> REJECTED_CREDENTIAL_OPTIONS = Set.of(
            "--username",
            "--user",
            "--password",
            "--password-file",
            "--auth-password",
            "--auth-password-file"
    );

    private final CredentialProvider credentialProvider;
    private final NetworkConnector connector;

    public ClientCli() {
        this(ClientCredentialPrompt::prompt, ClientCli::connectNetwork);
    }

    public ClientCli(CredentialProvider credentialProvider, NetworkConnector connector) {
        if (credentialProvider == null) {
            throw new IllegalArgumentException("credentialProvider is required");
        }
        if (connector == null) {
            throw new IllegalArgumentException("connector is required");
        }
        this.credentialProvider = credentialProvider;
        this.connector = connector;
    }

    public int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }
        try {
            return switch (args[0]) {
                case "list" -> list(args, out);
                case "health" -> health(args, out);
                case "manifest" -> manifest(args, out);
                case "mount-negotiate" -> mountNegotiate(args, out);
                case "mount-launch-plan" -> mountLaunchPlan(args, out);
                case "mount-list" -> mountList(args, out);
                case "reconstruction-plan" -> reconstructionPlan(args, out);
                case "stat" -> stat(args, out);
                case "read" -> read(args, out);
                case "delete" -> delete(args, out);
                case "rename" -> rename(args, out);
                case "write-bootstrap" -> writeBootstrap(args, out);
                default -> {
                    err.println("unknown client command: " + args[0]);
                    usage(err);
                    yield 2;
                }
            };
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println("client command failed: " + e.getMessage());
            return 1;
        }
    }

    private int list(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client list requires a path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, COMMON_OPTIONS);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                DirectoryListing listing = client.list(args[1]);
                out.println("cxnfs client list");
                out.println("host=" + clientOptions.host());
                out.println("path=" + listing.path());
                out.println("entries=" + listing.entries().size());
                for (FileMetadata entry : listing.entries()) {
                    out.println(entry.type().name().toLowerCase(Locale.ROOT)
                            + "\t" + entry.size()
                            + "\t" + entry.contentHash().map(ContentHash::toString).orElse("-")
                            + "\t" + entry.path());
                }
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int health(String[] args, PrintStream out) throws IOException {
        Map<String, String> options = parseOptions(args, 1);
        validateOptions(options, union(COMMON_OPTIONS, HEALTH_OPTIONS));
        String probePath = options.getOrDefault("--path", "/");
        String bulkProbePath = options.get("--read-path");
        if (bulkProbePath == null && options.containsKey("--read-length")) {
            throw new IllegalArgumentException("--read-length requires --read-path");
        }
        long bulkProbeLength = bulkProbePath == null
                ? 0
                : longOption(options, "--read-length", 1L);
        if (bulkProbePath != null && bulkProbeLength <= 0) {
            throw new IllegalArgumentException("--read-length must be positive");
        }

        ClientOptions clientOptions = clientOptions(options);
        try (ClientCredentials credentials = credentialProvider.prompt();
             ClientConnection client = connector.connect(clientOptions)) {
            AuthAccepted accepted = client.authenticate(credentials);
            DirectoryListing listing = client.list(probePath);
            StringBuilder report = new StringBuilder("cxnfs.client.health.v1")
                    .append(System.lineSeparator());
            out.println("cxnfs client health");
            writeHealthLine(out, report, "host", clientOptions.host());
            writeHealthLine(out, report, "control_port", Integer.toString(clientOptions.controlPort()));
            writeHealthLine(out, report, "transport_port", Integer.toString(clientOptions.transportPort()));
            writeHealthLine(out, report, "authenticated", "true");
            writeHealthLine(out, report, "session_id", accepted.sessionId().toString());
            writeHealthLine(out, report, "probe_path", listing.path());
            writeHealthLine(out, report, "probe_entries", Integer.toString(listing.entries().size()));
            if (bulkProbePath == null) {
                writeHealthLine(out, report, "bulk_probe", "skipped");
            } else {
                long returnedBytes = client.readRangeTo(
                        bulkProbePath,
                        0,
                        bulkProbeLength,
                        TransferPayloadFormat.CONTAINER,
                        OutputStream.nullOutputStream()
                );
                writeHealthLine(out, report, "bulk_probe", "ok");
                writeHealthLine(out, report, "bulk_probe_path", bulkProbePath);
                writeHealthLine(out, report, "bulk_probe_bytes", Long.toString(returnedBytes));
            }
            writeHealthLine(out, report, "status", "ready");
            writeHealthReport(options, report.toString(), out);
            return 0;
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int stat(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client stat requires a path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, COMMON_OPTIONS);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                Optional<FileMetadata> maybeMetadata = client.stat(args[1]);
                out.println("cxnfs client stat");
                out.println("host=" + clientOptions.host());
                out.println("path=" + args[1]);
                if (maybeMetadata.isEmpty()) {
                    out.println("found=false");
                    return 1;
                }
                FileMetadata metadata = maybeMetadata.orElseThrow();
                out.println("found=true");
                out.println("type=" + metadata.type().name().toLowerCase(Locale.ROOT));
                out.println("size=" + metadata.size());
                out.println("content_hash=" + metadata.contentHash().map(ContentHash::toString).orElse("-"));
                out.println("modified_at=" + metadata.modifiedAt());
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int manifest(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client manifest requires a file hash");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, COMMON_OPTIONS);
        ContentHash fileHash = ContentHash.fromHex(args[1]);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                Optional<FileManifest> maybeManifest = client.manifest(fileHash);
                out.println("cxnfs client manifest");
                out.println("host=" + clientOptions.host());
                out.println("file_hash=" + fileHash);
                if (maybeManifest.isEmpty()) {
                    out.println("found=false");
                    return 1;
                }
                FileManifest manifest = maybeManifest.orElseThrow();
                out.println("found=true");
                out.println("path=" + manifest.path());
                out.println("size=" + manifest.size());
                out.println("chunks=" + manifest.chunks().size());
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int reconstructionPlan(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client reconstruction-plan requires a file hash");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, COMMON_OPTIONS);
        ContentHash fileHash = ContentHash.fromHex(args[1]);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                ReconstructionPlan plan = client.reconstructionPlan(fileHash);
                out.println("cxnfs client reconstruction-plan");
                out.println("host=" + clientOptions.host());
                out.println("file_hash=" + plan.fileHash());
                out.println("file_size=" + plan.fileSize());
                out.println("terms=" + plan.terms().size());
                for (int index = 0; index < plan.terms().size(); index++) {
                    ReconstructionTerm term = plan.terms().get(index);
                    out.println("term." + index
                            + "=" + term.containerHash()
                            + ":" + term.firstChunkIndex()
                            + ":" + term.chunkCount());
                }
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int read(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client read requires a path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, union(COMMON_OPTIONS, READ_OPTIONS));
        long offset = longOption(options, "--offset");
        long length = longOption(options, "--length");
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                String outputValue = options.get("--output");
                long returnedBytes;
                byte[] data = null;
                if (outputValue != null) {
                    Path output = Path.of(outputValue);
                    Path parent = output.toAbsolutePath().normalize().getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    try (OutputStream file = Files.newOutputStream(output)) {
                        returnedBytes = client.readRangeTo(
                                args[1],
                                offset,
                                length,
                                TransferPayloadFormat.CONTAINER,
                                file
                        );
                    }
                } else {
                    data = client.readRange(args[1], offset, length, TransferPayloadFormat.CONTAINER);
                    returnedBytes = data.length;
                }
                out.println("cxnfs client read");
                out.println("host=" + clientOptions.host());
                out.println("path=" + args[1]);
                out.println("offset=" + offset);
                out.println("requested_length=" + length);
                out.println("returned_bytes=" + returnedBytes);
                if (outputValue == null) {
                    out.println("preview_hex=" + hexPreview(data, 32));
                } else {
                    out.println("output=" + outputValue);
                }
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int delete(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client delete requires a path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, union(COMMON_OPTIONS, DELETE_OPTIONS));
        boolean recursive = booleanOption(options, "--recursive", false);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                DeletePathResponse response = client.deletePath(args[1], recursive);
                out.println("cxnfs client delete");
                out.println("host=" + clientOptions.host());
                out.println("path=" + response.path());
                out.println("recursive=" + recursive);
                out.println("deleted_entries=" + response.deletedEntries());
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int rename(String[] args, PrintStream out) throws IOException {
        if (args.length < 3) {
            throw new IllegalArgumentException("client rename requires a source path and target path");
        }
        Map<String, String> options = parseOptions(args, 3);
        validateOptions(options, COMMON_OPTIONS);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                RenamePathResponse response = client.renamePath(args[1], args[2]);
                FileMetadata metadata = response.metadata();
                out.println("cxnfs client rename");
                out.println("host=" + clientOptions.host());
                out.println("source_path=" + response.sourcePath());
                out.println("target_path=" + response.targetPath());
                out.println("type=" + metadata.type().name().toLowerCase(Locale.ROOT));
                out.println("size=" + metadata.size());
                out.println("content_hash=" + metadata.contentHash().map(ContentHash::toString).orElse("-"));
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int withClient(ClientOptions options, ClientOperation operation) throws IOException {
        try (ClientCredentials credentials = credentialProvider.prompt();
             ClientConnection client = connector.connect(options)) {
            client.authenticate(credentials);
            return operation.run(client);
        }
    }

    private int mountNegotiate(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client mount-negotiate requires a backend");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, union(COMMON_OPTIONS, MOUNT_OPTIONS));
        MountBackend backend = parseMountBackend(args[1]);
        Set<MountCapability> requestedCapabilities = parseMountCapabilities(
                options.getOrDefault("--capabilities", "list,read")
        );
        validateMountHandoffOptions(options);
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                MountStrategy strategy = client.negotiateMount(backend, requestedCapabilities);
                out.println("cxnfs client mount-negotiate");
                out.println("host=" + clientOptions.host());
                out.println("requested_backend=" + mountBackendName(backend));
                out.println("requested_capabilities=" + mountCapabilities(requestedCapabilities));
                out.println("backend=" + mountBackendName(strategy.backend()));
                out.println("capabilities=" + mountCapabilities(strategy.capabilities()));
                String handoffReport = options.get("--handoff-report");
                if (handoffReport != null) {
                    MountAdapterHandoff handoff = new MountAdapterHandoff(
                            clientOptions.host(),
                            clientOptions.controlPort(),
                            clientOptions.transportPort(),
                            backend,
                            requestedCapabilities,
                            strategy,
                            options.getOrDefault("--remote-root", "/"),
                            options.get("--mount-point"),
                            clientOptions.readConfig(),
                            clientOptions.trustStore()
                    );
                    Path report = writeMountHandoffReport(handoff, handoffReport);
                    out.println("handoff_report=" + report);
                    out.println("handoff_format=" + MountAdapterHandoff.FORMAT);
                    out.println("adapter_binary=" + handoff.adapterBinaryName());
                }
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private static void validateMountHandoffOptions(Map<String, String> options) {
        if (options.containsKey("--handoff-report")) {
            String mountPoint = options.get("--mount-point");
            if (mountPoint == null || mountPoint.isBlank()) {
                throw new IllegalArgumentException("--handoff-report requires --mount-point");
            }
        }
    }

    private static Path writeMountHandoffReport(MountAdapterHandoff handoff, String reportValue) throws IOException {
        if (reportValue == null || reportValue.isBlank()) {
            throw new IllegalArgumentException("handoff report path is required");
        }
        Path report = Path.of(reportValue).toAbsolutePath().normalize();
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(report, handoff.toReportText(), StandardCharsets.UTF_8);
        return report;
    }

    private int mountLaunchPlan(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client mount-launch-plan requires a handoff report path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, MOUNT_LAUNCH_PLAN_OPTIONS);
        Path handoffPath = Path.of(args[1]).toAbsolutePath().normalize();
        MountAdapterHandoff handoff = MountAdapterHandoff.read(handoffPath);
        String adapterBinary = options.getOrDefault("--adapter-binary", handoff.adapterBinaryName());
        MountAdapterLaunchPlan plan = new MountAdapterLaunchPlan(handoffPath, handoff, adapterBinary);
        String report = plan.toReportText();
        out.print(report);
        String reportValue = options.get("--report");
        if (reportValue != null) {
            Path reportPath = Path.of(reportValue).toAbsolutePath().normalize();
            Path parent = reportPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(reportPath, report, StandardCharsets.UTF_8);
        }
        return 0;
    }

    private int mountList(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client mount-list requires a handoff report path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, MOUNT_LIST_OPTIONS);
        Path handoffPath = Path.of(args[1]).toAbsolutePath().normalize();
        MountAdapterHandoff handoff = MountAdapterHandoff.read(handoffPath);
        if (!handoff.strategy().capabilities().contains(MountCapability.LIST)) {
            throw new IllegalArgumentException("mount handoff does not include list capability");
        }
        String path = options.getOrDefault("--path", handoff.remoteRoot());
        ClientOptions clientOptions = clientOptions(handoff);
        try {
            return withClient(clientOptions, client -> {
                DirectoryListing listing = client.list(path);
                out.println("cxnfs.mount.list.v1");
                out.println("handoff_report=" + handoffPath);
                out.println("backend=" + mountBackendName(handoff.strategy().backend()));
                out.println("remote_root=" + handoff.remoteRoot());
                out.println("mount_point=" + handoff.mountPoint());
                out.println("host=" + clientOptions.host());
                out.println("path=" + listing.path());
                out.println("entries=" + listing.entries().size());
                for (FileMetadata entry : listing.entries()) {
                    out.println(entry.type().name().toLowerCase(Locale.ROOT)
                            + "\t" + entry.size()
                            + "\t" + entry.contentHash().map(ContentHash::toString).orElse("-")
                            + "\t" + entry.path());
                }
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int writeBootstrap(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("client write-bootstrap requires a root path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, union(COMMON_OPTIONS, WRITE_BOOTSTRAP_OPTIONS));
        TransferPayloadFormat payloadFormat = parseTransferPayloadFormat(
                options.getOrDefault("--format", "safetensors")
        );
        ClientOptions clientOptions = clientOptions(options);
        try {
            return withClient(clientOptions, client -> {
                TransferAcceptedResponse response = client.bootstrapWrite(args[1], payloadFormat);
                out.println("cxnfs client write-bootstrap");
                out.println("host=" + clientOptions.host());
                out.println("root_path=" + args[1]);
                out.println("transfer_id=" + response.transferId());
                out.println("direction=" + response.direction().name().toLowerCase(Locale.ROOT));
                out.println("payload_format=" + payloadFormat.name().toLowerCase(Locale.ROOT));
                out.println("accepted_initial_cords=" + response.acceptedInitialCords());
                out.println("accepted_max_cords=" + response.acceptedMaxCords());
                out.println("flow_control_window_bytes=" + response.flowControlWindowBytes());
                out.println("write_payload_admission_granted=" + response.writePayloadAdmissionGranted());
                return 0;
            });
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private static ClientConnection connectNetwork(ClientOptions options) throws IOException {
        ClientTlsContextFactory contextFactory = new ClientTlsContextFactory();
        SSLContext context = options.trustStore().isPresent()
                ? contextFactory.create(options.trustStore().orElseThrow(), options.trustStorePassword())
                : contextFactory.createDefault();
        SocketFactory socketFactory = new ControlTlsClientSocketFactory(context);
        if (options.bulkTransport().equals("quic")) {
            KeyStore trustStore = options.trustStore().isPresent()
                    ? loadTrustStore(options.trustStore().orElseThrow(), options.trustStorePassword())
                    : null;
            BulkTransferClientChannel bulkChannel = QuicBulkTransferClientChannel.connect(
                    new InetSocketAddress(options.host(), options.transportPort()),
                    options.readConfig(),
                    Optional.ofNullable(trustStore),
                    false
            );
            return new NetworkClientConnection(CxnFileNetworkClient.connect(
                    new InetSocketAddress(options.host(), options.controlPort()),
                    socketFactory,
                    options.readConfig(),
                    bulkChannel
            ));
        }
        return new NetworkClientConnection(CxnFileNetworkClient.connect(
                new InetSocketAddress(options.host(), options.controlPort()),
                new InetSocketAddress(options.host(), options.transportPort()),
                socketFactory,
                socketFactory,
                options.readConfig()
        ));
    }

    private static ClientOptions clientOptions(Map<String, String> options) throws IOException {
        String host = options.getOrDefault("--host", "127.0.0.1");
        int controlPort = intOption(options, "--control-port", DEFAULT_CONTROL_PORT);
        int transportPort = intOption(options, "--transport-port", DEFAULT_TRANSPORT_PORT);
        String bulkTransport = bulkTransportOption(options);
        Path trustStore = options.containsKey("--truststore")
                ? Path.of(options.get("--truststore"))
                : null;
        char[] trustStorePassword = trustStorePassword(options, trustStore != null);
        try {
            CxnFileNetworkClientConfig defaults = CxnFileNetworkClientConfig.defaults();
            CxnFileNetworkClientConfig readConfig = new CxnFileNetworkClientConfig(
                    intOption(options, "--initial-cords", defaults.initialCords()),
                    intOption(options, "--max-cords", defaults.maxCords()),
                    intOption(options, "--provisional-container-size", defaults.provisionalContainerSizeBytes()),
                    longOption(options, "--expected-size-limit", defaults.expectedSizeLimitBytes()),
                    intOption(options, "--connect-timeout-ms", defaults.connectTimeoutMillis()),
                    intOption(options, "--read-timeout-ms", defaults.readTimeoutMillis())
            );
            return new ClientOptions(host, controlPort, transportPort, bulkTransport, trustStore, trustStorePassword, readConfig);
        } finally {
            Arrays.fill(trustStorePassword, '\0');
        }
    }

    private static ClientOptions clientOptions(MountAdapterHandoff handoff) {
        return new ClientOptions(
                handoff.host(),
                handoff.controlPort(),
                handoff.transportPort(),
                "stream",
                handoff.trustStorePath().orElse(null),
                new char[0],
                handoff.clientConfig()
        );
    }

    private static String bulkTransportOption(Map<String, String> options) {
        String transport = options.getOrDefault("--bulk-transport", "stream")
                .trim()
                .toLowerCase(Locale.ROOT);
        if (!transport.equals("stream") && !transport.equals("quic")) {
            throw new IllegalArgumentException("--bulk-transport must be stream or quic");
        }
        return transport;
    }

    private static char[] trustStorePassword(Map<String, String> options, boolean trustStoreConfigured) throws IOException {
        String passwordFile = options.get("--truststore-password-file");
        boolean readFromStdin = booleanOption(options, "--truststore-password-stdin", false);
        if (passwordFile != null && readFromStdin) {
            throw new IllegalArgumentException("use only one of --truststore-password-file or --truststore-password-stdin");
        }
        if (passwordFile != null) {
            return readPasswordBytes(Files.readAllBytes(Path.of(passwordFile)));
        }
        if (readFromStdin) {
            return readPasswordBytes(System.in.readAllBytes());
        }
        if (!trustStoreConfigured) {
            return new char[0];
        }
        Console console = System.console();
        if (console != null) {
            char[] password = console.readPassword("CXNFS truststore password: ");
            return password == null ? new char[0] : password;
        }
        return new char[0];
    }

    private static char[] readPasswordBytes(byte[] bytes) {
        try {
            int end = bytes.length;
            while (end > 0 && (bytes[end - 1] == '\n' || bytes[end - 1] == '\r')) {
                end--;
            }
            return new String(bytes, 0, end, StandardCharsets.UTF_8).toCharArray();
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    private static KeyStore loadTrustStore(Path trustStorePath, char[] trustStorePassword) throws IOException {
        char[] password = trustStorePassword == null ? new char[0] : trustStorePassword.clone();
        try {
            KeyStore trustStore = KeyStore.getInstance(keyStoreType(trustStorePath));
            try (java.io.InputStream input = Files.newInputStream(trustStorePath)) {
                trustStore.load(input, password);
            }
            return trustStore;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to load client truststore: " + trustStorePath, e);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static String keyStoreType(Path path) {
        String name = path.getFileName() == null
                ? ""
                : path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx")) {
            return "PKCS12";
        }
        if (name.endsWith(".jks")) {
            return "JKS";
        }
        return KeyStore.getDefaultType();
    }

    private static Map<String, String> parseOptions(String[] args, int startIndex) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = startIndex; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("expected option but found: " + key);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for option: " + key);
            }
            options.put(key, args[++i]);
        }
        rejectCommandLineCredentials(options);
        return options;
    }

    private static void validateOptions(Map<String, String> options, Set<String> allowed) {
        for (String key : options.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("unknown option: " + key);
            }
        }
    }

    private static void rejectCommandLineCredentials(Map<String, String> options) {
        for (String key : options.keySet()) {
            if (REJECTED_CREDENTIAL_OPTIONS.contains(key)) {
                throw new IllegalArgumentException(
                        "client username and password must be entered through the local hidden prompt"
                );
            }
        }
    }

    private static int intOption(Map<String, String> options, String name, int defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static long longOption(Map<String, String> options, String name, long defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    private static long longOption(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required option: " + name);
        }
        return Long.parseLong(value);
    }

    private static boolean booleanOption(Map<String, String> options, String name, boolean defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static MountBackend parseMountBackend(String value) {
        String normalized = normalizeCliToken(value);
        for (MountBackend backend : MountBackend.values()) {
            if (normalizeCliToken(backend.name()).equals(normalized)) {
                return backend;
            }
        }
        throw new IllegalArgumentException("unknown mount backend: " + value);
    }

    private static Set<MountCapability> parseMountCapabilities(String value) {
        EnumSet<MountCapability> capabilities = EnumSet.noneOf(MountCapability.class);
        for (String token : value.split(",")) {
            String normalized = normalizeCliToken(token);
            if (normalized.isBlank()) {
                continue;
            }
            boolean matched = false;
            for (MountCapability capability : MountCapability.values()) {
                if (normalizeCliToken(capability.name()).equals(normalized)) {
                    capabilities.add(capability);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw new IllegalArgumentException("unknown mount capability: " + token);
            }
        }
        if (capabilities.isEmpty()) {
            throw new IllegalArgumentException("at least one mount capability must be requested");
        }
        return capabilities;
    }

    private static TransferPayloadFormat parseTransferPayloadFormat(String value) {
        String normalized = normalizeCliToken(value);
        for (TransferPayloadFormat payloadFormat : TransferPayloadFormat.values()) {
            if (normalizeCliToken(payloadFormat.name()).equals(normalized)) {
                if (payloadFormat == TransferPayloadFormat.MDS_SHARDS) {
                    throw new IllegalArgumentException("write-bootstrap does not use mds-shards payloads");
                }
                return payloadFormat;
            }
        }
        throw new IllegalArgumentException("unknown transfer payload format: " + value);
    }

    private static String normalizeCliToken(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replace('-', '_');
    }

    private static String mountBackendName(MountBackend backend) {
        return backend.name().toLowerCase(Locale.ROOT);
    }

    private static String mountCapabilities(Set<MountCapability> capabilities) {
        return capabilities.stream()
                .sorted(Comparator.comparing(Enum::ordinal))
                .map(capability -> capability.name().toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.joining(","));
    }

    private static Set<String> union(Set<String> first, Set<String> second) {
        java.util.LinkedHashSet<String> values = new java.util.LinkedHashSet<>(first);
        values.addAll(second);
        return values;
    }

    private static void writeHealthLine(PrintStream out, StringBuilder report, String key, String value) {
        String line = key + "=" + value;
        out.println(line);
        report.append(line).append(System.lineSeparator());
    }

    private static void writeHealthReport(Map<String, String> options, String reportText, PrintStream out) throws IOException {
        String reportValue = options.get("--report");
        if (reportValue == null || reportValue.isBlank()) {
            return;
        }
        Path report = Path.of(reportValue).toAbsolutePath().normalize();
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(report, reportText, StandardCharsets.UTF_8);
        out.println("report=" + report);
    }

    private static String hexPreview(byte[] data, int maxBytes) {
        StringBuilder builder = new StringBuilder();
        int limit = Math.min(data.length, maxBytes);
        for (int i = 0; i < limit; i++) {
            int value = data[i] & 0xff;
            builder.append(Character.forDigit(value >>> 4, 16));
            builder.append(Character.forDigit(value & 0x0f, 16));
        }
        if (data.length > limit) {
            builder.append("...");
        }
        return builder.toString();
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs client list <path> [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client health [--path path] [--read-path file] [--read-length n] [--report path] [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client manifest <file-hash> [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client mount-negotiate <backend> [--capabilities list,read,write] [--remote-root path] [--mount-point path --handoff-report path] [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client mount-launch-plan <handoff-report> [--adapter-binary path] [--report path]");
        out.println("cxnfs client mount-list <handoff-report> [--path path]");
        out.println("cxnfs client reconstruction-plan <file-hash> [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client stat <path> [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client read <path> --offset n --length n [--output path] [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client rename <source-path> <target-path> [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client delete <path> [--recursive true] [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--connect-timeout-ms n] [--read-timeout-ms n]");
        out.println("cxnfs client write-bootstrap <root-path> [--format safetensors|container] [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n]");
    }

    @FunctionalInterface
    public interface CredentialProvider {
        ClientCredentials prompt() throws IOException;
    }

    @FunctionalInterface
    public interface NetworkConnector {
        ClientConnection connect(ClientOptions options) throws IOException;
    }

    @FunctionalInterface
    private interface ClientOperation {
        int run(ClientConnection client) throws IOException;
    }

    public interface ClientConnection extends AutoCloseable {
        AuthAccepted authenticate(ClientCredentials credentials) throws IOException;

        DirectoryListing list(String path) throws IOException;

        Optional<FileMetadata> stat(String path) throws IOException;

        Optional<FileManifest> manifest(ContentHash fileHash) throws IOException;

        ReconstructionPlan reconstructionPlan(ContentHash fileHash) throws IOException;

        MountStrategy negotiateMount(MountBackend backend, Set<MountCapability> requestedCapabilities)
                throws IOException;

        byte[] readRange(String path, long offset, long length, TransferPayloadFormat payloadFormat) throws IOException;

        default long readRangeTo(
                String path,
                long offset,
                long length,
                TransferPayloadFormat payloadFormat,
                OutputStream output
        ) throws IOException {
            byte[] data = readRange(path, offset, length, payloadFormat);
            output.write(data);
            return data.length;
        }

        RenamePathResponse renamePath(String sourcePath, String targetPath) throws IOException;

        DeletePathResponse deletePath(String path, boolean recursive) throws IOException;

        TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) throws IOException;

        @Override
        void close() throws IOException;
    }

    public record ClientOptions(
            String host,
            int controlPort,
            int transportPort,
            String bulkTransport,
            Path trustStorePath,
            char[] trustStorePassword,
            CxnFileNetworkClientConfig readConfig
    ) {
        public ClientOptions {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host is required");
            }
            if (controlPort <= 0 || controlPort > 65535) {
                throw new IllegalArgumentException("controlPort must be between 1 and 65535");
            }
            if (transportPort <= 0 || transportPort > 65535) {
                throw new IllegalArgumentException("transportPort must be between 1 and 65535");
            }
            if (bulkTransport == null || bulkTransport.isBlank()) {
                throw new IllegalArgumentException("bulkTransport is required");
            }
            bulkTransport = bulkTransport.trim().toLowerCase(Locale.ROOT);
            if (!bulkTransport.equals("stream") && !bulkTransport.equals("quic")) {
                throw new IllegalArgumentException("bulkTransport must be stream or quic");
            }
            trustStorePassword = trustStorePassword == null ? new char[0] : trustStorePassword.clone();
            if (readConfig == null) {
                throw new IllegalArgumentException("readConfig is required");
            }
        }

        @Override
        public char[] trustStorePassword() {
            return trustStorePassword.clone();
        }

        public Optional<Path> trustStore() {
            return Optional.ofNullable(trustStorePath);
        }

        public void clearTrustStorePassword() {
            Arrays.fill(trustStorePassword, '\0');
        }
    }

    private record NetworkClientConnection(CxnFileNetworkClient client) implements ClientConnection {
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
