package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.client.ClientCli;
import com.fortrow.cxnfs.client.ClientCredentialPrompt;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.client.ClientTlsContextFactory;
import com.fortrow.cxnfs.client.BulkTransferClientChannel;
import com.fortrow.cxnfs.client.ControlTlsClientSocketFactory;
import com.fortrow.cxnfs.client.CxnFileNetworkClient;
import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import com.fortrow.cxnfs.client.QuicBulkTransferClientChannel;
import com.fortrow.cxnfs.performance.PerformanceObjective;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;

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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class BenchmarkCli {
    private static final int DEFAULT_CONTROL_PORT = 4509;
    private static final int DEFAULT_TRANSPORT_PORT = 4510;
    private static final Set<String> NATIVE_READ_OPTIONS = Set.of(
            "--label",
            "--iterations",
            "--warmup",
            "--block-size",
            "--offset",
            "--length",
            "--format",
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
            "--read-timeout-ms",
            "--report"
    );
    private static final Set<String> REJECTED_CREDENTIAL_OPTIONS = Set.of(
            "--username",
            "--user",
            "--password",
            "--password-file",
            "--auth-password",
            "--auth-password-file"
    );

    private final SequentialReadBenchmark benchmark;
    private final NativeSequentialReadBenchmark nativeBenchmark;
    private final ClientCli.CredentialProvider credentialProvider;
    private final ClientCli.NetworkConnector connector;

    public BenchmarkCli() {
        this(
                new SequentialReadBenchmark(),
                new NativeSequentialReadBenchmark(),
                ClientCredentialPrompt::prompt,
                BenchmarkCli::connectNetwork
        );
    }

    public BenchmarkCli(SequentialReadBenchmark benchmark) {
        this(
                benchmark,
                new NativeSequentialReadBenchmark(),
                ClientCredentialPrompt::prompt,
                BenchmarkCli::connectNetwork
        );
    }

    public BenchmarkCli(
            SequentialReadBenchmark benchmark,
            NativeSequentialReadBenchmark nativeBenchmark,
            ClientCli.CredentialProvider credentialProvider,
            ClientCli.NetworkConnector connector
    ) {
        if (benchmark == null) {
            throw new IllegalArgumentException("benchmark is required");
        }
        if (nativeBenchmark == null) {
            throw new IllegalArgumentException("nativeBenchmark is required");
        }
        if (credentialProvider == null) {
            throw new IllegalArgumentException("credentialProvider is required");
        }
        if (connector == null) {
            throw new IllegalArgumentException("connector is required");
        }
        this.benchmark = benchmark;
        this.nativeBenchmark = nativeBenchmark;
        this.credentialProvider = credentialProvider;
        this.connector = connector;
    }

    public int run(String[] args, PrintStream out, PrintStream err) throws IOException {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }

        try {
            return switch (args[0]) {
                case "read" -> runRead(args, out);
                case "native-read" -> runNativeRead(args, out);
                case "compare" -> runCompare(args, out);
                default -> {
                    err.println("unknown benchmark command: " + args[0]);
                    usage(err);
                    yield 2;
                }
            };
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    private int runRead(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("read requires a file path");
        }
        Map<String, String> options = parseOptions(args, 2);
        Path path = Path.of(args[1]);
        String label = options.getOrDefault("--label", "read");
        BenchmarkResult result = benchmark.run(config(label, path, options));
        out.println(BenchmarkReportFormatter.format(result));
        return 0;
    }

    private int runNativeRead(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("native-read requires a CXNFS path");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, NATIVE_READ_OPTIONS);
        ClientCli.ClientOptions clientOptions = clientOptions(options);
        NativeSequentialReadBenchmarkConfig config = nativeConfig(args[1], options);
        try (ClientCredentials credentials = credentialProvider.prompt();
             ClientCli.ClientConnection client = connector.connect(clientOptions)) {
            AuthAccepted accepted = client.authenticate(credentials);
            BenchmarkResult result = nativeBenchmark.run(config, client);
            out.println(BenchmarkReportFormatter.format(result));
            out.println("cxnfs_native host=" + clientOptions.host()
                    + " control_port=" + clientOptions.controlPort()
                    + " transport_port=" + clientOptions.transportPort()
                    + " bulk_transport=" + clientOptions.bulkTransport()
                    + " session_id=" + accepted.sessionId()
                    + " payload_format=" + formatName(config.payloadFormat())
                    + " offset=" + config.offset()
                    + " length=" + requestedLength(config.length())
                    + " initial_cords=" + clientOptions.readConfig().initialCords()
                    + " max_cords=" + clientOptions.readConfig().maxCords());
            writeNativeReport(options, clientOptions, accepted, config, result);
            return 0;
        } finally {
            clientOptions.clearTrustStorePassword();
        }
    }

    private int runCompare(String[] args, PrintStream out) throws IOException {
        Map<String, String> options = parseOptions(args, 1);
        Path baseline = requiredPath(options, "--baseline");
        Path candidate = requiredPath(options, "--candidate");
        String baselineLabel = options.getOrDefault("--baseline-label", "baseline");
        String candidateLabel = options.getOrDefault("--candidate-label", "candidate");
        String objectiveValue = options.get("--objective");
        boolean failOnMiss = booleanOption(options, "--fail-on-miss", false);

        BenchmarkComparison comparison = new BenchmarkComparison(
                benchmark.run(config(baselineLabel, baseline, options)),
                benchmark.run(config(candidateLabel, candidate, options))
        );
        out.println(BenchmarkReportFormatter.format(comparison));
        BenchmarkObjectiveEvaluation evaluation = null;
        if (objectiveValue != null && !objectiveValue.isBlank()) {
            evaluation = BenchmarkObjectiveEvaluator.evaluate(
                    objectiveName(objectiveValue),
                    comparison,
                    performanceObjective(objectiveValue)
            );
            out.println(BenchmarkReportFormatter.format(evaluation));
            if (failOnMiss && !evaluation.passed()) {
                writeCompareReport(options, comparison, evaluation);
                return 1;
            }
        }
        writeCompareReport(options, comparison, evaluation);
        return 0;
    }

    private static SequentialReadBenchmarkConfig config(String label, Path path, Map<String, String> options) {
        return new SequentialReadBenchmarkConfig(
                label,
                path,
                intOption(options, "--iterations", SequentialReadBenchmarkConfig.DEFAULT_ITERATIONS),
                intOption(options, "--warmup", SequentialReadBenchmarkConfig.DEFAULT_WARMUP_ITERATIONS),
                intOption(options, "--block-size", SequentialReadBenchmarkConfig.DEFAULT_BLOCK_SIZE_BYTES)
        );
    }

    private static NativeSequentialReadBenchmarkConfig nativeConfig(String path, Map<String, String> options) {
        return new NativeSequentialReadBenchmarkConfig(
                options.getOrDefault("--label", "cxnfs-native"),
                path,
                longOption(options, "--offset", 0),
                longOption(options, "--length", NativeSequentialReadBenchmarkConfig.READ_TO_EOF),
                intOption(options, "--iterations", SequentialReadBenchmarkConfig.DEFAULT_ITERATIONS),
                intOption(options, "--warmup", SequentialReadBenchmarkConfig.DEFAULT_WARMUP_ITERATIONS),
                intOption(options, "--block-size", SequentialReadBenchmarkConfig.DEFAULT_BLOCK_SIZE_BYTES),
                parseTransferPayloadFormat(options.getOrDefault("--format", "mds-shards"))
        );
    }

    private static Path requiredPath(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required option: " + name);
        }
        return Path.of(value);
    }

    private static int intOption(Map<String, String> options, String name, int defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static long longOption(Map<String, String> options, String name, long defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Long.parseLong(value);
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

    private static ClientCli.ClientOptions clientOptions(Map<String, String> options) throws IOException {
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
            return new ClientCli.ClientOptions(
                    host,
                    controlPort,
                    transportPort,
                    bulkTransport,
                    trustStore,
                    trustStorePassword,
                    readConfig
            );
        } finally {
            Arrays.fill(trustStorePassword, '\0');
        }
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

    private static char[] trustStorePassword(Map<String, String> options, boolean trustStoreConfigured)
            throws IOException {
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

    private static boolean booleanOption(Map<String, String> options, String name, boolean defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static TransferPayloadFormat parseTransferPayloadFormat(String value) {
        String normalized = normalizeCliToken(value);
        for (TransferPayloadFormat payloadFormat : TransferPayloadFormat.values()) {
            if (normalizeCliToken(payloadFormat.name()).equals(normalized)) {
                if (payloadFormat == TransferPayloadFormat.SAFETENSORS) {
                    throw new IllegalArgumentException("native-read does not use safetensors payloads");
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

    private static String formatName(TransferPayloadFormat payloadFormat) {
        return payloadFormat.name().toLowerCase(Locale.ROOT);
    }

    private static String requestedLength(long length) {
        return length == NativeSequentialReadBenchmarkConfig.READ_TO_EOF ? "eof" : Long.toString(length);
    }

    private static String objectiveName(String value) {
        String normalized = normalizeCliToken(value);
        if (normalized.equals("ai_training_25gbe")) {
            return "ai-training-25gbe";
        }
        throw new IllegalArgumentException("unknown benchmark objective: " + value);
    }

    private static PerformanceObjective performanceObjective(String value) {
        String normalized = normalizeCliToken(value);
        if (normalized.equals("ai_training_25gbe")) {
            return PerformanceObjective.beatLegacyMountedShareOn25GigabitDatasetReads();
        }
        throw new IllegalArgumentException("unknown benchmark objective: " + value);
    }

    private static void writeNativeReport(
            Map<String, String> options,
            ClientCli.ClientOptions clientOptions,
            AuthAccepted accepted,
            NativeSequentialReadBenchmarkConfig config,
            BenchmarkResult result
    ) throws IOException {
        String reportValue = options.get("--report");
        if (reportValue == null || reportValue.isBlank()) {
            return;
        }
        Path report = Path.of(reportValue).toAbsolutePath().normalize();
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(
                report,
                nativeReport(clientOptions, accepted, config, result),
                StandardCharsets.UTF_8
        );
    }

    private static String nativeReport(
            ClientCli.ClientOptions clientOptions,
            AuthAccepted accepted,
            NativeSequentialReadBenchmarkConfig config,
            BenchmarkResult result
    ) {
        return "cxnfs.benchmark.native-read.v1" + System.lineSeparator()
                + "label=" + config.label() + System.lineSeparator()
                + "host=" + clientOptions.host() + System.lineSeparator()
                + "control_port=" + clientOptions.controlPort() + System.lineSeparator()
                + "transport_port=" + clientOptions.transportPort() + System.lineSeparator()
                + "bulk_transport=" + clientOptions.bulkTransport() + System.lineSeparator()
                + "session_id=" + accepted.sessionId() + System.lineSeparator()
                + "path=" + config.path() + System.lineSeparator()
                + "offset=" + config.offset() + System.lineSeparator()
                + "length=" + requestedLength(config.length()) + System.lineSeparator()
                + "iterations=" + result.iterations() + System.lineSeparator()
                + "warmup=" + result.warmupIterations() + System.lineSeparator()
                + "block_size=" + result.blockSizeBytes() + System.lineSeparator()
                + "payload_format=" + formatName(config.payloadFormat()) + System.lineSeparator()
                + "initial_cords=" + clientOptions.readConfig().initialCords() + System.lineSeparator()
                + "max_cords=" + clientOptions.readConfig().maxCords() + System.lineSeparator()
                + "bytes=" + result.bytesRead() + System.lineSeparator()
                + "seconds=" + String.format(Locale.ROOT, "%.6f", result.seconds()) + System.lineSeparator()
                + "mib_per_second=" + String.format(Locale.ROOT, "%.2f", result.mebibytesPerSecond())
                + System.lineSeparator()
                + "mb_per_second=" + String.format(Locale.ROOT, "%.2f", result.megabytesPerSecond())
                + System.lineSeparator()
                + "gbit_per_second=" + String.format(Locale.ROOT, "%.3f", result.gigabitsPerSecond())
                + System.lineSeparator();
    }

    private static void writeCompareReport(
            Map<String, String> options,
            BenchmarkComparison comparison,
            BenchmarkObjectiveEvaluation evaluation
    ) throws IOException {
        String reportValue = options.get("--report");
        if (reportValue == null || reportValue.isBlank()) {
            return;
        }
        Path report = Path.of(reportValue).toAbsolutePath().normalize();
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(report, compareReport(comparison, evaluation), StandardCharsets.UTF_8);
    }

    private static String compareReport(
            BenchmarkComparison comparison,
            BenchmarkObjectiveEvaluation evaluation
    ) {
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder("cxnfs.benchmark.compare.v1")
                .append(lineSeparator);
        appendResult(builder, "baseline", comparison.baseline(), lineSeparator);
        appendResult(builder, "candidate", comparison.candidate(), lineSeparator);
        builder.append("improvement_ratio=")
                .append(String.format(Locale.ROOT, "%.6f", comparison.improvementRatio()))
                .append(lineSeparator)
                .append("candidate_faster=")
                .append(comparison.candidateIsFaster())
                .append(lineSeparator);
        if (evaluation != null) {
            builder.append("objective=")
                    .append(evaluation.objectiveName())
                    .append(lineSeparator)
                    .append("objective_baseline=")
                    .append(evaluation.objective().baselineName())
                    .append(lineSeparator)
                    .append("objective_bottleneck_gbit_s=")
                    .append(String.format(Locale.ROOT, "%.3f", evaluation.objective().bottleneckGigabitsPerSecond()))
                    .append(lineSeparator)
                    .append("objective_minimum_link_utilization=")
                    .append(String.format(Locale.ROOT, "%.3f", evaluation.objective().minimumLinkUtilization()))
                    .append(lineSeparator)
                    .append("objective_candidate_link_utilization=")
                    .append(String.format(Locale.ROOT, "%.3f", evaluation.candidateLinkUtilization()))
                    .append(lineSeparator)
                    .append("objective_minimum_candidate_mb_per_second=")
                    .append(String.format(Locale.ROOT, "%.2f", evaluation.objective().minimumReadBytesPerSecond()
                            / 1_000_000.0))
                    .append(lineSeparator)
                    .append("objective_candidate_mb_per_second=")
                    .append(String.format(Locale.ROOT, "%.2f", evaluation.comparison().candidate()
                            .megabytesPerSecond()))
                    .append(lineSeparator)
                    .append("objective_minimum_baseline_improvement=")
                    .append(String.format(Locale.ROOT, "%.6f", evaluation.objective().minimumBaselineImprovement()))
                    .append(lineSeparator)
                    .append("objective_candidate_improvement=")
                    .append(String.format(Locale.ROOT, "%.6f", evaluation.comparison().improvementRatio()))
                    .append(lineSeparator)
                    .append("objective_link_passed=")
                    .append(evaluation.linkUtilizationPassed())
                    .append(lineSeparator)
                    .append("objective_throughput_passed=")
                    .append(evaluation.minimumThroughputPassed())
                    .append(lineSeparator)
                    .append("objective_improvement_passed=")
                    .append(evaluation.baselineImprovementPassed())
                    .append(lineSeparator)
                    .append("objective_passed=")
                    .append(evaluation.passed())
                    .append(lineSeparator);
        }
        return builder.toString();
    }

    private static void appendResult(
            StringBuilder builder,
            String prefix,
            BenchmarkResult result,
            String lineSeparator
    ) {
        builder.append(prefix).append("_label=").append(result.label()).append(lineSeparator)
                .append(prefix).append("_path=").append(result.path()).append(lineSeparator)
                .append(prefix).append("_iterations=").append(result.iterations()).append(lineSeparator)
                .append(prefix).append("_warmup=").append(result.warmupIterations()).append(lineSeparator)
                .append(prefix).append("_block_size=").append(result.blockSizeBytes()).append(lineSeparator)
                .append(prefix).append("_bytes=").append(result.bytesRead()).append(lineSeparator)
                .append(prefix).append("_seconds=")
                .append(String.format(Locale.ROOT, "%.6f", result.seconds())).append(lineSeparator)
                .append(prefix).append("_mib_per_second=")
                .append(String.format(Locale.ROOT, "%.2f", result.mebibytesPerSecond())).append(lineSeparator)
                .append(prefix).append("_mb_per_second=")
                .append(String.format(Locale.ROOT, "%.2f", result.megabytesPerSecond())).append(lineSeparator)
                .append(prefix).append("_gbit_per_second=")
                .append(String.format(Locale.ROOT, "%.3f", result.gigabitsPerSecond())).append(lineSeparator);
    }

    private static ClientCli.ClientConnection connectNetwork(ClientCli.ClientOptions options) throws IOException {
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

    private static void usage(PrintStream out) {
        out.println("cxnfs benchmark read <file> [--label name] [--iterations n] [--warmup n] [--block-size bytes]");
        out.println("cxnfs benchmark native-read <path> [--label name] [--host host] [--control-port port] [--transport-port port] [--bulk-transport stream|quic] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--offset n] [--length n] [--iterations n] [--warmup n] [--block-size bytes] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n] [--report path]");
        out.println("cxnfs benchmark compare --baseline <file> --candidate <file> [--baseline-label legacy-share] [--candidate-label cxnfs] [--iterations n] [--warmup n] [--block-size bytes] [--objective ai-training-25gbe] [--fail-on-miss true|false] [--report path]");
    }

    private record NetworkClientConnection(CxnFileNetworkClient client) implements ClientCli.ClientConnection {
        @Override
        public AuthAccepted authenticate(ClientCredentials credentials) throws IOException {
            return client.authenticate(credentials);
        }

        @Override
        public com.fortrow.cxnfs.metadata.DirectoryListing list(String path) throws IOException {
            return client.list(path);
        }

        @Override
        public java.util.Optional<com.fortrow.cxnfs.metadata.FileMetadata> stat(String path) throws IOException {
            return client.stat(path);
        }

        @Override
        public java.util.Optional<com.fortrow.cxnfs.manifest.FileManifest> manifest(
                com.fortrow.cxnfs.hash.ContentHash fileHash
        ) throws IOException {
            return client.manifest(fileHash);
        }

        @Override
        public com.fortrow.cxnfs.reconstruction.ReconstructionPlan reconstructionPlan(
                com.fortrow.cxnfs.hash.ContentHash fileHash
        ) throws IOException {
            return client.reconstructionPlan(fileHash);
        }

        @Override
        public com.fortrow.cxnfs.mount.MountStrategy negotiateMount(
                com.fortrow.cxnfs.mount.MountBackend backend,
                Set<com.fortrow.cxnfs.mount.MountCapability> requestedCapabilities
        ) throws IOException {
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
        public com.fortrow.cxnfs.protocol.RenamePathResponse renamePath(String sourcePath, String targetPath)
                throws IOException {
            return client.renamePath(sourcePath, targetPath);
        }

        @Override
        public com.fortrow.cxnfs.protocol.DeletePathResponse deletePath(String path, boolean recursive)
                throws IOException {
            return client.deletePath(path, recursive);
        }

        @Override
        public com.fortrow.cxnfs.protocol.TransferAcceptedResponse bootstrapWrite(
                String rootPath,
                TransferPayloadFormat payloadFormat
        ) throws IOException {
            return client.bootstrapWrite(rootPath, payloadFormat);
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }
}
