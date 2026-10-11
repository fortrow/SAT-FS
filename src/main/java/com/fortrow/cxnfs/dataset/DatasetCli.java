package com.fortrow.cxnfs.dataset;

import com.fortrow.cxnfs.client.ClientCredentialPrompt;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.client.ClientTlsContextFactory;
import com.fortrow.cxnfs.client.ControlTlsClientSocketFactory;
import com.fortrow.cxnfs.client.CxnFileNetworkClient;
import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.transport.BulkDataFrame;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class DatasetCli {
    private static final int DEFAULT_CONTROL_PORT = 4509;
    private static final int DEFAULT_TRANSPORT_PORT = 4510;
    private static final Set<String> SEND_OPTIONS = Set.of(
            "--format",
            "--execute",
            "--remote-root",
            "--host",
            "--control-port",
            "--transport-port",
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
    private static final Set<String> REJECTED_CREDENTIAL_OPTIONS = Set.of(
            "--username",
            "--user",
            "--password",
            "--password-file",
            "--auth-password",
            "--auth-password-file"
    );

    private final DatasetFormatDecider decider;
    private final ClientCredentialProvider credentialProvider;
    private final NetworkConnector connector;
    private final SafetensorsOutputSender outputSender;

    public DatasetCli() {
        this(
                new DatasetFormatDecider(),
                ClientCredentialPrompt::prompt,
                DatasetCli::connectNetwork,
                new SafetensorsOutputSender()
        );
    }

    public DatasetCli(DatasetFormatDecider decider) {
        this(decider, ClientCredentialPrompt::prompt, DatasetCli::connectNetwork, new SafetensorsOutputSender());
    }

    DatasetCli(
            DatasetFormatDecider decider,
            ClientCredentialProvider credentialProvider,
            NetworkConnector connector,
            SafetensorsOutputSender outputSender
    ) {
        this.decider = decider;
        if (credentialProvider == null) {
            throw new IllegalArgumentException("credentialProvider is required");
        }
        if (connector == null) {
            throw new IllegalArgumentException("connector is required");
        }
        if (outputSender == null) {
            throw new IllegalArgumentException("outputSender is required");
        }
        this.credentialProvider = credentialProvider;
        this.connector = connector;
        this.outputSender = outputSender;
    }

    public int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }

        try {
            return switch (args[0]) {
                case "recommend" -> recommend(args, out);
                case "prepare" -> prepare(args, out);
                case "send" -> send(args, out);
                default -> {
                    err.println("unknown dataset command: " + args[0]);
                    usage(err);
                    yield 2;
                }
            };
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println("dataset command failed: " + e.getMessage());
            return 1;
        }
    }

    private int recommend(String[] args, PrintStream out) {
        if (args.length < 2) {
            throw new IllegalArgumentException("recommend requires a use case");
        }
        DatasetFormatRecommendation recommendation = decider.recommend(DatasetUseCase.parse(args[1]));
        out.println("dataset recommendation");
        out.println("use_case=" + recommendation.useCase().cliName());
        out.println("primary_format=" + recommendation.primaryFormat().cliName());
        out.println("companion_formats=" + formatList(recommendation.companionFormats()));
        out.println("native_python_access=" + recommendation.preferNativePythonAccess());
        out.println("reason=" + recommendation.reason());
        return 0;
    }

    private int prepare(String[] args, PrintStream out) {
        if (args.length < 3) {
            throw new IllegalArgumentException("prepare requires an input directory and dataset name");
        }
        Map<String, String> options = parseOptions(args, 3);
        DatasetFormat format = DatasetFormat.parse(options.getOrDefault("--format", DatasetFormat.MDS_SHARDS.cliName()));
        DatasetSourceFormat sourceFormat = DatasetSourceFormat.parse(
                options.getOrDefault("--source-format", DatasetSourceFormat.FILES.cliName())
        );
        long targetShardSizeBytes = longOption(
                options,
                "--target-shard-size",
                DatasetPreparePlan.DEFAULT_TARGET_SHARD_SIZE_BYTES
        );
        DatasetPreparePlan plan = new DatasetPreparePlan(
                Path.of(args[1]),
                args[2],
                sourceFormat,
                format,
                targetShardSizeBytes,
                true
        );
        out.println("dataset prepare plan");
        out.println("input_directory=" + plan.inputDirectory());
        out.println("dataset_name=" + plan.datasetName());
        out.println("source_format=" + plan.sourceFormat().cliName());
        out.println("format=" + plan.format().cliName());
        out.println("target_shard_size_bytes=" + plan.targetShardSizeBytes());
        out.println("native_python_access=" + plan.nativePythonAccess());
        DatasetConversionPlan conversionPlan = null;
        if (options.containsKey("--plan-manifest")) {
            conversionPlan = conversionPlan(plan, conversionPlan);
            writeConversionManifest(conversionPlan, Path.of(options.get("--plan-manifest")), out);
        }
        if (options.containsKey("--execute")) {
            String outputDirectory = options.get("--output-dir");
            if (outputDirectory == null) {
                throw new IllegalArgumentException("prepare --execute requires --output-dir <path>");
            }
            conversionPlan = conversionPlan(plan, conversionPlan);
            executeConversion(conversionPlan, Path.of(outputDirectory), out);
        }
        out.println("next=convert " + plan.sourceFormat().displayName() + " into CXNFS-addressed "
                + plan.format().displayName() + " dataset shards");
        return 0;
    }

    private int send(String[] args, PrintStream out) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("send requires an output directory");
        }
        Map<String, String> options = parseOptions(args, 2);
        validateOptions(options, SEND_OPTIONS);
        DatasetFormat format = DatasetFormat.parse(options.getOrDefault("--format", DatasetFormat.SAFETENSORS.cliName()));
        DatasetSendPlan plan = new DatasetSendPlan(Path.of(args[1]), format, true);
        if (options.containsKey("--execute")) {
            String remoteRoot = requiredOption(options, "--remote-root");
            DatasetClientOptions clientOptions = clientOptions(options);
            try {
                return withWriteClient(clientOptions, client -> {
                    DatasetSendResult result = outputSender.send(plan, remoteRoot, client);
                    out.println("dataset send result");
                    out.println("host=" + clientOptions.host());
                    out.println("control_port=" + clientOptions.controlPort());
                    out.println("transport_port=" + clientOptions.transportPort());
                    out.println("output_directory=" + result.outputDirectory());
                    out.println("remote_root=" + result.remoteRoot());
                    out.println("files_sent=" + result.fileCount());
                    out.println("tensor_files_sent=" + result.tensorFileCount());
                    out.println("bytes_sent=" + result.bytesSent());
                    out.println("content_hash=" + result.contentHash());
                    return 0;
                });
            } finally {
                clientOptions.clearTrustStorePassword();
            }
        }

        out.println("dataset send plan");
        out.println("output_directory=" + plan.outputDirectory());
        out.println("format=" + plan.format().cliName());
        out.println("native_python_access=" + plan.nativePythonAccess());
        if (options.containsKey("--remote-root")) {
            out.println("remote_root=" + options.get("--remote-root"));
        }
        out.println("next=add --execute --remote-root <path> --host <host> to send through authenticated CXNFS write operations");
        return 0;
    }

    private int withWriteClient(DatasetClientOptions options, DatasetWriteOperation operation) throws IOException {
        try (ClientCredentials credentials = credentialProvider.prompt();
             DatasetWriteConnection client = connector.connect(options)) {
            client.authenticate(credentials);
            return operation.run(client);
        }
    }

    private static Map<String, String> parseOptions(String[] args, int startIndex) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = startIndex; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("expected option but found: " + key);
            }
            if (key.equals("--execute")) {
                options.put(key, "true");
                continue;
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

    private static DatasetWriteConnection connectNetwork(DatasetClientOptions options) throws IOException {
        ClientTlsContextFactory contextFactory = new ClientTlsContextFactory();
        SSLContext context = options.trustStore().isPresent()
                ? contextFactory.create(options.trustStore().orElseThrow(), options.trustStorePassword())
                : contextFactory.createDefault();
        SocketFactory socketFactory = new ControlTlsClientSocketFactory(context);
        return new NetworkDatasetWriteConnection(CxnFileNetworkClient.connect(
                new InetSocketAddress(options.host(), options.controlPort()),
                new InetSocketAddress(options.host(), options.transportPort()),
                socketFactory,
                socketFactory,
                options.transferConfig()
        ));
    }

    private static DatasetClientOptions clientOptions(Map<String, String> options) throws IOException {
        String host = options.getOrDefault("--host", "127.0.0.1");
        int controlPort = intOption(options, "--control-port", DEFAULT_CONTROL_PORT);
        int transportPort = intOption(options, "--transport-port", DEFAULT_TRANSPORT_PORT);
        Path trustStore = options.containsKey("--truststore")
                ? Path.of(options.get("--truststore"))
                : null;
        char[] trustStorePassword = trustStorePassword(options, trustStore != null);
        try {
            CxnFileNetworkClientConfig defaults = CxnFileNetworkClientConfig.defaults();
            CxnFileNetworkClientConfig transferConfig = new CxnFileNetworkClientConfig(
                    intOption(options, "--initial-cords", defaults.initialCords()),
                    intOption(options, "--max-cords", defaults.maxCords()),
                    intOption(options, "--provisional-container-size", defaults.provisionalContainerSizeBytes()),
                    longOption(options, "--expected-size-limit", defaults.expectedSizeLimitBytes()),
                    intOption(options, "--connect-timeout-ms", defaults.connectTimeoutMillis()),
                    intOption(options, "--read-timeout-ms", defaults.readTimeoutMillis())
            );
            return new DatasetClientOptions(host, controlPort, transportPort, trustStore, trustStorePassword, transferConfig);
        } finally {
            Arrays.fill(trustStorePassword, '\0');
        }
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

    private static long longOption(Map<String, String> options, String name, long defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    private static int intOption(Map<String, String> options, String name, int defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static boolean booleanOption(Map<String, String> options, String name, boolean defaultValue) {
        String value = options.get(name);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static String requiredOption(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required option: " + name);
        }
        return value;
    }

    private static DatasetConversionPlan conversionPlan(DatasetPreparePlan preparePlan, DatasetConversionPlan current) {
        if (current != null) {
            return current;
        }
        try {
            return new DatasetConversionService().plan(preparePlan);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to plan dataset conversion: " + e.getMessage(), e);
        }
    }

    private static void writeConversionManifest(DatasetConversionPlan conversionPlan, Path outputPath, PrintStream out) {
        try {
            new DatasetConversionManifestWriter().write(conversionPlan, outputPath);
            out.println("source_files=" + conversionPlan.sourceFiles().size());
            out.println("planned_shards=" + conversionPlan.shards().size());
            out.println("conversion_manifest=" + outputPath);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to write conversion manifest: " + e.getMessage(), e);
        }
    }

    private static void executeConversion(DatasetConversionPlan conversionPlan, Path outputDirectory, PrintStream out) {
        try {
            DatasetConversionResult result = new DatasetConversionExecutor().execute(conversionPlan, outputDirectory);
            out.println("converted_shards=" + result.shardCount());
            out.println("converted_samples=" + result.sampleCount());
            out.println("converted_bytes=" + result.bytesWritten());
            out.println("dataset_index=" + result.indexPath());
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to execute dataset conversion: " + e.getMessage(), e);
        }
    }

    private static String formatList(Iterable<DatasetFormat> formats) {
        StringBuilder builder = new StringBuilder();
        for (DatasetFormat format : formats) {
            if (!builder.isEmpty()) {
                builder.append(',');
            }
            builder.append(format.cliName());
        }
        return builder.isEmpty() ? "none" : builder.toString();
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs dataset recommend <training-reads|analytics|checkpoint-writes|mixed>");
        out.println("cxnfs dataset prepare <input-dir> <dataset-name> [--source-format files|hdf5|tfrecord|webdataset|parquet] [--format mds-shards] [--target-shard-size bytes] [--plan-manifest path] [--execute --output-dir path]");
        out.println("cxnfs dataset send <output-dir> [--format safetensors] [--execute --remote-root path --host host] [--control-port port] [--transport-port port] [--truststore path] [--truststore-password-file path|--truststore-password-stdin true] [--initial-cords n] [--max-cords n] [--connect-timeout-ms n] [--read-timeout-ms n]");
    }

    @FunctionalInterface
    interface ClientCredentialProvider {
        ClientCredentials prompt() throws IOException;
    }

    @FunctionalInterface
    interface NetworkConnector {
        DatasetWriteConnection connect(DatasetClientOptions options) throws IOException;
    }

    @FunctionalInterface
    private interface DatasetWriteOperation {
        int run(DatasetWriteConnection client) throws IOException;
    }

    interface DatasetWriteConnection extends SafetensorsOutputSender.WriteClient, AutoCloseable {
        AuthAccepted authenticate(ClientCredentials credentials) throws IOException;

        @Override
        void close() throws IOException;
    }

    record DatasetClientOptions(
            String host,
            int controlPort,
            int transportPort,
            Path trustStorePath,
            char[] trustStorePassword,
            CxnFileNetworkClientConfig transferConfig
    ) {
        DatasetClientOptions {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host is required");
            }
            if (controlPort <= 0 || controlPort > 65535) {
                throw new IllegalArgumentException("controlPort must be between 1 and 65535");
            }
            if (transportPort <= 0 || transportPort > 65535) {
                throw new IllegalArgumentException("transportPort must be between 1 and 65535");
            }
            trustStorePassword = trustStorePassword == null ? new char[0] : trustStorePassword.clone();
            if (transferConfig == null) {
                throw new IllegalArgumentException("transferConfig is required");
            }
        }

        @Override
        public char[] trustStorePassword() {
            return trustStorePassword.clone();
        }

        public java.util.Optional<Path> trustStore() {
            return java.util.Optional.ofNullable(trustStorePath);
        }

        public void clearTrustStorePassword() {
            Arrays.fill(trustStorePassword, '\0');
        }
    }

    private record NetworkDatasetWriteConnection(CxnFileNetworkClient client) implements DatasetWriteConnection {
        @Override
        public AuthAccepted authenticate(ClientCredentials credentials) throws IOException {
            return client.authenticate(credentials);
        }

        @Override
        public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat)
                throws IOException {
            return client.bootstrapWrite(rootPath, payloadFormat);
        }

        @Override
        public void sendWriteFrame(BulkDataFrame frame) throws IOException {
            client.sendWriteFrame(frame);
        }

        @Override
        public TransferEntryAcceptedResponse acceptWriteEntry(TransferEntryUpdate update) throws IOException {
            return client.acceptWriteEntry(update);
        }

        @Override
        public TransferMetadataAcceptedResponse acceptWriteMetadata(TransferMetadataUpdate update) throws IOException {
            return client.acceptWriteMetadata(update);
        }

        @Override
        public TransferEntryCommittedResponse commitWriteEntry(TransferEntryCommitRequest request) throws IOException {
            return client.commitWriteEntry(request);
        }

        @Override
        public TransferCommittedResponse commitWriteTransfer(TransferCommitRequest request) throws IOException {
            return client.commitWriteTransfer(request);
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }
}
