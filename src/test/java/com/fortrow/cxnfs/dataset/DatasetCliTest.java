package com.fortrow.cxnfs.dataset;

import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DatasetCliTest {
    @TempDir
    Path tempDir;

    @Test
    void recommendTrainingReadsPrintsMdsShards() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{"recommend", "training-reads"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("primary_format=mds-shards"));
        assertTrue(output.contains("native_python_access=true"));
    }

    @Test
    void prepareDefaultsToMdsShards() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{"prepare", "/raw/imagenet", "imagenet"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("dataset prepare plan"));
        assertTrue(output.contains("source_format=files"));
        assertTrue(output.contains("format=mds-shards"));
        assertTrue(output.contains("target_shard_size_bytes=268435456"));
    }

    @Test
    void prepareAcceptsMdsAlias() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{"prepare", "/raw/imagenet", "imagenet", "--format", "mds"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertTrue(out.toString().contains("format=mds-shards"));
    }

    @Test
    void prepareAcceptsStreamingSourceFormatsForMdsShardConversion() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{"prepare", "/raw/webdataset", "images", "--source-format", "webdataset"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("source_format=webdataset"));
        assertTrue(output.contains("format=mds-shards"));
    }

    @Test
    void prepareCanWriteServerSideConversionManifest() throws Exception {
        Path input = tempDir.resolve("input");
        Files.createDirectories(input);
        Files.write(input.resolve("part-000.parquet"), new byte[4]);
        Files.write(input.resolve("part-001.parquet"), new byte[4]);
        Files.write(input.resolve("skip.txt"), new byte[4]);
        Path manifest = tempDir.resolve("plan/cxnfs-prepare-plan.json");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{
                        "prepare",
                        input.toString(),
                        "train",
                        "--source-format", "parquet",
                        "--target-shard-size", "5",
                        "--plan-manifest", manifest.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("source_files=2"));
        assertTrue(output.contains("planned_shards=2"));
        assertTrue(output.contains("conversion_manifest=" + manifest));
        String json = Files.readString(manifest);
        assertTrue(json.contains("\"source_format\": \"parquet\""));
        assertTrue(json.contains("\"shard_name\": \"train-000000.mds\""));
        assertTrue(json.contains("\"shard_name\": \"train-000001.mds\""));
        assertTrue(json.contains("part-000.parquet"));
    }

    @Test
    void prepareCanExecuteServerSideConversion() throws Exception {
        Path input = tempDir.resolve("execute-input");
        Files.createDirectories(input);
        Files.writeString(input.resolve("part-000.parquet"), "alpha");
        Files.writeString(input.resolve("part-001.parquet"), "beta");
        Path outputDir = tempDir.resolve("mds-output");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{
                        "prepare",
                        input.toString(),
                        "train",
                        "--source-format", "parquet",
                        "--execute",
                        "--output-dir", outputDir.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("converted_shards=1"));
        assertTrue(output.contains("converted_samples=2"));
        assertTrue(output.contains("dataset_index=" + outputDir.resolve("index.json")));
        assertTrue(Files.exists(outputDir.resolve("train-000000.mds")));
        assertTrue(Files.exists(outputDir.resolve("index.json")));
    }

    @Test
    void prepareRejectsParquetAsTrainingReadFormat() {
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{"prepare", "/raw/table", "table", "--format", "parquet"},
                System.out,
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("mds-shards"));
    }

    @Test
    void sendDefaultsToSafetensors() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli().run(
                new String[]{"send", "/outputs/checkpoint"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("dataset send plan"));
        assertTrue(output.contains("format=safetensors"));
        assertTrue(output.contains("native_python_access=true"));
    }

    @Test
    void sendExecuteUsesAuthenticatedWriteClient() throws Exception {
        Path outputDir = tempDir.resolve("checkpoint");
        Files.createDirectories(outputDir);
        Files.write(outputDir.resolve("model.safetensors"), "tensor".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        RecordingCredentialProvider credentials = new RecordingCredentialProvider();
        RecordingConnector connector = new RecordingConnector();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new DatasetCli(
                new DatasetFormatDecider(),
                credentials,
                connector,
                new SafetensorsOutputSender()
        ).run(
                new String[]{
                        "send",
                        outputDir.toString(),
                        "--execute",
                        "--remote-root", "/outputs/run-1",
                        "--host", "cxnfs.example.internal",
                        "--control-port", "4509",
                        "--transport-port", "4510",
                        "--initial-cords", "8",
                        "--max-cords", "64",
                        "--connect-timeout-ms", "1234",
                        "--read-timeout-ms", "5678"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertEquals(1, credentials.calls);
        assertEquals("trainer", connector.connection.authenticatedUsername);
        assertTrue(connector.connection.closed);
        assertEquals("cxnfs.example.internal", connector.options.host());
        assertEquals(4509, connector.options.controlPort());
        assertEquals(4510, connector.options.transportPort());
        assertEquals(8, connector.options.transferConfig().initialCords());
        assertEquals(64, connector.options.transferConfig().maxCords());
        assertEquals(1234, connector.options.transferConfig().connectTimeoutMillis());
        assertEquals(5678, connector.options.transferConfig().readTimeoutMillis());
        assertEquals("/outputs/run-1", connector.connection.bootstrapRoot);
        assertEquals(List.of("bootstrap", "frame", "entry", "metadata", "entry-commit", "transfer-commit"),
                connector.connection.events);
        String output = out.toString();
        assertTrue(output.contains("dataset send result"));
        assertTrue(output.contains("remote_root=/outputs/run-1"));
        assertTrue(output.contains("files_sent=1"));
        assertTrue(output.contains("tensor_files_sent=1"));
        assertTrue(output.contains("bytes_sent=6"));
    }

    @Test
    void sendRejectsCommandLinePasswordBeforePrompting() throws Exception {
        Path outputDir = tempDir.resolve("checkpoint");
        Files.createDirectories(outputDir);
        Files.write(outputDir.resolve("model.safetensors"), new byte[]{1});
        RecordingCredentialProvider credentials = new RecordingCredentialProvider();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = new DatasetCli(
                new DatasetFormatDecider(),
                credentials,
                new RecordingConnector(),
                new SafetensorsOutputSender()
        ).run(
                new String[]{
                        "send",
                        outputDir.toString(),
                        "--execute",
                        "--remote-root", "/outputs/run-1",
                        "--host", "cxnfs.example.internal",
                        "--password", "secret"
                },
                System.out,
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertEquals(0, credentials.calls);
        assertTrue(err.toString().contains("hidden prompt"));
    }

    private static final class RecordingCredentialProvider implements DatasetCli.ClientCredentialProvider {
        private int calls;

        @Override
        public ClientCredentials prompt() {
            calls++;
            return new ClientCredentials("trainer", "secret".toCharArray());
        }
    }

    private static final class RecordingConnector implements DatasetCli.NetworkConnector {
        private DatasetCli.DatasetClientOptions options;
        private final RecordingDatasetWriteConnection connection = new RecordingDatasetWriteConnection();

        @Override
        public DatasetCli.DatasetWriteConnection connect(DatasetCli.DatasetClientOptions options) {
            this.options = options;
            return connection;
        }
    }

    private static final class RecordingDatasetWriteConnection implements DatasetCli.DatasetWriteConnection {
        private final UUID transferId = UUID.randomUUID();
        private final List<String> events = new java.util.ArrayList<>();
        private String authenticatedUsername;
        private String bootstrapRoot;
        private boolean closed;

        @Override
        public AuthAccepted authenticate(ClientCredentials credentials) {
            authenticatedUsername = credentials.username();
            return new AuthAccepted(UUID.randomUUID(), List.of(AuthMechanism.USERNAME_PASSWORD));
        }

        @Override
        public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) {
            events.add("bootstrap");
            bootstrapRoot = rootPath;
            return new TransferAcceptedResponse(transferId, TransferDirection.WRITE, 1, 4, 64 * 1024 * 1024, true);
        }

        @Override
        public void sendWriteFrame(BulkDataFrame frame) {
            events.add("frame");
        }

        @Override
        public TransferEntryAcceptedResponse acceptWriteEntry(TransferEntryUpdate update) {
            events.add("entry");
            return new TransferEntryAcceptedResponse(update.transferId(), update.entrySequence(), update.path());
        }

        @Override
        public TransferMetadataAcceptedResponse acceptWriteMetadata(TransferMetadataUpdate update) {
            events.add("metadata");
            return new TransferMetadataAcceptedResponse(
                    update.transferId(),
                    update.entrySequence(),
                    update.metadataSequence(),
                    update.chunkHashes().size(),
                    update.chunkHashes().size(),
                    update.finalMetadata()
            );
        }

        @Override
        public TransferEntryCommittedResponse commitWriteEntry(TransferEntryCommitRequest request) {
            events.add("entry-commit");
            return new TransferEntryCommittedResponse(
                    request.transferId(),
                    request.entrySequence(),
                    request.path(),
                    request.contentHash(),
                    request.bytesWritten()
            );
        }

        @Override
        public TransferCommittedResponse commitWriteTransfer(TransferCommitRequest request) {
            events.add("transfer-commit");
            return new TransferCommittedResponse(request.transferId(), request.contentHash(), request.bytesWritten());
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
