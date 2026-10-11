package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.client.ClientCli;
import com.fortrow.cxnfs.client.ClientCredentials;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BenchmarkCliTest {
    @TempDir
    Path tempDir;

    @Test
    void readCommandPrintsSequentialReadResult() throws Exception {
        Path file = tempDir.resolve("read.bin");
        Files.write(file, new byte[2048]);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new BenchmarkCli().run(
                new String[]{"read", file.toString(), "--label", "legacy-share", "--iterations", "2", "--warmup", "0", "--block-size", "512"},
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("legacy-share"));
        assertTrue(output.contains("bytes=4096"));
    }

    @Test
    void compareCommandPrintsImprovementLine() throws Exception {
        Path baseline = tempDir.resolve("baseline.bin");
        Path candidate = tempDir.resolve("candidate.bin");
        Files.write(baseline, new byte[1024]);
        Files.write(candidate, new byte[1024]);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new BenchmarkCli().run(
                new String[]{
                        "compare",
                        "--baseline", baseline.toString(),
                        "--candidate", candidate.toString(),
                        "--baseline-label", "legacy-share",
                        "--candidate-label", "cxnfs",
                        "--iterations", "1",
                        "--warmup", "0",
                        "--block-size", "256"
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("legacy-share"));
        assertTrue(output.contains("cxnfs"));
        assertTrue(output.contains("candidate_vs_baseline"));
    }

    @Test
    void compareCommandPrintsObjectiveEvaluationWhenRequested() throws Exception {
        Path baseline = tempDir.resolve("baseline-objective.bin");
        Path candidate = tempDir.resolve("candidate-objective.bin");
        Path report = tempDir.resolve("reports/compare-objective.txt");
        Files.write(baseline, new byte[1024 * 1024]);
        Files.write(candidate, new byte[1024 * 1024]);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int exit = new BenchmarkCli().run(
                new String[]{
                        "compare",
                        "--baseline", baseline.toString(),
                        "--candidate", candidate.toString(),
                        "--baseline-label", "legacy-share",
                        "--candidate-label", "cxnfs",
                        "--iterations", "1",
                        "--warmup", "0",
                        "--block-size", "65536",
                        "--objective", "ai-training-25gbe",
                        "--report", report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        String output = out.toString();
        assertTrue(output.contains("candidate_vs_baseline"));
        assertTrue(output.contains("objective=ai-training-25gbe"));
        assertTrue(output.contains("baseline=legacy-mounted-share"));
        assertTrue(output.contains("objective_passed="));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.benchmark.compare.v1"));
        assertTrue(reportText.contains("baseline_label=legacy-share"));
        assertTrue(reportText.contains("candidate_label=cxnfs"));
        assertTrue(reportText.contains("objective=ai-training-25gbe"));
        assertTrue(reportText.contains("objective_passed="));
    }

    @Test
    void nativeReadCommandAuthenticatesAndReadsRemoteRanges() throws Exception {
        FakeClientConnection client = new FakeClientConnection(4096);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Path report = tempDir.resolve("native-report.txt");

        int exit = new BenchmarkCli(
                new SequentialReadBenchmark(),
                new NativeSequentialReadBenchmark(),
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> {
                    assertEquals("cxnfs.example.internal", options.host());
                    assertEquals("quic", options.bulkTransport());
                    assertEquals(8, options.readConfig().initialCords());
                    assertEquals(64, options.readConfig().maxCords());
                    return client;
                }
        ).run(
                new String[]{
                        "native-read",
                        "/datasets/train/shard.00000.mds",
                        "--host", "cxnfs.example.internal",
                        "--label", "cxnfs-native",
                        "--iterations", "2",
                        "--warmup", "1",
                        "--block-size", "1024",
                        "--bulk-transport", "quic",
                        "--initial-cords", "8",
                        "--max-cords", "64",
                        "--report", report.toString()
                },
                new PrintStream(out),
                System.err
        );

        assertEquals(0, exit);
        assertTrue(client.authenticated);
        assertTrue(client.closed);
        assertEquals(12, client.readCalls);
        assertEquals(12_288, client.readBytes);
        String output = out.toString();
        assertTrue(output.contains("cxnfs-native"));
        assertTrue(output.contains("bytes=8192"));
        assertTrue(output.contains("initial_cords=8"));
        assertTrue(output.contains("bulk_transport=quic"));
        String reportText = Files.readString(report);
        assertTrue(reportText.contains("cxnfs.benchmark.native-read.v1"));
        assertTrue(reportText.contains("path=/datasets/train/shard.00000.mds"));
        assertTrue(reportText.contains("payload_format=mds_shards"));
        assertTrue(reportText.contains("bulk_transport=quic"));
        assertTrue(reportText.contains("bytes=8192"));
    }

    @Test
    void nativeReadRejectsCommandLinePasswordOptions() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exit = new BenchmarkCli(
                new SequentialReadBenchmark(),
                new NativeSequentialReadBenchmark(),
                () -> new ClientCredentials("trainer", "secret".toCharArray()),
                options -> new FakeClientConnection(1024)
        ).run(
                new String[]{"native-read", "/datasets/train/shard.mds", "--password", "secret"},
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(err)
        );

        assertEquals(2, exit);
        assertTrue(err.toString().contains("local hidden prompt"));
    }

    private static final class FakeClientConnection implements ClientCli.ClientConnection {
        private final long size;
        private boolean authenticated;
        private boolean closed;
        private int readCalls;
        private long readBytes;

        private FakeClientConnection(long size) {
            this.size = size;
        }

        @Override
        public AuthAccepted authenticate(ClientCredentials credentials) {
            authenticated = true;
            return new AuthAccepted(UUID.fromString("00000000-0000-0000-0000-000000000123"),
                    List.of(AuthMechanism.USERNAME_PASSWORD));
        }

        @Override
        public DirectoryListing list(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileMetadata> stat(String path) {
            return Optional.of(new FileMetadata(
                    path,
                    "shard.00000.mds",
                    FileType.REGULAR_FILE,
                    size,
                    Optional.empty(),
                    Instant.EPOCH
            ));
        }

        @Override
        public Optional<FileManifest> manifest(ContentHash fileHash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReconstructionPlan reconstructionPlan(ContentHash fileHash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MountStrategy negotiateMount(MountBackend backend, Set<MountCapability> requestedCapabilities) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] readRange(String path, long offset, long length, TransferPayloadFormat payloadFormat) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long readRangeTo(
                String path,
                long offset,
                long length,
                TransferPayloadFormat payloadFormat,
                OutputStream output
        ) throws IOException {
            assertEquals(TransferPayloadFormat.MDS_SHARDS, payloadFormat);
            output.write(new byte[(int) length]);
            readCalls++;
            readBytes += length;
            return length;
        }

        @Override
        public RenamePathResponse renamePath(String sourcePath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeletePathResponse deletePath(String path, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
