package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkReadRequestCodec;
import com.fortrow.cxnfs.transport.BulkWriteAck;
import com.fortrow.cxnfs.transport.BulkWriteAckCodec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public final class ProtocolFixtureReport {
    private ProtocolFixtureReport() {
    }

    public static Result scan(Path fixtureRoot) throws IOException {
        if (fixtureRoot == null) {
            throw new IllegalArgumentException("fixtureRoot is required");
        }
        Path root = fixtureRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IOException("fixture root is not a directory: " + root);
        }

        List<Path> fixtures;
        try (var stream = Files.list(root)) {
            fixtures = stream
                    .filter(path -> path.getFileName().toString().endsWith(".hex"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        }

        EnumSet<ControlOperation> coveredOperations = EnumSet.noneOf(ControlOperation.class);
        List<String> bulkFixtures = new ArrayList<>();
        List<String> issues = new ArrayList<>();
        int controlFixtureFiles = 0;
        int bulkFixtureFiles = 0;

        for (Path fixture : fixtures) {
            String name = fixture.getFileName().toString();
            byte[] bytes;
            try {
                bytes = readHexFixture(fixture);
            } catch (RuntimeException | IOException e) {
                issues.add(name + ": invalid hex fixture: " + e.getMessage());
                continue;
            }

            try {
                if (name.startsWith("control-")) {
                    controlFixtureFiles++;
                    decodeControlFixture(name, bytes, coveredOperations, issues);
                } else if (name.equals("bulk-read-request.hex")) {
                    bulkFixtureFiles++;
                    BulkReadRequest request = BulkReadRequestCodec.decode(bytes);
                    if (!Arrays.equals(bytes, BulkReadRequestCodec.encode(request))) {
                        issues.add(name + ": bulk read request did not round trip");
                    }
                    bulkFixtures.add("BULK_READ_REQUEST");
                } else if (name.equals("bulk-data-frame.hex")) {
                    bulkFixtureFiles++;
                    BulkDataFrame frame = BulkDataFrameCodec.decode(bytes);
                    if (!Arrays.equals(bytes, BulkDataFrameCodec.encode(frame))) {
                        issues.add(name + ": bulk data frame did not round trip");
                    }
                    bulkFixtures.add("BULK_DATA_FRAME");
                } else if (name.equals("bulk-write-ack.hex")) {
                    bulkFixtureFiles++;
                    BulkWriteAck ack = BulkWriteAckCodec.decode(bytes);
                    if (!Arrays.equals(bytes, BulkWriteAckCodec.encode(ack))) {
                        issues.add(name + ": bulk write acknowledgement did not round trip");
                    }
                    bulkFixtures.add("BULK_WRITE_ACK");
                } else {
                    issues.add(name + ": unknown fixture kind");
                }
            } catch (RuntimeException | IOException e) {
                issues.add(name + ": decode failure: " + e.getMessage());
            }
        }

        EnumSet<ControlOperation> missingOperations = EnumSet.allOf(ControlOperation.class);
        missingOperations.removeAll(coveredOperations);

        return new Result(
                root,
                fixtures.size(),
                controlFixtureFiles,
                bulkFixtureFiles,
                sortedOperations(coveredOperations),
                sortedOperations(missingOperations),
                bulkFixtures.stream().distinct().sorted().toList(),
                List.copyOf(issues)
        );
    }

    public static String format(Result result) {
        String lineSeparator = System.lineSeparator();
        StringBuilder out = new StringBuilder();
        out.append("cxnfs.protocol.fixtures.v1").append(lineSeparator);
        out.append("fixture_root=").append(result.fixtureRoot()).append(lineSeparator);
        out.append("fixture_files=").append(result.fixtureFiles()).append(lineSeparator);
        out.append("control_fixture_files=").append(result.controlFixtureFiles()).append(lineSeparator);
        out.append("bulk_fixture_files=").append(result.bulkFixtureFiles()).append(lineSeparator);
        out.append("control_operations_covered_count=")
                .append(result.controlOperationsCovered().size())
                .append(lineSeparator);
        out.append("control_operations_covered=")
                .append(joinOperations(result.controlOperationsCovered()))
                .append(lineSeparator);
        out.append("control_operations_missing_count=")
                .append(result.controlOperationsMissing().size())
                .append(lineSeparator);
        out.append("control_operations_missing=")
                .append(joinOperations(result.controlOperationsMissing()))
                .append(lineSeparator);
        out.append("bulk_fixtures_covered_count=")
                .append(result.bulkFixturesCovered().size())
                .append(lineSeparator);
        out.append("bulk_fixtures_covered=")
                .append(String.join(",", result.bulkFixturesCovered()))
                .append(lineSeparator);
        out.append("coverage_complete=")
                .append(result.coverageComplete())
                .append(lineSeparator);
        out.append("issue_count=").append(result.issues().size()).append(lineSeparator);
        for (int i = 0; i < result.issues().size(); i++) {
            out.append("issue.")
                    .append(i)
                    .append("=")
                    .append(result.issues().get(i))
                    .append(lineSeparator);
        }
        out.append("status=").append(result.clean() ? "clean" : "issues").append(lineSeparator);
        return out.toString();
    }

    private static void decodeControlFixture(
            String name,
            byte[] bytes,
            Set<ControlOperation> coveredOperations,
            List<String> issues
    ) throws IOException {
        ControlFrame frame = ControlFrameCodec.decode(bytes);
        ControlPayload payload = ControlPayloadCodec.decode(frame.operation(), frame.payload());
        byte[] payloadBytes = ControlPayloadCodec.encode(payload);
        if (!Arrays.equals(frame.payload(), payloadBytes)) {
            issues.add(name + ": control payload did not round trip");
        }
        byte[] encodedFrame = ControlFrameCodec.encode(new ControlFrame(
                frame.version(),
                frame.messageId(),
                frame.sessionId(),
                frame.operation(),
                payloadBytes
        ));
        if (!Arrays.equals(bytes, encodedFrame)) {
            issues.add(name + ": control frame did not round trip");
        }
        coveredOperations.add(frame.operation());
    }

    private static byte[] readHexFixture(Path fixture) throws IOException {
        String hex = Files.readString(fixture).replaceAll("\\s+", "");
        return HexFormat.of().parseHex(hex);
    }

    private static List<ControlOperation> sortedOperations(Set<ControlOperation> operations) {
        return operations.stream()
                .sorted(Comparator.comparingInt(ControlOperation::code))
                .toList();
    }

    private static String joinOperations(List<ControlOperation> operations) {
        return operations.stream()
                .map(operation -> operation.name().toLowerCase(Locale.ROOT))
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    public record Result(
            Path fixtureRoot,
            int fixtureFiles,
            int controlFixtureFiles,
            int bulkFixtureFiles,
            List<ControlOperation> controlOperationsCovered,
            List<ControlOperation> controlOperationsMissing,
            List<String> bulkFixturesCovered,
            List<String> issues
    ) {
        public boolean clean() {
            return issues.isEmpty();
        }

        public boolean coverageComplete() {
            return controlOperationsMissing.isEmpty();
        }
    }
}
