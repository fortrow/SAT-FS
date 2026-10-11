package com.fortrow.cxnfs.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProtocolFixtureReportTest {
    @TempDir
    Path tempDir;

    @Test
    void scansAllProtocolFixturesAndReportsCoverage() throws Exception {
        ProtocolFixtureReport.Result result = ProtocolFixtureReport.scan(Path.of("protocol-fixtures"));

        assertTrue(result.clean());
        assertTrue(result.coverageComplete());
        assertEquals(37, result.fixtureFiles());
        assertEquals(34, result.controlFixtureFiles());
        assertEquals(3, result.bulkFixtureFiles());
        assertTrue(result.controlOperationsCovered().contains(ControlOperation.AUTH_START));
        assertTrue(result.controlOperationsCovered().contains(ControlOperation.LIST_DIRECTORY));
        assertTrue(result.controlOperationsCovered().contains(ControlOperation.TRANSFER_BOOTSTRAP));
        assertTrue(result.controlOperationsCovered().contains(ControlOperation.MOUNT_NEGOTIATE));
        assertTrue(result.controlOperationsCovered().contains(ControlOperation.ERROR));
        assertTrue(result.controlOperationsMissing().isEmpty());
        assertTrue(result.bulkFixturesCovered().contains("BULK_DATA_FRAME"));

        String report = ProtocolFixtureReport.format(result);
        assertTrue(report.contains("cxnfs.protocol.fixtures.v1"));
        assertTrue(report.contains("fixture_files=37"));
        assertTrue(report.contains("control_fixture_files=34"));
        assertTrue(report.contains("bulk_fixture_files=3"));
        assertTrue(report.contains("coverage_complete=true"));
        assertTrue(report.contains("control_operations_missing_count=0"));
        assertTrue(report.contains("status=clean"));
    }

    @Test
    void reportsMalformedFixtureAsIssue() throws Exception {
        Files.writeString(tempDir.resolve("control-bad-frame.hex"), "zz");

        ProtocolFixtureReport.Result result = ProtocolFixtureReport.scan(tempDir);

        assertFalse(result.clean());
        assertEquals(1, result.fixtureFiles());
        assertEquals(1, result.issues().size());
        assertTrue(result.issues().getFirst().contains("invalid hex fixture"));
        assertTrue(ProtocolFixtureReport.format(result).contains("status=issues"));
    }
}
