from pathlib import Path
import io
import tempfile
import unittest

from cxnfs import ControlOperation
from cxnfs.protocol_report import (
    format_protocol_fixture_report,
    run_protocol_fixture_report_cli,
    scan_protocol_fixtures,
)


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "protocol-fixtures"


class ProtocolFixtureReportTest(unittest.TestCase):
    def test_scans_all_fixtures_and_reports_complete_operation_coverage(self):
        report = scan_protocol_fixtures(FIXTURE_ROOT)

        self.assertTrue(report.clean)
        self.assertTrue(report.coverage_complete)
        self.assertEqual(37, report.fixture_files)
        self.assertEqual(34, report.control_fixture_files)
        self.assertEqual(3, report.bulk_fixture_files)
        self.assertEqual(set(ControlOperation), set(report.control_operations_covered))
        self.assertEqual((), report.control_operations_missing)
        self.assertEqual(
            ("BULK_DATA_FRAME", "BULK_READ_REQUEST", "BULK_WRITE_ACK"),
            report.bulk_fixtures_covered,
        )

        text = format_protocol_fixture_report(report)
        self.assertIn("cxnfs.protocol.fixtures.v1", text)
        self.assertIn("fixture_files=37", text)
        self.assertIn("control_fixture_files=34", text)
        self.assertIn("bulk_fixture_files=3", text)
        self.assertIn("control_operations_missing_count=0", text)
        self.assertIn("coverage_complete=true", text)
        self.assertIn("status=clean", text)

    def test_package_root_lazily_exports_report_api(self):
        from cxnfs import ProtocolFixtureReport, scan_protocol_fixtures as exported_scan

        report = exported_scan(FIXTURE_ROOT)

        self.assertIsInstance(report, ProtocolFixtureReport)
        self.assertTrue(report.coverage_complete)

    def test_reports_malformed_fixture_as_issue(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "control-bad-frame.hex").write_text("zz", encoding="utf-8")

            report = scan_protocol_fixtures(root)

        self.assertFalse(report.clean)
        self.assertEqual(1, report.fixture_files)
        self.assertEqual(1, len(report.issues))
        self.assertIn("invalid hex fixture", report.issues[0])
        self.assertIn("status=issues", format_protocol_fixture_report(report))

    def test_cli_writes_report_and_can_fail_on_missing_coverage(self):
        out = io.StringIO()
        err = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "protocol-fixtures.txt"
            exit_code = run_protocol_fixture_report_cli(
                [
                    str(FIXTURE_ROOT),
                    "--report",
                    str(report_path),
                    "--fail-on-missing",
                    "true",
                ],
                out=out,
                err=err,
            )

            report_text = report_path.read_text(encoding="utf-8")

        self.assertEqual(0, exit_code)
        self.assertEqual("", err.getvalue())
        self.assertIn("cxnfs.protocol.fixtures.v1", out.getvalue())
        self.assertIn("coverage_complete=true", out.getvalue())
        self.assertIn("status=clean", out.getvalue())
        self.assertIn("report=", out.getvalue())
        self.assertIn("coverage_complete=true", report_text)
        self.assertIn("status=clean", report_text)

    def test_cli_returns_usage_error_for_missing_fixture_directory(self):
        out = io.StringIO()
        err = io.StringIO()

        exit_code = run_protocol_fixture_report_cli(
            [str(FIXTURE_ROOT / "missing")],
            out=out,
            err=err,
        )

        self.assertEqual(2, exit_code)
        self.assertEqual("", out.getvalue())
        self.assertIn("failed to scan protocol fixtures", err.getvalue())


if __name__ == "__main__":
    unittest.main()
