package com.fortrow.cxnfs.protocol;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public final class ProtocolCli {
    public int run(String[] args, PrintStream out, PrintStream err) throws IOException {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            usage(out);
            return 0;
        }

        return switch (args[0]) {
            case "fixture-report" -> runFixtureReport(Arrays.copyOfRange(args, 1, args.length), out, err);
            default -> {
                err.println("unknown protocol command: " + args[0]);
                usage(err);
                yield 2;
            }
        };
    }

    private int runFixtureReport(String[] args, PrintStream out, PrintStream err) throws IOException {
        if (args.length == 0) {
            err.println("fixture-report requires a fixture directory");
            fixtureReportUsage(err);
            return 2;
        }

        Path fixtureRoot = Path.of(args[0]);
        Path reportPath = null;
        boolean failOnIssues = true;
        boolean failOnMissing = false;

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--report" -> {
                    if (!hasValue(args, i)) {
                        err.println("--report requires a value");
                        fixtureReportUsage(err);
                        return 2;
                    }
                    reportPath = Path.of(args[++i]);
                }
                case "--fail-on-issues" -> {
                    if (!hasValue(args, i)) {
                        err.println("--fail-on-issues requires a value");
                        fixtureReportUsage(err);
                        return 2;
                    }
                    failOnIssues = Boolean.parseBoolean(args[++i]);
                }
                case "--fail-on-missing" -> {
                    if (!hasValue(args, i)) {
                        err.println("--fail-on-missing requires a value");
                        fixtureReportUsage(err);
                        return 2;
                    }
                    failOnMissing = Boolean.parseBoolean(args[++i]);
                }
                default -> {
                    err.println("unknown fixture-report option: " + args[i]);
                    fixtureReportUsage(err);
                    return 2;
                }
            }
        }

        ProtocolFixtureReport.Result result;
        try {
            result = ProtocolFixtureReport.scan(fixtureRoot);
        } catch (IOException e) {
            err.println("failed to scan protocol fixtures: " + e.getMessage());
            return 2;
        }
        String report = ProtocolFixtureReport.format(result);
        out.print(report);
        if (reportPath != null) {
            Path normalized = reportPath.toAbsolutePath().normalize();
            if (normalized.getParent() != null) {
                Files.createDirectories(normalized.getParent());
            }
            Files.writeString(normalized, report);
            out.println("report=" + normalized);
        }

        if (failOnIssues && !result.clean()) {
            return 1;
        }
        if (failOnMissing && !result.coverageComplete()) {
            return 1;
        }
        return 0;
    }

    private static boolean hasValue(String[] args, int optionIndex) {
        return optionIndex + 1 < args.length && !args[optionIndex + 1].startsWith("--");
    }

    private static void usage(PrintStream out) {
        out.println("cxnfs protocol <command>");
        out.println("commands:");
        out.println("  fixture-report <fixture-dir>  decode and round-trip protocol fixtures");
    }

    private static void fixtureReportUsage(PrintStream out) {
        out.println("usage: cxnfs protocol fixture-report <fixture-dir> [--report <path>] "
                + "[--fail-on-issues true|false] [--fail-on-missing true|false]");
    }
}
