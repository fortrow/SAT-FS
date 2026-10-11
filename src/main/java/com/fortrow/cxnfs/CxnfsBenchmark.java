package com.fortrow.cxnfs;

import com.fortrow.cxnfs.benchmark.BenchmarkCli;

public final class CxnfsBenchmark {
    private CxnfsBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new BenchmarkCli().run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }
}
