package com.fortrow.satfs;

import com.fortrow.cxnfs.benchmark.BenchmarkCli;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.client.ClientCli;
import com.fortrow.cxnfs.dataset.DatasetCli;
import com.fortrow.cxnfs.install.InstallServiceCli;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.mount.MountBridgeCli;
import com.fortrow.cxnfs.mount.MountCli;
import com.fortrow.cxnfs.protocol.ProtocolCli;
import com.fortrow.cxnfs.server.ServerCli;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transfer.TransferCli;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * SAT-FS (Saturated Async Transfer File System) command-line interface.
 */
public final class Satfs {
    public static final String VERSION = "0.1.0";

    private Satfs() {
    }

    public static void main(String[] args) throws Exception {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args, PrintStream out, PrintStream err) throws Exception {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help") || args[0].equals("-h")) {
            usage(out);
            return 0;
        }

        if (args[0].equals("version") || args[0].equals("--version") || args[0].equals("-v")) {
            out.println("SAT-FS (Saturated Async Transfer File System) version " + VERSION);
            return 0;
        }

        return switch (args[0]) {
            case "benchmark" -> new BenchmarkCli().run(tail(args), out, err);
            case "client" -> new ClientCli().run(tail(args), out, err);
            case "dataset" -> new DatasetCli().run(tail(args), out, err);
            case "directory" -> new TransferCli().run("directory", tail(args), out, err);
            case "file" -> new TransferCli().run("file", tail(args), out, err);
            case "fuse-bridge" -> new MountBridgeCli().run(tail(args), out, err);
            case "install-service" -> new InstallServiceCli().run(tail(args), out, err);
            case "mount" -> new MountCli().run(tail(args), out, err);
            case "protocol" -> new ProtocolCli().run(tail(args), out, err);
            case "serve" -> new ServerCli().run(prependServe(tail(args)), out, err);
            case "serve-blockfs", "serve-cxbfs", "serve-satfs" -> new ServerCli().run(prependCommand(tail(args), args[0]), out, err);
            case "server" -> new ServerCli().run(tail(args), out, err);
            case "demo" -> runDemo(out);
            default -> {
                err.println("unknown command: " + args[0]);
                usage(err);
                yield 2;
            }
        };
    }

    private static int runDemo(PrintStream out) throws IOException {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest manifest = repository.put(
                "/hello.txt",
                "SAT-FS (Saturated Async Transfer File System) demo\n".getBytes(StandardCharsets.UTF_8)
        );
        out.println("stored " + manifest.path() + " as " + manifest.fileHash());
        return 0;
    }

    private static String[] tail(String[] args) {
        return Arrays.copyOfRange(args, 1, args.length);
    }

    private static String[] prependServe(String[] args) {
        String[] serverArgs = new String[args.length + 1];
        serverArgs[0] = "serve";
        System.arraycopy(args, 0, serverArgs, 1, args.length);
        return serverArgs;
    }

    private static String[] prependCommand(String[] args, String command) {
        String[] serverArgs = new String[args.length + 1];
        serverArgs[0] = command;
        System.arraycopy(args, 0, serverArgs, 1, args.length);
        return serverArgs;
    }

    private static void usage(PrintStream out) {
        out.println("sat-fs <command>");
        out.println("commands:");
        out.println("  serve           serve one or more directories through SAT-FS server facade");
        out.println("  serve-satfs     serve a mounted kernel blockfs tree through SAT-FS facade");
        out.println("  serve-cxbfs     compatibility alias for serve-satfs");
        out.println("  serve-blockfs   compatibility alias for serve-satfs");
        out.println("  server          server management subcommands");
        out.println("  client          authenticated network client commands");
        out.println("  file            send or read one file directly over saturated stream");
        out.println("  directory       send or read a directory hierarchy directly");
        out.println("  mount           mount a served namespace prefix into native filesystem");
        out.println("  dataset         prepare and stream MDS-Shards and Safetensors checkpoints");
        out.println("  install-service install the SAT-FS service for this platform");
        out.println("  protocol        inspect protocol fixture compatibility");
        out.println("  benchmark       benchmark saturated dataset-read and transfer throughput");
        out.println("  demo            store a local demo file");
        out.println("  version         display SAT-FS version");
    }
}
