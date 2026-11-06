package com.fortrow.cxnfs.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class LinuxBlockfsControlCommand {
    public static final Duration DEFAULT_PROCESS_TIMEOUT = Duration.ofSeconds(30);
    public static final Path DEFAULT_DEVICE = java.nio.file.Files.exists(Path.of("/dev/satfs_blockfs_ctl"))
            ? Path.of("/dev/satfs_blockfs_ctl")
            : Path.of("/dev/cxnfs_blockfs_ctl");
    public static final Path DEFAULT_EXECUTABLE = Path.of("satfs-blockfs-ctl");
    private static final int TIMEOUT_EXIT_CODE = 124;

    private final Path executable;
    private final Path device;
    private final CommandRunner runner;

    public LinuxBlockfsControlCommand(Path executable) {
        this(executable, DEFAULT_DEVICE, DEFAULT_PROCESS_TIMEOUT);
    }

    public LinuxBlockfsControlCommand(Path executable, Path device, Duration processTimeout) {
        this(executable, device, processRunner(processTimeout));
    }

    public LinuxBlockfsControlCommand(Path executable, Path device, CommandRunner runner) {
        if (executable == null) {
            throw new IllegalArgumentException("executable is required");
        }
        if (device == null) {
            throw new IllegalArgumentException("device is required");
        }
        if (runner == null) {
            throw new IllegalArgumentException("runner is required");
        }
        this.executable = executable;
        this.device = device;
        this.runner = runner;
    }

    public LinuxBlockfsStats stats() throws IOException, InterruptedException {
        CommandResult result = run(List.of("stats"));
        requireSuccess(result, "stats");
        return LinuxBlockfsStats.parse(result.stdout());
    }

    public LinuxBlockfsControlResult registerFile(LinuxBlockfsFile file)
            throws IOException, InterruptedException {
        return registerFile(LinuxBlockfsMountSelector.unselected(), file);
    }

    public LinuxBlockfsControlResult registerFile(
            LinuxBlockfsMountSelector selector,
            LinuxBlockfsFile file
    ) throws IOException, InterruptedException {
        if (file == null) {
            throw new IllegalArgumentException("file is required");
        }
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        List<String> args = new ArrayList<>();
        args.add("register-file");
        addMountSelector(args, selector);
        args.add("--inode");
        args.add(Long.toString(file.inodeNumber()));
        args.add("--path");
        args.add(file.path());
        args.add("--size");
        args.add(Long.toString(file.size()));
        args.add("--mode");
        args.add(String.format("0%o", file.mode()));
        CommandResult result = run(args);
        requireSuccess(result, "register-file");
        return LinuxBlockfsControlResult.accepted(result.stdout());
    }

    public LinuxBlockfsControlResult registerExtent(
            LinuxBlockfsExtent extent,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        return registerExtent(LinuxBlockfsMountSelector.unselected(), extent, allowUnsupported);
    }

    public LinuxBlockfsControlResult registerExtent(
            LinuxBlockfsMountSelector selector,
            LinuxBlockfsExtent extent,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        if (extent == null) {
            throw new IllegalArgumentException("extent is required");
        }
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        List<String> args = new ArrayList<>();
        args.add("register-extent");
        addMountSelector(args, selector);
        args.add("--inode");
        args.add(Long.toString(extent.inodeNumber()));
        args.add("--file-offset");
        args.add(Long.toString(extent.fileOffset()));
        args.add("--payload-block");
        args.add(Long.toString(extent.payloadBlock()));
        args.add("--payload-offset");
        args.add(Integer.toString(extent.payloadOffset()));
        args.add("--payload-length");
        args.add(Integer.toString(extent.payloadLength()));
        args.add("--hash");
        args.add(extent.expectedHashHex());
        if (allowUnsupported) {
            args.add("--allow-unsupported");
        }
        CommandResult result = run(args);
        return controlResult(result, "register-extent");
    }

    public LinuxBlockfsControlResult invalidate(
            LinuxBlockfsInvalidation invalidation,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        return invalidate(LinuxBlockfsMountSelector.unselected(), invalidation, allowUnsupported);
    }

    public LinuxBlockfsControlResult invalidate(
            LinuxBlockfsMountSelector selector,
            LinuxBlockfsInvalidation invalidation,
            boolean allowUnsupported
    ) throws IOException, InterruptedException {
        if (invalidation == null) {
            throw new IllegalArgumentException("invalidation is required");
        }
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        List<String> args = new ArrayList<>();
        args.add("invalidate");
        addMountSelector(args, selector);
        args.add("--inode");
        args.add(Long.toString(invalidation.inodeNumber()));
        args.add("--offset");
        args.add(Long.toString(invalidation.offset()));
        args.add("--length");
        args.add(Long.toString(invalidation.length()));
        if (allowUnsupported) {
            args.add("--allow-unsupported");
        }
        CommandResult result = run(args);
        return controlResult(result, "invalidate");
    }

    public LinuxBlockfsControlResult clearNamespace() throws IOException, InterruptedException {
        return clearNamespace(LinuxBlockfsMountSelector.unselected());
    }

    public LinuxBlockfsControlResult clearNamespace(LinuxBlockfsMountSelector selector)
            throws IOException, InterruptedException {
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        List<String> args = new ArrayList<>();
        args.add("clear-namespace");
        addMountSelector(args, selector);
        CommandResult result = run(args);
        requireSuccess(result, "clear-namespace");
        return LinuxBlockfsControlResult.accepted(result.stdout());
    }

    public Optional<LinuxBlockfsMissEvent> nextMissEvent(boolean nonblocking)
            throws IOException, InterruptedException {
        return nextMissEvent(LinuxBlockfsMountSelector.unselected(), nonblocking);
    }

    public Optional<LinuxBlockfsMissEvent> nextMissEvent(
            LinuxBlockfsMountSelector selector,
            boolean nonblocking
    ) throws IOException, InterruptedException {
        if (selector == null) {
            throw new IllegalArgumentException("selector is required");
        }
        List<String> args = new ArrayList<>();
        args.add("next-miss");
        addMountSelector(args, selector);
        if (nonblocking) {
            args.add("--nonblock");
        }
        CommandResult result = run(args);
        requireSuccess(result, "next-miss");
        return LinuxBlockfsMissEvent.parseOptional(result.stdout());
    }

    private CommandResult run(List<String> args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(args.size() + 3);
        command.add(executable.toString());
        command.add("--device");
        command.add(device.toString());
        command.addAll(args);
        return runner.run(List.copyOf(command));
    }

    private static void addMountSelector(List<String> args, LinuxBlockfsMountSelector selector) {
        if (selector.mountId() > 0) {
            args.add("--mount-id");
            args.add(Long.toString(selector.mountId()));
        } else if (!selector.metadataRoot().isBlank()) {
            args.add("--metadata-root");
            args.add(selector.metadataRoot());
        }
    }

    private static LinuxBlockfsControlResult controlResult(CommandResult result, String operation) {
        requireSuccess(result, operation);
        if (result.stdout().contains("unsupported=" + operation)) {
            return LinuxBlockfsControlResult.unsupported(result.stdout());
        }
        return LinuxBlockfsControlResult.accepted(result.stdout());
    }

    private static void requireSuccess(CommandResult result, String operation) {
        if (result.exitCode() != 0) {
            throw new IllegalStateException(operation + " failed with exit code "
                    + result.exitCode() + ": " + result.stderr());
        }
    }

    public static CommandResult runProcess(List<String> command) throws IOException, InterruptedException {
        return runProcess(command, DEFAULT_PROCESS_TIMEOUT);
    }

    public static CommandResult runProcess(List<String> command, Duration processTimeout)
            throws IOException, InterruptedException {
        if (command == null || command.isEmpty()) {
            throw new IllegalArgumentException("command is required");
        }
        validateProcessTimeout(processTimeout);
        Process process = new ProcessBuilder(command).start();
        CompletableFuture<byte[]> stdoutFuture = readFullyAsync(process.getInputStream());
        CompletableFuture<byte[]> stderrFuture = readFullyAsync(process.getErrorStream());
        try {
            boolean finished = process.waitFor(processTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroy();
                if (!process.waitFor(250, TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    process.waitFor();
                }
                byte[] stdout = awaitBytes(stdoutFuture);
                byte[] stderr = awaitBytes(stderrFuture);
                return new CommandResult(
                        TIMEOUT_EXIT_CODE,
                        new String(stdout, StandardCharsets.UTF_8),
                        withTimeoutMessage(new String(stderr, StandardCharsets.UTF_8), processTimeout)
                );
            }
        } catch (InterruptedException e) {
            destroyProcess(process);
            Thread.currentThread().interrupt();
            throw e;
        }
        try {
            byte[] stdout = awaitBytes(stdoutFuture);
            byte[] stderr = awaitBytes(stderrFuture);
            int exitCode = process.exitValue();
            return new CommandResult(
                    exitCode,
                    new String(stdout, StandardCharsets.UTF_8),
                    new String(stderr, StandardCharsets.UTF_8)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static CommandRunner processRunner(Duration processTimeout) {
        validateProcessTimeout(processTimeout);
        return command -> runProcess(command, processTimeout);
    }

    private static void validateProcessTimeout(Duration processTimeout) {
        if (processTimeout == null || processTimeout.isZero() || processTimeout.isNegative()) {
            throw new IllegalArgumentException("processTimeout must be positive");
        }
    }

    private static CompletableFuture<byte[]> readFullyAsync(InputStream in) {
        return CompletableFuture.supplyAsync(() -> {
            try (InputStream input = in;
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                input.transferTo(out);
                return out.toByteArray();
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    private static byte[] awaitBytes(CompletableFuture<byte[]> future)
            throws IOException, InterruptedException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CompletionException completion && completion.getCause() != null) {
                cause = completion.getCause();
            }
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("failed to read process output", cause);
        }
    }

    private static String withTimeoutMessage(String stderr, Duration processTimeout) {
        String message = "command timed out after " + processTimeout.toMillis() + " ms";
        if (stderr == null || stderr.isBlank()) {
            return message + System.lineSeparator();
        }
        if (stderr.endsWith("\n") || stderr.endsWith("\r")) {
            return stderr + message + System.lineSeparator();
        }
        return stderr + System.lineSeparator() + message + System.lineSeparator();
    }

    private static void destroyProcess(Process process) {
        process.destroy();
        if (process.isAlive()) {
            process.destroyForcibly();
        }
    }

    @FunctionalInterface
    public interface CommandRunner {
        CommandResult run(List<String> command) throws IOException, InterruptedException;
    }

    public record CommandResult(int exitCode, String stdout, String stderr) {
        public CommandResult {
            stdout = stdout == null ? "" : stdout;
            stderr = stderr == null ? "" : stderr;
        }
    }
}
