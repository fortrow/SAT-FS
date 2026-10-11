package com.fortrow.cxnfs.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ServerDaemonLockTest {
    @TempDir
    Path tempDir;

    @Test
    void replacesStaleDaemonLockWithDeadPid() throws Exception {
        Path lockFile = tempDir.resolve("server.lock");
        Files.writeString(lockFile, "pid=" + Long.MAX_VALUE + System.lineSeparator());

        try (AutoCloseable lock = createDaemonLock(lockFile)) {
            String lockText = Files.readString(lockFile);
            assertTrue(lockText.contains("pid=" + ProcessHandle.current().pid()));
        }

        assertFalse(Files.exists(lockFile));
    }

    @Test
    void rejectsDaemonLockWithLivePid() throws Exception {
        Path lockFile = tempDir.resolve("server.lock");
        Files.writeString(lockFile, "pid=" + ProcessHandle.current().pid() + System.lineSeparator());

        InvocationTargetException failure = assertThrows(
                InvocationTargetException.class,
                () -> createDaemonLock(lockFile)
        );

        assertTrue(failure.getCause().getMessage().contains("cxnfs daemon is already running"));
    }

    private static AutoCloseable createDaemonLock(Path lockFile) throws Exception {
        Method method = ServerCli.class.getDeclaredMethod("createDaemonLock", Path.class);
        method.setAccessible(true);
        return (AutoCloseable) method.invoke(null, lockFile);
    }
}
