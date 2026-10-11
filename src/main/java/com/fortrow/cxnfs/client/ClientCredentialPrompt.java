package com.fortrow.cxnfs.client;

import java.io.Console;
import java.io.IOException;
import java.util.Arrays;

public final class ClientCredentialPrompt {
    public interface Terminal {
        String readLine(String prompt) throws IOException;

        char[] readPassword(String prompt) throws IOException;
    }

    private ClientCredentialPrompt() {
    }

    public static ClientCredentials prompt() throws IOException {
        return prompt(systemTerminal());
    }

    public static ClientCredentials prompt(Terminal terminal) throws IOException {
        if (terminal == null) {
            throw new IllegalArgumentException("terminal is required");
        }
        String username = terminal.readLine("CXNFS username: ");
        char[] password = terminal.readPassword("CXNFS password: ");
        if (password == null) {
            password = new char[0];
        }
        try {
            return new ClientCredentials(username, password);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    public static Terminal systemTerminal() throws IOException {
        Console console = System.console();
        if (console == null) {
            throw new IOException("hidden password prompt is not available; run the client from an interactive console");
        }
        return new ConsoleTerminal(console);
    }

    private record ConsoleTerminal(Console console) implements Terminal {
        @Override
        public String readLine(String prompt) {
            return console.readLine(prompt);
        }

        @Override
        public char[] readPassword(String prompt) {
            return console.readPassword(prompt);
        }
    }
}
