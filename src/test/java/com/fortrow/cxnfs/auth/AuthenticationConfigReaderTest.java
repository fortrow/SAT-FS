package com.fortrow.cxnfs.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AuthenticationConfigReaderTest {
    @TempDir
    Path tempDir;

    @Test
    void readsUsernamePasswordAndBearerTokenConfig() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.mechanisms=username_password,bearer_token
                auth.user.trainer.password=secret
                auth.user.trainer.directories=/datasets,/srv
                auth.user.eval.password=eval-secret
                auth.bearer-token.training=token-1
                """
        );

        AuthenticationPolicy policy = new AuthenticationConfigReader().read(config);

        assertEquals(List.of(AuthMechanism.USERNAME_PASSWORD, AuthMechanism.BEARER_TOKEN), policy.acceptedMechanisms());
        assertTrue(authenticates(policy, "trainer", "secret"));
        assertTrue(authenticates(policy, "eval", "eval-secret"));
        assertFalse(authenticates(policy, "trainer", "eval-secret"));
        assertFalse(authenticates(policy, "unknown", "secret"));
        assertTrue(policy.canAccessPath("trainer", "/datasets/train/shard.mds"));
        assertTrue(policy.canAccessPath("trainer", "/srv/output.bin"));
        assertFalse(policy.canAccessPath("trainer", "/outputs/run-1/model.bin"));
        assertTrue(policy.canAccessPath("eval", "/outputs/run-1/model.bin"));
        assertFalse(policy.canAccessPath("unknown", "/datasets/train/shard.mds"));
        assertTrue(policy.authenticateBearerToken("token-1"));
    }

    @Test
    void derivesMechanismsFromConfiguredCredentials() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                """
        );

        AuthenticationPolicy policy = new AuthenticationConfigReader().read(config);

        assertEquals(List.of(AuthMechanism.USERNAME_PASSWORD), policy.acceptedMechanisms());
        assertTrue(authenticates(policy, "trainer", "secret"));
    }

    @Test
    void acceptsLegacyAndNamedUsersTogetherWhenUsernamesAreDistinct() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                auth.user.eval.password=eval-secret
                """
        );

        AuthenticationPolicy policy = new AuthenticationConfigReader().read(config);

        assertEquals(List.of(AuthMechanism.USERNAME_PASSWORD), policy.acceptedMechanisms());
        assertTrue(authenticates(policy, "trainer", "secret"));
        assertTrue(authenticates(policy, "eval", "eval-secret"));
    }

    @Test
    void acceptsDirectoriesForLegacyUser() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                auth.user.trainer.directories=/datasets
                """
        );

        AuthenticationPolicy policy = new AuthenticationConfigReader().read(config);

        assertTrue(authenticates(policy, "trainer", "secret"));
        assertTrue(policy.canAccessPath("trainer", "/datasets/train/shard.mds"));
        assertFalse(policy.canAccessPath("trainer", "/outputs/run-1/model.bin"));
    }

    @Test
    void rejectsDuplicateConfiguredUsername() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                auth.user.trainer.password=other-secret
                """
        );

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new AuthenticationConfigReader().read(config)
        );
        assertTrue(error.getMessage().contains("duplicate configured username: trainer"));
    }

    @Test
    void rejectsDirectoriesForUnknownUser() throws Exception {
        Path config = tempDir.resolve("cxnfs-server.properties");
        Files.writeString(
                config,
                """
                auth.username=trainer
                auth.password=secret
                auth.user.eval.directories=/datasets
                """
        );

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new AuthenticationConfigReader().read(config)
        );
        assertTrue(error.getMessage().contains("directories configured for unknown user: eval"));
    }

    private static boolean authenticates(AuthenticationPolicy policy, String username, String password) {
        byte[] nonce = PasswordAuthProof.newNonce();
        byte[] proof = PasswordAuthProof.derive(password.toCharArray(), nonce, 2);
        return policy.authenticateUsernamePasswordProof(username, nonce, 2, proof);
    }
}
