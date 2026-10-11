package com.fortrow.cxnfs.auth;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AuthenticationPolicyTest {
    @Test
    void requiresAtLeastOneMechanism() {
        assertThrows(IllegalArgumentException.class, () -> new AuthenticationPolicy(List.of()));
    }

    @Test
    void usernamePasswordPolicyIsTheDefaultBootstrapChoice() {
        AuthenticationPolicy policy = AuthenticationPolicy.requireUsernamePassword(Map.of("ryan", "secret"));
        byte[] nonce = PasswordAuthProof.newNonce();
        byte[] proof = PasswordAuthProof.derive("secret".toCharArray(), nonce, 2);

        assertEquals(List.of(AuthMechanism.USERNAME_PASSWORD), policy.acceptedMechanisms());
        assertTrue(policy.authenticateUsernamePasswordProof("ryan", nonce, 2, proof));
        assertFalse(policy.authenticateUsernamePasswordProof("ryan", nonce, 2, PasswordAuthProof.derive("wrong".toCharArray(), nonce, 2)));
    }

    @Test
    void bearerTokenIsOptionalAndExplicit() {
        AuthenticationPolicy policy = AuthenticationPolicy.fromConfig(
                List.of(AuthMechanism.USERNAME_PASSWORD, AuthMechanism.BEARER_TOKEN),
                Map.of("trainer", "secret"),
                List.of("token-1")
        );

        assertTrue(policy.authenticateBearerToken("token-1"));
        assertFalse(policy.authenticateBearerToken("token-2"));
    }

    @Test
    void passwordProofUsesPbkdf2Sha256KnownVector() {
        byte[] proof = PasswordAuthProof.derive("password".toCharArray(), "salt".getBytes(java.nio.charset.StandardCharsets.UTF_8), 1);

        assertArrayEquals(
                hex("120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"),
                proof
        );
    }

    @Test
    void mechanismsRequireConfiguredCredentials() {
        assertThrows(
                IllegalArgumentException.class,
                () -> AuthenticationPolicy.fromConfig(List.of(AuthMechanism.USERNAME_PASSWORD), Map.of(), List.of())
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> AuthenticationPolicy.fromConfig(List.of(AuthMechanism.BEARER_TOKEN), Map.of(), List.of())
        );
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int offset = i * 2;
            bytes[i] = (byte) Integer.parseInt(value.substring(offset, offset + 2), 16);
        }
        return bytes;
    }
}
