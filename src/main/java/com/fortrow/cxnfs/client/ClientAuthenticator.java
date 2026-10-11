package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.auth.PasswordAuthProof;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.AuthProof;
import com.fortrow.cxnfs.protocol.AuthStart;

import java.util.Arrays;

public final class ClientAuthenticator {
    public AuthStart start(ClientCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("credentials are required");
        }
        return new AuthStart(AuthMechanism.USERNAME_PASSWORD, credentials.username());
    }

    public AuthProof answer(AuthChallenge challenge, ClientCredentials credentials) {
        if (challenge == null) {
            throw new IllegalArgumentException("challenge is required");
        }
        if (credentials == null) {
            throw new IllegalArgumentException("credentials are required");
        }
        if (challenge.mechanism() != AuthMechanism.USERNAME_PASSWORD) {
            throw new IllegalArgumentException("unsupported client auth challenge mechanism: " + challenge.mechanism());
        }
        if (!PasswordAuthProof.PROOF_ALGORITHM.equals(challenge.proofAlgorithm())) {
            throw new IllegalArgumentException("unsupported client auth proof algorithm: " + challenge.proofAlgorithm());
        }
        if (!challenge.principal().equals(credentials.username())) {
            throw new IllegalArgumentException("auth challenge principal does not match credentials");
        }

        char[] password = credentials.password();
        try {
            byte[] proof = PasswordAuthProof.derive(password, challenge.nonce(), challenge.iterations());
            try {
                return new AuthProof(challenge.challengeId(), credentials.username(), proof);
            } finally {
                Arrays.fill(proof, (byte) 0);
            }
        } finally {
            Arrays.fill(password, '\0');
        }
    }
}
