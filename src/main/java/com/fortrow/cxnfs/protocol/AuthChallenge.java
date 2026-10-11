package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;

import java.util.Arrays;
import java.util.UUID;

public record AuthChallenge(
        UUID challengeId,
        AuthMechanism mechanism,
        String principal,
        byte[] nonce,
        String proofAlgorithm,
        int iterations
) implements ControlPayload {
    public AuthChallenge {
        if (challengeId == null) {
            throw new IllegalArgumentException("challengeId is required");
        }
        if (mechanism == null) {
            throw new IllegalArgumentException("mechanism is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
        if (nonce == null || nonce.length == 0) {
            throw new IllegalArgumentException("nonce is required");
        }
        if (proofAlgorithm == null || proofAlgorithm.isBlank()) {
            throw new IllegalArgumentException("proofAlgorithm is required");
        }
        if (iterations <= 0) {
            throw new IllegalArgumentException("iterations must be positive");
        }
        nonce = nonce.clone();
    }

    @Override
    public byte[] nonce() {
        return nonce.clone();
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.AUTH_CHALLENGE;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof AuthChallenge that
                && challengeId.equals(that.challengeId)
                && mechanism == that.mechanism
                && principal.equals(that.principal)
                && Arrays.equals(nonce, that.nonce)
                && proofAlgorithm.equals(that.proofAlgorithm)
                && iterations == that.iterations;
    }

    @Override
    public int hashCode() {
        int result = challengeId.hashCode();
        result = 31 * result + mechanism.hashCode();
        result = 31 * result + principal.hashCode();
        result = 31 * result + Arrays.hashCode(nonce);
        result = 31 * result + proofAlgorithm.hashCode();
        result = 31 * result + iterations;
        return result;
    }

    @Override
    public String toString() {
        return "AuthChallenge[challengeId=" + challengeId
                + ", mechanism=" + mechanism
                + ", principal=" + principal
                + ", nonceLength=" + nonce.length
                + ", proofAlgorithm=" + proofAlgorithm
                + ", iterations=" + iterations
                + "]";
    }
}
