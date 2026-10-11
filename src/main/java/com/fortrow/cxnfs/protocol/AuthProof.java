package com.fortrow.cxnfs.protocol;

import java.util.Arrays;
import java.util.UUID;

public record AuthProof(
        UUID challengeId,
        String principal,
        byte[] proof
) implements ControlPayload {
    public AuthProof {
        if (challengeId == null) {
            throw new IllegalArgumentException("challengeId is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
        if (proof == null || proof.length == 0) {
            throw new IllegalArgumentException("proof is required");
        }
        proof = proof.clone();
    }

    @Override
    public byte[] proof() {
        return proof.clone();
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.AUTH_PROOF;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof AuthProof that
                && challengeId.equals(that.challengeId)
                && principal.equals(that.principal)
                && Arrays.equals(proof, that.proof);
    }

    @Override
    public int hashCode() {
        int result = challengeId.hashCode();
        result = 31 * result + principal.hashCode();
        result = 31 * result + Arrays.hashCode(proof);
        return result;
    }

    @Override
    public String toString() {
        return "AuthProof[challengeId=" + challengeId
                + ", principal=" + principal
                + ", proofLength=" + proof.length
                + "]";
    }
}
