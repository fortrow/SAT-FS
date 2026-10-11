package com.fortrow.cxnfs.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

public final class PasswordAuthProof {
    public static final String PROOF_ALGORITHM = "PBKDF2WithHmacSHA256";
    public static final int DEFAULT_ITERATIONS = 65_536;
    public static final int NONCE_BYTES = 32;
    public static final int PROOF_BYTES = 32;

    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordAuthProof() {
    }

    public static byte[] newNonce() {
        byte[] nonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(nonce);
        return nonce;
    }

    public static byte[] derive(char[] password, byte[] nonce, int iterations) {
        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("password is required");
        }
        if (nonce == null || nonce.length == 0) {
            throw new IllegalArgumentException("nonce is required");
        }
        if (iterations <= 0) {
            throw new IllegalArgumentException("iterations must be positive");
        }

        PBEKeySpec spec = new PBEKeySpec(password, nonce.clone(), iterations, PROOF_BYTES * Byte.SIZE);
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(PROOF_ALGORITHM);
            return factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException("password proof algorithm is unavailable", e);
        } finally {
            spec.clearPassword();
        }
    }

    public static boolean verify(String expectedPassword, byte[] nonce, int iterations, byte[] proof) {
        if (expectedPassword == null || proof == null) {
            return false;
        }
        char[] password = expectedPassword.toCharArray();
        try {
            return verify(password, nonce, iterations, proof);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    public static boolean verify(char[] expectedPassword, byte[] nonce, int iterations, byte[] proof) {
        if (expectedPassword == null || expectedPassword.length == 0 || proof == null) {
            return false;
        }
        byte[] actual = derive(expectedPassword, nonce, iterations);
        try {
            return MessageDigest.isEqual(actual, proof);
        } finally {
            Arrays.fill(actual, (byte) 0);
        }
    }
}
