package com.cdc.etl.transformer;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Generates deterministic surrogate keys via MD5 hashing.
 * <p>
 * The formula is deliberately Python-compatible: given an MD5 hex digest,
 * take the first 16 characters and parse as a hexadecimal integer, equivalent
 * to Python's {@code int(md5_hash.hexdigest()[:16], 16)}.
 * <p>
 * The result is constrained to fit in a Java {@code long} (64-bit signed) by
 * masking with {@code 2^64 - 1}, matching the Python behavior where the
 * intermediate value is a big integer truncated to 64 bits.
 * <p>
 * Key format: composite keys are built as {@code "table|natural_key"}
 * to ensure uniqueness across dimension tables.
 */
@Component
public class Md5KeyGenerator {

    private static final Logger log = LoggerFactory.getLogger(Md5KeyGenerator.class);

    private static final BigInteger MASK_64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private final String algorithm;

    public Md5KeyGenerator(@Value("${keys.algorithm:MD5}") String algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Generate a non-negative surrogate key from a composite key string.
     * <p>
     * Formula: {@code MD5("table|natural_key") → first 16 hex chars → BigInteger(16) → mask 64 bits → long}
     *
     * @param table      table name (e.g. "dim_customer")
     * @param naturalKey the business key value (e.g. "42")
     * @return non-negative long surrogate key
     */
    public long generateKey(String table, String naturalKey) {
        String compositeKey = table + "|" + naturalKey;
        byte[] digest = md5(compositeKey);
        String hex = bytesToHex(digest);

        // Take first 16 hex chars → equivalent to Python int(md5_hex[:16], 16)
        String prefix16 = hex.substring(0, 16);
        BigInteger bigInt = new BigInteger(prefix16, 16);

        // Mask to 64 bits, ensuring non-negative
        long key = bigInt.and(MASK_64).longValue();
        return key;
    }

    /**
     * Generate a key from a multi-part natural key (e.g., composite FK).
     *
     * @param table       table name
     * @param naturalKeys ordered list of natural key components
     * @return non-negative long surrogate key
     */
    public long generateKey(String table, Object... naturalKeys) {
        StringBuilder sb = new StringBuilder(table);
        for (Object key : naturalKeys) {
            sb.append('|').append(key);
        }
        byte[] digest = md5(sb.toString());
        String hex = bytesToHex(digest);
        String prefix16 = hex.substring(0, 16);
        return new BigInteger(prefix16, 16).and(MASK_64).longValue();
    }

    /**
     * Generate the raw MD5 hex digest for test-vector verification.
     * Exposed for parity testing against Python.
     */
    public String md5Hex(String input) {
        return bytesToHex(md5(input));
    }

    // ── Internal helpers ────────────────────────────

    private byte[] md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            return md.digest(input.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available in this JVM", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
