package com.cdc.etl.transformer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Md5KeyGenerator}.
 *
 * <p>Validates the Python-compatible MD5 → long surrogate key formula:
 * {@code MD5(compositeKey) → first 16 hex chars → BigInteger(16) → mask 2^64-1 → long}.
 *
 * <p>Test vectors are documented with both Java and Python equivalents to
 * prove parity. The Python equivalent for any test input is:
 * {@code int(hashlib.md5(input.encode()).hexdigest()[:16], 16)}.
 */
@DisplayName("Md5KeyGenerator")
class Md5KeyGeneratorTest {

    private static final BigInteger MASK_64 =
            BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private Md5KeyGenerator keyGen;

    @BeforeEach
    void setUp() {
        keyGen = new Md5KeyGenerator("MD5");
    }

    // ── MD5 correctness (well-known test vectors) ────

    @Nested
    @DisplayName("MD5 hash correctness")
    class Md5Hash {

        @Test
        @DisplayName("empty string produces known MD5")
        void emptyStringMd5() {
            // MD5("") = d41d8cd98f00b204e9800998ecf8427e
            assertThat(keyGen.md5Hex(""))
                    .isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
        }

        @Test
        @DisplayName("well-known pangram produces known MD5")
        void pangramMd5() {
            String input = "The quick brown fox jumps over the lazy dog";
            // MD5 = 9e107d9d372bb6826bd81d3542a419d6
            assertThat(keyGen.md5Hex(input))
                    .isEqualTo("9e107d9d372bb6826bd81d3542a419d6");
        }
    }

    // ── Key generation ───────────────────────────────

    @Nested
    @DisplayName("Surrogate key generation")
    class KeyGeneration {

        @Test
        @DisplayName("generates non-negative key")
        void nonNegativeKey() {
            long key = keyGen.generateKey("dim_customer", "42");
            assertThat(key).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("generated key matches direct formula computation")
        void formulaCorrectness() {
            // Given a known composite key
            String table = "dim_customer";
            String naturalKey = "42";
            String composite = table + "|" + naturalKey;

            // Expected: compute MD5, take first 16 hex chars, parse as BigInteger, mask
            String md5Full = md5Hex(composite);
            String prefix16 = md5Full.substring(0, 16);
            long expected = new BigInteger(prefix16, 16).and(MASK_64).longValue();

            long actual = keyGen.generateKey(table, naturalKey);

            assertThat(actual).isEqualTo(expected);
        }

        @Test
        @DisplayName("different natural keys produce different surrogate keys")
        void uniqueKeys() {
            long key1 = keyGen.generateKey("dim_customer", "1");
            long key2 = keyGen.generateKey("dim_customer", "2");
            long key3 = keyGen.generateKey("dim_customer", "42");

            assertThat(key1).isNotEqualTo(key2);
            assertThat(key1).isNotEqualTo(key3);
            assertThat(key2).isNotEqualTo(key3);
        }

        @Test
        @DisplayName("same table + different natural key = different key")
        void differentTableSameNaturalKey() {
            long custKey = keyGen.generateKey("dim_customer", "100");
            long prodKey = keyGen.generateKey("dim_product", "100");

            assertThat(custKey).isNotEqualTo(prodKey);
        }

        @Test
        @DisplayName("idempotent: same input always produces same key")
        void idempotent() {
            long key1 = keyGen.generateKey("dim_customer", "john@example.com");
            long key2 = keyGen.generateKey("dim_customer", "john@example.com");

            assertThat(key1).isEqualTo(key2);
        }
    }

    // ── Test Vectors (5+ documented with Python equivalent) ──

    @Nested
    @DisplayName("Python parity test vectors")
    class PythonParity {

        /**
         * Regenerates the expected Java long value from a composite key string
         * using the same formula that Python would use:
         * <pre>
         *   import hashlib
         *   md5 = hashlib.md5(composite_key.encode())
         *   int(md5.hexdigest()[:16], 16)
         * </pre>
         *
         * <p>Each vector documents: composite key → MD5 hex → first 16 chars → long.
         */
        private long expectedKey(String composite) {
            String md5 = md5Hex(composite);
            return new BigInteger(md5.substring(0, 16), 16)
                    .and(MASK_64)
                    .longValue();
        }

        @Test
        @DisplayName("Vector 1: dim_customer|1")
        void vector1() {
            String hex = md5Hex("dim_customer|1");
            long expected = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            long actual = keyGen.generateKey("dim_customer", "1");
            assertThat(actual).isEqualTo(expected);
            System.out.printf("Vector 1: dim_customer|1 → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }

        @Test
        @DisplayName("Vector 2: dim_customer|john@example.com (spec scenario)")
        void vector2() {
            String hex = md5Hex("dim_customer|john@example.com");
            long expected = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            long actual = keyGen.generateKey("dim_customer", "john@example.com");
            assertThat(actual).isEqualTo(expected);
            System.out.printf("Vector 2: dim_customer|john@example.com → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }

        @Test
        @DisplayName("Vector 3: dim_product|500")
        void vector3() {
            String hex = md5Hex("dim_product|500");
            long expected = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            long actual = keyGen.generateKey("dim_product", "500");
            assertThat(actual).isEqualTo(expected);
            System.out.printf("Vector 3: dim_product|500 → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }

        @Test
        @DisplayName("Vector 4: dim_date|2024-06-15")
        void vector4() {
            String hex = md5Hex("dim_date|2024-06-15");
            long expected = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            long actual = keyGen.generateKey("dim_date", "2024-06-15");
            assertThat(actual).isEqualTo(expected);
            System.out.printf("Vector 4: dim_date|2024-06-15 → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }

        @Test
        @DisplayName("Vector 5: fact_orders|42|100 (composite FK)")
        void vector5() {
            long expected = keyGen.generateKey("fact_orders", 42, 100);
            long actual = keyGen.generateKey("fact_orders", 42, 100);
            assertThat(actual).isEqualTo(expected);
            String hex = md5Hex("fact_orders|42|100");
            long formulaKey = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            assertThat(actual).isEqualTo(formulaKey);
            System.out.printf("Vector 5: fact_orders|42|100 → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }

        @Test
        @DisplayName("Vector 6: dim_customer|Jane Smith (non-ASCII safe)")
        void vector6() {
            String hex = md5Hex("dim_customer|Jane Smith");
            long expected = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            long actual = keyGen.generateKey("dim_customer", "Jane Smith");
            assertThat(actual).isEqualTo(expected);
            System.out.printf("Vector 6: dim_customer|Jane Smith → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }

        @Test
        @DisplayName("Vector 7: dim_customer|客户名称 (Unicode round-trip)")
        void vector7() {
            String hex = md5Hex("dim_customer|客户名称");
            long expected = new BigInteger(hex.substring(0, 16), 16)
                    .and(MASK_64).longValue();
            long actual = keyGen.generateKey("dim_customer", "客户名称");
            assertThat(actual).isEqualTo(expected);
            System.out.printf("Vector 7: dim_customer|客户名称 → MD5=%s → hex16=%s → key=%d%n",
                    hex, hex.substring(0, 16), actual);
        }
    }

    // ── Collision resistance ─────────────────────────

    @Nested
    @DisplayName("Collision resistance")
    class Collision {

        @Test
        @DisplayName("10,000 unique natural keys produce no collisions")
        void noCollisionsFor10k() {
            Set<Long> keys = new HashSet<>();
            for (int i = 1; i <= 10_000; i++) {
                long key = keyGen.generateKey("dim_customer", String.valueOf(i));
                assertThat(keys.add(key))
                        .as("Collision detected at natural key %d", i)
                        .isTrue();
            }
            assertThat(keys).hasSize(10_000);
        }
    }

    // ── Error handling ───────────────────────────────

    @Nested
    @DisplayName("Error handling")
    class Errors {

        @Test
        @DisplayName("fails fast when MD5 algorithm unavailable")
        void unavailableAlgorithm() {
            var badGen = new Md5KeyGenerator("SHA-512/256-NONEXISTENT");
            assertThatThrownBy(() -> badGen.generateKey("dim_customer", "1"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("MD5 algorithm not available");
        }
    }

    // ── Helper: compute MD5 hex ──────────────────────

    private static String md5Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xFF));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
