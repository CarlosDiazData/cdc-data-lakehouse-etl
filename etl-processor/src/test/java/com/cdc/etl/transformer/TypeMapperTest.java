package com.cdc.etl.transformer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TypeMapper}.
 *
 * <p>Validates all PostgreSQL → Iceberg type mappings, nullable
 * inference, date/timestamp conversion, and edge cases.
 */
@DisplayName("TypeMapper")
class TypeMapperTest {

    private TypeMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new TypeMapper();
    }

    // ── Type inference from Java values ──────────────

    @Nested
    @DisplayName("Type inference (inferType)")
    class InferType {

        @Test
        @DisplayName("Integer → Iceberg INT")
        void integerToInt() {
            assertThat(mapper.inferType(42))
                    .isEqualTo(Types.IntegerType.get());
        }

        @Test
        @DisplayName("Long → Iceberg LONG")
        void longToLong() {
            assertThat(mapper.inferType(9999999999L))
                    .isEqualTo(Types.LongType.get());
        }

        @Test
        @DisplayName("BigDecimal → Iceberg DECIMAL(38,18)")
        void bigDecimalToDecimal() {
            assertThat(mapper.inferType(new BigDecimal("99.99")))
                    .isInstanceOf(Types.DecimalType.class);
            Types.DecimalType dt = (Types.DecimalType) mapper.inferType(new BigDecimal("99.99"));
            assertThat(dt.precision()).isEqualTo(38);
            assertThat(dt.scale()).isEqualTo(18);
        }

        @Test
        @DisplayName("Float → Iceberg DOUBLE")
        void floatToDouble() {
            assertThat(mapper.inferType(3.14f))
                    .isEqualTo(Types.DoubleType.get());
        }

        @Test
        @DisplayName("Double → Iceberg DOUBLE")
        void doubleToDouble() {
            assertThat(mapper.inferType(3.141592653589793))
                    .isEqualTo(Types.DoubleType.get());
        }

        @Test
        @DisplayName("String → Iceberg STRING")
        void stringToString() {
            assertThat(mapper.inferType("hello"))
                    .isEqualTo(Types.StringType.get());
        }

        @Test
        @DisplayName("Boolean → Iceberg BOOLEAN")
        void booleanToBoolean() {
            assertThat(mapper.inferType(true))
                    .isEqualTo(Types.BooleanType.get());
            assertThat(mapper.inferType(false))
                    .isEqualTo(Types.BooleanType.get());
        }

        @Test
        @DisplayName("java.sql.Timestamp → Iceberg TIMESTAMP")
        void sqlTimestampToTimestamp() {
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            assertThat(mapper.inferType(ts))
                    .isEqualTo(Types.TimestampType.withoutZone());
        }

        @Test
        @DisplayName("LocalDateTime → Iceberg TIMESTAMP")
        void localDateTimeToTimestamp() {
            assertThat(mapper.inferType(LocalDateTime.now()))
                    .isEqualTo(Types.TimestampType.withoutZone());
        }

        @Test
        @DisplayName("java.sql.Date → Iceberg DATE")
        void sqlDateToDate() {
            assertThat(mapper.inferType(java.sql.Date.valueOf("2024-06-15")))
                    .isEqualTo(Types.DateType.get());
        }

        @Test
        @DisplayName("LocalDate → Iceberg DATE")
        void localDateToDate() {
            assertThat(mapper.inferType(LocalDate.of(2024, 6, 15)))
                    .isEqualTo(Types.DateType.get());
        }

        @Test
        @DisplayName("byte[] → Iceberg BINARY")
        void byteArrayToBinary() {
            assertThat(mapper.inferType(new byte[]{0x01, 0x02, 0x03}))
                    .isEqualTo(Types.BinaryType.get());
        }

        @Test
        @DisplayName("unknown type falls back to STRING")
        void unknownFallsBackToString() {
            // An arbitrary Object that doesn't match any known type
            assertThat(mapper.inferType(new Object()))
                    .isEqualTo(Types.StringType.get());
        }

        @Test
        @DisplayName("null value throws exception")
        void nullValueThrows() {
            assertThatThrownBy(() -> mapper.inferType(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Cannot infer type from null value");
        }
    }

    // ── All PG → Iceberg mapping table ───────────────

    @Nested
    @DisplayName("Complete PostgreSQL → Iceberg mapping table")
    class CompleteMapping {

        @Test
        @DisplayName("INTEGER/SERIAL → INT")
        void integer()   { assertThat(mapper.inferType(1)).isEqualTo(Types.IntegerType.get()); }

        @Test
        @DisplayName("BIGINT → LONG")
        void bigint()    { assertThat(mapper.inferType(1L)).isEqualTo(Types.LongType.get()); }

        @Test
        @DisplayName("NUMERIC(10,2) → DECIMAL(10,2)")
        void numeric()   {
            Type t = mapper.inferType(new BigDecimal("123.45"));
            assertThat(t).isInstanceOf(Types.DecimalType.class);
        }

        @Test
        @DisplayName("VARCHAR/TEXT → STRING")
        void varchar()   { assertThat(mapper.inferType("text")).isEqualTo(Types.StringType.get()); }

        @Test
        @DisplayName("BOOLEAN → BOOLEAN")
        void bool()      { assertThat(mapper.inferType(false)).isEqualTo(Types.BooleanType.get()); }

        @Test
        @DisplayName("TIMESTAMP → TIMESTAMP (without zone)")
        void timestamp() { assertThat(mapper.inferType(new java.sql.Timestamp(0))).isEqualTo(Types.TimestampType.withoutZone()); }

        @Test
        @DisplayName("DATE → DATE")
        void date()      { assertThat(mapper.inferType(LocalDate.now())).isEqualTo(Types.DateType.get()); }

        @Test
        @DisplayName("NUMERIC(10,2) spec mapping → DECIMAL")
        void numericSpec() {
            // The spec table: NUMERIC(10,2) → DECIMAL(10,2)
            BigDecimal value = new BigDecimal("10.50");
            Type type = mapper.inferType(value);
            assertThat(type).isInstanceOf(Types.DecimalType.class);
        }
    }

    // ── Nullable column inference ────────────────────

    @Nested
    @DisplayName("Nullable column inference")
    class Nullable {

        @Test
        @DisplayName("column with nulls → optional")
        void hasNullsIsOptional() {
            assertThat(mapper.isOptional(true)).isTrue();
        }

        @Test
        @DisplayName("column without nulls → required")
        void noNullsIsRequired() {
            assertThat(mapper.isOptional(false)).isFalse();
        }
    }

    // ── Type name for error messages ─────────────────

    @Nested
    @DisplayName("Type name formatting (typeName)")
    class TypeName {

        @Test
        @DisplayName("INT")
        void intName()   { assertThat(TypeMapper.typeName(Types.IntegerType.get())).isEqualTo("INT"); }

        @Test
        @DisplayName("LONG")
        void longName()  { assertThat(TypeMapper.typeName(Types.LongType.get())).isEqualTo("LONG"); }

        @Test
        @DisplayName("DECIMAL(p,s)")
        void decimalName() {
            assertThat(TypeMapper.typeName(Types.DecimalType.of(10, 2)))
                    .isEqualTo("DECIMAL(10,2)");
        }

        @Test
        @DisplayName("STRING")
        void stringName() { assertThat(TypeMapper.typeName(Types.StringType.get())).isEqualTo("STRING"); }

        @Test
        @DisplayName("BOOLEAN")
        void boolName()   { assertThat(TypeMapper.typeName(Types.BooleanType.get())).isEqualTo("BOOLEAN"); }

        @Test
        @DisplayName("TIMESTAMP")
        void timestampName() { assertThat(TypeMapper.typeName(Types.TimestampType.withoutZone())).isEqualTo("TIMESTAMP"); }

        @Test
        @DisplayName("DATE")
        void dateName()   { assertThat(TypeMapper.typeName(Types.DateType.get())).isEqualTo("DATE"); }
    }

    // ── Date conversion ──────────────────────────────

    @Nested
    @DisplayName("Date conversion (toIcebergDate)")
    class DateConversion {

        @Test
        @DisplayName("java.sql.Date → epoch-day")
        void sqlDateToEpochDay() {
            java.sql.Date sqlDate = java.sql.Date.valueOf("2024-06-15");
            int epochDay = TypeMapper.toIcebergDate(sqlDate);
            assertThat(epochDay).isEqualTo((int) sqlDate.toLocalDate().toEpochDay());
        }

        @Test
        @DisplayName("LocalDate → epoch-day")
        void localDateToEpochDay() {
            LocalDate date = LocalDate.of(2024, 1, 1);
            int epochDay = TypeMapper.toIcebergDate(date);
            assertThat(epochDay).isEqualTo((int) date.toEpochDay());
        }

        @Test
        @DisplayName("Integer (already epoch-day) passes through")
        void integerPassThrough() {
            assertThat(TypeMapper.toIcebergDate(19750)).isEqualTo(19750);
        }

        @Test
        @DisplayName("unknown type throws")
        void unknownThrows() {
            assertThatThrownBy(() -> TypeMapper.toIcebergDate("not-a-date"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ── Timestamp conversion ─────────────────────────

    @Nested
    @DisplayName("Timestamp conversion (toIcebergTimestamp)")
    class TimestampConversion {

        @Test
        @DisplayName("java.sql.Timestamp → micros from epoch")
        void sqlTimestampToMicros() {
            Timestamp ts = new Timestamp(1000L); // 1 second after epoch
            long micros = TypeMapper.toIcebergTimestamp(ts);
            assertThat(micros).isEqualTo(1_000_000L);
        }

        @Test
        @DisplayName("LocalDateTime → micros from epoch")
        void localDateTimeToMicros() {
            LocalDateTime ldt = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
            long micros = TypeMapper.toIcebergTimestamp(ldt);
            long expectedMillis = ldt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            assertThat(micros).isEqualTo(expectedMillis * 1000L);
        }

        @Test
        @DisplayName("Long passes through")
        void longPassThrough() {
            assertThat(TypeMapper.toIcebergTimestamp(1234567890L))
                    .isEqualTo(1234567890L);
        }

        @Test
        @DisplayName("unknown type throws")
        void unknownThrows() {
            assertThatThrownBy(() -> TypeMapper.toIcebergTimestamp("not-a-timestamp"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
