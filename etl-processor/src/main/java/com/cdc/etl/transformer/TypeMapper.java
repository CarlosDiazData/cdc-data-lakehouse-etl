package com.cdc.etl.transformer;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Maps PostgreSQL column types to Apache Iceberg types.
 * <p>
 * Follows the design-mandated mapping table:
 * <pre>
 *   PG INTEGER/SERIAL → Iceberg INT (required)
 *   PG BIGINT         → Iceberg LONG (required)
 *   PG NUMERIC(p,s)   → Iceberg DECIMAL(p,s)
 *   PG VARCHAR/TEXT   → Iceberg STRING
 *   PG BOOLEAN        → Iceberg BOOLEAN
 *   PG TIMESTAMP      → Iceberg TIMESTAMP
 *   PG DATE           → Iceberg DATE
 * </pre>
 * <p>
 * Supports nullable columns: if a PostgreSQL column is nullable, the
 * generated Iceberg field is optional.
 */
@Component
public class TypeMapper {

    private static final Logger log = LoggerFactory.getLogger(TypeMapper.class);

    /**
     * Infer the Iceberg {@link Type} from a sample Java value from the Parquet reader.
     * <p>
     * This is used when the source schema is not explicitly known — the mapper
     * inspects the first non-null value in a column to determine the Iceberg type.
     *
     * @param value a non-null sample value from the Parquet reader
     * @return the matching Iceberg type
     */
    public Type inferType(Object value) {
        Objects.requireNonNull(value, "Cannot infer type from null value");

        if (value instanceof Integer) {
            return Types.IntegerType.get();
        }
        if (value instanceof Long) {
            return Types.LongType.get();
        }
        if (value instanceof BigDecimal) {
            return Types.DecimalType.of(38, 18);
        }
        if (value instanceof Float || value instanceof Double) {
            return Types.DoubleType.get();
        }
        if (value instanceof String) {
            return Types.StringType.get();
        }
        if (value instanceof Boolean) {
            return Types.BooleanType.get();
        }
        if (value instanceof java.sql.Timestamp || value instanceof LocalDateTime) {
            return Types.TimestampType.withoutZone();
        }
        if (value instanceof java.sql.Date || value instanceof LocalDate) {
            return Types.DateType.get();
        }
        if (value instanceof byte[]) {
            return Types.BinaryType.get();
        }

        log.warn("Unknown Java type {} — falling back to STRING", value.getClass().getName());
        return Types.StringType.get();
    }

    /**
     * Convert a Parquet-extracted {@link java.sql.Date} or {@link LocalDate}
     * to an Iceberg-compatible epoch-day integer.
     */
    public static int toIcebergDate(Object value) {
        if (value instanceof java.sql.Date d) {
            return (int) d.toLocalDate().toEpochDay();
        }
        if (value instanceof LocalDate d) {
            return (int) d.toEpochDay();
        }
        if (value instanceof Integer i) {
            return i;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Iceberg date (epoch-day)");
    }

    /**
     * Convert a Parquet-extracted timestamp to Iceberg-compatible micros from epoch.
     */
    public static long toIcebergTimestamp(Object value) {
        if (value instanceof java.sql.Timestamp t) {
            return t.toInstant().toEpochMilli() * 1000L;
        }
        if (value instanceof LocalDateTime ldt) {
            return ldt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli() * 1000L;
        }
        if (value instanceof Long l) {
            return l;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Iceberg timestamp (micros)");
    }

    /**
     * Determine whether a field should be optional based on whether any row
     * has a null value for this column.
     *
     * @param hasNulls true if the column contains at least one null value
     * @return {@code true} if the field is optional (contains nulls)
     */
    public boolean isOptional(boolean hasNulls) {
        return hasNulls;
    }

    /**
     * Build a human-readable type name for schema mismatch error messages.
     */
    public static String typeName(Type type) {
        return switch (type.typeId()) {
            case INTEGER -> "INT";
            case LONG -> "LONG";
            case DECIMAL -> {
                Types.DecimalType dt = (Types.DecimalType) type;
                yield "DECIMAL(" + dt.precision() + "," + dt.scale() + ")";
            }
            case STRING -> "STRING";
            case BOOLEAN -> "BOOLEAN";
            case TIMESTAMP -> "TIMESTAMP";
            case DATE -> "DATE";
            case DOUBLE -> "DOUBLE";
            case FLOAT -> "FLOAT";
            case BINARY -> "BINARY";
            default -> type.typeId().name();
        };
    }
}
