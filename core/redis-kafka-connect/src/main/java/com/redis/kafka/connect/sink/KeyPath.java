package com.redis.kafka.connect.sink;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

/**
 * A reference to a field inside a Kafka record key, written as {@code key.<field>[.<field>...]}.
 * Resolves against a {@link Struct} key (Avro, JSON Schema, Protobuf) or a schemaless {@link Map} key.
 */
final class KeyPath {

    static final String PREFIX = "key.";

    private final String reference;
    private final List<String> segments;

    private KeyPath(String reference, List<String> segments) {
        this.reference = reference;
        this.segments = segments;
    }

    /**
     * Parses a key reference.
     *
     * @throws IllegalArgumentException if the reference is not {@code key.<field>[.<field>...]}
     */
    static KeyPath parse(String reference) {
        if (reference == null || !reference.startsWith(PREFIX)) {
            throw invalid(reference);
        }
        List<String> segments = List.of(reference.substring(PREFIX.length()).split("\\.", -1));
        if (segments.stream().anyMatch(segment -> segment.isBlank())) {
            throw invalid(reference);
        }
        return new KeyPath(reference, segments);
    }

    private static IllegalArgumentException invalid(String reference) {
        return new IllegalArgumentException(
                String.format("Invalid key reference '%s': expected key.<field>[.<field>...]", reference));
    }

    List<String> segments() {
        return segments;
    }

    /**
     * Resolves this reference against a record key.
     *
     * @return the referenced value's bytes: binary values (byte arrays or byte buffers) as is,
     *         strings, booleans and numbers as text in {@code charset}, with decimals in plain
     *         notation without trailing zeros
     * @throws DataException if the key is null, a field is missing, or the value is null or of an
     *         unsupported type
     */
    byte[] resolve(Object recordKey, Charset charset) {
        if (recordKey == null) {
            throw error("record key is null");
        }
        Object value = recordKey;
        for (String segment : segments) {
            value = child(value, segment);
        }
        if (value == null) {
            throw error("value is null");
        }
        byte[] binary = BinaryValue.toBytes(value);
        if (binary != null) {
            return binary;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.stripTrailingZeros().toPlainString().getBytes(charset);
        }
        if (value instanceof String || value instanceof Boolean || value instanceof Number) {
            return value.toString().getBytes(charset);
        }
        throw error("unsupported value type " + value.getClass().getName());
    }

    private Object child(Object parent, String segment) {
        if (parent instanceof Struct struct) {
            if (struct.schema().field(segment) == null) {
                throw error("field '" + segment + "' not found");
            }
            return struct.get(segment);
        }
        if (parent instanceof Map<?, ?> map) {
            if (!map.containsKey(segment)) {
                throw error("field '" + segment + "' not found");
            }
            return map.get(segment);
        }
        throw error("expected a Struct or Map but found " + (parent == null ? "null" : parent.getClass().getName()));
    }

    private DataException error(String reason) {
        return new DataException(String.format("Could not resolve '%s' from record key: %s", reference, reason));
    }

    @Override
    public String toString() {
        return reference;
    }
}
