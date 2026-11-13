package com.redis.kafka.connect.sink;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class KeyPathTest {

    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("parentId", Schema.STRING_SCHEMA)
            .field("childId", Schema.STRING_SCHEMA).build();

    @Test
    void parseFlat() {
        assertEquals(List.of("parentId"), KeyPath.parse("key.parentId").segments());
    }

    @Test
    void parseNested() {
        assertEquals(List.of("parent", "id"), KeyPath.parse("key.parent.id").segments());
    }

    @Test
    void parseKeepsReference() {
        assertEquals("key.parent.id", KeyPath.parse("key.parent.id").toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "parentId", "key.", "key.a..b", "key. ", "value.x"})
    void parseInvalid(String reference) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> KeyPath.parse(reference));
        assertTrue(e.getMessage().contains("expected key.<field>[.<field>...]"), e.getMessage());
    }

    @Test
    void parseNull() {
        assertThrows(IllegalArgumentException.class, () -> KeyPath.parse(null));
    }

    @Test
    void resolveStruct() {
        Struct key = new Struct(KEY_SCHEMA).put("parentId", "p1").put("childId", "c1");
        assertArrayEquals(bytes("p1"), KeyPath.parse("key.parentId").resolve(key, UTF8));
        assertArrayEquals(bytes("c1"), KeyPath.parse("key.childId").resolve(key, UTF8));
    }

    @Test
    void resolveNestedStruct() {
        Schema parentSchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("parent", parentSchema).build();
        Struct key = new Struct(schema).put("parent", new Struct(parentSchema).put("id", "p1"));
        assertArrayEquals(bytes("p1"), KeyPath.parse("key.parent.id").resolve(key, UTF8));
    }

    @Test
    void resolveMap() {
        assertArrayEquals(bytes("p1"), KeyPath.parse("key.parentId").resolve(Map.of("parentId", "p1"), UTF8));
    }

    @Test
    void resolveNestedMap() {
        Map<String, Object> key = Map.of("parent", Map.of("id", "p1"));
        assertArrayEquals(bytes("p1"), KeyPath.parse("key.parent.id").resolve(key, UTF8));
    }

    @Test
    void resolveNumberLeaf() {
        assertArrayEquals(bytes("42"), KeyPath.parse("key.parentId").resolve(Map.of("parentId", 42L), UTF8));
    }

    @ParameterizedTest
    @MethodSource("numberLeaves")
    void resolveNumberLeafText(Number value, String expected) {
        assertArrayEquals(bytes(expected), KeyPath.parse("key.parentId").resolve(Map.of("parentId", value), UTF8));
    }

    // Decimals with the same value resolve to the same key whatever their scale.
    static Stream<Arguments> numberLeaves() {
        return Stream.of(
                Arguments.of(new BigDecimal("10.500"), "10.5"),
                Arguments.of(new BigDecimal("1.00"), "1"),
                Arguments.of(new BigDecimal("1E+3"), "1000"),
                Arguments.of(new BigInteger("123456789012345678901234567890"), "123456789012345678901234567890"),
                Arguments.of(10.0d, "10.0"));
    }

    @Test
    void resolveBooleanLeaf() {
        assertArrayEquals(bytes("true"), KeyPath.parse("key.parentId").resolve(Map.of("parentId", true), UTF8));
    }

    @Test
    void resolveBytesLeaf() {
        byte[] value = {1, 2, 3};
        assertArrayEquals(value, KeyPath.parse("key.parentId").resolve(Map.of("parentId", value), UTF8));
    }

    // Connect BYTES fields may hold a ByteBuffer instead of a byte array.
    @Test
    void resolveByteBufferLeaf() {
        ByteBuffer value = ByteBuffer.wrap(new byte[] {1, 2, 3});
        assertArrayEquals(new byte[] {1, 2, 3}, KeyPath.parse("key.parentId").resolve(Map.of("parentId", value), UTF8));
        assertEquals(0, value.position(), "resolve must not consume the record's buffer");
    }

    @Test
    void resolveStructByteBufferField() {
        Schema schema = SchemaBuilder.struct().field("parentId", Schema.BYTES_SCHEMA).build();
        // Only the remaining bytes belong to the value.
        ByteBuffer value = ByteBuffer.wrap(new byte[] {9, 1, 2, 3});
        value.position(1);
        Struct key = new Struct(schema).put("parentId", value);
        assertArrayEquals(new byte[] {1, 2, 3}, KeyPath.parse("key.parentId").resolve(key, UTF8));
        assertEquals(1, value.position());
    }

    @ParameterizedTest
    @MethodSource("unresolvableKeys")
    void resolveErrors(Object key) {
        DataException e = assertThrows(DataException.class, () -> KeyPath.parse("key.parentId").resolve(key, UTF8));
        assertTrue(e.getMessage().contains("key.parentId"), e.getMessage());
    }

    static Stream<Arguments> unresolvableKeys() {
        Schema optionalSchema = SchemaBuilder.struct().field("parentId", Schema.OPTIONAL_STRING_SCHEMA).build();
        Schema otherSchema = SchemaBuilder.struct().field("childId", Schema.STRING_SCHEMA).build();
        Schema nestedSchema = SchemaBuilder.struct().field("parentId", otherSchema).build();
        Map<String, Object> nullLeaf = new HashMap<>();
        nullLeaf.put("parentId", null);
        return Stream.of(
                Arguments.of((Object) null),
                Arguments.of("p1"),
                Arguments.of(new Struct(otherSchema).put("childId", "c1")),
                Arguments.of(Map.of("childId", "c1")),
                Arguments.of(new Struct(optionalSchema)),
                Arguments.of(nullLeaf),
                Arguments.of(new Struct(nestedSchema).put("parentId", new Struct(otherSchema).put("childId", "c1"))),
                Arguments.of(Map.of("parentId", Map.of("a", "b"))),
                Arguments.of(Map.of("parentId", List.of("p1"))),
                Arguments.of(Map.of("parentId", new Date())));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(UTF8);
    }
}
