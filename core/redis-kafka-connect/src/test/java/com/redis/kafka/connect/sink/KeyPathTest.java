package com.redis.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
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

class KeyPathTest {

	private static final Charset UTF8 = StandardCharsets.UTF_8;

	private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("parentId", Schema.STRING_SCHEMA)
			.field("childId", Schema.STRING_SCHEMA).build();

	@Test
	void parseFlat() {
		assertEquals(Collections.singletonList("parentId"), KeyPath.parse("key.parentId").segments());
	}

	@Test
	void parseNested() {
		assertEquals(Arrays.asList("parent", "id"), KeyPath.parse("key.parent.id").segments());
	}

	@Test
	void parseKeepsReference() {
		assertEquals("key.parent.id", KeyPath.parse("key.parent.id").toString());
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " ", "parentId", "key.", "key.a..b", "value.x" })
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
		Struct key = new Struct(KEY_SCHEMA).put("parentId", "A").put("childId", "s1");
		assertArrayEquals(bytes("A"), KeyPath.parse("key.parentId").resolve(key, UTF8));
		assertArrayEquals(bytes("s1"), KeyPath.parse("key.childId").resolve(key, UTF8));
	}

	@Test
	void resolveNestedStruct() {
		Schema parentSchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
		Schema schema = SchemaBuilder.struct().field("parent", parentSchema).build();
		Struct key = new Struct(schema).put("parent", new Struct(parentSchema).put("id", "A"));
		assertArrayEquals(bytes("A"), KeyPath.parse("key.parent.id").resolve(key, UTF8));
	}

	@Test
	void resolveMap() {
		Map<String, Object> key = Collections.singletonMap("parentId", "A");
		assertArrayEquals(bytes("A"), KeyPath.parse("key.parentId").resolve(key, UTF8));
	}

	@Test
	void resolveNestedMap() {
		Map<String, Object> key = Collections.singletonMap("parent", Collections.singletonMap("id", "A"));
		assertArrayEquals(bytes("A"), KeyPath.parse("key.parent.id").resolve(key, UTF8));
	}

	@Test
	void resolveNumberLeaf() {
		Map<String, Object> key = Collections.singletonMap("parentId", 42L);
		assertArrayEquals(bytes("42"), KeyPath.parse("key.parentId").resolve(key, UTF8));
	}

	@Test
	void resolveBooleanLeaf() {
		Map<String, Object> key = Collections.singletonMap("parentId", true);
		assertArrayEquals(bytes("true"), KeyPath.parse("key.parentId").resolve(key, UTF8));
	}

	@Test
	void resolveBytesLeaf() {
		byte[] value = { 1, 2, 3 };
		Map<String, Object> key = Collections.singletonMap("parentId", value);
		assertArrayEquals(value, KeyPath.parse("key.parentId").resolve(key, UTF8));
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
		return Stream.of(Arguments.of((Object) null), Arguments.of("A"),
				Arguments.of(new Struct(otherSchema).put("childId", "s1")),
				Arguments.of(Collections.singletonMap("childId", "s1")),
				Arguments.of(new Struct(optionalSchema)), Arguments.of(nullLeaf),
				Arguments.of(new Struct(nestedSchema).put("parentId", new Struct(otherSchema).put("childId", "s1"))),
				Arguments.of(Collections.singletonMap("parentId", Collections.singletonMap("a", "b"))),
				Arguments.of(Collections.singletonMap("parentId", List.of("A"))),
				Arguments.of(Collections.singletonMap("parentId", new Date())),
				Arguments.of(Collections.singletonMap("parentId", new BigDecimal("1.5"))));
	}

	private static byte[] bytes(String value) {
		return value.getBytes(UTF8);
	}

}
