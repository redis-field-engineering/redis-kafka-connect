package com.redis.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.sink.RedisSinkConfig.RedisType;

class RedisSinkConfigTest {

	private static Map<String, String> props(RedisType type, String... keyValues) {
		Map<String, String> props = new HashMap<>();
		props.put(RedisSinkConfigDef.URI_CONFIG, "redis://localhost:6379");
		props.put(RedisSinkConfigDef.TYPE_CONFIG, type.name());
		for (int i = 0; i < keyValues.length; i += 2) {
			props.put(keyValues[i], keyValues[i + 1]);
		}
		return props;
	}

	private static Map<String, String> hashFieldProps(String hashKey, String hashField) {
		Map<String, String> props = props(RedisType.HASH_FIELD);
		if (hashKey != null) {
			props.put(RedisSinkConfigDef.HASH_KEY_CONFIG, hashKey);
		}
		if (hashField != null) {
			props.put(RedisSinkConfigDef.HASH_FIELD_CONFIG, hashField);
		}
		return props;
	}

	private static List<String> errors(Map<String, String> props, String name) {
		Map<String, ConfigValue> results = new RedisSinkConfigDef().validateAll(props);
		return results.get(name).errorMessages();
	}

	private static Map<String, List<String>> allErrors(Map<String, String> props) {
		return new RedisSinkConfigDef().validateAll(props).entrySet().stream()
				.filter(e -> !e.getValue().errorMessages().isEmpty())
				.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().errorMessages()));
	}

	@Test
	void hashFieldValid() {
		Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
		assertTrue(allErrors(props).isEmpty(), allErrors(props).toString());
		RedisSinkConfig config = new RedisSinkConfig(props);
		assertEquals("key.parentId", config.getHashKey().toString());
		assertEquals("key.childId", config.getHashField().toString());
	}

	@Test
	void hashFieldMissingKey() {
		Map<String, String> props = hashFieldProps(null, "key.childId");
		assertTrue(errors(props, RedisSinkConfigDef.HASH_KEY_CONFIG)
				.contains("redis.hash.key is required when redis.type=HASH_FIELD"));
		assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
	}

	@Test
	void hashFieldMissingField() {
		Map<String, String> props = hashFieldProps("key.parentId", null);
		assertTrue(errors(props, RedisSinkConfigDef.HASH_FIELD_CONFIG)
				.contains("redis.hash.field is required when redis.type=HASH_FIELD"));
		assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
	}

	@Test
	void hashKeyWithOtherType() {
		Map<String, String> props = props(RedisType.HASH, RedisSinkConfigDef.HASH_KEY_CONFIG, "key.a");
		assertTrue(errors(props, RedisSinkConfigDef.HASH_KEY_CONFIG)
				.contains("redis.hash.key is only supported when redis.type=HASH_FIELD"));
		assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
	}

	@Test
	void hashFieldWithOtherType() {
		Map<String, String> props = props(RedisType.STRING, RedisSinkConfigDef.HASH_FIELD_CONFIG, "key.a");
		assertTrue(errors(props, RedisSinkConfigDef.HASH_FIELD_CONFIG)
				.contains("redis.hash.field is only supported when redis.type=HASH_FIELD"));
	}

	@Test
	void hashFieldMalformed() {
		Map<String, String> props = hashFieldProps("key.parentId", "childId");
		List<String> errors = errors(props, RedisSinkConfigDef.HASH_FIELD_CONFIG);
		assertTrue(errors.stream().anyMatch(m -> m.contains("Invalid key reference")), errors.toString());
		assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
	}

	@Test
	void otherTypesUnchanged() {
		Map<String, String> props = props(RedisType.STRING);
		assertTrue(allErrors(props).isEmpty(), allErrors(props).toString());
		RedisSinkConfig config = new RedisSinkConfig(props);
		assertNull(config.getHashKey());
		assertNull(config.getHashField());
	}

}
