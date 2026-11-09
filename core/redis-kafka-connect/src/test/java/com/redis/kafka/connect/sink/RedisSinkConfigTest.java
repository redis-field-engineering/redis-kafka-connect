package com.redis.kafka.connect.sink;

import com.redis.kafka.connect.sink.RedisSinkConfig.RedisType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class RedisSinkConfigTest {

    @Test
    void hashFieldParsesBothReferences() {
        Map<String, String> props = hashFieldProps("key.parentId", "key.parent.childId");
        assertEquals(Map.of(), errors(props));
        RedisSinkConfig config = new RedisSinkConfig(props);
        assertEquals(RedisType.HASH_FIELD, config.getType());
        assertEquals(List.of("parentId"), config.getHashPath().segments());
        assertEquals(List.of("parent", "childId"), config.getFieldPath().segments());
    }

    @Test
    void hashFieldReferencesAreTrimmed() {
        RedisSinkConfig config = new RedisSinkConfig(hashFieldProps(" key.parentId ", " key.childId "));
        assertEquals("key.parentId", config.getHashPath().toString());
        assertEquals("key.childId", config.getFieldPath().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG, RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG})
    void hashFieldRequiresBothReferences(String missing) {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.remove(missing);
        assertEquals(Map.of(missing, List.of(missing + " is required when redis.type=HASH_FIELD")), errors(props));
        String message = configException(props).getMessage();
        assertTrue(message.contains("for configuration " + missing), message);
    }

    @Test
    void hashFieldRejectsBlankReference() {
        Map<String, String> props = hashFieldProps("  ", "key.childId");
        assertEquals(List.of("redis.hashfield.hash is required when redis.type=HASH_FIELD"),
                errors(props).get(RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG));
        configException(props);
    }

    @ParameterizedTest
    @ValueSource(strings = {"childId", "key.", "key.parent..childId", "value.childId"})
    void hashFieldRejectsMalformedReference(String reference) {
        Map<String, String> props = hashFieldProps("key.parentId", reference);
        List<String> messages = errors(props).get(RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG);
        assertEquals(1, messages.size(), String.valueOf(messages));
        assertTrue(messages.get(0).contains("expected key.<field>[.<field>...]"), messages.get(0));
        assertTrue(configException(props).getMessage().contains(RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG));
    }

    @ParameterizedTest
    @ValueSource(strings = {RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG, RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG})
    void hashReferencesAreRejectedForOtherTypes(String name) {
        Map<String, String> props = props(RedisType.HASH);
        props.put(name, "key.parentId");
        assertEquals(Map.of(name, List.of(name + " is only supported when redis.type=HASH_FIELD")), errors(props));
        configException(props);
    }

    @Test
    void otherTypesAreUnchanged() {
        Map<String, String> props = props(RedisType.STRING);
        assertEquals(Map.of(), errors(props));
        RedisSinkConfig config = new RedisSinkConfig(props);
        assertNull(config.getHashPath());
        assertNull(config.getFieldPath());
    }

    @Test
    void hashReferencesTakePartInEquality() {
        RedisSinkConfig config = new RedisSinkConfig(hashFieldProps("key.parentId", "key.childId"));
        RedisSinkConfig same = new RedisSinkConfig(hashFieldProps("key.parentId", "key.childId"));
        assertEquals(config, same);
        assertEquals(config.hashCode(), same.hashCode());
        assertNotEquals(config, new RedisSinkConfig(hashFieldProps("key.parentId", "key.otherId")));
    }

    @Test
    void hashFieldExpireIsOffByDefault() {
        RedisSinkConfig config = new RedisSinkConfig(hashFieldProps("key.parentId", "key.childId"));
        assertFalse(config.isHashFieldExpireEnabled());
        assertEquals(-1, config.getHashFieldTTL());
    }

    @Test
    void hashFieldExpireAcceptsTtlWhenEnabled() {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG, "true");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG, "60");
        props.put(RedisSinkConfigDef.KEY_TTL_CONFIG, "3600");
        assertEquals(Map.of(), errors(props));
        RedisSinkConfig config = new RedisSinkConfig(props);
        assertTrue(config.isHashFieldExpireEnabled());
        assertEquals(60, config.getHashFieldTTL());
        assertEquals(3600, config.getKeyTTL());
    }

    @Test
    void hashFieldExpireEnabledWithoutTtlUsesHeadersOnly() {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG, "true");
        assertEquals(Map.of(), errors(props));
        RedisSinkConfig config = new RedisSinkConfig(props);
        assertTrue(config.isHashFieldExpireEnabled());
        assertEquals(-1, config.getHashFieldTTL());
    }

    @Test
    void hashFieldTtlRequiresExpireEnabled() {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG, "60");
        String name = RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG;
        assertEquals(Map.of(name, List.of(name + " requires redis.hashfield.hexpire.enabled=true")), errors(props));
        assertTrue(configException(props).getMessage().contains(name));
    }

    @ParameterizedTest
    @ValueSource(longs = {-2, RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_MAX + 1})
    void hashFieldTtlOutsideItsRangeIsRejected(long ttl) {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG, "true");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG, Long.toString(ttl));
        assertEquals(1, errors(props).get(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG).size());
        configException(props);
    }

    @Test
    void hashFieldTtlAcceptsItsMaximum() {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG, "true");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG, Long.toString(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_MAX));
        assertEquals(Map.of(), errors(props));
    }

    @ParameterizedTest
    @ValueSource(strings = {RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG,
            RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG})
    void hashFieldExpireIsRejectedForOtherTypes(String name) {
        Map<String, String> props = props(RedisType.HASH);
        props.put(name, name.equals(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG) ? "true" : "60");
        assertEquals(Map.of(name, List.of(name + " is only supported when redis.type=HASH_FIELD")), errors(props));
        assertTrue(configException(props).getMessage().contains(name));
    }

    @Test
    void hashFieldExpireTakesPartInEquality() {
        Map<String, String> props = hashFieldProps("key.parentId", "key.childId");
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG, "true");
        RedisSinkConfig enabled = new RedisSinkConfig(props);
        assertEquals(enabled, new RedisSinkConfig(props));
        assertNotEquals(enabled, new RedisSinkConfig(hashFieldProps("key.parentId", "key.childId")));
        props.put(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG, "60");
        assertNotEquals(enabled, new RedisSinkConfig(props));
    }

    private static Map<String, String> props(RedisType type) {
        Map<String, String> props = new HashMap<>();
        props.put(RedisSinkConfigDef.TYPE_CONFIG, type.name());
        return props;
    }

    private static Map<String, String> hashFieldProps(String hashKey, String hashField) {
        Map<String, String> props = props(RedisType.HASH_FIELD);
        props.put(RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG, hashKey);
        props.put(RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG, hashField);
        return props;
    }

    // Errors reported by the connector's validate() path, by property name.
    private static Map<String, List<String>> errors(Map<String, String> props) {
        return new RedisSinkConfigDef().validateAll(props).entrySet().stream()
                .filter(entry -> !entry.getValue().errorMessages().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().errorMessages()));
    }

    private static ConfigException configException(Map<String, String> props) {
        return assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
    }
}
