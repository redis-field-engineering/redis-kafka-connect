package com.redis.kafka.connect;

import com.redis.kafka.connect.source.KeySnapshot;
import com.redis.kafka.connect.source.RedisKeysSourceConfig;
import com.redis.kafka.connect.source.ToStructFunction;
import io.lettuce.core.ScoredValue;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KeysSourceConnectorTest {
    @Test
    void notificationsCreateOnlyOneTask() {
        RedisKeysSourceConnector connector = new RedisKeysSourceConnector();
        connector.start(Map.of("topic", "keys"));
        assertEquals(1, connector.taskConfigs(10).size());
        assertEquals("keys", connector.taskConfigs(10).get(0).get("topic"));
        assertTrue(connector.taskConfigs(0).isEmpty());
    }

    @Test
    void preservesPublishedKeySchema() {
        Schema schema = ToStructFunction.VALUE_SCHEMA;
        assertEquals("com.redis.kafka.connect.keys.Value", schema.name());
        assertEquals(List.of("key", "ttl", "type", "hash", "string", "json", "list", "set", "zset"),
                schema.fields().stream().map(field -> field.name()).toList());
        assertEquals(Schema.FLOAT64_SCHEMA, schema.field("zset").schema().keySchema());
        Struct value = new ToStructFunction().apply(new KeySnapshot("key", -1L, "string", "value"));
        assertEquals("value", value.getString("string"));
        assertNull(value.getMap("hash"));
    }

    @Test
    void duplicateSortedSetScoresFailWithoutDroppingMembers() {
        DataException error = assertThrows(DataException.class, () -> ToStructFunction.zsetMap(
                List.of(ScoredValue.just(1, "first"), ScoredValue.just(1, "second"))));
        assertTrue(error.getMessage().contains("same score"));
    }

    @Test
    void validatesReaderBoundsAndModes() {
        assertEquals(RedisKeysSourceConfig.ReaderMode.LIVEONLY,
                new RedisKeysSourceConfig(Map.of("mode", "liveonly")).getMode());
        for (Map<String, String> props : List.of(Map.of("mode", "invalid"), Map.of("batch.size", "0"),
                Map.of("redis.pool", "0"), Map.of("redis.keys.timeout", "-1"),
                Map.of("redis.keys.queue.capacity", "0"), Map.of("redis.timeout", "0"))) {
            assertThrows(ConfigException.class, () -> new RedisKeysSourceConfig(props));
        }
    }
}
