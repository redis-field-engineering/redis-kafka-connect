package com.redis.kafka.connect;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.kafka.connect.sink.RedisSinkTask;
import io.lettuce.core.cluster.SlotHash;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class RedisSinkIntegrationTests extends AbstractSinkIntegrationTests {

	private static final RedisContainer container = RedisContainerFactory.redis();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

    @Test
    void standaloneKeepsOneTransactionAcrossSlots() {
        assertNotEquals(SlotHash.getSlot("list-a"), SlotHash.getSlot("list-b"));
        long before = transactionCount();
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", container.getRedisURI(), "redis.type", "LIST",
                    "redis.multiexec", "true"));
            sink.put(List.of(
                    new SinkRecord("list-a", 0, Schema.STRING_SCHEMA, "first", Schema.STRING_SCHEMA, "value", 0),
                    new SinkRecord("list-b", 0, Schema.STRING_SCHEMA, "second", Schema.STRING_SCHEMA, "value", 0)));
            // One EXEC is Redis's atomic execution boundary; final values alone cannot prove isolation.
            assertEquals(1, transactionCount() - before);
            assertEquals(List.of("first"), redisConnection.sync().lrange("list-a", 0, -1));
            assertEquals(List.of("second"), redisConnection.sync().lrange("list-b", 0, -1));
        } finally {
            sink.stop();
        }
    }

    @Test
    void replicationTargetMustBeAcknowledged() {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", container.getRedisURI(), "redis.type", "STRING", "redis.keyspace", "",
                    "redis.wait.replicas", "1", "redis.wait.timeout", "10"));
            SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "value", 0);
            assertThrows(RetriableException.class, () -> sink.put(List.of(record)));
            assertEquals("value", redisConnection.sync().get("key"));
            assertNull(redisConnection.sync().get("com.redis.kafka.connect.sink.offset.default.topic.0"));
        } finally {
            sink.stop();
        }
    }

    @Test
    void transactionCommandErrorsArePropagated() {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", container.getRedisURI(), "redis.type", "LIST", "redis.keyspace", "wrongtype",
                    "redis.multiexec", "true"));
            redisConnection.sync().set("wrongtype", "string");
            SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "member", Schema.STRING_SCHEMA, "value", 0);
            assertThrows(ConnectException.class, () -> sink.put(List.of(record)));
            assertEquals("string", redisConnection.sync().get("wrongtype"));
        } finally {
            sink.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partialCommandFailureDiscardsConnectionAndWholeBatchCanBeReplayed(boolean transaction) {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", container.getRedisURI(), "redis.type", "LIST",
                    "redis.keyspace", "${topic}", "redis.pool", "1", "redis.multiexec", Boolean.toString(transaction)));
            sink.put(List.of(new SinkRecord("partial-a", 0, Schema.STRING_SCHEMA, "warmup", null, "value", 0)));
            Set<String> before = clientIds();
            redisConnection.sync().set("partial-b", "wrongtype");
            List<SinkRecord> batch = List.of(
                    new SinkRecord("partial-a", 0, Schema.STRING_SCHEMA, "first", null, "value", 1),
                    new SinkRecord("partial-b", 0, Schema.STRING_SCHEMA, "second", null, "value", 2));
            ConnectException failure = assertThrows(ConnectException.class, () -> sink.put(batch));
            assertFalse(failure instanceof RetriableException, "WRONGTYPE is a permanent command error");
            assertEquals(List.of("warmup", "first"), redisConnection.sync().lrange("partial-a", 0, -1));
            assertEquals("wrongtype", redisConnection.sync().get("partial-b"));
            awaitUntil(() -> {
                Set<String> removed = new HashSet<>(before);
                removed.removeAll(clientIds());
                return removed.size() == 1;
            });
            redisConnection.sync().del("partial-b");
            sink.put(batch);
            // A whole-batch replay can duplicate non-idempotent writes that already succeeded.
            assertEquals(List.of("warmup", "first", "first"), redisConnection.sync().lrange("partial-a", 0, -1));
            assertEquals(List.of("second"), redisConnection.sync().lrange("partial-b", 0, -1));
            Set<String> added = new HashSet<>(clientIds());
            added.removeAll(before);
            assertEquals(1, added.size(), "replay must use a replacement pooled connection");
        } finally {
            sink.stop();
        }
    }

    private Set<String> clientIds() {
        return redisConnection.sync().clientList().lines().flatMap(line -> Arrays.stream(line.split(" ")))
                .filter(field -> field.startsWith("id=")).collect(Collectors.toSet());
    }

    private long transactionCount() {
        return redisConnection.sync().info("commandstats").lines()
                .filter(line -> line.startsWith("cmdstat_exec:"))
                .map(line -> line.substring(line.indexOf("calls=") + 6).split(",")[0])
                .mapToLong(Long::parseLong).findFirst().orElse(0);
    }
}
