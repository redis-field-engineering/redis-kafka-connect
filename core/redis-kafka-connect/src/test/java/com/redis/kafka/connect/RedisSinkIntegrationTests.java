package com.redis.kafka.connect;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.kafka.connect.sink.RedisSinkTask;
import io.lettuce.core.cluster.SlotHash;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class RedisSinkIntegrationTests extends AbstractSinkIntegrationTests {

	private static final RedisContainer container = RedisContainerFactory.redis();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void standaloneKeepsOneTransactionUnlessSlotSplittingIsExplicit(boolean split) {
        assertNotEquals(SlotHash.getSlot("list-a"), SlotHash.getSlot("list-b"));
        long before = transactionCount();
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", container.getRedisURI(), "redis.type", "LIST",
                    "redis.multiexec", "true", "redis.multiexec.keyslot", Boolean.toString(split)));
            sink.put(List.of(
                    new SinkRecord("list-a", 0, Schema.STRING_SCHEMA, "first", Schema.STRING_SCHEMA, "value", 0),
                    new SinkRecord("list-b", 0, Schema.STRING_SCHEMA, "second", Schema.STRING_SCHEMA, "value", 0)));
            // One EXEC is Redis's atomic execution boundary; final values alone cannot prove isolation.
            assertEquals(split ? 2 : 1, transactionCount() - before);
            assertEquals(List.of("first"), redisConnection.sync().lrange("list-a", 0, -1));
            assertEquals(List.of("second"), redisConnection.sync().lrange("list-b", 0, -1));
        } finally {
            sink.stop();
        }
    }

    private long transactionCount() {
        return redisConnection.sync().info("commandstats").lines()
                .filter(line -> line.startsWith("cmdstat_exec:"))
                .map(line -> line.substring(line.indexOf("calls=") + 6).split(",")[0])
                .mapToLong(Long::parseLong).findFirst().orElse(0);
    }
}
