package com.redis.kafka.connect;

import com.redis.kafka.connect.source.RedisKeysSourceTask;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

class RedisSourceIntegrationTests extends AbstractSourceIntegrationTests {

	private static final RedisContainer container = RedisContainerFactory.redis();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

    @Test
    void keysSnapshotAndLiveReadsWorkWithScriptingDenied() throws Exception {
        redisConnection.sync().configSet("notify-keyspace-events", "KEA");
        assertEquals(0, container.execInContainer("redis-cli", "ACL", "SETUSER", "native-reader", "reset", "on",
                ">test-password", "~*", "&*", "+@all", "-@scripting").getExitCode());
        RedisClient restricted = RedisClient.create(RedisURI.builder(RedisURI.create(getRedisServer().getRedisURI()))
                .withAuthentication("native-reader", "test-password").build());
        RedisKeysSourceTask source = new RedisKeysSourceTask();
        try (StatefulRedisConnection<String, String> denied = restricted.connect()) {
            RedisCommandExecutionException error = assertThrows(RedisCommandExecutionException.class,
                    () -> denied.sync().eval("return 1", ScriptOutputType.INTEGER));
            assertTrue(error.getMessage().startsWith("NOPERM"));
            redisConnection.sync().set("native:string", "value");
            redisConnection.sync().hset("native:hash", Map.of("field", "value"));
            redisConnection.sync().rpush("native:list", "value");
            redisConnection.sync().sadd("native:set", "value");
            redisConnection.sync().zadd("native:zset", 1, "value");
            redisConnection.sync().jsonSet("native:json", JsonPath.ROOT_PATH, "{\"id\":1}");
            Set<String> expected = redisConnection.sync().keys("native*").stream().collect(Collectors.toSet());
            source.start(Map.of("redis.uri", getRedisServer().getRedisURI(), "redis.username", "native-reader",
                    "redis.password", "test-password", "redis.keys.pattern", "native*", "redis.keys.timeout", "100"));
            List<SourceRecord> records = new ArrayList<>();
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                records.addAll(source.poll());
                return records.stream().map(SourceRecord::key).collect(Collectors.toSet()).containsAll(expected);
            });
            assertFalse(expected.isEmpty());
            records.clear();
            redisConnection.sync().set("native:live", "updated");
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                records.addAll(source.poll());
                return records.stream().anyMatch(record -> "native:live".equals(record.key())
                        && "updated".equals(((Struct) record.value()).getString("string")));
            });
        } finally {
            try {
                source.stop();
            } finally {
                restricted.shutdown();
                redisConnection.sync().aclDeluser("native-reader");
            }
        }
    }

}
