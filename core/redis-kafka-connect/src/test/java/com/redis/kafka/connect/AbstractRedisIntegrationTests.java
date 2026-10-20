package com.redis.kafka.connect;

import com.redis.kafka.connect.common.ModuleCommand;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.lifecycle.Startable;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractRedisIntegrationTests {
    protected RedisClient redisClient;
    protected StatefulRedisConnection<String, String> redisConnection;

    protected abstract RedisServer getRedisServer();

    @BeforeAll
    void startRedis() {
        ((Startable) getRedisServer()).start();
        redisClient = RedisClient.create(getRedisServer().getRedisURI());
        redisConnection = redisClient.connect();
    }

    @BeforeEach
    void resetRedis() {
        redisConnection.sync().flushall();
        redisConnection.sync().configSet("notify-keyspace-events", "");
    }

    @AfterAll
    void stopRedis() {
        try {
            if (redisConnection != null) {
                redisConnection.close();
            }
        } finally {
            try {
                if (redisClient != null) {
                    redisClient.shutdown();
                }
            } finally {
                ((Startable) getRedisServer()).stop();
            }
        }
    }

    protected void enableKeyspaceNotifications() {
        redisConnection.sync().configSet("notify-keyspace-events", "KEA");
    }

    protected void awaitUntil(Callable<Boolean> condition) {
        Awaitility.await().atMost(Duration.ofSeconds(20)).until(condition);
    }

    protected List<Object> timeSeries(String key) {
        return redisConnection.sync().dispatch(ModuleCommand.TS_RANGE, new NestedMultiOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey(key).add("-").add("+"));
    }

    /** Deterministic fixtures span each published keys-source value field without a batch-job dependency. */
    protected void generateKeys(String prefix, int count) {
        RedisCommands<String, String> commands = redisConnection.sync();
        for (int index = 0; index < count; index++) {
            String key = prefix + ":" + index;
            switch (index % 6) {
                case 0 -> commands.set(key, "value" + index);
                case 1 -> commands.hset(key, "field", "value" + index);
                case 2 -> commands.rpush(key, "member" + index);
                case 3 -> commands.sadd(key, "member" + index);
                case 4 -> commands.zadd(key, index, "member" + index);
                case 5 -> commands.jsonSet(key, JsonPath.ROOT_PATH, "{\"id\":" + index + "}");
                default -> throw new AssertionError();
            }
        }
    }
}
