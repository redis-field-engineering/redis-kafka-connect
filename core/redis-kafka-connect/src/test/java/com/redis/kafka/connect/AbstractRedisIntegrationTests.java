package com.redis.kafka.connect;

import com.redis.kafka.connect.common.ModuleCommand;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.RedisClient;
import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.lifecycle.Startable;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractRedisIntegrationTests {
    protected AbstractRedisClient redisClient;
    protected TestConnection redisConnection;

    protected static final class TestConnection implements AutoCloseable {
        private final RedisConnection<String, String> connection;
        TestConnection(AbstractRedisClient client) { connection = RedisConnection.open(client, StringCodec.UTF8); }
        @SuppressWarnings("unchecked")
        RedisClusterCommands<String, String> sync() { return connection.sync(RedisClusterCommands.class); }
        void setTimeout(Duration timeout) { connection.stateful().setTimeout(timeout); }
        public void close() { connection.close(); }
    }

    protected Map<String, String> deploymentProps(Map<String, String> props) {
        Map<String, String> result = new LinkedHashMap<>(props);
        result.put("redis.uri", getRedisServer().getRedisURI());
        result.put("redis.cluster", Boolean.toString(getRedisServer().isRedisCluster()));
        return result;
    }

    @SuppressWarnings("unchecked")
    protected void onPrimaries(Consumer<RedisClusterCommands<String, String>> action) {
        if (redisClient instanceof RedisClusterClient cluster) {
            RedisAdvancedClusterCommands<String, String> commands = (RedisAdvancedClusterCommands<String, String>) redisConnection.sync();
            cluster.getPartitions().stream().filter(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM))
                    .forEach(node -> action.accept(commands.getConnection(node.getNodeId())));
        } else {
            action.accept(redisConnection.sync());
        }
    }

    protected String clientList() {
        StringBuilder clients = new StringBuilder();
        onPrimaries(commands -> clients.append(commands.clientList()));
        return clients.toString();
    }

    protected abstract RedisServer getRedisServer();

    @BeforeAll
    void startRedis() {
        ((Startable) getRedisServer()).start();
        redisClient = getRedisServer().isRedisCluster() ? RedisClusterClient.create(getRedisServer().getRedisURI())
                : RedisClient.create(getRedisServer().getRedisURI());
        redisConnection = new TestConnection(redisClient);
        if (redisClient instanceof RedisClusterClient cluster) {
            long primaries = cluster.getPartitions().stream().filter(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM)).count();
            if (getRedisServer() instanceof RedisEnterprise8Container) {
                Assertions.assertTrue(primaries > 0, "Enterprise must advertise a usable OSS API topology");
            } else {
                Assertions.assertEquals(3, primaries, "three OSS primaries required");
            }
            IntStream.range(0, 16384).forEach(slot ->
                    Assertions.assertNotNull(cluster.getPartitions().getPartitionBySlot(slot), "slot " + slot));
            onPrimaries(commands -> Assertions.assertEquals("PONG", commands.ping()));
        } else if (!(getRedisServer() instanceof RedisEnterprise8Container)) {
            Assertions.assertTrue(redisConnection.sync().info("cluster").contains("cluster_enabled:0"));
        }
    }

    @BeforeEach
    void resetRedis() {
        onPrimaries(commands -> {
            commands.flushall();
            commands.configSet("notify-keyspace-events", "");
        });
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
        onPrimaries(commands -> commands.configSet("notify-keyspace-events", "KEA"));
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
        RedisClusterCommands<String, String> commands = redisConnection.sync();
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
