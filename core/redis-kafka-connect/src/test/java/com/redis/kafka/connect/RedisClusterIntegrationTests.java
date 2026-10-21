package com.redis.kafka.connect;

import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import io.lettuce.core.cluster.SlotHash;
import org.junit.jupiter.api.TestInstance;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisClusterIntegrationTests {
    private final RedisOssClusterContainer container = new RedisOssClusterContainer();
    private RedisClusterClient client;
    private StatefulRedisClusterConnection<String, String> connection;

    @BeforeAll
    void startCluster() throws Exception {
        container.start();
        client = RedisClusterClient.create(uri());
        connection = client.connect();
        await().atMost(Duration.ofSeconds(20)).ignoreExceptions().untilAsserted(() -> {
            connection.sync().set("ready", "yes");
            assertEquals("yes", connection.sync().get("ready"));
        });
        List<String> primaries = client.getPartitions().stream()
                .filter(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM)).map(RedisClusterNode::getNodeId).toList();
        String primary = client.getPartitions().getPartitionBySlot(16383).getNodeId();
        assertEquals(0, container.execInContainer("redis-cli", "--cluster", "add-node", "127.0.0.1:17003",
                "127.0.0.1:17000", "--cluster-slave", "--cluster-master-id", primary).getExitCode());
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            client.reloadPartitions();
            return primaries.stream().allMatch(node -> connection.getConnection(node).sync().clusterNodes().lines()
                    .anyMatch(line -> line.contains(":17003@") && line.split(" ")[2].contains("slave")));
        });
    }

    private String uri() {
        return "redis://" + container.getHost() + ":17000";
    }

    @BeforeEach
    void clearCluster() {
        connection.sync().masters().commands().flushall();
        connection.sync().masters().commands().configSet("notify-keyspace-events", "KEA");
    }

    @AfterAll
    void stopCluster() {
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            try {
                if (client != null) {
                    client.shutdown();
                }
            } finally {
                container.stop();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sinkRoutesTransactionsAndTtlsAcrossSlots(boolean transaction) {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", uri(), "redis.cluster", "true", "redis.type", "LIST",
                    "redis.keyspace", "${topic}", "redis.multiexec", Boolean.toString(transaction), "redis.key.ttl", "3600"));
            List<SinkRecord> records = List.of(
                    new SinkRecord("list-a", 0, Schema.STRING_SCHEMA, "first", Schema.STRING_SCHEMA, "value", 0),
                    new SinkRecord("list-b", 0, Schema.STRING_SCHEMA, "other", Schema.STRING_SCHEMA, "value", 0),
                    new SinkRecord("list-a", 0, Schema.STRING_SCHEMA, "second", Schema.STRING_SCHEMA, "value", 1));
            sink.put(records);
            assertEquals(List.of("first", "second"), connection.sync().lrange("list-a", 0, -1));
            assertEquals(List.of("other"), connection.sync().lrange("list-b", 0, -1));
            assertTrue(connection.sync().ttl("list-a") > 3500);
            assertTrue(connection.sync().ttl("list-b") > 3500);
            sink.flush(Map.of(new TopicPartition("list-a", 0), new OffsetAndMetadata(2),
                    new TopicPartition("list-b", 0), new OffsetAndMetadata(1)));
            assertNotNull(connection.sync().get("com.redis.kafka.connect.sink.offset.default.list-a.0"));
            assertNotNull(connection.sync().get("com.redis.kafka.connect.sink.offset.default.list-b.0"));
        } finally {
            sink.stop();
        }
    }

    @Test
    void waitAcknowledgesWritesOnTheNonSeedPrimary() throws Exception {
        await().atMost(Duration.ofSeconds(20)).until(() -> container.execInContainer("redis-cli", "-p", "17003",
                "info", "replication").getStdout().contains("master_link_status:up"));
        String key = "{a}:replicated";
        assertNotEquals(17000, client.getPartitions().getPartitionBySlot(SlotHash.getSlot(key))
                .getUri().getPort());
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", uri(), "redis.cluster", "true", "redis.type", "STRING",
                    "redis.keyspace", "", "redis.wait.replicas", "1", "redis.wait.timeout", "2000"));
            sink.put(List.of(new SinkRecord("replicated", 0, Schema.STRING_SCHEMA, key,
                    Schema.STRING_SCHEMA, "replicated-value", 0)));
            assertEquals("replicated-value", connection.sync().get(key));
            try (RedisClient replica = RedisClient.create("redis://" + container.getHost() + ":17003");
                    StatefulRedisConnection<String, String> replicated = replica.connect()) {
                replicated.sync().readOnly();
                assertEquals("replicated-value", replicated.sync().get(key));
            }
        } finally {
            sink.stop();
        }
    }

    @Test
    void keysSnapshotAndLiveReadsWithReplicaOffline() throws Exception {
        assertEquals(0, container.execInContainer("redis-cli", "-p", "17003", "shutdown", "nosave").getExitCode());
        RedisKeysSourceTask source = new RedisKeysSourceTask();
        try {
            Set<String> keys = Set.of("{a}:snapshot", "{b}:snapshot", "{c}:snapshot");
            keys.forEach(key -> connection.sync().set(key, "snapshot"));
            source.start(Map.of("redis.uri", uri(), "redis.cluster", "true", "redis.timeout", "2",
                    "redis.keys.timeout", "100"));
            List<SourceRecord> records = new ArrayList<>();
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                records.addAll(source.poll());
                return records.stream().map(SourceRecord::key).collect(Collectors.toSet()).containsAll(keys);
            });
            assertTrue(records.stream().map(SourceRecord::value).map(Struct.class::cast)
                    .allMatch(value -> "snapshot".equals(value.getString("string"))));
            records.clear();
            Set<String> updates = Set.of("{a}:update", "{b}:update", "{c}:update");
            updates.forEach(key -> connection.sync().set(key, "live"));
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                records.addAll(source.poll());
                return records.stream().map(SourceRecord::key).collect(Collectors.toSet()).containsAll(updates);
            });
            assertTrue(records.stream().map(SourceRecord::value).map(Struct.class::cast)
                    .filter(value -> updates.contains(value.getString("key")))
                    .allMatch(value -> "live".equals(value.getString("string"))));
        } finally {
            try {
                source.stop();
            } finally {
                assertEquals(0, container.execInContainer("redis-server", "--loadmodule", "/usr/local/lib/redis/modules/rejson.so",
                        "--loadmodule", "/usr/local/lib/redis/modules/redistimeseries.so", "--port", "17003", "--cluster-enabled", "yes",
                        "--cluster-config-file", "/tmp/node3.conf", "--cluster-announce-ip", "127.0.0.1",
                        "--protected-mode", "no", "--daemonize", "yes").getExitCode());
            }
        }
    }

    @Test
    void keysSnapshotAndNotificationsCoverEveryPrimary() throws Exception {
        Set<String> expected = Set.of("{a}:snapshot", "{b}:snapshot", "{c}:snapshot");
        expected.forEach(key -> connection.sync().set(key, "snapshot"));
        RedisKeysSourceTask source = new RedisKeysSourceTask();
        try {
            source.start(Map.of("redis.uri", uri(), "redis.cluster", "true", "batch.size", "2"));
            List<SourceRecord> records = new ArrayList<>();
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                records.addAll(source.poll());
                return records.stream().map(SourceRecord::key).collect(Collectors.toSet()).containsAll(expected);
            });
            records.clear();
            Set<String> changed = Set.of("{a}:live", "{b}:live", "{c}:live");
            changed.forEach(key -> connection.sync().set(key, "live"));
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                records.addAll(source.poll());
                return records.stream().map(SourceRecord::key).collect(Collectors.toSet()).containsAll(changed);
            });
            assertTrue(records.stream().map(SourceRecord::value).map(Struct.class::cast)
                    .allMatch(value -> "live".equals(value.getString("string"))));
        } finally {
            source.stop();
        }
    }
}
