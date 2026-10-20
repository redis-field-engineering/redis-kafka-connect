package com.redis.kafka.connect;

import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
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
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisClusterIntegrationTests {
    // Equal internal/external ports keep advertised cluster addresses valid on local Docker hosts.
    private static final class ClusterContainer extends GenericContainer<ClusterContainer> {
        ClusterContainer() {
            super(DockerImageName.parse(RedisContainerFactory.REDIS_IMAGE));
            for (int port = 17000; port <= 17002; port++) {
                addFixedExposedPort(port, port);
            }
            withCommand("sh", "-ec", """
                    redis-server --port 17000 --cluster-enabled yes --cluster-config-file /tmp/node0.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
                    redis-server --port 17001 --cluster-enabled yes --cluster-config-file /tmp/node1.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
                    exec redis-server --port 17002 --cluster-enabled yes --cluster-config-file /tmp/node2.conf --cluster-announce-ip 127.0.0.1 --protected-mode no
                    """);
            waitingFor(Wait.forListeningPorts(17000, 17001, 17002));
            withStartupTimeout(Duration.ofMinutes(1));
        }
    }

    private final ClusterContainer container = new ClusterContainer();
    private RedisClusterClient client;
    private StatefulRedisClusterConnection<String, String> connection;

    @BeforeAll
    void startCluster() throws Exception {
        container.start();
        assertEquals(0, container.execInContainer("redis-cli", "--cluster", "create", "127.0.0.1:17000",
                "127.0.0.1:17001", "127.0.0.1:17002", "--cluster-yes").getExitCode());
        client = RedisClusterClient.create(uri());
        connection = client.connect();
        await().atMost(Duration.ofSeconds(20)).ignoreExceptions().untilAsserted(() -> {
            connection.sync().set("ready", "yes");
            assertEquals("yes", connection.sync().get("ready"));
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

    @Test
    void sinkRoutesTransactionsAndTtlsAcrossSlots() {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(Map.of("redis.uri", uri(), "redis.cluster", "true", "redis.type", "LIST",
                    "redis.keyspace", "${topic}", "redis.multiexec", "true", "redis.key.ttl", "3600"));
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
