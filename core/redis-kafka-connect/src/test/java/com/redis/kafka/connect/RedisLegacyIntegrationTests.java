package com.redis.kafka.connect;

import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Compatibility before PEXPIRETIME (7.0) and XAUTOCLAIM (6.2). */
class RedisLegacyIntegrationTests extends AbstractRedisIntegrationTests {
    private final RedisContainer container = new RedisContainer(DockerImageName.parse(
            System.getProperty("redis.legacy.image", "redis:6.0.20")));

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }

    @Test
    void snapshotExpiryAndNotificationsWorkWithoutPexpiretime() throws Exception {
        enableKeyspaceNotifications();
        redisConnection.sync().set("legacy:persistent", "persistent");
        redisConnection.sync().set("legacy:expiring", "expiring");
        redisConnection.sync().pexpire("legacy:expiring", 60000);
        long before = System.currentTimeMillis();
        RedisKeysSourceTask source = new RedisKeysSourceTask();
        try {
            source.start(deploymentProps(Map.of("redis.keys.pattern", "legacy:*", "redis.keys.timeout", "100")));
            List<SourceRecord> records = new ArrayList<>();
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                records.addAll(source.poll());
                return records.stream().map(SourceRecord::key).distinct().count() == 2;
            });
            Struct persistent = value(records, "legacy:persistent");
            Struct expiring = value(records, "legacy:expiring");
            assertEquals(-1L, persistent.getInt64("ttl"));
            assertTrue(expiring.getInt64("ttl") >= before + 55000);
            assertTrue(expiring.getInt64("ttl") <= System.currentTimeMillis() + 60000);
            records.clear();
            redisConnection.sync().set("legacy:persistent", "updated");
            redisConnection.sync().del("legacy:expiring");
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                records.addAll(source.poll());
                return records.stream().anyMatch(record -> "legacy:persistent".equals(record.key())
                                && "updated".equals(((Struct) record.value()).getString("string")))
                        && records.stream().anyMatch(record -> "legacy:expiring".equals(record.key())
                                && "none".equals(((Struct) record.value()).getString("type")));
            });
            assertEquals(-2L, value(records, "legacy:expiring").getInt64("ttl"));
        } finally {
            source.stop();
        }
    }

    private Struct value(List<SourceRecord> records, String key) {
        return (Struct) records.stream().filter(record -> key.equals(record.key())).findFirst().orElseThrow().value();
    }

    @Test
    void streamDefaultsAndOwnPendingRecoveryWorkWithoutXautoclaim() {
        RedisStreamSourceTask source = new RedisStreamSourceTask();
        Map<String, String> config = deploymentProps(Map.of("redis.stream.name", "legacy:stream", "redis.stream.block", "0"));
        try {
            source.start(config);
            String id = redisConnection.sync().xadd("legacy:stream", Map.of("value", "pending"));
            assertEquals(id, source.poll().get(0).key());
            source.stop();
            source.start(config);
            SourceRecord recovered = source.poll().get(0);
            assertEquals(id, recovered.key());
            source.commitRecord(recovered, null);
            source.commit();
            assertEquals(0, redisConnection.sync().xpending("legacy:stream", "kafka-consumer-group").getCount());
            String next = redisConnection.sync().xadd("legacy:stream", Map.of("value", "next"));
            assertEquals(next, source.poll().get(0).key());
        } finally {
            source.stop();
        }
    }

    @Test
    void sinkTransactionsWorkOnLegacyRedis() {
        RedisSinkTask sink = new RedisSinkTask();
        try {
            sink.start(deploymentProps(Map.of("redis.type", "LIST", "redis.multiexec", "true")));
            sink.put(List.of(new SinkRecord("legacy:list", 0, Schema.STRING_SCHEMA, "member", Schema.STRING_SCHEMA, "value", 0)));
            assertEquals(List.of("member"), redisConnection.sync().lrange("legacy:list", 0, -1));
        } finally {
            sink.stop();
        }
    }
}
