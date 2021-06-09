package com.redislabs.kafkaconnect;

import com.redislabs.kafkaconnect.source.RedisEnterpriseSourceTask;
import com.redislabs.testcontainers.RedisServer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisEnterpriseSourceTaskIT extends AbstractRedisEnterpriseIT {

    private static final String STREAM = "stream1";

    private RedisEnterpriseSourceTask task;

    @BeforeEach
    public void createTask() {
        task = new RedisEnterpriseSourceTask();
    }

    private void startTask(RedisServer redis) {
        final Map<String, String> config = new HashMap<>();
        config.put("redis.uri", redis.getRedisURI());
        config.put("redis.stream.name", STREAM);
        task.start(config);
    }

    @AfterEach
    public void teardown() {
        task.stop();
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void pollRetrievesStreamMessages(RedisServer redis) throws InterruptedException {
        startTask(redis);
        syncStream(redis).xadd(STREAM, map("field1", "value1", "field2", "value2"));
        syncStream(redis).xadd(STREAM, map("field1", "value1", "field2", "value2"));
        syncStream(redis).xadd(STREAM, map("field1", "value1", "field2", "value2"));
        Thread.sleep(100);
        final List<SourceRecord> sourceRecords = task.poll();
        assertEquals(3, sourceRecords.size());
        for (SourceRecord record : sourceRecords) {
            Assertions.assertEquals(STREAM, record.topic());
            Map<String, String> map = (Map<String, String>) record.value();
            Assertions.assertEquals("value1", map.get("field1"));
            Assertions.assertEquals("value2", map.get("field2"));
        }
    }
}
