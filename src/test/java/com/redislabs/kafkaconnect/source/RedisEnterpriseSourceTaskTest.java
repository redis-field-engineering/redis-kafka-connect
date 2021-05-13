package com.redislabs.kafkaconnect.source;

import com.redislabs.kafkaconnect.AbstractRedisEnterpriseTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisEnterpriseSourceTaskTest extends AbstractRedisEnterpriseTest {


    private static final String STREAM = "stream1";

    private final RedisEnterpriseSourceTask task = new RedisEnterpriseSourceTask();

    @BeforeEach
    public void setup() {
        final Map<String, String> config = new HashMap<>();
        config.put("redis.uri", redisUri);
        config.put("redis.stream.name", STREAM);
        task.start(config);
    }

    @AfterEach
    public void teardown() {
        task.stop();
    }

    @Test
    public void pollRetrievesStreamMessages() throws InterruptedException {
        syncCommands.xadd(STREAM, body("field1", "value1", "field2", "value2"));
        syncCommands.xadd(STREAM, body("field1", "value1", "field2", "value2"));
        syncCommands.xadd(STREAM, body("field1", "value1", "field2", "value2"));
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