package com.redislabs.kafkaconnect;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisEnterpriseSourceTaskTest extends AbstractRedisEnterpriseTest {

    @Test
    public void pollRetrievesStreamMessages() throws InterruptedException {
        String topic = "topic1";
        String stream = "stream1";
        final RedisEnterpriseSourceTask sourceTask = new RedisEnterpriseSourceTask();
        final Map<String, String> config = new HashMap<>();
        config.put("topic", topic);
        config.put("redis.uri", redisUri);
        config.put("redis.stream.name", stream);
        sourceTask.start(config);
        syncCommands.xadd(stream, body("field1", "value1", "field2", "value2"));
        syncCommands.xadd(stream, body("field1", "value1", "field2", "value2"));
        syncCommands.xadd(stream, body("field1", "value1", "field2", "value2"));
        Thread.sleep(100);
        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(3, sourceRecords.size());
        for (SourceRecord record : sourceRecords) {
            Map<String, String> map = (Map<String, String>) record.value();
            Assertions.assertEquals("value1", map.get("field1"));
            Assertions.assertEquals("value2", map.get("field2"));
        }
    }
}