package com.redislabs.kafka.connect;

import com.redislabs.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redislabs.kafka.connect.source.RedisEnterpriseSourceTask;
import com.redislabs.testcontainers.RedisServer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisEnterpriseSourceTaskIT extends AbstractRedisEnterpriseIT {

    private RedisEnterpriseSourceTask task;

    @BeforeEach
    public void createTask() {
        task = new RedisEnterpriseSourceTask();
    }

    private void startTask(RedisServer redis, String... props) {
        Map<String, String> config = map(props);
        config.put(RedisEnterpriseSourceConfig.REDIS_URI, redis.getRedisURI());
        task.start(config);
    }

    @AfterEach
    public void teardown() {
        task.stop();
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("redisServers")
    public void pollStream(RedisServer redis) throws InterruptedException {
        final String stream = "stream1";
        startTask(redis, RedisEnterpriseSourceConfig.TOPIC, RedisEnterpriseSourceConfig.TOKEN_STREAM, RedisEnterpriseSourceConfig.READER_TYPE, RedisEnterpriseSourceConfig.ReaderType.STREAM.name(), RedisEnterpriseSourceConfig.STREAM_NAME, stream);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        syncStream(redis).xadd(stream, map(field1, value1, field2, value2));
        syncStream(redis).xadd(stream, map(field1, value1, field2, value2));
        syncStream(redis).xadd(stream, map(field1, value1, field2, value2));
        Thread.sleep(100);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(3, sourceRecords.size());
        for (SourceRecord record : sourceRecords) {
            Assertions.assertEquals(stream, record.topic());
            Map<String, String> map = (Map<String, String>) record.value();
            Assertions.assertEquals(value1, map.get(field1));
            Assertions.assertEquals(value2, map.get(field2));
        }
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void pollKeys(RedisServer redis) throws InterruptedException {
        String topic = "mytopic";
        startTask(redis, RedisEnterpriseSourceConfig.TOPIC, topic, RedisEnterpriseSourceTask.KEYS_IDLE_TIMEOUT, "800");
        Thread.sleep(500);
        String stringKey = "key:1";
        String stringValue = "my string";
        syncString(redis).set(stringKey, stringValue);
        String hashKey = "key:2";
        Map<String, String> hashValue = map("field1", "value1", "field2", "value2");
        syncHash(redis).hset(hashKey, hashValue);
        Thread.sleep(100);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(2, sourceRecords.size());
        for (SourceRecord record : sourceRecords) {
            assertEquals(topic, record.topic());
        }
        assertEquals(stringValue, sourceRecords.get(0).value());
        assertEquals(hashValue, sourceRecords.get(1).value());
    }

}
