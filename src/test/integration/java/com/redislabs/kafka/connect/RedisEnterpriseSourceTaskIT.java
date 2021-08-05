package com.redislabs.kafka.connect;

import com.redislabs.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redislabs.kafka.connect.source.RedisEnterpriseSourceTask;
import com.redislabs.testcontainers.RedisServer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

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
        final String topicPrefix = "testprefix-";
        startTask(redis, RedisEnterpriseSourceConfig.TOPIC, topicPrefix + RedisEnterpriseSourceConfig.TOKEN_STREAM, RedisEnterpriseSourceConfig.READER_TYPE, RedisEnterpriseSourceConfig.ReaderType.STREAM.name(), RedisEnterpriseSourceConfig.STREAM_NAME, stream);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        final Map<String, String> body = map(field1, value1, field2, value2);
        final String id1 = syncStream(redis).xadd(stream, body);
        final String id2 = syncStream(redis).xadd(stream, body);
        final String id3 = syncStream(redis).xadd(stream, body);
        Thread.sleep(100);
        List<SourceRecord> sourceRecords = task.poll();
        Assertions.assertEquals(3, sourceRecords.size());
        assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
        assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
        assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
    }

    private void assertEquals(String expectedId, Map<String, String> expectedBody, String expectedStream, String expectedTopic, SourceRecord record) {
        Struct struct = (Struct) record.value();
        Assertions.assertEquals(expectedId, struct.get("id"));
        Assertions.assertEquals(expectedBody, struct.get("body"));
        Assertions.assertEquals(expectedStream, struct.get("stream"));
        Assertions.assertEquals(expectedTopic, record.topic());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void pollKeys(RedisServer redis) throws InterruptedException {
        String topic = "mytopic";
        startTask(redis, RedisEnterpriseSourceConfig.READER_TYPE, RedisEnterpriseSourceConfig.ReaderType.KEYS.name(), RedisEnterpriseSourceConfig.STREAM_NAME, "dummy", RedisEnterpriseSourceConfig.TOPIC, topic, RedisEnterpriseSourceTask.KEYS_IDLE_TIMEOUT, "800");
        Thread.sleep(500);
        String stringKey = "key:1";
        String stringValue = "my string";
        syncString(redis).set(stringKey, stringValue);
        String hashKey = "key:2";
        Map<String, String> hashValue = map("field1", "value1", "field2", "value2");
        syncHash(redis).hset(hashKey, hashValue);
        Thread.sleep(100);
        List<SourceRecord> sourceRecords = task.poll();
        Assertions.assertEquals(2, sourceRecords.size());
        for (SourceRecord record : sourceRecords) {
            Assertions.assertEquals(topic, record.topic());
        }
        Assertions.assertEquals(stringValue, sourceRecords.get(0).value());
        Assertions.assertEquals(hashValue, sourceRecords.get(1).value());
    }

}
