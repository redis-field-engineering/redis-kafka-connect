package com.redislabs.kafkaconnect;

import com.redislabs.kafkaconnect.sink.RedisEnterpriseSinkTask;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisEnterpriseSinkTaskTest extends AbstractRedisEnterpriseTest {

    private static final String VALUE_SCHEMA_NAME = "com.redislabs.kafkaconnect.test.value";
    private static final Schema MAP_VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME).build();
    private static final String FIELD1 = "field1";
    private static final String FIELD2 = "field2";
    private static final Schema STRUCT_VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD1, Schema.STRING_SCHEMA).field(FIELD2, Schema.STRING_SCHEMA);

    @Test
    public void putMaps() {
        final Map<String, String> config = new HashMap<>();
        config.put("redis.uri", redisUri);
        final String topic = "topic1";
        final int partition = 0;
        final Schema keySchema = null;
        final Object key = null;
        final Map<String, String> value = body(FIELD1, "value1", FIELD2, "value2");
        final long offset = 0L;

        // Configure task and write records
        final RedisEnterpriseSinkTask sinkTask = new RedisEnterpriseSinkTask();
        sinkTask.start(config);
        sinkTask.put(Arrays.asList(new SinkRecord(topic, partition, keySchema, key, MAP_VALUE_SCHEMA, value, offset), new SinkRecord(topic, partition, keySchema, key, MAP_VALUE_SCHEMA, value, offset + 1)));
        List<StreamMessage<String, String>> messages = syncCommands.xread(XReadArgs.Builder.block(200).count(10), XReadArgs.StreamOffset.from(topic, "0-0"));
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals("value1", message.getBody().get(FIELD1));
            Assertions.assertEquals("value2", message.getBody().get(FIELD2));
        }
    }

    @Test
    public void putStructs() {
        final Map<String, String> config = new HashMap<>();
        config.put("redis.uri", redisUri);
        final String topic = "topic1";
        final int partition = 0;
        final Schema keySchema = null;
        final Object key = null;

        final Struct value = new Struct(STRUCT_VALUE_SCHEMA).put(FIELD1, "value1").put(FIELD2, "value2");
        final long offset = 0L;

        // Configure task and write records
        final RedisEnterpriseSinkTask sinkTask = new RedisEnterpriseSinkTask();
        sinkTask.start(config);
        sinkTask.put(Arrays.asList(new SinkRecord(topic, partition, keySchema, key, STRUCT_VALUE_SCHEMA, value, offset), new SinkRecord(topic, partition, keySchema, key, STRUCT_VALUE_SCHEMA, value, offset + 1)));
        List<StreamMessage<String, String>> messages = syncCommands.xread(XReadArgs.Builder.block(200).count(10), XReadArgs.StreamOffset.from(topic, "0-0"));
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals("value1", message.getBody().get(FIELD1));
            Assertions.assertEquals("value2", message.getBody().get(FIELD2));
        }
    }

}
