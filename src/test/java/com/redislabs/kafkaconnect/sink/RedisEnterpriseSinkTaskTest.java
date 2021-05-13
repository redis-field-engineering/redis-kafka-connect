package com.redislabs.kafkaconnect.sink;

import com.redislabs.kafkaconnect.AbstractRedisEnterpriseTest;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

    private static final String TOPIC = "topic1";
    private static final int PARTITION = 0;
    private static final Schema KEY_SCHEMA = null;

    private final RedisEnterpriseSinkTask task = new RedisEnterpriseSinkTask();

    @BeforeEach
    public void setup() {
        final Map<String, String> config = new HashMap<>();
        config.put("redis.uri", redisUri);
        task.start(config);
    }

    @AfterEach
    public void teardown() {
        task.stop();
    }

    private SinkRecord sinkRecord(Schema valueSchema, Object value, long offset) {
        return new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, null, valueSchema, value, offset);
    }

    @Test
    public void maps() {
        final Map<String, String> value = body(FIELD1, "value1", FIELD2, "value2");
        final long offset = 0L;

        task.put(Arrays.asList(sinkRecord(MAP_VALUE_SCHEMA, value, offset), sinkRecord(MAP_VALUE_SCHEMA, value, offset + 1)));
        List<StreamMessage<String, String>> messages = syncCommands.xread(XReadArgs.Builder.block(200).count(10), XReadArgs.StreamOffset.from(TOPIC, "0-0"));
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals("value1", message.getBody().get(FIELD1));
            Assertions.assertEquals("value2", message.getBody().get(FIELD2));
        }
    }

    @Test
    public void structs() {
        final Struct value = new Struct(STRUCT_VALUE_SCHEMA).put(FIELD1, "value1").put(FIELD2, "value2");
        final long offset = 0L;
        task.put(Arrays.asList(sinkRecord(STRUCT_VALUE_SCHEMA, value, offset), sinkRecord(STRUCT_VALUE_SCHEMA, value, offset + 1)));
        List<StreamMessage<String, String>> messages = syncCommands.xread(XReadArgs.Builder.block(200).count(10), XReadArgs.StreamOffset.from(TOPIC, "0-0"));
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals(TOPIC, message.getStream());
            Assertions.assertEquals("value1", message.getBody().get(FIELD1));
            Assertions.assertEquals("value2", message.getBody().get(FIELD2));
        }
    }

}
