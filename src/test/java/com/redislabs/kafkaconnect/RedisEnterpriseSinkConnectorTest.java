package com.redislabs.kafkaconnect;

import com.redislabs.kafkaconnect.sink.RedisEnterpriseSinkConfig;
import com.redislabs.kafkaconnect.sink.RedisEnterpriseSinkTask;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.common.config.ConfigDef;
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

public class RedisEnterpriseSinkConnectorTest {

    @Test
    public void testConfig() {
        ConfigDef config = new RedisEnterpriseSinkConnector().config();
        Assertions.assertNotNull(config);
        Assertions.assertTrue(config instanceof RedisEnterpriseSinkConfig.RedisEnterpriseSinkConfigDef);
    }

    @Test
    public void testTask() {
        Assertions.assertEquals(RedisEnterpriseSinkTask.class, new RedisEnterpriseSinkConnector().taskClass());
    }


}
