package com.redislabs.kafkaconnect;

import com.redislabs.kafkaconnect.source.RedisEnterpriseSourceConfig;
import com.redislabs.kafkaconnect.source.RedisEnterpriseSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class RedisEnterpriseSourceConnectorTest {

    @Test
    public void testConfig() {
        ConfigDef config = new RedisEnterpriseSourceConnector().config();
        Assertions.assertNotNull(config);
        Map<String, ConfigValue> results = config.validateAll(new HashMap<>());
        ConfigValue value = results.get(RedisEnterpriseSourceConfig.STREAM_NAME);
        Assertions.assertEquals(RedisEnterpriseSourceConfig.STREAM_NAME, value.name());
        Assertions.assertNull(value.value());
        Assertions.assertEquals("Invalid value null for configuration redis.stream.name: Missing stream configuration: '" + RedisEnterpriseSourceConfig.STREAM_NAME + "'", value.errorMessages().get(0));
    }

    @Test
    public void testTask() {
        Assertions.assertEquals(RedisEnterpriseSourceTask.class, new RedisEnterpriseSourceConnector().taskClass());
    }

    @Test
    public void testTaskConfigs() {
        RedisEnterpriseSourceConnector connector = new RedisEnterpriseSourceConnector();
        HashMap<String, String> props = new HashMap<>();
        props.put("field1", "value1");
        connector.start(props);
        Assertions.assertEquals(props, connector.taskConfigs(123).get(0));
    }

    @Test
    public void testVersion() {
        Assertions.assertEquals(AppInfoParser.getVersion(), new RedisEnterpriseSourceConnector().version());
    }


}
