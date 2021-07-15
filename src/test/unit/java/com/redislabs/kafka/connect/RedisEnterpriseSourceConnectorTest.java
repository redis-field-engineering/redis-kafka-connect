package com.redislabs.kafka.connect;

import com.redislabs.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redislabs.kafka.connect.source.RedisEnterpriseSourceTask;
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
        ConfigValue value = results.get(RedisEnterpriseSourceConfig.READER_TYPE);
        Assertions.assertEquals(RedisEnterpriseSourceConfig.READER_TYPE, value.name());
        Assertions.assertEquals(RedisEnterpriseSourceConfig.ReaderType.KEYS.name(), value.value());
    }

    @Test
    public void testTask() {
        Assertions.assertEquals(RedisEnterpriseSourceTask.class, new RedisEnterpriseSourceConnector().taskClass());
    }

    @Test
    public void testTaskConfigs() {
        RedisEnterpriseSourceConnector connector = new RedisEnterpriseSourceConnector();
        HashMap<String, String> props = new HashMap<>();
        props.put(RedisEnterpriseSourceConfig.TOPIC, "topic");
        props.put("field1", "value1");
        connector.start(props);
        HashMap<String, String> expected = new HashMap<>(props);
        expected.put(RedisEnterpriseSourceConfig.KEY_PATTERNS, "*");
        Assertions.assertEquals(expected, connector.taskConfigs(123).get(0));
    }

    @Test
    public void testVersion() {
        Assertions.assertEquals(AppInfoParser.getVersion(), new RedisEnterpriseSourceConnector().version());
    }


}
