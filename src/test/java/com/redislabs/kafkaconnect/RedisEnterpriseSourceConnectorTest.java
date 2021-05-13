package com.redislabs.kafkaconnect;

import com.redislabs.kafkaconnect.source.RedisEnterpriseSourceConfig;
import com.redislabs.kafkaconnect.source.RedisEnterpriseSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RedisEnterpriseSourceConnectorTest {

    @Test
    public void testConfig() {
        ConfigDef config = new RedisEnterpriseSourceConnector().config();
        Assertions.assertNotNull(config);
        Assertions.assertTrue(config instanceof RedisEnterpriseSourceConfig.RedisEnterpriseSourceConfigDef);
    }

    @Test
    public void testTask() {
        Assertions.assertEquals(RedisEnterpriseSourceTask.class, new RedisEnterpriseSourceConnector().taskClass());
    }


}
