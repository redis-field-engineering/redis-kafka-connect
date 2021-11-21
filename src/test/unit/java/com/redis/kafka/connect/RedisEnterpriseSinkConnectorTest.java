package com.redis.kafka.connect;

import com.redis.kafka.connect.sink.RedisEnterpriseSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class RedisEnterpriseSinkConnectorTest {

	@Test
	void testConfig() {
		ConfigDef config = new RedisEnterpriseSinkConnector().config();
		Assertions.assertNotNull(config);
	}

	@Test
	void testTask() {
		Assertions.assertEquals(RedisEnterpriseSinkTask.class, new RedisEnterpriseSinkConnector().taskClass());
	}

	@Test
	void testTaskConfigs() {
		RedisEnterpriseSinkConnector connector = new RedisEnterpriseSinkConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put("field1", "value1");
		connector.start(props);
		Assertions.assertEquals(props, connector.taskConfigs(123).get(0));
	}

	@Test
	void testVersion() {
		Assertions.assertEquals(AppInfoParser.getVersion(), new RedisEnterpriseSinkConnector().version());
	}

}
