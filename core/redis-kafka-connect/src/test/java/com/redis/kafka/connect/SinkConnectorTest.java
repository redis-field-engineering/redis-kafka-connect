package com.redis.kafka.connect;

import java.util.HashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.kafka.connect.sink.RedisSinkTask;

class SinkConnectorTest {

	@Test
	void testConfig() {
		ConfigDef config = new RedisSinkConnector().config();
		Assertions.assertNotNull(config);
	}

	@Test
	void testTask() {
		Assertions.assertEquals(RedisSinkTask.class, new RedisSinkConnector().taskClass());
	}

	@Test
	void testTaskConfigs() {
		RedisSinkConnector connector = new RedisSinkConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put("field1", "value1");
		connector.start(props);
		int maxTasks = 123;
		Assertions.assertEquals(props, connector.taskConfigs(maxTasks).get(0));
		Assertions.assertEquals(maxTasks, connector.taskConfigs(maxTasks).size());
	}

	@Test
	void testVersion() {
		Assertions.assertEquals(ManifestVersionProvider.getVersion(), new RedisSinkConnector().version());
	}

}
