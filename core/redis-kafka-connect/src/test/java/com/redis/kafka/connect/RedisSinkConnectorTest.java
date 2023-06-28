package com.redis.kafka.connect;

import java.util.HashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.kafka.connect.sink.RedisSinkTask;

class RedisSinkConnectorTest {

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
		Assertions.assertEquals(props, connector.taskConfigs(123).get(0));
	}

	@Test
	void testVersion() {
		Assertions.assertEquals(ManifestVersionProvider.getVersion(), new RedisSinkConnector().version());
	}

}
