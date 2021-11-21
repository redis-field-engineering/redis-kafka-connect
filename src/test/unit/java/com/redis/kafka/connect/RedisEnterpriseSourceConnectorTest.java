package com.redis.kafka.connect;

import com.redis.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redis.kafka.connect.source.RedisEnterpriseSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class RedisEnterpriseSourceConnectorTest {

	@Test
	void testConfig() {
		ConfigDef config = new RedisEnterpriseSourceConnector().config();
		Assertions.assertNotNull(config);
		Map<String, ConfigValue> results = config.validateAll(new HashMap<>());
		ConfigValue value = results.get(RedisEnterpriseSourceConfig.READER_CONFIG);
		Assertions.assertEquals(RedisEnterpriseSourceConfig.READER_CONFIG, value.name());
		Assertions.assertEquals(RedisEnterpriseSourceConfig.ReaderType.STREAM.name(), value.value());
	}

	@Test
	void testTask() {
		Assertions.assertEquals(RedisEnterpriseSourceTask.class, new RedisEnterpriseSourceConnector().taskClass());
	}

	@Test
	void testStreamReaderTaskConfig() {
		RedisEnterpriseSourceConnector connector = new RedisEnterpriseSourceConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put(RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG, "mystream");
		props.put("field1", "value1");
		connector.start(props);
		HashMap<String, String> expected = new HashMap<>(props);
		expected.put("task.id", "0");
		Assertions.assertEquals(expected, connector.taskConfigs(123).get(0));
	}

	@Test
	void testKeyReaderTaskConfig() {
		RedisEnterpriseSourceConnector connector = new RedisEnterpriseSourceConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put(RedisEnterpriseSourceConfig.READER_CONFIG, RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
		props.put(RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG, "dummy");
		props.put(RedisEnterpriseSourceConfig.TOPIC_CONFIG, "mytopic");
		props.put(RedisEnterpriseSourceConfig.REDIS_URI_CONFIG, "redis://localhost:12000");
		props.put(RedisEnterpriseSourceConfig.KEY_PATTERNS_CONFIG, "a:*");
		props.put("field1", "value1");
		connector.start(props);
		HashMap<String, String> expected = new HashMap<>(props);
		Assertions.assertEquals(expected, connector.taskConfigs(123).get(0));

	}

	@Test
	void testVersion() {
		Assertions.assertEquals(AppInfoParser.getVersion(), new RedisEnterpriseSourceConnector().version());
	}

}
