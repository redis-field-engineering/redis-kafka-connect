package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.source.RedisSourceConfig;
import com.redis.kafka.connect.source.RedisSourceTask;

class RedisSourceConnectorTest {

	@Test
	void testConfig() {
		ConfigDef config = new RedisSourceConnector().config();
		Assertions.assertNotNull(config);
		Map<String, ConfigValue> results = config.validateAll(new HashMap<>());
		ConfigValue value = results.get(RedisSourceConfig.READER_CONFIG);
		Assertions.assertEquals(RedisSourceConfig.READER_CONFIG, value.name());
		Assertions.assertEquals(RedisSourceConfig.ReaderType.STREAM.name(), value.value());
	}

	@Test
	void testTask() {
		Assertions.assertEquals(RedisSourceTask.class, new RedisSourceConnector().taskClass());
	}

	@Test
	void testStreamReaderTaskConfig() {
		RedisSourceConnector connector = new RedisSourceConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put(RedisSourceConfig.STREAM_NAME_CONFIG, "mystream");
		props.put("field1", "value1");
		connector.start(props);
		HashMap<String, String> expected = new HashMap<>(props);
		expected.put("task.id", "0");
		Assertions.assertEquals(expected, connector.taskConfigs(123).get(0));
	}

	@Test
	void testKeyReaderTaskConfig() {
		RedisSourceConnector connector = new RedisSourceConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put(RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.KEYS.name());
		props.put(RedisSourceConfig.STREAM_NAME_CONFIG, "dummy");
		props.put(RedisSourceConfig.TOPIC_CONFIG, "mytopic");
		props.put(RedisSourceConfig.URI_CONFIG, "redis://localhost:12000");
		props.put(RedisSourceConfig.KEY_PATTERNS_CONFIG, "a:*");
		props.put("field1", "value1");
		connector.start(props);
		HashMap<String, String> expected = new HashMap<>(props);
		Assertions.assertEquals(expected, connector.taskConfigs(123).get(0));

	}

	@Test
	void testVersion() {
		Assertions.assertEquals(AppInfoParser.getVersion(), new RedisSourceConnector().version());
	}

	@Test
	void taskConfigsReturnsPartitionedTaskConfigs() {
		String redisURI = "redis://localhost:6379";

		final RedisSourceConnector sourceConnector = new RedisSourceConnector();

		final Map<String, String> connectorConfig = new HashMap<>();
		connectorConfig.put(RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.KEYS.name());
		connectorConfig.put(RedisSourceConfig.STREAM_NAME_CONFIG, "dummy");
		connectorConfig.put(RedisSourceConfig.TOPIC_CONFIG, "mytopic");
		connectorConfig.put(RedisSourceConfig.URI_CONFIG, redisURI);
		connectorConfig.put(RedisSourceConfig.KEY_PATTERNS_CONFIG, "a:*,b:*,c:*");

		final Map<String, String> expectedPartitionedConnectorConfigA = new HashMap<>();
		expectedPartitionedConnectorConfigA.put(RedisSourceConfig.READER_CONFIG,
				RedisSourceConfig.ReaderType.KEYS.name());
		expectedPartitionedConnectorConfigA.put(RedisSourceConfig.STREAM_NAME_CONFIG, "dummy");
		expectedPartitionedConnectorConfigA.put(RedisSourceConfig.TOPIC_CONFIG, "mytopic");
		expectedPartitionedConnectorConfigA.put(RedisSourceConfig.URI_CONFIG, redisURI);
		expectedPartitionedConnectorConfigA.put(RedisSourceConfig.KEY_PATTERNS_CONFIG, "a:*,b:*");

		final Map<String, String> expectedPartitionedConnectorConfigB = new HashMap<>();
		expectedPartitionedConnectorConfigB.put(RedisSourceConfig.READER_CONFIG,
				RedisSourceConfig.ReaderType.KEYS.name());
		expectedPartitionedConnectorConfigB.put(RedisSourceConfig.STREAM_NAME_CONFIG, "dummy");
		expectedPartitionedConnectorConfigB.put(RedisSourceConfig.TOPIC_CONFIG, "mytopic");
		expectedPartitionedConnectorConfigB.put(RedisSourceConfig.URI_CONFIG, redisURI);
		expectedPartitionedConnectorConfigB.put(RedisSourceConfig.KEY_PATTERNS_CONFIG, "c:*");

		sourceConnector.start(connectorConfig);

		final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(2);

		assertEquals(2, taskConfigs.size());
		assertEquals(expectedPartitionedConnectorConfigA, taskConfigs.get(0));
		assertEquals(expectedPartitionedConnectorConfigB, taskConfigs.get(1));
	}

}
