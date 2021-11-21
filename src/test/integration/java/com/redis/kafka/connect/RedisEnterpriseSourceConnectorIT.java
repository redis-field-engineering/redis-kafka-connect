package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.source.RedisEnterpriseSourceConfig;

class RedisEnterpriseSourceConnectorIT {

	@Test
	void taskConfigsReturnsPartitionedTaskConfigs() {
		String redisURI = "redis://localhost:6379";
		
		final RedisEnterpriseSourceConnector sourceConnector = new RedisEnterpriseSourceConnector();

		final Map<String, String> connectorConfig = new HashMap<>();
		connectorConfig.put(RedisEnterpriseSourceConfig.READER_CONFIG,
				RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
		connectorConfig.put(RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG, "dummy");
		connectorConfig.put(RedisEnterpriseSourceConfig.TOPIC_CONFIG, "mytopic");
		connectorConfig.put(RedisEnterpriseSourceConfig.REDIS_URI_CONFIG, redisURI);
		connectorConfig.put(RedisEnterpriseSourceConfig.KEY_PATTERNS_CONFIG, "a:*,b:*,c:*");

		final Map<String, String> expectedPartitionedConnectorConfigA = new HashMap<>();
		expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.READER_CONFIG,
				RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
		expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG, "dummy");
		expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.TOPIC_CONFIG, "mytopic");
		expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.REDIS_URI_CONFIG, redisURI);
		expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.KEY_PATTERNS_CONFIG, "a:*,b:*");

		final Map<String, String> expectedPartitionedConnectorConfigB = new HashMap<>();
		expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.READER_CONFIG,
				RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
		expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG, "dummy");
		expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.TOPIC_CONFIG, "mytopic");
		expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.REDIS_URI_CONFIG, redisURI);
		expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.KEY_PATTERNS_CONFIG, "c:*");

		sourceConnector.start(connectorConfig);

		final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(2);

		assertEquals(2, taskConfigs.size());
		assertEquals(expectedPartitionedConnectorConfigA, taskConfigs.get(0));
		assertEquals(expectedPartitionedConnectorConfigB, taskConfigs.get(1));
	}
}
