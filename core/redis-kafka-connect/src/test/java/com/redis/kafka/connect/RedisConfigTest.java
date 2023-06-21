package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.common.RedisConfig;
import com.redis.kafka.connect.common.RedisConfig.RedisConfigDef;
import com.redis.kafka.connect.source.RedisSourceConfig;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisURI;

class RedisConfigTest {

	@Test
	void testConfig() {
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

	@Test
	void testRedisConfig() {
		final Map<String, String> props = new HashMap<>();
		props.put(RedisConfig.HOST_CONFIG, "localhost:6379");
		String username = "user1";
		props.put(RedisConfig.USERNAME_CONFIG, username);
		String password = "password";
		props.put(RedisConfig.PASSWORD_CONFIG, password);
		RedisURI redisURI = new RedisConfig(new RedisConfigDef(), props).uri();
		RedisCredentials credentials = redisURI.getCredentialsProvider().resolveCredentials().block();
		assertEquals(username, credentials.getUsername());
		Assertions.assertArrayEquals(password.toCharArray(), credentials.getPassword());
		Assertions.assertFalse(redisURI.isSsl());
		props.put(RedisConfig.TLS_CONFIG, "true");
		redisURI = new RedisConfig(new RedisConfigDef(), props).uri();
		Assertions.assertTrue(redisURI.isSsl());
		props.put(RedisConfig.CLUSTER_CONFIG, "true");
		AbstractRedisClient client = new RedisConfig(new RedisConfigDef(), props).client();
		Assertions.assertInstanceOf(RedisModulesClusterClient.class, client);

	}

}
