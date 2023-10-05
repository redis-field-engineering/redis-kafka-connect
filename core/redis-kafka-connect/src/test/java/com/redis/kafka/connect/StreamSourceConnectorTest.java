package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.RedisStreamSourceConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisURI;

class StreamSourceConnectorTest {

	@Test
	void testTask() {
		Assertions.assertEquals(RedisStreamSourceTask.class, new RedisStreamSourceConnector().taskClass());
	}

	@Test
	void testStreamReaderTaskConfig() {
		RedisStreamSourceConnector connector = new RedisStreamSourceConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "mystream");
		props.put("field1", "value1");
		connector.start(props);
		HashMap<String, String> expected = new HashMap<>(props);
		expected.put("task.id", "0");
		Assertions.assertEquals(expected, connector.taskConfigs(123).get(0));
	}

	@Test
	void testVersion() {
		Assertions.assertEquals(ManifestVersionProvider.getVersion(), new RedisStreamSourceConnector().version());
	}

	@Test
	void taskConfigsReturnsPartitionedTaskConfigs() {
		String redisURI = "redis://localhost:6379";

		final RedisStreamSourceConnector sourceConnector = new RedisStreamSourceConnector();

		final Map<String, String> connectorConfig = new HashMap<>();
		connectorConfig.put(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "dummy");
		connectorConfig.put(RedisStreamSourceConfigDef.TOPIC_CONFIG, "mytopic");
		connectorConfig.put(RedisStreamSourceConfigDef.URI_CONFIG, redisURI);

		final Map<String, String> expectedPartitionedConnectorConfigA = new HashMap<>();
		expectedPartitionedConnectorConfigA.put(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "dummy");
		expectedPartitionedConnectorConfigA.put(RedisStreamSourceConfigDef.TOPIC_CONFIG, "mytopic");
		expectedPartitionedConnectorConfigA.put(RedisStreamSourceConfigDef.URI_CONFIG, redisURI);
		expectedPartitionedConnectorConfigA.put("task.id", "0");
		final Map<String, String> expectedPartitionedConnectorConfigB = new HashMap<>();
		expectedPartitionedConnectorConfigB.put(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "dummy");
		expectedPartitionedConnectorConfigB.put(RedisStreamSourceConfigDef.TOPIC_CONFIG, "mytopic");
		expectedPartitionedConnectorConfigB.put(RedisStreamSourceConfigDef.URI_CONFIG, redisURI);
		expectedPartitionedConnectorConfigB.put("task.id", "1");
		sourceConnector.start(connectorConfig);

		final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(2);

		assertEquals(2, taskConfigs.size());
		assertEquals(expectedPartitionedConnectorConfigA, taskConfigs.get(0));
		assertEquals(expectedPartitionedConnectorConfigB, taskConfigs.get(1));
	}

	@Test
	void testRedisStreamConfig() {
		final Map<String, String> props = new HashMap<>();
		props.put(RedisConfigDef.HOST_CONFIG, "localhost:6379");
		String username = "user1";
		props.put(RedisConfigDef.USERNAME_CONFIG, username);
		String password = "password";
		props.put(RedisConfigDef.PASSWORD_CONFIG, password);
		props.put(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "stream");
		RedisURI redisURI = new RedisStreamSourceConfig(props).uri();
		RedisCredentials credentials = redisURI.getCredentialsProvider().resolveCredentials().block();
		assertEquals(username, credentials.getUsername());
		Assertions.assertArrayEquals(password.toCharArray(), credentials.getPassword());
		Assertions.assertFalse(redisURI.isSsl());
		props.put(RedisConfigDef.TLS_CONFIG, "true");
		redisURI = new RedisStreamSourceConfig(props).uri();
		Assertions.assertTrue(redisURI.isSsl());
		props.put(RedisConfigDef.CLUSTER_CONFIG, "true");
		AbstractRedisClient client = new RedisStreamSourceConfig(props).client();
		Assertions.assertInstanceOf(RedisModulesClusterClient.class, client);

	}

}
