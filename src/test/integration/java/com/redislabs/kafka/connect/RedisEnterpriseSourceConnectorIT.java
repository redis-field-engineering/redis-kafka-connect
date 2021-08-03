package com.redislabs.kafka.connect;

import com.redislabs.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redislabs.testcontainers.RedisServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisEnterpriseSourceConnectorIT extends AbstractRedisEnterpriseIT {

    @ParameterizedTest
    @MethodSource("redisServers")
    public void taskConfigsReturnsPartitionedTaskConfigs(RedisServer server) {
        final RedisEnterpriseSourceConnector sourceConnector = new RedisEnterpriseSourceConnector();

        final Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put(RedisEnterpriseSourceConfig.READER_TYPE, RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
        connectorConfig.put(RedisEnterpriseSourceConfig.STREAM_NAME, "dummy");
        connectorConfig.put(RedisEnterpriseSourceConfig.TOPIC, "mytopic");
        connectorConfig.put(RedisEnterpriseSourceConfig.REDIS_URI, server.getRedisURI());
        connectorConfig.put(RedisEnterpriseSourceConfig.KEY_PATTERNS, "a:*,b:*,c:*");

        final Map<String, String> expectedPartitionedConnectorConfigA = new HashMap<>();
        expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.READER_TYPE, RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
        expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.STREAM_NAME, "dummy");
        expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.TOPIC, "mytopic");
        expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.REDIS_URI, server.getRedisURI());
        expectedPartitionedConnectorConfigA.put(RedisEnterpriseSourceConfig.KEY_PATTERNS, "a:*,b:*");

        final Map<String, String> expectedPartitionedConnectorConfigB = new HashMap<>();
        expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.READER_TYPE, RedisEnterpriseSourceConfig.ReaderType.KEYS.name());
        expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.STREAM_NAME, "dummy");
        expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.TOPIC, "mytopic");
        expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.REDIS_URI, server.getRedisURI());
        expectedPartitionedConnectorConfigB.put(RedisEnterpriseSourceConfig.KEY_PATTERNS, "c:*");

        sourceConnector.start(connectorConfig);

        final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(2);

        assertEquals(2, taskConfigs.size());
        assertEquals(expectedPartitionedConnectorConfigA, taskConfigs.get(0));
        assertEquals(expectedPartitionedConnectorConfigB, taskConfigs.get(1));
    }
}
