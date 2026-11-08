package com.redis.kafka.connect;

import java.util.HashMap;
import java.util.Map;

import com.redis.kafka.connect.sink.RedisSinkConfig;
import com.redis.kafka.connect.sink.RedisSinkConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

    @ParameterizedTest
    @CsvSource({"redis.multiexec,true", "redis.wait.replicas,1"})
    void rejectsClusterConnectionStateOptions(String option, String value) {
        Map<String, String> props = Map.of("redis.cluster", "true", "redis.type", "LIST", option, value);
        ConfigException error = Assertions.assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
        Assertions.assertTrue(error.getMessage().contains(option));
        Assertions.assertTrue(error.getMessage().contains("redis.cluster=false"));
        Assertions.assertFalse(new RedisSinkConfigDef().validateAll(props).get(option).errorMessages().isEmpty());
        Assertions.assertThrows(ConfigException.class, () -> new RedisSinkTask().start(props));
    }

    @Test
    void acceptsClusterPipelinesAndStandaloneConnectionStateOptions() {
        RedisSinkConfig cluster = new RedisSinkConfig(Map.of("redis.cluster", "true"));
        Assertions.assertFalse(cluster.isMultiExec());
        Assertions.assertEquals(0, cluster.getWaitReplicas());
        Map<String, String> props = Map.of("redis.type", "LIST", "redis.multiexec", "true", "redis.wait.replicas", "1");
        RedisSinkConfig standalone = new RedisSinkConfig(props);
        Assertions.assertTrue(standalone.isMultiExec());
        Assertions.assertEquals(1, standalone.getWaitReplicas());
        Assertions.assertTrue(new RedisSinkConfigDef().validateAll(props).values().stream()
                .allMatch(value -> value.errorMessages().isEmpty()));
        Assertions.assertFalse(RedisSinkConfig.CONFIG.names().contains("redis.multiexec.keyslot"));
    }

}
