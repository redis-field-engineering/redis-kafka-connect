package com.redis.kafka.connect;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(value = OS.LINUX)
class EnterpriseSinkIntegrationTests extends AbstractSinkIntegrationTests {

	private static final RedisEnterpriseContainer container = RedisContainerFactory.enterprise();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
