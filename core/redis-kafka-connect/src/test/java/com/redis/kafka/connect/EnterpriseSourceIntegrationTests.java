package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;

class EnterpriseSourceIntegrationTests extends AbstractSourceIntegrationTests {

	private static final RedisEnterprise8Container container = RedisContainerFactory.enterprise();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
