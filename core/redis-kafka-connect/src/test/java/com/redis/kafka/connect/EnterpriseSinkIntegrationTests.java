package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;

class EnterpriseSinkIntegrationTests extends AbstractSinkIntegrationTests {

	private static final RedisEnterprise8Container container = RedisContainerFactory.enterprise();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
