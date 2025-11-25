package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackSinkIntegrationTests extends AbstractSinkIntegrationTests {

	private static final RedisStackContainer container = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
