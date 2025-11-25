package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackSourceIntegrationTests extends AbstractSourceIntegrationTests {

	private static final RedisStackContainer container = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
