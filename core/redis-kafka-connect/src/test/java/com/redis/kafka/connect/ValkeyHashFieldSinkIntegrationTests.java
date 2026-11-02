package com.redis.kafka.connect;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

class ValkeyHashFieldSinkIntegrationTests extends AbstractHashFieldSinkIntegrationTests {

	private static final RedisContainer container = RedisContainerFactory.valkey();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
