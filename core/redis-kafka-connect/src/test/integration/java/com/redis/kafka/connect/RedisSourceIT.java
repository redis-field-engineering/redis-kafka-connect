package com.redis.kafka.connect;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

class RedisSourceIT extends AbstractSourceIT {

	private static final RedisContainer container = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
