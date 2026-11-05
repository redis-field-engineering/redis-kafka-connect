package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;
import org.testcontainers.utility.DockerImageName;

/** Runs the same connector contract against the Redis 7.2 fixture used by version 1.1.0. */
class Redis72SinkIntegrationTests extends AbstractSinkIntegrationTests {
    private final RedisStackContainer container = new RedisStackContainer(
            DockerImageName.parse(System.getProperty("redis.stack.image", "redis/redis-stack-server:7.2.0-v13")));

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }
}
