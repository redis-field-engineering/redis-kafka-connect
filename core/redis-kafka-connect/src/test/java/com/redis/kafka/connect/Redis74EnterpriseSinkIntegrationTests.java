package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;

/** Retains the Enterprise version exercised by connector 1.1.0. */
class Redis74EnterpriseSinkIntegrationTests extends AbstractSinkIntegrationTests {
    private final RedisEnterprise8Container container = RedisContainerFactory.enterprise74();

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }
}
