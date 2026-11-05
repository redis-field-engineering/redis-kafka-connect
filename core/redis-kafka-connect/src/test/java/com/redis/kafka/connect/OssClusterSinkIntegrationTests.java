package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;

class OssClusterSinkIntegrationTests extends AbstractSinkIntegrationTests {
    private final RedisServer server = new RedisOssClusterContainer();
    @Override protected RedisServer getRedisServer() { return server; }
}
