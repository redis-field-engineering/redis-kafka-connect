package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;

class OssClusterSourceIntegrationTests extends AbstractSourceIntegrationTests {
    private final RedisServer server = new RedisOssClusterContainer();
    @Override protected RedisServer getRedisServer() { return server; }
}
