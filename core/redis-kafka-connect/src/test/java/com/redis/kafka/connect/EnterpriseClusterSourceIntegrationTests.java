package com.redis.kafka.connect;

import com.redis.testcontainers.RedisServer;

class EnterpriseClusterSourceIntegrationTests extends AbstractSourceIntegrationTests {
    private final RedisServer server = RedisContainerFactory.enterpriseCluster();
    @Override protected RedisServer getRedisServer() { return server; }
}
