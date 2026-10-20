package com.redis.kafka.connect;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisContainer;
import org.testcontainers.utility.DockerImageName;

public interface RedisContainerFactory {

	String ENTERPRISE_IMAGE = System.getProperty("redis.enterprise.image", "redislabs/redis:8.2.0-125.19");
	String REDIS_IMAGE = System.getProperty("redis.image", "redis:8.2.10");

	static RedisContainer redis() {
		return new RedisContainer(DockerImageName.parse(REDIS_IMAGE));
	}

	@SuppressWarnings("resource")
	static RedisEnterprise8Container enterprise() {
		return new RedisEnterprise8Container(DockerImageName.parse(ENTERPRISE_IMAGE),
				Database.builder().name("ConnectorTests").port(12000).memoryMB(100).ossCluster(true)
						.modules(RedisModule.TIMESERIES, RedisModule.JSON, RedisModule.SEARCH).build());
	}

	static RedisEnterpriseServer enterpriseServer() {
		RedisEnterpriseServer server = new RedisEnterpriseServer();
		server.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
				.modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());
		return server;
	}

}
