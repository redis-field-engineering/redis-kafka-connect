package com.redis.kafka.connect;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractIntegrationTests {

	protected RedisURI redisURI;
	protected AbstractRedisClient client;
	protected StatefulRedisModulesConnection<String, String> connection;

	@BeforeAll
	void setup() {
		RedisServer server = getRedisServer();
		server.start();
		redisURI = RedisURI.create(server.getRedisURI());
		client = ClientBuilder.create(redisURI).cluster(server.isCluster()).build();
		connection = RedisModulesUtils.connection(client);
	}

	@BeforeEach
	void flushAll() {
		connection.sync().flushall();
	}

	@AfterAll
	void teardown() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
		getRedisServer().stop();
	}

	protected abstract RedisServer getRedisServer();

	protected void enableKeyspaceNotifications() {
		connection.sync().configSet("notify-keyspace-events", "AK");
	}

}
