package com.redislabs.kafkaconnect;

import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisStandaloneContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.util.Assert;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.LinkedHashMap;
import java.util.Map;

@Testcontainers
public class AbstractRedisEnterpriseTest {

    @Container
    private static final RedisStandaloneContainer REDIS = new RedisStandaloneContainer();

    protected static String redisUri;
    protected static RedisClient redisClient;
    protected static StatefulRedisConnection<String, String> connection;
    protected static RedisCommands<String, String> syncCommands;

    @BeforeEach
    public void setupAll() {
        redisUri = REDIS.getRedisURI();
        redisClient = RedisClient.create(redisUri);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    @AfterEach
    public void cleanupEach() {
        syncCommands.flushall();
        connection.close();
        redisClient.shutdown();
        redisClient.getResources().shutdown();
    }

    public static Map<String, String> body(String... args) {
        Assert.notNull(args, "Body args cannot be null");
        Assert.isTrue(args.length % 2 == 0, "Body args length is not a multiple of 2");
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }


}