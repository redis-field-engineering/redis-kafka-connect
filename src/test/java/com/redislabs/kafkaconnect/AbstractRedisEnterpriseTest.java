package com.redislabs.kafkaconnect;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.springframework.util.Assert;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class AbstractRedisEnterpriseTest {

    @Container
    private static final RedisContainer REDIS = new RedisContainer();

    protected static String redisUri;
    protected static RedisClient redisClient;
    protected static StatefulRedisConnection<String, String> connection;
    protected static RedisCommands<String, String> syncCommands;

    @BeforeAll
    static void setupAll() {
        redisUri = REDIS.getUri();
        redisClient = RedisClient.create(redisUri);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    @AfterEach
    public void cleanupEach() {
        syncCommands.flushall();
    }

    @AfterAll
    static void cleanupAll() {
        connection.close();
        redisClient.shutdown();
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