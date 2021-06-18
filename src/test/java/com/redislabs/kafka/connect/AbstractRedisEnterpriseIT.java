package com.redislabs.kafka.connect;

import com.redislabs.testcontainers.RedisEnterpriseContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisListCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.util.Assert;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
@Testcontainers
public class AbstractRedisEnterpriseIT {

    @Container
    private static final RedisEnterpriseContainer REDIS_ENTERPRISE = new RedisEnterpriseContainer();

    static Stream<RedisServer> redisServers() {
        return Stream.of(REDIS_ENTERPRISE);
    }

    protected Map<RedisServer, AbstractRedisClient> clients = new HashMap<>();
    protected Map<RedisServer, StatefulConnection<String, String>> connections = new HashMap<>();
    protected Map<RedisServer, BaseRedisCommands<String, String>> syncs = new HashMap<>();
    protected Map<RedisServer, BaseRedisAsyncCommands<String, String>> asyncs = new HashMap<>();
    protected Map<RedisServer, BaseRedisReactiveCommands<String, String>> reactives = new HashMap<>();

    @BeforeEach
    public void setupEach() {
        for (RedisServer redis : redisServers().collect(Collectors.toList())) {
            if (redis.isCluster()) {
                RedisClusterClient client = RedisClusterClient.create(redis.getRedisURI());
                clients.put(redis, client);
                StatefulRedisClusterConnection<String, String> connection = client.connect();
                connections.put(redis, connection);
                syncs.put(redis, connection.sync());
                asyncs.put(redis, connection.async());
                reactives.put(redis, connection.reactive());
            } else {
                RedisClient client = RedisClient.create(redis.getRedisURI());
                clients.put(redis, client);
                StatefulRedisConnection<String, String> connection = client.connect();
                connections.put(redis, connection);
                syncs.put(redis, connection.sync());
                asyncs.put(redis, connection.async());
                reactives.put(redis, connection.reactive());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @AfterEach
    public void cleanupEach() {
        for (BaseRedisCommands<String, String> sync : syncs.values()) {
            ((RedisServerCommands<String, String>) sync).flushall();
        }
        for (StatefulConnection<String, String> connection : connections.values()) {
            connection.close();
        }
        for (AbstractRedisClient client : clients.values()) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    protected BaseRedisCommands<String, String> sync(RedisServer redis) {
        return syncs.get(redis);
    }

    protected RedisStringCommands<String, String> syncString(RedisServer redis) {
        return ((RedisStringCommands<String, String>) sync(redis));
    }

    protected RedisHashCommands<String, String> syncHash(RedisServer redis) {
        return ((RedisHashCommands<String, String>) sync(redis));
    }

    protected RedisListCommands<String, String> syncList(RedisServer redis) {
        return ((RedisListCommands<String, String>) sync(redis));
    }

    protected RedisStreamCommands<String, String> syncStream(RedisServer redis) {
        return ((RedisStreamCommands<String, String>) sync(redis));
    }

    protected RedisSetCommands<String, String> syncSet(RedisServer redis) {
        return ((RedisSetCommands<String, String>) sync(redis));
    }

    protected RedisKeyCommands<String, String> syncKey(RedisServer redis) {
        return ((RedisKeyCommands<String, String>) sync(redis));
    }

    protected RedisServerCommands<String, String> syncServer(RedisServer redis) {
        return ((RedisServerCommands<String, String>) sync(redis));
    }

    protected RedisSortedSetCommands<String, String> syncSortedSet(RedisServer redis) {
        return ((RedisSortedSetCommands<String, String>) sync(redis));
    }

    protected BaseRedisAsyncCommands<String, String> async(RedisServer redis) {
        return asyncs.get(redis);
    }

    protected BaseRedisReactiveCommands<String, String> reactive(RedisServer redis) {
        return reactives.get(redis);
    }

    protected Map<String, String> map(String... args) {
        Assert.notNull(args, "Args cannot be null");
        Assert.isTrue(args.length % 2 == 0, "Args length is not a multiple of 2");
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }


}
