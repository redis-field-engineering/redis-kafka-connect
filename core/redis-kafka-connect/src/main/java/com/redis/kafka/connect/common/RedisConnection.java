package com.redis.kafka.connect.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.RedisCodec;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Task-owned connection with a common command boundary for standalone and cluster clients.
 *
 * @param <K> decoded key type
 * @param <V> decoded value type
 */
public final class RedisConnection<K, V> implements AutoCloseable {
    private final StatefulConnection<K, V> connection;
    private final Object sync;
    private final BaseRedisAsyncCommands<K, V> async;
    private final boolean owned;

    private RedisConnection(StatefulRedisConnection<K, V> connection, boolean owned) {
        this.connection = connection;
        this.sync = connection.sync();
        this.async = connection.async();
        this.owned = owned;
    }

    private RedisConnection(StatefulRedisClusterConnection<K, V> connection) {
        this.connection = connection;
        this.sync = connection.sync();
        this.async = connection.async();
        this.owned = true;
    }

    /**
     * Opens a connection owned by the returned wrapper.
     *
     * @param <K> decoded key type
     * @param <V> decoded value type
     * @param client standalone or cluster client
     * @param codec key and value codec
     * @return an open connection; the caller must close it
     */
    public static <K, V> RedisConnection<K, V> open(AbstractRedisClient client, RedisCodec<K, V> codec) {
        if (client instanceof RedisClusterClient cluster) {
            return new RedisConnection<>(cluster.connect(codec));
        }
        return new RedisConnection<>(((RedisClient) client).connect(codec), true);
    }

    /**
     * Returns the synchronous command interface requested by the caller.
     *
     * @param <T> command interface type
     * @param commands command interface class supported by the underlying connection
     * @return the typed command interface
     */
    public <T> T sync(Class<T> commands) {
        return commands.cast(sync);
    }

    /**
     * Returns the asynchronous command interface requested by the caller.
     *
     * @param <T> command interface type
     * @param commands command interface class supported by the underlying connection
     * @return the typed command interface
     */
    public <T> T async(Class<T> commands) {
        return commands.cast(async);
    }

    /**
     * Returns the asynchronous command interface shared by standalone and cluster connections.
     *
     * @return asynchronous commands
     */
    public BaseRedisAsyncCommands<K, V> async() {
        return async;
    }

    /**
     * Exposes the underlying connection for connection-level operations.
     *
     * @return the underlying stateful connection
     */
    public StatefulConnection<K, V> stateful() {
        return connection;
    }

    private String nodeIdForSlot(int slot) {
        if (connection instanceof StatefulRedisClusterConnection<?, ?> cluster) {
            RedisClusterNode node = cluster.getPartitions().getPartitionBySlot(slot);
            if (node == null) {
                throw new RedisConnectionException("No Redis primary is currently known for slot " + slot);
            }
            return node.getNodeId();
        }
        return "standalone";
    }

    /**
     * Selects the connection for the primary owning a cluster slot.
     *
     * @param slot Redis Cluster hash slot
     * @return a borrowed node connection, or this connection for standalone Redis
     */
    @SuppressWarnings("unchecked")
    public RedisConnection<K, V> nodeForSlot(int slot) {
        if (connection instanceof StatefulRedisClusterConnection<?, ?>) {
            StatefulRedisClusterConnection<K, V> cluster = (StatefulRedisClusterConnection<K, V>) connection;
            String node = nodeIdForSlot(slot);
            return new RedisConnection<>(cluster.getConnection(node), false);
        }
        return this;
    }

    /**
     * Waits for the entire pipeline and propagates failures from command futures.
     *
     * @param timeout maximum time to wait for the batch
     * @param futures command futures to await
     */
    public static void awaitAll(Duration timeout, List<? extends RedisFuture<?>> futures) {
        CompletableFuture<?>[] all = futures.stream().map(RedisFuture::toCompletableFuture)
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(all).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new RedisCommandTimeoutException("Redis batch timed out after " + timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RedisCommandExecutionException("Interrupted while waiting for Redis commands", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new RedisCommandExecutionException("Redis batch failed", cause);
        }
    }

    @Override
    public void close() {
        if (owned) {
            connection.close();
        }
    }
}
