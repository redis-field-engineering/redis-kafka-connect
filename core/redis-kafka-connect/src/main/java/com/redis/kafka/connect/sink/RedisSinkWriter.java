package com.redis.kafka.connect.sink;

import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisTransactionalCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

final class RedisSinkWriter implements AutoCloseable {
    private final GenericObjectPool<RedisConnection<byte[], byte[]>> pool;
    private final RedisSinkConfig config;
    private final Duration timeout;
    private final boolean cluster;

    RedisSinkWriter(AbstractRedisClient client, RedisSinkConfig config) {
        this.config = config;
        this.timeout = config.uri().getTimeout();
        this.cluster = client instanceof RedisClusterClient;
        GenericObjectPoolConfig<RedisConnection<byte[], byte[]>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(config.getPoolSize());
        poolConfig.setMaxWait(timeout);
        this.pool = new GenericObjectPool<>(new BasePooledObjectFactory<>() {
            @Override
            public RedisConnection<byte[], byte[]> create() {
                return RedisConnection.open(client, ByteArrayCodec.INSTANCE);
            }

            @Override
            public PooledObject<RedisConnection<byte[], byte[]>> wrap(RedisConnection<byte[], byte[]> connection) {
                return new DefaultPooledObject<>(connection);
            }

            @Override
            public void destroyObject(PooledObject<RedisConnection<byte[], byte[]>> connection) {
                connection.getObject().close();
            }
        }, poolConfig);
    }

    void write(List<RedisWrite> writes) throws Exception {
        if (writes.isEmpty()) {
            return;
        }
        RedisConnection<byte[], byte[]> connection = pool.borrowObject();
        boolean success = false;
        try {
            // Pipeline by primary, or by slot when using transactions. Keep WAIT on the connection that wrote.
            Map<String, List<RedisWrite>> groups = new LinkedHashMap<>();
            for (RedisWrite write : writes) {
                int slot = cluster ? SlotHash.getSlot(write.key()) : 0;
                String group = cluster && config.isMultiExec() ? "slot:" + slot : connection.nodeIdForSlot(slot);
                groups.computeIfAbsent(group, ignored -> new ArrayList<>()).add(write);
            }
            for (List<RedisWrite> group : groups.values()) {
                int slot = cluster ? SlotHash.getSlot(group.get(0).key()) : 0;
                pipeline(connection.nodeForSlot(slot), group);
            }
            success = true;
        } finally {
            if (success) {
                pool.returnObject(connection);
            } else {
                // Never reuse a timed-out connection or a connection with an uncertain transaction state.
                pool.invalidateObject(connection);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void pipeline(RedisConnection<byte[], byte[]> connection, List<RedisWrite> writes) {
        RedisTransactionalCommands<byte[], byte[]> transactions = connection.sync(RedisTransactionalCommands.class);
        boolean transaction = config.isMultiExec();
        if (transaction) {
            transactions.multi();
        }
        List<RedisFuture<?>> futures = new ArrayList<>();
        connection.stateful().setAutoFlushCommands(false);
        try {
            RedisKeyAsyncCommands<byte[], byte[]> keys = connection.async(RedisKeyAsyncCommands.class);
            for (RedisWrite write : writes) {
                futures.add(write.enqueue(connection));
                if (write.ttl() > 0) {
                    futures.add(keys.expire(write.key(), write.ttl()));
                }
            }
            if (transaction) {
                // EXEC must be dispatched before waiting for queued command futures.
                futures.add(connection.async(RedisTransactionalAsyncCommands.class).exec());
            }
            connection.stateful().flushCommands();
            RedisConnection.awaitAll(timeout, futures);
            for (RedisFuture<?> future : futures) {
                Object result = future.toCompletableFuture().getNow(null);
                if (result instanceof TransactionResult exec) {
                    if (exec.wasDiscarded()) {
                        throw new RedisCommandExecutionException("Redis transaction was discarded");
                    }
                    for (Object value : exec) {
                        if (value instanceof Throwable error) {
                            throw new RedisCommandExecutionException("Redis transaction command failed", error);
                        }
                    }
                }
            }
        } finally {
            connection.stateful().setAutoFlushCommands(true);
        }
        if (config.getWaitReplicas() > 0) {
            BaseRedisCommands<byte[], byte[]> server = connection.sync(BaseRedisCommands.class);
            long replicated = server.waitForReplication(config.getWaitReplicas(), config.getWaitTimeout().toMillis());
            if (replicated < config.getWaitReplicas()) {
                throw new RedisConnectionException("Redis replication acknowledgment target was not reached");
            }
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
