package com.redis.kafka.connect.source;

import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateAdapter;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisJsonAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;

/** Incremental snapshot and best-effort keyspace notification reader. No I/O runs on Pub/Sub callbacks. */
public final class RedisKeyReader implements AutoCloseable {
    private final AbstractRedisClient client;
    private final RedisKeysSourceConfig config;
    private final ArrayBlockingQueue<String> changes;
    private final RedisConnectionStateAdapter connectionListener;
    private final RedisPubSubAdapter<String, String> notificationListener;
    private RedisConnection<String, String> data;
    private StatefulRedisPubSubConnection<String, String> subscription;
    private ScanCursor cursor = ScanCursor.INITIAL;
    private Iterator<String> snapshotKeys = Collections.emptyIterator();
    private boolean snapshotComplete;
    private volatile boolean closed;
    private volatile ConnectException failure;
    private Set<String> subscribedNodes = Set.of();
    private final String channelPrefix;

    public RedisKeyReader(AbstractRedisClient client, RedisKeysSourceConfig config) {
        this.client = client;
        this.config = config;
        changes = new ArrayBlockingQueue<>(config.getInt(RedisKeysSourceConfigDef.QUEUE_CAPACITY_CONFIG));
        snapshotComplete = config.getMode() == RedisKeysSourceConfig.ReaderMode.LIVEONLY;
        channelPrefix = "__keyspace@" + config.uri().getDatabase() + "__:";
        notificationListener = new RedisPubSubAdapter<>() {
            @Override
            public void message(String pattern, String channel, String message) {
                if (!closed && channel.startsWith(channelPrefix)
                        && !changes.offer(channel.substring(channelPrefix.length()))) {
                    failure = new ConnectException("Redis key notification queue overflowed; updates may have been missed. Restart the task to take a new LIVE snapshot, or increase redis.keys.queue.capacity.");
                }
            }
        };
        connectionListener = new RedisConnectionStateAdapter() {
            @Override
            public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
                if (connection instanceof StatefulRedisPubSubConnection<?, ?>) {
                    disconnected();
                }
            }

            private void disconnected() {
                if (!closed) {
                    failure = new ConnectException("Redis key notification connection was interrupted; updates may have been missed. Restart the task to resubscribe and take a new LIVE snapshot.");
                }
            }
        };
    }

    public void open() {
        client.addListener(connectionListener);
        try {
            data = RedisConnection.open(client, StringCodec.UTF8);
            String pattern = channelPrefix + config.getKeyPattern();
            if (client instanceof RedisClusterClient cluster) {
                StatefulRedisClusterPubSubConnection<String, String> pubsub = cluster.connectPubSub(StringCodec.UTF8);
                subscription = pubsub;
                subscribedNodes = primaryNodes(cluster);
                for (String node : subscribedNodes) {
                    StatefulRedisPubSubConnection<String, String> nodeConnection = pubsub.getConnection(node);
                    nodeConnection.addListener(notificationListener);
                    nodeConnection.sync().psubscribe(pattern);
                }
            } else {
                subscription = ((RedisClient) client).connectPubSub(StringCodec.UTF8);
                subscription.addListener(notificationListener);
                subscription.sync().psubscribe(pattern);
            }
            // PSUBSCRIBE acknowledgments are complete before the first SCAN is issued.
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private Set<String> primaryNodes(RedisClusterClient cluster) {
        return cluster.getPartitions().stream().filter(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM))
                .map(RedisClusterNode::getNodeId).collect(Collectors.toSet());
    }

    private record PendingKey(String key, boolean snapshot) {}

    public List<KeySnapshot> poll() throws InterruptedException {
        if (closed) {
            return List.of();
        }
        checkFailure();
        int max = Math.toIntExact(config.getBatchSize());
        List<PendingKey> pending = new ArrayList<>();
        // Share each poll between snapshot and live traffic to keep the notification queue moving.
        int snapshotLimit = Math.max(1, max / 2);
        while (!snapshotComplete && pending.size() < snapshotLimit && !closed) {
            if (!snapshotKeys.hasNext()) {
                scan();
                if (!snapshotKeys.hasNext()) {
                    break;
                }
            }
            pending.add(new PendingKey(snapshotKeys.next(), true));
        }
        while (pending.size() < max && !closed) {
            String key = changes.poll();
            if (key == null) {
                break;
            }
            pending.add(new PendingKey(key, false));
        }
        if (pending.isEmpty() && snapshotComplete && !closed) {
            long waitMillis = config.getIdleTimeout().isZero() ? 100 : config.getIdleTimeout().toMillis();
            String key = changes.poll(waitMillis, TimeUnit.MILLISECONDS);
            if (closed) {
                return List.of();
            }
            if (key != null) {
                pending.add(new PendingKey(key, false));
            }
        }
        List<KeySnapshot> result = readBatch(pending);
        checkFailure();
        return result;
    }

    private record NativeRead(String type, RedisFuture<?> value, RedisFuture<Long> ttl,
            RedisFuture<List<String>> time, RedisFuture<String> finalType) {}

    private List<KeySnapshot> readBatch(List<PendingKey> pending) {
        if (pending.isEmpty() || closed) {
            return List.of();
        }
        List<KeySnapshot> values = new ArrayList<>(Collections.nCopies(pending.size(), null));
        List<Integer> remaining = IntStream.range(0, pending.size()).boxed().toList();
        for (int attempt = 0; attempt < 3 && !remaining.isEmpty(); attempt++) {
            List<PendingKey> page = remaining.stream().map(pending::get).toList();
            List<NativeRead> reads = readValues(page, readTypes(page));
            List<Integer> retry = new ArrayList<>();
            for (int index = 0; index < reads.size(); index++) {
                int position = remaining.get(index);
                KeySnapshot value = decode(page.get(index).key(), reads.get(index));
                if (value == null) {
                    retry.add(position);
                } else {
                    values.set(position, value);
                }
            }
            remaining = retry;
        }
        if (!remaining.isEmpty()) {
            throw new ConnectException("Redis keys changed type repeatedly during reads; restart the task to resnapshot");
        }
        List<KeySnapshot> result = new ArrayList<>();
        for (int index = 0; index < pending.size(); index++) {
            KeySnapshot value = values.get(index);
            if (!pending.get(index).snapshot() || !"none".equals(value.type())) {
                result.add(value);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<String> readTypes(List<PendingKey> pending) {
        RedisKeyAsyncCommands<String, String> keys = data.async(RedisKeyAsyncCommands.class);
        List<RedisFuture<String>> futures = new ArrayList<>();
        // Issue the asynchronous batch before awaiting replies. Keep automatic flushing
        // enabled so Lettuce owns dispatch to existing and lazily opened cluster connections.
        pending.forEach(key -> futures.add(keys.type(key.key())));
        RedisConnection.awaitAll(config.uri().getTimeout(), futures);
        return futures.stream().map(future -> future.toCompletableFuture().getNow(null)).toList();
    }

    @SuppressWarnings("unchecked")
    private List<NativeRead> readValues(List<PendingKey> pending, List<String> types) {
        RedisKeyAsyncCommands<String, String> keys = data.async(RedisKeyAsyncCommands.class);
        List<RedisFuture<?>> futures = new ArrayList<>();
        List<NativeRead> reads = new ArrayList<>();
        for (int index = 0; index < pending.size(); index++) {
            String key = pending.get(index).key();
            RedisServerAsyncCommands<String, String> server = data.nodeForSlot(SlotHash.getSlot(key))
                    .async(RedisServerAsyncCommands.class);
            NativeRead read = new NativeRead(types.get(index), readValue(key, types.get(index)),
                    keys.pttl(key), server.time(), keys.type(key));
            reads.add(read);
            futures.addAll(List.of(read.value(), read.ttl(), read.time(), read.finalType()));
        }
        try {
            RedisConnection.awaitAll(config.uri().getTimeout(), futures);
        } catch (RedisCommandExecutionException e) {
            if (!wrongType(e)) {
                throw e;
            }
            // Every future has completed. Decode each reply so other errors still propagate.
        }
        return reads;
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<?> readValue(String key, String type) {
        return switch (type) {
            case KeySnapshot.TYPE_STRING -> data.async(RedisStringAsyncCommands.class).get(key);
            case KeySnapshot.TYPE_HASH -> data.async(RedisHashAsyncCommands.class).hgetall(key);
            case KeySnapshot.TYPE_LIST -> data.async(RedisListAsyncCommands.class).lrange(key, 0, -1);
            case KeySnapshot.TYPE_SET -> data.async(RedisSetAsyncCommands.class).smembers(key);
            case KeySnapshot.TYPE_ZSET -> data.async(RedisSortedSetAsyncCommands.class).zrangeWithScores(key, 0, -1);
            case KeySnapshot.TYPE_JSON -> data.async(RedisJsonAsyncCommands.class).jsonGetRaw(key);
            // Unsupported types retain metadata without a value in the published schema.
            default -> data.async(RedisKeyAsyncCommands.class).type(key);
        };
    }

    private boolean wrongType(Throwable error) {
        return error instanceof RedisCommandExecutionException && error.getMessage() != null
                && error.getMessage().startsWith("WRONGTYPE");
    }

    private KeySnapshot decode(String key, NativeRead read) {
        long ttl = read.ttl().toCompletableFuture().getNow(null);
        List<String> time = read.time().toCompletableFuture().getNow(null);
        String finalType = read.finalType().toCompletableFuture().getNow(null);
        Object value;
        try {
            value = read.value().toCompletableFuture().getNow(null);
        } catch (CompletionException e) {
            if (wrongType(e.getCause())) {
                return null;
            }
            throw e;
        }
        if ("none".equals(finalType) && ttl == -2) {
            return new KeySnapshot(key, -2L, "none", null);
        }
        if ("none".equals(finalType) || !read.type().equals(finalType) || ttl == -2) {
            return null;
        }
        switch (read.type()) {
            case KeySnapshot.TYPE_JSON -> {
                List<?> json = (List<?>) value;
                if (json == null || json.isEmpty()) {
                    return null;
                }
                value = json.get(0);
            }
            case KeySnapshot.TYPE_STRING -> {
                if (value == null) {
                    return null;
                }
            }
            case KeySnapshot.TYPE_HASH -> {
                if (((Map<?, ?>) value).isEmpty()) {
                    return null;
                }
            }
            case KeySnapshot.TYPE_LIST, KeySnapshot.TYPE_SET, KeySnapshot.TYPE_ZSET -> {
                if (((Collection<?>) value).isEmpty()) {
                    return null;
                }
            }
            default -> value = null;
        }
        long expiry = ttl < 0 ? ttl : Long.parseLong(time.get(0)) * 1000
                + Long.parseLong(time.get(1)) / 1000 + ttl;
        return new KeySnapshot(key, expiry, read.type(), value);
    }

    @SuppressWarnings("unchecked")
    private void scan() {
        if (cursor.isFinished()) {
            snapshotComplete = true;
            return;
        }
        RedisKeyCommands<String, String> keys = data.sync(RedisKeyCommands.class);
        KeyScanCursor<String> next = keys.scan(cursor,
                ScanArgs.Builder.matches(config.getKeyPattern()).limit(config.getBatchSize()));
        cursor = next;
        snapshotKeys = next.getKeys().iterator();
        // Drain the final page before marking the snapshot complete.
    }

    private void checkFailure() {
        if (failure != null) {
            throw failure;
        }
        if (client instanceof RedisClusterClient cluster && !primaryNodes(cluster).equals(subscribedNodes)) {
            throw new ConnectException("Redis cluster primaries changed; restart the keys source to rebuild notification subscriptions and its snapshot");
        }
    }

    @Override
    public void close() {
        closed = true;
        client.removeListener(connectionListener);
        try {
            if (subscription != null) {
                subscription.close();
                subscription = null;
            }
        } finally {
            if (data != null) {
                data.close();
                data = null;
            }
            changes.clear();
            changes.offer(""); // Wake a blocked poll; closed is checked before decoding the key.
        }
    }
}
