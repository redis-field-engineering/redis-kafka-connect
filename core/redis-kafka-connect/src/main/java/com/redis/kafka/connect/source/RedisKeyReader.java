package com.redis.kafka.connect.source;

import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateAdapter;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisScriptingCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;

/** Incremental snapshot and best-effort keyspace notification reader. No I/O runs on Pub/Sub callbacks. */
public final class RedisKeyReader implements AutoCloseable {
    // A single server-side read prevents TYPE/value/expiry races during concurrent key replacement.
    private static final String READ_KEY = """
            local name = KEYS[1]
            local kind = redis.call('TYPE', name).ok
            local expiry = redis.call('PTTL', name)
            if expiry >= 0 then
              local clock = redis.call('TIME')
              expiry = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000) + expiry
            end
            local data = false
            if kind == 'string' then data = redis.call('GET', name)
            elseif kind == 'hash' then data = redis.call('HGETALL', name)
            elseif kind == 'list' then data = redis.call('LRANGE', name, 0, -1)
            elseif kind == 'set' then data = redis.call('SMEMBERS', name)
            elseif kind == 'zset' then data = redis.call('ZRANGE', name, 0, -1, 'WITHSCORES')
            elseif kind == 'ReJSON-RL' then data = redis.call('JSON.GET', name)
            end
            return {expiry, kind, data}
            """;

    private final AbstractRedisClient client;
    private final RedisKeysSourceConfig config;
    private final ArrayBlockingQueue<String> changes;
    private final RedisConnectionStateAdapter connectionListener;
    private final RedisPubSubAdapter<String, String> notificationListener;
    private RedisConnection<String, String> data;
    private StatefulRedisPubSubConnection<String, String> subscription;
    private String scriptDigest;
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
            loadScript();
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

    @SuppressWarnings("unchecked")
    private void loadScript() {
        byte[] script = READ_KEY.getBytes(StandardCharsets.UTF_8);
        if (client instanceof RedisClusterClient cluster) {
            Set<String> primaries = primaryNodes(cluster);
            if (primaries.isEmpty()) {
                throw new ConnectException("Redis cluster has no primaries for keys-source script loading");
            }
            StatefulRedisClusterConnection<String, String> connection =
                    (StatefulRedisClusterConnection<String, String>) data.stateful();
            // Reads route to primaries; a replica outage must not prevent loading or reloading.
            for (String node : primaries) {
                scriptDigest = connection.getConnection(node).sync().scriptLoad(script);
            }
            return;
        }
        RedisScriptingCommands<String, String> scripts = data.sync(RedisScriptingCommands.class);
        scriptDigest = scripts.scriptLoad(script);
    }

    private List<KeySnapshot> readBatch(List<PendingKey> pending) {
        if (pending.isEmpty() || closed) {
            return List.of();
        }
        List<RedisFuture<List<Object>>> values;
        try {
            values = readValues(pending);
        } catch (RedisNoScriptException e) {
            // SCRIPT FLUSH and server restart invalidate caches. These read-only operations may be replayed.
            loadScript();
            values = readValues(pending);
        }
        List<KeySnapshot> result = new ArrayList<>();
        for (int index = 0; index < pending.size(); index++) {
            PendingKey key = pending.get(index);
            KeySnapshot snapshot = decode(key.key(), values.get(index).toCompletableFuture().getNow(null));
            if (!key.snapshot() || !"none".equals(snapshot.type())) {
                result.add(snapshot);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<RedisFuture<List<Object>>> readValues(List<PendingKey> pending) {
        RedisConnection<String, String> connection = data;
        RedisScriptingAsyncCommands<String, String> scripts = connection.async(RedisScriptingAsyncCommands.class);
        List<RedisFuture<List<Object>>> futures = new ArrayList<>();
        connection.stateful().setAutoFlushCommands(false);
        try {
            for (PendingKey key : pending) {
                futures.add(scripts.evalsha(scriptDigest, ScriptOutputType.MULTI, new String[] {key.key()}));
            }
            connection.stateful().flushCommands();
            RedisConnection.awaitAll(config.uri().getTimeout(), futures);
            return futures;
        } finally {
            connection.stateful().setAutoFlushCommands(true);
        }
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

    @SuppressWarnings("unchecked")
    private KeySnapshot decode(String key, List<Object> fields) {
        long ttl = ((Number) fields.get(0)).longValue();
        String type = (String) fields.get(1);
        Object value = fields.get(2);
        if (KeySnapshot.TYPE_HASH.equals(type)) {
            List<String> flat = (List<String>) value;
            Map<String, String> map = new LinkedHashMap<>();
            for (int index = 0; index < flat.size(); index += 2) {
                map.put(flat.get(index), flat.get(index + 1));
            }
            value = map;
        } else if (KeySnapshot.TYPE_ZSET.equals(type)) {
            List<String> flat = (List<String>) value;
            List<ScoredValue<String>> values = new ArrayList<>();
            for (int index = 0; index < flat.size(); index += 2) {
                values.add(ScoredValue.just(Double.parseDouble(flat.get(index + 1)), flat.get(index)));
            }
            value = values;
        }
        return new KeySnapshot(key, ttl, type, value);
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
