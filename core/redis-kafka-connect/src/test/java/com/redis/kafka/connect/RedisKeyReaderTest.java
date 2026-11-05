package com.redis.kafka.connect;

import com.redis.kafka.connect.source.KeySnapshot;
import com.redis.kafka.connect.source.RedisKeyReader;
import com.redis.kafka.connect.source.RedisKeysSourceConfig;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class RedisKeyReaderTest {
    private RedisKeyReader reader;
    private RedisAsyncCommands<String, String> commands;
    private RedisCommands<String, String> syncCommands;
    private StatefulRedisConnection<String, String> connection;
    private RedisPubSubListener<String, String> listener;

    @BeforeEach
    void openReader() {
        reader = open(Map.of("mode", "LIVEONLY"));
        doReturn(completed(-1L)).when(commands).pexpiretime("key");
        listener.message("*", "__keyspace@0__:key", "set");
    }

    private RedisKeyReader open(Map<String, String> props) {
        RedisClient client = mock(RedisClient.class);
        connection = mock(StatefulRedisConnection.class);
        commands = mock(RedisAsyncCommands.class);
        syncCommands = mock(RedisCommands.class);
        StatefulRedisPubSubConnection<String, String> subscription = mock(StatefulRedisPubSubConnection.class);
        when(client.connect(StringCodec.UTF8)).thenReturn(connection);
        when(connection.async()).thenReturn(commands);
        when(connection.sync()).thenReturn(syncCommands);
        when(client.connectPubSub(StringCodec.UTF8)).thenReturn(subscription);
        when(subscription.sync()).thenReturn(mock(RedisPubSubCommands.class));
        RedisKeyReader opened = new RedisKeyReader(client, new RedisKeysSourceConfig(props));
        opened.open();
        ArgumentCaptor<RedisPubSubListener<String, String>> captor = ArgumentCaptor.forClass(RedisPubSubListener.class);
        verify(subscription).addListener(captor.capture());
        listener = captor.getValue();
        return opened;
    }

    @AfterEach
    void closeReader() {
        reader.close();
    }

    @Test
    void retriesDetectedTypeChanges() throws Exception {
        doReturn(completed("string"), completed("hash"),
                completed("hash"), completed("hash")).when(commands).type("key");
        doReturn(completed("stale")).when(commands).get("key");
        doReturn(completed(Map.of("field", "current"))).when(commands).hgetall("key");
        assertEquals(List.of(new KeySnapshot("key", -1L, "hash", Map.of("field", "current"))), reader.poll());
        verify(commands).get("key");
        verify(commands).hgetall("key");
    }

    @Test
    void retriesWrongTypeReplies() throws Exception {
        doReturn(completed("string"), completed("string"),
                completed("hash"), completed("hash")).when(commands).type("key");
        doReturn(failed(new RedisCommandExecutionException("WRONGTYPE key changed"))).when(commands).get("key");
        doReturn(completed(Map.of("field", "current"))).when(commands).hgetall("key");
        assertEquals("hash", reader.poll().get(0).type());
    }

    @Test
    void deletionDuringReadProducesMissingKey() throws Exception {
        doReturn(completed("string"), completed("none")).when(commands).type("key");
        doReturn(completed(null)).when(commands).get("key");
        doReturn(completed(-2L)).when(commands).pexpiretime("key");
        assertEquals(List.of(new KeySnapshot("key", -2L, "none", null)), reader.poll());
    }

    @Test
    void repeatedTypeChangesFailAfterThreeAttempts() {
        doReturn(completed("string"), completed("hash"),
                completed("string"), completed("hash"), completed("string"), completed("hash")).when(commands).type("key");
        doReturn(completed("stale")).when(commands).get("key");
        ConnectException error = assertThrows(ConnectException.class, () -> reader.poll());
        assertTrue(error.getMessage().contains("changed type repeatedly"));
        verify(commands, times(3)).get("key");
    }

    @Test
    void permissionErrorsPropagateWithoutRetry() {
        doReturn(completed("string")).when(commands).type("key");
        RedisCommandExecutionException denied = new RedisCommandExecutionException("NOPERM GET denied");
        doReturn(failed(denied)).when(commands).get("key");
        assertSame(denied, assertThrows(RedisCommandExecutionException.class, () -> reader.poll()));
        verify(commands).get("key");
    }

    @Test
    void expiryIsTheAbsoluteMillisecondExpireTime() throws Exception {
        doReturn(completed("string")).when(commands).type("key");
        doReturn(completed("value")).when(commands).get("key");
        doReturn(completed(1700000001623L)).when(commands).pexpiretime("key");
        assertEquals(1700000001623L, reader.poll().get(0).ttl());
    }

    @Test
    void notificationsAndSnapshotSharePollingCapacity() throws Exception {
        reader.close();
        reader = open(Map.of("mode", "LIVE", "batch.size", "1"));
        KeyScanCursor<String> page = new KeyScanCursor<>();
        page.getKeys().addAll(List.of("snap1", "snap2"));
        page.setCursor("0");
        page.setFinished(true);
        when(syncCommands.scan(any(ScanCursor.class), any(ScanArgs.class))).thenReturn(page);
        doReturn(completed("string")).when(commands).type(anyString());
        doReturn(completed("value")).when(commands).get(anyString());
        doReturn(completed(-1L)).when(commands).pexpiretime(anyString());
        listener.message("*", "__keyspace@0__:live1", "set");
        listener.message("*", "__keyspace@0__:live2", "set");
        assertEquals(List.of("live1"), keys(reader.poll()));
        assertEquals(List.of("snap1"), keys(reader.poll()));
        assertEquals(List.of("live2"), keys(reader.poll()));
        assertEquals(List.of("snap2"), keys(reader.poll()));
    }

    @Test
    void sustainedUpdatesCannotStarveSnapshotAtBatchSizeOne() throws Exception {
        reader.close();
        reader = open(Map.of("batch.size", "1", "redis.keys.queue.capacity", "1"));
        KeyScanCursor<String> page = new KeyScanCursor<>();
        page.getKeys().addAll(List.of("cold1", "cold2"));
        page.setCursor("0");
        page.setFinished(true);
        when(syncCommands.scan(any(ScanCursor.class), any(ScanArgs.class))).thenReturn(page);
        doReturn(completed("string")).when(commands).type(anyString());
        doReturn(completed("value")).when(commands).get(anyString());
        doReturn(completed(-1L)).when(commands).pexpiretime(anyString());
        Set<String> seen = new HashSet<>();
        for (int index = 0; index < 6; index++) {
            listener.message("*", "__keyspace@0__:hot", "set");
            List<KeySnapshot> batch = reader.poll();
            assertEquals(1, batch.size());
            seen.addAll(keys(batch));
        }
        assertEquals(Set.of("hot", "cold1", "cold2"), seen);
    }

    @Test
    void olderServersUsePttlAndTheirOwnClock() throws Exception {
        doReturn(completed("string")).when(commands).type("key");
        doReturn(completed("value")).when(commands).get("key");
        doReturn(failed(new RedisCommandExecutionException("ERR unknown command 'PEXPIRETIME'")))
                .when(commands).pexpiretime("key");
        doReturn(completed(1500L)).when(commands).pttl("key");
        doReturn(completed(List.of("1700000000", "123000"))).when(commands).time();
        assertEquals(1700000001623L, reader.poll().get(0).ttl());
        listener.message("*", "__keyspace@0__:key", "set");
        assertEquals(1700000001623L, reader.poll().get(0).ttl());
        verify(commands, times(1)).pexpiretime("key");
    }

    @Test
    void olderServersRetryTypeChangesWhenExpiryIsUnsupported() throws Exception {
        doReturn(completed("string"), completed("hash"), completed("hash"), completed("hash"),
                completed("hash")).when(commands).type("key");
        doReturn(failed(new RedisCommandExecutionException("WRONGTYPE key changed"))).when(commands).get("key");
        doReturn(completed(Map.of("field", "current"))).when(commands).hgetall("key");
        doReturn(failed(new RedisCommandExecutionException("ERR unknown command 'PEXPIRETIME'")))
                .when(commands).pexpiretime("key");
        doReturn(completed(-1L)).when(commands).pttl("key");
        doReturn(completed(List.of("1700000000", "123000"))).when(commands).time();
        assertEquals(List.of(new KeySnapshot("key", -1L, "hash", Map.of("field", "current"))), reader.poll());
        verify(commands, times(1)).pexpiretime("key");
    }

    @Test
    void expiryPermissionErrorsDoNotActivateCompatibilityFallback() {
        doReturn(completed("string")).when(commands).type("key");
        doReturn(completed("value")).when(commands).get("key");
        RedisCommandExecutionException denied = new RedisCommandExecutionException("NOPERM PEXPIRETIME denied");
        doReturn(failed(denied)).when(commands).pexpiretime("key");
        assertSame(denied, assertThrows(RedisCommandExecutionException.class, () -> reader.poll()));
        verify(commands, never()).pttl(anyString());
        verify(commands, never()).time();
    }

    private List<String> keys(List<KeySnapshot> snapshots) {
        return snapshots.stream().map(KeySnapshot::key).toList();
    }

    private <T> RedisFuture<T> completed(T value) {
        RedisFuture<T> future = mock(RedisFuture.class);
        when(future.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(value));
        return future;
    }

    private <T> RedisFuture<T> failed(Throwable error) {
        RedisFuture<T> future = mock(RedisFuture.class);
        when(future.toCompletableFuture()).thenReturn(CompletableFuture.failedFuture(error));
        return future;
    }
}
