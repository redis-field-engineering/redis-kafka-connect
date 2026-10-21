package com.redis.kafka.connect;

import com.redis.kafka.connect.source.KeySnapshot;
import com.redis.kafka.connect.source.RedisKeyReader;
import com.redis.kafka.connect.source.RedisKeysSourceConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import java.util.List;
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
    private StatefulRedisConnection<String, String> connection;

    @BeforeEach
    void openReader() {
        RedisClient client = mock(RedisClient.class);
        connection = mock(StatefulRedisConnection.class);
        commands = mock(RedisAsyncCommands.class);
        StatefulRedisPubSubConnection<String, String> subscription = mock(StatefulRedisPubSubConnection.class);
        when(client.connect(StringCodec.UTF8)).thenReturn(connection);
        when(connection.async()).thenReturn(commands);
        when(client.connectPubSub(StringCodec.UTF8)).thenReturn(subscription);
        when(subscription.sync()).thenReturn(mock(RedisPubSubCommands.class));
        doReturn(completed(-1L)).when(commands).pttl("key");
        doReturn(completed(List.of("1700000000", "123000"))).when(commands).time();
        reader = new RedisKeyReader(client, new RedisKeysSourceConfig(Map.of("mode", "LIVEONLY")));
        reader.open();
        ArgumentCaptor<RedisPubSubListener<String, String>> listener = ArgumentCaptor.forClass(RedisPubSubListener.class);
        verify(subscription).addListener(listener.capture());
        listener.getValue().message("*", "__keyspace@0__:key", "set");
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
        doReturn(completed(-2L)).when(commands).pttl("key");
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
    void expiryUsesServerClockAndMillisecondTtl() throws Exception {
        doReturn(completed("string")).when(commands).type("key");
        doReturn(completed("value")).when(commands).get("key");
        doReturn(completed(1500L)).when(commands).pttl("key");
        assertEquals(1700000001623L, reader.poll().get(0).ttl());
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
