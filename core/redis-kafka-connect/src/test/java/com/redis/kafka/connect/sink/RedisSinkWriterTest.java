package com.redis.kafka.connect.sink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class RedisSinkWriterTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void timeoutAfterPartialCompletionInvalidatesConnectionAndRetryUsesAnother(boolean transaction) throws Exception {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> interrupted = mock(StatefulRedisConnection.class);
        StatefulRedisConnection<byte[], byte[]> replacement = mock(StatefulRedisConnection.class);
        RedisAsyncCommands<byte[], byte[]> interruptedCommands = mock(RedisAsyncCommands.class);
        RedisAsyncCommands<byte[], byte[]> replacementCommands = mock(RedisAsyncCommands.class);
        when(client.connect(ByteArrayCodec.INSTANCE)).thenReturn(interrupted, replacement);
        when(interrupted.sync()).thenReturn(mock(RedisCommands.class));
        when(interrupted.async()).thenReturn(interruptedCommands);
        when(replacement.sync()).thenReturn(mock(RedisCommands.class));
        when(replacement.async()).thenReturn(replacementCommands);
        CompletableFuture<Long> firstReply = CompletableFuture.completedFuture(1L);
        CompletableFuture<Long> lostReply = new CompletableFuture<>();
        RedisFuture<Long> first = future(firstReply);
        RedisFuture<Long> lost = future(lostReply);
        RedisFuture<Long> replay = future(CompletableFuture.completedFuture(2L));
        RedisFuture<TransactionResult> exec = future(CompletableFuture.completedFuture(null));
        when(interruptedCommands.<Long>dispatch(eq(CommandType.RPUSH), any(), any())).thenReturn(first, lost);
        // One command has completed while the next command's reply never arrives.
        when(replacementCommands.<Long>dispatch(eq(CommandType.RPUSH), any(), any()))
                .thenReturn(replay);
        when(interruptedCommands.exec()).thenReturn(exec);
        when(replacementCommands.exec()).thenReturn(exec);
        RedisSinkConfig config = new RedisSinkConfig(Map.of("redis.type", "LIST", "redis.pool", "1",
                "redis.timeout", "1", "redis.multiexec", Boolean.toString(transaction)));
        List<RedisWrite> batch = List.of(write("first"), write("second"));
        try (RedisSinkWriter writer = new RedisSinkWriter(client, config)) {
            assertThrows(RedisCommandTimeoutException.class, () -> writer.write(batch));
            assertEquals(1L, firstReply.getNow(null));
            assertFalse(lostReply.isDone());
            verify(interrupted).close();
            writer.write(batch);
            verify(client, times(2)).connect(ByteArrayCodec.INSTANCE);
            verify(replacementCommands, times(2)).dispatch(eq(CommandType.RPUSH), any(), any());
            verify(replacement, never()).close();
            if (transaction) {
                verify(interruptedCommands).exec();
                verify(replacementCommands).exec();
            }
        }
        verify(replacement).close();
    }

    @Test
    void keyAndFieldTtlsAreSentIndependently() throws Exception {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        RedisAsyncCommands<byte[], byte[]> commands = mock(RedisAsyncCommands.class);
        when(client.connect(ByteArrayCodec.INSTANCE)).thenReturn(connection);
        when(connection.sync()).thenReturn(mock(RedisCommands.class));
        when(connection.async()).thenReturn(commands);
        RedisFuture<Long> reply = future(CompletableFuture.completedFuture(1L));
        RedisFuture<List<Long>> fieldReply = future(CompletableFuture.completedFuture(List.of(1L)));
        RedisFuture<Boolean> keyReply = future(CompletableFuture.completedFuture(true));
        when(commands.<Long>dispatch(any(), any(), any())).thenReturn(reply);
        when(commands.expire(any(byte[].class), anyLong())).thenReturn(keyReply);
        when(commands.hexpire(any(byte[].class), anyLong(), any(byte[][].class))).thenReturn(fieldReply);
        byte[] fieldOnly = bytes("parents:p1");
        byte[] keyOnly = bytes("parents:p2");
        byte[] both = bytes("parents:p3");
        byte[] field = bytes("c1");
        byte[] string = bytes("string");
        RedisSinkConfig config = new RedisSinkConfig(Map.of("redis.type", "HASH_FIELD", "redis.pool", "1",
                "redis.hashfield.hash", "key.parentId", "redis.hashfield.field", "key.childId"));
        try (RedisSinkWriter writer = new RedisSinkWriter(client, config)) {
            writer.write(List.of(
                    hset(fieldOnly, field, -1, 60),
                    hset(keyOnly, field, 3600, -1),
                    hset(both, field, 3600, 60),
                    new RedisWrite(CommandType.SET, string, List.of(bytes("v")), RedisWrite.Reply.STATUS, 30),
                    new RedisWrite(CommandType.HDEL, fieldOnly, List.of(field), RedisWrite.Reply.INTEGER, -1, field, -1)));
        }
        verify(commands).hexpire(fieldOnly, 60L, field);
        verify(commands).hexpire(both, 60L, field);
        verify(commands, times(2)).hexpire(any(byte[].class), anyLong(), any(byte[][].class));
        verify(commands).expire(keyOnly, 3600L);
        verify(commands).expire(both, 3600L);
        verify(commands).expire(string, 30L);
        verify(commands, times(3)).expire(any(byte[].class), anyLong());
    }

    private static RedisWrite hset(byte[] hash, byte[] field, long ttl, long hashFieldTtl) {
        return new RedisWrite(CommandType.HSET, hash, List.of(field, bytes("{}")), RedisWrite.Reply.INTEGER, ttl,
                field, hashFieldTtl);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private RedisWrite write(String member) {
        return new RedisWrite(CommandType.RPUSH, "list".getBytes(StandardCharsets.UTF_8),
                List.of(member.getBytes(StandardCharsets.UTF_8)), RedisWrite.Reply.INTEGER, -1);
    }

    private <T> RedisFuture<T> future(CompletableFuture<T> result) {
        RedisFuture<T> future = mock(RedisFuture.class);
        when(future.toCompletableFuture()).thenReturn(result);
        return future;
    }
}
