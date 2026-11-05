package com.redis.kafka.connect;

import com.redis.kafka.connect.common.RedisConnection;
import com.redis.kafka.connect.sink.RedisSinkConfig;
import com.redis.kafka.connect.sink.RedisSinkTask;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.sync.RedisStringCommands;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisSinkTaskTest {
    static Stream<RedisException> transientFailures() {
        return Stream.of(new RedisConnectionException("connection lost"),
                new RedisCommandTimeoutException("timed out"),
                new RedisCommandExecutionException("MOVED 1 localhost:6379"),
                new RedisCommandExecutionException("ASK 1 localhost:6379"),
                new RedisCommandExecutionException("READONLY replica"),
                new RedisCommandExecutionException("LOADING data"),
                new RedisCommandExecutionException("TRYAGAIN resharding"),
                new RedisCommandExecutionException("CLUSTERDOWN unavailable"));
    }

    @ParameterizedTest
    @MethodSource("transientFailures")
    @SuppressWarnings("unchecked")
    void retriesCompleteFlushAfterPartialFailure(RedisException failure) throws Exception {
        RedisStringCommands<String, String> commands = mock(RedisStringCommands.class);
        Map<String, String> persisted = new HashMap<>();
        AtomicInteger calls = new AtomicInteger();
        when(commands.set(anyString(), anyString())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == 2) {
                throw failure;
            }
            persisted.put(invocation.getArgument(0), invocation.getArgument(1));
            return "OK";
        });
        RedisSinkTask task = task(commands);
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
                new TopicPartition("topic", 0), new OffsetAndMetadata(12),
                new TopicPartition("topic", 1), new OffsetAndMetadata(34));
        Assertions.assertSame(failure, Assertions.assertThrows(RetriableException.class,
                () -> task.flush(offsets)).getCause());
        Assertions.assertEquals(1, persisted.size());
        task.flush(offsets);
        Assertions.assertEquals(4, calls.get(), "retry writes every partition");
        Assertions.assertEquals(Map.of(
                "com.redis.kafka.connect.sink.offset.test.topic.0", "{\"topic\":\"topic\",\"partition\":0,\"offset\":12}",
                "com.redis.kafka.connect.sink.offset.test.topic.1", "{\"topic\":\"topic\",\"partition\":1,\"offset\":34}"), persisted);
    }

    @Test
    @SuppressWarnings("unchecked")
    void permanentFlushFailureIsNotRetriable() throws Exception {
        RedisStringCommands<String, String> commands = mock(RedisStringCommands.class);
        when(commands.set(anyString(), anyString())).thenThrow(new RedisCommandExecutionException("NOPERM denied"));
        ConnectException error = Assertions.assertThrows(ConnectException.class, () -> task(commands).flush(
                Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(12))));
        Assertions.assertFalse(error instanceof RetriableException);
    }

    @SuppressWarnings("unchecked")
    private RedisSinkTask task(RedisStringCommands<String, String> commands) throws Exception {
        RedisConnection<String, String> connection = mock(RedisConnection.class);
        when(connection.sync(RedisStringCommands.class)).thenReturn(commands);
        RedisSinkTask task = new RedisSinkTask();
        // Inject failures at the task's existing Redis boundary without opening a socket.
        Field connectionField = RedisSinkTask.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(task, connection);
        Field configField = RedisSinkTask.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(task, new RedisSinkConfig(Map.of("name", "test")));
        return task;
    }
}
