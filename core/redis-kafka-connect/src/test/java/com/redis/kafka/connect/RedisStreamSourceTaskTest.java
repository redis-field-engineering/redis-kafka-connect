package com.redis.kafka.connect;

import com.redis.kafka.connect.common.RedisConnection;
import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.api.sync.RedisStreamCommands;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class RedisStreamSourceTaskTest {
    @Test
    void failedAcknowledgmentRetainsDeliveryCallbacksAndInFlightRecordsForRetry() throws Exception {
        RedisStreamCommands<String, String> commands = mock(RedisStreamCommands.class);
        RedisStreamSourceTask task = task(commands);
        task.commitRecord(record("1-0"), null);
        task.commitRecord(record("2-0"), null);
        inFlight(task).addAll(List.of("1-0", "2-0"));
        RedisCommandTimeoutException failure = new RedisCommandTimeoutException("XACK reply lost");
        when(commands.xack(eq("stream"), eq("group"), any(String[].class))).thenThrow(failure).thenReturn(2L);
        assertSame(failure, assertThrows(ConnectException.class, task::commit).getCause());
        assertEquals(Set.of("1-0", "2-0"), inFlight(task));
        task.commit();
        assertTrue(inFlight(task).isEmpty());
        task.commit();
        ArgumentCaptor<String[]> batches = ArgumentCaptor.forClass(String[].class);
        verify(commands, times(2)).xack(eq("stream"), eq("group"), batches.capture());
        batches.getAllValues().forEach(batch -> assertArrayEquals(new String[] {"1-0", "2-0"}, batch));
    }

    @Test
    void callbackArrivingDuringAcknowledgmentIsCommittedInTheNextBatch() throws Exception {
        RedisStreamCommands<String, String> commands = mock(RedisStreamCommands.class);
        RedisStreamSourceTask task = task(commands);
        inFlight(task).addAll(List.of("1-0", "2-0"));
        task.commitRecord(record("1-0"), null);
        CountDownLatch acknowledging = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(commands.xack(eq("stream"), eq("group"), any(String[].class))).thenAnswer(invocation -> {
            acknowledging.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS), "acknowledgment was not released");
            return 1L;
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> commit = executor.submit(task::commit);
            assertTrue(acknowledging.await(5, TimeUnit.SECONDS));
            task.commitRecord(record("2-0"), null);
            release.countDown();
            commit.get(5, TimeUnit.SECONDS);
            assertEquals(Set.of("2-0"), inFlight(task));
            task.commit();
            assertTrue(inFlight(task).isEmpty());
            task.commit();
            ArgumentCaptor<String[]> batches = ArgumentCaptor.forClass(String[].class);
            verify(commands, times(2)).xack(eq("stream"), eq("group"), batches.capture());
            assertArrayEquals(new String[] {"1-0"}, batches.getAllValues().get(0));
            assertArrayEquals(new String[] {"2-0"}, batches.getAllValues().get(1));
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private RedisStreamSourceTask task(RedisStreamCommands<String, String> commands) throws Exception {
        RedisStreamSourceTask task = new RedisStreamSourceTask();
        RedisConnection<String, String> connection = mock(RedisConnection.class);
        when(connection.sync(RedisStreamCommands.class)).thenReturn(commands);
        Field connectionField = RedisStreamSourceTask.class.getDeclaredField("acknowledgmentConnection");
        connectionField.setAccessible(true);
        connectionField.set(task, connection);
        Field consumerField = RedisStreamSourceTask.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(task, Consumer.from("group", "consumer-0"));
        Field configField = RedisStreamSourceTask.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(task, new RedisStreamSourceConfig(Map.of("redis.stream.name", "stream", "redis.stream.consumer.group", "group")));
        return task;
    }

    private Set<String> inFlight(RedisStreamSourceTask task) throws Exception {
        Field field = RedisStreamSourceTask.class.getDeclaredField("inFlight");
        field.setAccessible(true);
        return (Set<String>) field.get(task);
    }

    private SourceRecord record(String id) {
        return new SourceRecord(Map.of("stream", "stream"), Map.of("offset", id), "topic", null, null);
    }
}
