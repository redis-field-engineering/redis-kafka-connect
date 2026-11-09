package com.redis.kafka.connect.sink;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class RedisSinkTaskErrantRecordTest {

    private final RedisSinkWriter writer = mock(RedisSinkWriter.class);

    @Test
    void invalidRecordIsReportedAndRemainingRecordsAreWrittenInOrder() throws Exception {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        SinkRecord invalid = record(null, 1);
        task(reporter).put(List.of(record("first", 0), invalid, record("last", 2)));
        verify(reporter).report(eq(invalid), isA(DataException.class));
        ArgumentCaptor<List<RedisWrite>> writes = ArgumentCaptor.forClass(List.class);
        verify(writer).write(writes.capture());
        assertEquals(List.of("first", "last"), writes.getValue().stream()
                .map(write -> new String(write.key(), StandardCharsets.UTF_8)).toList());
    }

    @Test
    void reporterFailureFailsTaskBeforeAnyWrite() throws Exception {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        ConnectException toleranceExceeded = new ConnectException("Tolerance exceeded in error handler");
        when(reporter.report(any(), any())).thenThrow(toleranceExceeded);
        RedisSinkTask task = task(reporter);
        ConnectException error = assertThrows(ConnectException.class,
                () -> task.put(List.of(record("first", 0), record(null, 1), record("last", 2))));
        assertSame(toleranceExceeded, error);
        assertFalse(error instanceof RetriableException);
        verify(writer, never()).write(any());
    }

    @Test
    void invalidRecordWithoutReporterFailsTaskBeforeAnyWrite() throws Exception {
        RedisSinkTask task = task(null);
        assertThrows(DataException.class, () -> task.put(List.of(record("first", 0), record(null, 1))));
        verify(writer, never()).write(any());
    }

    @Test
    void reporterLookupToleratesMissingContextAndOldRuntimes() {
        assertNull(RedisSinkTask.errantRecordReporter(null));
        SinkTaskContext oldRuntime = mock(SinkTaskContext.class);
        when(oldRuntime.errantRecordReporter()).thenThrow(new NoSuchMethodError("errantRecordReporter"));
        assertNull(RedisSinkTask.errantRecordReporter(oldRuntime));
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        assertSame(reporter, RedisSinkTask.errantRecordReporter(context));
    }

    private static SinkRecord record(String key, long offset) {
        // A null key cannot be converted into a STRING command.
        return new SinkRecord("topic", 0, Schema.OPTIONAL_STRING_SCHEMA, key, Schema.STRING_SCHEMA, "value", offset);
    }

    private RedisSinkTask task(ErrantRecordReporter reporter) throws Exception {
        RedisSinkTask task = new RedisSinkTask();
        // Inject collaborators directly so put() runs without a Redis connection.
        set(task, "config", new RedisSinkConfig(Map.of("redis.type", "STRING", "redis.keyspace", "")));
        set(task, "writer", writer);
        set(task, "reporter", reporter);
        return task;
    }

    private static void set(RedisSinkTask task, String name, Object value) throws Exception {
        Field field = RedisSinkTask.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(task, value);
    }
}
