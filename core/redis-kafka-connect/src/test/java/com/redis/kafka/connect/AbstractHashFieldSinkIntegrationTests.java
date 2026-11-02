package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.sink.RedisSinkConfig.RedisType;
import com.redis.kafka.connect.sink.RedisSinkConfigDef;
import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.spring.batch.test.AbstractTestBase;

import io.lettuce.core.RedisCommandExecutionException;

abstract class AbstractHashFieldSinkIntegrationTests extends AbstractTestBase {

    protected static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key")
            .field("parentId", Schema.STRING_SCHEMA).field("childId", Schema.STRING_SCHEMA).build();

    protected static final Schema DETAILS_SCHEMA = SchemaBuilder.struct().name("Details")
            .field("level", Schema.STRING_SCHEMA).build();

    protected static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("name", Schema.STRING_SCHEMA).field("status", Schema.STRING_SCHEMA)
            .field("details", DETAILS_SCHEMA).build();

    private static final ObjectMapper mapper = new ObjectMapper();

    private RedisSinkTask task;

    @AfterEach
    public void stopTask() {
        if (task != null) {
            task.stop();
            task = null;
        }
    }

    /**
     * Starts a fresh HASH_FIELD sink task with the default properties, overridden by {@code props}, then puts
     * {@code records}. The default keyspace is the topic name.
     */
    protected void put(SinkTaskContext context, List<SinkRecord> records, String... props) {
        stopTask();
        Map<String, String> config = new LinkedHashMap<>();
        config.put(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI());
        config.put(RedisSinkConfigDef.TYPE_CONFIG, RedisType.HASH_FIELD.name());
        config.put(RedisSinkConfigDef.HASH_KEY_CONFIG, "key.parentId");
        config.put(RedisSinkConfigDef.HASH_FIELD_CONFIG, "key.childId");
        for (int index = 0; index < props.length; index += 2) {
            config.put(props[index], props[index + 1]);
        }
        task = new RedisSinkTask();
        task.initialize(context);
        task.start(config);
        task.put(records);
    }

    protected void put(List<SinkRecord> records, String... props) {
        SinkTaskContext context = mock(SinkTaskContext.class);
        String topic = records.isEmpty() ? "empty" : records.get(0).topic();
        when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        put(context, records, props);
    }

    protected static Struct value(String name) {
        return new Struct(VALUE_SCHEMA).put("name", name).put("status", "ACTIVE")
                .put("details", new Struct(DETAILS_SCHEMA).put("level", "high"));
    }

    /**
     * A record keyed by {@code {parentId, childId}}. A null {@code value} makes it a tombstone.
     */
    protected static SinkRecord child(String topic, String parentId, String childId, Struct value) {
        Struct key = new Struct(KEY_SCHEMA).put("parentId", parentId).put("childId", childId);
        return AbstractSinkIntegrationTests.sinkRecord(topic, new SchemaAndValue(KEY_SCHEMA, key),
                value == null ? null : new SchemaAndValue(VALUE_SCHEMA, value));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> json(String value) throws JsonProcessingException {
        return mapper.readValue(value, Map.class);
    }

    @Test
    void hashFieldUpsert() throws JsonProcessingException {
        String topic = "hashFieldUpsert";
        put(Arrays.asList(child(topic, "P1", "c1", value("first")), child(topic, "P1", "c2", value("second")),
                child(topic, "P1", "c3", value("third")), child(topic, "P2", "c4", value("fourth"))));
        assertEquals(3L, redisConnection.sync().hlen(topic + ":P1"));
        assertEquals(1L, redisConnection.sync().hlen(topic + ":P2"));
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("name", "first");
        expected.put("status", "ACTIVE");
        expected.put("details", Collections.singletonMap("level", "high"));
        assertEquals(expected, json(redisConnection.sync().hget(topic + ":P1", "c1")));
    }

    @Test
    void hashFieldUpdateInPlace() throws JsonProcessingException {
        String topic = "hashFieldUpdateInPlace";
        put(Arrays.asList(child(topic, "P1", "c1", value("before"))));
        put(Arrays.asList(child(topic, "P1", "c1", value("after"))));
        assertEquals(1L, redisConnection.sync().hlen(topic + ":P1"));
        assertEquals("after", json(redisConnection.sync().hget(topic + ":P1", "c1")).get("name"));
    }

    @Test
    void hashFieldTombstone() {
        String topic = "hashFieldTombstone";
        put(Arrays.asList(child(topic, "P1", "c1", value("first")), child(topic, "P1", "c2", value("second"))));
        put(Arrays.asList(child(topic, "P1", "c1", null)));
        assertEquals(Arrays.asList("c2"), redisConnection.sync().hkeys(topic + ":P1"));
    }

    @Test
    void hashFieldLastFieldRemovesKey() {
        String topic = "hashFieldLastFieldRemovesKey";
        put(Arrays.asList(child(topic, "P1", "c1", value("first"))));
        put(Arrays.asList(child(topic, "P1", "c1", null)));
        assertEquals(0L, redisConnection.sync().exists(topic + ":P1"));
    }

    @Test
    void hashFieldWriteThenTombstoneSameBatch() {
        String topic = "hashFieldWriteThenTombstoneSameBatch";
        put(Arrays.asList(child(topic, "P1", "c1", value("first")), child(topic, "P1", "c1", null)));
        assertEquals(0L, redisConnection.sync().exists(topic + ":P1"));
    }

    @Test
    void hashFieldTombstoneMissingHash() {
        String topic = "hashFieldTombstoneMissingHash";
        put(Arrays.asList(child(topic, "Z", "c1", null)));
        assertEquals(0L, redisConnection.sync().exists(topic + ":Z"));
    }

    @Test
    void hashFieldNestedPath() {
        String topic = "hashFieldNestedPath";
        Schema parentSchema = SchemaBuilder.struct().name("Parent").field("id", Schema.STRING_SCHEMA).build();
        Schema keySchema = SchemaBuilder.struct().name("NestedKey").field("parent", parentSchema)
                .field("childId", Schema.STRING_SCHEMA).build();
        Struct key = new Struct(keySchema).put("parent", new Struct(parentSchema).put("id", "P1")).put("childId",
                "c1");
        put(Arrays.asList(AbstractSinkIntegrationTests.sinkRecord(topic, new SchemaAndValue(keySchema, key),
                new SchemaAndValue(VALUE_SCHEMA, value("first")))), RedisSinkConfigDef.HASH_KEY_CONFIG,
                "key.parent.id");
        assertTrue(redisConnection.sync().hexists(topic + ":P1", "c1"));
    }

    @Test
    void hashFieldMapKey() throws JsonProcessingException {
        String topic = "hashFieldMapKey";
        Map<String, Object> key = new LinkedHashMap<>();
        key.put("parentId", "P1");
        key.put("childId", "c1");
        Map<String, Object> value = Collections.singletonMap("name", "first");
        put(Arrays.asList(AbstractSinkIntegrationTests.sinkRecord(topic, new SchemaAndValue(null, key),
                new SchemaAndValue(null, value))));
        assertEquals(value, json(redisConnection.sync().hget(topic + ":P1", "c1")));
    }

    @Test
    void hashFieldStringValuePassthrough() {
        String topic = "hashFieldStringValuePassthrough";
        Struct key1 = new Struct(KEY_SCHEMA).put("parentId", "P1").put("childId", "c1");
        Struct key2 = new Struct(KEY_SCHEMA).put("parentId", "P1").put("childId", "c2");
        put(Arrays.asList(
                AbstractSinkIntegrationTests.sinkRecord(topic, new SchemaAndValue(KEY_SCHEMA, key1),
                        new SchemaAndValue(Schema.STRING_SCHEMA, "raw")),
                AbstractSinkIntegrationTests.sinkRecord(topic, new SchemaAndValue(KEY_SCHEMA, key2),
                        new SchemaAndValue(Schema.BYTES_SCHEMA, "raw-bytes".getBytes(StandardCharsets.UTF_8)))));
        assertEquals("raw", redisConnection.sync().hget(topic + ":P1", "c1"));
        assertEquals("raw-bytes", redisConnection.sync().hget(topic + ":P1", "c2"));
    }

    private SinkTaskContext contextWithReporter(String topic, ErrantRecordReporter reporter) {
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        when(context.errantRecordReporter()).thenReturn(reporter);
        return context;
    }

    @Test
    void hashFieldStringKeyReported() {
        String topic = "hashFieldStringKeyReported";
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        SinkRecord stringKey = AbstractSinkIntegrationTests.sinkRecord(topic,
                new SchemaAndValue(Schema.STRING_SCHEMA, "P1:c2"), new SchemaAndValue(VALUE_SCHEMA, value("second")));
        put(contextWithReporter(topic, reporter), Arrays.asList(child(topic, "P1", "c1", value("first")), stringKey));
        verify(reporter, times(1)).report(eq(stringKey), any(DataException.class));
        assertTrue(redisConnection.sync().hexists(topic + ":P1", "c1"));
    }

    @Test
    void hashFieldBadKeysReportedAndSkipped() {
        String topic = "hashFieldBadKeysReportedAndSkipped";
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        Schema parentOnlySchema = SchemaBuilder.struct().name("ParentOnly").field("parentId", Schema.STRING_SCHEMA)
                .build();
        SinkRecord missingChild = AbstractSinkIntegrationTests.sinkRecord(topic,
                new SchemaAndValue(parentOnlySchema, new Struct(parentOnlySchema).put("parentId", "P1")),
                new SchemaAndValue(VALUE_SCHEMA, value("missing")));
        Schema optionalChildSchema = SchemaBuilder.struct().name("OptionalChild")
                .field("parentId", Schema.STRING_SCHEMA).field("childId", Schema.OPTIONAL_STRING_SCHEMA).build();
        SinkRecord nullChild = AbstractSinkIntegrationTests.sinkRecord(topic,
                new SchemaAndValue(optionalChildSchema, new Struct(optionalChildSchema).put("parentId", "P1")),
                new SchemaAndValue(VALUE_SCHEMA, value("null")));
        SinkRecord nullKey = new SinkRecord(topic, 1, null, null, null, null, 0);
        put(contextWithReporter(topic, reporter),
                Arrays.asList(missingChild, nullChild, nullKey, child(topic, "P1", "c1", value("good"))));
        ArgumentCaptor<SinkRecord> reported = ArgumentCaptor.forClass(SinkRecord.class);
        verify(reporter, times(3)).report(reported.capture(), any(DataException.class));
        assertEquals(Arrays.asList(missingChild, nullChild, nullKey), reported.getAllValues());
        assertEquals(Arrays.asList("c1"), redisConnection.sync().hkeys(topic + ":P1"));
    }

    @Test
    void hashFieldBadKeyWithoutReporterFailsTask() {
        String topic = "hashFieldBadKeyWithoutReporterFailsTask";
        SinkRecord stringKey = AbstractSinkIntegrationTests.sinkRecord(topic,
                new SchemaAndValue(Schema.STRING_SCHEMA, "P1:c1"), new SchemaAndValue(VALUE_SCHEMA, value("first")));
        ConnectException e = assertThrows(ConnectException.class,
                () -> put(contextWithReporter(topic, null), Arrays.asList(stringKey)));
        assertFalse(e instanceof RetriableException);
        assertTrue(e.getMessage().contains("offset="), e.getMessage());
    }

    // Runs the same HEXPIRE probe as the sink task: on a missing key it is a no-op where supported.
    private boolean engineSupportsHexpire() {
        try {
            redisConnection.sync().hexpire("__redis-kafka-connect:hexpire-probe", 1, "x");
            return true;
        } catch (RedisCommandExecutionException e) {
            return false;
        }
    }

    private static SinkRecord withTtlHeader(SinkRecord record, long ttl) {
        record.headers().addString(RedisSinkConfigDef.KEY_TTL_CONFIG, String.valueOf(ttl));
        return record;
    }

    @Test
    void hashFieldTtlConfig() {
        assumeTrue(engineSupportsHexpire());
        String topic = "hashFieldTtlConfig";
        put(Arrays.asList(child(topic, "P1", "c1", value("first"))), RedisSinkConfigDef.KEY_TTL_CONFIG, "3600");
        assertEquals(3600, redisConnection.sync().httl(topic + ":P1", "c1").get(0), 100);
        assertEquals(-1L, redisConnection.sync().ttl(topic + ":P1"));
    }

    @Test
    void hashFieldTtlHeader() {
        assumeTrue(engineSupportsHexpire());
        String topic = "hashFieldTtlHeader";
        put(Arrays.asList(withTtlHeader(child(topic, "P1", "c1", value("first")), 120)));
        assertEquals(120, redisConnection.sync().httl(topic + ":P1", "c1").get(0), 20);
    }

    @Test
    void hashFieldTtlReappliedOnUpdate() {
        assumeTrue(engineSupportsHexpire());
        String topic = "hashFieldTtlReappliedOnUpdate";
        put(Arrays.asList(withTtlHeader(child(topic, "P1", "c1", value("first")), 120)));
        put(Arrays.asList(child(topic, "P1", "c1", value("second"))), RedisSinkConfigDef.KEY_TTL_CONFIG, "3600");
        assertEquals(3600, redisConnection.sync().httl(topic + ":P1", "c1").get(0), 100);
    }

    @Test
    void hashFieldTtlUnsupportedFailsStart() {
        assumeFalse(engineSupportsHexpire());
        String topic = "hashFieldTtlUnsupportedFailsStart";
        ConnectException e = assertThrows(ConnectException.class, () -> put(
                Arrays.asList(child(topic, "P1", "c1", value("first"))), RedisSinkConfigDef.KEY_TTL_CONFIG, "3600"));
        assertTrue(e.getMessage().contains("requires a Redis engine that supports HEXPIRE"), e.getMessage());
    }

    @Test
    void hashFieldTtlHeaderUnsupportedReported() {
        assumeFalse(engineSupportsHexpire());
        String topic = "hashFieldTtlHeaderUnsupportedReported";
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        SinkRecord record = withTtlHeader(child(topic, "P1", "c1", value("first")), 120);
        put(contextWithReporter(topic, reporter), Arrays.asList(record));
        verify(reporter, times(1)).report(eq(record), any(DataException.class));
        assertEquals(0L, redisConnection.sync().exists(topic + ":P1"));
    }

    @Test
    void hashFieldNoTtlWorksEverywhere() {
        String topic = "hashFieldNoTtlWorksEverywhere";
        put(Arrays.asList(child(topic, "P1", "c1", value("first"))));
        assertTrue(redisConnection.sync().hexists(topic + ":P1", "c1"));
        assertEquals(-1L, redisConnection.sync().ttl(topic + ":P1"));
        if (engineSupportsHexpire()) {
            assertEquals(-1L, redisConnection.sync().httl(topic + ":P1", "c1").get(0));
        }
    }

    @Test
    void hashFieldEmptyKeyspace() {
        String topic = "hashFieldEmptyKeyspace";
        put(Arrays.asList(child(topic, "hashFieldEmptyKeyspaceP1", "c1", value("first"))),
                RedisSinkConfigDef.KEYSPACE_CONFIG, "");
        assertTrue(redisConnection.sync().hexists("hashFieldEmptyKeyspaceP1", "c1"));
    }

}
