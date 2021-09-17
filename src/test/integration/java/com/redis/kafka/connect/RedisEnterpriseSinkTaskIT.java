package com.redis.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.redis.kafka.connect.sink.RedisEnterpriseSinkConfig;
import com.redis.kafka.connect.sink.RedisEnterpriseSinkTask;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class RedisEnterpriseSinkTaskIT extends AbstractRedisEnterpriseIT {

    private RedisEnterpriseSinkTask task;

    @BeforeEach
    public void createTask() {
        task = new RedisEnterpriseSinkTask();
    }

    @AfterEach
    public void stopTask() {
        if (null != this.task) {
            this.task.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void emptyAssignment(RedisServer redis) {
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(ImmutableSet.of());
        this.task.initialize(context);
        this.task.start(ImmutableMap.of(RedisEnterpriseSinkConfig.REDIS_URI, redis.getRedisURI()));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putEmpty(RedisServer redis) {
        String topic = "putWrite";
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        this.task.initialize(context);
        this.task.start(ImmutableMap.of(RedisEnterpriseSinkConfig.REDIS_URI, redis.getRedisURI()));
        this.task.put(ImmutableList.of());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putHash(RedisServer redis) {
        String topic = "putHash";
        int count = 50;
        Map<String, Map<String, String>> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = "key" + i;
            Map<String, String> map = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
            expected.put(key, map);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key), new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), map)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.HASH, redis, records);
        for (String key : expected.keySet()) {
            Map<String, String> hash = expected.get(key);
            Map<String, String> actual = syncHash(redis).hgetall(key);
            assertEquals(hash, actual, String.format("Hash for key '%s' does not match.", key));
        }
    }

    @Data
    @Builder
    private static class Person {
        private long id;
        private String name;
        @Singular
        private Set<String> hobbies;
        private Address address;
    }

    @Data
    @Builder
    private static class Address {
        private String street;
        private String city;
        private String state;
        private String zip;
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putJSON(RedisServer redis) throws JsonProcessingException {
        String topic = "putJSON";
        List<Person> persons = new ArrayList<>();
        persons.add(Person.builder().id(1).name("Bodysnatch Cummerbund").address(Address.builder().city("New York").zip("10013").state("NY").street("150 Mott St").build()).hobby("Fishing").hobby("Singing").build());
        persons.add(Person.builder().id(2).name("Buffalo Custardbath").address(Address.builder().city("Los Angeles").zip("90001").state("CA").street("123 Sunset Blvd").build()).hobby("Surfing").hobby("Piano").build());
        persons.add(Person.builder().id(3).name("Bumblesnuff Crimpysnitch").address(Address.builder().city("Chicago").zip("60603").state("IL").street("100 S State St").build()).hobby("Skiing").hobby("Drums").build());
        List<SinkRecord> records = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (Person person : persons) {
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "person:" + person.getId()), new SchemaAndValue(Schema.STRING_SCHEMA, mapper.writeValueAsString(person))));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.JSON, redis, records);
        for (Person person : persons) {
            String json = syncJSON(redis).jsonGet("person:" + person.getId());
            assertEquals(mapper.writeValueAsString(person), json);
        }
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putLpush(RedisServer redis) {
        String topic = "putLpush";
        int count = 50;
        List<String> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "listmember:" + i;
            expected.add(member);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member), new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.LIST, redis, records);
        List<String> actual = syncList(redis).lrange(topic, 0, -1);
        Collections.reverse(actual);
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putRpush(RedisServer redis) {
        String topic = "putRpush";
        int count = 50;
        List<String> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "listmember:" + i;
            expected.add(member);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member), new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.LIST, redis, records, RedisEnterpriseSinkConfig.PUSH_DIRECTION, RedisEnterpriseSinkConfig.PushDirection.RIGHT.name());
        List<String> actual = syncList(redis).lrange(topic, 0, -1);
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putSet(RedisServer redis) {
        String topic = "putSet";
        int count = 50;
        Set<String> expected = new HashSet<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "setmember:" + i;
            expected.add(member);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member), new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.SET, redis, records);
        Set<String> members = syncSet(redis).smembers(topic);
        assertEquals(expected, members);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putStream(RedisServer redis) {
        String topic = "putStream";
        int count = 50;
        List<Map<String, String>> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, String> body = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
            expected.add(body);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "key" + i), new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), body)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.STREAM, redis, records);
        List<StreamMessage<String, String>> messages = syncStream(redis).xrange(topic, Range.unbounded());
        assertEquals(records.size(), messages.size());
        for (int index = 0; index < messages.size(); index++) {
            Map<String, String> body = expected.get(index);
            StreamMessage<String, String> message = messages.get(index);
            assertEquals(body, message.getBody(), String.format("Body for message #%s does not match.", index));
        }
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putString(RedisServer redis) {
        String topic = "putString";
        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = topic + i;
            String value = "This is value " + i;
            expected.put(key, value);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key), new SchemaAndValue(Schema.STRING_SCHEMA, value)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.STRING, redis, records);
        String[] keys = expected.keySet().toArray(new String[0]);
        List<KeyValue<String, String>> actual = syncString(redis).mget(keys);
        assertEquals(records.size(), actual.size());
        for (KeyValue<String, String> keyValue : actual) {
            assertEquals(expected.get(keyValue.getKey()), keyValue.getValue(), String.format("Value for key '%s' does not match.", keyValue.getKey()));
        }
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void setBytes(RedisServer redis) {
        String topic = "setBytes";
        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = topic + i;
            String value = "This is value " + i;
            expected.put(key, value);
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.BYTES_SCHEMA, key.getBytes(StandardCharsets.UTF_8)), new SchemaAndValue(Schema.BYTES_SCHEMA, value.getBytes(StandardCharsets.UTF_8))));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.STRING, redis, records);
        String[] keys = expected.keySet().toArray(new String[0]);
        List<KeyValue<String, String>> actual = syncString(redis).mget(keys);
        assertEquals(records.size(), actual.size());
        for (KeyValue<String, String> keyValue : actual) {
            assertEquals(expected.get(keyValue.getKey()), keyValue.getValue(), String.format("Value for key '%s' does not match.", keyValue.getKey()));
        }
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putZset(RedisServer redis) {
        String topic = "putZset";
        int count = 50;
        List<ScoredValue<String>> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String value = "zsetmember:" + i;
            expected.add(ScoredValue.just(i, value));
            records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, value), new SchemaAndValue(Schema.FLOAT64_SCHEMA, i)));
        }
        put(topic, RedisEnterpriseSinkConfig.DataType.ZSET, redis, records);
        List<ScoredValue<String>> actual = syncSortedSet(redis).zrangeWithScores(topic, 0, -1);
        expected.sort(Comparator.comparing(ScoredValue::getScore));
        assertEquals(expected, actual);
    }

    public void put(String topic, RedisEnterpriseSinkConfig.DataType type, RedisServer redis, List<SinkRecord> records, String... props) {
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        task.initialize(context);
        Map<String, String> propsMap = map(RedisEnterpriseSinkConfig.REDIS_URI, redis.getRedisURI(), RedisEnterpriseSinkConfig.TYPE, type.name());
        propsMap.putAll(map(props));
        task.start(propsMap);
        task.put(records);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void putDelete(RedisServer redis) {
        String topic = "putDelete";
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        this.task.initialize(context);
        this.task.start(ImmutableMap.of(RedisEnterpriseSinkConfig.REDIS_URI, redis.getRedisURI(), RedisEnterpriseSinkConfig.TYPE, RedisEnterpriseSinkConfig.DataType.STRING.name()));

        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            final String key = topic + i;
            final String value = "This is value " + i;
            records.add(SinkRecordHelper.delete(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key)));
            expected.put(key, value);
        }
        Map<String, String> values = expected.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        syncString(redis).mset(values);
        task.put(records);
        String[] keys = expected.keySet().toArray(new String[0]);
        long actual = syncKey(redis).exists(keys);
        assertEquals(0L, actual, "All of the keys should be removed from Redis.");
    }

}
