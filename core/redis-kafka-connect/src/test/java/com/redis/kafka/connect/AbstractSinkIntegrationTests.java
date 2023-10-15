package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;
import org.springframework.util.Assert;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.sink.RedisSinkConfig.RedisCommand;
import com.redis.kafka.connect.sink.RedisSinkConfigDef;
import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;

import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

abstract class AbstractSinkIntegrationTests extends AbstractTestBase {

    public static final int PARTITION = 1;

    public static final long OFFSET = 91283741L;

    public static final long TIMESTAMP = 1530286549123L;

    public static SinkRecord write(String topic, SchemaAndValue key, SchemaAndValue value) {
        Preconditions.notNull(topic, "topic cannot be null");
        Preconditions.notNull(key, "key cannot be null.");
        Preconditions.notNull(key.value(), "key cannot be null.");
        Preconditions.notNull(value, "value cannot be null.");
        Preconditions.notNull(value.value(), "value cannot be null.");

        return new SinkRecord(topic, PARTITION, key.schema(), key.value(), value.schema(), value.value(), OFFSET, TIMESTAMP,
                TimestampType.CREATE_TIME);
    }

    public static SinkRecord delete(String topic, SchemaAndValue key) {
        Preconditions.notNull(topic, "topic cannot be null");
        if (null == key) {
            throw new DataException("key cannot be null.");
        }
        if (null == key.value()) {
            throw new DataException("key cannot be null.");
        }

        return new SinkRecord(topic, PARTITION, key.schema(), key.value(), null, null, OFFSET, TIMESTAMP,
                TimestampType.CREATE_TIME);
    }

    @Override
    protected DataType[] generatorDataTypes() {
        return AbstractTestBase.REDIS_MODULES_GENERATOR_TYPES;
    }

    protected Map<String, String> map(String... args) {
        Assert.notNull(args, "Args cannot be null");
        Assert.isTrue(args.length % 2 == 0, "Args length is not a multiple of 2");
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }

    private RedisSinkTask task;

    @BeforeEach
    public void createTask() {
        task = new RedisSinkTask();
    }

    @AfterEach
    public void stopTask() {
        if (null != this.task) {
            this.task.stop();
        }
    }

    @Test
    void emptyAssignment() {
        SinkTaskContext taskContext = mock(SinkTaskContext.class);
        when(taskContext.assignment()).thenReturn(ImmutableSet.of());
        this.task.initialize(taskContext);
        this.task.start(ImmutableMap.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI()));
    }

    @Test
    void putEmpty() {
        String topic = "putWrite";
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        this.task.initialize(context);
        this.task.start(ImmutableMap.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI()));
        this.task.put(ImmutableList.of());
    }

    @Test
    void putHash() {
        String topic = "hash";
        int count = 50;
        Map<String, Map<String, String>> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, String> map = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
            expected.put("hash:" + i, map);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, i),
                    new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), map)));
        }
        put(topic, RedisCommand.HSET, records);
        for (String key : expected.keySet()) {
            Map<String, String> hash = expected.get(key);
            Map<String, String> actual = connection.sync().hgetall(key);
            assertEquals(hash, actual, String.format("Hash for key '%s' does not match.", key));
        }
    }

    public static class Person {

        private long id;

        private String name;

        private Set<String> hobbies = new HashSet<>();

        private Address address;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Set<String> getHobbies() {
            return hobbies;
        }

        public void setHobbies(Set<String> hobbies) {
            this.hobbies = hobbies;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, hobbies, id, name);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Person other = (Person) obj;
            return Objects.equals(address, other.address) && Objects.equals(hobbies, other.hobbies) && id == other.id
                    && Objects.equals(name, other.name);
        }

    }

    public static class Address {

        private String street;

        private String city;

        private String state;

        private String zip;

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getZip() {
            return zip;
        }

        public void setZip(String zip) {
            this.zip = zip;
        }

        @Override
        public int hashCode() {
            return Objects.hash(city, state, street, zip);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Address other = (Address) obj;
            return Objects.equals(city, other.city) && Objects.equals(state, other.state)
                    && Objects.equals(street, other.street) && Objects.equals(zip, other.zip);
        }

    }

    @Test
    void putJSON() throws JsonProcessingException {
        String topic = "putJSON";
        List<Person> persons = new ArrayList<>();
        Person person1 = new Person();
        person1.setId(1);
        person1.setName("Bodysnitch Canderbunt");
        person1.setHobbies(new HashSet<>(Arrays.asList("Fishing", "Singing")));
        Address address1 = new Address();
        address1.setCity("New York");
        address1.setZip("10013");
        address1.setState("NY");
        address1.setStreet("150 Mott St");
        person1.setAddress(address1);
        persons.add(person1);
        Person person2 = new Person();
        person2.setId(2);
        person2.setName("Buffalo Custardbath");
        person2.setHobbies(new HashSet<>(Arrays.asList("Surfing", "Piano")));
        Address address2 = new Address();
        address2.setCity("Los Angeles");
        address2.setZip("90001");
        address2.setState("CA");
        address2.setStreet("123 Sunset Blvd");
        person2.setAddress(address2);
        persons.add(person2);
        Person person3 = new Person();
        person3.setId(3);
        person3.setName("Bumblesnuff Crimpysnitch");
        person3.setHobbies(new HashSet<>(Arrays.asList("Skiing", "Drums")));
        Address address3 = new Address();
        address3.setCity("Chicago");
        address3.setZip("60603");
        address3.setState("IL");
        address3.setStreet("100 S State St");
        person3.setAddress(address3);
        persons.add(person3);
        List<SinkRecord> records = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (Person person : persons) {
            String json = mapper.writeValueAsString(person);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, person.getId()),
                    new SchemaAndValue(Schema.STRING_SCHEMA, json)));
        }
        put(topic, RedisCommand.JSONSET, records);
        for (Person person : persons) {
            String json = connection.sync().jsonGet(topic + ":" + person.getId());
            assertEquals(person, mapper.readValue(json, Person.class));
        }
    }

    @Test
    void putTimeSeries() {
        String topic = "putTimeSeries";
        int count = 50;
        long startTime = System.currentTimeMillis() - count;
        List<Sample> expectedSamples = new ArrayList<>();
        List<SinkRecord> records = new ArrayList<>();
        for (int index = 1; index <= count; index++) {
            long timestamp = startTime + index;
            double value = index;
            expectedSamples.add(Sample.of(timestamp, value));
            records.add(write(topic, new SchemaAndValue(Schema.INT64_SCHEMA, timestamp),
                    new SchemaAndValue(Schema.FLOAT64_SCHEMA, value)));
        }
        put(topic, RedisCommand.TSADD, records);
        List<Sample> actualSamples = connection.sync().tsRange(topic, TimeRange.unbounded());
        assertEquals(expectedSamples.size(), actualSamples.size());
        for (int index = 0; index < expectedSamples.size(); index++) {
            Sample expectedSample = expectedSamples.get(index);
            Sample actualSample = actualSamples.get(index);
            assertEquals(expectedSample.getTimestamp(), actualSample.getTimestamp());
            assertEquals(expectedSample.getValue(), actualSample.getValue());
        }
    }

    @Test
    void putLpush() {
        String topic = "putLpush";
        int count = 50;
        List<String> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "listmember:" + i;
            expected.add(member);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
                    new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisCommand.LPUSH, records);
        List<String> actual = connection.sync().lrange(topic, 0, -1);
        Collections.reverse(actual);
        assertEquals(expected, actual);
    }

    @Test
    void putRpush() {
        String topic = "putRpush";
        int count = 50;
        List<String> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "listmember:" + i;
            expected.add(member);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
                    new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisCommand.RPUSH, records);
        List<String> actual = connection.sync().lrange(topic, 0, -1);
        assertEquals(expected, actual);
    }

    @Test
    void putSet() {
        String topic = "putSet";
        int count = 50;
        Set<String> expected = new HashSet<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "setmember:" + i;
            expected.add(member);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
                    new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisCommand.SADD, records);
        Set<String> members = connection.sync().smembers(topic);
        assertEquals(expected, members);
    }

    @Test
    void putStream() {
        String topic = "putStream";
        int count = 50;
        List<Map<String, String>> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, String> body = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
            expected.add(body);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "key" + i),
                    new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), body)));
        }
        put(topic, RedisCommand.XADD, records);
        List<StreamMessage<String, String>> messages = connection.sync().xrange(topic, Range.unbounded());
        assertEquals(records.size(), messages.size());
        for (int index = 0; index < messages.size(); index++) {
            Map<String, String> body = expected.get(index);
            StreamMessage<String, String> message = messages.get(index);
            assertEquals(body, message.getBody(), String.format("Body for message #%s does not match.", index));
        }
    }

    @Test
    void putString() {
        String topic = "string";
        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = String.valueOf(i);
            String value = "This is value " + i;
            expected.put(topic + ":" + key, value);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key),
                    new SchemaAndValue(Schema.STRING_SCHEMA, value)));
        }
        put(topic, RedisCommand.SET, records);
        String[] keys = expected.keySet().toArray(new String[0]);
        List<KeyValue<String, String>> actual = connection.sync().mget(keys);
        assertEquals(records.size(), actual.size());
        for (KeyValue<String, String> keyValue : actual) {
            assertEquals(expected.get(keyValue.getKey()), keyValue.getValue(),
                    String.format("Value for key '%s' does not match.", keyValue.getKey()));
        }
    }

    @Test
    void setBytes() {
        String topic = "setBytes";
        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = topic + i;
            String value = "This is value " + i;
            expected.put(key, value);
            records.add(write(topic, new SchemaAndValue(Schema.BYTES_SCHEMA, key.getBytes(StandardCharsets.UTF_8)),
                    new SchemaAndValue(Schema.BYTES_SCHEMA, value.getBytes(StandardCharsets.UTF_8))));
        }
        put(topic, RedisCommand.SET, records, RedisSinkConfigDef.KEY_CONFIG, "");
        String[] keys = expected.keySet().toArray(new String[0]);
        List<KeyValue<String, String>> actual = connection.sync().mget(keys);
        assertEquals(records.size(), actual.size());
        for (KeyValue<String, String> keyValue : actual) {
            assertEquals(expected.get(keyValue.getKey()), keyValue.getValue(),
                    String.format("Value for key '%s' does not match.", keyValue.getKey()));
        }
    }

    @Test
    void putZset() {
        String topic = "putZset";
        int count = 50;
        List<ScoredValue<String>> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String value = "zsetmember:" + i;
            expected.add(ScoredValue.just(i, value));
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, value),
                    new SchemaAndValue(Schema.FLOAT64_SCHEMA, i)));
        }
        put(topic, RedisCommand.ZADD, records);
        List<ScoredValue<String>> actual = connection.sync().zrangeWithScores(topic, 0, -1);
        expected.sort(Comparator.comparing(ScoredValue::getScore));
        assertEquals(expected, actual);
    }

    public void put(String topic, RedisCommand command, List<SinkRecord> records, String... props) {
        SinkTaskContext taskContext = mock(SinkTaskContext.class);
        when(taskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        task.initialize(taskContext);
        Map<String, String> propsMap = map(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.COMMAND_CONFIG, command.name());
        propsMap.putAll(map(props));
        task.start(propsMap);
        task.put(records);
    }

    @Test
    void putDelete() {
        String topic = "putDelete";
        SinkTaskContext taskContext = mock(SinkTaskContext.class);
        when(taskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
        this.task.initialize(taskContext);
        this.task.start(ImmutableMap.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.COMMAND_CONFIG, RedisCommand.DEL.name()));

        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            final String value = "This is value " + i;
            records.add(delete(topic, new SchemaAndValue(Schema.STRING_SCHEMA, i)));
            expected.put(topic + ":" + i, value);
        }
        Map<String, String> values = expected.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        connection.sync().mset(values);
        task.put(records);
        String[] keys = expected.keySet().toArray(new String[0]);
        long actual = connection.sync().exists(keys);
        assertEquals(0L, actual, "All of the keys should be removed from Redis.");
    }

}
