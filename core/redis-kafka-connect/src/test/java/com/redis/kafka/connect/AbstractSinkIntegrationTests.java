package com.redis.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.sink.RedisSinkConfig.RedisType;
import com.redis.kafka.connect.sink.RedisSinkConfigDef;
import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.kafka.connect.common.RedisConnection;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

abstract class AbstractSinkIntegrationTests extends AbstractRedisIntegrationTests {

    public static final int PARTITION = 1;

    public static final long OFFSET = 91283741L;

    public static final long TIMESTAMP = 1530286549123L;

    private static final String OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s.%s";

    private static final String LEGACY_OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s";

    public static SinkRecord write(String topic, SchemaAndValue key, SchemaAndValue value) {
        Objects.requireNonNull(topic, "topic cannot be null");
        Objects.requireNonNull(key, "key cannot be null.");
        Objects.requireNonNull(key.value(), "key cannot be null.");
        Objects.requireNonNull(value, "value cannot be null.");
        Objects.requireNonNull(value.value(), "value cannot be null.");

        return new SinkRecord(topic, PARTITION, key.schema(), key.value(), value.schema(), value.value(), OFFSET, TIMESTAMP,
                TimestampType.CREATE_TIME);
    }

    protected Map<String, String> map(String... args) {
        Objects.requireNonNull(args, "Args cannot be null");
        if (args.length % 2 != 0) { throw new IllegalArgumentException("Args length is not a multiple of 2"); }
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }

    private Map<String, String> sinkProps(String connectorName) {
        return Map.of("name", connectorName, RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI());
    }

    private static String offsetKey(String namespace, TopicPartition topicPartition) {
        return String.format(OFFSET_KEY_FORMAT, namespace, topicPartition.topic(), topicPartition.partition());
    }

    private static String legacyOffsetKey(TopicPartition topicPartition) {
        return String.format(LEGACY_OFFSET_KEY_FORMAT, topicPartition.topic(), topicPartition.partition());
    }

    private static String offsetValue(String topic, int partition, long offset) {
        return "{\"topic\":\"" + topic + "\",\"partition\":" + partition + ",\"offset\":" + offset + "}";
    }

    private RedisSinkTask task;

    private void startSinkTask(Map<String, String> props) {
        task.start(deploymentProps(props));
    }

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
        when(taskContext.assignment()).thenReturn(Set.of());
        this.task.initialize(taskContext);
        this.startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI()));
    }

    @Test
    void putEmpty() {
        String topic = "putWrite";
        SinkTaskContext context = mock(SinkTaskContext.class);
        this.task.initialize(context);
        this.startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI()));
        this.task.put(List.of());
    }

    @Test
    void flushWritesNamespacedOffset() throws JsonProcessingException {
        String topic = "flushWritesNamespacedOffset";
        TopicPartition topicPartition = new TopicPartition(topic, PARTITION);
        SinkTaskContext context = mock(SinkTaskContext.class);
        this.task.initialize(context);
        this.startSinkTask(sinkProps("connector-a"));

        this.task.flush(Map.of(topicPartition, new OffsetAndMetadata(123L)));

        String value = redisConnection.sync().get(offsetKey("connector-a", topicPartition));
        @SuppressWarnings("unchecked")
        Map<String, Object> offset = new ObjectMapper().readValue(value, Map.class);
        assertEquals(topic, offset.get("topic"));
        assertEquals(PARTITION, offset.get("partition"));
        assertEquals(123, offset.get("offset"));
    }

    @Test
    void openRestoresNamespacedOffset() {
        String topic = "openRestoresNamespacedOffset";
        TopicPartition topicPartition = new TopicPartition(topic, PARTITION);
        redisConnection.sync().set(offsetKey("connector-a", topicPartition), offsetValue(topic, PARTITION, 456L));
        SinkTaskContext context = mock(SinkTaskContext.class);
        this.task.initialize(context);
        this.startSinkTask(sinkProps("connector-a"));

        this.task.open(List.of(topicPartition));

        verify(context).offset(Map.of(topicPartition, 456L));
    }

    @Test
    void openDoesNotSeekPartitionsWithoutStoredOffsets() {
        TopicPartition topicPartition = new TopicPartition("openDoesNotSeekPartitionsWithoutStoredOffsets", PARTITION);
        SinkTaskContext context = mock(SinkTaskContext.class);
        this.task.initialize(context);
        this.startSinkTask(sinkProps("connector-a"));

        this.task.open(List.of(topicPartition));

        verify(context, never()).offset(anyMap());
    }

    @Test
    void openFallsBackToLegacyOffsetKey() {
        String topic = "openFallsBackToLegacyOffsetKey";
        TopicPartition topicPartition = new TopicPartition(topic, PARTITION);
        redisConnection.sync().set(legacyOffsetKey(topicPartition), offsetValue(topic, PARTITION, 789L));
        SinkTaskContext context = mock(SinkTaskContext.class);
        this.task.initialize(context);
        this.startSinkTask(sinkProps("connector-a"));

        this.task.open(List.of(topicPartition));

        verify(context).offset(Map.of(topicPartition, 789L));
    }

    @Test
    void offsetsAreScopedByNamespace() {
        String topic = "offsetsAreScopedByNamespace";
        TopicPartition topicPartition = new TopicPartition(topic, PARTITION);
        SinkTaskContext context = mock(SinkTaskContext.class);
        this.task.initialize(context);
        this.startSinkTask(sinkProps("connector-a"));

        this.task.flush(Map.of(topicPartition, new OffsetAndMetadata(11L)));
        this.task.stop();
        this.task = new RedisSinkTask();
        this.task.initialize(context);
        this.startSinkTask(sinkProps("connector-b"));
        this.task.flush(Map.of(topicPartition, new OffsetAndMetadata(22L)));

        Assertions.assertNotNull(redisConnection.sync().get(offsetKey("connector-a", topicPartition)));
        Assertions.assertNotNull(redisConnection.sync().get(offsetKey("connector-b", topicPartition)));
        Assertions.assertNotEquals(redisConnection.sync().get(offsetKey("connector-a", topicPartition)),
                redisConnection.sync().get(offsetKey("connector-b", topicPartition)));
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
        put(topic, RedisType.HASH, records);
        for (String key : expected.keySet()) {
            Map<String, String> hash = expected.get(key);
            Map<String, String> actual = redisConnection.sync().hgetall(key);
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
            return Objects.equals(city, other.city) && Objects.equals(state, other.state) && Objects.equals(street,
                    other.street) && Objects.equals(zip, other.zip);
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
        put(topic, RedisType.JSON, records);
        for (Person person : persons) {
            String json = redisConnection.sync().jsonGetRaw(topic + ":" + person.getId()).get(0);
            assertEquals(person, mapper.readValue(json, Person.class));
        }
    }

    @Test
    void putTimeSeries() {
        String topic = "putTimeSeries";
        int count = 50;
        long startTime = System.currentTimeMillis() - count;
        List<Map.Entry<Long, Double>> expectedSamples = new ArrayList<>();
        List<SinkRecord> records = new ArrayList<>();
        for (int index = 1; index <= count; index++) {
            long timestamp = startTime + index;
            double value = index;
            expectedSamples.add(Map.entry(timestamp, value));
            records.add(write(topic, new SchemaAndValue(Schema.INT64_SCHEMA, timestamp),
                    new SchemaAndValue(Schema.FLOAT64_SCHEMA, value)));
        }
        put(topic, RedisType.TIMESERIES, records);
        List<Object> actualSamples = timeSeries(topic);
        assertEquals(expectedSamples.size(), actualSamples.size());
        for (int index = 0; index < expectedSamples.size(); index++) {
            Map.Entry<Long, Double> expectedSample = expectedSamples.get(index);
            List<?> actualSample = (List<?>) actualSamples.get(index);
            assertEquals(expectedSample.getKey().longValue(), ((Number) actualSample.get(0)).longValue());
            assertEquals(expectedSample.getValue().doubleValue(), Double.parseDouble(actualSample.get(1).toString()));
        }
    }

    @Test
    void zeroTimeSeriesTimestampUsesServerTime() {
        String topic = "automatic-timestamps";
        long before = Long.parseLong(redisConnection.sync().time().get(0)) * 1000;
        put(topic, RedisType.TIMESERIES, List.of(write(topic,
                new SchemaAndValue(Schema.INT64_SCHEMA, 0L), new SchemaAndValue(Schema.FLOAT64_SCHEMA, 1.0))));
        List<?> first = (List<?>) timeSeries(topic).get(0);
        long timestamp = ((Number) first.get(0)).longValue();
        Assertions.assertTrue(timestamp >= before, "zero must select server time, not Unix epoch");
        awaitUntil(() -> {
            List<String> time = redisConnection.sync().time();
            return Long.parseLong(time.get(0)) * 1000 + Long.parseLong(time.get(1)) / 1000 > timestamp;
        });
        task.put(List.of(write(topic, new SchemaAndValue(Schema.INT64_SCHEMA, 0L),
                new SchemaAndValue(Schema.FLOAT64_SCHEMA, 2.0))));
        Assertions.assertEquals(2, timeSeries(topic).size());
    }

    @Test
    void sortedSetsAcceptBothInfiniteScores() {
        String topic = "infinite-scores";
        put(topic, RedisType.ZSET, List.of(
                write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "positive"),
                        new SchemaAndValue(Schema.FLOAT64_SCHEMA, Double.POSITIVE_INFINITY)),
                write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "negative"),
                        new SchemaAndValue(Schema.FLOAT64_SCHEMA, Double.NEGATIVE_INFINITY))));
        assertEquals(List.of(ScoredValue.just(Double.NEGATIVE_INFINITY, "negative"),
                ScoredValue.just(Double.POSITIVE_INFINITY, "positive")),
                redisConnection.sync().zrangeWithScores(topic, 0, -1));
    }

    @Test
    void nanScoresAndNonfiniteSamplesRejectTheWholeBatch() {
        for (RedisType type : List.of(RedisType.ZSET, RedisType.TIMESERIES)) {
            startSinkTask(Map.of("redis.type", type.name()));
            List<Double> invalid = type == RedisType.ZSET ? List.of(Double.NaN)
                    : List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
            for (double value : invalid) {
                SchemaAndValue key = type == RedisType.ZSET ? new SchemaAndValue(Schema.STRING_SCHEMA, "member")
                        : new SchemaAndValue(Schema.INT64_SCHEMA, 1234L);
                Assertions.assertThrows(DataException.class, () -> task.put(List.of(
                        write("invalid-score", key, new SchemaAndValue(Schema.FLOAT64_SCHEMA, 1.0)),
                        write("invalid-score", key, new SchemaAndValue(Schema.FLOAT64_SCHEMA, value)))));
                Assertions.assertEquals(0, redisConnection.sync().exists("invalid-score"));
            }
            task.stop();
        }
    }

    @Test
    void putList() {
        String topic = "putList";
        int count = 50;
        List<String> expected = new ArrayList<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String member = "listmember:" + i;
            expected.add(member);
            records.add(write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
                    new SchemaAndValue(Schema.STRING_SCHEMA, member)));
        }
        put(topic, RedisType.LIST, records);
        assertEquals(expected, redisConnection.sync().lrange(topic, 0, -1));
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
        put(topic, RedisType.SET, records);
        Set<String> members = redisConnection.sync().smembers(topic);
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
        put(topic, RedisType.STREAM, records);
        List<StreamMessage<String, String>> messages = redisConnection.sync().xrange(topic, Range.unbounded());
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
        put(topic, RedisType.STRING, records);
        String[] keys = expected.keySet().toArray(new String[0]);
        List<KeyValue<String, String>> actual = Arrays.stream(keys).map(key -> KeyValue.fromNullable(key, redisConnection.sync().get(key))).toList();
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
        put(topic, RedisType.STRING, records, RedisSinkConfigDef.KEYSPACE_CONFIG, "");
        String[] keys = expected.keySet().toArray(new String[0]);
        List<KeyValue<String, String>> actual = Arrays.stream(keys).map(key -> KeyValue.fromNullable(key, redisConnection.sync().get(key))).toList();
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
        put(topic, RedisType.ZSET, records);
        List<ScoredValue<String>> actual = redisConnection.sync().zrangeWithScores(topic, 0, -1);
        expected.sort(Comparator.comparing(ScoredValue::getScore));
        assertEquals(expected, actual);
    }

    public void put(String topic, RedisType type, List<SinkRecord> records, String... props) {
        SinkTaskContext taskContext = mock(SinkTaskContext.class);
        when(taskContext.assignment()).thenReturn(Set.of(new TopicPartition(topic, 1)));
        task.initialize(taskContext);
        Map<String, String> propsMap = map(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, type.name());
        propsMap.putAll(map(props));
        startSinkTask(propsMap);
        task.put(records);
    }

    public static SinkRecord sinkRecord(String topic, SchemaAndValue key, SchemaAndValue value) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (key == null) {
            throw new DataException("key cannot be null.");
        }
        if (key.value() == null) {
            throw new DataException("key cannot be null.");
        }

        return new SinkRecord(topic, PARTITION, schema(key), value(key), schema(value), value(value), OFFSET, TIMESTAMP,
                TimestampType.CREATE_TIME);
    }

    private static Schema schema(SchemaAndValue value) {
        if (value == null) {
            return null;
        }
        return value.schema();
    }

    private static Object value(SchemaAndValue value) {
        if (value == null) {
            return null;
        }
        return value.value();
    }

    @Test
    void putDelete() {
        String topic = "putDelete";
        SinkTaskContext taskContext = mock(SinkTaskContext.class);
        when(taskContext.assignment()).thenReturn(Set.of(new TopicPartition(topic, 1)));
        this.task.initialize(taskContext);
        this.startSinkTask(
                Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(), RedisSinkConfigDef.TYPE_CONFIG,
                        RedisType.HASH.name()));
        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String value = "This is value " + i;
            records.add(sinkRecord(topic, new SchemaAndValue(Schema.STRING_SCHEMA, i), null));
            expected.put(topic + ":" + i, value);
        }
        Map<String, String> values = expected.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        values.forEach((key, value) -> redisConnection.sync().set(key, value));
        task.put(records);
        String[] keys = expected.keySet().toArray(new String[0]);
        long actual = Arrays.stream(keys).mapToLong(key -> redisConnection.sync().exists(key)).sum();
        assertEquals(0L, actual, "All of the keys should be removed from Redis.");
    }

    @Test
    void putExpire() {
        String topic = "putExpire";
        SinkTaskContext taskContext = mock(SinkTaskContext.class);
        when(taskContext.assignment()).thenReturn(Set.of(new TopicPartition(topic, 1)));
        this.task.initialize(taskContext);
        this.startSinkTask(
                Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(), RedisSinkConfigDef.TYPE_CONFIG,
                        RedisType.STRING.name(), RedisSinkConfigDef.KEY_TTL_CONFIG, "3600"));
        int count = 50;
        Map<String, String> expected = new LinkedHashMap<>(count);
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String value = "This is value " + i;
            records.add(sinkRecord(topic, new SchemaAndValue(Schema.STRING_SCHEMA, i),
                    new SchemaAndValue(Schema.STRING_SCHEMA, value)));
            expected.put(topic + ":" + i, value);
        }
        Map<String, String> values = expected.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        values.forEach((key, value) -> redisConnection.sync().set(key, value));
        task.put(records);
        String[] keys = expected.keySet().toArray(new String[0]);
        for (String key : keys) {
            Assertions.assertEquals(3600, redisConnection.sync().ttl(key), 100);
        }
    }


    @Test
    void writesAndTombstonesKeepInputOrder() {
        startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, "STRING", RedisSinkConfigDef.KEYSPACE_CONFIG, ""));
        SinkRecord first = new SinkRecord("order", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "first", 0);
        SinkRecord deleted = new SinkRecord("order", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, 1);
        SinkRecord last = new SinkRecord("order", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "last", 2);
        task.put(List.of(first, deleted, last));
        assertEquals("last", redisConnection.sync().get("key"));
        task.put(List.of(last, deleted));
        Assertions.assertNull(redisConnection.sync().get("key"));
    }

    @ParameterizedTest
    @EnumSource(value = RedisType.class, names = {"LIST", "SET", "ZSET"})
    void collectionTombstonesRemoveMembers(RedisType type) {
        startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, type.name(), RedisSinkConfigDef.KEYSPACE_CONFIG, "collection"));
        SinkRecord add = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "member", Schema.FLOAT64_SCHEMA, 3.0, 0);
        SinkRecord remove = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "member", Schema.FLOAT64_SCHEMA, null, 1);
        task.put(List.of(add, remove));
        assertEquals(0, redisConnection.sync().exists("collection"));
    }

    @Test
    void invalidBatchIsRejectedBeforeAnyCommands() {
        startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, "STRING", RedisSinkConfigDef.KEYSPACE_CONFIG, ""));
        SinkRecord valid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "valid", Schema.STRING_SCHEMA, "value", 0);
        SinkRecord invalid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "invalid", Schema.INT32_SCHEMA, 42, 1);
        Assertions.assertThrows(DataException.class, () -> task.put(List.of(valid, invalid)));
        assertEquals(0, redisConnection.sync().exists("valid"));
    }

    @Test
    void commandErrorsArePropagated() {
        startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, "LIST", RedisSinkConfigDef.KEYSPACE_CONFIG, "wrongtype"));
        redisConnection.sync().set("wrongtype", "string");
        SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "member", Schema.STRING_SCHEMA, "value", 0);
        Assertions.assertThrows(ConnectException.class, () -> task.put(List.of(record)));
        assertEquals("string", redisConnection.sync().get("wrongtype"));
    }

    @Test
    void recordTtlAndWritesStayOrdered() {
        startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, "STRING", RedisSinkConfigDef.KEYSPACE_CONFIG, ""));
        SinkRecord expiring = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "first", 0);
        expiring.headers().addLong(RedisSinkConfigDef.KEY_TTL_CONFIG, 3600);
        SinkRecord persistent = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "last", 1);
        task.put(List.of(expiring, persistent));
        assertEquals(-1, redisConnection.sync().ttl("key"));
        task.put(List.of(persistent, expiring));
        Assertions.assertTrue(redisConnection.sync().ttl("key") > 3500);
    }

    @Test
    void malformedOffsetStateFailsInsteadOfSeeking() {
        startSinkTask(sinkProps("bad-state"));
        TopicPartition partition = new TopicPartition("topic", 0);
        redisConnection.sync().set(offsetKey("bad-state", partition), "{\"topic\":\"topic\",\"partition\":0}");
        Assertions.assertThrows(DataException.class, () -> task.open(List.of(partition)));
    }

    @Test
    void namespacedBinaryKeysKeepOriginalBytes() {
        startSinkTask(Map.of("redis.uri", getRedisServer().getRedisURI(), "redis.type", "STRING", "redis.keyspace", "prefix"));
        byte[] suffix = new byte[] {0, 1, (byte) 255};
        task.put(List.of(new SinkRecord("topic", 0, Schema.BYTES_SCHEMA, suffix, Schema.STRING_SCHEMA, "value", 0)));
        byte[] prefix = "prefix:".getBytes(StandardCharsets.UTF_8);
        byte[] key = Arrays.copyOf(prefix, prefix.length + suffix.length);
        System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
        try (RedisConnection<byte[], byte[]> binary =
                RedisConnection.open(redisClient, ByteArrayCodec.INSTANCE)) {
            RedisStringCommands<byte[], byte[]> strings = binary.sync(RedisStringCommands.class);
            Assertions.assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), strings.get(key));
        }
    }

    @Test
    void hashFieldWritesOneFieldPerRecord() {
        startHashFieldSinkTask("parents");
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"n\":1}", 0), hashFieldRecord("p1", "c2", "{\"n\":2}", 1),
                hashFieldRecord("p2", "c1", "{\"n\":3}", 2)));
        assertEquals(Map.of("c1", "{\"n\":1}", "c2", "{\"n\":2}"), redisConnection.sync().hgetall("parents:p1"));
        assertEquals(Map.of("c1", "{\"n\":3}"), redisConnection.sync().hgetall("parents:p2"));
    }

    @Test
    void hashFieldResolvesStructKeys() {
        Schema parentSchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
        Schema keySchema = SchemaBuilder.struct().field("parent", parentSchema).field("childId", Schema.INT64_SCHEMA).build();
        Struct key = new Struct(keySchema).put("parent", new Struct(parentSchema).put("id", "p1")).put("childId", 42L);
        startSinkTask(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, "HASH_FIELD", RedisSinkConfigDef.KEYSPACE_CONFIG, "",
                RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG, "key.parent.id", RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG, "key.childId"));
        task.put(List.of(new SinkRecord("topic", 0, keySchema, key, Schema.STRING_SCHEMA, "{\"n\":1}", 0)));
        assertEquals(Map.of("42", "{\"n\":1}"), redisConnection.sync().hgetall("p1"));
    }

    @Test
    void hashFieldStoresByteValuesUnchanged() {
        startHashFieldSinkTask("parents");
        // Schema Registry wire format: magic byte 0, a 4-byte schema id, then the Avro binary payload.
        byte[] value = {0, 0, 0, 0, 7, 2, (byte) 0xC3, (byte) 0xFF};
        task.put(List.of(new SinkRecord("topic", 0, null, Map.of("parentId", "p1", "childId", "c1"),
                Schema.BYTES_SCHEMA, value, 0)));
        try (RedisConnection<byte[], byte[]> binary = RedisConnection.open(redisClient, ByteArrayCodec.INSTANCE)) {
            RedisHashCommands<byte[], byte[]> hashes = binary.sync(RedisHashCommands.class);
            Assertions.assertArrayEquals(value, hashes.hget(bytes("parents:p1"), bytes("c1")));
        }
    }

    // Connect BYTES values may be a ByteBuffer; only its remaining bytes are the value.
    @Test
    void hashFieldStoresByteBufferValuesUnchanged() {
        startHashFieldSinkTask("parents");
        ByteBuffer value = ByteBuffer.wrap(new byte[] {9, 0, 7, (byte) 0xFF});
        value.position(1);
        task.put(List.of(new SinkRecord("topic", 0, null, Map.of("parentId", "p1", "childId", "c1"),
                Schema.BYTES_SCHEMA, value, 0)));
        try (RedisConnection<byte[], byte[]> binary = RedisConnection.open(redisClient, ByteArrayCodec.INSTANCE)) {
            RedisHashCommands<byte[], byte[]> hashes = binary.sync(RedisHashCommands.class);
            Assertions.assertArrayEquals(new byte[] {0, 7, (byte) 0xFF}, hashes.hget(bytes("parents:p1"), bytes("c1")));
        }
    }

    @Test
    void hashFieldStoresStringValuesUnchanged() {
        startHashFieldSinkTask("parents");
        task.put(List.of(hashFieldRecord("p1", "c1", "plain text, not JSON", 0)));
        assertEquals("plain text, not JSON", redisConnection.sync().hget("parents:p1", "c1"));
    }

    @Test
    void hashFieldSerializesStructValuesAsJson() {
        startHashFieldSinkTask("parents");
        Schema valueSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).field("count", Schema.INT64_SCHEMA)
                .field("note", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("name", "widget").put("count", 3L);
        Schema keySchema = SchemaBuilder.struct().field("parentId", Schema.STRING_SCHEMA).field("childId", Schema.STRING_SCHEMA).build();
        Struct key = new Struct(keySchema).put("parentId", "p1").put("childId", "c1");
        task.put(List.of(new SinkRecord("topic", 0, keySchema, key, valueSchema, value, 0)));
        assertEquals("{\"name\":\"widget\",\"count\":3,\"note\":null}", redisConnection.sync().hget("parents:p1", "c1"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void hashFieldTombstoneRemovesOnlyItsField() {
        startHashFieldSinkTask("parents");
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"n\":1}", 0), hashFieldRecord("p1", "c2", "{\"n\":2}", 1)));
        task.put(List.of(hashFieldRecord("p1", "c1", null, 2)));
        assertEquals(Map.of("c2", "{\"n\":2}"), redisConnection.sync().hgetall("parents:p1"));
        task.put(List.of(hashFieldRecord("p1", "c2", null, 3)));
        assertEquals(0, redisConnection.sync().exists("parents:p1"));
    }

    @Test
    void hashFieldWritesAndTombstonesKeepInputOrder() {
        startHashFieldSinkTask("parents");
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"v\":1}", 0), hashFieldRecord("p1", "c1", null, 1),
                hashFieldRecord("p1", "c1", "{\"v\":2}", 2)));
        assertEquals("{\"v\":2}", redisConnection.sync().hget("parents:p1", "c1"));
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"v\":3}", 3), hashFieldRecord("p1", "c1", null, 4)));
        Assertions.assertFalse(redisConnection.sync().hexists("parents:p1", "c1"));
    }

    @Test
    void hashFieldUnresolvableKeyRejectsBatchBeforeWrites() {
        startHashFieldSinkTask("parents");
        SinkRecord valid = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        SinkRecord missingChild = new SinkRecord("topic", 0, null, Map.of("parentId", "p1"), Schema.STRING_SCHEMA, "{\"n\":2}", 1);
        DataException error = Assertions.assertThrows(DataException.class, () -> task.put(List.of(valid, missingChild)));
        Assertions.assertTrue(error.getMessage().contains("key.childId"), error.getMessage());
        assertEquals(0, redisConnection.sync().exists("parents:p1"));
    }

    @Test
    void hashFieldKeyTtlExpiresTheWholeHash() {
        startSinkTask(hashFieldProps("parents", RedisSinkConfigDef.KEY_TTL_CONFIG, "3600"));
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"n\":1}", 0)));
        Assertions.assertTrue(redisConnection.sync().ttl("parents:p1") > 3500);
        SinkRecord expiring = hashFieldRecord("p2", "c1", "{\"n\":2}", 1);
        expiring.headers().addLong(RedisSinkConfigDef.KEY_TTL_CONFIG, 60);
        task.put(List.of(expiring));
        long ttl = redisConnection.sync().ttl("parents:p2");
        Assertions.assertTrue(ttl > 0 && ttl <= 60, "the key TTL header overrides the configured key TTL: " + ttl);
        if (hexpireAvailable()) {
            assertEquals(-1L, redisConnection.sync().httl("parents:p1", "c1").get(0), "the field itself must not expire");
        }
    }

    @Test
    void hashFieldConfiguredTtlExpiresOnlyTheField() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true", HASH_FIELD_TTL, "3600"));
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"n\":1}", 0)));
        Assertions.assertTrue(redisConnection.sync().httl("parents:p1", "c1").get(0) > 3500);
        assertEquals(-1, redisConnection.sync().ttl("parents:p1"), "the hash itself must not expire");
    }

    @Test
    void hashFieldHeaderTtlAppliesOnlyToItsRecord() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true"));
        SinkRecord expiring = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        expiring.headers().addLong(HASH_FIELD_TTL, 3600);
        task.put(List.of(expiring, hashFieldRecord("p1", "c2", "{\"n\":2}", 1)));
        Assertions.assertTrue(redisConnection.sync().httl("parents:p1", "c1").get(0) > 3500);
        assertEquals(-1L, redisConnection.sync().httl("parents:p1", "c2").get(0), "TTL must not carry over");
        assertEquals(-1, redisConnection.sync().ttl("parents:p1"));
    }

    @Test
    void hashFieldHeaderTtlOverridesConfiguredTtl() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true", HASH_FIELD_TTL, "60"));
        SinkRecord expiring = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        expiring.headers().addString(HASH_FIELD_TTL, "3600");
        task.put(List.of(expiring, hashFieldRecord("p1", "c2", "{\"n\":2}", 1)));
        Assertions.assertTrue(redisConnection.sync().httl("parents:p1", "c1").get(0) > 3500);
        long configured = redisConnection.sync().httl("parents:p1", "c2").get(0);
        Assertions.assertTrue(configured > 0 && configured <= 60, "records without the header use the configured TTL");
    }

    @Test
    void hashFieldKeyAndFieldTtlsCombine() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", RedisSinkConfigDef.KEY_TTL_CONFIG, "3600", HASH_FIELD_EXPIRE, "true",
                HASH_FIELD_TTL, "60"));
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"n\":1}", 0)));
        Assertions.assertTrue(redisConnection.sync().ttl("parents:p1") > 3500);
        long field = redisConnection.sync().httl("parents:p1", "c1").get(0);
        Assertions.assertTrue(field > 0 && field <= 60, "field TTL: " + field);
    }

    @Test
    void hashFieldOverwriteWithoutTtlClearsTheFieldTtl() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true"));
        SinkRecord expiring = hashFieldRecord("p1", "c1", "{\"v\":1}", 0);
        expiring.headers().addLong(HASH_FIELD_TTL, 3600);
        SinkRecord persistent = hashFieldRecord("p1", "c1", "{\"v\":2}", 1);
        // The later record has no TTL, so overwriting the field must not keep the earlier record's TTL.
        task.put(List.of(expiring, persistent));
        assertEquals("{\"v\":2}", redisConnection.sync().hget("parents:p1", "c1"));
        assertEquals(-1L, redisConnection.sync().httl("parents:p1", "c1").get(0));
        // The reverse order ends with the TTL from the last record.
        task.put(List.of(persistent, expiring));
        Assertions.assertTrue(redisConnection.sync().httl("parents:p1", "c1").get(0) > 3500);
    }

    @Test
    void hashFieldHeaderTtlIgnoredWhileExpireDisabled() {
        // No HEXPIRE assumption: with field expiry disabled, the header is ignored on every server.
        startHashFieldSinkTask("parents");
        SinkRecord expiring = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        expiring.headers().addLong(HASH_FIELD_TTL, 3600);
        task.put(List.of(expiring));
        assertEquals("{\"n\":1}", redisConnection.sync().hget("parents:p1", "c1"));
        assertEquals(-1, redisConnection.sync().ttl("parents:p1"));
        if (hexpireAvailable()) {
            assertEquals(-1L, redisConnection.sync().httl("parents:p1", "c1").get(0));
        }
    }

    @Test
    void hashFieldInvalidHeaderTtlRejectsBatch() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true"));
        SinkRecord invalid = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        invalid.headers().addString(HASH_FIELD_TTL, "soon");
        DataException error = Assertions.assertThrows(DataException.class, () -> task.put(List.of(invalid)));
        Assertions.assertTrue(error.getMessage().contains("Invalid " + HASH_FIELD_TTL + " header"), error.getMessage());
        assertEquals(0, redisConnection.sync().exists("parents:p1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "-5"})
    void hashFieldHeaderTtlOfZeroOrBelowMeansNoExpiry(String ttl) {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true", HASH_FIELD_TTL, "60"));
        SinkRecord persistent = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        persistent.headers().addString(HASH_FIELD_TTL, ttl);
        task.put(List.of(persistent));
        assertEquals(-1L, redisConnection.sync().httl("parents:p1", "c1").get(0));
    }

    @Test
    void hashFieldHeaderTtlAboveMaximumRejectsBatch() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true"));
        SinkRecord invalid = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        invalid.headers().addLong(HASH_FIELD_TTL, RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_MAX + 1);
        DataException error = Assertions.assertThrows(DataException.class, () -> task.put(List.of(invalid)));
        Assertions.assertTrue(error.getMessage().contains("above " + RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_MAX),
                error.getMessage());
        assertEquals(0, redisConnection.sync().exists("parents:p1"));
    }

    @Test
    void hashFieldHeaderTtlAcceptsBytes() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true"));
        SinkRecord expiring = hashFieldRecord("p1", "c1", "{\"n\":1}", 0);
        expiring.headers().addBytes(HASH_FIELD_TTL, bytes("3600"));
        task.put(List.of(expiring));
        Assertions.assertTrue(redisConnection.sync().httl("parents:p1", "c1").get(0) > 3500);
    }

    @Test
    void hashFieldDefaultKeyspaceIsTheTopic() {
        Map<String, String> props = hashFieldProps("");
        props.remove(RedisSinkConfigDef.KEYSPACE_CONFIG);
        startSinkTask(props);
        task.put(List.of(hashFieldRecord("p1", "c1", "{\"n\":1}", 0)));
        assertEquals(Map.of("c1", "{\"n\":1}"), redisConnection.sync().hgetall("topic:p1"));
    }

    // On a cluster the hashes hash to different slots, so one batch spans several nodes.
    @Test
    void hashFieldBatchAcrossHashesAppliesBothTtls() {
        Assumptions.assumeTrue(hexpireAvailable(), "HEXPIRE not available");
        startSinkTask(hashFieldProps("parents", RedisSinkConfigDef.KEY_TTL_CONFIG, "3600", HASH_FIELD_EXPIRE, "true",
                HASH_FIELD_TTL, "60"));
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(hashFieldRecord("p" + i, "c1", "{\"n\":" + i + "}", i));
        }
        task.put(records);
        for (int i = 0; i < 10; i++) {
            String hash = "parents:p" + i;
            assertEquals("{\"n\":" + i + "}", redisConnection.sync().hget(hash, "c1"));
            Assertions.assertTrue(redisConnection.sync().ttl(hash) > 3500, hash);
            long field = redisConnection.sync().httl(hash, "c1").get(0);
            Assertions.assertTrue(field > 0 && field <= 60, hash + " field TTL: " + field);
        }
    }

    // An Avro decimal key field resolves to the same hash whatever its scale.
    @Test
    void hashFieldResolvesDecimalKeysCanonically() {
        startHashFieldSinkTask("parents");
        Schema keySchema = SchemaBuilder.struct().field("parentId", Decimal.schema(3))
                .field("childId", Schema.STRING_SCHEMA).build();
        Struct key = new Struct(keySchema).put("parentId", new BigDecimal("12.500")).put("childId", "c1");
        task.put(List.of(new SinkRecord("topic", 0, keySchema, key, Schema.STRING_SCHEMA, "{\"n\":1}", 0)));
        assertEquals("{\"n\":1}", redisConnection.sync().hget("parents:12.5", "c1"));
    }

    @Test
    void hashFieldExpireWithoutHexpireFailsStart() {
        Assumptions.assumeFalse(hexpireAvailable(), "HEXPIRE available");
        ConnectException error = Assertions.assertThrows(ConnectException.class,
                () -> startSinkTask(hashFieldProps("parents", HASH_FIELD_EXPIRE, "true")));
        Assertions.assertTrue(error.getMessage().contains("requires a Redis server that supports HEXPIRE"),
                error.getMessage());
    }

    private static final String HASH_FIELD_EXPIRE = RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG;

    private static final String HASH_FIELD_TTL = RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG;

    // Whether the server under test supports hash field expiration (HEXPIRE).
    private boolean hexpireAvailable() {
        try {
            redisConnection.sync().hexpire("hexpire-check", 1, "field");
            return true;
        } catch (RedisCommandExecutionException e) {
            if (String.valueOf(e.getMessage()).startsWith("ERR unknown command")) {
                return false;
            }
            throw e;
        }
    }

    // HASH_FIELD sink properties plus extra name/value pairs.
    private Map<String, String> hashFieldProps(String keyspace, String... extra) {
        Map<String, String> props = new LinkedHashMap<>(Map.of(RedisSinkConfigDef.URI_CONFIG, getRedisServer().getRedisURI(),
                RedisSinkConfigDef.TYPE_CONFIG, "HASH_FIELD", RedisSinkConfigDef.KEYSPACE_CONFIG, keyspace,
                RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG, "key.parentId", RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG, "key.childId"));
        for (int i = 0; i < extra.length; i += 2) {
            props.put(extra[i], extra[i + 1]);
        }
        return props;
    }

    private void startHashFieldSinkTask(String keyspace) {
        startSinkTask(hashFieldProps(keyspace));
    }

    // A schemaless Map key, as produced by JsonConverter with schemas.enable=false.
    private static SinkRecord hashFieldRecord(String parentId, String childId, String json, long offset) {
        return new SinkRecord("topic", 0, null, Map.of("parentId", parentId, "childId", childId),
                Schema.OPTIONAL_STRING_SCHEMA, json, offset);
    }

    @ParameterizedTest
    @EnumSource(value = RedisType.class, names = {"HASH", "STREAM"})
    void nullMapFieldsRejectWholeBatchBeforeWrites(RedisType type) {
        startSinkTask(Map.of("redis.uri", getRedisServer().getRedisURI(), "redis.type", type.name(), "redis.keyspace", ""));
        Map<String, Object> invalidBody = new LinkedHashMap<>();
        invalidBody.put("valid", "value");
        invalidBody.put("invalid", null);
        SinkRecord valid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "valid", null, Map.of("field", "value"), 0);
        SinkRecord invalid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "bad", null, invalidBody, 1);
        Assertions.assertThrows(DataException.class, () -> task.put(List.of(valid, invalid)));
        assertEquals(0, redisConnection.sync().exists("valid"));
        assertEquals(0, redisConnection.sync().exists("bad"));
        if (type == RedisType.STREAM) {
            assertEquals(0, redisConnection.sync().exists(""), "STREAM writes target the collection keyspace");
        }
    }

    @Test
    void malformedJsonBatchIsRejectedBeforeWrites() {
        startSinkTask(Map.of("redis.uri", getRedisServer().getRedisURI(), "redis.type", "JSON", "redis.keyspace", ""));
        SinkRecord valid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "valid", Schema.STRING_SCHEMA, "{\"id\":1}", 0);
        SinkRecord invalid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "bad", Schema.STRING_SCHEMA, "{oops", 1);
        Assertions.assertThrows(DataException.class, () -> task.put(List.of(valid, invalid)));
        assertEquals(0, redisConnection.sync().exists("valid"));
    }
}
