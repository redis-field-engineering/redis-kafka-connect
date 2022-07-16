package com.redis.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.kafka.connect.sink.RedisSinkConfig;
import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

class RedisSinkTaskIT extends AbstractTestcontainersRedisTestBase {

	@SuppressWarnings("resource")
	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(
				new RedisModulesContainer(
						RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG)),
				new RedisEnterpriseContainer(
						RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG))
						.withDatabase(Database.name("RedisKafkaConnectTests").memory(DataSize.ofMegabytes(100))
								.ossCluster(true).modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.TIMESERIES)
								.build()));
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

	@ParameterizedTest
	@RedisTestContextsSource
	void emptyAssignment(RedisTestContext context) {
		SinkTaskContext taskContext = mock(SinkTaskContext.class);
		when(taskContext.assignment()).thenReturn(ImmutableSet.of());
		this.task.initialize(taskContext);
		this.task.start(ImmutableMap.of(RedisSinkConfig.URI_CONFIG, context.getServer().getRedisURI()));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putEmpty(RedisTestContext redis) {
		String topic = "putWrite";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.of(RedisSinkConfig.URI_CONFIG, redis.getServer().getRedisURI()));
		this.task.put(ImmutableList.of());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putHash(RedisTestContext redis) {
		String topic = "hash";
		int count = 50;
		Map<String, Map<String, String>> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			Map<String, String> map = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
			expected.put("hash:" + i, map);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, i),
					new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), map)));
		}
		put(topic, RedisSinkConfig.DataType.HASH, redis, records);
		for (String key : expected.keySet()) {
			Map<String, String> hash = expected.get(key);
			Map<String, String> actual = redis.sync().hgetall(key);
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

	@ParameterizedTest
	@RedisTestContextsSource
	void putJSON(RedisTestContext redis) throws JsonProcessingException {
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
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, person.getId()),
					new SchemaAndValue(Schema.STRING_SCHEMA, json)));
		}
		put(topic, RedisSinkConfig.DataType.JSON, redis, records);
		for (Person person : persons) {
			String json = redis.sync().jsonGet(topic + ":" + person.getId());
			assertEquals(person, mapper.readValue(json, Person.class));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putTimeSeries(RedisTestContext redis) {
		String topic = "putTimeSeries";
		int count = 50;
		long startTime = System.currentTimeMillis() - count;
		List<Sample> expectedSamples = new ArrayList<>();
		List<SinkRecord> records = new ArrayList<>();
		for (int index = 1; index <= count; index++) {
			long timestamp = startTime + index;
			double value = index;
			expectedSamples.add(Sample.of(timestamp, value));
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.INT64_SCHEMA, timestamp),
					new SchemaAndValue(Schema.FLOAT64_SCHEMA, value)));
		}
		put(topic, RedisSinkConfig.DataType.TIMESERIES, redis, records);
		List<Sample> actualSamples = redis.sync().tsRange(topic, TimeRange.unbounded());
		assertEquals(expectedSamples.size(), actualSamples.size());
		for (int index = 0; index < expectedSamples.size(); index++) {
			Sample expectedSample = expectedSamples.get(index);
			Sample actualSample = actualSamples.get(index);
			assertEquals(expectedSample.getTimestamp(), actualSample.getTimestamp());
			assertEquals(expectedSample.getValue(), actualSample.getValue());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putLpush(RedisTestContext redis) {
		String topic = "putLpush";
		int count = 50;
		List<String> expected = new ArrayList<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String member = "listmember:" + i;
			expected.add(member);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
					new SchemaAndValue(Schema.STRING_SCHEMA, member)));
		}
		put(topic, RedisSinkConfig.DataType.LIST, redis, records);
		List<String> actual = redis.sync().lrange(topic, 0, -1);
		Collections.reverse(actual);
		assertEquals(expected, actual);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putRpush(RedisTestContext redis) {
		String topic = "putRpush";
		int count = 50;
		List<String> expected = new ArrayList<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String member = "listmember:" + i;
			expected.add(member);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
					new SchemaAndValue(Schema.STRING_SCHEMA, member)));
		}
		put(topic, RedisSinkConfig.DataType.LIST, redis, records, RedisSinkConfig.PUSH_DIRECTION_CONFIG,
				RedisSinkConfig.PushDirection.RIGHT.name());
		List<String> actual = redis.sync().lrange(topic, 0, -1);
		assertEquals(expected, actual);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putSet(RedisTestContext redis) {
		String topic = "putSet";
		int count = 50;
		Set<String> expected = new HashSet<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String member = "setmember:" + i;
			expected.add(member);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, member),
					new SchemaAndValue(Schema.STRING_SCHEMA, member)));
		}
		put(topic, RedisSinkConfig.DataType.SET, redis, records);
		Set<String> members = redis.sync().smembers(topic);
		assertEquals(expected, members);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putStream(RedisTestContext redis) {
		String topic = "putStream";
		int count = 50;
		List<Map<String, String>> expected = new ArrayList<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			Map<String, String> body = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
			expected.add(body);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "key" + i),
					new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), body)));
		}
		put(topic, RedisSinkConfig.DataType.STREAM, redis, records);
		List<StreamMessage<String, String>> messages = redis.sync().xrange(topic, Range.unbounded());
		assertEquals(records.size(), messages.size());
		for (int index = 0; index < messages.size(); index++) {
			Map<String, String> body = expected.get(index);
			StreamMessage<String, String> message = messages.get(index);
			assertEquals(body, message.getBody(), String.format("Body for message #%s does not match.", index));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putString(RedisTestContext redis) {
		String topic = "string";
		int count = 50;
		Map<String, String> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String key = String.valueOf(i);
			String value = "This is value " + i;
			expected.put(topic + ":" + key, value);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key),
					new SchemaAndValue(Schema.STRING_SCHEMA, value)));
		}
		put(topic, RedisSinkConfig.DataType.STRING, redis, records);
		String[] keys = expected.keySet().toArray(new String[0]);
		List<KeyValue<String, String>> actual = redis.sync().mget(keys);
		assertEquals(records.size(), actual.size());
		for (KeyValue<String, String> keyValue : actual) {
			assertEquals(expected.get(keyValue.getKey()), keyValue.getValue(),
					String.format("Value for key '%s' does not match.", keyValue.getKey()));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void setBytes(RedisTestContext redis) {
		String topic = "setBytes";
		int count = 50;
		Map<String, String> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String key = topic + i;
			String value = "This is value " + i;
			expected.put(key, value);
			records.add(SinkRecordHelper.write(topic,
					new SchemaAndValue(Schema.BYTES_SCHEMA, key.getBytes(StandardCharsets.UTF_8)),
					new SchemaAndValue(Schema.BYTES_SCHEMA, value.getBytes(StandardCharsets.UTF_8))));
		}
		put(topic, RedisSinkConfig.DataType.STRING, redis, records, RedisSinkConfig.KEY_CONFIG, "");
		String[] keys = expected.keySet().toArray(new String[0]);
		List<KeyValue<String, String>> actual = redis.sync().mget(keys);
		assertEquals(records.size(), actual.size());
		for (KeyValue<String, String> keyValue : actual) {
			assertEquals(expected.get(keyValue.getKey()), keyValue.getValue(),
					String.format("Value for key '%s' does not match.", keyValue.getKey()));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putZset(RedisTestContext context) {
		String topic = "putZset";
		int count = 50;
		List<ScoredValue<String>> expected = new ArrayList<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String value = "zsetmember:" + i;
			expected.add(ScoredValue.just(i, value));
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, value),
					new SchemaAndValue(Schema.FLOAT64_SCHEMA, i)));
		}
		put(topic, RedisSinkConfig.DataType.ZSET, context, records);
		List<ScoredValue<String>> actual = context.sync().zrangeWithScores(topic, 0, -1);
		expected.sort(Comparator.comparing(ScoredValue::getScore));
		assertEquals(expected, actual);
	}

	public void put(String topic, RedisSinkConfig.DataType type, RedisTestContext context, List<SinkRecord> records,
			String... props) {
		SinkTaskContext taskContext = mock(SinkTaskContext.class);
		when(taskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		task.initialize(taskContext);
		Map<String, String> propsMap = map(RedisSinkConfig.URI_CONFIG, context.getServer().getRedisURI(),
				RedisSinkConfig.TYPE_CONFIG, type.name());
		propsMap.putAll(map(props));
		task.start(propsMap);
		task.put(records);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putDelete(RedisTestContext context) {
		String topic = "putDelete";
		SinkTaskContext taskContext = mock(SinkTaskContext.class);
		when(taskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(taskContext);
		this.task.start(ImmutableMap.of(RedisSinkConfig.URI_CONFIG, context.getServer().getRedisURI(),
				RedisSinkConfig.TYPE_CONFIG, RedisSinkConfig.DataType.STRING.name()));

		int count = 50;
		Map<String, String> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);

		for (int i = 0; i < count; i++) {
			final String value = "This is value " + i;
			records.add(SinkRecordHelper.delete(topic, new SchemaAndValue(Schema.STRING_SCHEMA, i)));
			expected.put(topic + ":" + i, value);
		}
		Map<String, String> values = expected.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		context.sync().mset(values);
		task.put(records);
		String[] keys = expected.keySet().toArray(new String[0]);
		long actual = context.sync().exists(keys);
		assertEquals(0L, actual, "All of the keys should be removed from Redis.");
	}

}
