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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.redis.kafka.connect.sink.RedisEnterpriseSinkConfig;
import com.redis.kafka.connect.sink.RedisEnterpriseSinkTask;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.jupiter.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

class RedisEnterpriseSinkTaskIT extends AbstractTestcontainersRedisTestBase {

	@Container
	private static final RedisModulesContainer REDIS = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS);
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
	@RedisTestContextsSource
	void emptyAssignment(RedisTestContext context) {
		SinkTaskContext taskContext = mock(SinkTaskContext.class);
		when(taskContext.assignment()).thenReturn(ImmutableSet.of());
		this.task.initialize(taskContext);
		this.task.start(ImmutableMap.of(RedisEnterpriseSinkConfig.REDIS_URI_CONFIG, context.getServer().getRedisURI()));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putEmpty(RedisTestContext redis) {
		String topic = "putWrite";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.of(RedisEnterpriseSinkConfig.REDIS_URI_CONFIG, redis.getServer().getRedisURI()));
		this.task.put(ImmutableList.of());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void putHash(RedisTestContext redis) {
		String topic = "putHash";
		int count = 50;
		Map<String, Map<String, String>> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String key = "key" + i;
			Map<String, String> map = map("field1", "This is field1 value" + i, "field2", "This is field2 value " + i);
			expected.put(key, map);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key),
					new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), map)));
		}
		put(topic, RedisEnterpriseSinkConfig.DataType.HASH, redis, records);
		for (String key : expected.keySet()) {
			Map<String, String> hash = expected.get(key);
			Map<String, String> actual = redis.sync().hgetall(key);
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
	@RedisTestContextsSource
	void putJSON(RedisTestContext redis) throws JsonProcessingException {
		String topic = "putJSON";
		List<Person> persons = new ArrayList<>();
		persons.add(Person.builder().id(1).name("Bodysnatch Cummerbund")
				.address(Address.builder().city("New York").zip("10013").state("NY").street("150 Mott St").build())
				.hobby("Fishing").hobby("Singing").build());
		persons.add(Person.builder().id(2).name("Buffalo Custardbath").address(
				Address.builder().city("Los Angeles").zip("90001").state("CA").street("123 Sunset Blvd").build())
				.hobby("Surfing").hobby("Piano").build());
		persons.add(Person.builder().id(3).name("Bumblesnuff Crimpysnitch")
				.address(Address.builder().city("Chicago").zip("60603").state("IL").street("100 S State St").build())
				.hobby("Skiing").hobby("Drums").build());
		List<SinkRecord> records = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		for (Person person : persons) {
			records.add(
					SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, "person:" + person.getId()),
							new SchemaAndValue(Schema.STRING_SCHEMA, mapper.writeValueAsString(person))));
		}
		put(topic, RedisEnterpriseSinkConfig.DataType.JSON, redis, records);
		for (Person person : persons) {
			String json = redis.sync().jsonGet("person:" + person.getId());
			assertEquals(mapper.writeValueAsString(person), json);
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
		put(topic, RedisEnterpriseSinkConfig.DataType.TIMESERIES, redis, records);
		List<Sample> actualSamples = redis.sync().range(topic, RangeOptions.from(0).to(0).build());
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
		put(topic, RedisEnterpriseSinkConfig.DataType.LIST, redis, records);
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
		put(topic, RedisEnterpriseSinkConfig.DataType.LIST, redis, records,
				RedisEnterpriseSinkConfig.PUSH_DIRECTION_CONFIG, RedisEnterpriseSinkConfig.PushDirection.RIGHT.name());
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
		put(topic, RedisEnterpriseSinkConfig.DataType.SET, redis, records);
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
		put(topic, RedisEnterpriseSinkConfig.DataType.STREAM, redis, records);
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
		String topic = "putString";
		int count = 50;
		Map<String, String> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			String key = topic + i;
			String value = "This is value " + i;
			expected.put(key, value);
			records.add(SinkRecordHelper.write(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key),
					new SchemaAndValue(Schema.STRING_SCHEMA, value)));
		}
		put(topic, RedisEnterpriseSinkConfig.DataType.STRING, redis, records);
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
		put(topic, RedisEnterpriseSinkConfig.DataType.STRING, redis, records);
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
		put(topic, RedisEnterpriseSinkConfig.DataType.ZSET, context, records);
		List<ScoredValue<String>> actual = context.sync().zrangeWithScores(topic, 0, -1);
		expected.sort(Comparator.comparing(ScoredValue::getScore));
		assertEquals(expected, actual);
	}

	public void put(String topic, RedisEnterpriseSinkConfig.DataType type, RedisTestContext context,
			List<SinkRecord> records, String... props) {
		SinkTaskContext taskContext = mock(SinkTaskContext.class);
		when(taskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		task.initialize(taskContext);
		Map<String, String> propsMap = map(RedisEnterpriseSinkConfig.REDIS_URI_CONFIG,
				context.getServer().getRedisURI(), RedisEnterpriseSinkConfig.TYPE_CONFIG, type.name());
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
		this.task.start(ImmutableMap.of(RedisEnterpriseSinkConfig.REDIS_URI_CONFIG, context.getServer().getRedisURI(),
				RedisEnterpriseSinkConfig.TYPE_CONFIG, RedisEnterpriseSinkConfig.DataType.STRING.name()));

		int count = 50;
		Map<String, String> expected = new LinkedHashMap<>(count);
		List<SinkRecord> records = new ArrayList<>(count);

		for (int i = 0; i < count; i++) {
			final String key = topic + i;
			final String value = "This is value " + i;
			records.add(SinkRecordHelper.delete(topic, new SchemaAndValue(Schema.STRING_SCHEMA, key)));
			expected.put(key, value);
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
