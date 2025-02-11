package com.redis.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.source.RedisKeysSourceConfigDef;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.RedisStreamSourceConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceTask;
import com.redis.kafka.connect.source.ToStructFunction;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.test.AbstractTestBase;

import io.lettuce.core.models.stream.PendingMessages;

abstract class AbstractSourceIntegrationTests extends AbstractTestBase {

	private static final Logger log = LoggerFactory.getLogger(AbstractSourceIntegrationTests.class);

	private RedisStreamSourceTask streamSourceTask;
	private RedisKeysSourceTask keysSourceTask;

	@BeforeEach
	public void setupTasks() {
		streamSourceTask = new RedisStreamSourceTask();
		keysSourceTask = new RedisKeysSourceTask();
	}

	@AfterEach
	public void teardownTasks() {
		keysSourceTask.stop();
		streamSourceTask.stop();
	}

	// Used to initialize a task with a previous connect offset (as though records
	// had been committed).
	void initializeTask(String id) throws Exception {
		streamSourceTask.initialize(new SourceTaskContext() {

			@Override
			public OffsetStorageReader offsetStorageReader() {
				return new OffsetStorageReader() {

					@Override
					public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
						throw new UnsupportedOperationException("OffsetStorageReader.offsets()");
					}

					@Override
					public <T> Map<String, Object> offset(Map<String, T> partition) {
						return Collections.singletonMap(RedisStreamSourceTask.OFFSET_FIELD, id);
					}

				};
			}

			@Override
			public Map<String, String> configs() {
				throw new UnsupportedOperationException("SourceTaskContext.configs()");
			}

		});
		keysSourceTask.initialize(new SourceTaskContext() {

			@Override
			public OffsetStorageReader offsetStorageReader() {
				return null;
			}

			@Override
			public Map<String, String> configs() {
				throw new UnsupportedOperationException("SourceTaskContext.configs()");
			}

		});
	}

	private void startTask(SourceTask task, String... props) {
		Map<String, String> config = map(props);
		config.put(RedisConfigDef.URI_CONFIG, getRedisServer().getRedisURI());
		task.start(config);
	}

	private void startStreamSourceTask(String... props) {
		startTask(streamSourceTask, props);
	}

	private void startKeysSourceTask(String... props) {
		startTask(keysSourceTask, props);
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

	@Test
	void pollStreamAtMostOnce() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
				RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = redisConnection.sync().xadd(stream, body);
		String id2 = redisConnection.sync().xadd(stream, body);
		String id3 = redisConnection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
		assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
		assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
		PendingMessages pendingMsgs = redisConnection.sync().xpending(stream,
				RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(0, pendingMsgs.getCount(), "pending messages");
	}

	@Test
	void pollStreamAtLeastOnce() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = redisConnection.sync().xadd(stream, body);
		String id2 = redisConnection.sync().xadd(stream, body);
		String id3 = redisConnection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
		assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
		assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
		PendingMessages pendingMsgsBeforeCommit = redisConnection.sync().xpending(stream,
				RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
		streamSourceTask.commitRecord(sourceRecords.get(0), new RecordMetadata(null, 0, 0, 0, 0, 0));
		streamSourceTask.commitRecord(sourceRecords.get(1), new RecordMetadata(null, 0, 0, 0, 0, 0));
		streamSourceTask.commit();
		PendingMessages pendingMsgsAfterCommit = redisConnection.sync().xpending(stream,
				RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
	}

	@Test
	void pollStreamAtLeastOnceRecover() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);

		// create a new task, same config
		setupTasks();
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);

		Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));

		Assertions.assertEquals(6, recoveredRecords.size());
	}

	@Test
	void pollStreamAtLeastOnceRecoverUncommitted() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		String id3 = redisConnection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		streamSourceTask.commitRecord(sourceRecords.get(0), new RecordMetadata(null, 0, 0, 0, 0, 0));
		streamSourceTask.commitRecord(sourceRecords.get(1), new RecordMetadata(null, 0, 0, 0, 0, 0));
		streamSourceTask.commit();

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = redisConnection.sync().xadd(stream, body);
		String id5 = redisConnection.sync().xadd(stream, body);
		String id6 = redisConnection.sync().xadd(stream, body);

		// create a new task, same config
		setupTasks();
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));
		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
	}

	@Test
	void pollStreamAtLeastOnceRecoverFromOffset() throws Exception {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = redisConnection.sync().xadd(stream, body);
		log.info("ID1: " + id1);
		String id2 = redisConnection.sync().xadd(stream, body);
		log.info("ID2: " + id2);
		String id3 = redisConnection.sync().xadd(stream, body);
		log.info("ID3: " + id3);
		List<SourceRecord> records = new ArrayList<>();
		Awaitility.await().until(() -> records.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, records.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = redisConnection.sync().xadd(stream, body);
		log.info("ID4: " + id4);
		String id5 = redisConnection.sync().xadd(stream, body);
		log.info("ID5: " + id5);
		String id6 = redisConnection.sync().xadd(stream, body);
		log.info("ID6: " + id6);

		// create a new task, same config
		setupTasks();
		// this means connect committed records, but StreamSourceTask didn't get a
		// chance to ack first
		initializeTask(id3);
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));

		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id4, id5, id6), recoveredIds, "recoveredIds");
	}

	@Test
	void pollStreamAtMostOnceRecover() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
				RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = redisConnection.sync().xadd(stream, body);
		String id5 = redisConnection.sync().xadd(stream, body);
		String id6 = redisConnection.sync().xadd(stream, body);

		// create a new task, same config
		setupTasks();
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
				RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));
		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");
	}

	@Test
	void pollStreamRecoverAtLeastOnceToAtMostOnce() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
				RedisStreamSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		redisConnection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(streamSourceTask.poll()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = redisConnection.sync().xadd(stream, body);
		String id5 = redisConnection.sync().xadd(stream, body);
		String id6 = redisConnection.sync().xadd(stream, body);

		// create a new task, same config except AT_MOST_ONCE
		setupTasks();
		startStreamSourceTask(RedisStreamSourceConfigDef.TOPIC_CONFIG,
				topicPrefix + RedisStreamSourceConfigDef.TOKEN_STREAM, RedisStreamSourceConfigDef.STREAM_NAME_CONFIG,
				stream, RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG,
				RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(streamSourceTask.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(streamSourceTask.poll()));
		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

		PendingMessages pending = redisConnection.sync().xpending(stream,
				RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(0, pending.getCount(), "pending message count");
	}

	private void assertEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
			String expectedTopic, SourceRecord record) {
		Struct struct = (Struct) record.value();
		Assertions.assertEquals(expectedId, struct.get("id"));
		Assertions.assertEquals(expectedBody, struct.get("body"));
		Assertions.assertEquals(expectedStream, struct.get("stream"));
		Assertions.assertEquals(expectedTopic, record.topic());
	}

	@Test
	void pollKeys(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		String topic = "mytopic";
		startKeysSourceTask(RedisKeysSourceConfigDef.TOPIC_CONFIG, topic, RedisKeysSourceConfigDef.IDLE_TIMEOUT_CONFIG,
				"500");
		int count = 100;
		final List<SourceRecord> sourceRecords = new ArrayList<>();
		Executors.newSingleThreadScheduledExecutor().execute(() -> {
			GeneratorItemReader generator = generator(count);
			RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
			writer.setClient(redisClient);
			try {
				run(info, step(info, 1, generator, null, writer));
			} catch (Exception e) {
				throw new RuntimeException("Could not execute data gen");
			}
		});
		awaitUntil(() -> {
			List<SourceRecord> records = keysSourceTask.poll();
			log.info("Polled {} records", records.size());
			sourceRecords.addAll(records);
			return sourceRecords.size() >= count;
		});
		for (SourceRecord record : sourceRecords) {
			Assertions.assertEquals(topic, record.topic());
			Compare compare = values((Struct) record.value());
			if (compare != null) {
				Assertions.assertEquals(compare.expected, compare.actual);
			}
		}
	}

	private static class Compare {

		private final Object expected;

		private final Object actual;

		public Compare(Object expected, Object actual) {
			this.expected = expected;
			this.actual = actual;
		}

	}

	private Compare values(Struct struct) {
		String key = struct.getString(ToStructFunction.FIELD_KEY);
		String type = struct.getString(ToStructFunction.FIELD_TYPE);
		Assertions.assertEquals(redisConnection.sync().type(key), type);
		RedisModulesCommands<String, String> commands = redisConnection.sync();
		switch (type) {
		case KeyValue.TYPE_HASH:
			return compare(commands.hgetall(key), struct.getMap(ToStructFunction.FIELD_HASH));
		case KeyValue.TYPE_JSON:
			return compare(commands.jsonGet(key).get(0).toString(), struct.getString(ToStructFunction.FIELD_JSON));
		case KeyValue.TYPE_LIST:
			return compare(commands.lrange(key, 0, -1), struct.getArray(ToStructFunction.FIELD_LIST));
		case KeyValue.TYPE_SET:
			return compare(commands.smembers(key), new HashSet<>(struct.getArray(ToStructFunction.FIELD_SET)));
		case KeyValue.TYPE_STRING:
			return compare(commands.get(key), struct.getString(ToStructFunction.FIELD_STRING));
		case KeyValue.TYPE_ZSET:
			return compare(ToStructFunction.zsetMap(commands.zrangeWithScores(key, 0, -1)),
					struct.getMap(ToStructFunction.FIELD_ZSET));
		default:
			return null;
		}
	}

	private static Compare compare(Object expected, Object actual) {
		return new Compare(expected, actual);
	}

}
