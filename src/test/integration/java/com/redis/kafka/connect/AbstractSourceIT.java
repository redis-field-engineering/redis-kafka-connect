package com.redis.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.redis.kafka.connect.source.KeySourceRecordReader;
import com.redis.kafka.connect.source.RedisSourceConfig;
import com.redis.kafka.connect.source.RedisSourceTask;
import com.redis.kafka.connect.source.StreamSourceRecordReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.LiveRedisItemReader;

import io.lettuce.core.models.stream.PendingMessages;

abstract class AbstractSourceIT extends AbstractBaseIT {

	private static final Logger log = LoggerFactory.getLogger(AbstractSourceIT.class);

	private RedisSourceTask task;

	@BeforeEach
	public void createTask() {
		task = new RedisSourceTask();
	}

	// Used to initialize a task with a previous connect offset (as though records
	// had been committed).
	void initializeTask(String id) throws Exception {
		task.initialize(new SourceTaskContext() {
			@Override
			public OffsetStorageReader offsetStorageReader() {
				return new OffsetStorageReader() {
					@Override
					public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
						throw new UnsupportedOperationException("OffsetStorageReader.offsets()");
					}

					@Override
					public <T> Map<String, Object> offset(Map<String, T> partition) {
						return Collections.singletonMap(StreamSourceRecordReader.OFFSET_FIELD, id);
					}
				};
			}

			@Override
			public Map<String, String> configs() {
				throw new UnsupportedOperationException("SourceTaskContext.configs()");
			}
		});
	}

	private void startTask(String... props) {
		Map<String, String> config = map(props);
		config.put(RedisSourceConfig.URI_CONFIG, getRedisServer().getRedisURI());
		task.start(config);
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

	@AfterEach
	public void teardown() {
		task.stop();
	}

	@Test
	void pollStreamAtMostOnce() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream, RedisSourceConfig.STREAM_DELIVERY_CONFIG,
				RedisSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = connection.sync().xadd(stream, body);
		String id2 = connection.sync().xadd(stream, body);
		String id3 = connection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
		assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
		assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
		PendingMessages pendingMsgs = connection.sync().xpending(stream,
				RedisSourceConfig.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(0, pendingMsgs.getCount(), "pending messages");
	}

	@Test
	void pollStreamAtLeastOnce() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = connection.sync().xadd(stream, body);
		String id2 = connection.sync().xadd(stream, body);
		String id3 = connection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
		assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
		assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
		PendingMessages pendingMsgsBeforeCommit = connection.sync().xpending(stream,
				RedisSourceConfig.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
		task.commitRecord(sourceRecords.get(0), new RecordMetadata(null, 0, 0, 0, 0, 0));
		task.commitRecord(sourceRecords.get(1), new RecordMetadata(null, 0, 0, 0, 0, 0));
		task.commit();
		PendingMessages pendingMsgsAfterCommit = connection.sync().xpending(stream,
				RedisSourceConfig.STREAM_CONSUMER_GROUP_DEFAULT);
		Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
	}

	@Test
	void pollStreamAtLeastOnceRecover() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);

		// create a new task, same config
		createTask();
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);

		Awaitility.await().until(() -> recoveredRecords.addAll(task.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(task.poll()));

		Assertions.assertEquals(6, recoveredRecords.size());
	}

	@Test
	void pollStreamAtLeastOnceRecoverUncommitted() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		String id3 = connection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		task.commitRecord(sourceRecords.get(0), new RecordMetadata(null, 0, 0, 0, 0, 0));
		task.commitRecord(sourceRecords.get(1), new RecordMetadata(null, 0, 0, 0, 0, 0));
		task.commit();

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = connection.sync().xadd(stream, body);
		String id5 = connection.sync().xadd(stream, body);
		String id6 = connection.sync().xadd(stream, body);

		// create a new task, same config
		createTask();
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(task.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(task.poll()));
		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
	}

	@Test
	void pollStreamAtLeastOnceRecoverFromOffset() throws Exception {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = connection.sync().xadd(stream, body);
		log.info("ID1: " + id1);
		String id2 = connection.sync().xadd(stream, body);
		log.info("ID2: " + id2);
		String id3 = connection.sync().xadd(stream, body);
		log.info("ID3: " + id3);
		List<SourceRecord> records = new ArrayList<>();
		Awaitility.await().until(() -> records.addAll(task.poll()));
		Assertions.assertEquals(3, records.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = connection.sync().xadd(stream, body);
		log.info("ID4: " + id4);
		String id5 = connection.sync().xadd(stream, body);
		log.info("ID5: " + id5);
		String id6 = connection.sync().xadd(stream, body);
		log.info("ID6: " + id6);

		// create a new task, same config
		createTask();
		// this means connect committed records, but StreamSourceTask didn't get a
		// chance to ack first
		initializeTask(id3);
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(task.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(task.poll()));

		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id4, id5, id6), recoveredIds, "recoveredIds");
	}

	@Test
	void pollStreamAtMostOnceRecover() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream, RedisSourceConfig.STREAM_DELIVERY_CONFIG,
				RedisSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = connection.sync().xadd(stream, body);
		String id5 = connection.sync().xadd(stream, body);
		String id6 = connection.sync().xadd(stream, body);

		// create a new task, same config
		createTask();
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream, RedisSourceConfig.STREAM_DELIVERY_CONFIG,
				RedisSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(task.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(task.poll()));
		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");
	}

	@Test
	void pollStreamRecoverAtLeastOnceToAtMostOnce() throws InterruptedException {
		String stream = "stream1";
		String topicPrefix = "testprefix-";
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream, RedisSourceConfig.STREAM_DELIVERY_CONFIG,
				RedisSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		connection.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<SourceRecord> recoveredRecords = new ArrayList<>();
		String id4 = connection.sync().xadd(stream, body);
		String id5 = connection.sync().xadd(stream, body);
		String id6 = connection.sync().xadd(stream, body);

		// create a new task, same config except AT_MOST_ONCE
		createTask();
		startTask(RedisSourceConfig.TOPIC_CONFIG, topicPrefix + RedisSourceConfig.TOKEN_STREAM,
				RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.STREAM.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, stream, RedisSourceConfig.STREAM_DELIVERY_CONFIG,
				RedisSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE);

		// Wait until task.poll() doesn't return any more records
		Awaitility.await().until(() -> recoveredRecords.addAll(task.poll()));
		Awaitility.await().until(() -> !recoveredRecords.addAll(task.poll()));
		List<String> recoveredIds = recoveredRecords.stream().map(SourceRecord::key).map(String::valueOf)
				.collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

		PendingMessages pending = connection.sync().xpending(stream, RedisSourceConfig.STREAM_CONSUMER_GROUP_DEFAULT);
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
	void pollKeys() throws InterruptedException {
		String topic = "mytopic";
		startTask(RedisSourceConfig.READER_CONFIG, RedisSourceConfig.ReaderType.KEYS.name(),
				RedisSourceConfig.STREAM_NAME_CONFIG, "dummy", RedisSourceConfig.TOPIC_CONFIG, topic,
				RedisSourceTask.KEYS_IDLE_TIMEOUT, "3000");
		LiveRedisItemReader<String, DataStructure<String>> reader = ((KeySourceRecordReader) task.getReader())
				.getReader();
		Awaitility.await().until(reader::isOpen);
		String stringKey = "key:1";
		String stringValue = "my string";
		log.info("Setting {}={}", stringKey, stringValue);
		connection.sync().set(stringKey, stringValue);
		String hashKey = "key:2";
		Map<String, String> hashValue = map("field1", "value1", "field2", "value2");
		log.info("Setting {}={}", hashKey, hashValue);
		connection.sync().hset(hashKey, hashValue);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(2, sourceRecords.size());
		for (SourceRecord record : sourceRecords) {
			Assertions.assertEquals(topic, record.topic());
		}
		Assertions.assertEquals(stringValue, sourceRecords.get(0).value());
		Assertions.assertEquals(hashValue, sourceRecords.get(1).value());
	}

}
