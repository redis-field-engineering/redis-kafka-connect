package com.redis.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.util.Assert;
import org.testcontainers.junit.jupiter.Container;

import com.redis.kafka.connect.source.KeySourceRecordReader;
import com.redis.kafka.connect.source.RedisEnterpriseSourceConfig;
import com.redis.kafka.connect.source.RedisEnterpriseSourceTask;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.jupiter.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class RedisEnterpriseSourceTaskIT extends AbstractTestcontainersRedisTestBase {

	@Container
	private static final RedisContainer REDIS = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS);
	}

	private RedisEnterpriseSourceTask task;

	@BeforeEach
	public void createTask() {
		task = new RedisEnterpriseSourceTask();
	}

	private void startTask(RedisTestContext redis, String... props) {
		Map<String, String> config = map(props);
		config.put(RedisEnterpriseSourceConfig.REDIS_URI_CONFIG, redis.getServer().getRedisURI());
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

	@ParameterizedTest
	@RedisTestContextsSource
	void pollStream(RedisTestContext redis) throws InterruptedException {
		final String stream = "stream1";
		final String topicPrefix = "testprefix-";
		startTask(redis, RedisEnterpriseSourceConfig.TOPIC_CONFIG,
				topicPrefix + RedisEnterpriseSourceConfig.TOKEN_STREAM, RedisEnterpriseSourceConfig.READER_CONFIG,
				RedisEnterpriseSourceConfig.ReaderType.STREAM.name(), RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG,
				stream);
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		final Map<String, String> body = map(field1, value1, field2, value2);
		final String id1 = redis.sync().xadd(stream, body);
		final String id2 = redis.sync().xadd(stream, body);
		final String id3 = redis.sync().xadd(stream, body);
		List<SourceRecord> sourceRecords = new ArrayList<>();
		Awaitility.await().until(() -> sourceRecords.addAll(task.poll()));
		Assertions.assertEquals(3, sourceRecords.size());
		assertEquals(id1, body, stream, topicPrefix + stream, sourceRecords.get(0));
		assertEquals(id2, body, stream, topicPrefix + stream, sourceRecords.get(1));
		assertEquals(id3, body, stream, topicPrefix + stream, sourceRecords.get(2));
	}

	private void assertEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
			String expectedTopic, SourceRecord record) {
		Struct struct = (Struct) record.value();
		Assertions.assertEquals(expectedId, struct.get("id"));
		Assertions.assertEquals(expectedBody, struct.get("body"));
		Assertions.assertEquals(expectedStream, struct.get("stream"));
		Assertions.assertEquals(expectedTopic, record.topic());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void pollKeys(RedisTestContext redis) throws InterruptedException {
		String topic = "mytopic";
		startTask(redis, RedisEnterpriseSourceConfig.READER_CONFIG, RedisEnterpriseSourceConfig.ReaderType.KEYS.name(),
				RedisEnterpriseSourceConfig.STREAM_NAME_CONFIG, "dummy", RedisEnterpriseSourceConfig.TOPIC_CONFIG,
				topic, RedisEnterpriseSourceTask.KEYS_IDLE_TIMEOUT, "3000");
		LiveRedisItemReader<String, DataStructure<String>> reader = ((KeySourceRecordReader) task.getReader())
				.getReader();
		Awaitility.await().until(reader::isOpen);
		String stringKey = "key:1";
		String stringValue = "my string";
		log.info("Setting {}={}", stringKey, stringValue);
		redis.sync().set(stringKey, stringValue);
		String hashKey = "key:2";
		Map<String, String> hashValue = map("field1", "value1", "field2", "value2");
		log.info("Setting {}={}", hashKey, hashValue);
		redis.sync().hset(hashKey, hashValue);
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
