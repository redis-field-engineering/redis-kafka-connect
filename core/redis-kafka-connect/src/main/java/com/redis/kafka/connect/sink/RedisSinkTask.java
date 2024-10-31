/*
 * Copyright © 2021 Redis
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.writer.impl.Del;
import com.redis.spring.batch.item.redis.writer.impl.Hset;
import com.redis.spring.batch.item.redis.writer.impl.JsonSet;
import com.redis.spring.batch.item.redis.writer.impl.Noop;
import com.redis.spring.batch.item.redis.writer.impl.Rpush;
import com.redis.spring.batch.item.redis.writer.impl.Sadd;
import com.redis.spring.batch.item.redis.writer.impl.Set;
import com.redis.spring.batch.item.redis.writer.impl.TsAdd;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;
import com.redis.spring.batch.item.redis.writer.impl.Zadd;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

public class RedisSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);

	private static final String OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s";

	private static final ObjectMapper objectMapper = objectMapper();

	private static final Collector<SinkOffsetState, ?, Map<String, String>> offsetCollector = Collectors
			.toMap(RedisSinkTask::offsetKey, RedisSinkTask::offsetValue);

	private RedisSinkConfig config;

	private AbstractRedisClient client;
	private StatefulRedisModulesConnection<String, String> connection;
	private Converter jsonConverter;
	private RedisItemWriter<byte[], byte[], SinkRecord> writer;

	@Override
	public String version() {
		return ManifestVersionProvider.getVersion();
	}

	private static ObjectMapper objectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
		mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		return mapper;
	}

	@Override
	public void start(final Map<String, String> props) {
		config = new RedisSinkConfig(props);
		jsonConverter = new JsonConverter();
		jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
		client = config.client();
		connection = RedisModulesUtils.connection(client);
		writer = RedisItemWriter.operation(ByteArrayCodec.INSTANCE, new ConditionalDel(operation(), del()));
		writer.setClient(client);
		writer.setMultiExec(config.isMultiExec());
		writer.setWaitReplicas(config.getWaitReplicas());
		writer.setWaitTimeout(config.getWaitTimeout());
		writer.setPoolSize(config.getPoolSize());
		writer.open(new ExecutionContext());
		java.util.Set<TopicPartition> assignment = this.context.assignment();
		if (CollectionUtils.isEmpty(assignment)) {
			return;
		}
		Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
		for (SinkOffsetState state : offsetStates(assignment)) {
			partitionOffsets.put(state.topicPartition(), state.offset());
			log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
		}
		for (TopicPartition topicPartition : assignment) {
			partitionOffsets.putIfAbsent(topicPartition, 0L);
		}
		this.context.offset(partitionOffsets);
	}

	private Operation<byte[], byte[], SinkRecord, Object> del() {
		switch (config.getType()) {
		case HASH:
		case JSON:
		case STRING:
			return new Del<>(this::key);
		default:
			return new Noop<>();
		}
	}

	private class ConditionalDel implements Operation<byte[], byte[], SinkRecord, Object> {

		private final Predicate<SinkRecord> delPredicate = r -> r.value() == null;
		private final Operation<byte[], byte[], SinkRecord, Object> write;
		private final Operation<byte[], byte[], SinkRecord, Object> del;

		public ConditionalDel(Operation<byte[], byte[], SinkRecord, Object> delegate,
				Operation<byte[], byte[], SinkRecord, Object> remove) {
			this.write = delegate;
			this.del = remove;
		}

		@Override
		public List<RedisFuture<Object>> execute(RedisAsyncCommands<byte[], byte[]> commands,
				Chunk<? extends SinkRecord> items) {
			List<RedisFuture<Object>> futures = new ArrayList<>();
			List<SinkRecord> toRemove = items.getItems().stream().filter(delPredicate).collect(Collectors.toList());
			futures.addAll(del.execute(commands, new Chunk<>(toRemove)));
			List<SinkRecord> toWrite = items.getItems().stream().filter(delPredicate.negate())
					.collect(Collectors.toList());
			futures.addAll(write.execute(commands, new Chunk<>(toWrite)));
			return futures;
		}

	}

	private Collection<SinkOffsetState> offsetStates(java.util.Set<TopicPartition> assignment) {
		String[] partitionKeys = assignment.stream().map(this::offsetKey).toArray(String[]::new);
		List<KeyValue<String, String>> values = connection.sync().mget(partitionKeys);
		return values.stream().filter(KeyValue::hasValue).map(this::offsetState).collect(Collectors.toList());
	}

	private String offsetKey(TopicPartition partition) {
		return offsetKey(partition.topic(), partition.partition());
	}

	private SinkOffsetState offsetState(KeyValue<String, String> value) {
		try {
			return objectMapper.readValue(value.getValue(), SinkOffsetState.class);
		} catch (JsonProcessingException e) {
			throw new DataException("Could not parse sink offset state", e);
		}
	}

	private static String offsetKey(String topic, Integer partition) {
		return String.format(OFFSET_KEY_FORMAT, topic, partition);
	}

	private Operation<byte[], byte[], SinkRecord, Object> operation() {
		switch (config.getType()) {
		case HASH:
			return new Hset<>(this::key, this::map);
		case JSON:
			return new JsonSet<>(this::key, this::jsonValue);
		case STRING:
			return new Set<>(this::key, this::value);
		case STREAM:
			return new Xadd<>(this::collectionKey, this::streamMessages);
		case LIST:
			return new Rpush<>(this::collectionKey, this::members);
		case SET:
			return new Sadd<>(this::collectionKey, this::members);
		case TIMESERIES:
			return new TsAdd<>(this::collectionKey, this::samples);
		case ZSET:
			return new Zadd<>(this::collectionKey, this::scoredValues);
		default:
			throw new ConfigException(RedisSinkConfigDef.TYPE_CONFIG, config.getType());
		}
	}

	private Collection<ScoredValue<byte[]>> scoredValues(SinkRecord sinkRecord) {
		return Arrays.asList(ScoredValue.just(doubleValue(sinkRecord), member(sinkRecord)));
	}

	private Collection<Sample> samples(SinkRecord sinkRecord) {
		return Arrays.asList(Sample.of(longMember(sinkRecord), doubleValue(sinkRecord)));
	}

	private byte[] value(SinkRecord sinkRecord) {
		return bytes("value", sinkRecord.value());
	}

	private byte[] jsonValue(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value instanceof byte[]) {
			return (byte[]) value;
		}
		if (value instanceof String) {
			return ((String) value).getBytes(config.getCharset());
		}
		return jsonConverter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), value);
	}

	private Long longMember(SinkRecord sinkRecord) {
		Object key = sinkRecord.key();
		if (key == null) {
			return null;
		}
		if (key instanceof Number) {
			return ((Number) key).longValue();
		}
		throw new DataException(
				"The key for the record must be a number. Consider using a single message transformation to transform the data before it is written to Redis.");
	}

	private Double doubleValue(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value == null) {
			return null;
		}
		if (value instanceof Number) {
			return ((Number) value).doubleValue();
		}
		throw new DataException(
				"The value for the record must be a number. Consider using a single message transformation to transform the data before it is written to Redis.");
	}

	private byte[] key(SinkRecord sinkRecord) {
		if (config.getKeyspace().isEmpty()) {
			return bytes("key", sinkRecord.key());
		}
		String keyspace = keyspace(sinkRecord);
		String key = keyspace + config.getSeparator() + String.valueOf(sinkRecord.key());
		return key.getBytes(config.getCharset());
	}

	private Collection<byte[]> members(SinkRecord sinkRecord) {
		return Arrays.asList(member(sinkRecord));
	}

	private byte[] member(SinkRecord sinkRecord) {
		return bytes("key", sinkRecord.key());
	}

	private String keyspace(SinkRecord sinkRecord) {
		return config.getKeyspace().replace(RedisSinkConfigDef.TOKEN_TOPIC, sinkRecord.topic());
	}

	private byte[] bytes(String source, Object input) {
		if (input instanceof byte[]) {
			return (byte[]) input;
		}
		if (input instanceof String) {
			return ((String) input).getBytes(config.getCharset());
		}
		throw new DataException(String.format(
				"The %s for the record must be a string or byte array. Consider using the StringConverter or ByteArrayConverter if the data is stored in Kafka in the format needed in Redis.",
				source));
	}

	private byte[] collectionKey(SinkRecord sinkRecord) {
		return keyspace(sinkRecord).getBytes(config.getCharset());
	}

	private Collection<StreamMessage<byte[], byte[]>> streamMessages(SinkRecord sinkRecord) {
		return Arrays.asList(new StreamMessage<>(collectionKey(sinkRecord), null, map(sinkRecord)));
	}

	@SuppressWarnings("unchecked")
	private Map<byte[], byte[]> map(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value instanceof Struct) {
			Map<byte[], byte[]> body = new LinkedHashMap<>();
			Struct struct = (Struct) value;
			for (Field field : struct.schema().fields()) {
				Object fieldValue = struct.get(field);
				body.put(field.name().getBytes(config.getCharset()),
						fieldValue == null ? null : fieldValue.toString().getBytes(config.getCharset()));
			}
			return body;
		}
		if (value instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) value;
			Map<byte[], byte[]> body = new LinkedHashMap<>();
			for (Map.Entry<String, Object> e : map.entrySet()) {
				body.put(e.getKey().getBytes(config.getCharset()),
						String.valueOf(e.getValue()).getBytes(config.getCharset()));
			}
			return body;
		}
		throw new ConnectException("Unsupported source value type: " + sinkRecord.valueSchema().type().name());
	}

	@Override
	public void stop() {
		if (writer != null) {
			writer.close();
		}
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
	}

	@Override
	public void put(final Collection<SinkRecord> records) {
		log.debug("Processing {} records", records.size());
		try {
			writer.write(new Chunk<>(new ArrayList<>(records)));
		} catch (RedisConnectionException e) {
			throw new RetriableException("Could not get connection to Redis", e);
		} catch (RedisCommandTimeoutException e) {
			throw new RetriableException("Timeout while writing sink records", e);
		} catch (Exception e) {
			throw new RetriableException("Could not write sink records", e);
		}
		log.info("Wrote {} records", records.size());
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		Map<String, String> offsets = currentOffsets.entrySet().stream().map(this::offsetState)
				.collect(offsetCollector);
		log.trace("Writing offsets: {}", offsets);
		try {
			connection.sync().mset(offsets);
		} catch (RedisCommandTimeoutException e) {
			throw new RetriableException("Could not write offsets", e);
		}
	}

	private SinkOffsetState offsetState(Entry<TopicPartition, OffsetAndMetadata> entry) {
		return SinkOffsetState.of(entry.getKey(), entry.getValue().offset());
	}

	private static String offsetKey(SinkOffsetState state) {
		return offsetKey(state.topic(), state.partition());
	}

	private static String offsetValue(SinkOffsetState state) {
		try {
			return objectMapper.writeValueAsString(state);
		} catch (JsonProcessingException e) {
			throw new DataException("Could not serialize sink offset state", e);
		}
	}

}
