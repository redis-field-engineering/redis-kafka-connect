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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.writer.operation.Del;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Lpush;
import com.redis.spring.batch.writer.operation.Rpush;
import com.redis.spring.batch.writer.operation.Sadd;
import com.redis.spring.batch.writer.operation.Set;
import com.redis.spring.batch.writer.operation.TsAdd;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.util.internal.StringUtil;

public class RedisSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);
	private static final String OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s";

	private static final ObjectMapper objectMapper = objectMapper();

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
		this.client = config.client();
		this.connection = RedisModulesUtils.connection(client);
		writer = new WriterBuilder(client).multiExec(config.isMultiexec()).waitReplicas(config.getWaitReplicas())
				.waitTimeout(config.getWaitTimeout()).operation(ByteArrayCodec.INSTANCE, operation());
		writer.open(new ExecutionContext());
		writer.setPoolOptions(config.poolOptions());
		final java.util.Set<TopicPartition> assignment = this.context.assignment();
		if (!assignment.isEmpty()) {
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
	}

	private Collection<SinkOffsetState> offsetStates(java.util.Set<TopicPartition> assignment) {
		Collection<SinkOffsetState> offsetStates = new ArrayList<>();
		String[] partitionKeys = assignment.stream().map(a -> offsetKey(a.topic(), a.partition()))
				.toArray(String[]::new);
		List<KeyValue<String, String>> values = connection.sync().mget(partitionKeys);
		for (KeyValue<String, String> value : values) {
			if (value.hasValue()) {
				try {
					offsetStates.add(objectMapper.readValue(value.getValue(), SinkOffsetState.class));
				} catch (IOException e) {
					throw new DataException(e);
				}
			}
		}
		return offsetStates;
	}

	private String offsetKey(String topic, Integer partition) {
		return String.format(OFFSET_KEY_FORMAT, topic, partition);
	}

	private Operation<byte[], byte[], SinkRecord, ?> operation() {
		switch (config.getCommand()) {
		case HSET:
			return new Hset<>(this::key, this::map);
		case JSONSET:
			return new JsonSet<>(this::key, this::jsonValue);
		case SET:
			return new Set<>(this::key, this::value);
		case XADD:
			return new Xadd<>(this::collectionKey, this::map, m -> new XAddArgs());
		case LPUSH:
			return new Lpush<>(this::collectionKey, this::member);
		case RPUSH:
			return new Rpush<>(this::collectionKey, this::member);
		case SADD:
			return new Sadd<>(this::collectionKey, this::member);
		case TSADD:
			return new TsAdd<>(this::collectionKey, new SampleConverter<>(this::longMember, this::doubleValue));
		case ZADD:
			return new Zadd<>(this::collectionKey, new ScoredValueConverter<>(this::member, this::doubleValue));
		case DEL:
			return new Del<>(this::key);
		default:
			throw new ConfigException(RedisSinkConfigDef.COMMAND_CONFIG, config.getCommand());
		}
	}

	private byte[] value(SinkRecord sinkRecord) {
		return bytes("value", sinkRecord.value());
	}

	private byte[] jsonValue(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value == null) {
			return null;
		}
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

	private byte[] member(SinkRecord sinkRecord) {
		return bytes("key", sinkRecord.key());
	}

	private String keyspace(SinkRecord sinkRecord) {
		return config.getKeyspace().replace(RedisSinkConfigDef.TOKEN_TOPIC, sinkRecord.topic());
	}

	private byte[] bytes(String source, Object input) {
		if (input == null) {
			return null;
		}
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

	@SuppressWarnings("unchecked")
	private Map<byte[], byte[]> map(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value == null) {
			return null;
		}
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
			writer = null;
		}
		if (connection != null) {
			connection.close();
			connection = null;
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
			client = null;
		}
	}

	@Override
	public void put(final Collection<SinkRecord> records) {
		log.debug("Processing {} records", records.size());
		try {
			writer.write(new ArrayList<>(records));
			log.info("Wrote {} records", records.size());
		} catch (Exception e) {
			log.warn("Could not write {} records", records.size(), e);
		}
		Map<TopicPartition, Long> data = new ConcurrentHashMap<>(100);
		for (SinkRecord sinkRecord : records) {
			Assert.isTrue(!StringUtil.isNullOrEmpty(sinkRecord.topic()), "topic cannot be null or empty.");
			Assert.notNull(sinkRecord.kafkaPartition(), "partition cannot be null.");
			Assert.isTrue(sinkRecord.kafkaOffset() >= 0, "offset must be greater than or equal 0.");
			TopicPartition partition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
			long current = data.getOrDefault(partition, Long.MIN_VALUE);
			if (sinkRecord.kafkaOffset() > current) {
				data.put(partition, sinkRecord.kafkaOffset());
			}
		}
		List<SinkOffsetState> offsetData = data.entrySet().stream()
				.map(e -> SinkOffsetState.of(e.getKey(), e.getValue())).collect(Collectors.toList());
		if (!offsetData.isEmpty()) {
			Map<String, String> offsets = new LinkedHashMap<>(offsetData.size());
			for (SinkOffsetState e : offsetData) {
				String key = offsetKey(e.topic(), e.partition());
				String value;
				try {
					value = objectMapper.writeValueAsString(e);
				} catch (JsonProcessingException e1) {
					throw new DataException(e1);
				}
				offsets.put(key, value);
				log.trace("put() - Setting offset: {}", e);
			}
			connection.sync().mset(offsets);
		}
	}

}
