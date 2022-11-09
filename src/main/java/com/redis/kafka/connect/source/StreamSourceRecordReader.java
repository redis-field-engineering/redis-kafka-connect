package com.redis.kafka.connect.source;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamReaderOptions;
import com.redis.spring.batch.reader.StreamReaderOptions.AckPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulConnection;

public class StreamSourceRecordReader implements SourceRecordReader {

	public static final String OFFSET_FIELD = "offset";
	public static final String FIELD_ID = "id";
	public static final String FIELD_BODY = "body";
	public static final String FIELD_STREAM = "stream";
	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.stream.Value";
	private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD_ID, Schema.STRING_SCHEMA)
			.field(FIELD_BODY, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
			.field(FIELD_STREAM, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME).build();

	private final RedisSourceConfig config;
	private final String topic;
	private final String consumer;
	private final AckPolicy ackPolicy;
	private StreamItemReader<String, String> reader;
	private AbstractRedisClient client;
	private GenericObjectPool<StatefulConnection<String, String>> pool;
	Clock clock = Clock.systemDefaultZone();

	public StreamSourceRecordReader(RedisSourceConfig config, int taskId) {
		this.config = config;
		this.topic = config.getTopicName().replace(RedisSourceConfig.TOKEN_STREAM, config.getStreamName());
		this.consumer = config.getStreamConsumerName().replace(RedisSourceConfig.TOKEN_TASK, String.valueOf(taskId));
		this.ackPolicy = ackPolicy(config.getStreamDelivery());
	}

	private AckPolicy ackPolicy(String deliveryType) {
		switch (config.getStreamDelivery()) {
		case RedisSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE:
			return AckPolicy.AUTO;
		case RedisSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE:
			return AckPolicy.MANUAL;
		default:
			throw new IllegalArgumentException("Illegal value for " + RedisSourceConfig.STREAM_DELIVERY_CONFIG + ": "
					+ config.getStreamDelivery());
		}
	}

	@Override
	public void open(Map<String, Object> offset) {
		this.client = config.client(config.uri());
		this.pool = config.pool(client);
		this.reader = RedisItemReader
				.stream(pool, config.getStreamName(), Consumer.from(config.getStreamConsumerGroup(), consumer))
				.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).offset(offset(offset))
						.block(Duration.ofMillis(config.getStreamBlock())).count(config.getBatchSize())
						.ackPolicy(ackPolicy).build())
				.build();
		reader.open(new ExecutionContext());
	}

	private String offset(Map<String, Object> offset) {
		if (offset == null) {
			return config.getStreamOffset();
		}
		return (String) offset.get(OFFSET_FIELD);
	}

	@Override
	public List<SourceRecord> poll() {
		List<StreamMessage<String, String>> messages;
		try {
			messages = reader.readMessages();
		} catch (Exception e) {
			throw new ConnectException("Could not read messages from stream", e);
		}
		return messages.stream().map(this::convert).collect(Collectors.toList());
	}

	@Override
	public void close() {
		if (reader != null) {
			reader.close();
			reader = null;
		}
		if (pool != null) {
			pool.close();
			pool = null;
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
			client = null;
		}
	}

	public SourceRecord convert(StreamMessage<String, String> message) {
		Map<String, ?> sourcePartition = new HashMap<>();
		Map<String, ?> sourceOffset = Collections.singletonMap(OFFSET_FIELD, message.getId());
		String key = message.getId();
		Struct value = new Struct(VALUE_SCHEMA).put(FIELD_ID, message.getId()).put(FIELD_BODY, message.getBody())
				.put(FIELD_STREAM, message.getStream());
		return new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, key, VALUE_SCHEMA, value,
				clock.instant().toEpochMilli());
	}

	@Override
	public void commit(List<Map<String, ?>> sourceOffsets) {
		try {
			reader.ack(sourceOffsets.stream().map(m -> (String) m.get(OFFSET_FIELD)).toArray(String[]::new));
		} catch (Exception e) {
			throw new ConnectException("Could not connect to Redis", e);
		}
	}

}
