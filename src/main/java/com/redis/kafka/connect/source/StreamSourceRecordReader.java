package com.redis.kafka.connect.source;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamReaderOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulConnection;

public class StreamSourceRecordReader extends AbstractSourceRecordReader<StreamMessage<String, String>> {

	public static final String OFFSET_FIELD = "offset";
	public static final String FIELD_ID = "id";
	public static final String FIELD_BODY = "body";
	public static final String FIELD_STREAM = "stream";
	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.stream.Value";
	private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD_ID, Schema.STRING_SCHEMA)
			.field(FIELD_BODY, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
			.field(FIELD_STREAM, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME);
	private final String topic;
	private final String consumer;

	private StreamItemReader<String, String> reader;
	private AbstractRedisClient client;
	private GenericObjectPool<StatefulConnection<String, String>> pool;

	public StreamSourceRecordReader(RedisSourceConfig sourceConfig, int taskId) {
		super(sourceConfig);
		this.topic = sourceConfig.getTopicName().replace(RedisSourceConfig.TOKEN_STREAM, sourceConfig.getStreamName());
		this.consumer = sourceConfig.getStreamConsumerName().replace(RedisSourceConfig.TOKEN_TASK,
				String.valueOf(taskId));
	}

	@Override
	public void open() throws Exception {
		RedisURI uri = config.uri();
		this.client = config.client(uri);
		this.pool = config.pool(client);
		this.reader = RedisItemReader
				.stream(pool, config.getStreamName(), Consumer.from(config.getStreamConsumerGroup(), consumer))
				.options(StreamReaderOptions.builder().offset(config.getStreamOffset())
						.block(Duration.ofMillis(config.getStreamBlock())).count(config.getBatchSize()).build())
				.build();
		reader.open(new ExecutionContext());
	}

	@Override
	protected List<StreamMessage<String, String>> doPoll() throws Exception {
		return reader.readMessages();
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

	@Override
	protected SourceRecord convert(StreamMessage<String, String> message) {
		Map<String, ?> sourcePartition = new HashMap<>();
		Map<String, ?> sourceOffset = Collections.singletonMap(OFFSET_FIELD, message.getId());
		String key = message.getId();
		Struct value = new Struct(VALUE_SCHEMA).put(FIELD_ID, message.getId()).put(FIELD_BODY, message.getBody())
				.put(FIELD_STREAM, message.getStream());
		return new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, key, VALUE_SCHEMA, value,
				Instant.now().getEpochSecond());
	}

}
