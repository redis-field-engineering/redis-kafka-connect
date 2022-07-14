package com.redis.kafka.connect.source;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;

import io.lettuce.core.AbstractRedisClient;

public class KeySourceRecordReader extends AbstractSourceRecordReader<DataStructure<String>> {

	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final Schema STRING_VALUE_SCHEMA = Schema.STRING_SCHEMA;
	private static final String HASH_VALUE_SCHEMA_NAME = "com.redis.kafka.connect.HashEventValue";
	private static final Schema HASH_VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
			.name(HASH_VALUE_SCHEMA_NAME);

	private final int batchSize;
	private final String topic;
	private LiveRedisItemReader<String, DataStructure<String>> reader;
	private final Duration idleTimeout;

	public KeySourceRecordReader(RedisSourceConfig sourceConfig, Duration idleTimeout) {
		super(sourceConfig);
		this.topic = sourceConfig.getTopicName();
		this.batchSize = Math.toIntExact(sourceConfig.getBatchSize());
		this.idleTimeout = idleTimeout;
	}

	@Override
	protected void open(AbstractRedisClient client) throws Exception {
		reader = RedisItemReader.client(client).string().dataStructure().live().idleTimeout(idleTimeout)
				.keyPatterns(sourceConfig.getKeyPatterns().toArray(new String[0])).build();
		reader.open(new ExecutionContext());
	}

	public LiveRedisItemReader<String, DataStructure<String>> getReader() {
		return reader;
	}

	@Override
	protected List<DataStructure<String>> doPoll() throws Exception {
		return reader.read(batchSize);
	}

	@Override
	protected SourceRecord convert(DataStructure<String> input) {
		Map<String, ?> sourcePartition = new HashMap<>();
		Map<String, ?> sourceOffset = new HashMap<>();
		return new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, input.getKey(), schema(input),
				input.getValue(), Instant.now().getEpochSecond());
	}

	private Schema schema(DataStructure<String> input) {
		if (Type.HASH.name().equalsIgnoreCase(input.getType())) {
			return HASH_VALUE_SCHEMA;
		}
		return STRING_VALUE_SCHEMA;
	}

	@Override
	protected void doClose() {
		if (reader != null) {
			reader.close();
		}
	}
}
