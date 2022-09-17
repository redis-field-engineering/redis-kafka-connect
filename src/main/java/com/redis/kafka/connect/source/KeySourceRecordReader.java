package com.redis.kafka.connect.source;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;

import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.reader.LiveReaderOptions;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.step.FlushingOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

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
	private AbstractRedisClient client;
	private GenericObjectPool<StatefulConnection<String, String>> pool;
	private StatefulRedisPubSubConnection<String, String> pubSubConnection;

	public KeySourceRecordReader(RedisSourceConfig sourceConfig, Duration idleTimeout) {
		super(sourceConfig);
		this.topic = sourceConfig.getTopicName();
		this.batchSize = Math.toIntExact(sourceConfig.getBatchSize());
		this.idleTimeout = idleTimeout;
	}

	@Override
	public void open() throws Exception {
		RedisURI uri = config.uri();
		this.client = config.client(uri);
		this.pool = config.pool(client);
		this.pubSubConnection = RedisModulesUtils.pubSubConnection(client);
		reader = RedisItemReader
				.liveDataStructure(pool, JobRunner.inMemory(), pubSubConnection, uri.getDatabase(),
						config.getKeyPatterns().toArray(new String[0]))
				.options(LiveReaderOptions.builder()
						.flushingOptions(FlushingOptions.builder().timeout(idleTimeout).build()).build())
				.build();
		reader.open(new ExecutionContext());
	}

	@Override
	public void close() {
		if (reader != null) {
			reader.close();
			reader = null;
		}
		if (pubSubConnection != null) {
			pubSubConnection.close();
			pubSubConnection = null;
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
		if (input.getType() == Type.HASH) {
			return HASH_VALUE_SCHEMA;
		}
		return STRING_VALUE_SCHEMA;
	}

}
