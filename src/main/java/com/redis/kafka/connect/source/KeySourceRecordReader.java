package com.redis.kafka.connect.source;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.FlushingOptions;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ReaderOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class KeySourceRecordReader implements SourceRecordReader {

	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final Schema STRING_VALUE_SCHEMA = Schema.STRING_SCHEMA;
	private static final String HASH_VALUE_SCHEMA_NAME = "com.redis.kafka.connect.HashEventValue";
	private static final Schema HASH_VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
			.name(HASH_VALUE_SCHEMA_NAME).build();

	private static JobRunner jobRunner;

	private final Clock clock = Clock.systemDefaultZone();
	protected final RedisSourceConfig config;
	private final ReaderOptions options;
	private final FlushingOptions flushingOptions;
	private final int batchSize;
	private final String topic;
	private LiveRedisItemReader<String, DataStructure<String>> reader;
	private AbstractRedisClient client;
	private GenericObjectPool<StatefulConnection<String, String>> pool;
	private StatefulRedisPubSubConnection<String, String> pubSubConnection;

	public KeySourceRecordReader(RedisSourceConfig config, ReaderOptions options, FlushingOptions flushingOptions) {
		Assert.notNull(config, "Source connector config must not be null");
		Assert.notNull(options, "Options must not be null");
		this.config = config;
		this.options = options;
		this.flushingOptions = flushingOptions;
		this.topic = config.getTopicName();
		this.batchSize = Math.toIntExact(config.getBatchSize());
	}

	@Override
	public void open(Map<String, Object> offset) {
		RedisURI uri = config.uri();
		this.client = config.client(uri);
		this.pool = config.pool(client);
		this.pubSubConnection = RedisModulesUtils.pubSubConnection(client);
		checkJobRunner();
		reader = reader(client).live().database(uri.getDatabase())
				.keyPatterns(config.getKeyPatterns().toArray(new String[0])).jobRunner(jobRunner).readerOptions(options)
				.flushingOptions(flushingOptions).dataStructure();
		reader.open(new ExecutionContext());
	}

	private RedisItemReader.Builder<String, String> reader(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemReader.client((RedisModulesClusterClient) client);
		}
		return RedisItemReader.client((RedisModulesClient) client);
	}

	private static void checkJobRunner() {
		if (jobRunner == null) {
			try {
				jobRunner = JobRunner.inMemory();
			} catch (Exception e) {
				throw new ConnectException("Could not initialize in-memory job runner", e);
			}
		}
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
	public List<SourceRecord> poll() {
		return reader.read(batchSize).stream().map(this::convert).collect(Collectors.toList());
	}

	private SourceRecord convert(DataStructure<String> input) {
		Map<String, ?> sourcePartition = new HashMap<>();
		Map<String, ?> sourceOffset = new HashMap<>();
		return new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, input.getKey(), schema(input),
				input.getValue(), clock.instant().toEpochMilli());
	}

	private Schema schema(DataStructure<String> input) {
		if (input.getType() == Type.HASH) {
			return HASH_VALUE_SCHEMA;
		}
		return STRING_VALUE_SCHEMA;
	}

	@Override
	public void commit(List<Map<String, ?>> sourceOffsets) {
		// do nothing
	}

}
