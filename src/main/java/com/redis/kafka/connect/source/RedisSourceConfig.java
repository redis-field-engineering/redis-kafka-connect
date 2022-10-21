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
package com.redis.kafka.connect.source;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.redis.kafka.connect.common.RedisConfig;

public class RedisSourceConfig extends RedisConfig {

	public enum ReaderType {
		KEYS, STREAM
	}

	public enum AckPolicy {
		AUTO, EXPLICIT
	}

	public static final String TOKEN_STREAM = "${stream}";
	public static final String TOKEN_TASK = "${task}";

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_DEFAULT = TOKEN_STREAM;
	public static final String TOPIC_DOC = String.format(
			"Name of the destination topic, which may contain '%s' as a placeholder for the originating stream name. For example `redis_%s` for the stream 'orders' will map to the topic name 'redis_orders'.",
			TOKEN_STREAM, TOKEN_STREAM);

	public static final String READER_CONFIG = "redis.reader";
	public static final ReaderType READER_DEFAULT = ReaderType.STREAM;
	public static final String READER_DOC = "Source from which to read Redis records. " + ReaderType.KEYS
			+ ": generate records from key events and respective values generated from write operations in the Redis database. "
			+ ReaderType.STREAM + ": read messages from a Redis stream";

	public static final String BATCH_SIZE_CONFIG = "batch.size";
	public static final long BATCH_SIZE_DEFAULT = 500;
	public static final String BATCH_SIZE_DOC = "Maximum number of records to include in a single read when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";

	public static final String KEY_PATTERNS_CONFIG = "redis.keys.patterns";
	public static final String KEY_PATTERNS_DEFAULT = "*";
	public static final String KEY_PATTERNS_DOC = "Keyspace glob-style patterns to subscribe to, comma-separated.";

	public static final String STREAM_NAME_CONFIG = "redis.stream.name";
	public static final String STREAM_NAME_DOC = "Name of the Redis stream to read from";

	public static final String STREAM_ACK_CONFIG = "redis.stream.ack";
	public static final AckPolicy STREAM_ACK_DEFAULT = AckPolicy.EXPLICIT;
	public static final String STREAM_ACK_DOC = "Acknowledgment policy for stream messages. " + AckPolicy.EXPLICIT
			+ " mode acks each message after it's been committed (at-least-once processing). " + AckPolicy.AUTO
			+ " acks messages as soon as they're read (at-most-once processing).";

	public static final String STREAM_OFFSET_CONFIG = "redis.stream.offset";
	public static final String STREAM_OFFSET_DEFAULT = "0-0";
	public static final String STREAM_OFFSET_DOC = "Stream offset to start reading from";

	public static final String STREAM_CONSUMER_GROUP_CONFIG = "redis.stream.consumer.group";
	public static final String STREAM_CONSUMER_GROUP_DEFAULT = "kafka-consumer-group";
	public static final String STREAM_CONSUMER_GROUP_DOC = "Stream consumer group";

	public static final String STREAM_CONSUMER_NAME_CONFIG = "redis.stream.consumer.name";
	public static final String STREAM_CONSUMER_NAME_DEFAULT = "consumer-" + TOKEN_TASK;
	public static final String STREAM_CONSUMER_NAME_DOC = "A format string for the stream consumer, which may contain '"
			+ TOKEN_TASK + "' as a placeholder for the task id.\nFor example, 'consumer-" + TOKEN_TASK
			+ "' for the task id '123' will map to the consumer name 'consumer-123'.";

	public static final String STREAM_BLOCK_CONFIG = "redis.stream.block";
	public static final long STREAM_BLOCK_DEFAULT = 100;
	public static final String STREAM_BLOCK_DOC = "The max amount of time in milliseconds to wait while polling for stream messages (XREAD [BLOCK milliseconds])";

	private final ReaderType readerType;
	private final List<String> keyPatterns;
	private final String streamName;
	private final AckPolicy streamAckPolicy;
	private final String streamOffset;
	private final String streamConsumerGroup;
	private final String streamConsumerName;
	private final Long batchSize;
	private final Long streamBlock;
	private final String topicName;

	public RedisSourceConfig(Map<?, ?> originals) {
		super(new RedisSourceConfigDef(), originals);
		this.topicName = getString(TOPIC_CONFIG);
		this.readerType = ConfigUtils.getEnum(ReaderType.class, this, READER_CONFIG);
		this.batchSize = getLong(BATCH_SIZE_CONFIG);
		this.keyPatterns = getList(KEY_PATTERNS_CONFIG);
		this.streamName = getString(STREAM_NAME_CONFIG);
		this.streamAckPolicy = ConfigUtils.getEnum(AckPolicy.class, this, STREAM_ACK_CONFIG);
		this.streamOffset = getString(STREAM_OFFSET_CONFIG);
		this.streamConsumerGroup = getString(STREAM_CONSUMER_GROUP_CONFIG);
		this.streamConsumerName = getString(STREAM_CONSUMER_NAME_CONFIG);
		this.streamBlock = getLong(STREAM_BLOCK_CONFIG);
	}

	public ReaderType getReaderType() {
		return readerType;
	}

	public List<String> getKeyPatterns() {
		return keyPatterns;
	}

	public Long getStreamBlock() {
		return streamBlock;
	}

	public Long getBatchSize() {
		return batchSize;
	}

	public String getStreamName() {
		return streamName;
	}

	public AckPolicy getStreamAckPolicy() {
		return streamAckPolicy;
	}

	public String getStreamOffset() {
		return streamOffset;
	}

	public String getStreamConsumerGroup() {
		return streamConsumerGroup;
	}

	public String getStreamConsumerName() {
		return streamConsumerName;
	}

	public String getTopicName() {
		return topicName;
	}

	public static class RedisSourceConfigDef extends RedisConfigDef {

		public RedisSourceConfigDef() {
			define();
		}

		public RedisSourceConfigDef(ConfigDef base) {
			super(base);
			define();
		}

		private void define() {
			define(ConfigKeyBuilder.of(TOPIC_CONFIG, ConfigDef.Type.STRING).defaultValue(TOPIC_DEFAULT)
					.importance(ConfigDef.Importance.MEDIUM).documentation(TOPIC_DOC).build());
			define(ConfigKeyBuilder.of(BATCH_SIZE_CONFIG, ConfigDef.Type.LONG).defaultValue(BATCH_SIZE_DEFAULT)
					.importance(ConfigDef.Importance.LOW).documentation(BATCH_SIZE_DOC)
					.validator(ConfigDef.Range.atLeast(1L)).build());
			define(ConfigKeyBuilder.of(READER_CONFIG, ConfigDef.Type.STRING).documentation(READER_DOC)
					.defaultValue(READER_DEFAULT.name()).importance(ConfigDef.Importance.HIGH)
					.validator(Validators.validEnum(ReaderType.class)).internalConfig(true).build());
			define(ConfigKeyBuilder.of(KEY_PATTERNS_CONFIG, Type.LIST).documentation(KEY_PATTERNS_DOC)
					.defaultValue(KEY_PATTERNS_DEFAULT).importance(Importance.MEDIUM).internalConfig(true).build());
			define(ConfigKeyBuilder.of(STREAM_NAME_CONFIG, ConfigDef.Type.STRING).documentation(STREAM_NAME_DOC)
					.importance(ConfigDef.Importance.HIGH).build());
			define(ConfigKeyBuilder.of(STREAM_ACK_CONFIG, ConfigDef.Type.STRING).documentation(STREAM_ACK_DOC)
					.defaultValue(STREAM_ACK_DEFAULT.name()).importance(ConfigDef.Importance.HIGH)
					.validator(Validators.validEnum(AckPolicy.class)).build());
			define(ConfigKeyBuilder.of(STREAM_OFFSET_CONFIG, ConfigDef.Type.STRING).documentation(STREAM_OFFSET_DOC)
					.defaultValue(STREAM_OFFSET_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(STREAM_CONSUMER_GROUP_CONFIG, ConfigDef.Type.STRING)
					.documentation(STREAM_CONSUMER_GROUP_DOC).defaultValue(STREAM_CONSUMER_GROUP_DEFAULT)
					.importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(STREAM_CONSUMER_NAME_CONFIG, ConfigDef.Type.STRING)
					.documentation(STREAM_CONSUMER_NAME_DOC).defaultValue(STREAM_CONSUMER_NAME_DEFAULT)
					.importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(STREAM_BLOCK_CONFIG, ConfigDef.Type.LONG).defaultValue(STREAM_BLOCK_DEFAULT)
					.importance(ConfigDef.Importance.LOW).documentation(STREAM_BLOCK_DOC)
					.validator(ConfigDef.Range.atLeast(1L)).build());
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(batchSize, keyPatterns, readerType, streamBlock, streamConsumerGroup,
				streamConsumerName, streamName, streamAckPolicy, streamOffset, topicName);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		RedisSourceConfig other = (RedisSourceConfig) obj;
		return Objects.equals(batchSize, other.batchSize) && Objects.equals(keyPatterns, other.keyPatterns)
				&& readerType == other.readerType && Objects.equals(streamBlock, other.streamBlock)
				&& Objects.equals(streamConsumerGroup, other.streamConsumerGroup)
				&& Objects.equals(streamConsumerName, other.streamConsumerName)
				&& Objects.equals(streamName, other.streamName)
				&& Objects.equals(streamAckPolicy, other.streamAckPolicy)
				&& Objects.equals(streamOffset, other.streamOffset) && Objects.equals(topicName, other.topicName);
	}

}
