/*
 * Copyright © 2021 Redis Labs
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
package com.redislabs.kafka.connect.source;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.redislabs.kafka.connect.common.RedisEnterpriseConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class RedisEnterpriseSourceConfig extends RedisEnterpriseConfig {

    public static final String TOKEN_STREAM = "${stream}";
    public static final String TOKEN_TASK = "${task}";

    public static final String TOPIC = "topic";
    public static final String TOPIC_DEFAULT = TOKEN_STREAM;
    public static final String TOPIC_DOC = String.format("Name of the destination topic, which may contain '%s' as a placeholder for the originating stream name. For example `redis_%s` for the stream 'orders' will map to the topic name 'redis_orders'.", TOKEN_STREAM, TOKEN_STREAM);

    public static final String READER_TYPE = "redis.reader";
    public static final String READER_DEFAULT = ReaderType.STREAM.name();
    public static final String READER_DOC = "Source from which to read Redis records. " + ReaderType.KEYS + ": generate records from key events and respective values generated from write operations in the Redis database. " + ReaderType.STREAM + ": read messages from a Redis stream";

    public static final String BATCH_SIZE = "batch.size";
    public static final long BATCH_SIZE_DEFAULT = 500;
    public static final String BATCH_SIZE_DOC = "Maximum number of records to include in a single read when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";

    public static final String KEY_PATTERNS = "redis.keys.patterns";
    public static final String KEY_PATTERNS_DEFAULT = "*";
    public static final String KEY_PATTERNS_DOC = "Keyspace glob-style patterns to subscribe to, comma-separated.";

    public static final String STREAM_NAME = "redis.stream.name";
    public static final String STREAM_NAME_DOC = "Name of the Redis stream to read from";

    public static final String STREAM_OFFSET = "redis.stream.offset";
    public static final String STREAM_OFFSET_DEFAULT = "0-0";
    public static final String STREAM_OFFSET_DOC = "Stream offset to start reading from";

    public static final String STREAM_CONSUMER_GROUP = "redis.stream.consumer.group";
    public static final String STREAM_CONSUMER_GROUP_DEFAULT = "kafka-consumer-group";
    public static final String STREAM_CONSUMER_GROUP_DOC = "Stream consumer group";

    public static final String STREAM_CONSUMER_NAME = "redis.stream.consumer.name";
    public static final String STREAM_CONSUMER_NAME_DEFAULT = "consumer-" + TOKEN_TASK;
    public static final String STREAM_CONSUMER_NAME_DOC = "A format string for the stream consumer, which may contain '" + TOKEN_TASK + "' as a placeholder for the task id.\nFor example, 'consumer-" + TOKEN_TASK + "' for the task id '123' will map to the consumer name 'consumer-123'.";


    public static final String STREAM_BLOCK = "redis.stream.block";
    public static final long STREAM_BLOCK_DEFAULT = 100;
    public static final String STREAM_BLOCK_DOC = "The max amount of time in milliseconds to wait while polling for stream messages (XREAD [BLOCK milliseconds])";

    private final ReaderType readerType;
    private final List<String> keyPatterns;
    private final String streamName;
    private final String streamOffset;
    private final String streamConsumerGroup;
    private final String streamConsumerName;
    private final Long batchSize;
    private final Long streamBlock;
    private final String topicName;

    public RedisEnterpriseSourceConfig(Map<?, ?> originals) {
        super(new RedisEnterpriseSourceConfigDef(), originals);
        this.topicName = getString(TOPIC);
        this.readerType = ConfigUtils.getEnum(ReaderType.class, this, READER_TYPE);
        this.batchSize = getLong(BATCH_SIZE);
        this.keyPatterns = getList(KEY_PATTERNS);
        this.streamName = getString(STREAM_NAME);
        this.streamOffset = getString(STREAM_OFFSET);
        this.streamConsumerGroup = getString(STREAM_CONSUMER_GROUP);
        this.streamConsumerName = getString(STREAM_CONSUMER_NAME);
        this.streamBlock = getLong(STREAM_BLOCK);
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

    public static class RedisEnterpriseSourceConfigDef extends RedisEnterpriseConfigDef {

        public RedisEnterpriseSourceConfigDef() {
            define();
        }

        public RedisEnterpriseSourceConfigDef(ConfigDef base) {
            super(base);
            define();
        }

        private void define() {
            define(ConfigKeyBuilder.of(TOPIC, ConfigDef.Type.STRING).defaultValue(TOPIC_DEFAULT).importance(ConfigDef.Importance.MEDIUM).documentation(TOPIC_DOC).build());
            define(ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.LONG).defaultValue(BATCH_SIZE_DEFAULT).importance(ConfigDef.Importance.LOW).documentation(BATCH_SIZE_DOC).validator(ConfigDef.Range.atLeast(1L)).build());
            define(ConfigKeyBuilder.of(READER_TYPE, ConfigDef.Type.STRING).documentation(READER_DOC).defaultValue(READER_DEFAULT).importance(ConfigDef.Importance.HIGH).validator(Validators.validEnum(ReaderType.class)).internalConfig(true).build());
            define(ConfigKeyBuilder.of(KEY_PATTERNS, Type.LIST).documentation(KEY_PATTERNS_DOC).defaultValue(KEY_PATTERNS_DEFAULT).importance(Importance.MEDIUM).internalConfig(true).build());
            define(ConfigKeyBuilder.of(STREAM_NAME, ConfigDef.Type.STRING).documentation(STREAM_NAME_DOC).importance(ConfigDef.Importance.HIGH).build());
            define(ConfigKeyBuilder.of(STREAM_OFFSET, ConfigDef.Type.STRING).documentation(STREAM_OFFSET_DOC).defaultValue(STREAM_OFFSET_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(STREAM_CONSUMER_GROUP, ConfigDef.Type.STRING).documentation(STREAM_CONSUMER_GROUP_DOC).defaultValue(STREAM_CONSUMER_GROUP_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(STREAM_CONSUMER_NAME, ConfigDef.Type.STRING).documentation(STREAM_CONSUMER_NAME_DOC).defaultValue(STREAM_CONSUMER_NAME_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(STREAM_BLOCK, ConfigDef.Type.LONG).defaultValue(STREAM_BLOCK_DEFAULT).importance(ConfigDef.Importance.LOW).documentation(STREAM_BLOCK_DOC).validator(ConfigDef.Range.atLeast(1L)).build());
        }

    }

    public enum ReaderType {
        KEYS, STREAM
    }

}
