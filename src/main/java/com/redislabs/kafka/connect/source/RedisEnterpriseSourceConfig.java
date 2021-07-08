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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisEnterpriseSourceConfig extends RedisEnterpriseConfig {


    public static final String TOKEN_STREAM = "${stream}";

    public static final String READER_TYPE = "redis.reader";
    public static final String READER_DEFAULT = ReaderType.KEYS.name();
    public static final String READER_DOC = "Source from which to read Redis records. " + ReaderType.KEYS + ": generate records from key events and respective values generated from write operations in the Redis database. " + ReaderType.STREAM + ": read messages from a Redis stream";

    public static final String KEYS_PATTERN = "redis.keys.pattern";
    public static final String KEYS_PATTERN_DEFAULT = "*";
    public static final String KEYS_PATTERN_DOC = "Generate records only for key names that match the given glob-style pattern";

    public static final String KEYS_EVENT_TYPES = "redis.keys.eventtypes";
    public static final String KEYS_EVENT_TYPES_DEFAULT = "";
    public static final String KEYS_EVENT_TYPES_DOC = "Comma-separated event types for key reader. The list may contain one or more command or event (default: None)";

    public static final String STREAM_NAME = "redis.stream.name";
    public static final String STREAM_NAME_DEFAULT = "com.redislabs.kafka.connect.source.stream";
    public static final String STREAM_NAME_DOC = "Name of the Redis stream to read from";

    public static final String STREAM_OFFSET = "redis.stream.offset";
    public static final String STREAM_OFFSET_DEFAULT = "0-0";
    public static final String STREAM_OFFSET_DOC = "Stream offset to start reading from";

    public static final String STREAM_COUNT = "redis.stream.count";
    public static final long STREAM_COUNT_DEFAULT = 50;
    public static final String STREAM_COUNT_DOC = "Maximum number of stream messages to include in a single read when polling for new data (XREAD [COUNT count]). This setting can be used to limit the amount of data buffered internally in the connector.";

    public static final String STREAM_BLOCK = "redis.stream.block";
    public static final long STREAM_BLOCK_DEFAULT = 100;
    public static final String STREAM_BLOCK_DOC = "The max amount of time in milliseconds to wait while polling for stream messages (XREAD [BLOCK milliseconds])";

    public static final String TOPIC_NAME_FORMAT = "topic.name";
    public static final String TOPIC_NAME_FORMAT_DEFAULT = TOKEN_STREAM;
    public static final String TOPIC_NAME_FORMAT_DOC = String.format("A format string for the destination topic name, which may contain '%s' as a placeholder for the originating stream name. For example `redis_%s` for the stream 'orders' will map to the topic name 'redis_orders'.", TOKEN_STREAM, TOKEN_STREAM);

    private final ReaderType readerType;
    private final List<String> keysEventTypes;
    private final String keysPattern;
    private final String streamName;
    private final String streamOffset;
    private final Long streamCount;
    private final Long streamBlock;
    private final String topicNameFormat;

    public RedisEnterpriseSourceConfig(Map<?, ?> originals) {
        super(new RedisEnterpriseSourceConfigDef(), originals);
        this.readerType = ConfigUtils.getEnum(ReaderType.class, this, READER_TYPE);
        this.keysEventTypes = splitCommaSeparated(getString(KEYS_EVENT_TYPES));
        this.keysPattern = getString(KEYS_PATTERN);
        this.streamName = getString(STREAM_NAME);
        this.streamOffset = getString(STREAM_OFFSET);
        this.streamCount = getLong(STREAM_COUNT);
        this.streamBlock = getLong(STREAM_BLOCK);
        this.topicNameFormat = getString(TOPIC_NAME_FORMAT);
    }

    private List<String> splitCommaSeparated(String string) {
        if (string.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(string.split(","));
    }

    public List<String> getKeysEventTypes() {
        return keysEventTypes;
    }

    public ReaderType getReaderType() {
        return readerType;
    }

    public String getKeysPattern() {
        return keysPattern;
    }

    public Long getStreamBlock() {
        return streamBlock;
    }

    public Long getStreamCount() {
        return streamCount;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getStreamOffset() {
        return streamOffset;
    }

    public String getTopicNameFormat() {
        return topicNameFormat;
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
            define(ConfigKeyBuilder.of(READER_TYPE, ConfigDef.Type.STRING).documentation(READER_DOC).defaultValue(READER_DEFAULT).importance(ConfigDef.Importance.HIGH).validator(Validators.validEnum(ReaderType.class)).build());
            define(ConfigKeyBuilder.of(KEYS_EVENT_TYPES, Type.STRING).documentation(KEYS_EVENT_TYPES_DOC).defaultValue(KEYS_EVENT_TYPES_DEFAULT).importance(Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(KEYS_PATTERN, Type.STRING).documentation(KEYS_PATTERN_DOC).defaultValue(KEYS_PATTERN_DEFAULT).importance(Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(STREAM_NAME, ConfigDef.Type.STRING).documentation(STREAM_NAME_DOC).defaultValue(STREAM_NAME_DEFAULT).importance(ConfigDef.Importance.HIGH).build());
            define(ConfigKeyBuilder.of(STREAM_OFFSET, ConfigDef.Type.STRING).documentation(STREAM_OFFSET_DOC).defaultValue(STREAM_OFFSET_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(STREAM_COUNT, ConfigDef.Type.LONG).defaultValue(STREAM_COUNT_DEFAULT).importance(ConfigDef.Importance.LOW).documentation(STREAM_COUNT_DOC).validator(ConfigDef.Range.atLeast(1L)).build());
            define(ConfigKeyBuilder.of(STREAM_BLOCK, ConfigDef.Type.LONG).defaultValue(STREAM_BLOCK_DEFAULT).importance(ConfigDef.Importance.LOW).documentation(STREAM_BLOCK_DOC).validator(ConfigDef.Range.atLeast(1L)).build());
            define(ConfigKeyBuilder.of(TOPIC_NAME_FORMAT, ConfigDef.Type.STRING).defaultValue(TOPIC_NAME_FORMAT_DEFAULT).importance(ConfigDef.Importance.MEDIUM).documentation(TOPIC_NAME_FORMAT_DOC).build());
        }

    }

    public enum ReaderType {
        KEYS, STREAM
    }

}
