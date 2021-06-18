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
import com.redislabs.kafka.connect.common.RedisEnterpriseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedisEnterpriseSourceConfig extends RedisEnterpriseConnectorConfig {

    public static final String TOKEN_STREAM = "${stream}";

    public static final String STREAM_NAME = "redis.stream.name";
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

    public static final String TOPIC_NAME_FORMAT = "topic.name.format";
    public static final String TOPIC_NAME_FORMAT_DEFAULT = TOKEN_STREAM;
    public static final String TOPIC_NAME_FORMAT_DOC = "A format string for the destination topic name, which may contain '${stream}' as a " + "placeholder for the originating topic name.\n" + "For example, ``redis_${stream}`` for the stream 'orders' will map to the topic name " + "'redis_orders'.";

    private final String streamName;
    private final String streamOffset;
    private final Long streamCount;
    private final Long streamBlock;
    private final String topicNameFormat;

    public RedisEnterpriseSourceConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.streamName = getString(STREAM_NAME);
        this.streamOffset = getString(STREAM_OFFSET);
        this.streamCount = getLong(STREAM_COUNT);
        this.streamBlock = getLong(STREAM_BLOCK);
        this.topicNameFormat = getString(TOPIC_NAME_FORMAT);
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

    public static ConfigDef config() {
        return RedisEnterpriseConnectorConfig.config()
        .define(ConfigKeyBuilder.of(STREAM_NAME, ConfigDef.Type.STRING).documentation(STREAM_NAME_DOC).importance(ConfigDef.Importance.HIGH).validator(ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.NonEmptyString())).build())
        .define(ConfigKeyBuilder.of(STREAM_OFFSET, ConfigDef.Type.STRING).documentation(STREAM_OFFSET_DOC).defaultValue(STREAM_OFFSET_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build())
        .define(ConfigKeyBuilder.of(STREAM_COUNT, ConfigDef.Type.LONG).defaultValue(STREAM_COUNT_DEFAULT).importance(ConfigDef.Importance.LOW).documentation(STREAM_COUNT_DOC).validator(ConfigDef.Range.atLeast(1L)).build())
        .define(ConfigKeyBuilder.of(STREAM_BLOCK, ConfigDef.Type.LONG).defaultValue(STREAM_BLOCK_DEFAULT).importance(ConfigDef.Importance.LOW).documentation(STREAM_BLOCK_DOC).validator(ConfigDef.Range.atLeast(1L)).build())
        .define(ConfigKeyBuilder.of(TOPIC_NAME_FORMAT, ConfigDef.Type.STRING).defaultValue(TOPIC_NAME_FORMAT_DEFAULT).importance(ConfigDef.Importance.MEDIUM).documentation(TOPIC_NAME_FORMAT_DOC).build());
    }

}
