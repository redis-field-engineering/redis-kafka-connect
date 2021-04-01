/**
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
package com.redislabs.kafkaconnect.source;

import com.redislabs.kafkaconnect.common.RedisEnterpriseConfigException;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Collections;
import java.util.Map;

public class RedisEnterpriseSourceConfig extends AbstractConfig {

    public static final ConfigDef CONFIG_DEF = new RedisEnterpriseSourceConfigDef();

    public static final String REDIS_URI = "redis.uri";
    public static final String REDIS_URI_DEFAULT = "redis://localhost:6379";
    public static final String REDIS_URI_DISPLAY = "Connection URI";
    public static final String REDIS_URI_DOC = "URI of the Redis Enterprise database to connect to, e.g. redis://redis-12000.redislabs.com:12000";

    public static final String STREAM_NAME = "stream";
    public static final String STREAM_NAME_DISPLAY = "Stream name";
    public static final String STREAM_NAME_DOC = "Name of the Redis stream to read from";

    public static final String STREAM_OFFSET = "stream.offset";
    public static final String STREAM_OFFSET_DEFAULT = "0-0";
    public static final String STREAM_OFFSET_DISPLAY = "Stream offset";
    public static final String STREAM_OFFSET_DOC = "Stream offset to start reading from";

    public static final String STREAM_COUNT = "stream.count";
    public static final long STREAM_COUNT_DEFAULT = 50;
    public static final String STREAM_COUNT_DISPLAY = "The maximum batch size";
    public static final String STREAM_COUNT_DOC = "Maximum number of stream messages to include in a single read when polling for new data (XREAD [COUNT count]). This setting can be used to limit the amount of data buffered internally in the connector.";

    public static final String STREAM_BLOCK = "stream.block";
    public static final long STREAM_BLOCK_DEFAULT = 100;
    public static final String STREAM_BLOCK_DISPLAY = "Max poll duration";
    public static final String STREAM_BLOCK_DOC = "The max amount of time in milliseconds to wait while polling for stream messages (XREAD [BLOCK milliseconds])";

    public static final String TOPIC_NAME_FORMAT = "topic.name.format";
    public static final String TOPIC_NAME_FORMAT_DEFAULT = "${stream}";
    public static final String TOPIC_NAME_FORMAT_DOC = "A format string for the destination topic name, which may contain '${stream}' as a " + "placeholder for the originating topic name.\n" + "For example, ``redis_${stream}`` for the stream 'orders' will map to the topic name " + "'redis_orders'.";
    public static final String TOPIC_NAME_FORMAT_DISPLAY = "Topic Name Format";


    @Getter
    private final String redisUri;

    public RedisEnterpriseSourceConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
        redisUri = getString(REDIS_URI);
    }

    public String getStreamName() {
        return getString(STREAM_NAME);
    }

    public String getStreamOffset() {
        return getString(STREAM_OFFSET);
    }

    public long getStreamCount() {
        return getLong(STREAM_COUNT);
    }

    public long getStreamBlock() {
        return getLong(STREAM_BLOCK);
    }

    public String getTopicNameFormat() {
        return getString(TOPIC_NAME_FORMAT);
    }

    public void validateStream() {
        String stream = getString(STREAM_NAME);
        if (stream == null || stream.isEmpty()) {
            throw new RedisEnterpriseConfigException(STREAM_NAME, stream, String.format("Missing stream configuration: '%s'", STREAM_NAME));
        }
    }

    public static class RedisEnterpriseSourceConfigDef extends ConfigDef {

        public RedisEnterpriseSourceConfigDef() {
            String group = "Redis Enterprise";
            int index = 0;
            define(REDIS_URI, Type.STRING, REDIS_URI_DEFAULT, Importance.HIGH, REDIS_URI_DOC, group, ++index, Width.MEDIUM, REDIS_URI_DISPLAY);
            define(STREAM_NAME, Type.STRING, null, Importance.HIGH, STREAM_NAME_DOC, group, ++index, Width.SHORT, STREAM_NAME_DISPLAY);
            define(STREAM_OFFSET, Type.STRING, STREAM_OFFSET_DEFAULT, Importance.MEDIUM, STREAM_OFFSET_DOC, group, ++index, Width.SHORT, STREAM_OFFSET_DISPLAY);
            define(STREAM_COUNT, Type.LONG, STREAM_COUNT_DEFAULT, Range.atLeast(1), Importance.LOW, STREAM_COUNT_DOC, group, ++index, Width.MEDIUM, STREAM_COUNT_DISPLAY);
            define(STREAM_BLOCK, Type.LONG, STREAM_BLOCK_DEFAULT, Range.atLeast(1), Importance.LOW, STREAM_BLOCK_DOC, group, ++index, Width.MEDIUM, STREAM_BLOCK_DISPLAY);
            group = "Connector";
            index = 0;
            define(TOPIC_NAME_FORMAT, Type.STRING, TOPIC_NAME_FORMAT_DEFAULT, Importance.MEDIUM, TOPIC_NAME_FORMAT_DOC, group, ++index, Width.LONG, TOPIC_NAME_FORMAT_DISPLAY);
        }

        @Override
        public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
            Map<String, ConfigValue> results = super.validateAll(props);
            if (results.values().stream().anyMatch((c) -> !c.errorMessages().isEmpty())) {
                return results;
            }

            RedisEnterpriseSourceConfig config = new RedisEnterpriseSourceConfig(props);
            try {
                config.validateStream();
            } catch (RedisEnterpriseConfigException e) {
                results.put(e.getName(), new ConfigValue(e.getName(), e.getValue(), Collections.emptyList(), Collections.singletonList(e.getMessage())));
            }
            return results;
        }
    }
}
