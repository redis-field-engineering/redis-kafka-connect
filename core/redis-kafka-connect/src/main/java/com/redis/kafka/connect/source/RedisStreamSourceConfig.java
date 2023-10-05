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

import java.util.Map;
import java.util.Objects;

public class RedisStreamSourceConfig extends RedisSourceConfig {

    public static final String STREAM_DELIVERY_AT_MOST_ONCE = "at-most-once";

    public static final String STREAM_DELIVERY_AT_LEAST_ONCE = "at-least-once";

    public static final RedisStreamSourceConfigDef CONFIG = new RedisStreamSourceConfigDef();

    private final String streamName;

    private final String streamOffset;

    private final String streamDelivery;

    private final String streamConsumerGroup;

    private final String streamConsumerName;

    private final long streamBlock;

    private final String topicName;

    public RedisStreamSourceConfig(Map<?, ?> originals) {
        super(new RedisStreamSourceConfigDef(), originals);
        this.topicName = getString(RedisStreamSourceConfigDef.TOPIC_CONFIG);
        this.streamName = getString(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG);
        this.streamOffset = getString(RedisStreamSourceConfigDef.STREAM_OFFSET_CONFIG);
        this.streamDelivery = getString(RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG);
        this.streamConsumerGroup = getString(RedisStreamSourceConfigDef.STREAM_CONSUMER_GROUP_CONFIG);
        this.streamConsumerName = getString(RedisStreamSourceConfigDef.STREAM_CONSUMER_NAME_CONFIG);
        this.streamBlock = getLong(RedisStreamSourceConfigDef.STREAM_BLOCK_CONFIG);
    }

    public Long getStreamBlock() {
        return streamBlock;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getStreamOffset() {
        return streamOffset;
    }

    public String getStreamDelivery() {
        return streamDelivery;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(batchSize, streamBlock, streamConsumerGroup, streamConsumerName, streamName,
                streamOffset, streamDelivery, topicName);
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
        RedisStreamSourceConfig other = (RedisStreamSourceConfig) obj;
        return Objects.equals(batchSize, other.batchSize) && Objects.equals(streamBlock, other.streamBlock)
                && Objects.equals(streamConsumerGroup, other.streamConsumerGroup)
                && Objects.equals(streamConsumerName, other.streamConsumerName) && Objects.equals(streamName, other.streamName)
                && Objects.equals(streamOffset, other.streamOffset) && Objects.equals(streamDelivery, other.streamDelivery)
                && Objects.equals(topicName, other.topicName);
    }

}
