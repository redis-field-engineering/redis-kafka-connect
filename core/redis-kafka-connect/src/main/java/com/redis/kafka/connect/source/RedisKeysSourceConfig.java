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

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Parses key snapshot and keyspace notification settings. */
public class RedisKeysSourceConfig extends RedisSourceConfig {

    /** Selects whether to include an initial snapshot alongside live notifications. */
    public enum ReaderMode {
        /** Reads an initial snapshot and live updates. */
        LIVE,
        /** Reads only live updates. */
        LIVEONLY
    }

    /** Configuration definition for this connector. */
    public static final RedisKeysSourceConfigDef CONFIG = new RedisKeysSourceConfigDef();

    private final String keyPattern;

    private final String topicName;

    private final Duration idleTimeout;

    private final ReaderMode mode;

    /**
     * Parses and validates the connector configuration.
     *
     * @param originals connector properties
     */
    public RedisKeysSourceConfig(Map<?, ?> originals) {
        super(new RedisKeysSourceConfigDef(), originals);
        this.topicName = getString(RedisKeysSourceConfigDef.TOPIC_CONFIG);
        this.keyPattern = getString(RedisKeysSourceConfigDef.KEY_PATTERN_CONFIG);
        this.idleTimeout = Duration.ofMillis(getLong(RedisKeysSourceConfigDef.IDLE_TIMEOUT_CONFIG));
        this.mode = ReaderMode.valueOf(getString(RedisKeysSourceConfigDef.MODE_CONFIG).toUpperCase(Locale.ROOT));
    }

    /** {@return the Redis glob pattern used to select keys} */
    public String getKeyPattern() {
        return keyPattern;
    }

    /** {@return the destination Kafka topic name or template} */
    public String getTopicName() {
        return topicName;
    }

    /** {@return the maximum idle wait per poll; zero selects the bounded default interval} */
    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    /** {@return whether to include an initial snapshot alongside live updates} */
    public ReaderMode getMode() {
        return mode;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(batchSize, keyPattern, topicName);
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
        RedisKeysSourceConfig other = (RedisKeysSourceConfig) obj;
        return Objects.equals(batchSize, other.batchSize) && Objects.equals(keyPattern, other.keyPattern)
                && Objects.equals(topicName, other.topicName);
    }

}
