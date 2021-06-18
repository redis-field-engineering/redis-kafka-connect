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
package com.redislabs.kafka.connect.sink;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.redislabs.kafka.connect.common.RedisEnterpriseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Map;

public class RedisEnterpriseSinkConfig extends RedisEnterpriseConnectorConfig {

    public static final String TOKEN_TOPIC = "${topic}";

    public static final String KEY = "redis.key";
    public static final String KEY_DEFAULT = TOKEN_TOPIC;
    public static final String KEY_DOC = "A format string for the destination stream/set/zset/list key, which may contain '" + TOKEN_TOPIC + "' as a placeholder for the originating topic name.\nFor example, ``kafka_" + TOKEN_TOPIC + "`` for the topic 'orders' will map to the Redis key " + "'kafka_orders'.";

    public static final String MULTIEXEC = "redis.multiexec";
    public static final String MULTIEXEC_DEFAULT = "false";
    public static final String MULTIEXEC_DOC = "Whether to execute Redis commands in multi/exec transactions.";

    public static final String TYPE = "redis.type";
    public static final String TYPE_DEFAULT = DataType.STREAM.name();
    public static final String TYPE_DOC = "Destination data structure: " + String.join(", ", Arrays.stream(DataType.values()).map(DataType::name).toArray(String[]::new));

    public static final String PUSH_DIRECTION = "redis.push.direction";
    public static final String PUSH_DIRECTION_DEFAULT = PushDirection.LEFT.name();
    public static final String PUSH_DIRECTION_DOC = "List push direction: " + PushDirection.LEFT + " (LPUSH) or " + PushDirection.RIGHT + " (RPUSH)";

    private final DataType type;
    private final String keyFormat;
    private final PushDirection pushDirection;
    private final boolean multiexec;

    public RedisEnterpriseSinkConfig(Map<?, ?> originals) {
        super(config(), originals);
        type = ConfigUtils.getEnum(DataType.class, this, TYPE);
        keyFormat = getString(KEY).trim();
        pushDirection = ConfigUtils.getEnum(PushDirection.class, this, PUSH_DIRECTION);
        multiexec = Boolean.TRUE.equals(getBoolean(MULTIEXEC));
    }

    public DataType getType() {
        return type;
    }

    public String getKeyFormat() {
        return keyFormat;
    }

    public PushDirection getPushDirection() {
        return pushDirection;
    }

    public boolean isMultiexec() {
        return multiexec;
    }

    public static ConfigDef config() {
        return RedisEnterpriseConnectorConfig.config()
                .define(ConfigKeyBuilder.of(TYPE, ConfigDef.Type.STRING).documentation(TYPE_DOC).defaultValue(TYPE_DEFAULT).validator(Validators.validEnum(DataType.class)).importance(ConfigDef.Importance.HIGH).build())
                .define(ConfigKeyBuilder.of(KEY, ConfigDef.Type.STRING).documentation(KEY_DOC).defaultValue(KEY_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build())
                .define(ConfigKeyBuilder.of(PUSH_DIRECTION, ConfigDef.Type.STRING).documentation(PUSH_DIRECTION_DOC).defaultValue(PUSH_DIRECTION_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build())
                .define(ConfigKeyBuilder.of(MULTIEXEC, ConfigDef.Type.BOOLEAN).documentation(MULTIEXEC_DOC).defaultValue(MULTIEXEC_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
    }

    public enum DataType {
        HASH, STRING, STREAM, LIST, SET, ZSET
    }

    public enum PushDirection {
        LEFT, RIGHT
    }
}
