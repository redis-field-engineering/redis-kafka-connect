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
package com.redis.kafka.connect.sink;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.redis.kafka.connect.common.RedisEnterpriseConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RedisEnterpriseSinkConfig extends RedisEnterpriseConfig {

    public static final String TOKEN_TOPIC = "${topic}";

    public static final String CHARSET = "redis.charset";
    public static final String CHARSET_DEFAULT = Charset.defaultCharset().name();
    public static final String CHARSET_DOC = "Character set to encode Redis key and value strings.";

    public static final String KEY = "redis.key";
    public static final String KEY_DEFAULT = TOKEN_TOPIC;
    public static final String KEY_DOC = "A format string for the destination stream/set/zset/list key, which may contain '" + TOKEN_TOPIC + "' as a placeholder for the originating topic name.\nFor example, ``kafka_" + TOKEN_TOPIC + "`` for the topic 'orders' will map to the Redis key " + "'kafka_orders'.";

    public static final String MULTIEXEC = "redis.multiexec";
    public static final String MULTIEXEC_DEFAULT = "false";
    public static final String MULTIEXEC_DOC = "Whether to execute Redis commands in multi/exec transactions.";

    public static final String TYPE = "redis.type";
    public static final String TYPE_DEFAULT = DataType.STREAM.name();
    public static final String TYPE_DOC = "Destination data structure: " + ConfigUtils.enumValues(DataType.class);

    public static final Set<DataType> MULTI_EXEC_TYPES = new HashSet<>(Arrays.asList(DataType.STREAM, DataType.LIST, DataType.SET, DataType.ZSET));

    public static final String PUSH_DIRECTION = "redis.push.direction";
    public static final String PUSH_DIRECTION_DEFAULT = PushDirection.LEFT.name();
    public static final String PUSH_DIRECTION_DOC = "List push direction: " + PushDirection.LEFT + " (LPUSH) or " + PushDirection.RIGHT + " (RPUSH)";

    private final Charset charset;
    private final DataType type;
    private final String keyFormat;
    private final PushDirection pushDirection;
    private final boolean multiexec;

    public RedisEnterpriseSinkConfig(Map<?, ?> originals) {
        super(new RedisEnterpriseSinkConfigDef(), originals);
        String charsetName = getString(CHARSET).trim();
        charset = Charset.forName(charsetName);
        type = ConfigUtils.getEnum(DataType.class, this, TYPE);
        keyFormat = getString(KEY).trim();
        pushDirection = ConfigUtils.getEnum(PushDirection.class, this, PUSH_DIRECTION);
        multiexec = Boolean.TRUE.equals(getBoolean(MULTIEXEC));
    }

    public Charset getCharset() {
        return charset;
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

    public static class RedisEnterpriseSinkConfigDef extends RedisEnterpriseConfigDef {

        public RedisEnterpriseSinkConfigDef() {
            define();
        }

        public RedisEnterpriseSinkConfigDef(ConfigDef base) {
            super(base);
            define();
        }

        private void define() {
            define(ConfigKeyBuilder.of(CHARSET, ConfigDef.Type.STRING).documentation(CHARSET_DOC).defaultValue(CHARSET_DEFAULT).importance(ConfigDef.Importance.HIGH).build());
            define(ConfigKeyBuilder.of(TYPE, ConfigDef.Type.STRING).documentation(TYPE_DOC).defaultValue(TYPE_DEFAULT).importance(ConfigDef.Importance.HIGH).validator(Validators.validEnum(DataType.class)).build());
            define(ConfigKeyBuilder.of(KEY, ConfigDef.Type.STRING).documentation(KEY_DOC).defaultValue(KEY_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(PUSH_DIRECTION, ConfigDef.Type.STRING).documentation(PUSH_DIRECTION_DOC).defaultValue(PUSH_DIRECTION_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
            define(ConfigKeyBuilder.of(MULTIEXEC, ConfigDef.Type.BOOLEAN).documentation(MULTIEXEC_DOC).defaultValue(MULTIEXEC_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
        }

        @Override
        public Map<String, ConfigValue> validateAll(Map<String, String> props) {
            Map<String, ConfigValue> results = super.validateAll(props);
            if (results.values().stream().map(ConfigValue::errorMessages).anyMatch(l -> !l.isEmpty())) {
                return results;
            }
            DataType dataType = dataType(props);
            String multiexec = props.getOrDefault(MULTIEXEC, MULTIEXEC_DEFAULT).trim();
            if (multiexec.equalsIgnoreCase("true") && !MULTI_EXEC_TYPES.contains(dataType)) {
                String supportedTypes = String.join(", ", MULTI_EXEC_TYPES.stream().map(Enum::name).toArray(String[]::new));
                results.get(MULTIEXEC).addErrorMessage("multi/exec is only supported with these data structures: " + supportedTypes);
            }
            String charsetName = props.getOrDefault(CHARSET, CHARSET_DEFAULT).trim();
            try {
                Charset.forName(charsetName);
            } catch (Exception e) {
                results.get(CHARSET).addErrorMessage(e.getMessage());
            }
            return results;
        }

        private DataType dataType(Map<String, String> props) {
            return DataType.valueOf(props.getOrDefault(TYPE, TYPE_DEFAULT));
        }

    }

    public enum DataType {
        HASH, JSON, STRING, STREAM, LIST, SET, ZSET
    }

    public enum PushDirection {
        LEFT, RIGHT
    }
}
