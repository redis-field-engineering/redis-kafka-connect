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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.redis.kafka.connect.common.RedisEnterpriseConfig;

public class RedisEnterpriseSinkConfig extends RedisEnterpriseConfig {

	public enum DataType {
		HASH, JSON, TIMESERIES, STRING, STREAM, LIST, SET, ZSET
	}

	public enum PushDirection {
		LEFT, RIGHT
	}

	public static final String TOKEN_TOPIC = "${topic}";

	public static final String CHARSET_CONFIG = "redis.charset";
	public static final String CHARSET_DEFAULT = Charset.defaultCharset().name();
	public static final String CHARSET_DOC = "Character set to encode Redis key and value strings.";

	public static final String KEY_CONFIG = "redis.key";
	public static final String KEY_DEFAULT = TOKEN_TOPIC;
	public static final String KEY_DOC = "A format string for destination key space, which may contain '" + TOKEN_TOPIC
			+ "' as a placeholder for the originating topic name.\nFor example, ``kafka_" + TOKEN_TOPIC
			+ "`` for the topic 'orders' will map to the Redis key space "
			+ "'kafka_orders'.\nLeave empty for passthrough (only applicable to non-collection data structures).";

	public static final String SEPARATOR_CONFIG = "redis.separator";
	public static final String SEPARATOR_DEFAULT = ":";
	public static final String SEPARATOR_DOC = "Separator for non-collection destination keys.";

	public static final String MULTIEXEC_CONFIG = "redis.multiexec";
	public static final String MULTIEXEC_DEFAULT = "false";
	public static final String MULTIEXEC_DOC = "Whether to execute Redis commands in multi/exec transactions.";

	public static final String WAIT_REPLICAS_CONFIG = "redis.wait.replicas";
	public static final String WAIT_REPLICAS_DEFAULT = "0";
	public static final String WAIT_REPLICAS_DOC = "Number of replicas to wait for. Use 0 to disable waiting for replicas.";

	public static final String WAIT_TIMEOUT_CONFIG = "redis.wait.timeout";
	public static final String WAIT_TIMEOUT_DEFAULT = "1000";
	public static final String WAIT_TIMEOUT_DOC = "Timeout for WAIT command.";

	public static final String TYPE_CONFIG = "redis.type";
	public static final String TYPE_DEFAULT = DataType.STREAM.name();
	public static final String TYPE_DOC = "Destination data structure: " + ConfigUtils.enumValues(DataType.class);

	protected static final Set<DataType> MULTI_EXEC_TYPES = new HashSet<>(
			Arrays.asList(DataType.STREAM, DataType.LIST, DataType.SET, DataType.ZSET));

	public static final String PUSH_DIRECTION_CONFIG = "redis.push.direction";
	public static final String PUSH_DIRECTION_DEFAULT = PushDirection.LEFT.name();
	public static final String PUSH_DIRECTION_DOC = "List push direction: " + PushDirection.LEFT + " (LPUSH) or "
			+ PushDirection.RIGHT + " (RPUSH)";

	private final Charset charset;
	private final DataType type;
	private final String keyspace;
	private final String separator;
	private final PushDirection pushDirection;
	private final boolean multiexec;
	private final int waitReplicas;
	private final long waitTimeout;

	public RedisEnterpriseSinkConfig(Map<?, ?> originals) {
		super(new RedisEnterpriseSinkConfigDef(), originals);
		String charsetName = getString(CHARSET_CONFIG).trim();
		charset = Charset.forName(charsetName);
		type = ConfigUtils.getEnum(DataType.class, this, TYPE_CONFIG);
		keyspace = getString(KEY_CONFIG).trim();
		separator = getString(SEPARATOR_CONFIG).trim();
		pushDirection = ConfigUtils.getEnum(PushDirection.class, this, PUSH_DIRECTION_CONFIG);
		multiexec = Boolean.TRUE.equals(getBoolean(MULTIEXEC_CONFIG));
		waitReplicas = getInt(WAIT_REPLICAS_CONFIG);
		waitTimeout = getLong(WAIT_TIMEOUT_CONFIG);
	}

	public Charset getCharset() {
		return charset;
	}

	public DataType getType() {
		return type;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getSeparator() {
		return separator;
	}

	public PushDirection getPushDirection() {
		return pushDirection;
	}

	public boolean isMultiexec() {
		return multiexec;
	}

	public int getWaitReplicas() {
		return waitReplicas;
	}

	public long getWaitTimeout() {
		return waitTimeout;
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
			define(ConfigKeyBuilder.of(CHARSET_CONFIG, ConfigDef.Type.STRING).documentation(CHARSET_DOC)
					.defaultValue(CHARSET_DEFAULT).importance(ConfigDef.Importance.HIGH).build());
			define(ConfigKeyBuilder.of(TYPE_CONFIG, ConfigDef.Type.STRING).documentation(TYPE_DOC)
					.defaultValue(TYPE_DEFAULT).importance(ConfigDef.Importance.HIGH)
					.validator(Validators.validEnum(DataType.class)).build());
			define(ConfigKeyBuilder.of(KEY_CONFIG, ConfigDef.Type.STRING).documentation(KEY_DOC)
					.defaultValue(KEY_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(SEPARATOR_CONFIG, ConfigDef.Type.STRING).documentation(SEPARATOR_DOC)
					.defaultValue(SEPARATOR_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(PUSH_DIRECTION_CONFIG, ConfigDef.Type.STRING).documentation(PUSH_DIRECTION_DOC)
					.defaultValue(PUSH_DIRECTION_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(MULTIEXEC_CONFIG, ConfigDef.Type.BOOLEAN).documentation(MULTIEXEC_DOC)
					.defaultValue(MULTIEXEC_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(WAIT_REPLICAS_CONFIG, ConfigDef.Type.INT).documentation(WAIT_REPLICAS_DOC)
					.defaultValue(WAIT_REPLICAS_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
			define(ConfigKeyBuilder.of(WAIT_TIMEOUT_CONFIG, ConfigDef.Type.LONG).documentation(WAIT_TIMEOUT_DOC)
					.defaultValue(WAIT_TIMEOUT_DEFAULT).importance(ConfigDef.Importance.MEDIUM).build());
		}

		@Override
		public Map<String, ConfigValue> validateAll(Map<String, String> props) {
			Map<String, ConfigValue> results = super.validateAll(props);
			if (results.values().stream().map(ConfigValue::errorMessages).anyMatch(l -> !l.isEmpty())) {
				return results;
			}
			DataType dataType = dataType(props);
			String multiexec = props.getOrDefault(MULTIEXEC_CONFIG, MULTIEXEC_DEFAULT).trim();
			if (multiexec.equalsIgnoreCase("true") && !MULTI_EXEC_TYPES.contains(dataType)) {
				String supportedTypes = String.join(", ",
						MULTI_EXEC_TYPES.stream().map(Enum::name).toArray(String[]::new));
				results.get(MULTIEXEC_CONFIG)
						.addErrorMessage("multi/exec is only supported with these data structures: " + supportedTypes);
			}
			String charsetName = props.getOrDefault(CHARSET_CONFIG, CHARSET_DEFAULT).trim();
			try {
				Charset.forName(charsetName);
			} catch (Exception e) {
				results.get(CHARSET_CONFIG).addErrorMessage(e.getMessage());
			}
			return results;
		}

		private DataType dataType(Map<String, String> props) {
			return DataType.valueOf(props.getOrDefault(TYPE_CONFIG, TYPE_DEFAULT));
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ Objects.hash(charset, keyspace, separator, multiexec, pushDirection, type, waitReplicas, waitTimeout);
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
		RedisEnterpriseSinkConfig other = (RedisEnterpriseSinkConfig) obj;
		return Objects.equals(charset, other.charset) && Objects.equals(keyspace, other.keyspace)
				&& Objects.equals(separator, other.separator) && multiexec == other.multiexec
				&& pushDirection == other.pushDirection && type == other.type && waitReplicas == other.waitReplicas
				&& waitTimeout == other.waitTimeout;
	}

}
