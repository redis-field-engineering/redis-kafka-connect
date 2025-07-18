package com.redis.kafka.connect.sink;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.sink.RedisSinkConfig.RedisType;

public class RedisSinkConfigDef extends RedisConfigDef {

	public static final String TOKEN_TOPIC = "${topic}";

	public static final String CHARSET_CONFIG = "redis.charset";

	public static final String CHARSET_DEFAULT = Charset.defaultCharset().name();

	public static final String CHARSET_DOC = "Character set to encode Redis key and value strings.";

	public static final String KEYSPACE_CONFIG = "redis.keyspace";

	public static final String KEYSPACE_DEFAULT = TOKEN_TOPIC;

	public static final String KEYSPACE_DOC = "A format string for destination key space, which may contain '"
			+ TOKEN_TOPIC + "' as a placeholder for the originating topic name.\nFor example, ``kafka_" + TOKEN_TOPIC
			+ "`` for the topic 'orders' will map to the Redis key space " + "'kafka_orders'.";

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

	public static final String WAIT_TIMEOUT_DOC = "Timeout in millis for WAIT command.";

	public static final String TYPE_CONFIG = "redis.type";

	public static final RedisType TYPE_DEFAULT = RedisType.STREAM;

	public static final String TYPE_DOC = "Destination data structure: "
			+ String.join(",", Stream.of(RedisType.values()).map(RedisType::name).toArray(String[]::new));

	public static final String KEY_TTL_CONFIG = "redis.key.ttl";

	public static final String KEY_TTL_CONFIG_DEFAULT = "-1";

	public static final String KEY_TTL_CONFIG_DOC = "Time to live in seconds for the key. If not set, the record will not expire.";

	protected static final Set<RedisType> MULTI_EXEC_COMMANDS = Stream
			.of(RedisType.STREAM, RedisType.LIST, RedisType.SET, RedisType.ZSET).collect(Collectors.toSet());

	public RedisSinkConfigDef() {
		define();
	}

	public RedisSinkConfigDef(ConfigDef base) {
		super(base);
		define();
	}

	private void define() {
		define(CHARSET_CONFIG, Type.STRING, CHARSET_DEFAULT, Importance.HIGH, CHARSET_DOC);
		define(TYPE_CONFIG, Type.STRING, TYPE_DEFAULT.name(), Importance.HIGH, TYPE_DOC);
		define(KEYSPACE_CONFIG, Type.STRING, KEYSPACE_DEFAULT, Importance.MEDIUM, KEYSPACE_DOC);
		define(SEPARATOR_CONFIG, Type.STRING, SEPARATOR_DEFAULT, Importance.MEDIUM, SEPARATOR_DOC);
		define(MULTIEXEC_CONFIG, Type.BOOLEAN, MULTIEXEC_DEFAULT, Importance.MEDIUM, MULTIEXEC_DOC);
		define(WAIT_REPLICAS_CONFIG, Type.INT, WAIT_REPLICAS_DEFAULT, Importance.MEDIUM, WAIT_REPLICAS_DOC);
		define(WAIT_TIMEOUT_CONFIG, Type.LONG, WAIT_TIMEOUT_DEFAULT, Importance.MEDIUM, WAIT_TIMEOUT_DOC);
		define(KEY_TTL_CONFIG, Type.LONG, KEY_TTL_CONFIG_DEFAULT, Importance.MEDIUM, KEY_TTL_CONFIG_DOC);
	}

	@Override
	public Map<String, ConfigValue> validateAll(Map<String, String> props) {
		Map<String, ConfigValue> results = super.validateAll(props);
		if (results.values().stream().map(ConfigValue::errorMessages).anyMatch(l -> !l.isEmpty())) {
			return results;
		}
		RedisType command = redisCommand(props);
		String multiexec = props.getOrDefault(MULTIEXEC_CONFIG, MULTIEXEC_DEFAULT).trim();
		if (multiexec.equalsIgnoreCase("true") && !MULTI_EXEC_COMMANDS.contains(command)) {
			String supportedTypes = String.join(", ",
					MULTI_EXEC_COMMANDS.stream().map(Enum::name).toArray(String[]::new));
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

	private RedisType redisCommand(Map<String, String> props) {
		return RedisType.valueOf(props.getOrDefault(TYPE_CONFIG, TYPE_DEFAULT.name()));
	}

}
