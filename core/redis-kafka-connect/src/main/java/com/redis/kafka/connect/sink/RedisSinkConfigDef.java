package com.redis.kafka.connect.sink;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.sink.RedisSinkConfig.RedisType;

/** Defines Redis sink options and validates their combinations. */
public class RedisSinkConfigDef extends RedisConfigDef {

	/** Placeholder replaced with the originating Kafka topic name. */
	public static final String TOKEN_TOPIC = "${topic}";

	/** Configuration key for {@code redis.charset}. */
	public static final String CHARSET_CONFIG = "redis.charset";

	/** Default value for {@link #CHARSET_CONFIG}. */
	public static final String CHARSET_DEFAULT = Charset.defaultCharset().name();

	/** User-facing description of {@link #CHARSET_CONFIG}. */
	public static final String CHARSET_DOC = "Character set to encode Redis key and value strings.";

	/** Configuration key for {@code redis.keyspace}. */
	public static final String KEYSPACE_CONFIG = "redis.keyspace";

	/** Default value for {@link #KEYSPACE_CONFIG}. */
	public static final String KEYSPACE_DEFAULT = TOKEN_TOPIC;

	/** User-facing description of {@link #KEYSPACE_CONFIG}. */
	public static final String KEYSPACE_DOC = "A format string for destination key space, which may contain '"
			+ TOKEN_TOPIC + "' as a placeholder for the originating topic name.\nFor example, ``kafka_" + TOKEN_TOPIC
			+ "`` for the topic 'orders' will map to the Redis key space " + "'kafka_orders'.";

	/** Configuration key for {@code redis.separator}. */
	public static final String SEPARATOR_CONFIG = "redis.separator";

	/** Default value for {@link #SEPARATOR_CONFIG}. */
	public static final String SEPARATOR_DEFAULT = ":";

	/** User-facing description of {@link #SEPARATOR_CONFIG}. */
	public static final String SEPARATOR_DOC = "Separator for non-collection destination keys.";

	/** Configuration key for {@code redis.multiexec}. */
	public static final String MULTIEXEC_CONFIG = "redis.multiexec";

	/** Default value for {@link #MULTIEXEC_CONFIG}. */
	public static final String MULTIEXEC_DEFAULT = "false";

	/** User-facing description of {@link #MULTIEXEC_CONFIG}. */
	public static final String MULTIEXEC_DOC = "Whether to execute Redis commands in multi/exec transactions. Requires redis.cluster=false.";

	/** Configuration key for {@code redis.wait.replicas}. */
	public static final String WAIT_REPLICAS_CONFIG = "redis.wait.replicas";

	/** Default value for {@link #WAIT_REPLICAS_CONFIG}. */
	public static final String WAIT_REPLICAS_DEFAULT = "0";

	/** User-facing description of {@link #WAIT_REPLICAS_CONFIG}. */
	public static final String WAIT_REPLICAS_DOC = "Number of replicas to wait for. Use 0 to disable waiting for replicas. Requires redis.cluster=false.";

	/** Configuration key for {@code redis.wait.timeout}. */
	public static final String WAIT_TIMEOUT_CONFIG = "redis.wait.timeout";

	/** Default value for {@link #WAIT_TIMEOUT_CONFIG}. */
	public static final String WAIT_TIMEOUT_DEFAULT = "1000";

	/** User-facing description of {@link #WAIT_TIMEOUT_CONFIG}. */
	public static final String WAIT_TIMEOUT_DOC = "Timeout in millis for WAIT command.";

	/** Configuration key for {@code redis.type}. */
	public static final String TYPE_CONFIG = "redis.type";

	/** Default value for {@link #TYPE_CONFIG}. */
	public static final RedisType TYPE_DEFAULT = RedisType.STREAM;

	/** User-facing description of {@link #TYPE_CONFIG}. */
	public static final String TYPE_DOC = "Destination data structure: "
			+ String.join(",", Stream.of(RedisType.values()).map(RedisType::name).toArray(String[]::new));

	/** Configuration key for {@code redis.key.ttl}. */
	public static final String KEY_TTL_CONFIG = "redis.key.ttl";

	/** Default value for {@link #KEY_TTL_CONFIG}. */
	public static final String KEY_TTL_CONFIG_DEFAULT = "-1";

	/** User-facing description of {@link #KEY_TTL_CONFIG}. */
	public static final String KEY_TTL_CONFIG_DOC = "Time to live in seconds for the key. If not set, the record will not expire.";

	/** Configuration key for {@code redis.offset.namespace}. */
	public static final String OFFSET_NAMESPACE_CONFIG = "redis.offset.namespace";

	/** Default value for {@link #OFFSET_NAMESPACE_CONFIG}. */
	public static final String OFFSET_NAMESPACE_DEFAULT = "";

	/** User-facing description of {@link #OFFSET_NAMESPACE_CONFIG}. */
	public static final String OFFSET_NAMESPACE_DOC = "Namespace for sink offset keys stored in Redis. If empty, the connector name is used.";

	/** Configuration key for {@code redis.hashfield.hash}. */
	public static final String HASH_FIELD_HASH_CONFIG = "redis.hashfield.hash";

	/** Default value for {@link #HASH_FIELD_HASH_CONFIG}. */
	public static final String HASH_FIELD_HASH_DEFAULT = "";

	/** User-facing description of {@link #HASH_FIELD_HASH_CONFIG}. */
	public static final String HASH_FIELD_HASH_DOC = "Field of the record key that names the destination hash when "
			+ TYPE_CONFIG + "=HASH_FIELD, as key.<field> or key.<field>.<nested>, for example key.parentId. The hash is "
			+ "named <" + KEYSPACE_CONFIG + "><" + SEPARATOR_CONFIG + "><value>. " + KEYSPACE_CONFIG + " defaults to "
			+ TOKEN_TOPIC + "; leave it empty to use the value alone.";

	/** Configuration key for {@code redis.hashfield.field}. */
	public static final String HASH_FIELD_FIELD_CONFIG = "redis.hashfield.field";

	/** Default value for {@link #HASH_FIELD_FIELD_CONFIG}. */
	public static final String HASH_FIELD_FIELD_DEFAULT = "";

	/** User-facing description of {@link #HASH_FIELD_FIELD_CONFIG}. */
	public static final String HASH_FIELD_FIELD_DOC = "Field of the record key that names the field within the hash when "
			+ TYPE_CONFIG + "=HASH_FIELD, as key.<field> or key.<field>.<nested>, for example key.childId. The value is "
			+ "used as is, with no prefix.";

	/** Configuration key for {@code redis.hashfield.hexpire.ttl}, also the name of the per-record header. */
	public static final String HASH_FIELD_EXPIRE_TTL_CONFIG = "redis.hashfield.hexpire.ttl";

	/** Configuration key for {@code redis.hashfield.hexpire.enabled}. */
	public static final String HASH_FIELD_EXPIRE_ENABLED_CONFIG = "redis.hashfield.hexpire.enabled";

	/** Default value for {@link #HASH_FIELD_EXPIRE_ENABLED_CONFIG}. */
	public static final String HASH_FIELD_EXPIRE_ENABLED_DEFAULT = "false";

	/** User-facing description of {@link #HASH_FIELD_EXPIRE_ENABLED_CONFIG}. */
	public static final String HASH_FIELD_EXPIRE_ENABLED_DOC = "With " + TYPE_CONFIG + "=HASH_FIELD, must be true to "
			+ "expire hash fields with HEXPIRE, through either " + HASH_FIELD_EXPIRE_TTL_CONFIG + " or its record header. "
			+ "The task checks at startup and fails if the server does not support HEXPIRE. "
			+ "Headers are ignored while false.";

	/** Largest accepted field TTL in seconds, well below the largest expire time Redis accepts. */
	public static final long HASH_FIELD_EXPIRE_TTL_MAX = Long.MAX_VALUE / 1000 / 2;

	/** Default value for {@link #HASH_FIELD_EXPIRE_TTL_CONFIG}. */
	public static final String HASH_FIELD_EXPIRE_TTL_DEFAULT = "-1";

	/** User-facing description of {@link #HASH_FIELD_EXPIRE_TTL_CONFIG}. */
	public static final String HASH_FIELD_EXPIRE_TTL_DOC = "Time to live in seconds for each record's hash field when "
			+ TYPE_CONFIG + "=HASH_FIELD. A record header with this name overrides it. Requires "
			+ HASH_FIELD_EXPIRE_ENABLED_CONFIG + "=true. A value of 0 or below means no field expiry. Separate from "
			+ KEY_TTL_CONFIG + ", which expires the whole hash.";

	/** Destination types supported by sink MULTI/EXEC transactions. */
	protected static final Set<RedisType> MULTI_EXEC_COMMANDS = Stream
			.of(RedisType.STREAM, RedisType.LIST, RedisType.SET, RedisType.ZSET).collect(Collectors.toSet());

	/** Creates the configuration definition. */
	public RedisSinkConfigDef() {
		define();
	}

	/**
	 * Creates the configuration definition.
	 *
	 * @param base base configuration to copy
	 */
	public RedisSinkConfigDef(ConfigDef base) {
		super(base);
		define();
	}

	private void define() {
		define(CHARSET_CONFIG, Type.STRING, CHARSET_DEFAULT, Importance.HIGH, CHARSET_DOC);
		define(TYPE_CONFIG, Type.STRING, TYPE_DEFAULT.name(), ValidString.in(Stream.of(RedisType.values()).map(Enum::name).toArray(String[]::new)), Importance.HIGH, TYPE_DOC);
		define(KEYSPACE_CONFIG, Type.STRING, KEYSPACE_DEFAULT, Importance.MEDIUM, KEYSPACE_DOC);
		define(SEPARATOR_CONFIG, Type.STRING, SEPARATOR_DEFAULT, Importance.MEDIUM, SEPARATOR_DOC);
		define(MULTIEXEC_CONFIG, Type.BOOLEAN, MULTIEXEC_DEFAULT, Importance.MEDIUM, MULTIEXEC_DOC);
		define(WAIT_REPLICAS_CONFIG, Type.INT, WAIT_REPLICAS_DEFAULT, Range.atLeast(0), Importance.MEDIUM, WAIT_REPLICAS_DOC);
		define(WAIT_TIMEOUT_CONFIG, Type.LONG, WAIT_TIMEOUT_DEFAULT, Range.atLeast(0), Importance.MEDIUM, WAIT_TIMEOUT_DOC);
		define(KEY_TTL_CONFIG, Type.LONG, KEY_TTL_CONFIG_DEFAULT, Range.atLeast(-1), Importance.MEDIUM, KEY_TTL_CONFIG_DOC);
		define(OFFSET_NAMESPACE_CONFIG, Type.STRING, OFFSET_NAMESPACE_DEFAULT, Importance.MEDIUM, OFFSET_NAMESPACE_DOC);
		define(HASH_FIELD_HASH_CONFIG, Type.STRING, HASH_FIELD_HASH_DEFAULT, Importance.MEDIUM, HASH_FIELD_HASH_DOC);
		define(HASH_FIELD_FIELD_CONFIG, Type.STRING, HASH_FIELD_FIELD_DEFAULT, Importance.MEDIUM, HASH_FIELD_FIELD_DOC);
		define(HASH_FIELD_EXPIRE_ENABLED_CONFIG, Type.BOOLEAN, HASH_FIELD_EXPIRE_ENABLED_DEFAULT, Importance.MEDIUM,
				HASH_FIELD_EXPIRE_ENABLED_DOC);
		define(HASH_FIELD_EXPIRE_TTL_CONFIG, Type.LONG, HASH_FIELD_EXPIRE_TTL_DEFAULT,
				Range.between(-1, HASH_FIELD_EXPIRE_TTL_MAX),
				Importance.MEDIUM, HASH_FIELD_EXPIRE_TTL_DOC);
	}

	/**
	 * Checks the HASH_FIELD key references: both are required and must be {@code key.<field>[.<field>...]}
	 * when the type is HASH_FIELD, and must be empty otherwise.
	 *
	 * @return error message by property name, in property order
	 */
	static Map<String, String> hashFieldErrors(RedisType type, String hashPath, String fieldPath) {
		Map<String, String> errors = new LinkedHashMap<>();
		hashFieldError(type, HASH_FIELD_HASH_CONFIG, hashPath).ifPresent(message -> errors.put(HASH_FIELD_HASH_CONFIG, message));
		hashFieldError(type, HASH_FIELD_FIELD_CONFIG, fieldPath).ifPresent(message -> errors.put(HASH_FIELD_FIELD_CONFIG, message));
		return errors;
	}

	private static Optional<String> hashFieldError(RedisType type, String name, String reference) {
		String value = reference == null ? "" : reference.trim();
		if (type != RedisType.HASH_FIELD) {
			return value.isEmpty() ? Optional.empty()
					: Optional.of(name + " is only supported when " + TYPE_CONFIG + "=HASH_FIELD");
		}
		if (value.isEmpty()) {
			return Optional.of(name + " is required when " + TYPE_CONFIG + "=HASH_FIELD");
		}
		try {
			KeyPath.parse(value);
			return Optional.empty();
		} catch (IllegalArgumentException e) {
			return Optional.of(e.getMessage());
		}
	}

	/**
	 * Checks the hash field expiry options: both are only supported when the type is HASH_FIELD, and a field TTL
	 * requires expiry to be enabled.
	 *
	 * @return error message by property name, in property order
	 */
	static Map<String, String> hashFieldExpireErrors(RedisType type, boolean enabled, long ttl) {
		Map<String, String> errors = new LinkedHashMap<>();
		if (type != RedisType.HASH_FIELD) {
			if (enabled) {
				errors.put(HASH_FIELD_EXPIRE_ENABLED_CONFIG,
						HASH_FIELD_EXPIRE_ENABLED_CONFIG + " is only supported when " + TYPE_CONFIG + "=HASH_FIELD");
			}
			if (ttl > 0) {
				errors.put(HASH_FIELD_EXPIRE_TTL_CONFIG,
						HASH_FIELD_EXPIRE_TTL_CONFIG + " is only supported when " + TYPE_CONFIG + "=HASH_FIELD");
			}
		} else if (ttl > 0 && !enabled) {
			errors.put(HASH_FIELD_EXPIRE_TTL_CONFIG,
					HASH_FIELD_EXPIRE_TTL_CONFIG + " requires " + HASH_FIELD_EXPIRE_ENABLED_CONFIG + "=true");
		}
		return errors;
	}

	@Override
	public Map<String, ConfigValue> validateAll(Map<String, String> props) {
		Map<String, ConfigValue> results = super.validateAll(props);
		if (results.values().stream().map(ConfigValue::errorMessages).anyMatch(l -> !l.isEmpty())) {
			return results;
		}
		if (Boolean.TRUE.equals(results.get(CLUSTER_CONFIG).value())) {
			if (Boolean.TRUE.equals(results.get(MULTIEXEC_CONFIG).value())) {
				results.get(MULTIEXEC_CONFIG).addErrorMessage("Transactions require redis.cluster=false");
			}
			if ((Integer) results.get(WAIT_REPLICAS_CONFIG).value() > 0) {
				results.get(WAIT_REPLICAS_CONFIG).addErrorMessage("WAIT requires redis.cluster=false");
			}
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
		hashFieldErrors(command, props.get(HASH_FIELD_HASH_CONFIG), props.get(HASH_FIELD_FIELD_CONFIG))
				.forEach((name, message) -> results.get(name).addErrorMessage(message));
		hashFieldExpireErrors(command, (Boolean) results.get(HASH_FIELD_EXPIRE_ENABLED_CONFIG).value(),
				(Long) results.get(HASH_FIELD_EXPIRE_TTL_CONFIG).value())
				.forEach((name, message) -> results.get(name).addErrorMessage(message));
		return results;
	}

	private RedisType redisCommand(Map<String, String> props) {
		return RedisType.valueOf(props.getOrDefault(TYPE_CONFIG, TYPE_DEFAULT.name()));
	}

}
