package com.redis.kafka.connect.source;

public class RedisKeysSourceConfigDef extends RedisSourceConfigDef {

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_DEFAULT = "rediskeys";
	public static final String TOPIC_DOC = "Name of the destination topic";

	public static final String KEY_PATTERN_CONFIG = "redis.keys.pattern";
	public static final String KEY_PATTERN_DEFAULT = "*";
	public static final String KEY_PATTERN_DOC = "Keyspace glob-style pattern to subscribe to.";

	public static final String IDLE_TIMEOUT_CONFIG = "redis.keys.timeout";
	public static final String IDLE_TIMEOUT_DEFAULT = "0";
	public static final String IDLE_TIMEOUT_DOC = "Idle timeout in millis. Use 0 to disable.";

	public RedisKeysSourceConfigDef() {
		define();
	}

	public RedisKeysSourceConfigDef(RedisSourceConfigDef base) {
		super(base);
		define();
	}

	private void define() {
		define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.MEDIUM, TOPIC_DOC);
		define(KEY_PATTERN_CONFIG, Type.STRING, KEY_PATTERN_DEFAULT, Importance.MEDIUM, KEY_PATTERN_DOC);
		define(IDLE_TIMEOUT_CONFIG, Type.LONG, IDLE_TIMEOUT_DEFAULT, Importance.LOW, IDLE_TIMEOUT_DOC);
	}

}