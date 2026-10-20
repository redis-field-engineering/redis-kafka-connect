package com.redis.kafka.connect.source;

import java.util.Locale;
import org.apache.kafka.common.config.ConfigException;

public class RedisKeysSourceConfigDef extends RedisSourceConfigDef {

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_DEFAULT = "rediskeys";
	public static final String TOPIC_DOC = "Name of the destination topic";

	public static final String KEY_PATTERN_CONFIG = "redis.keys.pattern";
	public static final String KEY_PATTERN_DEFAULT = "*";
	public static final String KEY_PATTERN_DOC = "Keyspace glob-style pattern to subscribe to.";

	public static final String IDLE_TIMEOUT_CONFIG = "redis.keys.timeout";
	public static final String IDLE_TIMEOUT_DEFAULT = "0";
	public static final String IDLE_TIMEOUT_DOC = "Maximum idle wait per poll in milliseconds. Use 0 for the default bounded polling interval.";

	public static final String QUEUE_CAPACITY_CONFIG = "redis.keys.queue.capacity";
    public static final int QUEUE_CAPACITY_DEFAULT = 10000;
    public static final String QUEUE_CAPACITY_DOC = "Maximum queued key notifications. Overflow fails the task rather than silently dropping changes.";

    public static final String MODE_CONFIG = "mode";
	public static final RedisKeysSourceConfig.ReaderMode MODE_DEFAULT = RedisKeysSourceConfig.ReaderMode.LIVE;
	public static final String MODE_DOC = "Key reading mode. Use LIVE for snapshot + updates, LIVEONLY for just updates.";

	public RedisKeysSourceConfigDef() {
		define();
	}

	public RedisKeysSourceConfigDef(RedisSourceConfigDef base) {
		super(base);
		define();
	}

	private void define() {
		define(QUEUE_CAPACITY_CONFIG, Type.INT, QUEUE_CAPACITY_DEFAULT, Range.atLeast(1), Importance.MEDIUM, QUEUE_CAPACITY_DOC);
		define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.MEDIUM, TOPIC_DOC);
		define(KEY_PATTERN_CONFIG, Type.STRING, KEY_PATTERN_DEFAULT, Importance.MEDIUM, KEY_PATTERN_DOC);
		define(IDLE_TIMEOUT_CONFIG, Type.LONG, IDLE_TIMEOUT_DEFAULT, Range.atLeast(0), Importance.LOW, IDLE_TIMEOUT_DOC);
		define(MODE_CONFIG, Type.STRING, MODE_DEFAULT.name(), new Validator() {
            @Override
            public void ensureValid(String name, Object value) {
                try {
                    RedisKeysSourceConfig.ReaderMode.valueOf(value.toString().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new ConfigException(name, value, "Expected LIVE or LIVEONLY");
                }
            }
        }, Importance.MEDIUM, MODE_DOC);
	}

}
