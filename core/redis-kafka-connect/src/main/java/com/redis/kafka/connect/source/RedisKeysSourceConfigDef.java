package com.redis.kafka.connect.source;

import java.util.Locale;
import org.apache.kafka.common.config.ConfigException;

/** Defines key snapshot and keyspace notification options. */
public class RedisKeysSourceConfigDef extends RedisSourceConfigDef {

	/** Configuration key for {@code topic}. */
	public static final String TOPIC_CONFIG = "topic";
	/** Default value for {@link #TOPIC_CONFIG}. */
	public static final String TOPIC_DEFAULT = "rediskeys";
	/** User-facing description of {@link #TOPIC_CONFIG}. */
	public static final String TOPIC_DOC = "Name of the destination topic";

	/** Configuration key for {@code redis.keys.pattern}. */
	public static final String KEY_PATTERN_CONFIG = "redis.keys.pattern";
	/** Default value for {@link #KEY_PATTERN_CONFIG}. */
	public static final String KEY_PATTERN_DEFAULT = "*";
	/** User-facing description of {@link #KEY_PATTERN_CONFIG}. */
	public static final String KEY_PATTERN_DOC = "Keyspace glob-style pattern to subscribe to.";

	/** Configuration key for {@code redis.keys.timeout}. */
	public static final String IDLE_TIMEOUT_CONFIG = "redis.keys.timeout";
	/** Default value for {@link #IDLE_TIMEOUT_CONFIG}. */
	public static final String IDLE_TIMEOUT_DEFAULT = "0";
	/** User-facing description of {@link #IDLE_TIMEOUT_CONFIG}. */
	public static final String IDLE_TIMEOUT_DOC = "Maximum idle wait per poll in milliseconds. Use 0 for the default bounded polling interval.";

	/** Configuration key for {@code redis.keys.queue.capacity}. */
	public static final String QUEUE_CAPACITY_CONFIG = "redis.keys.queue.capacity";
    /** Default value for {@link #QUEUE_CAPACITY_CONFIG}. */
    public static final int QUEUE_CAPACITY_DEFAULT = 10000;
    /** User-facing description of {@link #QUEUE_CAPACITY_CONFIG}. */
    public static final String QUEUE_CAPACITY_DOC = "Maximum queued key notifications. Overflow fails the task rather than silently dropping changes.";

    /** Configuration key for {@code mode}. */
    public static final String MODE_CONFIG = "mode";
	/** Default value for {@link #MODE_CONFIG}. */
	public static final RedisKeysSourceConfig.ReaderMode MODE_DEFAULT = RedisKeysSourceConfig.ReaderMode.LIVE;
	/** User-facing description of {@link #MODE_CONFIG}. */
	public static final String MODE_DOC = "Key reading mode. Use LIVE for snapshot + updates, LIVEONLY for just updates.";

	/** Creates the configuration definition. */
	public RedisKeysSourceConfigDef() {
		define();
	}

	/**
	 * Creates the configuration definition.
	 *
	 * @param base base configuration to copy
	 */
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
