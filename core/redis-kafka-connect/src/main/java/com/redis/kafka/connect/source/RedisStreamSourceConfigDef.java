package com.redis.kafka.connect.source;

/** Defines stream consumption and delivery options. */
public class RedisStreamSourceConfigDef extends RedisSourceConfigDef {

	/** Placeholder replaced with the originating Redis stream name. */
	public static final String TOKEN_STREAM = "${stream}";
	/** Placeholder replaced with the source task ID. */
	public static final String TOKEN_TASK = "${task}";

	/** Configuration key for {@code topic}. */
	public static final String TOPIC_CONFIG = "topic";
	/** Default value for {@link #TOPIC_CONFIG}. */
	public static final String TOPIC_DEFAULT = TOKEN_STREAM;
	/** User-facing description of {@link #TOPIC_CONFIG}. */
	public static final String TOPIC_DOC = String.format(
			"Name of the destination topic, which may contain '%s' as a placeholder for the originating stream name. For example `redis_%s` for the stream 'orders' will map to the topic name 'redis_orders'.",
			TOKEN_STREAM, TOKEN_STREAM);

	/** Configuration key for {@code redis.stream.name}. */
	public static final String STREAM_NAME_CONFIG = "redis.stream.name";
	/** User-facing description of {@link #STREAM_NAME_CONFIG}. */
	public static final String STREAM_NAME_DOC = "Name of the Redis stream to read from";

	/** Configuration key for {@code redis.stream.offset}. */
	public static final String STREAM_OFFSET_CONFIG = "redis.stream.offset";
	/** Default value for {@link #STREAM_OFFSET_CONFIG}. */
	public static final String STREAM_OFFSET_DEFAULT = "0-0";
	/** User-facing description of {@link #STREAM_OFFSET_CONFIG}. */
	public static final String STREAM_OFFSET_DOC = "Stream offset to start reading from";

	/** Configuration key for {@code redis.stream.claim.idle}. */
	public static final String STREAM_CLAIM_IDLE_CONFIG = "redis.stream.claim.idle";
    /** Default value for {@link #STREAM_CLAIM_IDLE_CONFIG}. */
    public static final long STREAM_CLAIM_IDLE_DEFAULT = 60000;
    /** User-facing description of {@link #STREAM_CLAIM_IDLE_CONFIG}. */
    public static final String STREAM_CLAIM_IDLE_DOC = "Minimum idle milliseconds before reclaiming pending messages from a consumer. Use 0 to disable. Must exceed normal end-to-end processing time.";

    /** Configuration key for {@code redis.stream.block}. */
    public static final String STREAM_BLOCK_CONFIG = "redis.stream.block";
	/** Default value for {@link #STREAM_BLOCK_CONFIG}. */
	public static final long STREAM_BLOCK_DEFAULT = 100;
	/** User-facing description of {@link #STREAM_BLOCK_CONFIG}. */
	public static final String STREAM_BLOCK_DOC = "The max amount of time in milliseconds to wait while polling for stream messages (XREAD [BLOCK milliseconds])";

	/** Configuration key for {@code redis.stream.delivery}. */
	public static final String STREAM_DELIVERY_CONFIG = "redis.stream.delivery";
	/** Default value for {@link #STREAM_DELIVERY_CONFIG}. */
	public static final String STREAM_DELIVERY_DEFAULT = RedisStreamSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE;
	/** User-facing description of {@link #STREAM_DELIVERY_CONFIG}. */
	public static final String STREAM_DELIVERY_DOC = "Stream message delivery guarantee, either 'at-least-once' or 'at-most-once'";

	/** Configuration key for {@code redis.stream.consumer.group}. */
	public static final String STREAM_CONSUMER_GROUP_CONFIG = "redis.stream.consumer.group";
	/** Default value for {@link #STREAM_CONSUMER_GROUP_CONFIG}. */
	public static final String STREAM_CONSUMER_GROUP_DEFAULT = "kafka-consumer-group";
	/** User-facing description of {@link #STREAM_CONSUMER_GROUP_CONFIG}. */
	public static final String STREAM_CONSUMER_GROUP_DOC = "Stream consumer group";

	/** Configuration key for {@code redis.stream.consumer.name}. */
	public static final String STREAM_CONSUMER_NAME_CONFIG = "redis.stream.consumer.name";
	/** Default value for {@link #STREAM_CONSUMER_NAME_CONFIG}. */
	public static final String STREAM_CONSUMER_NAME_DEFAULT = "consumer-" + TOKEN_TASK;
	/** User-facing description of {@link #STREAM_CONSUMER_NAME_CONFIG}. */
	public static final String STREAM_CONSUMER_NAME_DOC = "A format string for the stream consumer, which may contain '"
			+ TOKEN_TASK + "' as a placeholder for the task id.\nFor example, 'consumer-" + TOKEN_TASK
			+ "' for the task id '123' will map to the consumer name 'consumer-123'.";

	/** Creates the configuration definition. */
	public RedisStreamSourceConfigDef() {
		define();
	}

	/**
	 * Creates the configuration definition.
	 *
	 * @param base base configuration to copy
	 */
	public RedisStreamSourceConfigDef(RedisStreamSourceConfigDef base) {
		super(base);
		define();
	}

	private void define() {
		define(STREAM_CLAIM_IDLE_CONFIG, Type.LONG, STREAM_CLAIM_IDLE_DEFAULT, Range.atLeast(0), Importance.MEDIUM, STREAM_CLAIM_IDLE_DOC);
		define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.MEDIUM, TOPIC_DOC);
		define(STREAM_NAME_CONFIG, Type.STRING, Importance.HIGH, STREAM_NAME_DOC);
		define(STREAM_OFFSET_CONFIG, Type.STRING, STREAM_OFFSET_DEFAULT, Importance.MEDIUM, STREAM_OFFSET_DOC);
		define(STREAM_DELIVERY_CONFIG, Type.STRING, STREAM_DELIVERY_DEFAULT, ValidString.in(RedisStreamSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE, RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE), Importance.MEDIUM, STREAM_DELIVERY_DOC);
		define(STREAM_CONSUMER_GROUP_CONFIG, Type.STRING, STREAM_CONSUMER_GROUP_DEFAULT, Importance.MEDIUM,
				STREAM_CONSUMER_GROUP_DOC);
		define(STREAM_CONSUMER_NAME_CONFIG, Type.STRING, STREAM_CONSUMER_NAME_DEFAULT, Importance.MEDIUM,
				STREAM_CONSUMER_NAME_DOC);
		define(STREAM_BLOCK_CONFIG, Type.LONG, STREAM_BLOCK_DEFAULT, Range.atLeast(0), Importance.LOW, STREAM_BLOCK_DOC);
	}

}
