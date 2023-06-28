package com.redis.kafka.connect.source;

public class RedisStreamSourceConfigDef extends RedisSourceConfigDef {

	public static final String TOKEN_STREAM = "${stream}";
	public static final String TOKEN_TASK = "${task}";

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_DEFAULT = TOKEN_STREAM;
	public static final String TOPIC_DOC = String.format(
			"Name of the destination topic, which may contain '%s' as a placeholder for the originating stream name. For example `redis_%s` for the stream 'orders' will map to the topic name 'redis_orders'.",
			TOKEN_STREAM, TOKEN_STREAM);

	public static final String STREAM_NAME_CONFIG = "redis.stream.name";
	public static final String STREAM_NAME_DOC = "Name of the Redis stream to read from";

	public static final String STREAM_OFFSET_CONFIG = "redis.stream.offset";
	public static final String STREAM_OFFSET_DEFAULT = "0-0";
	public static final String STREAM_OFFSET_DOC = "Stream offset to start reading from";

	public static final String STREAM_BLOCK_CONFIG = "redis.stream.block";
	public static final long STREAM_BLOCK_DEFAULT = 100;
	public static final String STREAM_BLOCK_DOC = "The max amount of time in milliseconds to wait while polling for stream messages (XREAD [BLOCK milliseconds])";

	public static final String STREAM_DELIVERY_CONFIG = "redis.stream.delivery";
	public static final String STREAM_DELIVERY_DEFAULT = RedisStreamSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE;
	public static final String STREAM_DELIVERY_DOC = "Stream message delivery guarantee, either 'at-least-once' or 'at-most-once'";

	public static final String STREAM_CONSUMER_GROUP_CONFIG = "redis.stream.consumer.group";
	public static final String STREAM_CONSUMER_GROUP_DEFAULT = "kafka-consumer-group";
	public static final String STREAM_CONSUMER_GROUP_DOC = "Stream consumer group";

	public static final String STREAM_CONSUMER_NAME_CONFIG = "redis.stream.consumer.name";
	public static final String STREAM_CONSUMER_NAME_DEFAULT = "consumer-" + TOKEN_TASK;
	public static final String STREAM_CONSUMER_NAME_DOC = "A format string for the stream consumer, which may contain '"
			+ TOKEN_TASK + "' as a placeholder for the task id.\nFor example, 'consumer-" + TOKEN_TASK
			+ "' for the task id '123' will map to the consumer name 'consumer-123'.";

	public RedisStreamSourceConfigDef() {
		define();
	}

	public RedisStreamSourceConfigDef(RedisStreamSourceConfigDef base) {
		super(base);
		define();
	}

	private void define() {
		define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.MEDIUM, TOPIC_DOC);
		define(STREAM_NAME_CONFIG, Type.STRING, Importance.HIGH, STREAM_NAME_DOC);
		define(STREAM_OFFSET_CONFIG, Type.STRING, STREAM_OFFSET_DEFAULT, Importance.MEDIUM, STREAM_OFFSET_DOC);
		define(STREAM_DELIVERY_CONFIG, Type.STRING, STREAM_DELIVERY_DEFAULT, Importance.MEDIUM, STREAM_DELIVERY_DOC);
		define(STREAM_CONSUMER_GROUP_CONFIG, Type.STRING, STREAM_CONSUMER_GROUP_DEFAULT, Importance.MEDIUM,
				STREAM_CONSUMER_GROUP_DOC);
		define(STREAM_CONSUMER_NAME_CONFIG, Type.STRING, STREAM_CONSUMER_NAME_DEFAULT, Importance.MEDIUM,
				STREAM_CONSUMER_NAME_DOC);
		define(STREAM_BLOCK_CONFIG, Type.LONG, STREAM_BLOCK_DEFAULT, Importance.LOW, STREAM_BLOCK_DOC);
	}

}