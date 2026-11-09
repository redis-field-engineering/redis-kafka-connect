package com.redis.kafka.connect.source;

import io.lettuce.core.StreamMessage;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Converts Redis stream messages to Kafka Connect source records. */
public class StreamMessageConverter implements Function<StreamMessage<String, String>, SourceRecord> {

	/** Schema field name for id. */
	public static final String FIELD_ID = "id";
	/** Schema field name for body. */
	public static final String FIELD_BODY = "body";
	/** Schema field name for stream. */
	public static final String FIELD_STREAM = "stream";

	private static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.stream.Value";
	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	/** Kafka Connect schema for stream message records. */
	public static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD_ID, Schema.STRING_SCHEMA)
			.field(FIELD_BODY, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
			.field(FIELD_STREAM, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME).build();

	private final Clock clock;
	private final RedisStreamSourceConfig config;

	/**
	 * Creates a stream record converter.
	 *
	 * @param clock clock used to timestamp records
	 * @param config stream identity, delivery, and topic settings
	 */
	public StreamMessageConverter(Clock clock, RedisStreamSourceConfig config) {
		this.clock = clock;
		this.config = config;
	}

	@Override
	public SourceRecord apply(StreamMessage<String, String> message) {
		Map<String, ?> partition = config.sourcePartition();
		Map<String, ?> offset = Collections.singletonMap(RedisStreamSourceTask.OFFSET_FIELD, message.getId());
		String topic = config.getTopicName().replace(RedisStreamSourceConfigDef.TOKEN_STREAM, message.getStream());
		String key = message.getId();
		Struct value = struct(message);
		long epoch = clock.instant().toEpochMilli();
		return new SourceRecord(partition, offset, topic, null, KEY_SCHEMA, key, VALUE_SCHEMA, value, epoch);
	}

	/**
	 * Converts a stream message to the published value schema.
	 *
	 * @param message Redis stream message
	 * @return message ID, body, and stream as a struct
	 */
	public static Struct struct(StreamMessage<String, String> message) {
		return new Struct(VALUE_SCHEMA).put(FIELD_ID, message.getId()).put(FIELD_BODY, message.getBody())
				.put(FIELD_STREAM, message.getStream());
	}

}
