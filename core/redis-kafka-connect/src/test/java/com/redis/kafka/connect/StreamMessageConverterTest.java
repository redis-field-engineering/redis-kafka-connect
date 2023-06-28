package com.redis.kafka.connect;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.redis.kafka.connect.source.RedisStreamSourceConfig;
import com.redis.kafka.connect.source.StreamMessageConverter;

import io.lettuce.core.StreamMessage;

class StreamMessageConverterTest {

	public static final String OFFSET_FIELD = "offset";
	public static final String FIELD_ID = "id";
	public static final String FIELD_BODY = "body";
	public static final String FIELD_STREAM = "stream";
	private static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.stream.Value";
	private static final long NOW = System.currentTimeMillis();
	private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneId.systemDefault());

	// This is published, so if it's changed, it may impact users.
	private static final Schema PUBLISHED_SCHEMA = SchemaBuilder.struct().field(FIELD_ID, Schema.STRING_SCHEMA)
			.field(FIELD_BODY, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
			.field(FIELD_STREAM, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME).build();

	@ParameterizedTest
	@ArgumentsSource(ConvertArgs.class)
	void testConvertStreamMessageOfStringString(ConvertArgs args) {
		RedisStreamSourceConfig config = new RedisStreamSourceConfig(args.props);
		StreamMessageConverter r = new StreamMessageConverter(CLOCK, config);
		SourceRecord got = r.apply(args.message);
		assertThat(got, equalTo(args.want));
	}

	static class ConvertArgs implements ArgumentsProvider, Arguments {
		@Override
		public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
			return Stream.of(
					new ConvertArgs(mapOf("redis.stream.name", "stream1"),
							new StreamMessage<>("stream1", "1-0", mapOf()),
							new SourceRecord(mapOf(), mapOf("offset", "1-0"), "stream1", null, Schema.STRING_SCHEMA,
									"1-0", PUBLISHED_SCHEMA,
									new Struct(PUBLISHED_SCHEMA).put(FIELD_ID, "1-0").put(FIELD_STREAM, "stream1")
											.put(FIELD_BODY, mapOf()),
									NOW)),

					new ConvertArgs(mapOf("redis.stream.name", "stream2"),
							new StreamMessage<>("stream2", "2-0", mapOf("key2", "value2")),
							new SourceRecord(mapOf(), mapOf("offset", "2-0"), "stream2", null, Schema.STRING_SCHEMA,
									"2-0", PUBLISHED_SCHEMA,
									new Struct(PUBLISHED_SCHEMA).put(FIELD_ID, "2-0").put(FIELD_STREAM, "stream2")
											.put(FIELD_BODY, mapOf("key2", "value2")),
									NOW)),

					new ConvertArgs(mapOf("redis.stream.name", "stream3", "topic", "topic3"),
							new StreamMessage<>("stream3", "3-0", mapOf("key3", "value3")),
							new SourceRecord(mapOf(), mapOf("offset", "3-0"), "topic3", null, Schema.STRING_SCHEMA,
									"3-0", PUBLISHED_SCHEMA, new Struct(PUBLISHED_SCHEMA).put(FIELD_ID, "3-0")
											.put(FIELD_STREAM, "stream3").put(FIELD_BODY, mapOf("key3", "value3")),
									NOW)));
		}

		Map<String, String> props;
		StreamMessage<String, String> message;
		SourceRecord want;

		ConvertArgs() {
		}

		ConvertArgs(Map<String, String> props, StreamMessage<String, String> message, SourceRecord want) {
			this.props = props;
			this.message = message;
			this.want = want;
		}

		@Override
		public Object[] get() {
			return new Object[] { this };
		}
	}

	static Map<String, String> mapOf(String... args) {
		HashMap<String, String> ret = new HashMap<>();
		int i = 0;
		for (; i < args.length; i += 2) {
			ret.put(args[i], args[i + 1]);
		}
		if (i != args.length) {
			throw new IllegalArgumentException("Expects an even number of arguments");
		}
		return ret;
	}

}
