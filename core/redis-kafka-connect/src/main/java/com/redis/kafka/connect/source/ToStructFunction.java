package com.redis.kafka.connect.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;

public class ToStructFunction implements Function<KeyValue<String>, Struct> {

	public static final String FIELD_KEY = "key";

	public static final String FIELD_TTL = "ttl";

	public static final String FIELD_TYPE = "type";

	public static final String FIELD_VALUE = "value";

	public static final String FIELD_HASH = "hash";

	public static final String FIELD_JSON = "json";

	public static final String FIELD_LIST = "list";

	public static final String FIELD_SET = "set";

	public static final String FIELD_STRING = "string";

	public static final String FIELD_ZSET = "zset";

	public static final Schema TTL_SCHEMA = Schema.OPTIONAL_INT64_SCHEMA;

	public static final Schema TYPE_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;

	public static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.keys.Value";

	public static final Schema HASH_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional();

	public static final Schema LIST_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional();

	public static final Schema SET_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional();

	public static final Schema STRING_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;

	public static final Schema ZSET_SCHEMA = SchemaBuilder.map(Schema.FLOAT64_SCHEMA, Schema.STRING_SCHEMA).optional();

	public static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD_KEY, Schema.STRING_SCHEMA)
			.field(FIELD_TTL, TTL_SCHEMA).field(FIELD_TYPE, TYPE_SCHEMA).field(FIELD_HASH, HASH_SCHEMA)
			.field(FIELD_STRING, STRING_SCHEMA).field(FIELD_JSON, STRING_SCHEMA).field(FIELD_LIST, LIST_SCHEMA)
			.field(FIELD_SET, SET_SCHEMA).field(FIELD_ZSET, ZSET_SCHEMA).name(VALUE_SCHEMA_NAME).build();

	@Override
	public Struct apply(KeyValue<String> input) {
		Struct struct = new Struct(VALUE_SCHEMA);
		struct.put(FIELD_KEY, input.getKey());
		struct.put(FIELD_TTL, input.getTtl());
		struct.put(FIELD_TYPE, input.getType());
		if (input.getType() != null) {
			switch (input.getType()) {
			case KeyValue.TYPE_HASH:
				struct.put(FIELD_HASH, input.getValue());
				break;
			case KeyValue.TYPE_JSON:
				struct.put(FIELD_JSON, input.getValue());
				break;
			case KeyValue.TYPE_LIST:
				struct.put(FIELD_LIST, input.getValue());
				break;
			case KeyValue.TYPE_SET:
				struct.put(FIELD_SET, list(input));
				break;
			case KeyValue.TYPE_STRING:
				struct.put(FIELD_STRING, input.getValue());
				break;
			case KeyValue.TYPE_ZSET:
				struct.put(FIELD_ZSET, zsetMap(input));
				break;
			default:
				break;
			}
		}
		return struct;
	}

	@SuppressWarnings("unchecked")
	private Object list(KeyValue<String> input) {
		return new ArrayList<>((Collection<String>) input.getValue());
	}

	@SuppressWarnings("unchecked")
	public static Map<Double, String> zsetMap(KeyValue<String> input) {
		Collection<ScoredValue<String>> value = (Collection<ScoredValue<String>>) input.getValue();
		return zsetMap(value);
	}

	public static Map<Double, String> zsetMap(Collection<ScoredValue<String>> value) {
		return value.stream().collect(Collectors.toMap(ScoredValue::getScore, ScoredValue::getValue));
	}

	public static Map<Long, Double> timeseriesMap(Collection<Sample> samples) {
		return samples.stream().collect(Collectors.toMap(Sample::getTimestamp, Sample::getValue));
	}

}
