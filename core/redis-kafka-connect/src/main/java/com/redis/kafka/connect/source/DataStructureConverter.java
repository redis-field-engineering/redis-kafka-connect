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
import com.redis.spring.batch.common.DataStructure;

import io.lettuce.core.ScoredValue;

public class DataStructureConverter implements Function<DataStructure<String>, Struct> {

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
	public static final Schema TYPE_SCHEMA = Schema.STRING_SCHEMA;
	public static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.keys.Value";
	public static final Schema HASH_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional();
	public static final Schema LIST_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional();
	public static final Schema SET_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional();
	public static final Schema STREAM_SCHEMA = SchemaBuilder.array(StreamMessageConverter.VALUE_SCHEMA).optional();
	public static final Schema STRING_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;
	public static final Schema ZSET_SCHEMA = SchemaBuilder.map(Schema.FLOAT64_SCHEMA, Schema.STRING_SCHEMA).optional();
	public static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD_KEY, Schema.STRING_SCHEMA)
			.field(FIELD_TTL, TTL_SCHEMA).field(FIELD_TYPE, TYPE_SCHEMA).field(FIELD_HASH, HASH_SCHEMA)
			.field(FIELD_STRING, STRING_SCHEMA).field(FIELD_JSON, STRING_SCHEMA).field(FIELD_LIST, LIST_SCHEMA)
			.field(FIELD_SET, SET_SCHEMA).field(FIELD_ZSET, ZSET_SCHEMA).name(VALUE_SCHEMA_NAME).build();

	@Override
	public Struct apply(DataStructure<String> input) {
		Struct struct = new Struct(VALUE_SCHEMA);
		struct.put(FIELD_KEY, input.getKey());
		struct.put(FIELD_TTL, input.getTtl());
		struct.put(FIELD_TYPE, input.getType());
		struct.put(fieldName(input), fieldValue(input));
		return struct;
	}

	@SuppressWarnings("unchecked")
	private Object fieldValue(DataStructure<String> input) {
		switch (input.getType()) {
		case DataStructure.ZSET:
			return zsetMap((Collection<ScoredValue<String>>) input.getValue());
		case DataStructure.SET:
			return new ArrayList<>(((Collection<String>) input.getValue()));
		default:
			return input.getValue();
		}
	}

	public static Map<Double, String> zsetMap(Collection<ScoredValue<String>> value) {
		return value.stream().collect(Collectors.toMap(ScoredValue::getScore, ScoredValue::getValue));
	}

	public static Map<Long, Double> timeseriesMap(Collection<Sample> samples) {
		return samples.stream().collect(Collectors.toMap(Sample::getTimestamp, Sample::getValue));
	}

	public static String fieldName(DataStructure<String> input) {
		switch (input.getType()) {
		case DataStructure.HASH:
			return FIELD_HASH;
		case DataStructure.JSON:
			return FIELD_JSON;
		case DataStructure.LIST:
			return FIELD_LIST;
		case DataStructure.SET:
			return FIELD_SET;
		case DataStructure.STRING:
			return FIELD_STRING;
		case DataStructure.ZSET:
			return FIELD_ZSET;
		default:
			return FIELD_STRING;
		}
	}

}
