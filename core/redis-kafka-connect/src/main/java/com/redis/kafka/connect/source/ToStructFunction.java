package com.redis.kafka.connect.source;

import io.lettuce.core.ScoredValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

/** Converts Redis key snapshots to the published Kafka Connect value schema. */
public class ToStructFunction implements Function<KeySnapshot, Struct> {

	/** Creates a key snapshot converter. */
	public ToStructFunction() {
	}

	/** Schema field name for key. */
	public static final String FIELD_KEY = "key";

	/** Schema field name for ttl. */
	public static final String FIELD_TTL = "ttl";

	/** Schema field name for type. */
	public static final String FIELD_TYPE = "type";

	/** Schema field name for value. */
	public static final String FIELD_VALUE = "value";

	/** Schema field name for hash. */
	public static final String FIELD_HASH = "hash";

	/** Schema field name for json. */
	public static final String FIELD_JSON = "json";

	/** Schema field name for list. */
	public static final String FIELD_LIST = "list";

	/** Schema field name for set. */
	public static final String FIELD_SET = "set";

	/** Schema field name for string. */
	public static final String FIELD_STRING = "string";

	/** Schema field name for zset. */
	public static final String FIELD_ZSET = "zset";

	/** Kafka Connect schema for ttl values. */
	public static final Schema TTL_SCHEMA = Schema.OPTIONAL_INT64_SCHEMA;

	/** Kafka Connect schema for type values. */
	public static final Schema TYPE_SCHEMA = Schema.STRING_SCHEMA;

	/** Published name of the key snapshot value schema. */
	public static final String VALUE_SCHEMA_NAME = "com.redis.kafka.connect.keys.Value";

	/** Kafka Connect schema for hash values. */
	public static final Schema HASH_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional();

	/** Kafka Connect schema for list values. */
	public static final Schema LIST_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional();

	/** Kafka Connect schema for set values. */
	public static final Schema SET_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional();

	/** Kafka Connect schema for string values. */
	public static final Schema STRING_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;

	/** Kafka Connect schema for zset values. */
	public static final Schema ZSET_SCHEMA = SchemaBuilder.map(Schema.FLOAT64_SCHEMA, Schema.STRING_SCHEMA).optional();

	/** Kafka Connect schema for key snapshot records. */
	public static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field(FIELD_KEY, Schema.STRING_SCHEMA)
			.field(FIELD_TTL, TTL_SCHEMA).field(FIELD_TYPE, TYPE_SCHEMA).field(FIELD_HASH, HASH_SCHEMA)
			.field(FIELD_STRING, STRING_SCHEMA).field(FIELD_JSON, STRING_SCHEMA).field(FIELD_LIST, LIST_SCHEMA)
			.field(FIELD_SET, SET_SCHEMA).field(FIELD_ZSET, ZSET_SCHEMA).name(VALUE_SCHEMA_NAME).build();

	@Override
	public Struct apply(KeySnapshot input) {
		Struct struct = new Struct(VALUE_SCHEMA);
		struct.put(FIELD_KEY, input.key());
		struct.put(FIELD_TTL, input.ttl());
		struct.put(FIELD_TYPE, input.type());
		if (input.type() != null) {
			switch (input.type()) {
			case KeySnapshot.TYPE_HASH:
				struct.put(FIELD_HASH, input.value());
				break;
			case KeySnapshot.TYPE_JSON:
				struct.put(FIELD_JSON, input.value());
				break;
			case KeySnapshot.TYPE_LIST:
				struct.put(FIELD_LIST, input.value());
				break;
			case KeySnapshot.TYPE_SET:
				struct.put(FIELD_SET, list(input));
				break;
			case KeySnapshot.TYPE_STRING:
				struct.put(FIELD_STRING, input.value());
				break;
			case KeySnapshot.TYPE_ZSET:
				struct.put(FIELD_ZSET, zsetMap(input));
				break;
			default:
				break;
			}
		}
		return struct;
	}

	@SuppressWarnings("unchecked")
	private Object list(KeySnapshot input) {
		return new ArrayList<>((Collection<String>) input.value());
	}

	/**
	 * Converts sorted-set members to the published score-to-member map.
	 *
	 * @param input snapshot containing scored string values
	 * @return members indexed by score
	 * @throws DataException if multiple members have the same score
	 */
	@SuppressWarnings("unchecked")
	public static Map<Double, String> zsetMap(KeySnapshot input) {
		Collection<ScoredValue<String>> value = (Collection<ScoredValue<String>>) input.value();
		return zsetMap(value);
	}

	/**
	 * Converts sorted-set members to the published score-to-member map.
	 *
	 * @param value scored string values
	 * @return members indexed by score
	 * @throws DataException if multiple members have the same score
	 */
	public static Map<Double, String> zsetMap(Collection<ScoredValue<String>> value) {
		return value.stream().collect(Collectors.toMap(ScoredValue::getScore, ScoredValue::getValue, (first, second) -> {
            throw new DataException("The published zset source schema cannot represent multiple members with the same score");
        }));
	}

}
