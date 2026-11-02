/*
 * Copyright © 2021 Redis
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect.sink;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

/**
 * A reference to a field inside a Kafka record key, written as
 * {@code key.<field>[.<field>...]}. Resolves against a {@link Struct} key
 * (Avro, JSON Schema, Protobuf) or a schemaless {@link Map} key.
 */
public final class KeyPath {

	public static final String PREFIX = "key.";

	private final String reference;

	private final List<String> segments;

	private KeyPath(String reference, List<String> segments) {
		this.reference = reference;
		this.segments = segments;
	}

	public static KeyPath parse(String reference) {
		if (reference == null || !reference.startsWith(PREFIX)) {
			throw invalid(reference);
		}
		String[] segments = reference.substring(PREFIX.length()).split("\\.", -1);
		for (String segment : segments) {
			if (segment.trim().isEmpty()) {
				throw invalid(reference);
			}
		}
		return new KeyPath(reference, Collections.unmodifiableList(Arrays.asList(segments)));
	}

	private static IllegalArgumentException invalid(String reference) {
		return new IllegalArgumentException(
				String.format("Invalid key reference '%s': expected key.<field>[.<field>...]", reference));
	}

	public List<String> segments() {
		return segments;
	}

	public byte[] resolve(Object recordKey, Charset charset) {
		if (recordKey == null) {
			throw error("record key is null");
		}
		Object value = recordKey;
		for (String segment : segments) {
			value = child(value, segment);
		}
		if (value == null) {
			throw error("value is null");
		}
		if (value instanceof byte[]) {
			return (byte[]) value;
		}
		if (value instanceof String || value instanceof Boolean
				|| (value instanceof Number && !(value instanceof BigDecimal) && !(value instanceof BigInteger))) {
			return value.toString().getBytes(charset);
		}
		throw error("unsupported value type " + value.getClass().getName());
	}

	private Object child(Object parent, String segment) {
		if (parent instanceof Struct) {
			Struct struct = (Struct) parent;
			if (struct.schema().field(segment) == null) {
				throw error("field '" + segment + "' not found");
			}
			return struct.get(segment);
		}
		if (parent instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) parent;
			if (!map.containsKey(segment)) {
				throw error("field '" + segment + "' not found");
			}
			return map.get(segment);
		}
		throw error("expected a Struct or Map but found " + (parent == null ? "null" : parent.getClass().getName()));
	}

	private DataException error(String reason) {
		return new DataException(String.format("Could not resolve '%s' from record key: %s", reference, reason));
	}

	@Override
	public String toString() {
		return reference;
	}

}
