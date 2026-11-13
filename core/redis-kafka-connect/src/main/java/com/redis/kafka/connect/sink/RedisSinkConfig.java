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

import com.redis.kafka.connect.common.RedisConfig;
import com.redis.kafka.connect.common.RedisConfigDef;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigException;

/** Parses Redis sink settings and validates standalone-only operations. */
public class RedisSinkConfig extends RedisConfig {

	/** Redis data structures supported as sink destinations. */
	public enum RedisType {
		/** Stores record fields in a hash. */
		HASH,
		/** Stores a JSON document. */
		JSON,
		/** Adds a time-series sample. */
		TIMESERIES,
		/** Stores a string value. */
		STRING,
		/** Appends an entry to a stream. */
		STREAM,
		/** Appends a value to a list. */
		LIST,
		/** Adds a member to a set. */
		SET,
		/** Adds a scored member to a sorted set. */
		ZSET,
		/** Stores each record as one field of a hash named by the record key. */
		HASH_FIELD
	}

	/** Configuration definition for this connector. */
	public static final RedisSinkConfigDef CONFIG = new RedisSinkConfigDef();

	private final Charset charset;
	private final RedisType type;
	private final String keyspace;
	private final String separator;
	private final boolean multiExec;
	private final int waitReplicas;
	private final Duration waitTimeout;
	private final long keyTTL;
	private final String offsetNamespace;
	private final KeyPath hashPath;
	private final KeyPath fieldPath;
	private final boolean hashFieldExpireEnabled;
	private final long hashFieldTTL;

	/**
	 * Parses and validates the connector configuration.
	 *
	 * @param originals connector properties
	 */
	public RedisSinkConfig(Map<?, ?> originals) {
		super(new RedisSinkConfigDef(), originals);
		String charsetName = getString(RedisSinkConfigDef.CHARSET_CONFIG).trim();
		charset = Charset.forName(charsetName);
		type = RedisType.valueOf(getString(RedisSinkConfigDef.TYPE_CONFIG));
		keyspace = getString(RedisSinkConfigDef.KEYSPACE_CONFIG).trim();
		separator = getString(RedisSinkConfigDef.SEPARATOR_CONFIG).trim();
		multiExec = Boolean.TRUE.equals(getBoolean(RedisSinkConfigDef.MULTIEXEC_CONFIG));
		waitReplicas = getInt(RedisSinkConfigDef.WAIT_REPLICAS_CONFIG);
		if (Boolean.TRUE.equals(getBoolean(RedisConfigDef.CLUSTER_CONFIG))) {
			if (multiExec) {
				throw new ConfigException(RedisSinkConfigDef.MULTIEXEC_CONFIG, true,
						"Transactions require redis.cluster=false");
			}
			if (waitReplicas > 0) {
				throw new ConfigException(RedisSinkConfigDef.WAIT_REPLICAS_CONFIG, waitReplicas,
						"WAIT requires redis.cluster=false");
			}
		}
		waitTimeout = Duration.ofMillis(getLong(RedisSinkConfigDef.WAIT_TIMEOUT_CONFIG));
		keyTTL = getLong(RedisSinkConfigDef.KEY_TTL_CONFIG);
		offsetNamespace = offsetNamespace(originals);
		String hashReference = getString(RedisSinkConfigDef.HASH_FIELD_HASH_CONFIG);
		String fieldReference = getString(RedisSinkConfigDef.HASH_FIELD_FIELD_CONFIG);
		RedisSinkConfigDef.hashFieldErrors(type, hashReference, fieldReference).entrySet().stream().findFirst()
				.ifPresent(error -> {
					throw new ConfigException(error.getKey(), getString(error.getKey()), error.getValue());
				});
		hashFieldExpireEnabled = Boolean.TRUE.equals(getBoolean(RedisSinkConfigDef.HASH_FIELD_EXPIRE_ENABLED_CONFIG));
		hashFieldTTL = getLong(RedisSinkConfigDef.HASH_FIELD_EXPIRE_TTL_CONFIG);
		RedisSinkConfigDef.hashFieldExpireErrors(type, hashFieldExpireEnabled, hashFieldTTL).entrySet().stream().findFirst()
				.ifPresent(error -> {
					throw new ConfigException(error.getKey(), originals().get(error.getKey()), error.getValue());
				});
		hashPath = type == RedisType.HASH_FIELD ? KeyPath.parse(hashReference.trim()) : null;
		fieldPath = type == RedisType.HASH_FIELD ? KeyPath.parse(fieldReference.trim()) : null;
	}

	private String offsetNamespace(Map<?, ?> originals) {
		String namespace = getString(RedisSinkConfigDef.OFFSET_NAMESPACE_CONFIG).trim();
		if (!namespace.isBlank()) {
			return namespace;
		}
		Object connectorName = originals.get("name");
		if (connectorName != null && !connectorName.toString().isBlank()) {
			return connectorName.toString().trim();
		}
		return "default";
	}

	/** {@return the character set used to encode Redis keys and values} */
	public Charset getCharset() {
		return charset;
	}

	/** {@return the destination Redis data structure} */
	public RedisType getType() {
		return type;
	}

	/** {@return the destination keyspace template, which may contain the topic placeholder} */
	public String getKeyspace() {
		return keyspace;
	}

	/** {@return the separator used in non-collection destination keys} */
	public String getSeparator() {
		return separator;
	}

	/** {@return whether writes use MULTI/EXEC transactions} */
	public boolean isMultiExec() {
		return multiExec;
	}

	/** {@return the number of replicas required by WAIT, or zero to disable waiting} */
	public int getWaitReplicas() {
		return waitReplicas;
	}

	/** {@return the timeout for replica acknowledgments} */
	public Duration getWaitTimeout() {
		return waitTimeout;
	}

	/** {@return the destination key lifetime in seconds, or -1 for no expiry} */
	public long getKeyTTL() {
		return keyTTL;
	}

	/** {@return the namespace used for sink offset keys in Redis} */
	public String getOffsetNamespace() {
		return offsetNamespace;
	}

	/** {@return the record key reference that names the hash, or null unless the type is HASH_FIELD} */
	KeyPath getHashPath() {
		return hashPath;
	}

	/** {@return the record key reference that names the field within the hash, or null unless the type is HASH_FIELD} */
	KeyPath getFieldPath() {
		return fieldPath;
	}

	/** {@return whether HASH_FIELD fields can expire with HEXPIRE} */
	public boolean isHashFieldExpireEnabled() {
		return hashFieldExpireEnabled;
	}

	/** {@return the hash field lifetime in seconds for HASH_FIELD; 0 or below means no expiry} */
	public long getHashFieldTTL() {
		return hashFieldTTL;
	}

	private static String reference(KeyPath path) {
		return path == null ? null : path.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ Objects.hash(charset, keyspace, separator, multiExec, type, waitReplicas, waitTimeout, keyTTL,
						offsetNamespace, reference(hashPath), reference(fieldPath), hashFieldExpireEnabled, hashFieldTTL);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		RedisSinkConfig other = (RedisSinkConfig) obj;
		return Objects.equals(charset, other.charset) && Objects.equals(keyspace, other.keyspace)
				&& Objects.equals(separator, other.separator) && multiExec == other.multiExec && type == other.type
				&& waitReplicas == other.waitReplicas && Objects.equals(waitTimeout, other.waitTimeout) && keyTTL == other.keyTTL
				&& Objects.equals(offsetNamespace, other.offsetNamespace)
				&& Objects.equals(reference(hashPath), reference(other.hashPath))
				&& Objects.equals(reference(fieldPath), reference(other.fieldPath))
				&& hashFieldExpireEnabled == other.hashFieldExpireEnabled && hashFieldTTL == other.hashFieldTTL;
	}

}
