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

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigException;
import org.springframework.util.StringUtils;

import com.redis.kafka.connect.common.RedisConfig;

public class RedisSinkConfig extends RedisConfig {

	public enum RedisType {
		HASH, JSON, TIMESERIES, STRING, STREAM, LIST, SET, ZSET, HASH_FIELD
	}

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
	private final KeyPath hashKey;
	private final KeyPath hashField;

	public RedisSinkConfig(Map<?, ?> originals) {
		super(new RedisSinkConfigDef(), originals);
		String charsetName = getString(RedisSinkConfigDef.CHARSET_CONFIG).trim();
		charset = Charset.forName(charsetName);
		type = RedisType.valueOf(getString(RedisSinkConfigDef.TYPE_CONFIG));
		keyspace = getString(RedisSinkConfigDef.KEYSPACE_CONFIG).trim();
		separator = getString(RedisSinkConfigDef.SEPARATOR_CONFIG).trim();
		multiExec = Boolean.TRUE.equals(getBoolean(RedisSinkConfigDef.MULTIEXEC_CONFIG));
		waitReplicas = getInt(RedisSinkConfigDef.WAIT_REPLICAS_CONFIG);
		waitTimeout = Duration.ofMillis(getLong(RedisSinkConfigDef.WAIT_TIMEOUT_CONFIG));
		keyTTL = getLong(RedisSinkConfigDef.KEY_TTL_CONFIG);
		offsetNamespace = offsetNamespace(originals);
		validateHashField();
		hashKey = keyPath(RedisSinkConfigDef.HASH_KEY_CONFIG);
		hashField = keyPath(RedisSinkConfigDef.HASH_FIELD_CONFIG);
	}

	private void validateHashField() {
		Map<String, String> props = Map.of(RedisSinkConfigDef.TYPE_CONFIG, type.name(),
				RedisSinkConfigDef.HASH_KEY_CONFIG, getString(RedisSinkConfigDef.HASH_KEY_CONFIG),
				RedisSinkConfigDef.HASH_FIELD_CONFIG, getString(RedisSinkConfigDef.HASH_FIELD_CONFIG));
		RedisSinkConfigDef.hashFieldErrors(props).entrySet().stream().findFirst().ifPresent(e -> {
			throw new ConfigException(e.getKey(), props.get(e.getKey()), e.getValue());
		});
	}

	private KeyPath keyPath(String name) {
		return type == RedisType.HASH_FIELD ? KeyPath.parse(getString(name).trim()) : null;
	}

	private String offsetNamespace(Map<?, ?> originals) {
		String namespace = getString(RedisSinkConfigDef.OFFSET_NAMESPACE_CONFIG).trim();
		if (StringUtils.hasText(namespace)) {
			return namespace;
		}
		Object connectorName = originals.get("name");
		if (connectorName != null && StringUtils.hasText(connectorName.toString())) {
			return connectorName.toString().trim();
		}
		return "default";
	}

	public Charset getCharset() {
		return charset;
	}

	public RedisType getType() {
		return type;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getSeparator() {
		return separator;
	}

	public boolean isMultiExec() {
		return multiExec;
	}

	public int getWaitReplicas() {
		return waitReplicas;
	}

	public Duration getWaitTimeout() {
		return waitTimeout;
	}

	public long getKeyTTL() {
		return keyTTL;
	}

	public String getOffsetNamespace() {
		return offsetNamespace;
	}

	/**
	 * @return the record key reference naming the hash, or null unless the type is HASH_FIELD
	 */
	public KeyPath getHashKey() {
		return hashKey;
	}

	/**
	 * @return the record key reference naming the hash field, or null unless the type is HASH_FIELD
	 */
	public KeyPath getHashField() {
		return hashField;
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
						offsetNamespace, reference(hashKey), reference(hashField));
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
				&& waitReplicas == other.waitReplicas && waitTimeout == other.waitTimeout && keyTTL == other.keyTTL
				&& Objects.equals(offsetNamespace, other.offsetNamespace)
				&& Objects.equals(reference(hashKey), reference(other.hashKey))
				&& Objects.equals(reference(hashField), reference(other.hashField));
	}

}
