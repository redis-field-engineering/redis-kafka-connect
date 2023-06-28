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
package com.redis.kafka.connect.source;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RedisKeysSourceConfig extends RedisSourceConfig {

	private final List<String> keyPatterns;
	private final String topicName;
	private Duration idleTimeout;

	public RedisKeysSourceConfig(Map<?, ?> originals) {
		super(new RedisKeysSourceConfigDef(), originals);
		this.topicName = getString(RedisKeysSourceConfigDef.TOPIC_CONFIG);
		this.keyPatterns = getList(RedisKeysSourceConfigDef.KEY_PATTERNS_CONFIG);
		this.idleTimeout = Duration.ofMillis(getLong(RedisKeysSourceConfigDef.IDLE_TIMEOUT_CONFIG));
	}

	public List<String> getKeyPatterns() {
		return keyPatterns;
	}

	public String getTopicName() {
		return topicName;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(batchSize, keyPatterns, topicName);
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
		RedisKeysSourceConfig other = (RedisKeysSourceConfig) obj;
		return Objects.equals(batchSize, other.batchSize) && Objects.equals(keyPatterns, other.keyPatterns)
				&& Objects.equals(topicName, other.topicName);
	}

}
