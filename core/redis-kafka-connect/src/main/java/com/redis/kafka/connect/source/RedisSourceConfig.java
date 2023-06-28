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

import java.util.Map;
import java.util.Objects;

import com.redis.kafka.connect.common.RedisConfig;

public class RedisSourceConfig extends RedisConfig {

	protected final int batchSize;

	protected RedisSourceConfig(RedisSourceConfigDef def, Map<?, ?> originals) {
		super(def, originals);
		this.batchSize = getInt(RedisSourceConfigDef.BATCH_SIZE_CONFIG);
	}

	public int getBatchSize() {
		return batchSize;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(batchSize);
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
		RedisSourceConfig other = (RedisSourceConfig) obj;
		return Objects.equals(batchSize, other.batchSize);
	}

}
