/*
 * Copyright © 2021 Redis
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceConnector;

import com.redis.kafka.connect.common.ManifestVersionProvider;

public abstract class AbstractRedisSourceConnector extends SourceConnector {

	private Map<String, String> props;

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> taskConfig = new HashMap<>(props);
			taskConfig.put(RedisStreamSourceTask.TASK_ID, Integer.toString(i));
			taskConfigs.add(taskConfig);
		}
		return taskConfigs;
	}

	@Override
	public void stop() {
		// Do nothing
	}

	@Override
	public String version() {
		return ManifestVersionProvider.getVersion();
	}
}
