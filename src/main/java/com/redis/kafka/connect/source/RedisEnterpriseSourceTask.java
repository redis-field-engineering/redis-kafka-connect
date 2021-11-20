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

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.redis.kafka.connect.RedisEnterpriseSourceConnector;

public class RedisEnterpriseSourceTask extends SourceTask {

	public static final String TASK_ID = "task.id";
	public static final String KEYS_IDLE_TIMEOUT = "keys.idletimeout";

	private SourceRecordReader reader;

	@Override
	public String version() {
		return new RedisEnterpriseSourceConnector().version();
	}

	public SourceRecordReader getReader() {
		return reader;
	}

	@Override
	public void start(Map<String, String> props) {
		this.reader = reader(props);
		try {
			this.reader.open();
		} catch (Exception e) {
			throw new RetriableException("Could not open reader", e);
		}
	}

	private SourceRecordReader reader(Map<String, String> props) {
		RedisEnterpriseSourceConfig sourceConfig = new RedisEnterpriseSourceConfig(props);
		if (sourceConfig.getReaderType() == RedisEnterpriseSourceConfig.ReaderType.STREAM) {
			String taskIdString = props.get(TASK_ID);
			int taskId = taskIdString == null ? 0 : Integer.parseInt(taskIdString);
			return new StreamSourceRecordReader(sourceConfig, taskId);
		}
		String idleTimeoutString = props.get(KEYS_IDLE_TIMEOUT);
		return new KeySourceRecordReader(sourceConfig,
				idleTimeoutString == null ? null : Duration.ofMillis(Long.parseLong(idleTimeoutString)));
	}

	@Override
	public void stop() {
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public List<SourceRecord> poll() {
		return reader.poll();
		// TODO: return heartbeat if no records
	}

}
