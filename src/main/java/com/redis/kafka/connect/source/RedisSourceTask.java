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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.redis.kafka.connect.RedisSourceConnector;
import com.redis.spring.batch.reader.LiveReaderOptions;
import com.redis.spring.batch.step.FlushingOptions;

public class RedisSourceTask extends SourceTask {

	public static final String TASK_ID = "task.id";
	public static final String KEYS_IDLE_TIMEOUT = "keys.idletimeout";

	/**
	 * The offsets that have been processed and that are to be acknowledged by the
	 * reader in {@link RedisSourceTask#commit()}.
	 */
	private final List<Map<String, ?>> sourceOffsets = new ArrayList<>();

	private SourceRecordReader reader;

	@Override
	public String version() {
		return new RedisSourceConnector().version();
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
		RedisSourceConfig sourceConfig = new RedisSourceConfig(props);
		switch (sourceConfig.getReaderType()) {
		case STREAM:
			String taskId = props.getOrDefault(TASK_ID, String.valueOf(0));
			return new StreamSourceRecordReader(sourceConfig, Integer.parseInt(taskId));
		case KEYS:
			String idleMillis = props.getOrDefault(KEYS_IDLE_TIMEOUT,
					String.valueOf(FlushingOptions.DEFAULT_FLUSHING_INTERVAL.toMillis()));
			FlushingOptions flushingOptions = FlushingOptions.builder()
					.timeout(Duration.ofMillis(Long.parseLong(idleMillis))).build();
			LiveReaderOptions liveReaderOptions = LiveReaderOptions.builder().flushingOptions(flushingOptions).build();
			return new KeySourceRecordReader(sourceConfig, liveReaderOptions);
		default:
			throw new IllegalArgumentException("Unknown reader type: " + sourceConfig.getReaderType());
		}
	}

	private void addSourceOffset(Map<String, ?> sourceOffset) {
		sourceOffsets.add(sourceOffset);
	}

	@Deprecated
	@Override
	public void commitRecord(SourceRecord sourceRecord) throws InterruptedException {
		Map<String, ?> currentOffset = sourceRecord.sourceOffset();
		if (currentOffset != null) {
			addSourceOffset(currentOffset);
		}
	}

	@Override
	public void commit() throws InterruptedException {
		if (reader != null) {
			reader.commit(sourceOffsets);
		}
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
