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

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.springframework.batch.item.ExecutionContext;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;

public class RedisStreamSourceTask extends SourceTask {

	public static final String TASK_ID = "task.id";

	public static final String OFFSET_FIELD = "offset";

	/**
	 * The offsets that have been processed and that are to be acknowledged by the
	 * reader in {@link RedisStreamSourceTask#commit()}.
	 */
	private final List<Map<String, ?>> sourceOffsets = new ArrayList<>();
	private final Clock clock;

	private RedisStreamSourceConfig config;
	private StreamItemReader<String, String> reader;
	private AbstractRedisClient client;
	private StreamMessageConverter converter;

	public RedisStreamSourceTask() {
		this(Clock.systemDefaultZone());
	}

	public RedisStreamSourceTask(Clock clock) {
		this.clock = clock;
	}

	@Override
	public String version() {
		return ManifestVersionProvider.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.config = new RedisStreamSourceConfig(props);
		this.converter = new StreamMessageConverter(clock, config);
		this.client = config.client();
		int taskId = Integer.parseInt(props.getOrDefault(TASK_ID, String.valueOf(0)));
		this.reader = reader(client, taskId, config);
		System.out.println("[RedisStreamSourceTask] STARTING: stream=" + config.getStreamName()
				+ ", consumerGroup=" + config.getStreamConsumerGroup()
				+ ", delivery=" + config.getStreamDelivery() + ", taskId=" + taskId);
		reader.open(new ExecutionContext());
	}

	@SuppressWarnings("unchecked")
	private StreamItemReader<String, String> reader(AbstractRedisClient client, int taskId,
			RedisStreamSourceConfig config) {
		String task = String.valueOf(taskId);
		String consumerName = config.getStreamConsumerName().replace(RedisStreamSourceConfigDef.TOKEN_TASK, task);
		Consumer<String> consumer = Consumer.from(config.getStreamConsumerGroup(), consumerName);
		String offset = offsetMap().map(m -> (String) m.get(OFFSET_FIELD)).orElse(config.getStreamOffset());
		String stream = config.getStreamName();
		StreamOffset<String> streamOffset = StreamOffset.from(stream, offset);
		StreamItemReader<String, String> reader = new StreamItemReader<>(client, StringCodec.UTF8, streamOffset);
		reader.setConsumer(consumer);
		reader.setBlock(Duration.ofMillis(config.getStreamBlock()));
		reader.setCount(config.getBatchSize());
		reader.setAckPolicy(ackPolicy(config));
		return reader;
	}

	private Optional<Map<String, Object>> offsetMap() {
		if (context == null) {
			return Optional.empty();
		}
		return Optional.ofNullable(context.offsetStorageReader().offset(Collections.emptyMap()));
	}

	@Override
	public void commitRecord(SourceRecord sourceRecord, RecordMetadata metadata) throws InterruptedException {
		Map<String, ?> currentOffset = sourceRecord.sourceOffset();
		if (currentOffset != null) {
			sourceOffsets.add(currentOffset);
			System.out.println("[RedisStreamSourceTask] RECORD_COMMITTED: offset=" + currentOffset.get(OFFSET_FIELD));
		} else {
			System.err.println("[RedisStreamSourceTask] WARN: Record with null sourceOffset, skipping ack");
		}
	}

	@Override
	public void commit() throws InterruptedException {
		if (reader != null && !sourceOffsets.isEmpty()) {
			String[] ids = sourceOffsets.stream().map(m -> (String) m.get(OFFSET_FIELD)).toArray(String[]::new);
			System.out.println("[RedisStreamSourceTask] ACKING: " + ids.length + " message(s) to stream '" + config.getStreamName() + "': " + String.join(", ", ids));
			try {
			reader.ack(config.getStreamName(), ids);
			sourceOffsets.clear();
			System.out.println("[RedisStreamSourceTask] ACK_OK: Successfully acked " + ids.length + " message(s)");
			} catch (Exception e) {
				System.err.println("[RedisStreamSourceTask] ACK_FAILED: " + ids.length + " message(s) to '" + config.getStreamName() + "': " + e.getMessage());
				throw new ConnectException("Could not ack offsets to Redis", e);
			}
		} else if (reader == null) {
			System.out.println("[RedisStreamSourceTask] COMMIT_SKIPPED: reader is null");
		}
	}

	@Override
	public void stop() {
		System.out.println("[RedisStreamSourceTask] STOPPING");
		if (reader != null) {
			reader.close();
			reader = null;
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
			client = null;
		}
		System.out.println("[RedisStreamSourceTask] STOPPED");
	}

	@Override
	public List<SourceRecord> poll() {
		List<StreamMessage<String, String>> messages;
		try {
			messages = reader.readMessages();
		} catch (RedisCommandTimeoutException e) {
			System.out.println("[RedisStreamSourceTask] POLL_TIMEOUT: " + config.getStreamBlock() + "ms, no new messages");
			throw new RetriableException("Timeout while reading stream messages", e);
		} catch (Exception e) {
			System.err.println("[RedisStreamSourceTask] POLL_ERROR: stream='" + config.getStreamName() + "': " + e.getMessage());
			throw new ConnectException("Could not read messages from stream", e);
		}
		if (!messages.isEmpty()) {
			System.out.println("[RedisStreamSourceTask] POLL_OK: Read " + messages.size() + " message(s) from '" + config.getStreamName() + "'");
		}
		return messages.stream().map(converter).collect(Collectors.toList());
	}

	private StreamItemReader.AckPolicy ackPolicy(RedisStreamSourceConfig config) {
		switch (config.getStreamDelivery()) {
		case RedisStreamSourceConfig.STREAM_DELIVERY_AT_MOST_ONCE:
			return AckPolicy.AUTO;
		case RedisStreamSourceConfig.STREAM_DELIVERY_AT_LEAST_ONCE:
			return AckPolicy.MANUAL;
		default:
			throw new IllegalArgumentException("Illegal value for " + RedisStreamSourceConfigDef.STREAM_DELIVERY_CONFIG
					+ ": " + config.getStreamDelivery());
		}
	}

}
