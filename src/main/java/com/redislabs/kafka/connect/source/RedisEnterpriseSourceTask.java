/**
 * Copyright © 2021 Redis Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redislabs.kafka.connect.source;

import com.redislabs.kafka.connect.RedisEnterpriseSourceConnector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class RedisEnterpriseSourceTask extends SourceTask {

  public static final String STREAM_FIELD = "stream";
  public static final String OFFSET_FIELD = "offset";
  private static final Schema VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

  private RedisClient client;
  private StatefulRedisConnection<String, String> connection;
  private RedisCommands<String, String> commands;
  private RedisEnterpriseSourceConfig sourceConfig;
  private Iterator<StreamMessage<String, String>> iterator;
  private boolean stopped;

  @Override
  public String version() {
    return new RedisEnterpriseSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.sourceConfig = new RedisEnterpriseSourceConfig(props);
    this.client = RedisClient.create(sourceConfig.getRedisUri());
    this.connection = this.client.connect();
    this.commands = connection.sync();
    this.iterator = Collections.emptyIterator();
    this.stopped = false;
  }

  @Override
  public void stop() {
    stopped = true;
    if (connection != null) {
      connection.close();
    }
    if (client != null) {
      client.shutdown();
    }
  }

  @Override
  public List<SourceRecord> poll() {
    log.debug("Starting to poll");
    final long endTime = System.currentTimeMillis() + sourceConfig.getPollInterval();
    String topic = sourceConfig.getTopic();
    List<SourceRecord> records = new ArrayList<>();
    int batchSize = sourceConfig.getBatchSize();
    StreamMessage<String, String> message;
    while (!stopped && (message = nextMessage(endTime - System.currentTimeMillis())) != null && records.size() < batchSize) {
      records.add(new SourceRecord(offsetKey(), offsetValue(message.getId()), topic, null, null, null, VALUE_SCHEMA, message.getBody(), System.currentTimeMillis()));
    }
    if (stopped) {
      return null;
    }
    if (records.isEmpty()) {
      //TODO return heartbeat
      return null;
    }
    return records;
  }

  private String currentOffset() {
    Map<String, Object> offset = context.offsetStorageReader().offset(offsetKey());
    if (offset != null) {
      Object lastRecordedOffset = offset.get(OFFSET_FIELD);
      if (lastRecordedOffset != null) {
        if (lastRecordedOffset instanceof String) {
          log.debug("Found previous offset: {}", lastRecordedOffset);
          return (String) lastRecordedOffset;
        }
        throw new ConnectException("Offset position is the incorrect type");
      }
    }
    return sourceConfig.getOffset();
  }

  private Map<String, String> offsetKey() {
    return Collections.singletonMap(STREAM_FIELD, sourceConfig.getStream());
  }

  private Map<String, String> offsetValue(String offset) {
    return Collections.singletonMap(OFFSET_FIELD, offset);
  }


  private StreamMessage<String, String> nextMessage(long block) {
    if (!iterator.hasNext()) {
      XReadArgs args = XReadArgs.Builder.block(block);
      if (sourceConfig.getCount() != null) {
        args.count(sourceConfig.getCount());
      }
      XReadArgs.StreamOffset<String> offset = XReadArgs.StreamOffset.from(sourceConfig.getStream(), currentOffset());
      List<StreamMessage<String, String>> messages = commands.xread(args, offset);
      if (messages == null || messages.isEmpty()) {
        return null;
      }
      iterator = messages.iterator();
    }
    return iterator.next();
  }
}
