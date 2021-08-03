package com.redislabs.kafka.connect.source;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.StreamItemReader;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamSourceRecordReader extends AbstractSourceRecordReader<StreamMessage<String, String>> {

    public static final String OFFSET_FIELD = "offset";
    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE_SCHEMA_NAME = "com.redislabs.kafka.connect.StreamEventValue";
    private static final Schema VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name(VALUE_SCHEMA_NAME);
    private final String topic;
    private final String consumer;

    private StreamItemReader reader;

    public StreamSourceRecordReader(RedisEnterpriseSourceConfig sourceConfig, int taskId) {
        super(sourceConfig);
        this.topic = sourceConfig.getTopicName().replace(RedisEnterpriseSourceConfig.TOKEN_STREAM, sourceConfig.getStreamName());
        this.consumer = sourceConfig.getStreamConsumerName().replace(RedisEnterpriseSourceConfig.TOKEN_TASK, String.valueOf(taskId));
    }

    @Override
    protected void open(RedisClient client) {
        XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from(sourceConfig.getStreamName(), sourceConfig.getStreamOffset());
        reader = StreamItemReader.client(client).offset(streamOffset).block(Duration.ofMillis(sourceConfig.getStreamBlock())).count(sourceConfig.getBatchSize()).consumerGroup(sourceConfig.getStreamConsumerGroup()).consumer(consumer).build();
        reader.open(new ExecutionContext());
    }

    @Override
    protected List<StreamMessage<String, String>> doPoll() throws Exception {
        return reader.readMessages();
    }

    @Override
    protected void doClose() {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected SourceRecord convert(StreamMessage<String, String> message) {
        Map<String, ?> sourcePartition = new HashMap<>();
        Map<String, ?> sourceOffset = Collections.singletonMap(OFFSET_FIELD, message.getId());
        return new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, message.getId(), VALUE_SCHEMA, message.getBody(), Instant.now().getEpochSecond());
    }

}
