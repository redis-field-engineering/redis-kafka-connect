package com.redis.kafka.connect.source;

import io.lettuce.core.RedisClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeySourceRecordReader extends AbstractSourceRecordReader<DataStructure> {

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Schema STRING_VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String HASH_VALUE_SCHEMA_NAME = "com.redis.kafka.connect.HashEventValue";
    private static final Schema HASH_VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name(HASH_VALUE_SCHEMA_NAME);

    private final int batchSize;
    private final String topic;
    private LiveKeyValueItemReader<DataStructure> reader;
    private final Duration idleTimeout;

    public KeySourceRecordReader(RedisEnterpriseSourceConfig sourceConfig, Duration idleTimeout) {
        super(sourceConfig);
        this.topic = sourceConfig.getTopicName();
        this.batchSize = Math.toIntExact(sourceConfig.getBatchSize());
        this.idleTimeout = idleTimeout;
    }

    @Override
    protected void open(RedisClient client) {
        reader = DataStructureItemReader.client(client).live().idleTimeout(idleTimeout).keyPatterns(sourceConfig.getKeyPatterns().toArray(new String[0])).build();
        reader.open(new ExecutionContext());
    }

    @Override
    protected List<DataStructure> doPoll() throws Exception {
        return reader.read(batchSize);
    }

    @Override
    protected SourceRecord convert(DataStructure input) {
        Map<String, ?> sourcePartition = new HashMap<>();
        Map<String, ?> sourceOffset = new HashMap<>();
        return new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, input.getKey(), schema(input), input.getValue(), Instant.now().getEpochSecond());
    }

    private Schema schema(DataStructure input) {
        if (DataStructure.HASH.equals(input.getType())) {
            return HASH_VALUE_SCHEMA;
        }
        return STRING_VALUE_SCHEMA;
    }

    @Override
    protected void doClose() {
        if (reader != null) {
            reader.close();
        }
    }
}
