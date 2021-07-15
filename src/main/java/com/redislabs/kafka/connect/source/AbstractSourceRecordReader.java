package com.redislabs.kafka.connect.source;

import com.redislabs.mesclun.RedisModulesClient;
import io.lettuce.core.RedisClient;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractSourceRecordReader<T> implements SourceRecordReader {

    private static final Logger log = LoggerFactory.getLogger(AbstractSourceRecordReader.class);

    protected final RedisEnterpriseSourceConfig sourceConfig;
    private RedisClient client;

    protected AbstractSourceRecordReader(RedisEnterpriseSourceConfig sourceConfig) {
        Assert.notNull(sourceConfig, "Source connector config must not be null");
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void open() {
        this.client = RedisModulesClient.create(sourceConfig.getRedisUri());
        open(client);
    }

    protected abstract void open(RedisClient client);

    @Override
    public List<SourceRecord> poll() {
        List<T> records;
        try {
            records = doPoll();
        } catch (Exception e) {
            log.error("Could not read messages", e);
            return null;
        }
        return records.stream().map(this::convert).collect(Collectors.toList());
    }

    protected abstract List<T> doPoll() throws Exception;

    protected abstract SourceRecord convert(T input);

    @Override
    public void close() {
        doClose();
        client.shutdown();
        client.getResources().shutdown();
    }

    protected abstract void doClose();
}
