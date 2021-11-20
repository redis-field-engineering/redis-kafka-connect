package com.redis.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface SourceRecordReader {

    void open() throws Exception;

    List<SourceRecord> poll();

    void close();
}
