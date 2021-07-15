package com.redislabs.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface SourceRecordReader {

    void open();

    List<SourceRecord> poll();

    void close();
}
