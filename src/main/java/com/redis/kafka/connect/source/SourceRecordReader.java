package com.redis.kafka.connect.source;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

public interface SourceRecordReader {

	void open();

	List<SourceRecord> poll();

	void close();

	void commit(List<Map<String, ?>> sourceOffsets);
}
