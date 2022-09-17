package com.redis.kafka.connect.source;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

public abstract class AbstractSourceRecordReader<T> implements SourceRecordReader {

	private static final Logger log = LoggerFactory.getLogger(AbstractSourceRecordReader.class);

	protected final RedisSourceConfig config;

	protected AbstractSourceRecordReader(RedisSourceConfig sourceConfig) {
		Assert.notNull(sourceConfig, "Source connector config must not be null");
		this.config = sourceConfig;
	}

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

}
