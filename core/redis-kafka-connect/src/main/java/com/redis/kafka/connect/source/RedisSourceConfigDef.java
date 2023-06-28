package com.redis.kafka.connect.source;

import com.redis.kafka.connect.common.RedisConfigDef;

public abstract class RedisSourceConfigDef extends RedisConfigDef {

	public static final String BATCH_SIZE_CONFIG = "batch.size";
	public static final int BATCH_SIZE_DEFAULT = 500;
	public static final String BATCH_SIZE_DOC = "Maximum number of records to include in a single read when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";

	protected RedisSourceConfigDef() {
		defineSourceConfig();
	}

	protected RedisSourceConfigDef(RedisSourceConfigDef base) {
		super(base);
		defineSourceConfig();
	}

	private void defineSourceConfig() {
		define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Importance.LOW, BATCH_SIZE_DOC);
	}

}