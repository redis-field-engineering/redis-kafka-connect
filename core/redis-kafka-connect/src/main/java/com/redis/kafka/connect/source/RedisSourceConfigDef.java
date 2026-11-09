package com.redis.kafka.connect.source;

import com.redis.kafka.connect.common.RedisConfigDef;

/** Defines batch settings shared by Redis sources. */
public abstract class RedisSourceConfigDef extends RedisConfigDef {

	/** Configuration key for {@code batch.size}. */
	public static final String BATCH_SIZE_CONFIG = "batch.size";
	/** Default value for {@link #BATCH_SIZE_CONFIG}. */
	public static final int BATCH_SIZE_DEFAULT = 500;
	/** User-facing description of {@link #BATCH_SIZE_CONFIG}. */
	public static final String BATCH_SIZE_DOC = "Maximum number of records to include in a single read when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";

	/** Creates the configuration definition. */
	protected RedisSourceConfigDef() {
		defineSourceConfig();
	}

	/**
	 * Creates the configuration definition.
	 *
	 * @param base base configuration to copy
	 */
	protected RedisSourceConfigDef(RedisSourceConfigDef base) {
		super(base);
		defineSourceConfig();
	}

	private void defineSourceConfig() {
		define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Range.atLeast(1), Importance.LOW, BATCH_SIZE_DOC);
	}

}
