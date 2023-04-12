package com.redis.kafka.connect.common;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide version information.
 */
public class VersionProvider {

	private static final Logger log = LoggerFactory.getLogger(VersionProvider.class);

	private static final Properties properties = new Properties();
	
	private VersionProvider() {
		
	}

	static {
		try {
			properties
					.load(VersionProvider.class.getClassLoader().getResourceAsStream("redis-kafka-connect.properties"));
		} catch (IOException exception) {
			log.error("Failed to load properties", exception);
		}
	}

	/**
	 * Get version.
	 *
	 * @return package version
	 */
	public static String getVersion() {
		return properties.getProperty("version", "0.0.0");
	}
}