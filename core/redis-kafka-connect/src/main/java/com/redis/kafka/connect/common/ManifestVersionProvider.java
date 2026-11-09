package com.redis.kafka.connect.common;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Returns version information from the jar file's {@code /META-INF/MANIFEST.MF}
 */
public class ManifestVersionProvider {

	/** Creates a manifest version provider. */
	public ManifestVersionProvider() {
	}

	private static final Logger log = LoggerFactory.getLogger(ManifestVersionProvider.class);

	private static final String IMPLEMENTATION_TITLE = "Implementation-Title";
	private static final String IMPLEMENTATION_VERSION = "Implementation-Version";
	private static final String IMPLEMENTATION_TITLE_VALUE = "redis-kafka-connect";
	private static final String UNKNOWN_VERSION = "N/A";
	private static final String MANIFEST = "META-INF/MANIFEST.MF";

	/**
	 * Reads the connector version from its manifest.
	 *
	 * @return implementation version, or {@code N/A} if no matching manifest can be read
	 */
	public static String getVersion() {
		try {
			Enumeration<URL> resources = ManifestVersionProvider.class.getClassLoader().getResources(MANIFEST);
			while (resources.hasMoreElements()) {
				Optional<String> version = version(resources.nextElement());
				if (version.isPresent()) {
					return version.get();
				}
			}
		} catch (Exception e) {
			log.error("Could not get version from MANIFEST.MF", e);
		}
		return UNKNOWN_VERSION;
	}

	private static Optional<String> version(URL url) throws IOException {
		Manifest manifest = new Manifest(url.openStream());
		if (isApplicableManifest(manifest)) {
			Attributes attr = manifest.getMainAttributes();
			return Optional.of(String.valueOf(get(attr, IMPLEMENTATION_VERSION)));
		}
		return Optional.empty();
	}

	private static boolean isApplicableManifest(Manifest manifest) {
		Attributes attributes = manifest.getMainAttributes();
		return IMPLEMENTATION_TITLE_VALUE.equals(get(attributes, IMPLEMENTATION_TITLE));
	}

	private static Object get(Attributes attributes, String key) {
		return attributes.get(new Attributes.Name(key));
	}
}