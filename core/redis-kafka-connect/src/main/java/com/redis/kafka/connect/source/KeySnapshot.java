package com.redis.kafka.connect.source;

/**
 * Best-effort key data read with native commands.
 *
 * @param key Redis key name
 * @param ttl expiry as an epoch millisecond timestamp, or a Redis negative TTL sentinel
 * @param type Redis type name, including the module type name for JSON
 * @param value decoded value, or null when no value was read
 */
public record KeySnapshot(String key, Long ttl, String type, Object value) {
    /** Redis type name for hash values. */
    public static final String TYPE_HASH = "hash";
    /** Redis type name for json values. */
    public static final String TYPE_JSON = "ReJSON-RL";
    /** Redis type name for list values. */
    public static final String TYPE_LIST = "list";
    /** Redis type name for set values. */
    public static final String TYPE_SET = "set";
    /** Redis type name for string values. */
    public static final String TYPE_STRING = "string";
    /** Redis type name for sorted set values. */
    public static final String TYPE_ZSET = "zset";
}
