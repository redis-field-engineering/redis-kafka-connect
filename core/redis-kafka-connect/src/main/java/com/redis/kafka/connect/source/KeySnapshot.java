package com.redis.kafka.connect.source;

/** Point-in-time key data. Expiry is an epoch millisecond timestamp, or a Redis negative TTL sentinel. */
public record KeySnapshot(String key, Long ttl, String type, Object value) {
    public static final String TYPE_HASH = "hash";
    public static final String TYPE_JSON = "ReJSON-RL";
    public static final String TYPE_LIST = "list";
    public static final String TYPE_SET = "set";
    public static final String TYPE_STRING = "string";
    public static final String TYPE_ZSET = "zset";
}
