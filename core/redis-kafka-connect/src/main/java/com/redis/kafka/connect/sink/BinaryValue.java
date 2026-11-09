package com.redis.kafka.connect.sink;

import java.nio.ByteBuffer;

/**
 * Reads a Connect {@code BYTES} value, which may be a byte array or a {@link ByteBuffer}.
 */
final class BinaryValue {

    private BinaryValue() {
    }

    /**
     * Returns the bytes of a binary value.
     *
     * @return a byte array as is, the remaining bytes of a byte buffer (leaving its position
     *         unchanged), or null when the value is not binary
     */
    static byte[] toBytes(Object value) {
        if (value instanceof byte[] binary) {
            return binary;
        }
        if (value instanceof ByteBuffer buffer) {
            byte[] binary = new byte[buffer.remaining()];
            buffer.duplicate().get(binary);
            return binary;
        }
        return null;
    }
}
