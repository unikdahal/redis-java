package com.redis.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper class for Redis values supporting multiple data types.
 * Immutable value container with type information.
 *
 * Supported types:
 * - STRING: Simple string values
 * - LIST: Ordered list of strings (supports LPUSH, RPUSH, LRANGE, etc.)
 * - SET: Unordered collection of unique strings (supports SADD, SMEMBERS, etc.)
 * - HASH: Map of field-value pairs (supports HSET, HGET, HGETALL, etc.)
 * - SORTED_SET: Set with scores for ordering (supports ZADD, ZRANGE, etc.)
 */
public final class RedisValue {

    /**
     * Redis data types.
     */
    public enum Type {
        STRING,
        LIST,
        SET,
        HASH,
        SORTED_SET
    }

    private final Type type;
    private final Object data;

    private RedisValue(Type type, Object data) {
        this.type = type;
        this.data = data;
    }

    /**
     * Get the type of this value.
     */
    public Type getType() {
        return type;
    }

    /**
     * Get the raw data object.
     */
    public Object getData() {
        return data;
    }

    // ==================== Factory Methods ====================

    /**
     * Create a STRING value.
     */
    public static RedisValue string(String value) {
        return new RedisValue(Type.STRING, value);
    }

    /**
     * Create a LIST value.
     */
    public static RedisValue list(List<String> value) {
        return new RedisValue(Type.LIST, value);
    }

    /**
     * Create a SET value.
     */
    public static RedisValue set(Set<String> value) {
        return new RedisValue(Type.SET, value);
    }

    /**
     * Create a HASH value.
     */
    public static RedisValue hash(Map<String, String> value) {
        return new RedisValue(Type.HASH, value);
    }

    // ==================== Type-Safe Accessors ====================

    /**
     * Get value as String. Throws if type mismatch.
     */
    public String asString() {
        if (type != Type.STRING) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected STRING, got " + type);
        }
        return (String) data;
    }

    /**
     * Get value as List. Throws if type mismatch.
     */
    @SuppressWarnings("unchecked")
    public List<String> asList() {
        if (type != Type.LIST) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected LIST, got " + type);
        }
        return (List<String>) data;
    }

    /**
     * Get value as Set. Throws if type mismatch.
     */
    @SuppressWarnings("unchecked")
    public Set<String> asSet() {
        if (type != Type.SET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected SET, got " + type);
        }
        return (Set<String>) data;
    }

    /**
     * Get value as Hash (Map). Throws if type mismatch.
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> asHash() {
        if (type != Type.HASH) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected HASH, got " + type);
        }
        return (Map<String, String>) data;
    }

    /**
     * Check if this value is of the specified type.
     */
    public boolean isType(Type expectedType) {
        return this.type == expectedType;
    }

    @Override
    public String toString() {
        return "RedisValue{type=" + type + ", data=" + data + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisValue that = (RedisValue) o;
        return type == that.type && java.util.Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(type, data);
    }
}
