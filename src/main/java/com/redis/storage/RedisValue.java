package com.redis.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    /**
     * Create a RedisValue with the specified type and associated raw data.
     *
     * @param type the stored value's {@link Type}
     * @param data the raw data associated with the type (e.g. `String`, `List<String>`, `Set<String>`, or `Map<String,String>`)
     */
    private RedisValue(Type type, Object data) {
        this.type = type;
        this.data = data;
    }

    /**
     * The Redis data type tag associated with this value.
     *
     * @return the {@code Type} enum constant that identifies the stored Redis data type
     */
    public Type getType() {
        return type;
    }

    /**
     * Underlying raw data stored in this RedisValue.
     * Package-private to prevent external callers from breaking immutability guarantees.
     *
     * @return the raw data object; its concrete type corresponds to the instance's {@code Type}
     */
    Object getData() {
        return data;
    }

    // ==================== Factory Methods ====================

    /**
     * Create a RedisValue representing a Redis string.
     *
     * @param value the string content to store
     * @return a RedisValue of type STRING containing the provided string
     */
    public static RedisValue string(String value) {
        return new RedisValue(Type.STRING, value);
    }

    /**
     * Creates a RedisValue representing a Redis LIST.
     * The input list is defensively copied to ensure immutability.
     *
     * @param value the list of strings to store as the value
     * @return the RedisValue typed as LIST containing a copy of the provided list
     */
    public static RedisValue list(List<String> value) {
        return new RedisValue(Type.LIST, new ArrayList<>(value));
    }

    /**
     * Creates a RedisValue representing a Redis SET.
     * The input set is defensively copied to ensure immutability.
     *
     * @param value the set of string members to store
     * @return a RedisValue with type SET that wraps a copy of the provided set
     */
    public static RedisValue set(Set<String> value) {
        return new RedisValue(Type.SET, new HashSet<>(value));
    }

    /**
     * Create a RedisValue that represents a Redis hash from the provided mapping.
     * The input map is defensively copied to ensure immutability.
     *
     * @param value mapping of hash fields to their string values
     * @return a RedisValue of type HASH containing a copy of the provided map
     */
    public static RedisValue hash(Map<String, String> value) {
        return new RedisValue(Type.HASH, new HashMap<>(value));
    }

    // ==================== Type-Safe Accessors ====================

    /**
     * Retrieve the stored value as a String.
     *
     * @return the value cast to a `String`
     * @throws IllegalStateException if the stored type is not STRING
     */
    public String asString() {
        if (type != Type.STRING) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected STRING, got " + type);
        }
        return (String) data;
    }

    /**
     * Return the underlying value as a {@code List<String>}.
     * Returns an unmodifiable view to preserve immutability.
     *
     * @return an unmodifiable view of the stored {@code List<String>}
     * @throws IllegalStateException if the stored type is not {@code Type.LIST}
     */
    @SuppressWarnings("unchecked")
    public List<String> asList() {
        if (type != Type.LIST) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected LIST, got " + type);
        }
        return Collections.unmodifiableList((List<String>) data);
    }

    /**
     * Return the stored value as a set of strings.
     * Returns an unmodifiable view to preserve immutability.
     *
     * @return an unmodifiable view of the stored value as a {@code Set<String>}
     * @throws IllegalStateException if the stored type is not {@code Type.SET}
     */
    @SuppressWarnings("unchecked")
    public Set<String> asSet() {
        if (type != Type.SET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected SET, got " + type);
        }
        return Collections.unmodifiableSet((Set<String>) data);
    }

    /**
     * Return the stored value as a Map representing a Redis hash.
     * Returns an unmodifiable view to preserve immutability.
     *
     * @return an unmodifiable view of the underlying data as a Map<String, String>
     * @throws IllegalStateException if the stored type is not {@code HASH}
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> asHash() {
        if (type != Type.HASH) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected HASH, got " + type);
        }
        return Collections.unmodifiableMap((Map<String, String>) data);
    }

    /**
     * Determines whether this value has the specified Redis type.
     *
     * @param expectedType the type to compare against
     * @return {@code true} if the stored type equals {@code expectedType}, {@code false} otherwise
     */
    public boolean isType(Type expectedType) {
        return this.type == expectedType;
    }

    /**
     * String representation of the RedisValue including its type and data.
     *
     * @return a string in the form {@code RedisValue{type=..., data=...}}
     */
    @Override
    public String toString() {
        return "RedisValue{type=" + type + ", data=" + data + "}";
    }

    /**
     * Determines whether the given object is equal to this RedisValue.
     *
     * Compares both the stored Type and the underlying data for value-based equality.
     *
     * @param o the object to compare with
     * @return `true` if `o` is a `RedisValue` with the same `Type` and equal data, `false` otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisValue that = (RedisValue) o;
        return type == that.type && java.util.Objects.equals(data, that.data);
    }

    /**
     * Computes a hash code for this RedisValue based on its type and data.
     *
     * @return the hash code derived from the value's type and data
     */
    @Override
    public int hashCode() {
        return java.util.Objects.hash(type, data);
    }
}