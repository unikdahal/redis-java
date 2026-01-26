package com.redis.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper class for Redis values supporting multiple data types.
 * Using Java 25 sealed interface and records for high scalability and type safety.
 */
public sealed interface RedisValue permits 
    RedisValue.StringValue, 
    RedisValue.ListValue, 
    RedisValue.SetValue, 
    RedisValue.HashValue,
    RedisValue.SortedSetValue {

    /**
     * Redis data types.
     */
    enum Type {
        STRING,
        LIST,
        SET,
        HASH,
        SORTED_SET
    }

    /**
     * Get the type of this value.
     */
    Type getType();

    /**
     * Get the raw data object.
     */
    Object getData();

    // ==================== Factory Methods ====================

    /**
     * Create a STRING value.
     */
    static RedisValue string(String value) {
        return new StringValue(value);
    }

    /**
     * Create a LIST value.
     */
    static RedisValue list(List<String> value) {
        return new ListValue(value);
    }

    /**
     * Create a SET value.
     */
    static RedisValue set(Set<String> value) {
        return new SetValue(value);
    }

    /**
     * Create a HASH value.
     */
    static RedisValue hash(Map<String, String> value) {
        return new HashValue(value);
    }

    /**
     * Create a SORTED_SET value.
     */
    static RedisValue sortedSet(Map<String, Double> value) {
        return new SortedSetValue(value);
    }

    // ==================== Type-Safe Accessors ====================

    /**
     * Get value as String. Throws if type mismatch.
     */
    default String asString() {
        if (this instanceof StringValue(String value)) {
            return value;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected STRING, got " + getType());
    }

    /**
     * Get value as List. Throws if type mismatch.
     */
    default List<String> asList() {
        if (this instanceof ListValue(List<String> list)) {
            return list;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected LIST, got " + getType());
    }

    /**
     * Get value as Set. Throws if type mismatch.
     */
    default Set<String> asSet() {
        if (this instanceof SetValue(Set<String> set)) {
            return set;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected SET, got " + getType());
    }

    /**
     * Get value as Hash (Map). Throws if type mismatch.
     */
    default Map<String, String> asHash() {
        if (this instanceof HashValue(Map<String, String> hash)) {
            return hash;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected HASH, got " + getType());
    }

    /**
     * Get value as Sorted Set (Map). Throws if type mismatch.
     */
    default Map<String, Double> asSortedSet() {
        if (this instanceof SortedSetValue(Map<String, Double> sortedSet)) {
            return sortedSet;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected SORTED_SET, got " + getType());
    }

    /**
     * Check if this value is of the specified type.
     */
    default boolean isType(Type expectedType) {
        return getType() == expectedType;
    }

    // ==================== Implementation Records ====================

    record StringValue(String value) implements RedisValue {
        @Override public Type getType() { return Type.STRING; }
        @Override public Object getData() { return value; }
        @Override public String toString() { return "RedisValue{type=STRING, data=" + value + "}"; }
    }

    record ListValue(List<String> list) implements RedisValue {
        @Override public Type getType() { return Type.LIST; }
        @Override public Object getData() { return list; }
        @Override public String toString() { return "RedisValue{type=LIST, data=" + list + "}"; }
    }

    record SetValue(Set<String> set) implements RedisValue {
        @Override public Type getType() { return Type.SET; }
        @Override public Object getData() { return set; }
        @Override public String toString() { return "RedisValue{type=SET, data=" + set + "}"; }
    }

    record HashValue(Map<String, String> hash) implements RedisValue {
        @Override public Type getType() { return Type.HASH; }
        @Override public Object getData() { return hash; }
        @Override public String toString() { return "RedisValue{type=HASH, data=" + hash + "}"; }
    }

    record SortedSetValue(Map<String, Double> sortedSet) implements RedisValue {
        @Override public Type getType() { return Type.SORTED_SET; }
        @Override public Object getData() { return sortedSet; }
        @Override public String toString() { return "RedisValue{type=SORTED_SET, data=" + sortedSet + "}"; }
    }
}
