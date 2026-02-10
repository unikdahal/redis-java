package com.redis.storage;

import com.redis.util.StreamId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The unified container for all Redis data types.
 * <p>
 * <b>Architecture: Sealed Interface + Records</b>
 * This design uses Java's modern type system to strictly define what a "Value" can be.
 * By sealing the interface, we guarantee that the compiler knows exactly which 6 types exist,
 * enabling exhaustive pattern matching and preventing invalid data types from entering the system.
 * <p>
 * <b>Thread Safety:</b>
 * While the 'Record' wrapper is immutable (you can't swap the underlying List reference),
 * the collections inside (ArrayList, HashMap) are wrapped in thread-safe implementations
 * (ConcurrentHashMap, SynchronizedList) to support concurrent access.
 */
public sealed interface RedisValue permits RedisValue.StringValue, RedisValue.ListValue, RedisValue.SetValue, RedisValue.HashValue, RedisValue.SortedSetValue, RedisValue.StreamValue, RedisValue.ExpiringValue {

    /**
     * Enumeration of supported Redis data types.
     * Used for the "TYPE" command and error reporting.
     */
    enum Type {
        STRING, LIST, SET, HASH, SORTED_SET, STREAM
    }

    /**
     * @return The runtime type of the stored value.
     */
    Type getType();

    /**
     * @return The raw underlying Java object (e.g., String, List, Map).
     * Useful for generic serialization or debugging.
     */
    Object getData();

    // ==================== Factory Methods ====================
    // These static factories provide a clean API to create values without exposing
    // the specific Record constructors directly.

    static RedisValue string(String value) {
        return new StringValue(value);
    }

    static RedisValue list(List<String> value) {
        return new ListValue(value);
    }

    static RedisValue set(Set<String> value) {
        return new SetValue(value);
    }

    static RedisValue hash(Map<String, String> value) {
        return new HashValue(value);
    }

    static RedisValue sortedSet(Map<String, Double> value) {
        return new SortedSetValue(value);
    }

    static RedisValue stream(Map<StreamId, Map<String, String>> value) {
        return new StreamValue(value);
    }

    // ==================== Type-Safe Accessors ====================
    // These methods implement the strict "WRONGTYPE" checking required by the Redis Protocol.
    // If a user tries to run a List command on a String value, we must throw an exception.

    /**
     * Extracts the String value.
     * Uses Java Pattern Matching to simultaneously check type and cast.
     *
     * @throws IllegalStateException if the value is not a String.
     */
    default String asString() {
        if (this instanceof StringValue(String value)) {
            return value;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected STRING, got " + getType());
    }

    /**
     * Extracts the List value.
     */
    default List<String> asList() {
        if (this instanceof ListValue(List<String> list)) {
            return list;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected LIST, got " + getType());
    }

    /**
     * Extracts the Set value.
     */
    default Set<String> asSet() {
        if (this instanceof SetValue(Set<String> set)) {
            return set;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected SET, got " + getType());
    }

    /**
     * Extracts the Hash (Map) value.
     */
    default Map<String, String> asHash() {
        if (this instanceof HashValue(Map<String, String> hash)) {
            return hash;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected HASH, got " + getType());
    }

    /**
     * Extracts the Sorted Set (Map: Member -> Score).
     */
    default Map<String, Double> asSortedSet() {
        if (this instanceof SortedSetValue(Map<String, Double> sortedSet)) {
            return sortedSet;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected SORTED_SET, got " + getType());
    }

    /**
     * Extracts the Stream (Map: StreamId -> Entry).
     */
    default Map<StreamId, Map<String, String>> asStream() {
        if (this instanceof StreamValue(Map<StreamId, Map<String, String>> stream)) {
            return stream;
        }
        throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value. Expected STREAM, got " + getType());
    }

    /**
     * Check whether this value's Redis type matches the given type.
     *
     * @param expectedType the Redis type to compare against
     * @return {@code true} if the value's type equals {@code expectedType}, {@code false} otherwise
     */
    default boolean isType(Type expectedType) {
        return getType() == expectedType;
    }

    // ==================== Expiry Support (for Snapshots) ====================

    /**
     * Gets the expiry time associated with this value.
     * <p>
     * This is only set during snapshot operations for RDB serialization.
     * Normal storage expiry is managed by RedisDatabase.
     *
     * @return Expiry timestamp in milliseconds, or null if not set
     */
    default Long getExpiryTime() {
        return null;
    }

    /**
     * Determine whether the value has passed its expiry time.
     *
     * @return `true` if the value has an expiry time and that time is less than or equal to the current system time, `false` otherwise.
     */
    default boolean isExpired() {
        Long expiry = getExpiryTime();
        return expiry != null && expiry <= System.currentTimeMillis();
    }

    /**
     * Creates a copy of this value that carries an expiry timestamp.
     *
     * Used when creating snapshots to preserve expiry metadata.
     *
     * @param expiryTimeMillis the expiry timestamp in milliseconds since the Unix epoch
     * @return a RedisValue that reports the given expiry time and delegates type/data access to this value
     */
    default RedisValue withExpiry(long expiryTimeMillis) {
        return new ExpiringValue(this, expiryTimeMillis);
    }

    // ==================== Implementation Records ====================
    // Records serve as immutable containers for the mutable data structures.
    // The constructor logic ensures that whatever list/map is passed in gets
    // converted to the correct Thread-Safe implementation.

    /**
     * Storage for Redis Strings.
     * Note: Redis Strings are binary safe, but here we use Java String (UTF-16) for simplicity.
     */
    record StringValue(String value) implements RedisValue {
        @Override
        public Type getType() {
            return Type.STRING;
        }

        @Override
        public Object getData() {
            return value;
        }

        @Override
        public String toString() {
            return "RedisValue{type=STRING, data=" + value + "}";
        }
    }

    /**
     * Storage for Redis Lists (Linked Lists).
     * Uses synchronizedList to ensure thread safety for simple operations.
     */
    record ListValue(List<String> list) implements RedisValue {
        public ListValue {
            // Defensiveness: Copy the input to a new ArrayList to detach from original source,
            // then wrap in synchronizedList for thread safety.
            list = Collections.synchronizedList(new ArrayList<>(list));
        }

        @Override
        public Type getType() {
            return Type.LIST;
        }

        @Override
        public Object getData() {
            return list;
        }

        @Override
        public String toString() {
            return "RedisValue{type=LIST, data=" + list + "}";
        }
    }

    /**
     * Storage for Redis Sets (Unordered, Unique).
     * Uses ConcurrentHashMap.newKeySet() which is a thread-safe Set backed by a ConcurrentHashMap.
     */
    record SetValue(Set<String> set) implements RedisValue {
        public SetValue {
            // Defensiveness: Create a fresh thread-safe Set and populate it.
            Set<String> newSet = ConcurrentHashMap.newKeySet();
            newSet.addAll(set);
            set = newSet;
        }

        @Override
        public Type getType() {
            return Type.SET;
        }

        @Override
        public Object getData() {
            return set;
        }

        @Override
        public String toString() {
            return "RedisValue{type=SET, data=" + set + "}";
        }
    }

    /**
     * Storage for Redis Hashes (Field-Value pairs).
     * Uses ConcurrentHashMap for high-concurrency read/write access.
     */
    record HashValue(Map<String, String> hash) implements RedisValue {
        public HashValue {
            hash = new ConcurrentHashMap<>(hash);
        }

        @Override
        public Type getType() {
            return Type.HASH;
        }

        @Override
        public Object getData() {
            return hash;
        }

        @Override
        public String toString() {
            return "RedisValue{type=HASH, data=" + hash + "}";
        }
    }

    /**
     * Storage for Redis Sorted Sets (ZSET).
     * Mapped as Member -> Score.
     * Note: Real Redis ZSETs use a dual structure (SkipList + HashMap).
     * This simple Map<String, Double> implementation is O(N) for range queries but O(1) for lookups.
     */
    record SortedSetValue(Map<String, Double> sortedSet) implements RedisValue {
        public SortedSetValue {
            sortedSet = new ConcurrentHashMap<>(sortedSet);
        }

        @Override
        public Type getType() {
            return Type.SORTED_SET;
        }

        @Override
        public Object getData() {
            return sortedSet;
        }

        @Override
        public String toString() {
            return "RedisValue{type=SORTED_SET, data=" + sortedSet + "}";
        }
    }

    /**
     * Storage for Redis Streams.
     * Uses ConcurrentSkipListMap because Streams require:
     * 1. Ordering (by StreamId)
     * 2. Thread Safety
     * SkipListMap provides O(log n) access and keeps keys sorted naturally.
     */
    record StreamValue(Map<StreamId, Map<String, String>> stream) implements RedisValue {
        public StreamValue {
            // Ensure we use the sorted, thread-safe map implementation
            if (!(stream instanceof ConcurrentSkipListMap)) {
                stream = new ConcurrentSkipListMap<>(stream);
            }
        }

        @Override
        public Type getType() {
            return Type.STREAM;
        }

        @Override
        public Object getData() {
            return stream;
        }

        /**
         * String representation of this RedisValue when it holds a stream.
         *
         * @return a string describing the value's type (`STREAM`) and its underlying stream data
         */
        @Override
        public String toString() {
            return "RedisValue{type=STREAM, data=" + stream + "}";
        }
    }

    /**
     * Wrapper for values with expiry metadata.
     * <p>
     * Used during snapshot operations to preserve TTL information for RDB serialization.
     * This is a transient wrapper, not used for normal storage.
     */
    record ExpiringValue(RedisValue wrapped, long expiryTimeMillis) implements RedisValue {
        /**
         * Get the runtime RedisValue.Type of the wrapped value.
         *
         * @return the RedisValue.Type of the wrapped value
         */
        @Override
        public Type getType() {
            return wrapped.getType();
        }

        /**
         * Get the underlying Java object used to represent this value for serialization and debugging.
         *
         * @return the wrapped value's underlying data object
         */
        @Override
        public Object getData() {
            return wrapped.getData();
        }

        /**
         * The expiry time in milliseconds since the Unix epoch, or null when no expiry is set.
         *
         * @return the expiry time in milliseconds since epoch, or {@code null} if not set
         */
        @Override
        public Long getExpiryTime() {
            return expiryTimeMillis;
        }

        /**
         * Checks whether the stored expiry time has been reached.
         *
         * @return `true` if the current system time is greater than or equal to the expiry time, `false` otherwise
         */
        @Override
        public boolean isExpired() {
            return expiryTimeMillis <= System.currentTimeMillis();
        }

        /**
         * Create a new RedisValue wrapper that associates the same wrapped value with a different expiry timestamp.
         *
         * @param newExpiryTimeMillis the expiry time in milliseconds since the epoch
         * @return a new RedisValue whose wrapped value is the same as this instance's wrapped value and whose expiry time is `newExpiryTimeMillis`
         */
        @Override
        public RedisValue withExpiry(long newExpiryTimeMillis) {
            return new ExpiringValue(wrapped, newExpiryTimeMillis);
        }

        /**
         * Retrieve the stored string value from the wrapped RedisValue.
         *
         * @return the wrapped value's string
         * @throws IllegalStateException if the wrapped value is not of type STRING
         */
        @Override
        public String asString() {
            return wrapped.asString();
        }

        /**
         * Exposes the wrapped value as a list view.
         *
         * @return the underlying value as a List<String>
         * @throws IllegalStateException if the wrapped value is not a list (WRONGTYPE)
         */
        @Override
        public List<String> asList() {
            return wrapped.asList();
        }

        /**
         * Provides the set view of this value.
         *
         * @return the set of strings represented by this value
         */
        @Override
        public Set<String> asSet() {
            return wrapped.asSet();
        }

        /**
         * Return the wrapped value as a hash map.
         *
         * @return the underlying map of field to value for the wrapped hash value
         * @throws IllegalStateException if the wrapped value is not of type HASH
         */
        @Override
        public Map<String, String> asHash() {
            return wrapped.asHash();
        }

        /**
         * Get the wrapped value as a sorted-set view.
         *
         * @return a map of members to their scores
         * @throws IllegalStateException if the value's type is not SORTED_SET
         */
        @Override
        public Map<String, Double> asSortedSet() {
            return wrapped.asSortedSet();
        }

        /**
         * Provides the stream entries indexed by StreamId for this value.
         *
         * @return a map from each `StreamId` to the entry's field-value map (`Map<String, String>`).
         */
        @Override
        public Map<StreamId, Map<String, String>> asStream() {
            return wrapped.asStream();
        }

        /**
         * Provide a string representation of the expiring RedisValue that includes its runtime type, expiry timestamp, and wrapped data.
         *
         * @return a string containing the value's runtime type, the expiryTimeMillis, and the wrapped data
         */
        @Override
        public String toString() {
            return "RedisValue{type=" + getType() + ", expiry=" + expiryTimeMillis + ", data=" + wrapped.getData() + "}";
        }
    }
};