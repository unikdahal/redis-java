package com.redis.storage;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory key-value store with expiry support using DelayQueue-based cleanup.
 * This is the core data storage for the Redis server.
 *
 * Supports multiple value types via RedisValue wrapper:
 * - STRING: Simple string values
 * - LIST: Ordered list of strings
 * - SET: Unordered collection of unique strings
 * - HASH: Map of field-value pairs
 * - SORTED_SET: Set with scores for ordering
 *
 * Expiry is managed by ExpiryManager (zero-polling approach with DelayQueue).
 */
public class RedisDatabase {
    private static RedisDatabase INSTANCE;

    /**
     * Internal entry that wraps a RedisValue with optional expiry time.
     */
    private static class ValueEntry {
        final RedisValue value;
        final long expiryMillis; /**
         * Creates a ValueEntry that wraps the given RedisValue and associates it with an expiry timestamp.
         *
         * @param value the RedisValue to store
         * @param expiryMillis the absolute expiry time in milliseconds since the epoch; use {@code Long.MAX_VALUE} to indicate no expiry
         */

        ValueEntry(RedisValue value, long expiryMillis) {
            this.value = value;
            this.expiryMillis = expiryMillis;
        }
    }

    private final ConcurrentHashMap<String, ValueEntry> map = new ConcurrentHashMap<>();
    private final ExpiryManager expiryManager;

    /**
     * Create a RedisDatabase instance and initialize its expiry manager.
     *
     * Initializes the ExpiryManager with a callback to {@link #removeKey(String)} so expired entries are removed
     * from the in-memory store when their TTL elapses.
     */
    private RedisDatabase() {
        this.expiryManager = new ExpiryManager(this::removeKey);
    }

    /**
     * Return the singleton RedisDatabase instance, initializing it if necessary.
     *
     * @return the single, thread-safe RedisDatabase instance
     */
    public static RedisDatabase getInstance() {
        if (INSTANCE == null) {
            synchronized (RedisDatabase.class) {
                if (INSTANCE == null) {
                    INSTANCE = new RedisDatabase();
                }
            }
        }
        return INSTANCE;
    }

    // ==================== Generic RedisValue Methods ====================

    /**
     * Store the given RedisValue under the specified key with no expiry.
     *
     * This overwrites any existing value for the key.
     *
     * @param key   the key under which to store the value
     * @param value the RedisValue to store
     * @throws IllegalArgumentException if value is null
     */
    public void put(String key, RedisValue value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        map.put(key, new ValueEntry(value, Long.MAX_VALUE));
    }

    /**
     * Store a RedisValue under the given key with a time-to-live.
     *
     * @param key the key under which to store the value
     * @param value the RedisValue to store
     * @param ttlMillis time-to-live in milliseconds; if less than or equal to zero the value is stored without expiry
     * @throws IllegalArgumentException if value is null
     */
    public void put(String key, RedisValue value, long ttlMillis) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (ttlMillis <= 0) {
            put(key, value);
            return;
        }

        long expiryTimeMillis = System.currentTimeMillis() + ttlMillis;
        map.put(key, new ValueEntry(value, expiryTimeMillis));
        expiryManager.scheduleExpiry(key, expiryTimeMillis);
    }

    /**
     * Retrieve the stored RedisValue for the given key.
     *
     * If the key is missing or its entry has expired, the method returns null and any expired entry is removed.
     *
     * @param key the key to look up
     * @return the stored RedisValue for the key, or `null` if the key does not exist or has expired
     */
    public RedisValue getValue(String key) {
        var entry = map.get(key);
        if (entry == null) return null;
        if (isExpired(entry)) {
            map.remove(key, entry);
            return null;
        }
        return entry.value;
    }

    /**
     * Retrieve the stored value for the given key if it exists, is not expired, and matches the expected type.
     *
     * @param key the key to look up
     * @param expectedType the expected RedisValue.Type to match
     * @param <T> the expected data type returned
     * @return the stored value cast to the requested type, or {@code null} if the key is missing, expired, or the stored value's type does not match {@code expectedType}
     */
    @SuppressWarnings("unchecked")
    public <T> T getTyped(String key, RedisValue.Type expectedType) {
        RedisValue value = getValue(key);
        if (value == null || value.getType() != expectedType) {
            return null;
        }
        return (T) value.getData();
    }

    /**
     * Get the stored RedisValue type for the given key.
     *
     * @return the RedisValue.Type for the key, or `null` if the key does not exist or has expired.
     */
    public RedisValue.Type getType(String key) {
        RedisValue value = getValue(key);
        return value != null ? value.getType() : null;
    }

    // ==================== String Convenience Methods (Backward Compatible) ====================

    /**
     * Store a string value with no expiry.
     *
     * Stores the given string under the specified key as a STRING-type value and does not set a TTL.
     *
     * @param key   the key under which to store the value
     * @param value the string value to store
     */
    public void put(String key, String value) {
        put(key, RedisValue.string(value));
    }

    /**
     * Store a string value under the given key with a specified time-to-live.
     *
     * @param key       the key under which to store the value
     * @param value     the string value to store
     * @param ttlMillis time-to-live in milliseconds; if less than or equal to zero the value is stored without expiry
     */
    public void put(String key, String value, long ttlMillis) {
        put(key, RedisValue.string(value), ttlMillis);
    }

    /**
     * Retrieve the STRING value associated with the given key.
     *
     * @param key the key to look up
     * @return the string stored at the key, or `null` if the key does not exist, has expired, or is not a STRING
     */
    public String get(String key) {
        RedisValue value = getValue(key);
        if (value == null || value.getType() != RedisValue.Type.STRING) {
            return null;
        }
        return value.asString();
    }

    // ==================== Key Operations ====================

    /**
     * Determines whether the given key exists and is not expired.
     *
     * @param key the key to check
     * @return {@code true} if the key exists and is not expired, {@code false} otherwise
     */
    public boolean exists(String key) {
        var entry = map.get(key);
        if (entry == null) return false;
        if (isExpired(entry)) {
            map.remove(key, entry);
            return false;
        }
        return true;
    }

    /**
     * Removes the mapping for the specified key from the database.
     *
     * @param key the key to remove
     * @return `true` if a value was removed, `false` otherwise
     */
    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    /**
     * Removes each key in the given collection and returns the count of keys that were removed.
     *
     * @param keys collection of keys to remove
     * @return the number of keys successfully removed
     */
    public int removeAll(Collection<String> keys) {
        int count = 0;
        for (String k : keys) {
            if (remove(k)) count++;
        }
        return count;
    }

    /**
     * Check whether the given stored entry has passed its expiry time.
     *
     * @param entry the ValueEntry to evaluate (expiryMillis == Long.MAX_VALUE means no expiry)
     * @return `true` if the entry has an expiry set and that expiry time is less than or equal to the current time, `false` otherwise
     */

    private boolean isExpired(ValueEntry entry) {
        return entry.expiryMillis != Long.MAX_VALUE &&
               entry.expiryMillis <= System.currentTimeMillis();
    }

    /**
     * Removes the entry associated with the given key from the internal store.
     *
     * @param key the key to remove; no effect if the key is not present
     */
    private void removeKey(String key) {
        map.remove(key);
    }

    /**
     * Get the number of keys in the database.
     */
    public int size() {
        return map.size();
    }

    /**
     * Gracefully shutdown the database and its expiry manager.
     */
    public void shutdown() {
        expiryManager.shutdown();
    }
}