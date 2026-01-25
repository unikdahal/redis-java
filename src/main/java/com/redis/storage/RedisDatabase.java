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
        final long expiryMillis; // Long.MAX_VALUE means no expiry

        ValueEntry(RedisValue value, long expiryMillis) {
            this.value = value;
            this.expiryMillis = expiryMillis;
        }
    }

    private final ConcurrentHashMap<String, ValueEntry> map = new ConcurrentHashMap<>();
    private final ExpiryManager expiryManager;

    private RedisDatabase() {
        this.expiryManager = new ExpiryManager(this::removeKey);
    }

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
     * Store a RedisValue without expiry.
     */
    public void put(String key, RedisValue value) {
        map.put(key, new ValueEntry(value, Long.MAX_VALUE));
    }

    /**
     * Store a RedisValue with a time-to-live in milliseconds.
     */
    public void put(String key, RedisValue value, long ttlMillis) {
        if (ttlMillis <= 0) {
            put(key, value);
            return;
        }

        long expiryTimeMillis = System.currentTimeMillis() + ttlMillis;
        map.put(key, new ValueEntry(value, expiryTimeMillis));
        expiryManager.scheduleExpiry(key, expiryTimeMillis);
    }

    /**
     * Retrieve a RedisValue. Returns null if key doesn't exist or has expired.
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
     * Get value with expected type. Returns null if key doesn't exist, expired, or type mismatch.
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
     * Get the type of a key's value. Returns null if key doesn't exist.
     */
    public RedisValue.Type getType(String key) {
        RedisValue value = getValue(key);
        return value != null ? value.getType() : null;
    }

    // ==================== String Convenience Methods (Backward Compatible) ====================

    /**
     * Store a string value without expiry.
     * Convenience method for STRING type.
     */
    public void put(String key, String value) {
        put(key, RedisValue.string(value));
    }

    /**
     * Store a string value with TTL.
     * Convenience method for STRING type.
     */
    public void put(String key, String value, long ttlMillis) {
        put(key, RedisValue.string(value), ttlMillis);
    }

    /**
     * Retrieve a string value. Returns null if key doesn't exist, expired, or not a STRING.
     * Convenience method for STRING type.
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
     * Check if a key exists (not expired).
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
     * Remove a key from the database.
     */
    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    /**
     * Remove multiple keys and return the count of removed keys.
     */
    public int removeAll(Collection<String> keys) {
        int count = 0;
        for (String k : keys) {
            if (remove(k)) count++;
        }
        return count;
    }

    // ==================== Utility Methods ====================

    private boolean isExpired(ValueEntry entry) {
        return entry.expiryMillis != Long.MAX_VALUE &&
               entry.expiryMillis <= System.currentTimeMillis();
    }

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
