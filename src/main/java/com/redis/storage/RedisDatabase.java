package com.redis.storage;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /**
     * Get the absolute expiry time in milliseconds for a key.
     * Returns -1 if key doesn't exist.
     * Returns Long.MAX_VALUE if key exists but has no expiry.
     */
    public long getExpiryTime(String key) {
        var entry = map.get(key);
        if (entry == null) return -1;
        if (isExpired(entry)) {
            map.remove(key, entry);
            return -1;
        }
        return entry.expiryMillis;
    }

    /**
     * Set a new expiry time for an existing key.
     * Returns true if successful, false if key doesn't exist.
     */
    public boolean setExpiryTime(String key, long expiryTimeMillis) {
        AtomicBoolean updated = new AtomicBoolean(false);
        map.computeIfPresent(key, (k, existing) -> {
            if (isExpired(existing)) return null;
            updated.set(true);
            if (expiryTimeMillis == Long.MAX_VALUE) {
                expiryManager.clearExpiry(key);
            } else {
                expiryManager.scheduleExpiry(key, expiryTimeMillis);
            }
            return new ValueEntry(existing.value, expiryTimeMillis);
        });
        return updated.get();
    }

    // ==================== Generic RedisValue Methods ====================

    /**
     * Store a RedisValue without expiry.
     */
    public void put(String key, RedisValue value) {
        map.put(key, new ValueEntry(value, Long.MAX_VALUE));
        expiryManager.clearExpiry(key);
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
        boolean removed = map.remove(key) != null;
        if (removed) {
            expiryManager.clearExpiry(key);
        }
        return removed;
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

    // ==================== Atomic Operations ====================

    /**
     * Atomically compute a new value for a key.
     * Thread-safe read-modify-write operation using ConcurrentHashMap.compute().
     * Preserves TTL for existing non-expired keys.
     *
     * @param key the key to compute
     * @param remappingFunction function that takes existing RedisValue (or null) and returns new value
     */
    public void compute(String key, java.util.function.Function<RedisValue, RedisValue> remappingFunction) {
        map.compute(key, (k, existingEntry) -> {
            // Check if entry exists and is not expired
            boolean validEntry = existingEntry != null && !isExpired(existingEntry);
            RedisValue currentValue = validEntry ? existingEntry.value : null;

            // Apply remapping function
            RedisValue newValue = remappingFunction.apply(currentValue);

            // If function returns null, remove the key
            if (newValue == null) {
                return null;
            }

            // Micro-optimization: avoid new ValueEntry if value hasn't changed
            if (newValue == currentValue && validEntry) {
                return existingEntry;
            }

            // Preserve expiry for existing non-expired entries, otherwise no expiry
            long expiry = validEntry ? existingEntry.expiryMillis : Long.MAX_VALUE;

            return new ValueEntry(newValue, expiry);
        });
    }
}
