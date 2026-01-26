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

    /**
     * Provide the lazily initialized, thread-safe singleton instance of RedisDatabase.
     *
     * @return the singleton RedisDatabase instance
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

    /**
     * Retrieve the absolute expiry time (epoch milliseconds) for the given key.
     *
     * @param key the key to query
     * @return -1 if the key does not exist or is expired, Long.MAX_VALUE if the key exists without expiry, otherwise the absolute expiry time in milliseconds
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
     * Update the absolute expiry time for an existing key.
     *
     * If `expiryTimeMillis` is `Long.MAX_VALUE` the key's expiry is cleared (made persistent).
     * If the key does not exist or is already expired the entry is removed and no update is performed.
     *
     * @param key the key whose expiry to update
     * @param expiryTimeMillis the new absolute expiry time in milliseconds since epoch, or `Long.MAX_VALUE` to remove expiry
     * @return `true` if an existing non-expired key's expiry was updated, `false` if the key did not exist or was expired
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
         * Store a RedisValue under the given key with no expiry.
         *
         * Any existing expiry associated with the key is cleared.
         *
         * @param key   the key to store the value under
         * @param value the value to store
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
     * Remove the mapping for the given key and cancel any scheduled expiry.
     *
     * @param key the key to remove
     * @return `true` if a mapping was removed, `false` otherwise
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
     * Compute and install a new value for a key using the provided remapping function, preserving TTL for unexpired entries.
     *
     * The remapping function is invoked with the current value for the key, or `null` if the key is absent or expired.
     * If the function returns `null`, the key is removed. If it returns a non-null value, that value is stored;
     * an existing unexpired entry's expiry is preserved, otherwise the new entry has no expiry.
     * The operation is performed atomically.
     *
     * @param key the key to compute
     * @param remappingFunction function that receives the current `RedisValue` (or `null`) and returns the new `RedisValue`, or `null` to remove the key
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