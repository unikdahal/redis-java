package com.redis.storage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory key-value store with expiry support using DelayQueue-based cleanup.
 * This is the core data storage for the Redis server.
 * Values are stored as String. Expiry is managed by ExpiryManager (zero-polling approach).
 */
public class RedisDatabase {
    private static RedisDatabase INSTANCE;

    private static class ValueEntry {
        final String value;
        final long expiryMillis; // Long.MAX_VALUE means no expiry

        ValueEntry(String value, long expiryMillis) {
            this.value = value;
            this.expiryMillis = expiryMillis;
        }
    }

    private final ConcurrentHashMap<String, ValueEntry> map = new ConcurrentHashMap<>();
    private final ExpiryManager expiryManager;

    private RedisDatabase() {
        // Initialize expiry manager with callback to remove expired keys
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
     * Store a key-value pair without expiry.
     */
    public void put(String key, String value) {
        map.put(key, new ValueEntry(value, Long.MAX_VALUE));
    }

    /**
     * Store a key-value pair with a time-to-live in milliseconds.
     * The key will automatically be removed when the TTL expires.
     */
    public void put(String key, String value, long ttlMillis) {
        if (ttlMillis <= 0) {
            // Invalid TTL, treat as no expiry
            put(key, value);
            return;
        }

        long expiryTimeMillis = System.currentTimeMillis() + ttlMillis;
        map.put(key, new ValueEntry(value, expiryTimeMillis));

        // Schedule expiry with DelayQueue (zero-polling approach)
        expiryManager.scheduleExpiry(key, expiryTimeMillis);
    }

    /**
     * Retrieve a key's value. Returns null if key doesn't exist or has expired.
     * Lazy expiry: checks expiry time on access.
     */
    public String get(String key) {
        var entry = map.get(key);
        if (entry == null) {
            return null;
        }

        // Check if expired
        if (entry.expiryMillis != Long.MAX_VALUE && entry.expiryMillis <= System.currentTimeMillis()) {
            map.remove(key, entry);
            return null;
        }

        return entry.value;
    }

    /**
     * Remove a key from the database.
     */
    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    /**
     * Internal method called by ExpiryManager when a key expires.
     */
    private void removeKey(String key) {
        map.remove(key);
        System.out.println("[RedisDatabase] Expired key: " + key);
    }

    /**
     * Remove multiple keys and return the count of removed keys.
     */
    public int removeAll(java.util.Collection<String> keys) {
        int count = 0;
        for (String k : keys) {
            if (remove(k)) count++;
        }
        return count;
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
