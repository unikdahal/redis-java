package com.redis.storage;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Central In-Memory Storage Engine.
 * <p>
 * <b>Role:</b> This singleton acts as the "Heap" for the Redis server. It manages the lifecycle
 * of all keys, including storage, retrieval, and expiration.
 * <p>
 * <b>Concurrency Model:</b>
 * Uses {@link ConcurrentHashMap} for high-throughput, thread-safe storage.
 * Reads are generally lock-free. Writes are locked per-bucket.
 * Atomic operations (like 'compute') allow for safe read-modify-write cycles needed for commands like INCR or APPEND.
 * <p>
 * <b>Expiration Strategy (Hybrid):</b>
 * 1. <b>Lazy (Passive):</b> Every read operation (get, exists) checks if the key is expired. If yes, it's deleted immediately.
 * 2. <b>Active:</b> The {@link ExpiryManager} (background component) removes keys when their TTL hits zero.
 */
public class RedisDatabase {

    // Singleton Instance
    private static volatile RedisDatabase INSTANCE;

    /**
     * Internal wrapper. Bundles the data payload with its metadata (expiry).
     * <p>
     * <b>Why a Record?</b> Immutable data carrier.
     * <b>Why absolute time?</b> Storing 'expiryMillis' (Epoch) is cheaper to check than 'ttl' (Duration).
     * Just compare {@code entry.expiry < System.currentTimeMillis()}.
     */
    private record ValueEntry(RedisValue value, long expiryMillis) { }

    // The core storage map.
    // Key = Redis Key (String)
    // Value = Wrapper containing Data + Expiry
    private final ConcurrentHashMap<String, ValueEntry> map = new ConcurrentHashMap<>();

    // Helper component to handle background cleanup of expired keys.
    private final ExpiryManager expiryManager;

    private RedisDatabase() {
        // Initialize the manager with a callback to remove keys from this map
        this.expiryManager = new ExpiryManager(this::removeKey);
    }

    /**
     * Holder Class Pattern
    * */
    public static class RedisDatabaseHolder {
        private static final RedisDatabase INSTANCE = new RedisDatabase();
    }

    /**
     * Singleton accessor.
     * @return the singleton instance
     */
    public static RedisDatabase getInstance() {
        return RedisDatabaseHolder.INSTANCE;
    }

    // ==================== Expiry Management ====================

    /**
     * checks the Time-To-Live (TTL) of a key.
     * <p>
     * <b>Lazy Expiration Logic:</b>
     * If we find the key is expired during this check, we delete it immediately.
     * This prevents the user from seeing "ghost" keys that exist but are technically dead.
     *
     * @return Absolute epoch time in ms, or -1 if no expiry/not exists.
     */
    public long getExpiryTime(String key) {
        var entry = map.get(key);
        if (entry == null) return -1;

        // LAZY CHECK: Is it dead?
        if (isExpired(entry)) {
            map.remove(key, entry); // Clean up trash
            return -1;
        }
        return entry.expiryMillis();
    }

    /**
     * Updates the expiry of an existing key.
     * <p>
     * <b>Atomicity:</b> Uses `computeIfPresent` to ensure we don't resurrect a key
     * that was deleted by another thread milliseconds ago.
     * * @param expiryTimeMillis Absolute Epoch time. Long.MAX_VALUE means "Persistent" (No expiry).
     */
    public boolean setExpiryTime(String key, long expiryTimeMillis) {
        AtomicBoolean updated = new AtomicBoolean(false);

        map.computeIfPresent(key, (k, existing) -> {
            // Edge case: It expired just before we got the lock
            if (isExpired(existing)) return null;

            updated.set(true);

            // Notify the background manager
            if (expiryTimeMillis == Long.MAX_VALUE) {
                expiryManager.clearExpiry(key); // Remove from delay queue
            } else {
                expiryManager.scheduleExpiry(key, expiryTimeMillis); // Add/Update delay queue
            }

            // Return a new entry with updated time but same value
            return new ValueEntry(existing.value(), expiryTimeMillis);
        });

        return updated.get();
    }

    // ==================== Storage Operations ====================

    /**
     * Basic "SET" operation.
     * Clears any previous expiry because a generic SET removes the TTL in Redis protocol.
     */
    public void put(String key, RedisValue value) {
        map.put(key, new ValueEntry(value, Long.MAX_VALUE));
        expiryManager.clearExpiry(key);
    }

    /**
     * "SETEX" operation (Set with Expiry).
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
     * Core "GET" operation.
     * <p>
     * <b>Crucial:</b> This is the primary point of Lazy Expiration.
     * Every generic read goes through here.
     */
    public RedisValue getValue(String key) {
        var entry = map.get(key);
        if (entry == null) return null;

        if (isExpired(entry)) {
            map.remove(key, entry); // Lazy Delete
            return null;
        }
        return entry.value();
    }

    /**
     * Type-Safe Retrieval.
     * Used by specific commands (e.g., LPUSH needs a List, not a String).
     *
     * @param expectedType The internal enum type we expect (LIST, HASH, etc.)
     * @return The raw Java object (List, Map) cast to T, or null on mismatch.
     */
    @SuppressWarnings("unchecked")
    public <T> T getTyped(String key, RedisValue.Type expectedType) {
        RedisValue value = getValue(key);
        // Validates existence and type compatibility
        if (value == null || value.getType() != expectedType) {
            return null;
        }
        return (T) value.getData();
    }

    /**
     * Lightweight check for "TYPE" command.
     */
    public RedisValue.Type getType(String key) {
        RedisValue value = getValue(key);
        return value != null ? value.getType() : null;
    }

    // ==================== String Helpers ====================
    // Convenience wrappers to avoid manually creating RedisValue.StringValue every time.

    public void put(String key, String value) {
        put(key, RedisValue.string(value));
    }

    public void put(String key, String value, long ttlMillis) {
        put(key, RedisValue.string(value), ttlMillis);
    }

    public String get(String key) {
        RedisValue value = getValue(key);
        if (value == null || value.getType() != RedisValue.Type.STRING) {
            return null;
        }
        return value.asString();
    }

    // ==================== Key Management ====================

    public boolean exists(String key) {
        var entry = map.get(key);
        if (entry == null) return false;
        if (isExpired(entry)) {
            map.remove(key, entry);
            return false;
        }
        return true;
    }

    public boolean remove(String key) {
        // remove() returns the previous value or null.
        boolean removed = map.remove(key) != null;
        if (removed) {
            expiryManager.clearExpiry(key); // Remember to clean up the background task!
        }
        return removed;
    }

    public int removeAll(Collection<String> keys) {
        int count = 0;
        for (String k : keys) {
            if (remove(k)) count++;
        }
        return count;
    }

    // ==================== Internal Logic ====================

    /**
     * Checks if the entry has passed its expiration timestamp.
     * returns false if expiry is Long.MAX_VALUE (persistent).
     */
    private boolean isExpired(ValueEntry entry) {
        return entry.expiryMillis() != Long.MAX_VALUE &&
                entry.expiryMillis() <= System.currentTimeMillis();
    }

    /**
     * Callback used by ExpiryManager to physically remove the key.
     * Unlike remove(), this doesn't need to call clearExpiry() because
     * it was triggered BY the expiry manager.
     */
    private void removeKey(String key) {
        map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public void shutdown() {
        expiryManager.shutdown();
    }

    // ==================== Atomic Operations ====================

    /**
     * The "Holy Grail" of Thread-Safe Read-Modify-Write.
     * <p>
     * <b>Why use this?</b>
     * If you want to append a string, you cannot do:
     * <pre>
     * val = get(k);
     * put(k, val + "new");
     * </pre>
     * Between the get and put, another thread might have changed the value.
     * <p>
     * <b>How it works:</b>
     * ConcurrentHashMap locks the specific key bucket. It feeds the current value
     * to your function, and atomically updates the map with your result.
     * No other thread can touch this key until the function finishes.
     */
    public void compute(String key, java.util.function.Function<RedisValue, RedisValue> remappingFunction) {
        map.compute(key, (k, existingEntry) -> {
            // 1. Validate existing data (Lazy Expiry check inside the lock)
            boolean validEntry = existingEntry != null && !isExpired(existingEntry);
            RedisValue currentValue = validEntry ? existingEntry.value() : null;

            // 2. Run the user's logic
            RedisValue newValue = remappingFunction.apply(currentValue);

            // 3. Handle deletion
            if (newValue == null) {
                return null; // Signals map.compute to remove the key
            }

            // Optimization: If nothing changed, don't create new objects
            if (newValue == currentValue && validEntry) {
                return existingEntry;
            }

            // 4. Preserve TTL
            // If the key existed, keep its old expiry. If it's new, it has no expiry.
            long expiry = validEntry ? existingEntry.expiryMillis() : Long.MAX_VALUE;

            return new ValueEntry(newValue, expiry);
        });
    }
}