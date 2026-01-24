package com.redis.storage;

import com.redis.util.ExpiryTask;

import java.util.concurrent.DelayQueue;
import java.util.function.Consumer;

/**
 * Manages key expiration using a DelayQueue-based approach.
 * Instead of polling all keys every N seconds, the cleaner thread waits for
 * the next key to expire and processes only that key at the exact right time.
 * This is vastly more efficient than the previous polling approach.
 */
public class ExpiryManager {
    private final DelayQueue<ExpiryTask> expiryQueue = new DelayQueue<>();
    private final Thread cleanerThread;
    private volatile boolean running = true;

    // For deduplication: track the latest expiry time for each key
    private final java.util.concurrent.ConcurrentHashMap<String, Long> keyExpiryMap = new java.util.concurrent.ConcurrentHashMap<>();

    public ExpiryManager(Consumer<String> onExpire) {
        cleanerThread = new Thread(() -> runExpiryCleaner(onExpire), "redis-expiry-manager");
        cleanerThread.setDaemon(true);
        cleanerThread.start();
    }

    /**
     * Schedule a key for expiration at the given absolute time in milliseconds.
     * If the key is re-set with a new TTL before expiring, this will schedule a new task
     * and the old one will be ignored (deduplication via keyExpiryMap).
     */
    public void scheduleExpiry(String key, long expiryTimeMillis) {
        // Store/update the latest expiry time for this key
        keyExpiryMap.put(key, expiryTimeMillis);
        // Add task to queue (may add duplicates, but deduplication check happens on poll)
        expiryQueue.offer(new ExpiryTask(key, expiryTimeMillis));
    }

    /**
     * Main cleanup loop: waits for the next key to expire, then removes it.
     */
    private void runExpiryCleaner(Consumer<String> onExpire) {
        while (running) {
            try {
                // Take the next task (blocks until a task is ready)
                ExpiryTask task = expiryQueue.take();
                String key = task.getKey();
                long expectedExpiryTime = task.getExpiryTimeMillis();

                // Deduplication: only process if this is still the latest expiry for this key
                Long currentExpiry = keyExpiryMap.get(key);
                if (currentExpiry != null && currentExpiry.equals(expectedExpiryTime)) {
                    // This is the expected expiry time, proceed with removal
                    keyExpiryMap.remove(key);
                    onExpire.accept(key);
                }
                // else: key was re-set with a different TTL, skip this stale task
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Gracefully shutdown the expiry manager.
     */
    public void shutdown() {
        running = false;
        if (cleanerThread != null && cleanerThread.isAlive()) {
            cleanerThread.interrupt();
            try {
                cleanerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int getPendingExpiryCount() {
        return expiryQueue.size();
    }
}
