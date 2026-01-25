package com.redis.storage;

import com.redis.util.ExpiryTask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.function.Consumer;

/**
 * Manages key expiration using a DelayQueue-based approach.
 * Instead of polling all keys every N seconds, the cleaner thread waits for
 * the next key to expire and processes only that key at the exact right time.
 * This is vastly more efficient than the previous polling approach.
 *
 * Optimizations:
 * - Lock-free deduplication tracking
 * - Minimal object allocation in hot path
 * - Early termination check for stopped state
 */
public class ExpiryManager {
    private final DelayQueue<ExpiryTask> expiryQueue = new DelayQueue<>();
    private final ConcurrentHashMap<String, Long> keyExpiryMap = new ConcurrentHashMap<>();
    private final Thread cleanerThread;
    private volatile boolean running = true;

    public ExpiryManager(Consumer<String> onExpire) {
        cleanerThread = new Thread(() -> runExpiryCleaner(onExpire), "redis-expiry-manager");
        cleanerThread.setDaemon(true);
        cleanerThread.setPriority(Thread.MIN_PRIORITY); // Don't steal CPU from command handling
        cleanerThread.start();
    }

    /**
     * Schedule a key for expiration at the given absolute time in milliseconds.
     * If the key is re-set with a new TTL before expiring, this will schedule a new task
     * and the old one will be ignored (deduplication via keyExpiryMap).
     *
     * Optimization: Lock-free update using ConcurrentHashMap
     */
    public void scheduleExpiry(String key, long expiryTimeMillis) {
        keyExpiryMap.put(key, expiryTimeMillis);
        expiryQueue.offer(new ExpiryTask(key, expiryTimeMillis));
    }

    /**
     * Main cleanup loop: waits for the next key to expire, then removes it.
     * The thread sleeps between expirations (zero-polling approach).
     */
    private void runExpiryCleaner(Consumer<String> onExpire) {
        while (running) {
            try {
                // Take blocks until a task is ready (efficient waiting)
                ExpiryTask task = expiryQueue.take();

                if (!running) break; // Fast shutdown check

                String key = task.getKey();
                long expectedExpiryTime = task.getExpiryTimeMillis();

                // Deduplication: only process if this is still the latest expiry for this key
                Long currentExpiry = keyExpiryMap.get(key);
                if (currentExpiry != null && currentExpiry == expectedExpiryTime) {
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
     * Fast and non-blocking termination.
     */
    public void shutdown() {
        running = false;
        if (cleanerThread != null && cleanerThread.isAlive()) {
            cleanerThread.interrupt();
            try {
                cleanerThread.join(1000); // Wait max 1 second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Remove any pending expiry for the given key.
     * This should be called when a key is deleted or overwritten without TTL.
     */
    public void clearExpiry(String key) {
        keyExpiryMap.remove(key);
    }

    public int getPendingExpiryCount() {
        return expiryQueue.size();
    }
}
