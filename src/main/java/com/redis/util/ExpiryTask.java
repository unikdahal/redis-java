package com.redis.util;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A Delayed task representing a key expiration scheduled for a specific time.
 * Used by ExpiryManager to efficiently schedule and execute expiry cleanups.
 */
public class ExpiryTask implements Delayed {
    private final String key;
    private final long expiryTimeMillis;

    public ExpiryTask(String key, long expiryTimeMillis) {
        this.key = key;
        this.expiryTimeMillis = expiryTimeMillis;
    }

    public String getKey() {
        return key;
    }

    public long getExpiryTimeMillis() {
        return expiryTimeMillis;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long delayMillis = expiryTimeMillis - System.currentTimeMillis();
        return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        long thisDelay = getDelay(TimeUnit.MILLISECONDS);
        long otherDelay = other.getDelay(TimeUnit.MILLISECONDS);
        return Long.compare(thisDelay, otherDelay);
    }

    @Override
    public String toString() {
        return "ExpiryTask{" +
                "key='" + key + '\'' +
                ", expiryTimeMillis=" + expiryTimeMillis +
                ", delayMs=" + getDelay(TimeUnit.MILLISECONDS) +
                '}';
    }
}
