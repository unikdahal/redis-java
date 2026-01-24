package com.redis.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Central configuration for the Redis server.
 * Loads defaults and can be overridden by application.properties or environment variables.
 */
public class RedisConfig {
    private static RedisConfig instance;
    private Properties properties = new Properties();

    // Default configuration values
    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_BOSS_THREADS = 1;
    private static final int DEFAULT_WORKER_THREADS = 1;
    private static final int DEFAULT_CLEANUP_INTERVAL_MS = 5000;
    private static final boolean DEFAULT_ENABLE_EXPIRY = true;

    private RedisConfig() {
        loadProperties();
    }

    public static RedisConfig getInstance() {
        if (instance == null) {
            instance = new RedisConfig();
        }
        return instance;
    }

    private void loadProperties() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                properties.load(is);
                System.out.println("[RedisConfig] Loaded application.properties");
            } else {
                System.out.println("[RedisConfig] No application.properties found, using defaults");
            }
        } catch (IOException e) {
            System.err.println("[RedisConfig] Error loading properties: " + e.getMessage());
        }
    }

    public int getPort() {
        return Integer.parseInt(properties.getProperty("redis.port", String.valueOf(DEFAULT_PORT)));
    }

    public int getBossThreads() {
        return Integer.parseInt(properties.getProperty("redis.boss.threads", String.valueOf(DEFAULT_BOSS_THREADS)));
    }

    public int getWorkerThreads() {
        return Integer.parseInt(properties.getProperty("redis.worker.threads", String.valueOf(DEFAULT_WORKER_THREADS)));
    }

    public int getCleanupIntervalMs() {
        return Integer.parseInt(properties.getProperty("redis.cleanup.interval.ms", String.valueOf(DEFAULT_CLEANUP_INTERVAL_MS)));
    }

    public boolean isExpiryEnabled() {
        return Boolean.parseBoolean(properties.getProperty("redis.expiry.enabled", String.valueOf(DEFAULT_ENABLE_EXPIRY)));
    }

    @Override
    public String toString() {
        return "RedisConfig{" +
                "port=" + getPort() +
                ", bossThreads=" + getBossThreads() +
                ", workerThreads=" + getWorkerThreads() +
                ", cleanupIntervalMs=" + getCleanupIntervalMs() +
                ", expiryEnabled=" + isExpiryEnabled() +
                '}';
    }
}
