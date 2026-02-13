package com.redis.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Central configuration for the Redis server.
 * <p>
 * <b>Configuration Priority (highest to lowest):</b>
 * <ol>
 *   <li>Command-line arguments (--port, --replicaof)</li>
 *   <li>Environment variables (REDIS_PORT, etc.)</li>
 *   <li>application.properties file</li>
 *   <li>Default values</li>
 * </ol>
 * <p>
 * <b>Command-Line Arguments:</b>
 * <ul>
 *   <li>--port &lt;port&gt; - Server listening port</li>
 *   <li>--replicaof &lt;host&gt; &lt;port&gt; - Configure as replica of master</li>
 * </ul>
 */
public class RedisConfig {
    private static RedisConfig instance;
    private final Properties properties = new Properties();

    // Default configuration values
    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_BOSS_THREADS = 1;
    private static final int DEFAULT_WORKER_THREADS = 1;
    private static final int DEFAULT_CLEANUP_INTERVAL_MS = 5000;
    private static final boolean DEFAULT_ENABLE_EXPIRY = true;

    // Command-line overrides
    private Integer cliPort;
    private String replicaOfHost;
    private Integer replicaOfPort;

    private RedisConfig() {
        loadProperties();
    }

    public static RedisConfig getInstance() {
        if (instance == null) {
            instance = new RedisConfig();
        }
        return instance;
    }

    /**
     * Parses command-line arguments and updates configuration.
     * <p>
     * Supported arguments:
     * <ul>
     *   <li>--port &lt;port&gt;</li>
     *   <li>--replicaof &lt;host&gt; &lt;port&gt;</li>
     * </ul>
     */
    public void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();

            switch (arg) {
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            cliPort = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("[RedisConfig] Invalid port: " + args[i]);
                        }
                    }
                    break;

                case "--replicaof":
                    if (i + 2 < args.length) {
                        replicaOfHost = args[++i];
                        try {
                            replicaOfPort = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("[RedisConfig] Invalid replica port: " + args[i]);
                            replicaOfHost = null;
                        }
                    }
                    break;

                default:
                    if (arg.startsWith("--")) {
                        System.err.println("[RedisConfig] Unknown argument: " + arg);
                    }
            }
        }
    }

    private void loadProperties() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                properties.load(is);
            }
        } catch (IOException e) {
            System.err.println("[RedisConfig] Warning: Error loading properties: " + e.getMessage());
        }
    }

    public int getPort() {
        // Priority: CLI > ENV > Properties > Default
        if (cliPort != null) {
            return cliPort;
        }
        String envPort = System.getenv("REDIS_PORT");
        if (envPort != null) {
            try {
                return Integer.parseInt(envPort);
            } catch (NumberFormatException e) {
                System.err.println("[RedisConfig] Warning: Invalid REDIS_PORT value '" + envPort + "', using default");
            }
        }
        String propValue = properties.getProperty("redis.port", String.valueOf(DEFAULT_PORT));
        try {
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            System.err.println("[RedisConfig] Warning: Invalid redis.port property value '" + propValue + "', using default");
            return DEFAULT_PORT;
        }
    }

    public int getBossThreads() {
        String envThreads = System.getenv("REDIS_BOSS_THREADS");
        if (envThreads != null) {
            try {
                return Integer.parseInt(envThreads);
            } catch (NumberFormatException e) {
                System.err.println("[RedisConfig] Warning: Invalid REDIS_BOSS_THREADS value '" + envThreads + "', using default");
            }
        }
        String propValue = properties.getProperty("redis.boss.threads", String.valueOf(DEFAULT_BOSS_THREADS));
        try {
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            System.err.println("[RedisConfig] Warning: Invalid redis.boss.threads property value '" + propValue + "', using default");
            return DEFAULT_BOSS_THREADS;
        }
    }

    public int getWorkerThreads() {
        String envThreads = System.getenv("REDIS_WORKER_THREADS");
        if (envThreads != null) {
            try {
                return Integer.parseInt(envThreads);
            } catch (NumberFormatException e) {
                System.err.println("[RedisConfig] Warning: Invalid REDIS_WORKER_THREADS value '" + envThreads + "', using default");
            }
        }
        String propValue = properties.getProperty("redis.worker.threads", String.valueOf(DEFAULT_WORKER_THREADS));
        try {
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            System.err.println("[RedisConfig] Warning: Invalid redis.worker.threads property value '" + propValue + "', using default");
            return DEFAULT_WORKER_THREADS;
        }
    }

    public int getCleanupIntervalMs() {
        String envInterval = System.getenv("REDIS_CLEANUP_INTERVAL_MS");
        if (envInterval != null) {
            try {
                return Integer.parseInt(envInterval);
            } catch (NumberFormatException e) {
                System.err.println("[RedisConfig] Warning: Invalid REDIS_CLEANUP_INTERVAL_MS value '" + envInterval + "', using default");
            }
        }
        String propValue = properties.getProperty("redis.cleanup.interval.ms", String.valueOf(DEFAULT_CLEANUP_INTERVAL_MS));
        try {
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            System.err.println("[RedisConfig] Warning: Invalid redis.cleanup.interval.ms property value '" + propValue + "', using default");
            return DEFAULT_CLEANUP_INTERVAL_MS;
        }
    }

    public boolean isExpiryEnabled() {
        String envExpiry = System.getenv("REDIS_EXPIRY_ENABLED");
        if (envExpiry != null) {
            return Boolean.parseBoolean(envExpiry);
        }
        return Boolean.parseBoolean(properties.getProperty("redis.expiry.enabled", String.valueOf(DEFAULT_ENABLE_EXPIRY)));
    }

    /**
     * Returns true if this server should be a replica.
     */
    public boolean isReplica() {
        return replicaOfHost != null && replicaOfPort != null;
    }

    /**
     * Gets the master host if configured as replica.
     */
    public String getReplicaOfHost() {
        return replicaOfHost;
    }

    /**
     * Gets the master port if configured as replica.
     */
    public Integer getReplicaOfPort() {
        return replicaOfPort;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RedisConfig{");
        sb.append("port=").append(getPort());
        sb.append(", bossThreads=").append(getBossThreads());
        sb.append(", workerThreads=").append(getWorkerThreads());
        sb.append(", cleanupIntervalMs=").append(getCleanupIntervalMs());
        sb.append(", expiryEnabled=").append(isExpiryEnabled());
        if (isReplica()) {
            sb.append(", replicaOf=").append(replicaOfHost).append(":").append(replicaOfPort);
        }
        sb.append('}');
        return sb.toString();
    }
}
