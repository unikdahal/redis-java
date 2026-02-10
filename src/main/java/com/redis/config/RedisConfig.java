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

    /**
     * Constructs the singleton RedisConfig and initializes configuration from application properties.
     */
    private RedisConfig() {
        loadProperties();
    }

    /**
     * Retrieve the singleton RedisConfig instance.
     *
     * The instance is created on first access and reused for subsequent calls.
     *
     * @return the shared RedisConfig instance
     */
    public static RedisConfig getInstance() {
        if (instance == null) {
            instance = new RedisConfig();
        }
        return instance;
    }

    /**
     * Parse command-line arguments and apply recognized configuration overrides.
     *
     * <p>Recognized (case-insensitive) flags:
     * <ul>
     *   <li>{@code --port <port>} — sets the CLI override port.</li>
     *   <li>{@code --replicaof <host> <port>} — sets the replica master host and port.</li>
     * </ul>
     *
     * <p>On invalid numeric values or unknown {@code --}-prefixed arguments the method will print an
     * error message to standard error. If the replica port is invalid the replica host will be cleared.
     *
     * @param args the command-line arguments to parse
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

    /**
     * Loads "application.properties" from the classpath into the instance's properties.
     *
     * If the resource is not found, the method returns without modifying properties.
     * If an I/O error occurs while reading, a warning message is printed to stderr and the method returns.
     */
    private void loadProperties() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                properties.load(is);
            }
        } catch (IOException e) {
            System.err.println("[RedisConfig] Warning: Error loading properties: " + e.getMessage());
        }
    }

    /**
     * Determine the Redis server port using the following precedence: CLI argument, REDIS_PORT environment variable, properties file value, then DEFAULT_PORT.
     *
     * @return the resolved port number to bind the Redis server to
     */
    public int getPort() {
        // Priority: CLI > ENV > Properties > Default
        if (cliPort != null) {
            return cliPort;
        }
        String envPort = System.getenv("REDIS_PORT");
        if (envPort != null) {
            return Integer.parseInt(envPort);
        }
        return Integer.parseInt(properties.getProperty("redis.port", String.valueOf(DEFAULT_PORT)));
    }

    /**
     * Determine the configured number of boss threads for the Redis server.
     *
     * @return the configured boss thread count: value of the `REDIS_BOSS_THREADS` environment variable if set, otherwise the `redis.boss.threads` property, otherwise the default value.
     */
    public int getBossThreads() {
        String envThreads = System.getenv("REDIS_BOSS_THREADS");
        if (envThreads != null) {
            return Integer.parseInt(envThreads);
        }
        return Integer.parseInt(properties.getProperty("redis.boss.threads", String.valueOf(DEFAULT_BOSS_THREADS)));
    }

    /**
     * Determines the number of worker threads the server should use.
     *
     * @return the worker thread count resolved from (in order) the `REDIS_WORKER_THREADS` environment variable, the `redis.worker.threads` property, or the default `DEFAULT_WORKER_THREADS`
     */
    public int getWorkerThreads() {
        String envThreads = System.getenv("REDIS_WORKER_THREADS");
        if (envThreads != null) {
            return Integer.parseInt(envThreads);
        }
        return Integer.parseInt(properties.getProperty("redis.worker.threads", String.valueOf(DEFAULT_WORKER_THREADS)));
    }

    /**
     * Get the interval, in milliseconds, used for periodic cleanup of expired entries.
     *
     * Resolution order: environment variable `REDIS_CLEANUP_INTERVAL_MS` → property `redis.cleanup.interval.ms` → default `DEFAULT_CLEANUP_INTERVAL_MS`.
     *
     * @return the cleanup interval in milliseconds
     */
    public int getCleanupIntervalMs() {
        String envInterval = System.getenv("REDIS_CLEANUP_INTERVAL_MS");
        if (envInterval != null) {
            return Integer.parseInt(envInterval);
        }
        return Integer.parseInt(properties.getProperty("redis.cleanup.interval.ms", String.valueOf(DEFAULT_CLEANUP_INTERVAL_MS)));
    }

    /**
     * Determine whether key expiry is enabled by consulting configuration sources in precedence order:
     * environment variable `REDIS_EXPIRY_ENABLED`, application properties (`redis.expiry.enabled`), then the built-in default.
     *
     * @return `true` if expiry is enabled, `false` otherwise.
     */
    public boolean isExpiryEnabled() {
        String envExpiry = System.getenv("REDIS_EXPIRY_ENABLED");
        if (envExpiry != null) {
            return Boolean.parseBoolean(envExpiry);
        }
        return Boolean.parseBoolean(properties.getProperty("redis.expiry.enabled", String.valueOf(DEFAULT_ENABLE_EXPIRY)));
    }

    /**
     * Indicates whether this server is configured to act as a replica.
     *
     * @return `true` if both the replica host and replica port are configured, `false` otherwise.
     */
    public boolean isReplica() {
        return replicaOfHost != null && replicaOfPort != null;
    }

    /**
     * Returns the configured master host when this instance is set up as a replica.
     *
     * @return the master host configured via command-line or properties, or `null` if no replica master is configured
     */
    public String getReplicaOfHost() {
        return replicaOfHost;
    }

    /**
     * Return the configured master port when this instance is a replica.
     *
     * @return the master port configured via CLI or properties, or `null` if no replica port is set
     */
    public Integer getReplicaOfPort() {
        return replicaOfPort;
    }

    /**
     * Render a compact single-line representation of the current Redis configuration.
     *
     * @return a string describing the configured port, bossThreads, workerThreads, cleanupIntervalMs,
     *         expiryEnabled, and, if configured, the replica master as `host:port`
     */
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