package com.redis.commands.replication;

import com.redis.commands.ICommand;
import com.redis.config.RedisConfig;
import com.redis.replication.ReplicationManager;
import io.netty.channel.ChannelHandlerContext;

import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Optimized INFO command implementation.
 */
public class InfoCommand implements ICommand {

    private static final long SERVER_START_TIME = System.currentTimeMillis();
    private static final String STATIC_SERVER_HEADER;
    private static final long MS_IN_DAY = 86400000L;

    static {
        // Pre-build immutable server info once to save CPU and allocations per call
        StringBuilder sb = new StringBuilder(512);
        sb.append("# Server\r\n");
        sb.append("redis_version:7.0.0-java\r\n");
        sb.append("redis_mode:standalone\r\n");
        sb.append("os:").append(System.getProperty("os.name")).append(" ")
                .append(System.getProperty("os.version")).append("\r\n");
        sb.append("arch_bits:").append(System.getProperty("os.arch").contains("64") ? "64" : "32").append("\r\n");
        sb.append("executable:java\r\n");
        STATIC_SERVER_HEADER = sb.toString();
    }

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        String section = args.isEmpty() ? "all" : args.get(0).toLowerCase();

        // Pre-size the builder to avoid internal array copying
        StringBuilder info = new StringBuilder(2048);

        switch (section) {
            case "server":      appendServerInfo(info); break;
            case "replication": appendReplicationInfo(info); break;
            case "clients":     appendClientsInfo(info); break;
            case "memory":      appendMemoryInfo(info); break;
            case "stats":       appendStatsInfo(info); break;
            case "all":
            default:
                appendServerInfo(info);
                appendReplicationInfo(info);
                appendClientsInfo(info);
                appendMemoryInfo(info);
                appendStatsInfo(info);
                break;
        }

        String content = info.toString();
        // Redis Bulk String format: $[len]\r\n[data]\r\n
        return "$" + content.length() + "\r\n" + content + "\r\n";
    }

    private void appendServerInfo(StringBuilder sb) {
        long uptimeMs = System.currentTimeMillis() - SERVER_START_TIME;
        sb.append(STATIC_SERVER_HEADER);
        sb.append("tcp_port:").append(RedisConfig.getInstance().getPort()).append("\r\n");
        sb.append("uptime_in_seconds:").append(uptimeMs / 1000).append("\r\n");
        sb.append("uptime_in_days:").append(uptimeMs / MS_IN_DAY).append("\r\n");
        sb.append("\r\n");
    }

    private void appendReplicationInfo(StringBuilder sb) {
        sb.append(ReplicationManager.getInstance().getInfoReplication()).append("\r\n");
    }

    private void appendClientsInfo(StringBuilder sb) {
        sb.append("# Clients\r\nconnected_clients:1\r\nblocked_clients:0\r\n\r\n");
    }

    private void appendMemoryInfo(StringBuilder sb) {
        Runtime rt = Runtime.getRuntime();
        long usedMemory = rt.totalMemory() - rt.freeMemory();
        long maxMemory = rt.maxMemory();

        sb.append("# Memory\r\n");
        sb.append("used_memory:").append(usedMemory).append("\r\n");
        sb.append("used_memory_human:"); formatBytes(sb, usedMemory); sb.append("\r\n");
        sb.append("total_system_memory:").append(maxMemory).append("\r\n");
        sb.append("total_system_memory_human:"); formatBytes(sb, maxMemory); sb.append("\r\n");
        sb.append("\r\n");
    }

    private void appendStatsInfo(StringBuilder sb) {
        sb.append("# Stats\r\ntotal_connections_received:1\r\ntotal_commands_processed:0\r\n\r\n");
    }

    /**
     * Optimized byte formatter:
     * 1. Appends directly to existing StringBuilder to avoid temporary String objects.
     * 2. Uses basic math instead of expensive String.format().
     */
    private void formatBytes(StringBuilder sb, long bytes) {
        if (bytes < 1024) {
            sb.append(bytes).append('B');
        } else if (bytes < 1048576) { // 1024 * 1024
            sb.append(bytes / 1024).append('.').append((bytes % 1024) * 100 / 1024).append('K');
        } else if (bytes < 1073741824) { // 1024^3
            sb.append(bytes / 1048576).append('.').append((bytes % 1048576) * 100 / 1048576).append('M');
        } else {
            sb.append(bytes / 1073741824).append('.').append((bytes % 1073741824) * 100 / 1073741824).append('G');
        }
    }

    @Override
    public String name() {
        return "INFO";
    }
}