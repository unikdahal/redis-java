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

    /**
     * Assembles the requested Redis INFO sections and returns them in Redis Bulk String format.
     *
     * If no section is specified (or the first argument is "all"), the response includes the
     * server, replication, clients, memory, and stats sections. Valid section names are:
     * "server", "replication", "clients", "memory", and "stats".
     *
     * @param args optional arguments where the first element selects which INFO section to include;
     *             when omitted or "all", all sections are included
     * @return a Redis Bulk String containing the requested INFO content (format: $&lt;length&gt;\r\n&lt;content&gt;\r\n)
     */
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

    /**
     * Appends the "Server" INFO section to the provided StringBuilder.
     *
     * The section includes the precomputed server header, `tcp_port`, `uptime_in_seconds`,
     * and `uptime_in_days`, and is terminated with an empty line.
     *
     * @param sb the StringBuilder to append the server information to; this builder is modified
     */
    private void appendServerInfo(StringBuilder sb) {
        long uptimeMs = System.currentTimeMillis() - SERVER_START_TIME;
        sb.append(STATIC_SERVER_HEADER);
        sb.append("tcp_port:").append(RedisConfig.getInstance().getPort()).append("\r\n");
        sb.append("uptime_in_seconds:").append(uptimeMs / 1000).append("\r\n");
        sb.append("uptime_in_days:").append(uptimeMs / MS_IN_DAY).append("\r\n");
        sb.append("\r\n");
    }

    /**
     * Appends the replication section content to the given StringBuilder.
     *
     * Appends the replication manager's INFO output followed by a CRLF ("\r\n").
     */
    private void appendReplicationInfo(StringBuilder sb) {
        sb.append(ReplicationManager.getInstance().getInfoReplication()).append("\r\n");
    }

    /**
     * Appends the "Clients" INFO section to the provided StringBuilder.
     *
     * @param sb the StringBuilder to append the Clients section to; receives `connected_clients` and `blocked_clients` lines
     */
    private void appendClientsInfo(StringBuilder sb) {
        sb.append("# Clients\r\nconnected_clients:1\r\nblocked_clients:0\r\n\r\n");
    }

    /**
     * Appends the "Memory" INFO section to the provided StringBuilder.
     *
     * The section includes `used_memory`, a human-readable `used_memory_human`,
     * `total_system_memory`, and a human-readable `total_system_memory_human`,
     * followed by a blank line.
     *
     * @param sb the StringBuilder to append the Memory section to
     */
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

    /**
     * Append the Redis INFO "Stats" section to the provided StringBuilder.
     *
     * @param sb the StringBuilder to append the Stats section to
     */
    private void appendStatsInfo(StringBuilder sb) {
        sb.append("# Stats\r\ntotal_connections_received:1\r\ntotal_commands_processed:0\r\n\r\n");
    }

    /**
     * Append a human-readable representation of a byte count to the given StringBuilder using unit suffixes.
     *
     * Formats values as bytes ("B") or with one decimal place using "K", "M", or "G" as appropriate and appends
     * the result directly to the provided StringBuilder.
     *
     * @param sb the StringBuilder to append the formatted value to
     * @param bytes the byte count to format
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

    /**
     * Provide the command name for this ICommand implementation.
     *
     * @return the command name "INFO"
     */
    @Override
    public String name() {
        return "INFO";
    }
}