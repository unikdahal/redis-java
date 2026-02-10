package com.redis.replication;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Handles command propagation from master to replicas.
 * <p>
 * <b>Responsibilities:</b>
 * <ul>
 *   <li>Determines which commands should be propagated (write commands only)</li>
 *   <li>Converts commands to canonical RESP format for replication</li>
 *   <li>Ensures consistent state by rewriting non-deterministic commands</li>
 * </ul>
 *
 * <h2>Command Rewriting for Replication</h2>
 * <p>
 * Some commands produce non-deterministic results that would cause inconsistent
 * state if sent verbatim to replicas:
 * <ul>
 *   <li><b>XADD with * ID:</b> Auto-generated IDs depend on timestamp.
 *       We rewrite to use the actual generated ID.</li>
 *   <li><b>SET with EX/PX:</b> Relative expiry would cause different absolute times.
 *       We rewrite to use PXAT with absolute timestamp.</li>
 * </ul>
 *
 * <h2>Propagation Flow</h2>
 * <pre>
 * Client Command → Execute on Master → Generate Canonical Form → Send to Replicas
 * </pre>
 *
 * @see ReplicationManager#propagateToReplicas(String)
 */
public class CommandPropagator {

    /**
     * Commands that modify state and must be propagated to replicas.
     * Read-only commands (GET, LRANGE, etc.) are not included.
     */
    private static final Set<String> WRITE_COMMANDS = Set.of(
        // String commands
        "SET", "APPEND", "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
        "SETEX", "PSETEX", "SETNX", "SETRANGE", "MSET", "MSETNX",

        // Key commands
        "DEL", "UNLINK", "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT",
        "PERSIST", "RENAME", "RENAMENX",

        // List commands
        "LPUSH", "LPUSHX", "RPUSH", "RPUSHX", "LPOP", "RPOP", "BLPOP", "BRPOP",
        "LSET", "LINSERT", "LREM", "LTRIM",

        // Stream commands
        "XADD", "XDEL", "XTRIM", "XSETID",

        // Hash commands
        "HSET", "HSETNX", "HMSET", "HINCRBY", "HINCRBYFLOAT", "HDEL",

        // Set commands
        "SADD", "SREM", "SPOP", "SMOVE"
    );

    /**
     * Determines if a command should be propagated to replicas.
     * <p>
     * Only write commands that modify state need to be propagated.
     * Read commands, transaction control (MULTI/EXEC), and replication
     * commands (REPLCONF, PSYNC) are not propagated.
     *
     * @param commandName The uppercase command name
     * @return true if the command should be propagated to replicas
     */
    public static boolean shouldPropagate(String commandName) {
        return WRITE_COMMANDS.contains(commandName);
    }

    /**
     * Propagates a command to all connected replicas.
     * <p>
     * This method converts the command and arguments to RESP format
     * and sends to replicas via the ReplicationManager.
     * <p>
     * <b>Note:</b> For commands that need rewriting (XADD with *, SET with EX),
     * use {@link #propagateRewritten(String, List)} instead to ensure
     * consistent state across replicas.
     *
     * @param commandName The command name (e.g., "SET", "DEL")
     * @param args The command arguments (not including command name)
     */
    public static void propagate(String commandName, List<String> args) {
        ReplicationManager replMgr = ReplicationManager.getInstance();

        // Only master should propagate
        if (!replMgr.isMaster()) {
            return;
        }

        String respCommand = buildRespArray(commandName, args);
        replMgr.propagateToReplicas(respCommand);
    }

    /**
     * Propagates a rewritten/canonical command to replicas.
     * <p>
     * Use this method when the command has been rewritten to its canonical form
     * (e.g., XADD with actual ID instead of *, SET with PXAT instead of EX).
     *
     * @param commandName The command name
     * @param rewrittenArgs The canonical arguments (already normalized)
     */
    public static void propagateRewritten(String commandName, List<String> rewrittenArgs) {
        propagate(commandName, rewrittenArgs);
    }

    /**
     * Propagates a pre-built RESP command string.
     * <p>
     * Use this when the RESP format has already been constructed,
     * avoiding redundant serialization.
     *
     * @param respCommand The complete RESP-encoded command
     */
    public static void propagateRaw(String respCommand) {
        ReplicationManager replMgr = ReplicationManager.getInstance();

        if (!replMgr.isMaster()) {
            return;
        }

        replMgr.propagateToReplicas(respCommand);
    }

    /**
     * Builds a RESP Array from command name and arguments.
     * <p>
     * RESP Array format: *{count}\r\n${len}\r\n{element}\r\n...
     * <p>
     * Example: SET key value becomes:
     * <pre>
     * *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
     * </pre>
     *
     * @param commandName The command name
     * @param args The command arguments
     * @return RESP-encoded array string
     */
    public static String buildRespArray(String commandName, List<String> args) {
        int totalElements = 1 + args.size(); // command name + args

        // Estimate capacity to avoid StringBuilder resizing
        // Each element: $<len>\r\n<data>\r\n = ~10 + data.length
        int estimatedCapacity = 16 + commandName.length();
        for (String arg : args) {
            estimatedCapacity += 10 + (arg != null ? arg.length() : 4);
        }

        StringBuilder sb = new StringBuilder(estimatedCapacity);
        sb.append('*').append(totalElements).append("\r\n");

        // Append command name as bulk string
        appendBulkString(sb, commandName);

        // Append each argument as bulk string
        for (String arg : args) {
            appendBulkString(sb, arg);
        }

        return sb.toString();
    }

    /**
     * Appends a bulk string to the StringBuilder.
     * <p>
     * Bulk String format: ${length}\r\n{data}\r\n
     *
     * @param sb StringBuilder to append to
     * @param value The string value to encode
     */
    private static void appendBulkString(StringBuilder sb, String value) {
        if (value == null) {
            sb.append("$-1\r\n");
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n");
            sb.append(value).append("\r\n");
        }
    }
}
