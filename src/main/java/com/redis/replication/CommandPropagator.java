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
     * Determines whether the given command modifies dataset state and therefore must be propagated to replicas.
     *
     * @param commandName the command name (expected in uppercase)
     * @return true if the command should be propagated to replicas, false otherwise
     */
    public static boolean shouldPropagate(String commandName) {
        return WRITE_COMMANDS.contains(commandName);
    }

    /**
     * Propagates the given command and its arguments from the master to all connected replicas.
     *
     * If the current node is not the master, this method performs no action.
     *
     * @param commandName the command name (e.g., "SET", "DEL")
     * @param args        the command arguments (excluding the command name); may contain nulls to represent bulk nils
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
     * Propagates a command already converted to its canonical RESP arguments to replicas.
     *
     * @param commandName the command name
     * @param rewrittenArgs canonical, normalized arguments to propagate
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
     * Constructs a RESP Array–encoded string representing the given command and its arguments.
     *
     * Null argument elements are encoded as RESP bulk nil entries.
     *
     * @param commandName the command name to encode (e.g., "SET")
     * @param args the command arguments in order; elements may be null to produce bulk nil
     * @return a RESP Array-encoded string for the command and arguments
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
     * Appends the given value to the StringBuilder as a RESP Bulk String; encodes null as a Bulk Nil.
     *
     * The byte length used for the bulk string header is computed from the UTF-8 encoding of {@code value}.
     *
     * @param sb the StringBuilder to append to
     * @param value the string to encode; if {@code null}, a RESP Bulk Nil ({@code $-1\r\n}) is appended
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