package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * Base interface for all Redis commands.
 * <p>
 * Each command implementation must define its name and execution logic.
 * Commands are registered with {@link CommandRegistry} for O(1) lookup.
 *
 * <h2>Command Pipeline</h2>
 * <p>
 * Every command goes through this exact pipeline:
 * <pre>
 * parse → validate → canonicalize → execute → append to log → send to replicas
 * </pre>
 *
 * <h2>Replication Contract</h2>
 * <p>
 * Write commands that modify state must follow these rules:
 * <ol>
 *   <li>Override {@link #isWriteCommand()} to return {@code true}</li>
 *   <li>For non-deterministic commands, override {@link #getReplicationArgs(List, String)}
 *       to return canonical arguments</li>
 *   <li>If the command should replicate as a different command, override
 *       {@link #getReplicationCommandName()}</li>
 * </ol>
 *
 * <h2>Canonicalization Examples</h2>
 * <table border="1">
 *   <tr><th>Original Command</th><th>Canonical Form</th><th>Reason</th></tr>
 *   <tr><td>{@code XADD stream * field value}</td>
 *       <td>{@code XADD stream 1706745600000-0 field value}</td>
 *       <td>Auto-generated ID depends on timestamp</td></tr>
 *   <tr><td>{@code SET key value EX 60}</td>
 *       <td>{@code SET key value PXAT 1706745660000}</td>
 *       <td>Relative time → absolute timestamp</td></tr>
 *   <tr><td>{@code EXPIRE key 60}</td>
 *       <td>{@code PEXPIREAT key 1706745660000}</td>
 *       <td>Relative seconds → absolute milliseconds</td></tr>
 * </table>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Command instances are typically singletons registered once. The {@link #execute}
 * method must be thread-safe as it may be called concurrently from multiple
 * Netty event loop threads.
 *
 * @see CommandRegistry
 * @see com.redis.replication.CommandPropagator
 */
public interface ICommand {
    /**
     * Execute the command with the provided arguments.
     * The args list contains only the command arguments (command name removed).
     * Returns a RESP-formatted string (including trailing CRLF) that will be written to the client.
     *
     * @param args the command arguments (excluding command name)
     * @param ctx  the Netty channel context for optional interaction
     * @return RESP-formatted response string
     */
    String execute(List<String> args, ChannelHandlerContext ctx);

    /**
 * Provides the canonical name of the command.
 *
 * @return the command name (e.g., "SET", "GET", "DEL"); lookup of command names is case-insensitive
 */
    String name();

    /**
     * Provide the canonical argument list to use when propagating this command to replicas.
     *
     * <p>Override to return a fixed, canonical form when replication must not depend on
     * client-provided or runtime-generated values (for example: generated IDs, or relative
     * time arguments converted to absolute timestamps).
     *
     * @param originalArgs the original command arguments (excluding command name)
     * @param response the response produced by {@link #execute(List, io.netty.channel.ChannelHandlerContext)},
     *                 which may contain generated values needed to form canonical args
     * @return the canonical arguments to replicate, or `null` to indicate the original arguments should be used
     */
    default List<String> getReplicationArgs(List<String> originalArgs, String response) {
        return null; // Default: use original args
    }

    /**
     * Indicates whether this command modifies state and should be propagated to replicas.
     * <p>
     * Override to return true for write commands (SET, DEL, LPUSH, etc.).
     * Default is false (read-only command).
     *
     * @return true if this command should be propagated to replicas
     */
    default boolean isWriteCommand() {
        return false;
    }

    /**
     * Specifies an alternative command name to use when replicating this command.
     *
     * Override to substitute a different command name for replication (for example,
     * replicate EXPIRE as PEXPIREAT to use absolute timestamps).
     *
     * @return the replication command name, or `null` to use the original command name
     */
    default String getReplicationCommandName() {
        return null;
    }
}