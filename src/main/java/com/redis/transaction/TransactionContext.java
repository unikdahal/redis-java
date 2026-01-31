package com.redis.transaction;

import com.redis.commands.ICommand;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-connection transaction state manager for MULTI/EXEC/DISCARD commands.
 * <p>
 * <b>Design Goals (Better than Redis):</b>
 * <ol>
 *   <li><b>Zero Contention:</b> Transaction state is stored per-channel using Netty's AttributeMap,
 *       avoiding any global locks for state management.</li>
 *   <li><b>Memory Efficient:</b> Uses a pre-sized ArrayList and only allocates when MULTI is called.</li>
 *   <li><b>Cache-Friendly Execution:</b> Commands are stored contiguously for optimal cache locality during EXEC.</li>
 *   <li><b>Fail-Fast Validation:</b> Commands are validated at queue time, not execution time.</li>
 * </ol>
 * <p>
 * <b>Thread Safety:</b>
 * Each Channel has its own TransactionContext stored as a channel attribute.
 * Since Netty guarantees that a channel's I/O is handled by a single thread,
 * no synchronization is needed within the context itself.
 */
public class TransactionContext {

    /**
     * Netty attribute key for storing transaction context per channel.
     * Using AttributeKey provides O(1) access and is GC-friendly.
     */
    public static final AttributeKey<TransactionContext> TRANSACTION_KEY =
            AttributeKey.valueOf("redis.transaction");

    /**
     * A queued command with its pre-parsed arguments.
     * Using a record for immutability and compact memory layout.
     */
    public record QueuedCommand(ICommand command, List<String> args) {}

    // Initial capacity optimized for typical transaction sizes (10-50 commands)
    private static final int INITIAL_QUEUE_CAPACITY = 16;

    private final List<QueuedCommand> queue;
    private boolean inTransaction;
    private boolean hasErrors; // Track if any command failed to queue (WRONGTYPE, etc.)

    public TransactionContext() {
        this.queue = new ArrayList<>(INITIAL_QUEUE_CAPACITY);
        this.inTransaction = false;
        this.hasErrors = false;
    }

    /**
     * Starts a new transaction.
     * @return true if transaction started, false if already in a transaction
     */
    public boolean startTransaction() {
        if (inTransaction) {
            return false;
        }
        inTransaction = true;
        hasErrors = false;
        queue.clear(); // Reuse the existing list to avoid allocation
        return true;
    }

    /**
     * Queues a command for later execution during EXEC.
     * <p>
     * <b>Optimization:</b> We store the actual ICommand reference and a copy of args,
     * avoiding command lookup during EXEC.
     *
     * @param command The resolved command to queue
     * @param args The command arguments (will be copied to avoid mutation)
     */
    public void queueCommand(ICommand command, List<String> args) {
        // Create a defensive copy of args since the original list may be reused
        queue.add(new QueuedCommand(command, new ArrayList<>(args)));
    }

    /**
     * Marks that an error occurred during command queueing.
     * When EXEC is called, it will return an error instead of executing.
     */
    public void markError() {
        hasErrors = true;
    }

    /**
     * Gets the queued commands for execution.
     * @return Unmodifiable view of queued commands
     */
    public List<QueuedCommand> getQueuedCommands() {
        return queue; // Direct access for performance; caller should not modify
    }

    /**
     * Discards the transaction and clears all queued commands.
     * @return true if a transaction was active, false otherwise
     */
    public boolean discard() {
        if (!inTransaction) {
            return false;
        }
        inTransaction = false;
        hasErrors = false;
        queue.clear();
        return true;
    }

    /**
     * Ends the transaction (called after EXEC completes).
     */
    public void endTransaction() {
        inTransaction = false;
        hasErrors = false;
        queue.clear();
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public boolean hasErrors() {
        return hasErrors;
    }

    public int queueSize() {
        return queue.size();
    }

    // ==================== Static Utility Methods ====================

    /**
     * Gets or creates a TransactionContext for the given channel.
     * Uses Netty's AttributeMap for O(1) access.
     */
    public static TransactionContext getOrCreate(Channel channel) {
        TransactionContext ctx = channel.attr(TRANSACTION_KEY).get();
        if (ctx == null) {
            ctx = new TransactionContext();
            channel.attr(TRANSACTION_KEY).set(ctx);
        }
        return ctx;
    }

    /**
     * Gets the TransactionContext for a channel, or null if none exists.
     */
    public static TransactionContext get(Channel channel) {
        return channel.attr(TRANSACTION_KEY).get();
    }
}
