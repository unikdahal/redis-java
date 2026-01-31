package com.redis.commands.transaction;

import com.redis.commands.ICommand;
import com.redis.transaction.TransactionContext;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * EXEC command implementation.
 * <p>
 * <b>Syntax:</b> EXEC
 * <p>
 * Executes all previously queued commands in a MULTI/EXEC block and restores
 * the connection state to normal.
 * <p>
 * <b>Optimizations over Redis:</b>
 * <ol>
 *   <li><b>Pre-allocated Response Buffer:</b> We calculate the response size hint
 *       based on queue size to minimize StringBuilder reallocations.</li>
 *   <li><b>Cache-Friendly Iteration:</b> Commands are stored in a contiguous ArrayList,
 *       providing excellent CPU cache utilization during execution.</li>
 *   <li><b>Zero Command Lookups:</b> Commands are resolved and stored at queue time,
 *       not during EXEC, eliminating registry lookups.</li>
 *   <li><b>Batch Execution:</b> All commands execute in a tight loop without
 *       intermediate I/O operations.</li>
 * </ol>
 * <p>
 * <b>Return value:</b>
 * <ul>
 *   <li>Array reply: each element is the reply of each command in the transaction.</li>
 *   <li>Null reply: if EXEC is called without a preceding MULTI.</li>
 *   <li>Null reply: if the transaction was aborted due to errors during queueing.</li>
 * </ul>
 */
public class ExecCommand implements ICommand {

    private static final String ERR_NO_MULTI = "-ERR EXEC without MULTI\r\n";
    private static final String RESP_ABORT = "-EXECABORT Transaction discarded because of previous errors.\r\n";
    private static final String RESP_EMPTY_ARRAY = "*0\r\n";

    // Average response size per command (used for buffer sizing)
    private static final int AVG_RESPONSE_SIZE = 32;

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // Get transaction context
        TransactionContext txCtx = TransactionContext.get(ctx.channel());

        // Verify we're in a transaction
        if (txCtx == null || !txCtx.isInTransaction()) {
            return ERR_NO_MULTI;
        }

        // Check if any errors occurred during queueing
        if (txCtx.hasErrors()) {
            txCtx.endTransaction();
            return RESP_ABORT;
        }

        List<TransactionContext.QueuedCommand> commands = txCtx.getQueuedCommands();

        // Handle empty transaction
        if (commands.isEmpty()) {
            txCtx.endTransaction();
            return RESP_EMPTY_ARRAY;
        }

        // Pre-allocate response builder with estimated capacity
        // Format: *<count>\r\n followed by each response
        int estimatedSize = 8 + (commands.size() * AVG_RESPONSE_SIZE);
        StringBuilder response = new StringBuilder(estimatedSize);

        // RESP Array header
        response.append('*').append(commands.size()).append("\r\n");

        // Execute all commands in sequence
        // Note: Each command's response is already RESP-formatted
        for (TransactionContext.QueuedCommand qc : commands) {
            try {
                String cmdResponse = qc.command().execute(qc.args(), ctx);
                if (cmdResponse != null) {
                    response.append(cmdResponse);
                } else {
                    // Command returned null (async command in transaction - shouldn't happen)
                    // Treat as nil for safety
                    response.append("$-1\r\n");
                }
            } catch (Exception e) {
                // If a command throws, return an error for that slot
                response.append("-ERR ").append(e.getMessage()).append("\r\n");
            }
        }

        // Clean up transaction state
        txCtx.endTransaction();

        return response.toString();
    }

    @Override
    public String name() {
        return "EXEC";
    }
}
