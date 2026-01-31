package com.redis.commands.transaction;

import com.redis.commands.ICommand;
import com.redis.transaction.TransactionContext;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * MULTI command implementation.
 * <p>
 * <b>Syntax:</b> MULTI
 * <p>
 * Marks the start of a transaction block. Subsequent commands will be queued
 * for atomic execution when EXEC is called.
 * <p>
 * <b>Optimization over Redis:</b>
 * <ul>
 *   <li>Transaction state is stored directly in the Netty channel's attribute map,
 *       providing O(1) access without any global data structure lookups.</li>
 *   <li>The transaction context is lazily created only when MULTI is called.</li>
 * </ul>
 * <p>
 * <b>Return value:</b> Simple string reply: always OK.
 */
public class MultiCommand implements ICommand {

    private static final String RESP_OK = "+OK\r\n";
    private static final String ERR_NESTED = "-ERR MULTI calls can not be nested\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // Get or create transaction context for this connection
        TransactionContext txCtx = TransactionContext.getOrCreate(ctx.channel());

        // Check for nested MULTI (not allowed)
        if (txCtx.isInTransaction()) {
            return ERR_NESTED;
        }

        // Start the transaction
        txCtx.startTransaction();
        return RESP_OK;
    }

    @Override
    public String name() {
        return "MULTI";
    }
}
