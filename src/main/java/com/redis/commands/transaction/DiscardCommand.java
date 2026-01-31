package com.redis.commands.transaction;

import com.redis.commands.ICommand;
import com.redis.transaction.TransactionContext;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * DISCARD command implementation.
 * <p>
 * <b>Syntax:</b> DISCARD
 * <p>
 * Flushes all previously queued commands in a transaction and restores
 * the connection state to normal.
 * <p>
 * <b>Optimization:</b>
 * The queued commands list is cleared but not reallocated, allowing the
 * memory to be reused if another transaction starts on the same connection.
 * <p>
 * <b>Return value:</b> Simple string reply: always OK.
 * <p>
 * <b>Error:</b> Returns an error if called without a prior MULTI.
 */
public class DiscardCommand implements ICommand {

    private static final String RESP_OK = "+OK\r\n";
    private static final String ERR_NO_MULTI = "-ERR DISCARD without MULTI\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // Get transaction context
        TransactionContext txCtx = TransactionContext.get(ctx.channel());

        // Verify we're in a transaction
        if (txCtx == null || !txCtx.isInTransaction()) {
            return ERR_NO_MULTI;
        }

        // Discard the transaction
        txCtx.discard();
        return RESP_OK;
    }

    @Override
    public String name() {
        return "DISCARD";
    }
}
