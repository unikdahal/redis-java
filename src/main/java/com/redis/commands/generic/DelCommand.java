package com.redis.commands.generic;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * DEL command implementation.
 * Syntax: DEL key [key ...]
 * Deletes one or more keys. Returns the number of keys that were deleted.
 */
public class DelCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'DEL' command\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.isEmpty()) {
            return ERR_WRONG_ARGS;
        }

        // Direct iteration is faster than creating a new ArrayList
        int removedCount = RedisDatabase.getInstance().removeAll(args);

        // Return RESP integer: :<count>\r\n
        return new StringBuilder(16)
            .append(':').append(removedCount).append("\r\n")
            .toString();
    }

    @Override
    public String name() {
        return "DEL";
    }
}
