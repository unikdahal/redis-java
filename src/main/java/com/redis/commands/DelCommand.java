package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * DEL command implementation.
 * Basic syntax: DEL key [key ...]
 * Deletes one or more keys. Returns the number of keys that were deleted.
 */
public class DelCommand implements ICommand {
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.isEmpty()) {
            return "-ERR wrong number of arguments for 'DEL' command\r\n";
        }

        List<String> keys = new ArrayList<>(args);
        int removedCount = RedisDatabase.getInstance().removeAll(keys);

        // Return RESP integer: number of keys removed
        return ":" + removedCount + "\r\n";
    }

    @Override
    public String name() {
        return "DEL";
    }
}
