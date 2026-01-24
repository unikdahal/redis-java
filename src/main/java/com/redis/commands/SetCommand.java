package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * SET command implementation.
 * Basic syntax: SET key value
 * Stores a string value at the given key.
 */
public class SetCommand implements ICommand {
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return "-ERR wrong number of arguments for 'SET' command\r\n";
        }

        String key = args.get(0);
        String value = args.get(1);

        // TODO: Future enhancement - support options like EX, PX, NX, XX
        // For now, simple SET without expiry

        RedisDatabase.getInstance().put(key, value);
        return "+OK\r\n";
    }

    @Override
    public String name() {
        return "SET";
    }
}
