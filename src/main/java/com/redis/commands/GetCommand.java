package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * GET command implementation.
 * Basic syntax: GET key
 * Retrieves the string value at the given key.
 * Returns nil if the key does not exist or has expired.
 */
public class GetCommand implements ICommand {
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 1) {
            return "-ERR wrong number of arguments for 'GET' command\r\n";
        }

        String key = args.get(0);
        String value = RedisDatabase.getInstance().get(key);

        if (value == null) {
            // Return RESP nil bulk string
            return "$-1\r\n";
        }

        // Return RESP bulk string
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    @Override
    public String name() {
        return "GET";
    }
}
