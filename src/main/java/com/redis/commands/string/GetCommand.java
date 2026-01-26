package com.redis.commands.string;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * GET command implementation.
 * Syntax: GET key
 * Retrieves the string value at the given key.
 * Returns nil if the key does not exist or has expired.
 */
public class GetCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'GET' command\r\n";
    private static final String RESP_NIL = "$-1\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() != 1) {
            return ERR_WRONG_ARGS;
        }

        String key = args.getFirst();
        String value = RedisDatabase.getInstance().get(key);

        if (value == null) {
            // Return RESP nil bulk string
            return RESP_NIL;
        }

        // Return RESP bulk string: $<len>\r\n<value>\r\n
        int len = value.length();
        return new StringBuilder(len + 16)
            .append('$').append(len).append("\r\n")
            .append(value).append("\r\n")
            .toString();
    }

    @Override
    public String name() {
        return "GET";
    }
}
