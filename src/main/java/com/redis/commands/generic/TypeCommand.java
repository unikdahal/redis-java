package com.redis.commands.generic;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * TYPE key
 * Returns the string representation of the type of the value stored at key.
 * The different types that can be returned are: string, list, set, zset and hash.
 * If the key does not exist, none is returned.
 */
public class TypeCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'TYPE' command\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() != 1) {
            return ERR_WRONG_ARGS;
        }

        String key = args.getFirst();
        RedisValue.Type type = RedisDatabase.getInstance().getType(key);

        if (type == null) {
            return "+none\r\n";
        }

        return switch (type) {
            case STRING -> "+string\r\n";
            case LIST -> "+list\r\n";
            case SET -> "+set\r\n";
            case HASH -> "+hash\r\n";
            case SORTED_SET -> "+zset\r\n";
            case STREAM -> "+stream\r\n";
        };
    }

    @Override
    public String name() {
        return "TYPE";
    }
}
