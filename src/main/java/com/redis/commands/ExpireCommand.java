package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * EXPIRE key seconds [NX | XX | GT | LT]
 * Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
 * 
 * Simple implementation supporting seconds.
 */
public class ExpireCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'EXPIRE' command\r\n";
    private static final String ERR_VALUE = "-ERR value is not an integer or out of range\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) return ERR_WRONG_ARGS;

        String key = args.get(0);
        long seconds;
        try {
            seconds = Long.parseLong(args.get(1));
        } catch (NumberFormatException e) {
            return ERR_VALUE;
        }

        long expiryTimeMillis = System.currentTimeMillis() + (seconds * 1000L);
        boolean success = RedisDatabase.getInstance().setExpiryTime(key, expiryTimeMillis);
        
        return success ? ":1\r\n" : ":0\r\n";
    }

    @Override
    public String name() {
        return "EXPIRE";
    }
}
