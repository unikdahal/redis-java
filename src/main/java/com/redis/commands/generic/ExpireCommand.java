package com.redis.commands.generic;

import com.redis.commands.ICommand;

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

    /**
     * Set a seconds-based expiration timestamp for the specified key.
     *
     * Expects {@code args} to contain the key at index 0 and the expiry in seconds at index 1.
     * Returns the command error reply {@code ERR_WRONG_ARGS} if fewer than two arguments are provided,
     * or {@code ERR_VALUE} if the expiry value is not a valid integer.
     *
     * @param args the command arguments: {@code [key, seconds]}
     * @param ctx   the Netty channel context (not used by this implementation)
     * @return {@code ":1\r\n"} if the expiry was set, {@code ":0\r\n"} otherwise
     */
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

        // Note: Negative seconds values are accepted (like Redis) and result in immediate expiration
        // since the calculated timestamp will be in the past.
        long expiryTimeMillis = System.currentTimeMillis() + (seconds * 1000L);
        boolean success = RedisDatabase.getInstance().setExpiryTime(key, expiryTimeMillis);
        
        return success ? ":1\r\n" : ":0\r\n";
    }

    /**
     * Command name handled by this command implementation.
     *
     * @return the command name "EXPIRE"
     */
    @Override
    public String name() {
        return "EXPIRE";
    }
}