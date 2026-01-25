package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * TTL key
 * Returns the remaining time to live of a key that has a timeout.
 * -2 if the key does not exist.
 * -1 if the key exists but has no associated expire.
 */
public class TtlCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'TTL' command\r\n";

    /**
     * Returns the remaining time to live for the given key encoded as a Redis RESP integer reply.
     *
     * <p>If {@code args} is empty the method returns the wrong-arguments error string.
     *
     * @param args list of command arguments; the first element is the key to check
     * @return a RESP integer reply string:
     *         {@code -2} if the key does not exist or has just expired,
     *         {@code -1} if the key exists but has no expiry,
     *         the TTL in seconds otherwise; or the wrong-arguments error string when {@code args} is empty
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.isEmpty()) return ERR_WRONG_ARGS;

        String key = args.get(0);
        long expiryTime = RedisDatabase.getInstance().getExpiryTime(key);

        if (expiryTime == -1) {
            return ":-2\r\n"; // Key doesn't exist
        }

        if (expiryTime == Long.MAX_VALUE) {
            return ":-1\r\n"; // No expiry
        }

        long ttlSeconds = (expiryTime - System.currentTimeMillis()) / 1000L;
        if (ttlSeconds < 0) return ":-2\r\n"; // Just expired

        return ":" + ttlSeconds + "\r\n";
    }

    /**
     * Command name for the TTL command.
     *
     * @return the literal command name "TTL"
     */
    @Override
    public String name() {
        return "TTL";
    }
}