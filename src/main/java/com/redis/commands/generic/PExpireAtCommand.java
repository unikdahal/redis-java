package com.redis.commands.generic;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * PEXPIREAT key milliseconds-timestamp
 * <p>
 * Set an absolute Unix timestamp (in milliseconds) as the key's expiration time.
 * The key will be automatically deleted after this timestamp.
 * <p>
 * This command is primarily used internally for replication to ensure consistent
 * expiry times across master and replicas. EXPIRE commands are converted to
 * PEXPIREAT before being propagated to replicas.
 * <p>
 * <b>Return value:</b>
 * <ul>
 *   <li>1 if the timeout was set successfully</li>
 *   <li>0 if the key does not exist</li>
 * </ul>
 */
public class PExpireAtCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'PEXPIREAT' command\r\n";
    private static final String ERR_VALUE = "-ERR value is not an integer or out of range\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        long expiryTimeMillis;

        try {
            expiryTimeMillis = Long.parseLong(args.get(1));
        } catch (NumberFormatException e) {
            return ERR_VALUE;
        }

        // Set the absolute expiry time
        boolean success = RedisDatabase.getInstance().setExpiryTime(key, expiryTimeMillis);

        return success ? ":1\r\n" : ":0\r\n";
    }

    @Override
    public String name() {
        return "PEXPIREAT";
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}
