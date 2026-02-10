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

    /**
     * Sets the absolute expiration time (milliseconds since epoch) for the given key and returns a Redis protocol response.
     *
     * @param args command arguments where args.get(0) is the key and args.get(1) is the expiry timestamp in milliseconds
     * @param ctx  the Netty channel handler context
     * @return ":1\r\n" if the expiry was set, ":0\r\n" if it was not set, ERR_WRONG_ARGS if fewer than two arguments were provided, or ERR_VALUE if the timestamp is not a valid integer
     */
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

    /**
     * The Redis command name handled by this command implementation.
     *
     * @return the command name "PEXPIREAT"
     */
    @Override
    public String name() {
        return "PEXPIREAT";
    }

    /**
     * Indicates that this command modifies the database state.
     *
     * @return {@code true} if the command performs a write operation, {@code false} otherwise.
     */
    @Override
    public boolean isWriteCommand() {
        return true;
    }
}