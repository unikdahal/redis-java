package com.redis.commands.generic;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * EXPIRE key seconds [NX | XX | GT | LT]
 * Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
 * 
 * Simple implementation supporting seconds.
 *
 * <h2>Replication Notes</h2>
 * <p>
 * For replication, EXPIRE commands are converted to PEXPIREAT with absolute
 * timestamps to ensure consistent expiry times across replicas.
 */
public class ExpireCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'EXPIRE' command\r\n";
    private static final String ERR_VALUE = "-ERR value is not an integer or out of range\r\n";

    /**
     * Thread-local storage for computed absolute expiry time.
     */
    private static final ThreadLocal<Long> lastComputedExpiry = new ThreadLocal<>();

    /**
         * Set an expiration for the given key using a relative seconds value.
         *
         * Expects {@code args} to contain the key at index 0 and the expiry in seconds at index 1.
         * On successful expiry set, records the computed absolute expiry timestamp (milliseconds since epoch)
         * in a thread-local for replication rewriting.
         *
         * @param args the command arguments: {@code [key, seconds]}
         * @return {@code ":1\r\n"} if the expiry was set, {@code ":0\r\n"} otherwise
         */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        lastComputedExpiry.remove();

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
        
        // Store for replication rewriting
        if (success) {
            lastComputedExpiry.set(expiryTimeMillis);
        }

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

    /**
     * Indicates this command performs a write operation that modifies the dataset.
     *
     * @return `true` if the command modifies the data (is a write), `false` otherwise.
     */
    @Override
    public boolean isWriteCommand() {
        return true;
    }

    /**
         * Produce replication arguments by converting EXPIRE's relative seconds into
         * PEXPIREAT's absolute expiration timestamp in milliseconds.
         *
         * <p>Only rewrites when the original command succeeded; otherwise returns null.
         *
         * @param originalArgs the original EXPIRE arguments as [key, seconds]
         * @param response the raw response returned by execute()
         * @return the replication arguments as [key, timestamp_ms], or null if the command did not succeed or the computed expiry is unavailable
         */
    @Override
    public List<String> getReplicationArgs(List<String> originalArgs, String response) {
        // Only rewrite if successful
        if (!":1\r\n".equals(response)) {
            return null;
        }

        Long expiryTime = lastComputedExpiry.get();
        lastComputedExpiry.remove();

        if (expiryTime == null) {
            return null;
        }

        // Rewrite as PEXPIREAT with absolute timestamp
        List<String> replicationArgs = new ArrayList<>(2);
        replicationArgs.add(originalArgs.get(0)); // key
        replicationArgs.add(String.valueOf(expiryTime)); // absolute time in ms

        return replicationArgs;
    }

    /**
     * Provide the replication command name to use when rewriting EXPIRE for replication.
     *
     * @return the replication command name "PEXPIREAT", which represents expiry as an absolute Unix-time-millisecond timestamp
     */
    public String getReplicationCommandName() {
        return "PEXPIREAT";
    }
}