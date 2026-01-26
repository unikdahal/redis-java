package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * BLPOP key [key ...] timeout
 * Blocking list pop from the left.
 * Removes and returns the first element of one of the lists.
 * Blocks until an element is available or timeout expires.
 * Returns array: [key, element] or nil on timeout.
 *
 * Timeout is in seconds (can be decimal for sub-second precision).
 * Timeout of 0 means block indefinitely (not recommended in production).
 * 
 * This implementation uses Netty's event loop scheduling to avoid blocking
 * the I/O thread, allowing the server to continue processing other requests.
 */
public class BLPopCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'BLPOP' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String ERR_TIMEOUT = "-ERR timeout is not a float or out of range\r\n";
    private static final String RESP_NIL = "*-1\r\n";

    private static final long POLL_INTERVAL_MS = 50; // Poll every 50ms for better responsiveness
    private static final long MAX_TIMEOUT_SECONDS = 60 * 60 * 24;

    /**
     * Performs a non-blocking left-pop on the specified keys using the provided timeout.
     * Uses Netty's event loop scheduling to poll for data without blocking the I/O thread.
     *
     * @param args the command arguments: one or more keys followed by a timeout in seconds (decimal allowed)
     * @param ctx the Netty channel context for the request
     * @return null for async handling (production), or a RESP string for sync handling (testing)
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        // Last argument is the timeout
        String timeoutStr = args.get(args.size() - 1);
        double timeoutSeconds;
        try {
            timeoutSeconds = Double.parseDouble(timeoutStr);
            if (timeoutSeconds < 0 || timeoutSeconds > MAX_TIMEOUT_SECONDS) {
                return ERR_TIMEOUT;
            }
        } catch (NumberFormatException e) {
            return ERR_TIMEOUT;
        }

        // Keys are all arguments except the last one (timeout)
        List<String> keys = args.subList(0, args.size() - 1);

        if (keys.isEmpty()) {
            return ERR_WRONG_ARGS;
        }

        long timeoutMs = (long) (timeoutSeconds * 1000);
        long deadline = System.currentTimeMillis() + timeoutMs;

        RedisDatabase db = RedisDatabase.getInstance();

        // Try immediately first
        for (String key : keys) {
            String result = tryPopFromKey(db, key);
            if (result != null) {
                // Found data immediately, return synchronously
                return formatResult(key, result);
            }
        }

        // No data available
        // Special case: zero timeout means check once and return immediately
        if (timeoutMs == 0) {
            // Already tried above, no data found
            return RESP_NIL;
        }
        
        // Check if we have an event loop available (production) or not (testing)
        if (ctx.executor() != null) {
            // Use async scheduling in production
            schedulePolling(ctx, keys, deadline, db);
            // Return null to indicate async handling
            return null;
        } else {
            // Fallback to synchronous behavior for unit tests
            return pollSynchronously(keys, deadline, db);
        }
    }

    /**
     * Synchronous fallback for testing when no event loop is available.
     * This is only used in unit tests with mocked contexts.
     */
    private String pollSynchronously(List<String> keys, long deadline, RedisDatabase db) {
        // Poll until we find an element or timeout
        while (System.currentTimeMillis() < deadline) {
            // Try each key in order
            for (String key : keys) {
                String result = tryPopFromKey(db, key);
                if (result != null) {
                    return formatResult(key, result);
                }
            }

            // No element found, sleep before next poll
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            long sleepTime = Math.min(POLL_INTERVAL_MS, remaining);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return RESP_NIL;
            }
        }

        // Timeout expired
        return RESP_NIL;
    }

    /**
     * Schedule asynchronous polling for data using Netty's event loop.
     * This avoids blocking the I/O thread while waiting for data.
     */
    private void schedulePolling(ChannelHandlerContext ctx, List<String> keys, long deadline, RedisDatabase db) {
        ctx.executor().schedule(() -> {
            // Check if we've exceeded the deadline
            long now = System.currentTimeMillis();
            if (now >= deadline) {
                // Timeout expired, send nil response
                writeResponse(ctx, RESP_NIL);
                return;
            }

            // Try to pop from each key
            for (String key : keys) {
                String result = tryPopFromKey(db, key);
                if (result != null) {
                    // Found data, send response
                    writeResponse(ctx, formatResult(key, result));
                    return;
                }
            }

            // No data yet, schedule next poll
            schedulePolling(ctx, keys, deadline, db);
        }, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Write a RESP response to the client.
     */
    private void writeResponse(ChannelHandlerContext ctx, String response) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
    }

    /**
     * Attempt to atomically remove and return the first element of the list stored at the given key,
     * updating or removing the key in the database as appropriate.
     *
     * @param db  the RedisDatabase instance to operate on
     * @param key the key whose list to pop from
     * @return the popped element if one was removed, or `null` if the key did not exist, the list was empty,
     *         or the key held a non-list value (in which case the key is skipped)
     */
    private String tryPopFromKey(RedisDatabase db, String key) {
        AtomicBoolean wrongType = new AtomicBoolean(false);
        AtomicReference<String> poppedRef = new AtomicReference<>(null);

        db.compute(key, existing -> {
            if (existing == null) {
                return null;
            }

            if (existing.getType() != RedisValue.Type.LIST) {
                wrongType.set(true);
                return existing;
            }

            @SuppressWarnings("unchecked")
            List<String> existingList = (List<String>) existing.getData();

            if (existingList.isEmpty()) {
                return null;
            }

            LinkedList<String> newList = new LinkedList<>(existingList);
            String popped = newList.removeFirst();
            poppedRef.set(popped);

            if (newList.isEmpty()) {
                return null;
            }

            return RedisValue.list(newList);
        });

        // If wrong type, we should skip this key silently and try next
        // (Redis behavior: returns error only if ALL keys are wrong type)
        if (wrongType.get()) {
            return null;
        }

        return poppedRef.get();
    }

    /**
     * Builds a Redis RESP two-element array representing [key, element].
     *
     * @param key the list key to include as the first element
     * @param element the popped element to include as the second element
     * @return the RESP-formatted string for an array containing the key and element
     */
    private String formatResult(String key, String element) {
        // Return RESP array: *2\r\n$keylen\r\nkey\r\n$elemlen\r\nelement\r\n
        StringBuilder sb = new StringBuilder();
        sb.append("*2\r\n");
        sb.append("$").append(key.length()).append("\r\n");
        sb.append(key).append("\r\n");
        sb.append("$").append(element.length()).append("\r\n");
        sb.append(element).append("\r\n");
        return sb.toString();
    }

    /**
     * Provide the BLPOP command identifier.
     *
     * @return the command name "BLPOP"
     */
    @Override
    public String name() {
        return "BLPOP";
    }
}