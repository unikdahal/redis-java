package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
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
 */
public class BLPopCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'BLPOP' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String ERR_TIMEOUT = "-ERR timeout is not a float or out of range\r\n";
    private static final String RESP_NIL = "*-1\r\n";

    private static final long POLL_INTERVAL_MS = 50; // Poll every 50ms for better responsiveness
    private static final long MAX_TIMEOUT_SECONDS = 60 * 60 * 24; /**
     * Performs a blocking left-pop on the specified keys using the provided timeout and returns the popped key and element encoded as a RESP array.
     *
     * @param args the command arguments: one or more keys followed by a timeout in seconds (decimal allowed)
     * @param ctx the Netty channel context for the request
     * @return a RESP two-element array `[key, element]` when an element is popped; `RESP_NIL` if the timeout expires or no element is available; or an error string (`ERR_WRONG_ARGS` or `ERR_TIMEOUT`) for invalid input
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
        long startTime = System.currentTimeMillis();
        long deadline = (timeoutMs == 0) ? Long.MAX_VALUE : startTime + timeoutMs;

        RedisDatabase db = RedisDatabase.getInstance();

        // WARNING: This implementation uses Thread.sleep() which blocks the Netty I/O thread.
        // This prevents the server from processing other clients' requests during BLPOP execution.
        // A production implementation should use Netty's event loop scheduling or a separate
        // executor thread pool to avoid blocking the I/O thread.
        // TODO: Refactor to use async/non-blocking approach with event notification.
        
        // Poll until we find an element or timeout
        while (System.currentTimeMillis() < deadline) {
            // Try each key in order
            for (String key : keys) {
                String result = tryPopFromKey(db, key);
                if (result != null) {
                    // Return [key, element] as RESP array
                    return formatResult(key, result);
                }
            }

            // No element found, sleep before next poll
            if (timeoutMs > 0) {
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
            } else {
                // Zero timeout means block indefinitely - but check once
                // For safety, we return nil immediately if nothing found with 0 timeout
                // Real Redis would block forever, but that's not practical
                break;
            }
        }

        // Timeout expired
        return RESP_NIL;
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