package com.redis.commands.string;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * SET command implementation with full Redis compatibility.
 * Syntax: SET key value [EX seconds] [PX milliseconds] [NX|XX]
 *
 * Options:
 *   EX seconds  - Set expiry in seconds
 *   PX millis   - Set expiry in milliseconds
 *   NX - Only set if key does not exist
 *   XX          - Only set if key exists
 *
 * <h2>Replication Notes</h2>
 * <p>
 * For replication, relative expiry options (EX, PX) are converted to absolute
 * timestamps (PXAT) to ensure consistent expiry times across replicas.
 */
public class SetCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'SET' command\r\n";
    private static final String ERR_INVALID_EXPIRY = "-ERR invalid expire time in set\r\n";
    private static final String ERR_SYNTAX = "-ERR syntax error\r\n";
    private static final String RESP_OK = "+OK\r\n";
    private static final String RESP_NIL = "$-1\r\n";

    /**
     * Thread-local storage for computed absolute expiry time.
     * Used to pass the computed PXAT value to getReplicationArgs() without
     * recalculating or modifying the execute() signature.
     */
    private static final ThreadLocal<Long> lastComputedPxat = new ThreadLocal<>();

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // Clear any previous value
        lastComputedPxat.remove();

        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        String value = args.get(1);
        long ttlMillis = -1; // -1 means no expiry
        boolean nx = false;
        boolean xx = false;
        boolean hasRelativeExpiry = false; // Track if EX or PX was used

        // Parse options efficiently
        for (int i = 2; i < args.size(); i++) {
            String opt = args.get(i).toUpperCase();

            switch (opt) {
                case "EX":
                    if (i + 1 >= args.size()) return ERR_SYNTAX;
                    try {
                        ttlMillis = Long.parseLong(args.get(++i)) * 1000L; // Convert seconds to millis
                        if (ttlMillis <= 0) return ERR_INVALID_EXPIRY;
                        hasRelativeExpiry = true;
                    } catch (NumberFormatException e) {
                        return ERR_INVALID_EXPIRY;
                    }
                    break;
                case "PX":
                    if (i + 1 >= args.size()) return ERR_SYNTAX;
                    try {
                        ttlMillis = Long.parseLong(args.get(++i));
                        if (ttlMillis <= 0) return ERR_INVALID_EXPIRY;
                        hasRelativeExpiry = true;
                    } catch (NumberFormatException e) {
                        return ERR_INVALID_EXPIRY;
                    }
                    break;
                case "PXAT":
                    // Absolute milliseconds timestamp - no conversion needed
                    if (i + 1 >= args.size()) return ERR_SYNTAX;
                    try {
                        long pxat = Long.parseLong(args.get(++i));
                        ttlMillis = pxat - System.currentTimeMillis();
                        if (ttlMillis <= 0) {
                            // Already expired, but still set and let it expire
                            ttlMillis = 1;
                        }
                    } catch (NumberFormatException e) {
                        return ERR_INVALID_EXPIRY;
                    }
                    break;
                case "EXAT":
                    // Absolute seconds timestamp - no conversion needed
                    if (i + 1 >= args.size()) return ERR_SYNTAX;
                    try {
                        long exat = Long.parseLong(args.get(++i)) * 1000L;
                        ttlMillis = exat - System.currentTimeMillis();
                        if (ttlMillis <= 0) {
                            ttlMillis = 1;
                        }
                    } catch (NumberFormatException e) {
                        return ERR_INVALID_EXPIRY;
                    }
                    break;
                case "NX":
                    nx = true;
                    break;
                case "XX":
                    xx = true;
                    break;
                default:
                    return ERR_SYNTAX;
            }
        }

        // Conflicting options
        if (nx && xx) {
            return ERR_SYNTAX;
        }

        RedisDatabase db = RedisDatabase.getInstance();
        boolean exists = db.exists(key);

        // NX: only set if not exists
        if (nx && exists) {
            return RESP_NIL;
        }

        // XX: only set if exists
        if (xx && !exists) {
            return RESP_NIL;
        }

        // Set the value
        if (ttlMillis > 0) {
            db.put(key, value, ttlMillis);
            // Store the absolute expiry time for replication if relative time was used
            if (hasRelativeExpiry) {
                lastComputedPxat.set(System.currentTimeMillis() + ttlMillis);
            }
        } else {
            db.put(key, value);
        }

        return RESP_OK;
    }

    @Override
    public String name() {
        return "SET";
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }

    /**
     * Returns canonical arguments for replication.
     * <p>
     * Converts relative expiry options (EX, PX) to absolute PXAT to ensure
     * all replicas set the same absolute expiry time.
     * <p>
     * Example: {@code SET key value EX 60} at time T becomes
     * {@code SET key value PXAT <T+60000>} for replication.
     *
     * @param originalArgs The original arguments with potential EX/PX
     * @param response The response from execute()
     * @return Arguments with PXAT substituted for EX/PX, or null if no rewriting needed
     */
    @Override
    public List<String> getReplicationArgs(List<String> originalArgs, String response) {
        // Only rewrite if successful and we computed an absolute expiry
        if (!RESP_OK.equals(response)) {
            return null;
        }

        Long pxat = lastComputedPxat.get();
        lastComputedPxat.remove(); // Clean up

        if (pxat == null) {
            // No relative expiry was used, no rewriting needed
            return null;
        }

        // Build rewritten args: key value [NX|XX] PXAT <timestamp>
        List<String> replicationArgs = new ArrayList<>();
        replicationArgs.add(originalArgs.get(0)); // key
        replicationArgs.add(originalArgs.get(1)); // value

        // Preserve NX/XX flags
        for (int i = 2; i < originalArgs.size(); i++) {
            String opt = originalArgs.get(i).toUpperCase();
            if (opt.equals("NX") || opt.equals("XX")) {
                replicationArgs.add(opt);
            }
            // Skip EX/PX and their values - we'll add PXAT instead
            if (opt.equals("EX") || opt.equals("PX") || opt.equals("PXAT") || opt.equals("EXAT")) {
                i++; // Skip the value too
            }
        }

        // Add absolute expiry
        replicationArgs.add("PXAT");
        replicationArgs.add(String.valueOf(pxat));

        return replicationArgs;
    }
}
