package com.redis.commands.stream;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import com.redis.util.StreamId;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of XREAD (Blocking Stream Read).
 * <p>
 * <b>Syntax:</b> XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
 * <p>
 * <b>Architecture: Asynchronous Polling</b>
 * This command demonstrates how to handle "Blocking" operations in a Non-Blocking framework (Netty).
 * We cannot block the thread (Thread.sleep) because it would freeze the server.
 * Instead, if no data is found, we schedule a background task to check again later (Polling).
 */
public class XReadCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'XREAD' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String RESP_NIL_ARRAY = "*-1\r\n"; // Standard Redis "Null Array" response
    private static final long POLL_INTERVAL_MS = 50; // How often to check for new data during BLOCK

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 3) return ERR_WRONG_ARGS;

        // --- Step 1: Parse Options (COUNT, BLOCK) ---
        int count = Integer.MAX_VALUE; // Default: Return all available
        long blockMs = -1;             // Default: Non-blocking
        int streamsIdx = -1;           // Index where "STREAMS" keyword appears

        try {
            for (int i = 0; i < args.size(); i++) {
                String arg = args.get(i).toUpperCase();
                if (arg.equals("COUNT")) {
                    if (i + 1 >= args.size()) return ERR_WRONG_ARGS;
                    count = Integer.parseInt(args.get(++i));
                } else if (arg.equals("BLOCK")) {
                    if (i + 1 >= args.size()) return ERR_WRONG_ARGS;
                    blockMs = Long.parseLong(args.get(++i));
                } else if (arg.equals("STREAMS")) {
                    streamsIdx = i + 1;
                    break; // Parsing done, everything after this is Keys/IDs
                }
            }
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }

        // Validation: Must have STREAMS keyword and arguments following it
        if (streamsIdx == -1 || streamsIdx >= args.size()) return ERR_WRONG_ARGS;

        // The remaining arguments must be even (Key1, Key2 ... ID1, ID2)
        int remainingArgs = args.size() - streamsIdx;
        if (remainingArgs <= 0 || remainingArgs % 2 != 0) return ERR_WRONG_ARGS;

        int numStreams = remainingArgs / 2;
        List<String> keys = new ArrayList<>(numStreams);
        List<String> ids = new ArrayList<>(numStreams);

        // Split arguments into Keys and IDs lists
        // Example: STREAMS s1 s2 0-0 0-0  -> Keys:[s1, s2], IDs:[0-0, 0-0]
        for (int i = 0; i < numStreams; i++) {
            keys.add(args.get(streamsIdx + i));
            ids.add(args.get(streamsIdx + numStreams + i));
        }

        RedisDatabase db = RedisDatabase.getInstance();

        // --- Step 2: Attempt Immediate Read ---
        // If data is already there, return immediately. No need to block.
        String result = tryRead(db, keys, ids, count);
        if (result != null) return result;

        // --- Step 3: Handle Blocking ---

        // Case A: No BLOCK option requested.
        // Standard Redis behavior: If no data found in non-blocking mode, return Nil.
        if (blockMs < 0) return "*-1\r\n";

        // Case B: BLOCK requested.
        // Calculate when we should stop waiting. (0 means block forever).
        long deadline = blockMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + blockMs;

        if (ctx.executor() != null) {
            // ASYNC PATH (Correct for Netty):
            // We return null to tell the main handler "Don't send a response yet".
            // We schedule a background task to keep checking.
            schedulePolling(ctx, keys, ids, count, deadline, db);
            return null;
        } else {
            // SYNC PATH (Fallback / Testing):
            // Should be avoided in production as it blocks the thread.
            return pollSynchronously(keys, ids, count, deadline, db);
        }
    }

    /**
     * core logic to check the database for matching entries.
     * Returns a RESP-formatted string if data is found, or null if nothing is found.
     */
    private String tryRead(RedisDatabase db, List<String> keys, List<String> idArgs, int count) {
        List<String> streamResponses = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String idArg = idArgs.get(i);

            RedisValue value = db.getValue(key);
            if (value == null) continue; // Stream doesn't exist
            if (value.getType() != RedisValue.Type.STREAM) continue; // Skip wrong types

            // Access the underlying SkipList
            @SuppressWarnings("unchecked")
            NavigableMap<StreamId, Map<String, String>> streamMap = (NavigableMap<StreamId, Map<String, String>>) value.getData();

            // Resolve the ID to start reading from
            StreamId lastId;
            if (idArg.equals("$")) {
                // Special ID "$": Means "Only new messages".
                // So we start strictly AFTER the current last key.
                lastId = streamMap.isEmpty() ? new StreamId(0, 0) : streamMap.lastKey();
            } else {
                try {
                    lastId = StreamId.parse(idArg);
                } catch (IllegalArgumentException e) {
                    continue; // Skip invalid IDs
                }
            }

            // CRITICAL: tailMap(lastId, false)
            // 'false' means EXCLUSIVE (strictly greater than lastId).
            // This gives us O(log N) access to the new entries.
            NavigableMap<StreamId, Map<String, String>> tail = streamMap.tailMap(lastId, false);
            if (tail.isEmpty()) continue;

            // --- Build RESP Response for this Stream ---
            StringBuilder streamSb = new StringBuilder();

            // 1. Array Header for this stream (Name + Entries)
            streamSb.append("*2\r\n");

            // 2. Stream Name
            streamSb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");

            // 3. Array of Entries
            int entriesToReturn = Math.min(count, tail.size());
            streamSb.append("*").append(entriesToReturn).append("\r\n");

            int j = 0;
            for (Map.Entry<StreamId, Map<String, String>> entry : tail.entrySet()) {
                if (j++ >= count) break;

                // Entry Structure: [ID, [Field, Value, ...]]
                streamSb.append("*2\r\n");

                // ID
                String sid = entry.getKey().toString();
                streamSb.append("$").append(sid.length()).append("\r\n").append(sid).append("\r\n");

                // Field-Value Array
                Map<String, String> fields = entry.getValue();
                streamSb.append("*").append(fields.size() * 2).append("\r\n");
                for (Map.Entry<String, String> f : fields.entrySet()) {
                    String fk = f.getKey();
                    String fv = f.getValue();
                    streamSb.append("$").append(fk.length()).append("\r\n").append(fk).append("\r\n");
                    streamSb.append("$").append(fv.length()).append("\r\n").append(fv).append("\r\n");
                }
            }
            streamResponses.add(streamSb.toString());
        }

        if (streamResponses.isEmpty()) return null;

        // Wrap all stream responses in one outer array
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(streamResponses.size()).append("\r\n");
        for (String s : streamResponses) sb.append(s);
        return sb.toString();
    }

    /**
     * Async Polling Logic.
     * Schedules itself to run repeatedly on the EventLoop until data is found or timeout occurs.
     */
    private void schedulePolling(ChannelHandlerContext ctx, List<String> keys, List<String> ids, int count, long deadline, RedisDatabase db) {
        ctx.executor().schedule(() -> {
            // 1. Check Timeout
            if (System.currentTimeMillis() >= deadline) {
                writeResponse(ctx, RESP_NIL_ARRAY);
                return;
            }

            // 2. Check Data
            String result = tryRead(db, keys, ids, count);
            if (result != null) {
                writeResponse(ctx, result);
                return;
            }

            // 3. Reschedule (Recursive step, but async so no stack overflow)
            schedulePolling(ctx, keys, ids, count, deadline, db);
        }, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Fallback for contexts without an executor (e.g. unit tests).
     * WARNING: Blocks the calling thread.
     */
    private String pollSynchronously(List<String> keys, List<String> ids, int count, long deadline, RedisDatabase db) {
        while (System.currentTimeMillis() < deadline) {
            String result = tryRead(db, keys, ids, count);
            if (result != null) return result;

            try { Thread.sleep(POLL_INTERVAL_MS); } catch (InterruptedException e) { break; }
        }
        return RESP_NIL_ARRAY;
    }

    private void writeResponse(ChannelHandlerContext ctx, String response) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
    }

    @Override
    public String name() {
        return "XREAD";
    }
}