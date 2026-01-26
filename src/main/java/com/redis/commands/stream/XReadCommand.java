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
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
 * Read data from one or multiple streams, only returning entries with an ID greater
 * than the last received ID reported by the caller.
 */
public class XReadCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'XREAD' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String RESP_NIL_ARRAY = "*-1\r\n";
    private static final long POLL_INTERVAL_MS = 50;

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 3) return ERR_WRONG_ARGS;

        int count = Integer.MAX_VALUE;
        long blockMs = -1;
        int streamsIdx = -1;

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
                    break;
                }
            }
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }

        if (streamsIdx == -1 || streamsIdx >= args.size()) return ERR_WRONG_ARGS;

        int remainingArgs = args.size() - streamsIdx;
        if (remainingArgs <= 0 || remainingArgs % 2 != 0) return ERR_WRONG_ARGS;

        int numStreams = remainingArgs / 2;
        List<String> keys = new ArrayList<>(numStreams);
        List<String> ids = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            keys.add(args.get(streamsIdx + i));
            ids.add(args.get(streamsIdx + numStreams + i));
        }

        RedisDatabase db = RedisDatabase.getInstance();

        // Immediate read
        String result = tryRead(db, keys, ids, count);
        if (result != null) return result;

        if (blockMs < 0) return "*-1\r\n"; // Non-blocking and no data: Redis returns nil for XREAD? 
        // Wait, standard Redis XREAD (without BLOCK) returns (nil) if no data.

        // Blocking read
        long deadline = blockMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + blockMs;

        if (ctx.executor() != null) {
            schedulePolling(ctx, keys, ids, count, deadline, db);
            return null;
        } else {
            return pollSynchronously(keys, ids, count, deadline, db);
        }
    }

    private String tryRead(RedisDatabase db, List<String> keys, List<String> idArgs, int count) {
        List<String> streamResponses = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String idArg = idArgs.get(i);
            
            RedisValue value = db.getValue(key);
            if (value == null) continue;
            if (value.getType() != RedisValue.Type.STREAM) {
                 // In Redis, if one key is not a stream, it might error or skip. 
                 // Usually it errors.
                 continue; 
            }

            @SuppressWarnings("unchecked")
            NavigableMap<StreamId, Map<String, String>> streamMap = (NavigableMap<StreamId, Map<String, String>>) value.getData();
            
            StreamId lastId;
            if (idArg.equals("$")) {
                lastId = streamMap.isEmpty() ? new StreamId(0, 0) : streamMap.lastKey();
            } else {
                try {
                    lastId = StreamId.parse(idArg);
                } catch (IllegalArgumentException e) {
                    continue; // Skip invalid ID for now
                }
            }

            NavigableMap<StreamId, Map<String, String>> tail = streamMap.tailMap(lastId, false);
            if (tail.isEmpty()) continue;

            StringBuilder streamSb = new StringBuilder();
            streamSb.append("*2\r\n");
            // Stream name
            streamSb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
            
            // Entries
            int entriesToReturn = Math.min(count, tail.size());
            streamSb.append("*").append(entriesToReturn).append("\r\n");
            
            int j = 0;
            for (Map.Entry<StreamId, Map<String, String>> entry : tail.entrySet()) {
                if (j++ >= count) break;
                streamSb.append("*2\r\n");
                String sid = entry.getKey().toString();
                streamSb.append("$").append(sid.length()).append("\r\n").append(sid).append("\r\n");
                
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

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(streamResponses.size()).append("\r\n");
        for (String s : streamResponses) sb.append(s);
        return sb.toString();
    }

    private void schedulePolling(ChannelHandlerContext ctx, List<String> keys, List<String> ids, int count, long deadline, RedisDatabase db) {
        ctx.executor().schedule(() -> {
            if (System.currentTimeMillis() >= deadline) {
                writeResponse(ctx, RESP_NIL_ARRAY);
                return;
            }

            String result = tryRead(db, keys, ids, count);
            if (result != null) {
                writeResponse(ctx, result);
                return;
            }

            schedulePolling(ctx, keys, ids, count, deadline, db);
        }, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

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
