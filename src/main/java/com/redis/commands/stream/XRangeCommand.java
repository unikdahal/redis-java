package com.redis.commands.stream;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import com.redis.util.StreamId;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Implementation of the XRANGE command.
 * <p>
 * <b>Syntax:</b> XRANGE key start end [COUNT count]
 * <p>
 * <b>Role:</b> Performs efficient range queries on the Stream.
 * Because the Stream is backed by a {@link java.util.concurrent.ConcurrentSkipListMap},
 * range lookups are O(log N) rather than O(N).
 */
public class XRangeCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'XRANGE' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // Syntax: XRANGE <key> <start> <end> [COUNT <n>]
        if (args.size() < 3) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        String startArg = args.get(1);
        String endArg = args.get(2);

        // Optional COUNT argument handling
        int count = Integer.MAX_VALUE;
        if (args.size() >= 5 && args.get(3).equalsIgnoreCase("COUNT")) {
            try {
                count = Integer.parseInt(args.get(4));
            } catch (NumberFormatException e) {
                return "-ERR value is not an integer or out of range\r\n";
            }
        }

        RedisDatabase db = RedisDatabase.getInstance();
        RedisValue value = db.getValue(key);

        // Case 1: Stream does not exist -> Return Empty Array (Standard Redis behavior)
        if (value == null) {
            return "*0\r\n";
        }

        // Case 2: Key exists but is not a Stream -> Error
        if (value.getType() != RedisValue.Type.STREAM) {
            return ERR_WRONG_TYPE;
        }

        // Retrieve the sorted map
        @SuppressWarnings("unchecked")
        NavigableMap<StreamId, Map<String, String>> streamMap = (NavigableMap<StreamId, Map<String, String>>) value.getData();

        // Parse bounds with context awareness (Start vs End)
        StreamId start = parseBound(startArg, true);
        StreamId end = parseBound(endArg, false);

        if (start == null || end == null) {
            return "-ERR Invalid stream ID specified as range part\r\n";
        }

        // CORE LOGIC: Get a view of the map for the requested range.
        // true, true means inclusive boundaries [start, end].
        // This is efficient (O(log N)) and does not copy data.
        NavigableMap<StreamId, Map<String, String>> range = streamMap.subMap(start, true, end, true);

        // Build RESP Array Response
        StringBuilder sb = new StringBuilder();

        // Calculate exact size (considering COUNT limit)
        int entriesToReturn = Math.min(count, range.size());
        sb.append("*").append(entriesToReturn).append("\r\n");

        int i = 0;
        for (Map.Entry<StreamId, Map<String, String>> entry : range.entrySet()) {
            if (i++ >= count) break;

            // Start Entry Array
            sb.append("*2\r\n");

            // 1. The ID
            String idStr = entry.getKey().toString();
            sb.append("$").append(idStr.length()).append("\r\n").append(idStr).append("\r\n");

            // 2. The Field-Value pairs
            Map<String, String> fields = entry.getValue();
            sb.append("*").append(fields.size() * 2).append("\r\n");

            for (Map.Entry<String, String> field : fields.entrySet()) {
                String k = field.getKey();
                String v = field.getValue();
                sb.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
                sb.append("$").append(v.length()).append("\r\n").append(v).append("\r\n");
            }
        }

        return sb.toString();
    }

    /**
     * Parses user input for Range bounds.
     * Handles special characters ('-', '+') and incomplete IDs ('1000').
     *
     * @param bound The string argument (e.g., "1500", "1500-1", "-", "+")
     * @param start True if this is the Start bound, False if End bound.
     */
    private StreamId parseBound(String bound, boolean start) {
        // Special Bounds
        if (bound.equals("-")) return StreamId.MIN; // 0-0
        if (bound.equals("+")) return StreamId.MAX; // MaxLong-MaxLong

        // Case: Incomplete ID (e.g., "1000")
        if (!bound.contains("-")) {
            try {
                long ms = Long.parseLong(bound);
                // Context Aware Logic:
                // Start: "1000" -> 1000-0 (Beginning of ms)
                // End:   "1000" -> 1000-MAX (End of ms)
                return start ? new StreamId(ms, 0) : new StreamId(ms, Long.MAX_VALUE);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        // Case: Explicit ID (e.g., "1000-1")
        try {
            return StreamId.parse(bound);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String name() {
        return "XRANGE";
    }
}