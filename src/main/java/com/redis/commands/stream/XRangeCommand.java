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
 * XRANGE key start end [COUNT count]
 * Returns the stream entries matching a given range of IDs.
 */
public class XRangeCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'XRANGE' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 3) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        String startArg = args.get(1);
        String endArg = args.get(2);

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

        if (value == null) {
            return "*0\r\n";
        }

        if (value.getType() != RedisValue.Type.STREAM) {
            return ERR_WRONG_TYPE;
        }

        @SuppressWarnings("unchecked")
        NavigableMap<StreamId, Map<String, String>> streamMap = (NavigableMap<StreamId, Map<String, String>>) value.getData();

        StreamId start = parseBound(startArg, true);
        StreamId end = parseBound(endArg, false);

        if (start == null || end == null) {
             return "-ERR Invalid stream ID specified as range part\r\n";
        }

        NavigableMap<StreamId, Map<String, String>> range = streamMap.subMap(start, true, end, true);

        StringBuilder sb = new StringBuilder();
        int entriesToReturn = Math.min(count, range.size());
        sb.append("*").append(entriesToReturn).append("\r\n");

        int i = 0;
        for (Map.Entry<StreamId, Map<String, String>> entry : range.entrySet()) {
            if (i++ >= count) break;
            
            sb.append("*2\r\n");
            // Entry ID
            String idStr = entry.getKey().toString();
            sb.append("$").append(idStr.length()).append("\r\n").append(idStr).append("\r\n");
            
            // Entry Fields
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

    private StreamId parseBound(String bound, boolean start) {
        if (bound.equals("-")) return StreamId.MIN;
        if (bound.equals("+")) return StreamId.MAX;
        
        if (!bound.contains("-")) {
            try {
                long ms = Long.parseLong(bound);
                return start ? new StreamId(ms, 0) : new StreamId(ms, Long.MAX_VALUE);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        
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
