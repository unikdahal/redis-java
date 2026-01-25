package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ListIterator;

public class LRangeCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'LRANGE' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String ERR_NOT_INTEGER = "-ERR value is not an integer or out of range\r\n";
    private static final String EMPTY_ARRAY = "*0\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() != 3) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        int start, stop;

        try {
            start = Integer.parseInt(args.get(1));
            stop = Integer.parseInt(args.get(2));
        } catch (NumberFormatException e) {
            return ERR_NOT_INTEGER;
        }

        RedisDatabase db = RedisDatabase.getInstance();
        RedisValue value = db.getValue(key);

        // Key doesn't exist - return empty array
        if (value == null) {
            return EMPTY_ARRAY;
        }

        // Wrong type check
        if (value.getType() != RedisValue.Type.LIST) {
            return ERR_WRONG_TYPE;
        }

        List<String> list = value.asList();
        int size = list.size();

        // Handle negative indices (Redis supports negative indexing)
        if (start < 0) {
            start = size + start;
        }
        if (stop < 0) {
            stop = size + stop;
        }

        // Clamp start to 0 if still negative
        if (start < 0) {
            start = 0;
        }

        // Start >= size - return empty array
        if (start >= size) {
            return EMPTY_ARRAY;
        }

        // Clamp stop to last index if exceeds size
        if (stop >= size) {
            stop = size - 1;
        }

        // Start > stop - return empty array
        if (start > stop) {
            return EMPTY_ARRAY;
        }

        // Build RESP array response
        int count = stop - start + 1;
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(count).append("\r\n");

        //Using ListIterator here to reduce the time complexity from O(n^2) to O(n) in case of linked list
        ListIterator<String> it = list.listIterator(start);
        for (int i = start; i <= stop && it.hasNext(); i++) {
            String element = it.next();
            byte[] bytes = element.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n");
            sb.append(element).append("\r\n");
        }

        return sb.toString();
    }

    @Override
    public String name() {
        return "LRANGE";
    }
}
