package com.redis.commands.stream;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import com.redis.util.StreamId;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * XADD key ID field value [field value ...]
 * Appends the specified stream entry to the stream at the specified key.
 * If the key does not exist, as a side effect the stream is created.
 */
public class XAddCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'XADD' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String ERR_ID_TOO_SMALL = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
    private static final String ERR_ID_ZERO = "-ERR The ID specified in XADD must be greater than 0-0\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 3) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        String idArg = args.get(1);

        int fieldStart = 2;
        // Basic argument validation for field-value pairs
        if ((args.size() - fieldStart) % 2 != 0) {
             return ERR_WRONG_ARGS;
        }

        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = fieldStart; i < args.size(); i += 2) {
            fields.put(args.get(i), args.get(i + 1));
        }

        RedisDatabase db = RedisDatabase.getInstance();
        AtomicReference<String> error = new AtomicReference<>(null);
        AtomicReference<StreamId> addedId = new AtomicReference<>(null);

        db.compute(key, existing -> {
            ConcurrentSkipListMap<StreamId, Map<String, String>> streamMap;
            if (existing == null) {
                streamMap = new ConcurrentSkipListMap<>();
            } else if (existing.getType() != RedisValue.Type.STREAM) {
                error.set(ERR_WRONG_TYPE);
                return existing;
            } else {
                @SuppressWarnings("unchecked")
                var data = (Map<StreamId, Map<String, String>>) existing.getData();
                streamMap = (ConcurrentSkipListMap<StreamId, Map<String, String>>) data;
            }

            StreamId lastId = streamMap.isEmpty() ? new StreamId(0, 0) : streamMap.lastKey();
            StreamId newId;

            try {
                if (idArg.equals("*")) {
                    long now = System.currentTimeMillis();
                    if (now > lastId.time()) {
                        newId = new StreamId(now, 0);
                    } else {
                        newId = new StreamId(lastId.time(), lastId.sequence() + 1);
                    }
                } else if (idArg.endsWith("-*")) {
                    long ms = Long.parseLong(idArg.substring(0, idArg.length() - 2));
                    if (ms < lastId.time()) {
                        error.set(ERR_ID_TOO_SMALL);
                        return existing;
                    }
                    long seq = (ms == lastId.time()) ? lastId.sequence() + 1 : 0;
                    newId = new StreamId(ms, seq);
                } else {
                    newId = StreamId.parse(idArg);
                }

                if (newId.time() == 0 && newId.sequence() == 0) {
                    error.set(ERR_ID_ZERO);
                    return existing;
                }

                if (!streamMap.isEmpty() && !newId.isGreaterThan(lastId)) {
                    error.set(ERR_ID_TOO_SMALL);
                    return existing;
                }

                streamMap.put(newId, fields);
                addedId.set(newId);
                return RedisValue.stream(streamMap);
            } catch (Exception e) {
                error.set("-ERR " + e.getMessage() + "\r\n");
                return existing;
            }
        });

        if (error.get() != null) {
            return error.get();
        }

        String idStr = addedId.get().toString();
        return "$" + idStr.length() + "\r\n" + idStr + "\r\n";
    }

    @Override
    public String name() {
        return "XADD";
    }
}
