package com.redis.commands.list;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class LPushCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'LPUSH' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        RedisDatabase db = RedisDatabase.getInstance();

        AtomicInteger resultSize = new AtomicInteger(-1);
        AtomicBoolean wrongType = new AtomicBoolean(false);

        // Atomic compute operation - thread-safe read-modify-write
        db.compute(key, existing -> {
            LinkedList<String> newList;

            if (existing == null) {
                newList = new LinkedList<>();
            } else if (existing.getType() != RedisValue.Type.LIST) {
                wrongType.set(true);
                return existing; // Return unchanged, signal error
            } else {
                // Create new list with existing elements to avoid concurrent modification
                @SuppressWarnings("unchecked")
                List<String> existingList = (List<String>) existing.getData();
                newList = new LinkedList<>(existingList);
            }

            for (int i = 1; i < args.size(); i++) {
                newList.addFirst(args.get(i));
            }

            resultSize.set(newList.size());
            return RedisValue.list(newList);
        });

        if (wrongType.get()) {
            return ERR_WRONG_TYPE;
        }

        return ":" + resultSize.get() + "\r\n";
    }

    @Override
    public String name() {
        return "LPUSH";
    }
}
