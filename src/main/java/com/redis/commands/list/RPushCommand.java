package com.redis.commands.list;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RPUSH key element [element ...]
 * Appends elements to the tail of the list. Creates list if not exists.
 * Returns the length of the list after the push.
 */
public class RPushCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'RPUSH' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    /**
     * Append the given values to the tail of the list stored at the specified key and report the list's new length.
     *
     * @param args a list where the first element is the target key and the subsequent elements are the values to append
     * @return a Redis RESP integer string of the new list size in the form ":<size>\r\n"; returns ERR_WRONG_ARGS if fewer than two arguments are provided, or ERR_WRONG_TYPE if the key exists and is not a list
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        String key = args.getFirst();
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
                return existing;
            } else {
                // Create new list with existing elements to avoid concurrent modification
                @SuppressWarnings("unchecked")
                List<String> existingList = (List<String>) existing.getData();
                newList = new LinkedList<>(existingList);
            }

            // Append all elements to tail - O(1) per element for LinkedList
            for (int i = 1; i < args.size(); i++) {
                newList.addLast(args.get(i));
            }

            resultSize.set(newList.size());
            return RedisValue.list(newList);
        });

        if (wrongType.get()) {
            return ERR_WRONG_TYPE;
        }

        return ":" + resultSize.get() + "\r\n";
    }

    /**
     * Redis command name implemented by this class.
     *
     * @return the command name "RPUSH".
     */
    @Override
    public String name() {
        return "RPUSH";
    }
}