package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;

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

        // Atomic get-or-create and push operation
        int newSize = pushElements(db, key, args);
        if (newSize == -1) {
            return ERR_WRONG_TYPE;
        }

        return ":" + newSize + "\r\n";
    }

    /**
     * Append the provided elements to the tail of the list stored at the given key, creating the list if it does not exist.
     *
     * @param db   the RedisDatabase to operate on
     * @param key  the key for the target list
     * @param args the command arguments where elements to append start at index 1
     * @return the size of the list after the append, or -1 if the existing value at the key is not a list
     */
    @SuppressWarnings("unchecked")
    private int pushElements(RedisDatabase db, String key, List<String> args) {
        RedisValue existing = db.getValue(key);

        LinkedList<String> list;
        if (existing == null) {
            // Create new list
            list = new LinkedList<>();
        } else if (existing.getType() != RedisValue.Type.LIST) {
            return -1; // Wrong type
        } else {
            // Get an existing list (LinkedList is mutable, we reuse it)
            list = (LinkedList<String>) existing.getData();
        }

        // Append all elements to tail - O(1) per element for LinkedList
        for (int i = 1; i < args.size(); i++) {
            list.addLast(args.get(i));
        }

        // Store back (only needed for new lists, but keeps logic simple)
        if (existing == null) {
            db.put(key, RedisValue.list(list));
        }

        return list.size();
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