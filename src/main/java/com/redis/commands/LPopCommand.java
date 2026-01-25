package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LPOP key [count]
 * Removes and returns the first element(s) of the list at key.
 * Without count: returns single element as bulk string.
 * With count: returns array of elements.
 */
public class LPopCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'LPOP' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String ERR_NOT_INTEGER = "-ERR value is not an integer or out of range\r\n";
    private static final String RESP_NIL = "$-1\r\n";
    private static final String RESP_EMPTY_ARRAY = "*0\r\n";

    /**
     * Execute the LPOP command on the database and produce a RESP-formatted reply.
     *
     * <p>The first element of {@code args} is the key whose list is popped. If a second
     * argument is provided it is parsed as a non-negative integer count; when present the
     * command returns an array of up to that many popped elements, otherwise it returns a
     * single bulk string for the removed element.</p>
     *
     * @param args command arguments: {@code args.get(0)} is the key, {@code args.get(1)}
     *             when present is the optional non-negative pop count
     * @param ctx  Netty channel context (unused by this implementation)
     * @return a RESP reply string:
     *         - an error reply for wrong argument count, non-integer or negative count, or wrong type,
     *         - `nil` (RESP bulk nil) when no element is removed in single-element mode,
     *         - an empty RESP array when count mode is used and nothing is removed,
     *         - a RESP bulk string for a single removed element,
     *         - a RESP array of bulk strings for multiple removed elements.
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.isEmpty() || args.size() > 2) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        int count = 1;
        boolean returnArray = false;

        if (args.size() == 2) {
            try {
                count = Integer.parseInt(args.get(1));
                if (count < 0) {
                    return ERR_NOT_INTEGER;
                }
                returnArray = true;
            } catch (NumberFormatException e) {
                return ERR_NOT_INTEGER;
            }
        }

        if (count == 0) {
            return returnArray ? RESP_EMPTY_ARRAY : RESP_NIL;
        }

        RedisDatabase db = RedisDatabase.getInstance();
        AtomicBoolean wrongType = new AtomicBoolean(false);
        AtomicBoolean keyNotFound = new AtomicBoolean(false);
        AtomicReference<List<String>> poppedRef = new AtomicReference<>(new ArrayList<>());

        final int popCount = count;

        db.compute(key, existing -> {
            if (existing == null) {
                keyNotFound.set(true);
                return null;
            }

            if (existing.getType() != RedisValue.Type.LIST) {
                wrongType.set(true);
                return existing;
            }

            @SuppressWarnings("unchecked")
            List<String> existingList = (List<String>) existing.getData();

            if (existingList.isEmpty()) {
                keyNotFound.set(true);
                return null; // Remove empty list
            }

            LinkedList<String> newList = new LinkedList<>(existingList);
            List<String> popped = new ArrayList<>();

            int toPop = Math.min(popCount, newList.size());
            for (int i = 0; i < toPop; i++) {
                popped.add(newList.removeFirst());
            }

            poppedRef.set(popped);

            // If list is now empty, remove the key
            if (newList.isEmpty()) {
                return null;
            }

            return RedisValue.list(newList);
        });

        if (wrongType.get()) {
            return ERR_WRONG_TYPE;
        }

        if (keyNotFound.get() || poppedRef.get().isEmpty()) {
            return returnArray ? RESP_EMPTY_ARRAY : RESP_NIL;
        }

        List<String> popped = poppedRef.get();

        // Single element mode (no count argument)
        if (!returnArray) {
            String element = popped.get(0);
            return "$" + element.length() + "\r\n" + element + "\r\n";
        }

        // Array mode (count argument provided)
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(popped.size()).append("\r\n");
        for (String element : popped) {
            sb.append("$").append(element.length()).append("\r\n");
            sb.append(element).append("\r\n");
        }
        return sb.toString();
    }

    /**
     * Provides the command's name identifier.
     *
     * @return the command name "LPOP"
     */
    @Override
    public String name() {
        return "LPOP";
    }
}