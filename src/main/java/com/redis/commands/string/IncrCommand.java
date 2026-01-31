package com.redis.commands.string;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * INCR command implementation.
 * <p>
 * <b>Syntax:</b> INCR key
 * <p>
 * Increments the number stored at key by one. If the key does not exist,
 * it is set to 0 before performing the operation.
 * <p>
 * An error is returned if the key contains a value of the wrong type or
 * contains a string that cannot be represented as integer.
 * <p>
 * <b>Return value:</b> Integer reply: the value of key after the increment.
 */
public class IncrCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'INCR' command\r\n";
    private static final String ERR_NOT_INTEGER = "-ERR value is not an integer or out of range\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() != 1) {
            return ERR_WRONG_ARGS;
        }

        String key = args.getFirst();
        RedisDatabase db = RedisDatabase.getInstance();

        // Use AtomicReference to capture result from compute lambda
        AtomicReference<String> result = new AtomicReference<>();

        db.compute(key, currentValue -> {
            long newValue;

            if (currentValue == null) {
                // Key does not exist, initialize to 0 then increment
                newValue = 1;
            } else if (currentValue.getType() != RedisValue.Type.STRING) {
                // Wrong type error
                result.set(ERR_WRONG_TYPE);
                return currentValue; // Return unchanged
            } else {
                // Key exists and is a string, try to parse as integer
                String strValue = currentValue.asString();
                try {
                    long currentNum = Long.parseLong(strValue);
                    // Check for overflow
                    if (currentNum == Long.MAX_VALUE) {
                        result.set(ERR_NOT_INTEGER);
                        return currentValue; // Return unchanged
                    }
                    newValue = currentNum + 1;
                } catch (NumberFormatException e) {
                    result.set(ERR_NOT_INTEGER);
                    return currentValue; // Return unchanged
                }
            }

            // Success: set the result and return the new value
            result.set(":" + newValue + "\r\n");
            return RedisValue.string(String.valueOf(newValue));
        });

        return result.get();
    }

    @Override
    public String name() {
        return "INCR";
    }
}
