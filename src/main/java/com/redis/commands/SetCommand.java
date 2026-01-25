package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * SET command implementation with full Redis compatibility.
 * Syntax: SET key value [EX seconds] [PX milliseconds] [NX|XX]
 *
 * Options:
 *   EX seconds  - Set expiry in seconds
 *   PX millis   - Set expiry in milliseconds
 *   NX - Only set if key does not exist
 *   XX          - Only set if key exists
 */
public class SetCommand implements ICommand {
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'SET' command\r\n";
    private static final String ERR_INVALID_EXPIRY = "-ERR invalid expire time in set\r\n";
    private static final String ERR_SYNTAX = "-ERR syntax error\r\n";
    private static final String RESP_OK = "+OK\r\n";
    private static final String RESP_NIL = "$-1\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        String key = args.getFirst();
        String value = args.get(1);
        long ttlMillis = -1; // -1 means no expiry
        boolean nx = false;
        boolean xx = false;

        // Parse options efficiently
        for (int i = 2; i < args.size(); i++) {
            String opt = args.get(i).toUpperCase();

            switch (opt) {
                case "EX":
                    if (i + 1 >= args.size()) return ERR_SYNTAX;
                    try {
                        ttlMillis = Long.parseLong(args.get(++i)) * 1000L; // Convert seconds to millis
                        if (ttlMillis <= 0) return ERR_INVALID_EXPIRY;
                    } catch (NumberFormatException e) {
                        return ERR_INVALID_EXPIRY;
                    }
                    break;
                case "PX":
                    if (i + 1 >= args.size()) return ERR_SYNTAX;
                    try {
                        ttlMillis = Long.parseLong(args.get(++i));
                        if (ttlMillis <= 0) return ERR_INVALID_EXPIRY;
                    } catch (NumberFormatException e) {
                        return ERR_INVALID_EXPIRY;
                    }
                    break;
                case "NX":
                    nx = true;
                    break;
                case "XX":
                    xx = true;
                    break;
                default:
                    return ERR_SYNTAX;
            }
        }

        // Conflicting options
        if (nx && xx) {
            return ERR_SYNTAX;
        }

        RedisDatabase db = RedisDatabase.getInstance();
        boolean exists = db.exists(key);

        // NX: only set if not exists
        if (nx && exists) {
            return RESP_NIL;
        }

        // XX: only set if exists
        if (xx && !exists) {
            return RESP_NIL;
        }

        // Set the value
        if (ttlMillis > 0) {
            db.put(key, value, ttlMillis);
        } else {
            db.put(key, value);
        }

        return RESP_OK;
    }

    @Override
    public String name() {
        return "SET";
    }
}
