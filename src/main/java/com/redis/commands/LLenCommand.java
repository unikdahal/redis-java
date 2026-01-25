package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class LLenCommand implements ICommand{

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'LLEN' command\r\n";
    private static final String RESP_ZERO = ":0\r\n";
    private static final String ERR_NOT_LIST = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";


    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args == null || args.size() != 1) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        RedisValue value = RedisDatabase.getInstance().getValue(key);

        if (value == null) {
            return RESP_ZERO; // key doesn't exist
        }

        if (value.getType() != RedisValue.Type.LIST) {
            return ERR_NOT_LIST;
        }

        int size = value.asList().size();
        return ":" + size + "\r\n"; // RESP integer
    }

    @Override
    public String name() {
        return "LLEN";
    }
}
