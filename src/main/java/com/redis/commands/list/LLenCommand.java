package com.redis.commands.list;

import com.redis.commands.ICommand;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class LLenCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'LLEN' command\r\n";
    private static final String RESP_ZERO = ":0\r\n";
    private static final String ERR_NOT_LIST = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";


    /**
     * Return the length of the list stored at the provided key as a RESP integer string.
     *
     * @param args the command arguments; expects a single element: the key whose list length to return
     * @param ctx   Netty channel context for the request (not documented)
     * @return `ERR_WRONG_ARGS` if `args` does not contain exactly one element; `RESP_ZERO` if the key does not exist;
     *         `ERR_NOT_LIST` if the value at the key is not a list; otherwise a RESP integer string `":" + size + "\r\n"`
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args == null || args.size() != 1) {
            return ERR_WRONG_ARGS;
        }

        String key = args.getFirst();
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

    /**
     * The Redis command name handled by this command implementation.
     *
     * @return the command name "LLEN"
     */
    @Override
    public String name() {
        return "LLEN";
    }
}