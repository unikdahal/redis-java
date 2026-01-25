package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class RPushCommand implements ICommand {

    /**
     * Append one or more values to the tail of the list stored at the specified key and return the list's new length.
     *
     * @param args the command arguments where args.get(0) is the key and the subsequent elements are the values to append
     * @return the new length of the list as a String
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {

        return "";
    }

    /**
     * The command name for this implementation.
     *
     * @return the Redis command name "RPUSH"
     */
    @Override
    public String name() {
        return "RPUSH";
    }
}