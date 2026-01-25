package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class EchoCommand implements ICommand {
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args == null || args.size() != 1) {
            return "-ERR wrong number of arguments for 'ECHO' command\r\n";
        }
        String msg = args.get(0);
        return "$" + msg.length() + "\r\n" + msg + "\r\n";
    }

    @Override
    public String name() {
        return "ECHO";
    }
}
