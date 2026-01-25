package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class PingCommand implements ICommand {
    private static final String RESP_PONG = "+PONG\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // PING [message] - if message provided, echo it as bulk string
        if (args != null && args.size() == 1) {
            String msg = args.get(0);
            return "$" + msg.length() + "\r\n" + msg + "\r\n";
        }
        return RESP_PONG;
    }

    @Override
    public String name() {
        return "PING";
    }
}
