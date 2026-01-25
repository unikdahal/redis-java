package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class PingCommand implements ICommand {
    private static final String RESP_PONG = "+PONG\r\n";

    /**
     * Handle the PING command by echoing a provided message or returning the standard PONG response.
     *
     * @param args list of command arguments; if it contains exactly one element that element is echoed back
     * @return `+PONG\r\n` when no message is supplied, otherwise a RESP bulk string containing the provided message in the form `$<length>\r\n<message>\r\n`
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // PING [message] - if message provided, echo it as bulk string
        if (args != null && args.size() == 1) {
            String msg = args.get(0);
            return "$" + msg.length() + "\r\n" + msg + "\r\n";
        }
        return RESP_PONG;
    }

    /**
     * Provides the command's name.
     *
     * @return the literal command name "PING"
     */
    @Override
    public String name() {
        return "PING";
    }
}