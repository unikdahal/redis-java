package com.redis.commands.generic;

import com.redis.commands.ICommand;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class PingCommand implements ICommand {
    private static final String RESP_PONG = "+PONG\r\n";

    /**
     * Handle the PING command by echoing a provided message or returning the standard PONG response.
     *
     * <p>Redis semantics:
     * <ul>
     *   <li>{@code PING} with no arguments returns {@code +PONG\r\n}</li>
     *   <li>{@code PING message} returns {@code $&lt;length&gt;\r\n&lt;message&gt;\r\n}</li>
     *   <li>{@code PING} with more than one argument returns an error</li>
     * </ul>
     *
     * @param args list of command arguments; if it contains exactly one element that element is echoed back
     * @return {@code +PONG\r\n} when no message is supplied, a RESP bulk string containing the provided message
     *         when exactly one argument is provided, or a RESP error when more than one argument is supplied
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // PING [message] - if message provided, echo it as bulk string; error on too many args
        if (args != null) {
            if (args.size() == 1) {
                String msg = args.get(0);
                return "$" + msg.length() + "\r\n" + msg + "\r\n";
            }
            if (args.size() > 1) {
                return "-ERR wrong number of arguments for 'ping' command\r\n";
            }
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