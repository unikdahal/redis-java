package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class EchoCommand implements ICommand {
    /**
     * Execute the Redis ECHO command and return its response formatted as a RESP bulk string.
     *
     * @param args a list containing exactly one element: the message to echo
     * @param ctx  the channel handler context (unused by this implementation)
     * @return a RESP bulk string in the form `$<length>\r\n<message>\r\n` when exactly one argument is provided; otherwise the error string `-ERR wrong number of arguments for 'ECHO' command\r\n`
     */
    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args == null || args.size() != 1) {
            return "-ERR wrong number of arguments for 'ECHO' command\r\n";
        }
        String msg = args.get(0);
        return "$" + msg.length() + "\r\n" + msg + "\r\n";
    }

    /**
     * Get the command's canonical name.
     *
     * @return the command name {@code "ECHO"}.
     */
    @Override
    public String name() {
        return "ECHO";
    }
}