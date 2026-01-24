package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * Base interface for all Redis commands.
 * Each command implementation must define its name and execution logic.
 */
public interface ICommand {
    /**
     * Execute the command with the provided arguments.
     * The args list contains only the command arguments (command name removed).
     * Returns a RESP-formatted string (including trailing CRLF) that will be written to the client.
     *
     * @param args the command arguments (excluding command name)
     * @param ctx  the Netty channel context for optional interaction
     * @return RESP-formatted response string
     */
    String execute(List<String> args, ChannelHandlerContext ctx);

    /**
     * Returns the command name (e.g., "SET", "GET", "DEL").
     * Command names are case-insensitive at lookup time.
     */
    String name();
}
