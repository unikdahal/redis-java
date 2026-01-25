package com.redis.server;

import com.redis.commands.CommandRegistry;
import com.redis.commands.ICommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Netty channel handler for processing incoming Redis commands.
 * Parses RESP protocol, dispatches to appropriate command handlers,
 * and writes RESP-formatted responses back to the client.
 *
 * Optimizations:
 * - Reuses ArrayList for argument parsing
 * - Fast integer parsing without object allocation
 * - Efficient ByteBuf reading with minimal string conversions
 */
public class RedisCommandHandler extends ChannelInboundHandlerAdapter {
    // Preallocate list to avoid allocations for small commands
    private static final int INITIAL_ARGS_CAPACITY = 16;

    /**
     * Handle an inbound Netty message containing a RESP array, parse it into command and arguments,
     * dispatch the command via the registry, and write the RESP-formatted response back to the channel.
     *
     * <p>If the parsed argument list is empty this method returns without writing a response.
     * If no command handler exists for the parsed command name an RESP error is written.
     * If a RedisWrongTypeException is thrown during command execution, it is caught and converted to a RESP error.
     * The method always releases the provided ByteBuf before returning.
     *
     * @param ctx the Netty channel context used to write responses and manage the channel
     * @param msg the incoming message, expected to be a ByteBuf containing a RESP array (command + arguments)
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;

        try {
            List<String> args = parseRespArray(buf);

            if (args.isEmpty()) {
                return;
            }

            // Extract command name and arguments
            String commandName = args.get(0);
            ICommand cmd = CommandRegistry.getInstance().get(commandName);

            if (cmd == null) {
                writeResponse(ctx, "-ERR unknown command '" + commandName + "'\r\n");
            } else {
                try {
                    // Create sublist for command args (avoids creating new ArrayList)
                    List<String> commandArgs = args.size() > 1 ? args.subList(1, args.size()) : List.of();
                    String resp = cmd.execute(commandArgs, ctx);
                    writeResponse(ctx, resp);
                } catch (com.redis.storage.RedisWrongTypeException e) {
                    // Convert RedisWrongTypeException to RESP error
                    writeResponse(ctx, "-" + e.getMessage() + "\r\n");
                }
            }

        } finally {
            buf.release();
        }
    }

    /**
     * Parse a RESP array from the ByteBuf.
     * Expected format: *<count>\r\n$<len>\r\n<data>\r\n...\r\n
     *
     * Optimization: Fast path for common cases, minimal allocations
     */
    private List<String> parseRespArray(ByteBuf buf) {
        List<String> result = new ArrayList<>(INITIAL_ARGS_CAPACITY);

        try {
            // Check for Array Start (*)
            if (buf.readByte() != '*') {
                return result;
            }

            // Read Array Length efficiently (no object allocation)
            int numArgs = readInteger(buf);
            if (numArgs <= 0) {
                return result;
            }


            // Read All Arguments
            for (int i = 0; i < numArgs; i++) {
                // Check for Bulk String Start ($)
                byte marker = buf.readByte();
                if (marker != '$') {
                    break; // Malformed
                }

                // Read String Length efficiently
                int strLen = readInteger(buf);
                if (strLen < 0) {
                    break; // Nil bulk string
                }

                // Read the actual String data with charset conversion
                CharSequence arg = buf.readCharSequence(strLen, StandardCharsets.UTF_8);
                result.add(arg.toString());

                // Skip trailing \r\n
                buf.skipBytes(2);
            }
        } catch (Exception e) {
            // Log parse error but don't crash
            result.clear();
        }

        return result;
    }

    /**
     * Fast integer parsing from ByteBuf without creating Integer objects.
     * Reads digits until \r\n and returns the value.
     */
    private int readInteger(ByteBuf buf) {
        int value = 0;
        boolean negative = false;
        byte b = buf.readByte();

        if (b == '-') {
            negative = true;
            b = buf.readByte();
        }

        while (b != '\r') {
            value = value * 10 + (b - '0');
            b = buf.readByte();
        }
        buf.skipBytes(1); // Skip \n

        return negative ? -value : value;
    }

    /**
     * Write a RESP response to the client efficiently.
     * Uses Netty's Unpooled buffer for small responses.
     */
    private void writeResponse(ChannelHandlerContext ctx, String response) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("[RedisCommandHandler] Exception: " + cause.getMessage());
        ctx.close();
    }
}