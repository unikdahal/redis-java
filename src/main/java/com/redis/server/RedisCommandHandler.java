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
 */
public class RedisCommandHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;

        try {
            // Parse RESP protocol and execute command
            List<String> args = parseRespArray(buf);

            if (args.isEmpty()) {
                return;
            }

            // Extract command name and arguments
            String commandName = args.get(0);
            List<String> commandArgs = new ArrayList<>();
            for (int i = 1; i < args.size(); i++) {
                commandArgs.add(args.get(i));
            }

            // Look up command in registry
            ICommand cmd = CommandRegistry.getInstance().get(commandName);
            if (cmd == null) {
                writeResponse(ctx, "-ERR unknown command '" + commandName + "'\r\n");
            } else {
                // Execute command and send response
                String resp = cmd.execute(commandArgs, ctx);
                writeResponse(ctx, resp);
            }

        } finally {
            buf.release();
        }
    }

    /**
     * Parse a RESP array from the ByteBuf.
     * Expected format: *<count>\r\n$<len>\r\n<data>\r\n...\r\n
     */
    private List<String> parseRespArray(ByteBuf buf) {
        List<String> result = new ArrayList<>();

        try {
            // Check for Array Start (*)
            if (buf.readByte() != '*') {
                return result;
            }

            // Read Array Length
            int numArgs = readInteger(buf);
            if (numArgs <= 0) {
                return result;
            }

            // Read All Arguments
            for (int i = 0; i < numArgs; i++) {
                // Check for Bulk String Start ($)
                if (buf.readByte() != '$') {
                    break; // Malformed
                }

                // Read String Length
                int strLen = readInteger(buf);
                if (strLen < 0) {
                    break; // Nil bulk string
                }

                // Read the actual String data
                CharSequence arg = buf.readCharSequence(strLen, StandardCharsets.UTF_8);
                result.add(arg.toString());

                // Skip trailing \r\n
                buf.skipBytes(2);
            }
        } catch (Exception e) {
            System.err.println("[RedisCommandHandler] Parse error: " + e.getMessage());
        }

        return result;
    }

    /**
     * Read an integer from the buffer (digits until \r\n).
     */
    private int readInteger(ByteBuf buf) {
        int value = 0;
        while (buf.getByte(buf.readerIndex()) != '\r') {
            value = value * 10 + (buf.readByte() - '0');
        }
        buf.skipBytes(2); // Skip \r\n
        return value;
    }

    /**
     * Write a RESP response to the client.
     */
    private void writeResponse(ChannelHandlerContext ctx, String response) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("[RedisCommandHandler] Exception: " + cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}
