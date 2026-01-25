package com.redis.server;

import com.redis.commands.CommandRegistry;
import com.redis.commands.ICommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Netty channel handler for processing incoming Redis commands.
 * Parses RESP protocol using ByteToMessageDecoder for fragmentation support.
 * Optimizations:
 * - Reuses ArrayList for argument parsing
 * - Fast integer parsing without object allocation
 * - Robust handling of pipelined commands
 */
public class RedisCommandHandler extends ByteToMessageDecoder {
    // Preallocate list to avoid allocations for small commands
    private static final int INITIAL_ARGS_CAPACITY = 16;
    private final List<String> argsBuffer = new ArrayList<>(INITIAL_ARGS_CAPACITY);

    /**
     * Parses and processes as many complete RESP array commands as are available in the input buffer.
     *
     * <p>Reads RESP arrays from {@code in}, reusing the handler's argument buffer to collect the command
     * name and arguments. If a full command is not yet available the reader index is reset and decoding
     * stops so more data can arrive. For each complete command, resolves the command implementation,
     * executes it with a view of the argument list, and writes the RESP response to the channel. Unknown
     * commands result in the Redis error reply "-ERR unknown command 'name'". This method supports
     * pipelined commands by looping until the input buffer has no more complete commands.</p>
     *
     * @param ctx the Netty channel handler context used to write responses
     * @param in the inbound byte buffer containing RESP data; reader index is advanced for consumed data
     * @param out the list to which decoded messages would normally be added (not used; responses are written directly)
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.readableBytes() > 0) {
            in.markReaderIndex();
            argsBuffer.clear();

            if (!parseRespArray(in, argsBuffer)) {
                in.resetReaderIndex();
                return; // Incomplete command, wait for more data
            }

            if (argsBuffer.isEmpty()) {
                continue;
            }

            // Extract command name and arguments
            String commandName = argsBuffer.getFirst();
            ICommand cmd = CommandRegistry.getInstance().get(commandName);

            if (cmd == null) {
                writeResponse(ctx, "-ERR unknown command '" + commandName + "'\r\n");
            } else {
                // Create a sublist for command args (view only)
                List<String> commandArgs = argsBuffer.size() > 1 ? argsBuffer.subList(1, argsBuffer.size()) : List.of();
                String resp = cmd.execute(commandArgs, ctx);
                writeResponse(ctx, resp);
            }
        }
    }

    private static final int INCOMPLETE = Integer.MIN_VALUE;

    /**
     * Parses a RESP array from the given ByteBuf and appends its elements to the provided list.
     *
     * Each bulk string element is added as a UTF-8 Java String; nil bulk strings are represented as `null`.
     *
     * @param buf    the ByteBuf containing RESP-encoded data (reader index will be advanced as bytes are consumed)
     * @param result the list to populate with parsed array elements; existing contents are not cleared by this method
     * @return       `true` if a complete RESP array was parsed and its elements appended to `result`, `false` if more data is required or the input is malformed
     */
    private boolean parseRespArray(ByteBuf buf, List<String> result) {
        try {
            if (buf.readableBytes() < 1) return false;
            
            // Check for Array Start (*)
            if (buf.readByte() != '*') {
                return false;
            }

            // Read Array Length
            int numArgs = readInteger(buf);
            if (numArgs == INCOMPLETE) return false;
            if (numArgs < 0) return true; // Nil array (*) or empty

            // Read All Arguments
            for (int i = 0; i < numArgs; i++) {
                if (buf.readableBytes() < 1) return false;
                
                // Check for Bulk String Start ($)
                byte marker = buf.readByte();
                if (marker != '$') return false;

                // Read String Length
                int strLen = readInteger(buf);
                if (strLen == INCOMPLETE) return false;
                if (strLen < 0) {
                    result.add(null);
                    continue;
                }

                if (buf.readableBytes() < strLen + 2) return false;

                // Read the actual String data
                CharSequence arg = buf.readCharSequence(strLen, StandardCharsets.UTF_8);
                result.add(arg.toString());

                // Skip trailing \r\n
                buf.skipBytes(2);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Parse a RESP-style integer directly from the given ByteBuf and advance the reader index past its terminating CRLF.
     *
     * The method accepts an optional leading '-' for negative values and consumes the trailing "\r\n" when a full
     * integer is available. If the buffer does not contain a complete integer line (no '\r' found or not enough bytes
     * for the terminating CRLF), the reader index is not advanced and the method returns the INCOMPLETE sentinel.
     *
     * @param buf the buffer to read the integer from
     * @return the parsed integer (negative if prefixed with '-'), or INCOMPLETE if more data is required
     */
    private int readInteger(ByteBuf buf) {
        int rIndex = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) '\r');
        if (rIndex == -1) return INCOMPLETE;
        if (buf.readableBytes() < (rIndex - buf.readerIndex() + 2)) return INCOMPLETE; // Need \r\n

        int value = 0;
        boolean negative = false;
        byte b = buf.readByte();
        if (b == '-') {
            negative = true;
        } else if (b >= '0' && b <= '9') {
            value = b - '0';
        }

        while (buf.readerIndex() <= rIndex) {
            b = buf.readByte();
            if (b == '\r') break;
            if (b >= '0' && b <= '9') {
                value = value * 10 + (b - '0');
            }
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