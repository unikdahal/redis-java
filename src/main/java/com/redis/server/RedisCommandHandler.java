package com.redis.server;

import com.redis.commands.CommandRegistry;
import com.redis.commands.ICommand;
import com.redis.transaction.TransactionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Netty channel handler responsible for the "Framing" phase of the Redis protocol.
 * <p>
 * <b>Role in Pipeline:</b>
 * This handler sits between the raw network bytes and the command execution logic.
 * Its job is to turn a stream of fragmented TCP bytes into discrete, actionable Redis commands.
 * <p>
 * <b>Thread Safety & Lifecycle:</b>
 * IMPORTANT: This class is stateful (it holds `argsBuffer`). It must be instantiated
 * <b>per-channel</b> (one instance per connected client). It is NOT @ChannelHandler.Sharable.
 * <p>
 * <b>Optimizations:</b>
 * 1. <b>Zero-Allocation Integer Parsing:</b> Avoids creating String objects just to read lengths.
 * 2. <b>Object Pooling:</b> Reuses a single ArrayList {@code argsBuffer} for the lifetime of the connection
 * to reduce Garbage Collection pressure during high-throughput bursts.
 */
public class RedisCommandHandler extends ByteToMessageDecoder {

    // Initial capacity for the arguments list. Most Redis commands have fewer than 16 arguments.
    private static final int INITIAL_ARGS_CAPACITY = 16;

    /**
     * Commands that are allowed to execute immediately even within a transaction.
     * These are the transaction control commands themselves.
     */
    private static final Set<String> TRANSACTION_COMMANDS = Set.of("EXEC", "DISCARD", "MULTI");

    /**
     * RESP response for successfully queued commands in a transaction.
     */
    private static final String RESP_QUEUED = "+QUEUED\r\n";

    /**
     * A reusable buffer for command arguments.
     * We clear and refill this list for every command instead of allocating a new ArrayList each time.
     * This significantly reduces "Object Churn" in the JVM.
     */
    private final List<String> argsBuffer = new ArrayList<>(INITIAL_ARGS_CAPACITY);

    /**
     * Sentinel value indicating that the buffer does not yet contain a complete integer ending in CRLF.
     */
    private static final int INCOMPLETE = Integer.MIN_VALUE;

    /**
     * The main entry point called by Netty whenever new data arrives from the network.
     * * @param ctx Context to interact with the channel pipeline (e.g., writing responses).
     *
     * @param in  The input ByteBuf containing raw bytes received from the OS.
     * @param out (Unused) We write responses directly to ctx, rather than passing objects up the pipeline.
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // PIPELINING SUPPORT:
        // A client might send 5 commands in a single TCP packet. We must loop to process
        // all of them before returning control to the Netty event loop.
        while (in.readableBytes() > 0) {

            // CHECKPOINTING:
            // We mark the current reader index. If we start parsing a command but realize
            // the packet is fragmented (incomplete data), we will reset the reader index
            // back to this mark so we can try again later when the rest of the data arrives.
            in.markReaderIndex();

            // Reset the reusable argument buffer for the new command
            argsBuffer.clear();

            // Attempt to parse one complete RESP command
            if (!parseRespArray(in, argsBuffer)) {
                // FAILURE CASE (FRAGMENTATION):
                // We ran out of data in the middle of a command.
                // Reset the read pointer to the start of this command and return.
                // Netty will call decode() again when more bytes arrive.
                in.resetReaderIndex();
                return;
            }

            // Edge case: Parser returned true but found an empty array (unlikely in valid RESP but possible)
            if (argsBuffer.isEmpty()) {
                continue;
            }

            // COMMAND RESOLUTION:
            // The first element of the array is always the command name (e.g., "SET", "GET").
            String commandName = argsBuffer.getFirst();
            String upperCommandName = commandName != null ? commandName.toUpperCase() : "";
            ICommand cmd = CommandRegistry.getInstance().get(commandName);

            if (cmd == null) {
                // Protocol requires reporting unknown commands to the client,
                // But if in transaction, we still need to track the error
                TransactionContext txCtx = TransactionContext.get(ctx.channel());
                if (txCtx != null && txCtx.isInTransaction()) {
                    txCtx.markError();
                }
                writeResponse(ctx, "-ERR unknown command '" + commandName + "'\r\n");
            } else {
                // Create a view of the arguments (skipping the command name).
                // subList is a lightweight view, not a copy.
                List<String> commandArgs = argsBuffer.size() > 1 ? argsBuffer.subList(1, argsBuffer.size()) : List.of();

                // TRANSACTION HANDLING:
                // Check if we're in a transaction and this isn't a transaction control command
                TransactionContext txCtx = TransactionContext.get(ctx.channel());
                if (txCtx != null && txCtx.isInTransaction() && !TRANSACTION_COMMANDS.contains(upperCommandName)) {
                    // Queue the command instead of executing it
                    txCtx.queueCommand(cmd, commandArgs);
                    writeResponse(ctx, RESP_QUEUED);
                } else {
                    // EXECUTION:
                    // Run the actual logic (e.g., modifying the KeyValue store).
                    String resp = cmd.execute(commandArgs, ctx);

                    // If response is not null, write it immediately.
                    // Null responses implies the command handles its own writing asynchronously (e.g., blocking ops).
                    if (resp != null) {
                        writeResponse(ctx, resp);
                    }
                }
            }
        }
    }

    /**
     * Parses a RESP Array (the standard format for Redis commands).
     * <p>
     * <b>Protocol Format:</b> {@code *<number_of_args>\r\n$<len>\r\n<arg1>\r\n...}
     * <p>
     * <b>All-or-Nothing Strategy:</b>
     * This method returns {@code false} immediately if the buffer is missing ANY part of the command.
     * It ensures we never partially consume the buffer, which simplifies state management.
     *
     * @param buf    The network buffer.
     * @param result The list to populate with parsed strings.
     * @return {@code true} if a full command was parsed, {@code false} if data is incomplete.
     */
    private boolean parseRespArray(ByteBuf buf, List<String> result) {
        try {
            // Need at least 1 byte to check for the '*' marker
            if (buf.readableBytes() < 1) return false;

            // VALIDATION: RESP Arrays must start with '*'
            if (buf.readByte() != '*') {
                return false; // Malformed request or wrong protocol
            }

            // Step 1: Read the number of arguments in the array
            int numArgs = readInteger(buf);
            if (numArgs == INCOMPLETE) return false; // Waiting for length delimiter

            if (numArgs < 0) return true; // Handle null array (rare in requests, common in responses)

            // Step 2: Loop through each argument
            for (int i = 0; i < numArgs; i++) {
                // Need at least 1 byte for '$'
                if (buf.readableBytes() < 1) return false;

                // VALIDATION: Bulk Strings must start with '$'
                byte marker = buf.readByte();
                if (marker != '$') return false;

                // Step 3: Read the length of the string
                int strLen = readInteger(buf);
                if (strLen == INCOMPLETE) return false; // Waiting for string length

                // Handle special case: Null Bulk String ($-1)
                if (strLen < 0) {
                    result.add(null);
                    continue;
                }

                // Step 4: BOUNDS CHECKING (Crucial)
                // We verify we have the FULL string payload + the 2 trailing bytes (\r\n)
                // BEFORE we attempt to read. This prevents reading half a string.
                if (buf.readableBytes() < strLen + 2) return false;

                // Step 5: Materialize the String
                // readCharSequence reads bytes and advances the readerIndex
                CharSequence arg = buf.readCharSequence(strLen, StandardCharsets.UTF_8);
                result.add(arg.toString());

                // Consume the trailing CRLF (\r\n) required by RESP
                buf.skipBytes(2);
            }
            return true; // Success: Full command parsed
        } catch (Exception e) {
            // In production, you might log this. Returning false triggers a resetReaderIndex.
            return false;
        }
    }

    /**
     * Custom integer parser optimized for the RESP protocol.
     * <p>
     * <b>Why not Integer.parseInt?</b>
     * Standard parsing requires extracting a substring (allocation), creating a String object (allocation),
     * and then parsing it. This method reads raw bytes and computes the integer mathematically,
     * generating <b>zero garbage</b>.
     *
     * @param buf The buffer to read from.
     * @return The parsed integer, or {@code INCOMPLETE} if the delimiter (\r) isn't found yet.
     */
    private int readInteger(ByteBuf buf) {
        // Step 1: Scan for the end of the line ('\r')
        // We look from current readerIndex up to writerIndex (available data)
        int rIndex = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) '\r');

        // If no '\r' found, we don't have the full number yet.
        if (rIndex == -1) return INCOMPLETE;

        // Ensure we also have the '\n' following the '\r'
        if (buf.readableBytes() < (rIndex - buf.readerIndex() + 2)) return INCOMPLETE;

        int value = 0;
        boolean negative = false;

        // Step 2: Read first byte to check for sign
        byte b = buf.readByte();
        if (b == '-') {
            negative = true;
        } else if (b >= '0' && b <= '9') {
            value = b - '0'; // '0' is 48 in ASCII. '5' - '0' = 5.
        }

        // Step 3: ASCII Arithmetic Loop
        // Iterate until we hit the '\r' we found earlier
        while (buf.readerIndex() <= rIndex) {
            b = buf.readByte();
            if (b == '\r') break; // Stop at delimiter
            if (b >= '0' && b <= '9') {
                // Shift existing value left (x10) and add new digit
                value = value * 10 + (b - '0');
            }
        }

        // Step 4: Skip the '\n' byte (we already processed '\r')
        buf.skipBytes(1);

        return negative ? -value : value;
    }

    /**
     * Helper to write a response string back to the client.
     * Uses an unpooled buffer because response strings are typically short lived.
     */
    private void writeResponse(ChannelHandlerContext ctx, String response) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Standard Netty error handling: log and close connection on fatal errors
        System.err.println("[RedisCommandHandler] Exception: " + cause.getMessage());
        ctx.close();
    }
}