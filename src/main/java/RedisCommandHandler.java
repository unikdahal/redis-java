import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RedisCommandHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;

        try {
            // 1. Check for Array Start (*)
            // Note: We don't use resetReaderIndex here because if it's not *,
            // we probably want to drop the connection or handle inline commands (ignoring for now).
            if (buf.readByte() != '*') {
                return;
            }

            // 2. Read Array Length
            int numArgs = 0;
            while (buf.getByte(buf.readerIndex()) != '\r') {
                numArgs = numArgs * 10 + (buf.readByte() - '0');
            }
            buf.skipBytes(2); // Skip \r\n

            // 3. Read All Arguments
            List<String> args = new ArrayList<>();

            for (int i = 0; i < numArgs; i++) {
                // Check for Bulk String Start ($)
                if (buf.readByte() != '$') {
                    return; // Error or invalid format
                }

                // Read String Length
                int strLen = 0;
                while (buf.getByte(buf.readerIndex()) != '\r') {
                    strLen = strLen * 10 + (buf.readByte() - '0');
                }
                buf.skipBytes(2); // Skip \r\n

                // Read the actual String data
                CharSequence arg = buf.readCharSequence(strLen, StandardCharsets.UTF_8);
                args.add(arg.toString());

                buf.skipBytes(2); // Skip \r\n
            }

            // 4. Execute Command
            // We only look at the FIRST argument to decide what to do (e.g., "SET")
            if (args.isEmpty()) return;

            String commandName = args.get(0).toUpperCase();

            if (commandName.equals("PING")) {
                // RESP Simple Strings start with "+"
                writeResponse(ctx, "+PONG\r\n");
            }
            else if (commandName.equals("ECHO")) {
                if (args.size() > 1) {
                    String payload = args.get(1);
                    // Return as Bulk String
                    writeResponse(ctx, "$" + payload.length() + "\r\n" + payload + "\r\n");
                } else {
                    writeResponse(ctx, "-ERR wrong number of arguments for 'echo' command\r\n");
                }
            }
            else {
                writeResponse(ctx, "-ERR unknown command '" + commandName + "'\r\n");
            }

        } finally {
            buf.release();
        }
    }

    private void writeResponse(ChannelHandlerContext ctx, String response) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}