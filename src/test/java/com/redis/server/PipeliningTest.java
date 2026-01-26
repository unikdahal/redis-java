package com.redis.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PipeliningTest {

    @Test
    public void testPipelinedPing() {
        EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());

        // Send two PING commands in one ByteBuf
        String pipelinedCommands = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        ByteBuf buf = Unpooled.copiedBuffer(pipelinedCommands, StandardCharsets.UTF_8);

        channel.writeInbound(buf);

        // We expect two PONG responses
        ByteBuf response1 = channel.readOutbound();
        ByteBuf response2 = channel.readOutbound();

        assertEquals("+PONG\r\n", response1.toString(StandardCharsets.UTF_8));
        assertEquals("+PONG\r\n", response2.toString(StandardCharsets.UTF_8), "Second pipelined command should be processed");
    }

    @Test
    public void testFragmentedCommand() {
        EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());

        // Fragment: "*1\r\n$4\r\nPI" and then "NG\r\n"
        channel.writeInbound(Unpooled.copiedBuffer("*1\r\n$4\r\nPI", StandardCharsets.UTF_8));
        
        // No response yet
        Object response = channel.readOutbound();
        assertEquals(null, response, "No response should be sent for fragmented command");

        channel.writeInbound(Unpooled.copiedBuffer("NG\r\n", StandardCharsets.UTF_8));

        // Now we expect PONG
        ByteBuf finalResponse = channel.readOutbound();
        assertEquals("+PONG\r\n", finalResponse.toString(StandardCharsets.UTF_8));
    }
}
