package com.redis.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for PING command
 */
@DisplayName("PING Command Unit Tests")
public class PingCommandTest {

    private PingCommand command;
    private ChannelHandlerContext mockCtx;

    @BeforeEach
    void setUp() {
        command = new PingCommand();
        mockCtx = mock(ChannelHandlerContext.class);
    }

    @Test
    @DisplayName("PING: No arguments returns PONG")
    void testPingNoArgs() {
        List<String> args = Collections.emptyList();
        String result = command.execute(args, mockCtx);
        assertEquals("+PONG\r\n", result);
    }

    @Test
    @DisplayName("PING: Null arguments returns PONG")
    void testPingNullArgs() {
        String result = command.execute(null, mockCtx);
        assertEquals("+PONG\r\n", result);
    }

    @Test
    @DisplayName("PING: With message returns message as bulk string")
    void testPingWithMessage() {
        List<String> args = Collections.singletonList("hello");
        String result = command.execute(args, mockCtx);
        assertEquals("$5\r\nhello\r\n", result);
    }

    @Test
    @DisplayName("PING: With empty message returns empty bulk string")
    void testPingWithEmptyMessage() {
        List<String> args = Collections.singletonList("");
        String result = command.execute(args, mockCtx);
        assertEquals("$0\r\n\r\n", result);
    }

    @Test
    @DisplayName("PING: With multi-byte message")
    void testPingWithMultiByteMessage() {
        List<String> args = Collections.singletonList("hello world!");
        String result = command.execute(args, mockCtx);
        assertEquals("$12\r\nhello world!\r\n", result);
    }

    @Test
    @DisplayName("PING: With special characters")
    void testPingWithSpecialChars() {
        List<String> args = Collections.singletonList("hello\r\nworld");
        String result = command.execute(args, mockCtx);
        assertEquals("$12\r\nhello\r\nworld\r\n", result);
    }

    @Test
    @DisplayName("PING: Multiple arguments returns error")
    void testPingMultipleArgs() {
        List<String> args = java.util.Arrays.asList("hello", "world");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("PING: Name returns PING")
    void testPingName() {
        assertEquals("PING", command.name());
    }
}
