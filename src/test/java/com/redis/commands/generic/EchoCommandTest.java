package com.redis.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for ECHO command
 */
@DisplayName("ECHO Command Unit Tests")
public class EchoCommandTest {

    private EchoCommand command;
    private ChannelHandlerContext mockCtx;

    @BeforeEach
    void setUp() {
        command = new EchoCommand();
        mockCtx = mock(ChannelHandlerContext.class);
    }

    @Test
    @DisplayName("ECHO: With message returns message as bulk string")
    void testEchoWithMessage() {
        List<String> args = Collections.singletonList("hello");
        String result = command.execute(args, mockCtx);
        assertEquals("$5\r\nhello\r\n", result);
    }

    @Test
    @DisplayName("ECHO: With empty message returns empty bulk string")
    void testEchoWithEmptyMessage() {
        List<String> args = Collections.singletonList("");
        String result = command.execute(args, mockCtx);
        assertEquals("$0\r\n\r\n", result);
    }

    @Test
    @DisplayName("ECHO: With multi-byte message")
    void testEchoWithMultiByteMessage() {
        List<String> args = Collections.singletonList("hello world!");
        String result = command.execute(args, mockCtx);
        assertEquals("$12\r\nhello world!\r\n", result);
    }

    @Test
    @DisplayName("ECHO: With special characters")
    void testEchoWithSpecialChars() {
        List<String> args = Collections.singletonList("hello\r\nworld");
        String result = command.execute(args, mockCtx);
        assertEquals("$12\r\nhello\r\nworld\r\n", result);
    }

    @Test
    @DisplayName("ECHO: With long message")
    void testEchoWithLongMessage() {
        String longMessage = "a".repeat(1000);
        List<String> args = Collections.singletonList(longMessage);
        String result = command.execute(args, mockCtx);
        assertEquals("$1000\r\n" + longMessage + "\r\n", result);
    }

    @Test
    @DisplayName("ECHO: No arguments returns error")
    void testEchoNoArgs() {
        List<String> args = Collections.emptyList();
        String result = command.execute(args, mockCtx);
        assertEquals("-ERR wrong number of arguments for 'ECHO' command\r\n", result);
    }

    @Test
    @DisplayName("ECHO: Null arguments returns error")
    void testEchoNullArgs() {
        String result = command.execute(null, mockCtx);
        assertEquals("-ERR wrong number of arguments for 'ECHO' command\r\n", result);
    }

    @Test
    @DisplayName("ECHO: Multiple arguments returns error")
    void testEchoMultipleArgs() {
        List<String> args = java.util.Arrays.asList("hello", "world");
        String result = command.execute(args, mockCtx);
        assertEquals("-ERR wrong number of arguments for 'ECHO' command\r\n", result);
    }

    @Test
    @DisplayName("ECHO: Name returns ECHO")
    void testEchoName() {
        assertEquals("ECHO", command.name());
    }
}
