package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for GET command
 */
@DisplayName("GET Command Unit Tests")
public class GetCommandTest {

    private GetCommand command;
    private ChannelHandlerContext mockCtx;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        command = new GetCommand();
        mockCtx = mock(ChannelHandlerContext.class);
        db = RedisDatabase.getInstance();
    }

    @Test
    @DisplayName("GET: Retrieve existing key")
    void testGetExisting() {
        db.put("get_test", "test_value");
        List<String> args = Collections.singletonList("get_test");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("test_value"));
    }

    @Test
    @DisplayName("GET: Non-existent key returns nil")
    void testGetNonExistent() {
        List<String> args = Collections.singletonList("nonexistent_key_xyz");
        String result = command.execute(args, mockCtx);
        assertEquals("$-1\r\n", result);
    }

    @Test
    @DisplayName("GET: Expired key returns nil")
    void testGetExpired() throws InterruptedException {
        db.put("exp_key", "value", 100);  // 100ms TTL
        Thread.sleep(150);

        List<String> args = Collections.singletonList("exp_key");
        String result = command.execute(args, mockCtx);
        assertEquals("$-1\r\n", result);
    }

    @Test
    @DisplayName("GET: Large value")
    void testGetLargeValue() {
        String largeValue = "y".repeat(50000);
        db.put("large", largeValue);

        List<String> args = Collections.singletonList("large");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains(largeValue));
    }

    @Test
    @DisplayName("GET: Empty value")
    void testGetEmptyValue() {
        db.put("empty", "");
        List<String> args = Collections.singletonList("empty");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("$0"));
    }

    @Test
    @DisplayName("GET: No arguments (error)")
    void testGetNoArgs() {
        List<String> args = Collections.emptyList();
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("GET: Too many arguments (error)")
    void testGetTooManyArgs() {
        List<String> args = java.util.Arrays.asList("key", "extra_arg");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("GET: Name returns GET")
    void testGetName() {
        assertEquals("GET", command.name());
    }
}
