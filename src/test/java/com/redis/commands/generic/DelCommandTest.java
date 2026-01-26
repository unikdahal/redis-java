package com.redis.commands.generic;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for DEL command
 */
@DisplayName("DEL Command Unit Tests")
public class DelCommandTest {

    private DelCommand command;
    private ChannelHandlerContext mockCtx;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        command = new DelCommand();
        mockCtx = mock(ChannelHandlerContext.class);
        db = RedisDatabase.getInstance();
    }

    @Test
    @DisplayName("DEL: Delete single existing key")
    void testDelSingleExisting() {
        db.put("del_key", "value");
        List<String> args = Collections.singletonList("del_key");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains(":1"));

        assertNull(db.get("del_key"));
    }

    @Test
    @DisplayName("DEL: Non-existent key returns 0")
    void testDelNonExistent() {
        List<String> args = Collections.singletonList("nonexist");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains(":0"));
    }

    @Test
    @DisplayName("DEL: Multiple keys")
    void testDelMultiple() {
        db.put("k1", "v1");
        db.put("k2", "v2");
        db.put("k3", "v3");

        List<String> args = Arrays.asList("k1", "k2", "k3");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains(":3"));

        assertNull(db.get("k1"));
        assertNull(db.get("k2"));
        assertNull(db.get("k3"));
    }

    @Test
    @DisplayName("DEL: Mixed existing and non-existent")
    void testDelMixed() {
        db.put("exist", "value");
        List<String> args = Arrays.asList("exist", "nonexist");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains(":1"));
    }

    @Test
    @DisplayName("DEL: Same key multiple times")
    void testDelSameKeyMultiple() {
        db.put("multi_del", "value");
        List<String> args = Arrays.asList("multi_del", "multi_del", "multi_del");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains(":1"));
    }

    @Test
    @DisplayName("DEL: No arguments (error)")
    void testDelNoArgs() {
        List<String> args = Collections.emptyList();
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("DEL: Name returns DEL")
    void testDelName() {
        assertEquals("DEL", command.name());
    }
}
