package com.redis.commands.generic;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for TYPE command
 */
@DisplayName("TYPE Command Unit Tests")
public class TypeCommandTest {

    private TypeCommand command;
    private ChannelHandlerContext mockCtx;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        command = new TypeCommand();
        mockCtx = mock(ChannelHandlerContext.class);
        db = RedisDatabase.getInstance();
    }

    @Test
    @DisplayName("TYPE: Non-existent key returns none")
    void testTypeNonExistent() {
        db.remove("nonexistent");
        String result = command.execute(Collections.singletonList("nonexistent"), mockCtx);
        assertEquals("+none\r\n", result);
    }

    @Test
    @DisplayName("TYPE: String value returns string")
    void testTypeString() {
        db.put("key_str", "value");
        String result = command.execute(Collections.singletonList("key_str"), mockCtx);
        assertEquals("+string\r\n", result);
    }

    @Test
    @DisplayName("TYPE: List value returns list")
    void testTypeList() {
        db.put("key_list", RedisValue.list(List.of("a", "b")));
        String result = command.execute(Collections.singletonList("key_list"), mockCtx);
        assertEquals("+list\r\n", result);
    }

    @Test
    @DisplayName("TYPE: Set value returns set")
    void testTypeSet() {
        db.put("key_set", RedisValue.set(Set.of("a", "b")));
        String result = command.execute(Collections.singletonList("key_set"), mockCtx);
        assertEquals("+set\r\n", result);
    }

    @Test
    @DisplayName("TYPE: Hash value returns hash")
    void testTypeHash() {
        db.put("key_hash", RedisValue.hash(Map.of("f1", "v1")));
        String result = command.execute(Collections.singletonList("key_hash"), mockCtx);
        assertEquals("+hash\r\n", result);
    }

    @Test
    @DisplayName("TYPE: Sorted Set value returns zset")
    void testTypeSortedSet() {
        db.put("key_zset", RedisValue.sortedSet(Map.of("m1", 1.0)));
        String result = command.execute(Collections.singletonList("key_zset"), mockCtx);
        assertEquals("+zset\r\n", result);
    }

    @Test
    @DisplayName("TYPE: Wrong number of arguments")
    void testTypeWrongArgs() {
        String result = command.execute(List.of("k1", "k2"), mockCtx);
        assertEquals("-ERR wrong number of arguments for 'TYPE' command\r\n", result);
    }
}
