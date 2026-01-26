package com.redis.commands.list;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LPushCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private LPushCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new LPushCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testName() {
        assertEquals("LPUSH", command.name());
    }

    @Test
    void testWrongNumberOfArgs_NoArgs() {
        String result = command.execute(List.of(), ctx);
        assertEquals("-ERR wrong number of arguments for 'LPUSH' command\r\n", result);
    }

    @Test
    void testWrongNumberOfArgs_OnlyKey() {
        String result = command.execute(List.of("mylist"), ctx);
        assertEquals("-ERR wrong number of arguments for 'LPUSH' command\r\n", result);
    }

    @Test
    void testLPush_NewList_SingleElement() {
        db.remove("lpushtest1");

        String result = command.execute(List.of("lpushtest1", "value1"), ctx);
        assertEquals(":1\r\n", result);

        RedisValue value = db.getValue("lpushtest1");
        assertNotNull(value);
        assertEquals(RedisValue.Type.LIST, value.getType());
        assertEquals(1, value.asList().size());
        assertEquals("value1", value.asList().getFirst());

        db.remove("lpushtest1");
    }

    @Test
    void testLPush_NewList_MultipleElements() {
        db.remove("lpushtest2");

        // LPUSH adds elements to head, so order is reversed
        String result = command.execute(List.of("lpushtest2", "a", "b", "c"), ctx);
        assertEquals(":3\r\n", result);

        RedisValue value = db.getValue("lpushtest2");
        List<String> list = value.asList();
        assertEquals(3, list.size());
        // Elements are pushed left-to-right, each at head
        // So "a" pushed first, then "b" at head, then "c" at head
        assertEquals("c", list.get(0));
        assertEquals("b", list.get(1));
        assertEquals("a", list.get(2));

        db.remove("lpushtest2");
    }

    @Test
    void testLPush_ExistingList_Prepends() {
        db.remove("lpushtest3");

        // First push
        command.execute(List.of("lpushtest3", "first"), ctx);

        // Second push - should prepend
        String result = command.execute(List.of("lpushtest3", "second", "third"), ctx);
        assertEquals(":3\r\n", result);

        List<String> list = db.getValue("lpushtest3").asList();
        assertEquals(3, list.size());
        // "third" is at head (pushed last), then "second", then "first" at tail
        assertEquals("third", list.get(0));
        assertEquals("second", list.get(1));
        assertEquals("first", list.get(2));

        db.remove("lpushtest3");
    }

    @Test
    void testLPush_WrongType_ReturnsError() {
        db.remove("stringkey");
        db.put("stringkey", "stringvalue");

        String result = command.execute(List.of("stringkey", "element"), ctx);
        assertEquals("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", result);

        db.remove("stringkey");
    }

    @Test
    void testLPush_OrderPreserved_LIFO() {
        db.remove("ordertest");

        // Push elements one by one
        command.execute(List.of("ordertest", "1"), ctx);
        command.execute(List.of("ordertest", "2"), ctx);
        command.execute(List.of("ordertest", "3"), ctx);

        List<String> list = db.getValue("ordertest").asList();
        // LPUSH is LIFO - last pushed is first
        assertEquals(List.of("3", "2", "1"), list);

        db.remove("ordertest");
    }

    @Test
    void testLPush_IsMutableAndThreadSafe() {
        db.remove("mutabletest");

        command.execute(List.of("mutabletest", "element"), ctx);

        RedisValue value = db.getValue("mutabletest");
        List<String> list = value.asList();
        
        // Should be mutable
        assertDoesNotThrow(() -> list.add("new element"));
        assertEquals(2, list.size());

        db.remove("mutabletest");
    }

    @Test
    void testLPush_CombinedWithRPush() {
        db.remove("combinedtest");

        // RPUSH first
        RPushCommand rpush = new RPushCommand();
        rpush.execute(List.of("combinedtest", "middle"), ctx);

        // LPUSH to head
        command.execute(List.of("combinedtest", "head"), ctx);

        // RPUSH to tail
        rpush.execute(List.of("combinedtest", "tail"), ctx);

        List<String> list = db.getValue("combinedtest").asList();
        assertEquals(List.of("head", "middle", "tail"), list);

        db.remove("combinedtest");
    }

    @Test
    void testLPush_ReturnsCorrectSize() {
        db.remove("sizetest");

        assertEquals(":1\r\n", command.execute(List.of("sizetest", "a"), ctx));
        assertEquals(":2\r\n", command.execute(List.of("sizetest", "b"), ctx));
        assertEquals(":5\r\n", command.execute(List.of("sizetest", "c", "d", "e"), ctx));

        db.remove("sizetest");
    }
}
