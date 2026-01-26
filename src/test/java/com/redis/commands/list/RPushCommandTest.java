package com.redis.commands.list;
import com.redis.commands.generic.*;
import com.redis.commands.string.*;
import com.redis.commands.list.*;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RPushCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private RPushCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new RPushCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testName() {
        assertEquals("RPUSH", command.name());
    }

    @Test
    void testWrongNumberOfArgs_NoArgs() {
        String result = command.execute(List.of(), ctx);
        assertEquals("-ERR wrong number of arguments for 'RPUSH' command\r\n", result);
    }

    @Test
    void testWrongNumberOfArgs_OnlyKey() {
        String result = command.execute(List.of("mylist"), ctx);
        assertEquals("-ERR wrong number of arguments for 'RPUSH' command\r\n", result);
    }

    @Test
    void testRPush_NewList_SingleElement() {
        db.remove("testlist1");

        String result = command.execute(List.of("testlist1", "value1"), ctx);
        assertEquals(":1\r\n", result);

        RedisValue value = db.getValue("testlist1");
        assertNotNull(value);
        assertEquals(RedisValue.Type.LIST, value.getType());
        assertEquals(1, value.asList().size());
        assertEquals("value1", value.asList().get(0));

        db.remove("testlist1");
    }

    @Test
    void testRPush_NewList_MultipleElements() {
        db.remove("testlist2");

        String result = command.execute(List.of("testlist2", "a", "b", "c"), ctx);
        assertEquals(":3\r\n", result);

        RedisValue value = db.getValue("testlist2");
        List<String> list = value.asList();
        assertEquals(3, list.size());
        assertEquals("a", list.get(0));
        assertEquals("b", list.get(1));
        assertEquals("c", list.get(2));

        db.remove("testlist2");
    }

    @Test
    void testRPush_ExistingList_Appends() {
        db.remove("testlist3");

        command.execute(List.of("testlist3", "first"), ctx);

        String result = command.execute(List.of("testlist3", "second", "third"), ctx);
        assertEquals(":3\r\n", result);

        List<String> list = db.getValue("testlist3").asList();
        assertEquals(3, list.size());
        assertEquals("first", list.get(0));
        assertEquals("second", list.get(1));
        assertEquals("third", list.get(2));

        db.remove("testlist3");
    }

    @Test
    void testRPush_WrongType_ReturnsError() {
        db.remove("stringkey");
        db.put("stringkey", "stringvalue");

        String result = command.execute(List.of("stringkey", "element"), ctx);
        assertEquals("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", result);

        db.remove("stringkey");
    }

    @Test
    void testRPush_OrderPreserved() {
        db.remove("ordertest");

        command.execute(List.of("ordertest", "1"), ctx);
        command.execute(List.of("ordertest", "2"), ctx);
        command.execute(List.of("ordertest", "3"), ctx);

        List<String> list = db.getValue("ordertest").asList();
        assertEquals(List.of("1", "2", "3"), list);

        db.remove("ordertest");
    }

    @Test
    void testRPush_UsesLinkedList() {
        db.remove("linkedtest");

        command.execute(List.of("linkedtest", "element"), ctx);

        RedisValue value = db.getValue("linkedtest");
        assertTrue(value.getData() instanceof LinkedList<?>);

        db.remove("linkedtest");
    }
}
