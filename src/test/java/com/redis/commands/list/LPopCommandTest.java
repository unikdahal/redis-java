package com.redis.commands.list;
import com.redis.commands.generic.*;
import com.redis.commands.string.*;
import com.redis.commands.list.*;

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

class LPopCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private LPopCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new LPopCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testName() {
        assertEquals("LPOP", command.name());
    }

    @Test
    void testWrongNumberOfArgs_NoArgs() {
        assertEquals("-ERR wrong number of arguments for 'LPOP' command\r\n",
                command.execute(List.of(), ctx));
    }

    @Test
    void testWrongNumberOfArgs_TooMany() {
        assertEquals("-ERR wrong number of arguments for 'LPOP' command\r\n",
                command.execute(List.of("key", "1", "extra"), ctx));
    }

    @Test
    void testNonExistentKey_ReturnsNil() {
        db.remove("nonexistent");
        assertEquals("$-1\r\n", command.execute(List.of("nonexistent"), ctx));
    }

    @Test
    void testNonExistentKey_WithCount_ReturnsEmptyArray() {
        db.remove("nonexistent");
        assertEquals("*0\r\n", command.execute(List.of("nonexistent", "3"), ctx));
    }

    @Test
    void testWrongType_ReturnsError() {
        db.remove("stringkey");
        db.put("stringkey", "value");
        assertEquals("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                command.execute(List.of("stringkey"), ctx));
        db.remove("stringkey");
    }

    @Test
    void testInvalidCount_NotInteger() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b"));
        db.put("mylist", RedisValue.list(list));

        assertEquals("-ERR value is not an integer or out of range\r\n",
                command.execute(List.of("mylist", "abc"), ctx));
        db.remove("mylist");
    }

    @Test
    void testInvalidCount_Negative() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b"));
        db.put("mylist", RedisValue.list(list));

        assertEquals("-ERR value is not an integer or out of range\r\n",
                command.execute(List.of("mylist", "-1"), ctx));
        db.remove("mylist");
    }

    @Test
    void testCount_Zero_ReturnsEmptyArray() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b"));
        db.put("mylist", RedisValue.list(list));

        assertEquals("*0\r\n", command.execute(List.of("mylist", "0"), ctx));

        // List should be unchanged
        assertEquals(2, db.getValue("mylist").asList().size());
        db.remove("mylist");
    }

    @Test
    void testSinglePop_ReturnsFirstElement() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("first", "second", "third"));
        db.put("mylist", RedisValue.list(list));

        String result = command.execute(List.of("mylist"), ctx);
        assertEquals("$5\r\nfirst\r\n", result);

        // Verify list state
        List<String> remaining = db.getValue("mylist").asList();
        assertEquals(2, remaining.size());
        assertEquals("second", remaining.get(0));
        assertEquals("third", remaining.get(1));

        db.remove("mylist");
    }

    @Test
    void testMultiplePop_ReturnsArray() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c", "d", "e"));
        db.put("mylist", RedisValue.list(list));

        String result = command.execute(List.of("mylist", "3"), ctx);
        assertTrue(result.startsWith("*3\r\n"));
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));

        // Verify list state
        List<String> remaining = db.getValue("mylist").asList();
        assertEquals(2, remaining.size());
        assertEquals("d", remaining.get(0));
        assertEquals("e", remaining.get(1));

        db.remove("mylist");
    }

    @Test
    void testPopMoreThanAvailable_ReturnsAllElements() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("x", "y"));
        db.put("mylist", RedisValue.list(list));

        String result = command.execute(List.of("mylist", "10"), ctx);
        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("x"));
        assertTrue(result.contains("y"));

        // Key should be removed (list is empty)
        assertNull(db.getValue("mylist"));
    }

    @Test
    void testPopLastElement_RemovesKey() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("only"));
        db.put("mylist", RedisValue.list(list));

        String result = command.execute(List.of("mylist"), ctx);
        assertEquals("$4\r\nonly\r\n", result);

        // Key should be removed
        assertNull(db.getValue("mylist"));
    }

    @Test
    void testPopAllElements_WithCount_RemovesKey() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        command.execute(List.of("mylist", "3"), ctx);

        // Key should be removed
        assertNull(db.getValue("mylist"));
    }

    @Test
    void testEmptyList_ReturnsNil() {
        db.remove("emptylist");
        LinkedList<String> list = new LinkedList<>();
        db.put("emptylist", RedisValue.list(list));

        assertEquals("$-1\r\n", command.execute(List.of("emptylist"), ctx));

        // Empty list should be removed
        assertNull(db.getValue("emptylist"));
    }

    @Test
    void testSequentialPops() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("1", "2", "3", "4", "5"));
        db.put("mylist", RedisValue.list(list));

        assertEquals("$1\r\n1\r\n", command.execute(List.of("mylist"), ctx));
        assertEquals("$1\r\n2\r\n", command.execute(List.of("mylist"), ctx));
        assertEquals("$1\r\n3\r\n", command.execute(List.of("mylist"), ctx));
        assertEquals("$1\r\n4\r\n", command.execute(List.of("mylist"), ctx));
        assertEquals("$1\r\n5\r\n", command.execute(List.of("mylist"), ctx));
        assertEquals("$-1\r\n", command.execute(List.of("mylist"), ctx));
    }

    @Test
    void testLPushThenLPop_LIFO() {
        db.remove("stack");

        LPushCommand lpush = new LPushCommand();
        lpush.execute(List.of("stack", "first"), ctx);
        lpush.execute(List.of("stack", "second"), ctx);
        lpush.execute(List.of("stack", "third"), ctx);

        // LPUSH pushes to head, LPOP pops from head -> LIFO
        assertEquals("$5\r\nthird\r\n", command.execute(List.of("stack"), ctx));
        assertEquals("$6\r\nsecond\r\n", command.execute(List.of("stack"), ctx));
        assertEquals("$5\r\nfirst\r\n", command.execute(List.of("stack"), ctx));

        db.remove("stack");
    }
}
