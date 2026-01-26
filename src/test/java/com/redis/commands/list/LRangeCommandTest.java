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

import static org.junit.jupiter.api.Assertions.assertEquals;

class LRangeCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private LRangeCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new LRangeCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testName() {
        assertEquals("LRANGE", command.name());
    }

    @Test
    void testWrongNumberOfArgs_TooFew() {
        assertEquals("-ERR wrong number of arguments for 'LRANGE' command\r\n",
            command.execute(List.of("key"), ctx));
        assertEquals("-ERR wrong number of arguments for 'LRANGE' command\r\n",
            command.execute(List.of("key", "0"), ctx));
    }

    @Test
    void testWrongNumberOfArgs_TooMany() {
        assertEquals("-ERR wrong number of arguments for 'LRANGE' command\r\n",
            command.execute(List.of("key", "0", "1", "extra"), ctx));
    }

    @Test
    void testInvalidIndex_NotInteger() {
        assertEquals("-ERR value is not an integer or out of range\r\n",
            command.execute(List.of("key", "abc", "1"), ctx));
        assertEquals("-ERR value is not an integer or out of range\r\n",
            command.execute(List.of("key", "0", "xyz"), ctx));
    }

    @Test
    void testNonExistentKey_ReturnsEmptyArray() {
        db.remove("nonexistent");
        assertEquals("*0\r\n", command.execute(List.of("nonexistent", "0", "-1"), ctx));
    }

    @Test
    void testWrongType_ReturnsError() {
        db.remove("stringkey");
        db.put("stringkey", "value");

        assertEquals("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            command.execute(List.of("stringkey", "0", "-1"), ctx));

        db.remove("stringkey");
    }

    @Test
    void testBasicRange() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c", "d", "e"));
        db.put("mylist", RedisValue.list(list));

        // LRANGE mylist 0 2 -> a, b, c
        String result = command.execute(List.of("mylist", "0", "2"), ctx);
        assertEquals("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", result);

        db.remove("mylist");
    }

    @Test
    void testFullRange() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("one", "two", "three"));
        db.put("mylist", RedisValue.list(list));

        // LRANGE mylist 0 -1 -> all elements
        String result = command.execute(List.of("mylist", "0", "-1"), ctx);
        assertEquals("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", result);

        db.remove("mylist");
    }

    @Test
    void testNegativeIndices() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c", "d", "e"));
        db.put("mylist", RedisValue.list(list));

        // LRANGE mylist -3 -1 -> c, d, e
        String result = command.execute(List.of("mylist", "-3", "-1"), ctx);
        assertEquals("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n", result);

        db.remove("mylist");
    }

    @Test
    void testStartGreaterThanOrEqualToSize_ReturnsEmpty() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        // Start index >= size
        assertEquals("*0\r\n", command.execute(List.of("mylist", "3", "5"), ctx));
        assertEquals("*0\r\n", command.execute(List.of("mylist", "10", "20"), ctx));

        db.remove("mylist");
    }

    @Test
    void testStopGreaterThanOrEqualToSize_ClampedToLastElement() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        // Stop index > size, should return all from start
        String result = command.execute(List.of("mylist", "0", "100"), ctx);
        assertEquals("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", result);

        db.remove("mylist");
    }

    @Test
    void testStartGreaterThanStop_ReturnsEmpty() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        // Start > stop after normalization
        assertEquals("*0\r\n", command.execute(List.of("mylist", "2", "1"), ctx));

        db.remove("mylist");
    }

    @Test
    void testNegativeStartClampedToZero() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        // Start -100 on 3-element list -> clamped to 0
        String result = command.execute(List.of("mylist", "-100", "2"), ctx);
        assertEquals("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", result);

        db.remove("mylist");
    }

    @Test
    void testSingleElement() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        // Single element range
        String result = command.execute(List.of("mylist", "1", "1"), ctx);
        assertEquals("*1\r\n$1\r\nb\r\n", result);

        db.remove("mylist");
    }

    @Test
    void testEmptyList() {
        db.remove("emptylist");
        LinkedList<String> list = new LinkedList<>();
        db.put("emptylist", RedisValue.list(list));

        assertEquals("*0\r\n", command.execute(List.of("emptylist", "0", "-1"), ctx));

        db.remove("emptylist");
    }
}
