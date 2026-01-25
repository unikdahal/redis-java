package com.redis.commands;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class BLPopCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private BLPopCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new BLPopCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testName() {
        assertEquals("BLPOP", command.name());
    }

    @Test
    void testWrongNumberOfArgs_NoArgs() {
        assertEquals("-ERR wrong number of arguments for 'BLPOP' command\r\n",
                command.execute(List.of(), ctx));
    }

    @Test
    void testWrongNumberOfArgs_OnlyTimeout() {
        assertEquals("-ERR wrong number of arguments for 'BLPOP' command\r\n",
                command.execute(List.of("0"), ctx));
    }

    @Test
    void testInvalidTimeout_NotNumber() {
        assertEquals("-ERR timeout is not a float or out of range\r\n",
                command.execute(List.of("mykey", "abc"), ctx));
    }

    @Test
    void testInvalidTimeout_Negative() {
        assertEquals("-ERR timeout is not a float or out of range\r\n",
                command.execute(List.of("mykey", "-1"), ctx));
    }

    @Test
    void testNonExistentKey_ZeroTimeout_ReturnsNil() {
        db.remove("nonexistent");
        String result = command.execute(List.of("nonexistent", "0"), ctx);
        assertEquals("*-1\r\n", result);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void testNonExistentKey_ShortTimeout_ReturnsNil() {
        db.remove("nonexistent");
        long start = System.currentTimeMillis();
        String result = command.execute(List.of("nonexistent", "0.1"), ctx);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("*-1\r\n", result);
        assertTrue(elapsed >= 80 && elapsed < 500, "Should wait approximately 100ms");
    }

    @Test
    void testExistingList_ReturnsKeyAndElement() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("first", "second"));
        db.put("mylist", RedisValue.list(list));

        String result = command.execute(List.of("mylist", "0"), ctx);

        // Should return *2\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n
        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("mylist"));
        assertTrue(result.contains("first"));

        // Verify element was removed
        List<String> remaining = db.getValue("mylist").asList();
        assertEquals(1, remaining.size());
        assertEquals("second", remaining.get(0));

        db.remove("mylist");
    }

    @Test
    void testMultipleKeys_FirstWithData() {
        db.remove("key1");
        db.remove("key2");
        db.remove("key3");

        LinkedList<String> list = new LinkedList<>(List.of("value1"));
        db.put("key1", RedisValue.list(list));

        String result = command.execute(List.of("key1", "key2", "key3", "0"), ctx);

        assertTrue(result.contains("key1"));
        assertTrue(result.contains("value1"));

        db.remove("key1");
    }

    @Test
    void testMultipleKeys_SecondWithData() {
        db.remove("key1");
        db.remove("key2");
        db.remove("key3");

        LinkedList<String> list = new LinkedList<>(List.of("value2"));
        db.put("key2", RedisValue.list(list));

        String result = command.execute(List.of("key1", "key2", "key3", "0"), ctx);

        assertTrue(result.contains("key2"));
        assertTrue(result.contains("value2"));

        db.remove("key2");
    }

    @Test
    void testMultipleKeys_AllEmpty_ZeroTimeout() {
        db.remove("key1");
        db.remove("key2");

        String result = command.execute(List.of("key1", "key2", "0"), ctx);
        assertEquals("*-1\r\n", result);
    }

    @Test
    void testWrongType_SkipsToNextKey() {
        db.remove("stringkey");
        db.remove("listkey");

        db.put("stringkey", "notalist");
        LinkedList<String> list = new LinkedList<>(List.of("element"));
        db.put("listkey", RedisValue.list(list));

        String result = command.execute(List.of("stringkey", "listkey", "0"), ctx);

        // Should skip stringkey and pop from listkey
        assertTrue(result.contains("listkey"));
        assertTrue(result.contains("element"));

        db.remove("stringkey");
        db.remove("listkey");
    }

    @Test
    void testPopLastElement_RemovesKey() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("only"));
        db.put("mylist", RedisValue.list(list));

        command.execute(List.of("mylist", "0"), ctx);

        assertNull(db.getValue("mylist"));
    }

    @Test
    void testEmptyList_ReturnsNil() {
        db.remove("emptylist");
        LinkedList<String> list = new LinkedList<>();
        db.put("emptylist", RedisValue.list(list));

        String result = command.execute(List.of("emptylist", "0"), ctx);
        assertEquals("*-1\r\n", result);
    }

    @Test
    void testDecimalTimeout() {
        db.remove("nonexistent");

        long start = System.currentTimeMillis();
        command.execute(List.of("nonexistent", "0.2"), ctx);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed >= 150 && elapsed < 500);
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    void testBlockingWithDataArrival() throws Exception {
        db.remove("blocktest");

        // Start BLPOP in a separate thread
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(() ->
            command.execute(List.of("blocktest", "2"), ctx)
        );

        // Wait a bit, then push data
        Thread.sleep(100);
        LPushCommand lpush = new LPushCommand();
        lpush.execute(List.of("blocktest", "arrived"), ctx);

        // BLPOP should return with the data
        String result = future.get(2, TimeUnit.SECONDS);
        assertTrue(result.contains("blocktest"));
        assertTrue(result.contains("arrived"));

        executor.shutdown();
        db.remove("blocktest");
    }

    @Test
    void testPriorityOrder_FirstKeyWins() {
        db.remove("key1");
        db.remove("key2");

        LinkedList<String> list1 = new LinkedList<>(List.of("val1"));
        LinkedList<String> list2 = new LinkedList<>(List.of("val2"));
        db.put("key1", RedisValue.list(list1));
        db.put("key2", RedisValue.list(list2));

        String result = command.execute(List.of("key1", "key2", "0"), ctx);

        // key1 should be checked first
        assertTrue(result.contains("key1"));
        assertTrue(result.contains("val1"));

        // key2 should still have data
        assertNotNull(db.getValue("key2"));

        db.remove("key1");
        db.remove("key2");
    }

    @Test
    void testSequentialBLPops() {
        db.remove("mylist");
        LinkedList<String> list = new LinkedList<>(List.of("a", "b", "c"));
        db.put("mylist", RedisValue.list(list));

        String r1 = command.execute(List.of("mylist", "0"), ctx);
        assertTrue(r1.contains("a"));

        String r2 = command.execute(List.of("mylist", "0"), ctx);
        assertTrue(r2.contains("b"));

        String r3 = command.execute(List.of("mylist", "0"), ctx);
        assertTrue(r3.contains("c"));

        String r4 = command.execute(List.of("mylist", "0"), ctx);
        assertEquals("*-1\r\n", r4);

        db.remove("mylist");
    }
}
