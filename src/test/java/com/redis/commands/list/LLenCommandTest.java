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

class LLenCommandTest {

    @Mock
    ChannelHandlerContext ctx;

    private LLenCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new LLenCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testName() {
        assertEquals("LLEN", command.name());
    }

    @Test
    void testWrongArgs() {
        assertEquals("-ERR wrong number of arguments for 'LLEN' command\r\n", command.execute(List.of(), ctx));
        assertEquals("-ERR wrong number of arguments for 'LLEN' command\r\n", command.execute(List.of("a", "b"), ctx));
    }

    @Test
    void testNonExistentKeyReturnsZero() {
        db.remove("no-such-list");
        String resp = command.execute(List.of("no-such-list"), ctx);
        assertEquals(":0\r\n", resp);
    }

    @Test
    void testWrongTypeReturnsError() {
        db.remove("mykey");
        db.put("mykey", "a-string");
        String resp = command.execute(List.of("mykey"), ctx);
        assertEquals("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", resp);
        db.remove("mykey");
    }

    @Test
    void testEmptyList() {
        db.remove("empty");
        db.put("empty", RedisValue.list(new LinkedList<>()));
        String resp = command.execute(List.of("empty"), ctx);
        assertEquals(":0\r\n", resp);
        db.remove("empty");
    }

    @Test
    void testPopulatedList() {
        db.remove("alist");
        LinkedList<String> list = new LinkedList<>(List.of("a","b","c"));
        db.put("alist", RedisValue.list(list));
        String resp = command.execute(List.of("alist"), ctx);
        assertEquals(":3\r\n", resp);
        db.remove("alist");
    }

    @Test
    void testConcurrentSafePreserveTTLWithLPush() throws InterruptedException {
        // Set a list with TTL and perform LPUSH (uses compute) and ensure TTL preserved
        db.remove("ttl-list");
        LinkedList<String> list = new LinkedList<>(List.of("x"));
        db.put("ttl-list", RedisValue.list(list), 2000); // 2 seconds

        // perform LPUSH via command which uses compute
        LPushCommand lpush = new LPushCommand();
        String result = lpush.execute(List.of("ttl-list", "y"), ctx);
        assertTrue(result.startsWith(":"));

        // Immediately check value still has TTL (can't access expiry directly, but expiry manager will remove after TTL)
        // Sleep slightly less than TTL and ensure key still exists
        Thread.sleep(1000);
        assertNotNull(db.getValue("ttl-list"));

        // Sleep until after TTL
        Thread.sleep(1500);
        assertNull(db.getValue("ttl-list"));
    }
}
