package com.redis.commands;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExpireTtlCommandTest {

    private RedisDatabase db;
    private ChannelHandlerContext ctx;

    @BeforeEach
    public void setUp() {
        db = RedisDatabase.getInstance();
        ctx = Mockito.mock(ChannelHandlerContext.class);
    }

    @Test
    public void testExpireAndTtl() throws InterruptedException {
        String key = "expireKey";
        db.put(key, "value");

        // Initial TTL should be -1
        TtlCommand ttl = new TtlCommand();
        assertEquals(":-1\r\n", ttl.execute(List.of(key), ctx));

        // Set EXPIRE 2 seconds
        ExpireCommand expire = new ExpireCommand();
        assertEquals(":1\r\n", expire.execute(List.of(key, "2"), ctx));

        // TTL should be around 2
        String ttlResult = ttl.execute(List.of(key), ctx);
        assertTrue(ttlResult.equals(":1\r\n") || ttlResult.equals(":2\r\n"));

        // Wait 3 seconds
        Thread.sleep(3000);

        // TTL should be -2 (expired)
        assertEquals(":-2\r\n", ttl.execute(List.of(key), ctx));
        
        // Key should be gone
        assertEquals(null, db.get(key));
    }
}
