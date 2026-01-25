package com.redis.storage;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ExpiryBugTest {

    @Test
    public void testReAddWithoutTTLAfterTTL() throws InterruptedException {
        RedisDatabase db = RedisDatabase.getInstance();
        String key = "bugKey";
        String val1 = "valueWithTTL";
        String val2 = "valueWithoutTTL";

        // 1. Set key with short TTL (100ms)
        db.put(key, val1, 100);
        assertEquals(val1, db.get(key));

        // 2. Immediately overwrite without TTL
        db.put(key, val2);
        assertEquals(val2, db.get(key));

        // 3. Wait for the original TTL to pass (200ms)
        Thread.sleep(200);

        // 4. Check if the key still exists. 
        // BUG: In the current implementation, it will be null because the background 
        // cleaner task for the first 'put' will see that keyExpiryMap still has 
        // the old expiry time (because it wasn't cleared) and remove the key.
        assertEquals(val2, db.get(key), "Key should still exist after overwriting without TTL");
    }

    @Test
    public void testDeleteAfterTTL() throws InterruptedException {
        RedisDatabase db = RedisDatabase.getInstance();
        String key = "bugKeyDelete";
        String val1 = "valueWithTTL";

        // 1. Set key with short TTL (100ms)
        db.put(key, val1, 100);
        
        // 2. Delete the key
        db.remove(key);
        assertNull(db.get(key));

        // 3. Set it again without TTL
        db.put(key, "newVal");

        // 4. Wait for original TTL
        Thread.sleep(200);

        // BUG: Background cleaner might remove it.
        assertEquals("newVal", db.get(key), "Key should still exist even if previously it had a TTL");
    }
}
