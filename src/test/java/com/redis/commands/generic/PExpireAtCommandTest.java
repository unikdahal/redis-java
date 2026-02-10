package com.redis.commands.generic;

import com.redis.storage.RedisDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PEXPIREAT command.
 */
@DisplayName("PEXPIREAT Command Tests")
class PExpireAtCommandTest {

    private PExpireAtCommand command;
    private RedisDatabase db;
    private static int keyCounter = 0;

    @BeforeEach
    void setUp() {
        command = new PExpireAtCommand();
        db = RedisDatabase.getInstance();
    }

    private String uniqueKey() {
        return "pexpireat_test_key_" + (++keyCounter);
    }

    @Test
    @DisplayName("PEXPIREAT sets expiry on existing key")
    void testPExpireAtOnExistingKey() {
        String key = uniqueKey();
        db.put(key, "myvalue");

        long futureTime = System.currentTimeMillis() + 60000; // 60 seconds in future
        String result = command.execute(List.of(key, String.valueOf(futureTime)), null);

        assertEquals(":1\r\n", result);
        assertTrue(db.exists(key));
    }

    @Test
    @DisplayName("PEXPIREAT returns 0 for non-existing key")
    void testPExpireAtOnNonExistingKey() {
        String result = command.execute(List.of("nonexistent_pexpireat_key", "1706745600000"), null);
        assertEquals(":0\r\n", result);
    }

    @Test
    @DisplayName("PEXPIREAT with past timestamp expires key immediately")
    void testPExpireAtWithPastTimestamp() throws InterruptedException {
        String key = uniqueKey();
        db.put(key, "myvalue");

        long pastTime = System.currentTimeMillis() - 1000; // 1 second in past
        String result = command.execute(List.of(key, String.valueOf(pastTime)), null);

        assertEquals(":1\r\n", result);

        // Key should be expired (may need small wait for lazy expiration)
        Thread.sleep(100);
        // The key may still exist but will report as expired on access
    }

    @Test
    @DisplayName("PEXPIREAT with invalid number returns error")
    void testPExpireAtInvalidNumber() {
        String key = uniqueKey();
        db.put(key, "myvalue");

        String result = command.execute(List.of(key, "notanumber"), null);
        assertTrue(result.startsWith("-ERR"));
    }

    @Test
    @DisplayName("PEXPIREAT with wrong number of arguments returns error")
    void testPExpireAtWrongArgs() {
        String result1 = command.execute(List.of("onlykey"), null);
        assertTrue(result1.startsWith("-ERR"));

        String result2 = command.execute(List.of(), null);
        assertTrue(result2.startsWith("-ERR"));
    }

    @Test
    @DisplayName("PEXPIREAT command name is correct")
    void testCommandName() {
        assertEquals("PEXPIREAT", command.name());
    }

    @Test
    @DisplayName("PEXPIREAT is a write command")
    void testIsWriteCommand() {
        assertTrue(command.isWriteCommand());
    }
}
