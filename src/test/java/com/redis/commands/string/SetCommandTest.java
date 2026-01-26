package com.redis.commands.string;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for SET command with all options (EX, PX, NX, XX)
 *
 * Test Coverage:
 * - Basic SET operation
 * - SET with EX (seconds)
 * - SET with PX (milliseconds)
 * - SET with NX (only if not exists)
 * - SET with XX (only if exists)
 * - Combined options
 * - Error cases
 * - Edge cases
 */
@DisplayName("SET Command Unit Tests")
public class SetCommandTest {

    private SetCommand command;
    private ChannelHandlerContext mockCtx;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        command = new SetCommand();
        mockCtx = mock(ChannelHandlerContext.class);
        db = RedisDatabase.getInstance();
        // Clear database before each test
        try {
            // We can't easily flush, so just remove test keys
        } catch (Exception e) {
            // Ignore
        }
    }

    @Test
    @DisplayName("SET: Basic set without options")
    void testSetBasic() {
        List<String> args = Arrays.asList("test_key", "test_value");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        // Verify value was set
        String retrieved = db.get("test_key");
        assertEquals("test_value", retrieved);
    }

    @Test
    @DisplayName("SET: Overwrite existing key")
    void testSetOverwrite() {
        db.put("overwrite_key", "old_value");
        List<String> args = Arrays.asList("overwrite_key", "new_value");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("overwrite_key");
        assertEquals("new_value", retrieved);
    }

    @Test
    @DisplayName("SET: EX option with valid seconds")
    void testSetWithExValid() {
        List<String> args = Arrays.asList("ex_key", "ex_value", "EX", "60");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("ex_key");
        assertEquals("ex_value", retrieved);
    }

    @Test
    @DisplayName("SET: EX option with invalid (non-numeric) value")
    void testSetWithExInvalid() {
        List<String> args = Arrays.asList("bad_ex", "value", "EX", "notanumber");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: EX option with negative value")
    void testSetWithExNegative() {
        List<String> args = Arrays.asList("neg_ex", "value", "EX", "-1");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: PX option with valid milliseconds")
    void testSetWithPxValid() {
        List<String> args = Arrays.asList("px_key", "px_value", "PX", "5000");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("px_key");
        assertEquals("px_value", retrieved);
    }

    @Test
    @DisplayName("SET: PX option with invalid value")
    void testSetWithPxInvalid() {
        List<String> args = Arrays.asList("bad_px", "value", "PX", "invalid");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: NX option on new key (should succeed)")
    void testSetNxNewKey() {
        List<String> args = Arrays.asList("nx_new", "nx_value", "NX");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("nx_new");
        assertEquals("nx_value", retrieved);
    }

    @Test
    @DisplayName("SET: NX option on existing key (should fail)")
    void testSetNxExistingKey() {
        db.put("nx_exist", "old_value");
        List<String> args = Arrays.asList("nx_exist", "new_value", "NX");
        String result = command.execute(args, mockCtx);
        assertEquals("$-1\r\n", result);

        // Value should not change
        String retrieved = db.get("nx_exist");
        assertEquals("old_value", retrieved);
    }

    @Test
    @DisplayName("SET: XX option on existing key (should succeed)")
    void testSetXxExistingKey() {
        db.put("xx_exist", "old_value");
        List<String> args = Arrays.asList("xx_exist", "updated_value", "XX");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("xx_exist");
        assertEquals("updated_value", retrieved);
    }

    @Test
    @DisplayName("SET: XX option on non-existent key (should fail)")
    void testSetXxNonExistent() {
        List<String> args = Arrays.asList("xx_new", "value", "XX");
        String result = command.execute(args, mockCtx);
        assertEquals("$-1\r\n", result);

        // Key should not exist
        String retrieved = db.get("xx_new");
        assertNull(retrieved);
    }

    @Test
    @DisplayName("SET: NX + EX combined")
    void testSetNxExCombined() {
        List<String> args = Arrays.asList("combo_nx_ex", "value", "NX", "EX", "30");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("combo_nx_ex");
        assertEquals("value", retrieved);
    }

    @Test
    @DisplayName("SET: XX + PX combined")
    void testSetXxPxCombined() {
        db.put("combo_xx_px", "old");
        List<String> args = Arrays.asList("combo_xx_px", "new", "XX", "PX", "10000");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("combo_xx_px");
        assertEquals("new", retrieved);
    }

    @Test
    @DisplayName("SET: NX and XX together (conflict - error)")
    void testSetNxXxConflict() {
        List<String> args = Arrays.asList("conflict", "value", "NX", "XX");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: Invalid option")
    void testSetInvalidOption() {
        List<String> args = Arrays.asList("key", "value", "INVALID");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: Missing value")
    void testSetMissingValue() {
        List<String> args = Collections.singletonList("key_only");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: Missing expiry value for EX")
    void testSetMissingExValue() {
        List<String> args = Arrays.asList("key", "value", "EX");
        String result = command.execute(args, mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("SET: Large value")
    void testSetLargeValue() {
        String largeValue = "x".repeat(10000);
        List<String> args = Arrays.asList("large_key", largeValue);
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("large_key");
        assertEquals(largeValue, retrieved);
    }

    @Test
    @DisplayName("SET: Empty string value")
    void testSetEmptyValue() {
        List<String> args = Arrays.asList("empty_key", "");
        String result = command.execute(args, mockCtx);
        assertEquals("+OK\r\n", result);

        String retrieved = db.get("empty_key");
        assertEquals("", retrieved);
    }

    @Test
    @DisplayName("SET: Case insensitive options")
    void testSetCaseInsensitiveOptions() {
        List<String> args1 = Arrays.asList("k1", "v1", "nx");
        String result1 = command.execute(args1, mockCtx);
        assertEquals("+OK\r\n", result1);

        List<String> args2 = Arrays.asList("k2", "v2", "Ex", "60");
        String result2 = command.execute(args2, mockCtx);
        assertEquals("+OK\r\n", result2);
    }

    @Test
    @DisplayName("SET: Name returns SET")
    void testSetName() {
        assertEquals("SET", command.name());
    }
}
