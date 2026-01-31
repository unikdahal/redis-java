package com.redis.commands.string;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
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
 * Unit tests for INCR command.
 *
 * Test Coverage:
 * - Basic increment of existing key
 * - Increment of non-existent key (initializes to 0 then increments)
 * - Increment of negative numbers
 * - Error case: non-integer value
 * - Error case: wrong type (e.g., list)
 * - Error case: wrong number of arguments
 * - Edge cases: large numbers, overflow
 */
@DisplayName("INCR Command Unit Tests")
public class IncrCommandTest {

    private IncrCommand command;
    private ChannelHandlerContext mockCtx;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        command = new IncrCommand();
        mockCtx = mock(ChannelHandlerContext.class);
        db = RedisDatabase.getInstance();
    }

    @Test
    @DisplayName("INCR: Increment existing integer key")
    void testIncrExistingKey() {
        // Setup: Set a key with value "10"
        db.put("incr_test_existing", "10");

        List<String> args = Collections.singletonList("incr_test_existing");
        String result = command.execute(args, mockCtx);

        // Should return :11
        assertEquals(":11\r\n", result);

        // Verify value was incremented
        String retrieved = db.get("incr_test_existing");
        assertEquals("11", retrieved);
    }

    @Test
    @DisplayName("INCR: Increment non-existent key (should initialize to 0 then increment)")
    void testIncrNonExistentKey() {
        // Remove key if it exists
        db.remove("incr_test_nonexistent");

        List<String> args = Collections.singletonList("incr_test_nonexistent");
        String result = command.execute(args, mockCtx);

        // Should return :1 (0 + 1)
        assertEquals(":1\r\n", result);

        // Verify value was set
        String retrieved = db.get("incr_test_nonexistent");
        assertEquals("1", retrieved);
    }

    @Test
    @DisplayName("INCR: Increment zero")
    void testIncrZero() {
        db.put("incr_test_zero", "0");

        List<String> args = Collections.singletonList("incr_test_zero");
        String result = command.execute(args, mockCtx);

        assertEquals(":1\r\n", result);
        assertEquals("1", db.get("incr_test_zero"));
    }

    @Test
    @DisplayName("INCR: Increment negative number")
    void testIncrNegative() {
        db.put("incr_test_negative", "-5");

        List<String> args = Collections.singletonList("incr_test_negative");
        String result = command.execute(args, mockCtx);

        assertEquals(":-4\r\n", result);
        assertEquals("-4", db.get("incr_test_negative"));
    }

    @Test
    @DisplayName("INCR: Multiple increments")
    void testIncrMultiple() {
        db.put("incr_test_multi", "0");
        List<String> args = Collections.singletonList("incr_test_multi");

        // First increment
        String result1 = command.execute(args, mockCtx);
        assertEquals(":1\r\n", result1);

        // Second increment
        String result2 = command.execute(args, mockCtx);
        assertEquals(":2\r\n", result2);

        // Third increment
        String result3 = command.execute(args, mockCtx);
        assertEquals(":3\r\n", result3);

        assertEquals("3", db.get("incr_test_multi"));
    }

    @Test
    @DisplayName("INCR: Error on non-integer string value")
    void testIncrNonIntegerString() {
        db.put("incr_test_string", "hello");

        List<String> args = Collections.singletonList("incr_test_string");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("not an integer"));

        // Value should remain unchanged
        assertEquals("hello", db.get("incr_test_string"));
    }

    @Test
    @DisplayName("INCR: Error on float value")
    void testIncrFloatValue() {
        db.put("incr_test_float", "3.14");

        List<String> args = Collections.singletonList("incr_test_float");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));

        // Value should remain unchanged
        assertEquals("3.14", db.get("incr_test_float"));
    }

    @Test
    @DisplayName("INCR: Error on wrong type (list)")
    void testIncrWrongTypeList() {
        // Create a list value
        db.put("incr_test_list", RedisValue.list(new java.util.ArrayList<>(Collections.singletonList("item1"))));

        List<String> args = Collections.singletonList("incr_test_list");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("WRONGTYPE"));
    }

    @Test
    @DisplayName("INCR: Error on wrong number of arguments (no args)")
    void testIncrNoArgs() {
        List<String> args = Collections.emptyList();
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("wrong number of arguments"));
    }

    @Test
    @DisplayName("INCR: Error on wrong number of arguments (too many args)")
    void testIncrTooManyArgs() {
        List<String> args = Arrays.asList("key1", "extra_arg");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("wrong number of arguments"));
    }

    @Test
    @DisplayName("INCR: Large positive number")
    void testIncrLargeNumber() {
        db.put("incr_test_large", "999999999");

        List<String> args = Collections.singletonList("incr_test_large");
        String result = command.execute(args, mockCtx);

        assertEquals(":1000000000\r\n", result);
        assertEquals("1000000000", db.get("incr_test_large"));
    }

    @Test
    @DisplayName("INCR: Overflow protection (Long.MAX_VALUE)")
    void testIncrOverflow() {
        db.put("incr_test_overflow", String.valueOf(Long.MAX_VALUE));

        List<String> args = Collections.singletonList("incr_test_overflow");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("not an integer or out of range"));

        // Value should remain unchanged
        assertEquals(String.valueOf(Long.MAX_VALUE), db.get("incr_test_overflow"));
    }

    @Test
    @DisplayName("INCR: Command name is correct")
    void testCommandName() {
        assertEquals("INCR", command.name());
    }

    @Test
    @DisplayName("INCR: Increment very large negative number approaching zero")
    void testIncrLargeNegativeApproachingZero() {
        db.put("incr_test_large_neg", String.valueOf(Long.MIN_VALUE + 1));

        List<String> args = Collections.singletonList("incr_test_large_neg");
        String result = command.execute(args, mockCtx);

        assertEquals(":" + (Long.MIN_VALUE + 2) + "\r\n", result);
        assertEquals(String.valueOf(Long.MIN_VALUE + 2), db.get("incr_test_large_neg"));
    }

    @Test
    @DisplayName("INCR: Increment at boundary (Long.MAX_VALUE - 1)")
    void testIncrNearMaxValue() {
        db.put("incr_test_near_max", String.valueOf(Long.MAX_VALUE - 1));

        List<String> args = Collections.singletonList("incr_test_near_max");
        String result = command.execute(args, mockCtx);

        assertEquals(":" + Long.MAX_VALUE + "\r\n", result);
        assertEquals(String.valueOf(Long.MAX_VALUE), db.get("incr_test_near_max"));
    }

    @Test
    @DisplayName("INCR: Leading zeros are parsed correctly")
    void testIncrWithLeadingZeros() {
        db.put("incr_test_leading_zeros", "0000042");

        List<String> args = Collections.singletonList("incr_test_leading_zeros");
        String result = command.execute(args, mockCtx);

        assertEquals(":43\r\n", result);
        assertEquals("43", db.get("incr_test_leading_zeros"));
    }

    @Test
    @DisplayName("INCR: String with whitespace is invalid")
    void testIncrStringWithWhitespace() {
        db.put("incr_test_whitespace", " 42 ");

        List<String> args = Collections.singletonList("incr_test_whitespace");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("not an integer"));
        assertEquals(" 42 ", db.get("incr_test_whitespace"));
    }

    @Test
    @DisplayName("INCR: Empty string is invalid")
    void testIncrEmptyString() {
        db.put("incr_test_empty", "");

        List<String> args = Collections.singletonList("incr_test_empty");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertEquals("", db.get("incr_test_empty"));
    }

    @Test
    @DisplayName("INCR: Scientific notation is invalid")
    void testIncrScientificNotation() {
        db.put("incr_test_scientific", "1e5");

        List<String> args = Collections.singletonList("incr_test_scientific");
        String result = command.execute(args, mockCtx);

        assertTrue(result.contains("ERR"));
        assertEquals("1e5", db.get("incr_test_scientific"));
    }

    @Test
    @DisplayName("INCR: Underflow at Long.MIN_VALUE")
    void testIncrUnderflow() {
        db.put("incr_test_underflow", String.valueOf(Long.MIN_VALUE));

        List<String> args = Collections.singletonList("incr_test_underflow");
        String result = command.execute(args, mockCtx);

        // Should succeed: MIN_VALUE + 1 is valid
        assertEquals(":" + (Long.MIN_VALUE + 1) + "\r\n", result);
        assertEquals(String.valueOf(Long.MIN_VALUE + 1), db.get("incr_test_underflow"));
    }

    @Test
    @DisplayName("INCR: Concurrent-like behavior with multiple increments")
    void testIncrConcurrentLike() {
        db.put("incr_test_concurrent", "100");
        List<String> args = Collections.singletonList("incr_test_concurrent");

        // Simulate rapid increments
        for (int i = 0; i < 10; i++) {
            String result = command.execute(args, mockCtx);
            int expectedValue = 101 + i;
            assertEquals(":" + expectedValue + "\r\n", result);
        }

        assertEquals("110", db.get("incr_test_concurrent"));
    }

    @Test
    @DisplayName("INCR: Plus sign prefix is handled")
    void testIncrPlusSignPrefix() {
        // Java's Long.parseLong accepts + prefix, so +42 parses as 42
        // This differs from Redis, which treats +42 as invalid
        db.put("incr_test_plus", "+42");

        List<String> args = Collections.singletonList("incr_test_plus");
        String result = command.execute(args, mockCtx);

        // Java accepts +42 as valid, so it increments to 43
        assertEquals(":43\r\n", result);
        assertEquals("43", db.get("incr_test_plus"));
    }
}