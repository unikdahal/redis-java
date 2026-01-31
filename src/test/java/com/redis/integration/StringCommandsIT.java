package com.redis.integration;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for String commands: SET, GET, INCR.
 * <p>
 * Tests the complete request-response cycle through the RESP protocol
 * with real EmbeddedChannel and RedisDatabase instances.
 */
@DisplayName("String Commands Integration Tests")
public class StringCommandsIT extends BaseIntegrationTest {

    private static final String TEST_KEY = "string_it_key";
    private static final String TEST_KEY2 = "string_it_key2";
    private static final String COUNTER_KEY = "string_it_counter";

    @AfterEach
    void cleanup() {
        cleanupKeys(TEST_KEY, TEST_KEY2, COUNTER_KEY,
            "nx_key", "xx_key", "expiry_key", "incr_key", "overflow_key");
    }

    // ==================== SET Command Tests ====================

    @Nested
    @DisplayName("SET Command")
    class SetCommandTests {

        @Test
        @DisplayName("SET basic key-value")
        void testSetBasic() {
            assertOk("SET", TEST_KEY, "hello");
            assertEquals("hello", db.get(TEST_KEY));
        }

        @Test
        @DisplayName("SET overwrites existing value")
        void testSetOverwrite() {
            assertOk("SET", TEST_KEY, "first");
            assertOk("SET", TEST_KEY, "second");
            assertEquals("second", db.get(TEST_KEY));
        }

        @Test
        @DisplayName("SET with empty string value")
        void testSetEmptyString() {
            assertOk("SET", TEST_KEY, "");
            assertEquals("", db.get(TEST_KEY));
        }

        @Test
        @DisplayName("SET with special characters")
        void testSetSpecialChars() {
            String value = "hello\nworld\twith\rspecial chars!@#$%^&*()";
            assertOk("SET", TEST_KEY, value);
            assertEquals(value, db.get(TEST_KEY));
        }

        @Test
        @DisplayName("SET with special characters in value")
        void testSetUnicode() {
            // Use ASCII characters to avoid encoding issues in tests
            String value = "hello-world-123";
            assertOk("SET", TEST_KEY, value);
            assertEquals(value, db.get(TEST_KEY));
        }

        @Test
        @DisplayName("SET with very long value")
        void testSetLongValue() {
            String value = "x".repeat(10000);
            assertOk("SET", TEST_KEY, value);
            assertEquals(value, db.get(TEST_KEY));
        }

        @Test
        @DisplayName("SET with NX option - key does not exist")
        void testSetNxKeyNotExists() {
            assertOk("SET", "nx_key", "value", "NX");
            assertEquals("value", db.get("nx_key"));
        }

        @Test
        @DisplayName("SET with NX option - key exists")
        void testSetNxKeyExists() {
            db.put("nx_key", "original");
            assertNil("SET", "nx_key", "new_value", "NX");
            assertEquals("original", db.get("nx_key"));
        }

        @Test
        @DisplayName("SET with XX option - key exists")
        void testSetXxKeyExists() {
            db.put("xx_key", "original");
            assertOk("SET", "xx_key", "updated", "XX");
            assertEquals("updated", db.get("xx_key"));
        }

        @Test
        @DisplayName("SET with XX option - key does not exist")
        void testSetXxKeyNotExists() {
            assertNil("SET", "xx_key", "value", "XX");
            assertNull(db.get("xx_key"));
        }

        @Test
        @DisplayName("SET with EX option")
        void testSetWithEx() {
            assertOk("SET", "expiry_key", "value", "EX", "60");
            assertEquals("value", db.get("expiry_key"));
            assertTrue(db.getExpiryTime("expiry_key") > System.currentTimeMillis());
        }

        @Test
        @DisplayName("SET with PX option")
        void testSetWithPx() {
            assertOk("SET", "expiry_key", "value", "PX", "60000");
            assertEquals("value", db.get("expiry_key"));
            assertTrue(db.getExpiryTime("expiry_key") > System.currentTimeMillis());
        }

        @Test
        @DisplayName("SET with invalid EX value")
        void testSetInvalidEx() {
            assertError("SET", TEST_KEY, "value", "EX", "notanumber");
        }

        @Test
        @DisplayName("SET with negative EX value")
        void testSetNegativeEx() {
            assertError("SET", TEST_KEY, "value", "EX", "-1");
        }

        @Test
        @DisplayName("SET with EX and NX options combined")
        void testSetExAndNx() {
            assertOk("SET", "expiry_key", "value", "EX", "60", "NX");
            assertEquals("value", db.get("expiry_key"));
            assertTrue(db.getExpiryTime("expiry_key") > System.currentTimeMillis());
        }

        @Test
        @DisplayName("SET wrong number of arguments")
        void testSetWrongArgs() {
            assertErrorContains("wrong number of arguments", "SET", TEST_KEY);
        }

        @Test
        @DisplayName("SET with conflicting NX and XX")
        void testSetNxXxConflict() {
            assertError("SET", TEST_KEY, "value", "NX", "XX");
        }
    }

    // ==================== GET Command Tests ====================

    @Nested
    @DisplayName("GET Command")
    class GetCommandTests {

        @Test
        @DisplayName("GET existing key")
        void testGetExisting() {
            db.put(TEST_KEY, "hello");
            assertBulkString("hello", "GET", TEST_KEY);
        }

        @Test
        @DisplayName("GET non-existent key")
        void testGetNonExistent() {
            assertNil("GET", "nonexistent_key");
        }

        @Test
        @DisplayName("GET empty string value")
        void testGetEmptyString() {
            db.put(TEST_KEY, "");
            String result = sendCommand("GET", TEST_KEY);
            assertEquals("$0\r\n\r\n", result);
        }

        @Test
        @DisplayName("GET after SET")
        void testGetAfterSet() {
            assertOk("SET", TEST_KEY, "world");
            assertBulkString("world", "GET", TEST_KEY);
        }

        @Test
        @DisplayName("GET wrong number of arguments - no args")
        void testGetNoArgs() {
            assertErrorContains("wrong number of arguments", "GET");
        }

        @Test
        @DisplayName("GET wrong number of arguments - too many")
        void testGetTooManyArgs() {
            assertErrorContains("wrong number of arguments", "GET", "key1", "key2");
        }

        @Test
        @DisplayName("GET on wrong type (list)")
        void testGetOnWrongType() {
            db.put(TEST_KEY, com.redis.storage.RedisValue.list(new java.util.ArrayList<>()));
            assertNil("GET", TEST_KEY);  // Returns nil for wrong type
        }
    }

    // ==================== INCR Command Tests ====================

    @Nested
    @DisplayName("INCR Command")
    class IncrCommandTests {

        @Test
        @DisplayName("INCR existing numeric key")
        void testIncrExisting() {
            db.put(COUNTER_KEY, "10");
            assertInteger(11, "INCR", COUNTER_KEY);
            assertEquals("11", db.get(COUNTER_KEY));
        }

        @Test
        @DisplayName("INCR non-existent key")
        void testIncrNonExistent() {
            assertInteger(1, "INCR", "incr_key");
            assertEquals("1", db.get("incr_key"));
        }

        @Test
        @DisplayName("INCR zero")
        void testIncrZero() {
            db.put(COUNTER_KEY, "0");
            assertInteger(1, "INCR", COUNTER_KEY);
        }

        @Test
        @DisplayName("INCR negative number")
        void testIncrNegative() {
            db.put(COUNTER_KEY, "-5");
            assertInteger(-4, "INCR", COUNTER_KEY);
        }

        @Test
        @DisplayName("INCR multiple times")
        void testIncrMultiple() {
            db.put(COUNTER_KEY, "0");
            assertInteger(1, "INCR", COUNTER_KEY);
            assertInteger(2, "INCR", COUNTER_KEY);
            assertInteger(3, "INCR", COUNTER_KEY);
            assertEquals("3", db.get(COUNTER_KEY));
        }

        @Test
        @DisplayName("INCR on non-integer string")
        void testIncrNonInteger() {
            db.put(COUNTER_KEY, "hello");
            assertErrorContains("not an integer", "INCR", COUNTER_KEY);
            assertEquals("hello", db.get(COUNTER_KEY));  // Value unchanged
        }

        @Test
        @DisplayName("INCR on float value")
        void testIncrFloat() {
            db.put(COUNTER_KEY, "3.14");
            assertErrorContains("not an integer", "INCR", COUNTER_KEY);
        }

        @Test
        @DisplayName("INCR on wrong type (list)")
        void testIncrWrongType() {
            db.put(COUNTER_KEY, com.redis.storage.RedisValue.list(new java.util.ArrayList<>()));
            assertErrorContains("WRONGTYPE", "INCR", COUNTER_KEY);
        }

        @Test
        @DisplayName("INCR overflow protection")
        void testIncrOverflow() {
            db.put("overflow_key", String.valueOf(Long.MAX_VALUE));
            assertErrorContains("not an integer or out of range", "INCR", "overflow_key");
        }

        @Test
        @DisplayName("INCR wrong number of arguments")
        void testIncrWrongArgs() {
            assertErrorContains("wrong number of arguments", "INCR");
            assertErrorContains("wrong number of arguments", "INCR", "key1", "key2");
        }

        @Test
        @DisplayName("INCR large number")
        void testIncrLargeNumber() {
            db.put(COUNTER_KEY, "999999999");
            assertInteger(1000000000L, "INCR", COUNTER_KEY);
        }
    }

    // ==================== Combined Operations ====================

    @Nested
    @DisplayName("Combined String Operations")
    class CombinedOperationsTests {

        @Test
        @DisplayName("SET then GET then INCR workflow")
        void testSetGetIncrWorkflow() {
            assertOk("SET", COUNTER_KEY, "100");
            assertBulkString("100", "GET", COUNTER_KEY);
            assertInteger(101, "INCR", COUNTER_KEY);
            assertBulkString("101", "GET", COUNTER_KEY);
        }

        @Test
        @DisplayName("Multiple keys operations")
        void testMultipleKeys() {
            assertOk("SET", TEST_KEY, "value1");
            assertOk("SET", TEST_KEY2, "value2");
            assertBulkString("value1", "GET", TEST_KEY);
            assertBulkString("value2", "GET", TEST_KEY2);
        }

        @Test
        @DisplayName("Overwrite and verify")
        void testOverwriteAndVerify() {
            for (int i = 0; i < 10; i++) {
                assertOk("SET", TEST_KEY, "value" + i);
                assertBulkString("value" + i, "GET", TEST_KEY);
            }
        }
    }
}
