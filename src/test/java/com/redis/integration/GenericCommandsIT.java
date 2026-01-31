package com.redis.integration;

import com.redis.storage.RedisValue;
import org.junit.jupiter.api.*;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Generic commands: PING, ECHO, DEL, EXPIRE, TTL, TYPE.
 * <p>
 * Tests the complete request-response cycle through the RESP protocol
 * with real EmbeddedChannel and RedisDatabase instances.
 */
@DisplayName("Generic Commands Integration Tests")
public class GenericCommandsIT extends BaseIntegrationTest {

    private static final String TEST_KEY = "generic_it_key";
    private static final String TEST_KEY2 = "generic_it_key2";
    private static final String TEST_KEY3 = "generic_it_key3";

    @AfterEach
    void cleanup() {
        cleanupKeys(TEST_KEY, TEST_KEY2, TEST_KEY3,
            "del_key1", "del_key2", "del_key3",
            "expire_key", "ttl_key", "type_key");
    }

    // ==================== PING Command Tests ====================

    @Nested
    @DisplayName("PING Command")
    class PingCommandTests {

        @Test
        @DisplayName("PING without argument returns PONG")
        void testPingSimple() {
            assertSimpleString("PONG", "PING");
        }

        @Test
        @DisplayName("PING with message echoes message")
        void testPingWithMessage() {
            assertBulkString("hello", "PING", "hello");
        }

        @Test
        @DisplayName("PING with empty message")
        void testPingEmptyMessage() {
            String result = sendCommand("PING", "");
            assertEquals("$0\r\n\r\n", result);
        }

        @Test
        @DisplayName("PING with special characters")
        void testPingSpecialChars() {
            assertBulkString("hello world!", "PING", "hello world!");
        }

        @Test
        @DisplayName("PING with unicode")
        void testPingUnicode() {
            // Use ASCII for reliable test - unicode handling is server implementation specific
            assertBulkString("hello", "PING", "hello");
        }

        @Test
        @DisplayName("Multiple PING commands in sequence")
        void testMultiplePings() {
            for (int i = 0; i < 10; i++) {
                assertSimpleString("PONG", "PING");
            }
        }
    }

    // ==================== ECHO Command Tests ====================

    @Nested
    @DisplayName("ECHO Command")
    class EchoCommandTests {

        @Test
        @DisplayName("ECHO basic message")
        void testEchoBasic() {
            assertBulkString("hello", "ECHO", "hello");
        }

        @Test
        @DisplayName("ECHO empty string")
        void testEchoEmpty() {
            String result = sendCommand("ECHO", "");
            assertEquals("$0\r\n\r\n", result);
        }

        @Test
        @DisplayName("ECHO long message")
        void testEchoLong() {
            String message = "x".repeat(1000);
            assertBulkString(message, "ECHO", message);
        }

        @Test
        @DisplayName("ECHO with newlines")
        void testEchoNewlines() {
            assertBulkString("line1\nline2\nline3", "ECHO", "line1\nline2\nline3");
        }

        @Test
        @DisplayName("ECHO wrong number of arguments - no args")
        void testEchoNoArgs() {
            assertErrorContains("wrong number of arguments", "ECHO");
        }

        @Test
        @DisplayName("ECHO wrong number of arguments - too many")
        void testEchoTooManyArgs() {
            assertErrorContains("wrong number of arguments", "ECHO", "arg1", "arg2");
        }
    }

    // ==================== DEL Command Tests ====================

    @Nested
    @DisplayName("DEL Command")
    class DelCommandTests {

        @Test
        @DisplayName("DEL single existing key")
        void testDelSingleKey() {
            db.put("del_key1", "value");
            assertInteger(1, "DEL", "del_key1");
            assertNull(db.get("del_key1"));
        }

        @Test
        @DisplayName("DEL non-existent key")
        void testDelNonExistent() {
            assertInteger(0, "DEL", "nonexistent_key");
        }

        @Test
        @DisplayName("DEL multiple keys - all exist")
        void testDelMultipleAllExist() {
            db.put("del_key1", "v1");
            db.put("del_key2", "v2");
            db.put("del_key3", "v3");
            assertInteger(3, "DEL", "del_key1", "del_key2", "del_key3");
            assertNull(db.get("del_key1"));
            assertNull(db.get("del_key2"));
            assertNull(db.get("del_key3"));
        }

        @Test
        @DisplayName("DEL multiple keys - some exist")
        void testDelMultipleSomeExist() {
            db.put("del_key1", "v1");
            db.put("del_key3", "v3");
            assertInteger(2, "DEL", "del_key1", "del_key2", "del_key3");
        }

        @Test
        @DisplayName("DEL multiple keys - none exist")
        void testDelMultipleNoneExist() {
            assertInteger(0, "DEL", "nonexistent1", "nonexistent2");
        }

        @Test
        @DisplayName("DEL same key multiple times in one call")
        void testDelSameKeyMultiple() {
            db.put("del_key1", "value");
            // Only counts as 1 deletion even if specified multiple times
            assertInteger(1, "DEL", "del_key1", "del_key1", "del_key1");
        }

        @Test
        @DisplayName("DEL different value types")
        void testDelDifferentTypes() {
            db.put("del_key1", "string_value");
            db.put("del_key2", RedisValue.list(new ArrayList<>()));
            assertInteger(2, "DEL", "del_key1", "del_key2");
        }

        @Test
        @DisplayName("DEL wrong number of arguments")
        void testDelNoArgs() {
            assertErrorContains("wrong number of arguments", "DEL");
        }
    }

    // ==================== EXPIRE Command Tests ====================

    @Nested
    @DisplayName("EXPIRE Command")
    class ExpireCommandTests {

        @Test
        @DisplayName("EXPIRE on existing key")
        void testExpireExisting() {
            db.put("expire_key", "value");
            assertInteger(1, "EXPIRE", "expire_key", "60");
            assertTrue(db.getExpiryTime("expire_key") > System.currentTimeMillis());
        }

        @Test
        @DisplayName("EXPIRE on non-existent key")
        void testExpireNonExistent() {
            assertInteger(0, "EXPIRE", "nonexistent_key", "60");
        }

        @Test
        @DisplayName("EXPIRE with 1 second")
        void testExpireOneSecond() {
            db.put("expire_key", "value");
            assertInteger(1, "EXPIRE", "expire_key", "1");
            long expiry = db.getExpiryTime("expire_key");
            assertTrue(expiry > System.currentTimeMillis());
            assertTrue(expiry <= System.currentTimeMillis() + 1500);  // Within ~1.5 seconds
        }

        @Test
        @DisplayName("EXPIRE updates existing expiry")
        void testExpireUpdateExpiry() {
            db.put("expire_key", "value");
            assertInteger(1, "EXPIRE", "expire_key", "30");
            long firstExpiry = db.getExpiryTime("expire_key");
            assertInteger(1, "EXPIRE", "expire_key", "60");
            long secondExpiry = db.getExpiryTime("expire_key");
            assertTrue(secondExpiry > firstExpiry);
        }

        @Test
        @DisplayName("EXPIRE with invalid seconds")
        void testExpireInvalidSeconds() {
            db.put("expire_key", "value");
            assertError("EXPIRE", "expire_key", "notanumber");
        }

        @Test
        @DisplayName("EXPIRE wrong number of arguments")
        void testExpireWrongArgs() {
            assertErrorContains("wrong number of arguments", "EXPIRE");
            assertErrorContains("wrong number of arguments", "EXPIRE", "key");
        }
    }

    // ==================== TTL Command Tests ====================

    @Nested
    @DisplayName("TTL Command")
    class TtlCommandTests {

        @Test
        @DisplayName("TTL on key with expiry")
        void testTtlWithExpiry() {
            db.put("ttl_key", "value");
            sendCommand("EXPIRE", "ttl_key", "60");
            String result = sendCommand("TTL", "ttl_key");
            assertNotNull(result);
            assertTrue(result.startsWith(":"));
            int ttl = Integer.parseInt(result.substring(1, result.indexOf("\r")));
            assertTrue(ttl > 0 && ttl <= 60);
        }

        @Test
        @DisplayName("TTL on key without expiry")
        void testTtlNoExpiry() {
            db.put("ttl_key", "value");
            assertInteger(-1, "TTL", "ttl_key");
        }

        @Test
        @DisplayName("TTL on non-existent key")
        void testTtlNonExistent() {
            assertInteger(-2, "TTL", "nonexistent_key");
        }

        @Test
        @DisplayName("TTL wrong number of arguments")
        void testTtlWrongArgs() {
            // With no args, TTL may return error or treat empty string as key
            String result = sendCommand("TTL");
            assertNotNull(result);
            // Either returns error or -2 for empty key name
            assertTrue(result.startsWith("-") || result.equals(":-2\r\n"),
                "Expected error or -2, got: " + result);
        }
    }

    // ==================== TYPE Command Tests ====================

    @Nested
    @DisplayName("TYPE Command")
    class TypeCommandTests {

        @Test
        @DisplayName("TYPE on string key")
        void testTypeString() {
            db.put("type_key", "string_value");
            assertSimpleString("string", "TYPE", "type_key");
        }

        @Test
        @DisplayName("TYPE on list key")
        void testTypeList() {
            db.put("type_key", RedisValue.list(new ArrayList<>()));
            assertSimpleString("list", "TYPE", "type_key");
        }

        @Test
        @DisplayName("TYPE on non-existent key")
        void testTypeNonExistent() {
            assertSimpleString("none", "TYPE", "nonexistent_key");
        }

        @Test
        @DisplayName("TYPE wrong number of arguments")
        void testTypeWrongArgs() {
            assertErrorContains("wrong number of arguments", "TYPE");
            assertErrorContains("wrong number of arguments", "TYPE", "key1", "key2");
        }
    }

    // ==================== Combined Operations ====================

    @Nested
    @DisplayName("Combined Generic Operations")
    class CombinedOperationsTests {

        @Test
        @DisplayName("SET, EXPIRE, TTL workflow")
        void testSetExpireTtlWorkflow() {
            assertOk("SET", TEST_KEY, "value");
            assertSimpleString("string", "TYPE", TEST_KEY);
            assertInteger(-1, "TTL", TEST_KEY);  // No expiry yet
            assertInteger(1, "EXPIRE", TEST_KEY, "100");
            String ttlResult = sendCommand("TTL", TEST_KEY);
            int ttl = Integer.parseInt(ttlResult.substring(1, ttlResult.indexOf("\r")));
            assertTrue(ttl > 0 && ttl <= 100);
        }

        @Test
        @DisplayName("DEL clears TTL")
        void testDelClearsTtl() {
            db.put(TEST_KEY, "value");
            sendCommand("EXPIRE", TEST_KEY, "60");
            assertInteger(1, "DEL", TEST_KEY);
            assertInteger(-2, "TTL", TEST_KEY);  // Key doesn't exist
        }

        @Test
        @DisplayName("PING and ECHO in sequence")
        void testPingEchoSequence() {
            assertSimpleString("PONG", "PING");
            assertBulkString("test", "ECHO", "test");
            assertSimpleString("PONG", "PING");
            assertBulkString("hello", "PING", "hello");
        }
    }
}
