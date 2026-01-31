package com.redis.integration;

import com.redis.storage.RedisValue;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for List commands: LPUSH, RPUSH, LPOP, LLEN, LRANGE, BLPOP.
 * <p>
 * Tests the complete request-response cycle through the RESP protocol
 * with real EmbeddedChannel and RedisDatabase instances.
 */
@DisplayName("List Commands Integration Tests")
public class ListCommandsIT extends BaseIntegrationTest {

    private static final String LIST_KEY = "list_it_key";
    private static final String LIST_KEY2 = "list_it_key2";

    @AfterEach
    void cleanup() {
        cleanupKeys(LIST_KEY, LIST_KEY2, "lpush_key", "rpush_key", "lpop_key",
            "llen_key", "lrange_key", "blpop_key", "blpop_key2");
    }

    // ==================== LPUSH Command Tests ====================

    @Nested
    @DisplayName("LPUSH Command")
    class LPushCommandTests {

        @Test
        @DisplayName("LPUSH single element to new key")
        void testLPushSingleNew() {
            assertInteger(1, "LPUSH", "lpush_key", "one");
            List<String> list = db.getTyped("lpush_key", RedisValue.Type.LIST);
            assertEquals("one", list.get(0));
        }

        @Test
        @DisplayName("LPUSH multiple elements")
        void testLPushMultiple() {
            assertInteger(3, "LPUSH", "lpush_key", "one", "two", "three");
            List<String> list = db.getTyped("lpush_key", RedisValue.Type.LIST);
            // LPUSH adds to head, so order is reversed
            assertEquals(3, list.size());
            assertEquals("three", list.get(0));
            assertEquals("two", list.get(1));
            assertEquals("one", list.get(2));
        }

        @Test
        @DisplayName("LPUSH to existing list")
        void testLPushExisting() {
            assertInteger(1, "LPUSH", "lpush_key", "first");
            assertInteger(2, "LPUSH", "lpush_key", "second");
            List<String> list = db.getTyped("lpush_key", RedisValue.Type.LIST);
            assertEquals("second", list.get(0));
            assertEquals("first", list.get(1));
        }

        @Test
        @DisplayName("LPUSH on wrong type (string)")
        void testLPushWrongType() {
            db.put("lpush_key", "string_value");
            assertErrorContains("WRONGTYPE", "LPUSH", "lpush_key", "value");
        }

        @Test
        @DisplayName("LPUSH wrong number of arguments")
        void testLPushWrongArgs() {
            assertErrorContains("wrong number of arguments", "LPUSH");
            assertErrorContains("wrong number of arguments", "LPUSH", "key");
        }

        @Test
        @DisplayName("LPUSH empty string element")
        void testLPushEmptyString() {
            assertInteger(1, "LPUSH", "lpush_key", "");
            List<String> list = db.getTyped("lpush_key", RedisValue.Type.LIST);
            assertEquals("", list.get(0));
        }
    }

    // ==================== RPUSH Command Tests ====================

    @Nested
    @DisplayName("RPUSH Command")
    class RPushCommandTests {

        @Test
        @DisplayName("RPUSH single element to new key")
        void testRPushSingleNew() {
            assertInteger(1, "RPUSH", "rpush_key", "one");
            List<String> list = db.getTyped("rpush_key", RedisValue.Type.LIST);
            assertEquals("one", list.get(0));
        }

        @Test
        @DisplayName("RPUSH multiple elements")
        void testRPushMultiple() {
            assertInteger(3, "RPUSH", "rpush_key", "one", "two", "three");
            List<String> list = db.getTyped("rpush_key", RedisValue.Type.LIST);
            // RPUSH adds to tail, so order is preserved
            assertEquals("one", list.get(0));
            assertEquals("two", list.get(1));
            assertEquals("three", list.get(2));
        }

        @Test
        @DisplayName("RPUSH to existing list")
        void testRPushExisting() {
            assertInteger(1, "RPUSH", "rpush_key", "first");
            assertInteger(2, "RPUSH", "rpush_key", "second");
            List<String> list = db.getTyped("rpush_key", RedisValue.Type.LIST);
            assertEquals("first", list.get(0));
            assertEquals("second", list.get(1));
        }

        @Test
        @DisplayName("RPUSH on wrong type")
        void testRPushWrongType() {
            db.put("rpush_key", "string_value");
            assertErrorContains("WRONGTYPE", "RPUSH", "rpush_key", "value");
        }

        @Test
        @DisplayName("RPUSH wrong number of arguments")
        void testRPushWrongArgs() {
            assertErrorContains("wrong number of arguments", "RPUSH");
            assertErrorContains("wrong number of arguments", "RPUSH", "key");
        }
    }

    // ==================== LPOP Command Tests ====================

    @Nested
    @DisplayName("LPOP Command")
    class LPopCommandTests {

        @Test
        @DisplayName("LPOP from list with elements")
        void testLPopExisting() {
            sendCommand("RPUSH", "lpop_key", "one", "two", "three");
            assertBulkString("one", "LPOP", "lpop_key");
            assertBulkString("two", "LPOP", "lpop_key");
            assertBulkString("three", "LPOP", "lpop_key");
        }

        @Test
        @DisplayName("LPOP from empty list")
        void testLPopEmpty() {
            sendCommand("RPUSH", "lpop_key", "one");
            sendCommand("LPOP", "lpop_key");  // Remove the only element
            assertNil("LPOP", "lpop_key");
        }

        @Test
        @DisplayName("LPOP from non-existent key")
        void testLPopNonExistent() {
            assertNil("LPOP", "nonexistent_key");
        }

        @Test
        @DisplayName("LPOP with count")
        void testLPopWithCount() {
            sendCommand("RPUSH", "lpop_key", "a", "b", "c", "d", "e");
            String result = assertArraySize(3, "LPOP", "lpop_key", "3");
            assertTrue(result.contains("$1\r\na\r\n"));
            assertTrue(result.contains("$1\r\nb\r\n"));
            assertTrue(result.contains("$1\r\nc\r\n"));
        }

        @Test
        @DisplayName("LPOP with count larger than list size")
        void testLPopCountLargerThanList() {
            sendCommand("RPUSH", "lpop_key", "a", "b");
            String result = assertArraySize(2, "LPOP", "lpop_key", "5");
            assertTrue(result.contains("$1\r\na\r\n"));
            assertTrue(result.contains("$1\r\nb\r\n"));
        }

        @Test
        @DisplayName("LPOP on wrong type")
        void testLPopWrongType() {
            db.put("lpop_key", "string_value");
            assertErrorContains("WRONGTYPE", "LPOP", "lpop_key");
        }
    }

    // ==================== LLEN Command Tests ====================

    @Nested
    @DisplayName("LLEN Command")
    class LLenCommandTests {

        @Test
        @DisplayName("LLEN on list with elements")
        void testLLenExisting() {
            sendCommand("RPUSH", "llen_key", "a", "b", "c");
            assertInteger(3, "LLEN", "llen_key");
        }

        @Test
        @DisplayName("LLEN on empty list")
        void testLLenEmpty() {
            sendCommand("RPUSH", "llen_key", "a");
            sendCommand("LPOP", "llen_key");
            // After popping the only element, key may be deleted
            assertInteger(0, "LLEN", "llen_key");
        }

        @Test
        @DisplayName("LLEN on non-existent key")
        void testLLenNonExistent() {
            assertInteger(0, "LLEN", "nonexistent_key");
        }

        @Test
        @DisplayName("LLEN on wrong type")
        void testLLenWrongType() {
            db.put("llen_key", "string_value");
            assertErrorContains("WRONGTYPE", "LLEN", "llen_key");
        }

        @Test
        @DisplayName("LLEN wrong number of arguments")
        void testLLenWrongArgs() {
            assertErrorContains("wrong number of arguments", "LLEN");
            assertErrorContains("wrong number of arguments", "LLEN", "key1", "key2");
        }
    }

    // ==================== LRANGE Command Tests ====================

    @Nested
    @DisplayName("LRANGE Command")
    class LRangeCommandTests {

        @BeforeEach
        void setupList() {
            sendCommand("RPUSH", "lrange_key", "a", "b", "c", "d", "e");
        }

        @Test
        @DisplayName("LRANGE full list")
        void testLRangeFull() {
            String result = assertArraySize(5, "LRANGE", "lrange_key", "0", "-1");
            List<String> elements = parseArrayResponse(result);
            assertEquals(List.of("a", "b", "c", "d", "e"), elements);
        }

        @Test
        @DisplayName("LRANGE partial - first 3")
        void testLRangeFirst3() {
            String result = assertArraySize(3, "LRANGE", "lrange_key", "0", "2");
            List<String> elements = parseArrayResponse(result);
            assertEquals(List.of("a", "b", "c"), elements);
        }

        @Test
        @DisplayName("LRANGE with negative indices")
        void testLRangeNegative() {
            String result = assertArraySize(2, "LRANGE", "lrange_key", "-2", "-1");
            List<String> elements = parseArrayResponse(result);
            assertEquals(List.of("d", "e"), elements);
        }

        @Test
        @DisplayName("LRANGE out of bounds")
        void testLRangeOutOfBounds() {
            String result = assertArraySize(5, "LRANGE", "lrange_key", "0", "100");
            List<String> elements = parseArrayResponse(result);
            assertEquals(5, elements.size());
        }

        @Test
        @DisplayName("LRANGE start > end")
        void testLRangeStartGreaterThanEnd() {
            assertEmptyArray("LRANGE", "lrange_key", "3", "1");
        }

        @Test
        @DisplayName("LRANGE on non-existent key")
        void testLRangeNonExistent() {
            assertEmptyArray("LRANGE", "nonexistent_key", "0", "-1");
        }

        @Test
        @DisplayName("LRANGE on wrong type")
        void testLRangeWrongType() {
            db.put("lrange_key", "string_value");
            assertErrorContains("WRONGTYPE", "LRANGE", "lrange_key", "0", "-1");
        }

        @Test
        @DisplayName("LRANGE wrong number of arguments")
        void testLRangeWrongArgs() {
            assertErrorContains("wrong number of arguments", "LRANGE");
            assertErrorContains("wrong number of arguments", "LRANGE", "key");
            assertErrorContains("wrong number of arguments", "LRANGE", "key", "0");
        }
    }

    // ==================== BLPOP Command Tests ====================

    @Nested
    @DisplayName("BLPOP Command")
    class BLPopCommandTests {

        @Test
        @DisplayName("BLPOP immediate return when data exists")
        void testBLPopImmediate() {
            sendCommand("RPUSH", "blpop_key", "value1", "value2");
            String result = assertArraySize(2, "BLPOP", "blpop_key", "0");
            assertTrue(result.contains("blpop_key"));
            assertTrue(result.contains("value1"));
        }

        @Test
        @DisplayName("BLPOP timeout on empty key")
        void testBLPopTimeout() {
            // With timeout 0 and no data, should return nil immediately for check-once behavior
            assertNullArray("BLPOP", "blpop_key", "0");
        }

        @Test
        @DisplayName("BLPOP multiple keys - first has data")
        void testBLPopMultipleKeysFirstHasData() {
            sendCommand("RPUSH", "blpop_key", "value");
            String result = assertArraySize(2, "BLPOP", "blpop_key", "blpop_key2", "0");
            assertTrue(result.contains("blpop_key"));
            assertTrue(result.contains("value"));
        }

        @Test
        @DisplayName("BLPOP multiple keys - second has data")
        void testBLPopMultipleKeysSecondHasData() {
            sendCommand("RPUSH", "blpop_key2", "value");
            String result = assertArraySize(2, "BLPOP", "blpop_key", "blpop_key2", "0");
            assertTrue(result.contains("blpop_key2"));
            assertTrue(result.contains("value"));
        }

        @Test
        @DisplayName("BLPOP wrong number of arguments")
        void testBLPopWrongArgs() {
            assertErrorContains("wrong number of arguments", "BLPOP");
            assertErrorContains("wrong number of arguments", "BLPOP", "key");
        }

        @Test
        @DisplayName("BLPOP invalid timeout")
        void testBLPopInvalidTimeout() {
            assertError("BLPOP", "blpop_key", "notanumber");
        }

        @Test
        @DisplayName("BLPOP negative timeout")
        void testBLPopNegativeTimeout() {
            assertError("BLPOP", "blpop_key", "-1");
        }
    }

    // ==================== Combined List Operations ====================

    @Nested
    @DisplayName("Combined List Operations")
    class CombinedOperationsTests {

        @Test
        @DisplayName("LPUSH and RPUSH on same list")
        void testLPushRPushCombined() {
            assertInteger(1, "LPUSH", LIST_KEY, "middle");
            assertInteger(2, "LPUSH", LIST_KEY, "first");
            assertInteger(3, "RPUSH", LIST_KEY, "last");

            String result = sendCommand("LRANGE", LIST_KEY, "0", "-1");
            List<String> elements = parseArrayResponse(result);
            assertEquals(List.of("first", "middle", "last"), elements);
        }

        @Test
        @DisplayName("Build and drain list")
        void testBuildAndDrainList() {
            // Build list
            for (int i = 0; i < 5; i++) {
                sendCommand("RPUSH", LIST_KEY, "item" + i);
            }
            assertInteger(5, "LLEN", LIST_KEY);

            // Drain list
            for (int i = 0; i < 5; i++) {
                assertBulkString("item" + i, "LPOP", LIST_KEY);
            }
            assertInteger(0, "LLEN", LIST_KEY);
        }

        @Test
        @DisplayName("List as stack (LPUSH/LPOP)")
        void testListAsStack() {
            sendCommand("LPUSH", LIST_KEY, "a");
            sendCommand("LPUSH", LIST_KEY, "b");
            sendCommand("LPUSH", LIST_KEY, "c");

            assertBulkString("c", "LPOP", LIST_KEY);
            assertBulkString("b", "LPOP", LIST_KEY);
            assertBulkString("a", "LPOP", LIST_KEY);
        }

        @Test
        @DisplayName("List as queue (RPUSH/LPOP)")
        void testListAsQueue() {
            sendCommand("RPUSH", LIST_KEY, "first");
            sendCommand("RPUSH", LIST_KEY, "second");
            sendCommand("RPUSH", LIST_KEY, "third");

            assertBulkString("first", "LPOP", LIST_KEY);
            assertBulkString("second", "LPOP", LIST_KEY);
            assertBulkString("third", "LPOP", LIST_KEY);
        }

        @Test
        @DisplayName("Large list operations")
        void testLargeList() {
            // Push 100 elements
            for (int i = 0; i < 100; i++) {
                sendCommand("RPUSH", LIST_KEY, "element" + i);
            }
            assertInteger(100, "LLEN", LIST_KEY);

            // Range query
            String result = sendCommand("LRANGE", LIST_KEY, "0", "9");
            assertNotNull(result);
            assertTrue(result.startsWith("*10\r\n"));

            // Pop some elements
            for (int i = 0; i < 50; i++) {
                sendCommand("LPOP", LIST_KEY);
            }
            assertInteger(50, "LLEN", LIST_KEY);
        }
    }
}
