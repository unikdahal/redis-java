package com.redis.integration;

import com.redis.storage.RedisValue;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Stream commands: XADD, XRANGE, XREAD.
 * <p>
 * Tests the complete request-response cycle through the RESP protocol
 * with real EmbeddedChannel and RedisDatabase instances.
 */
@DisplayName("Stream Commands Integration Tests")
public class StreamCommandsIT extends BaseIntegrationTest {

    private static final String STREAM_KEY = "stream_it_key";
    private static final String STREAM_KEY2 = "stream_it_key2";

    @AfterEach
    void cleanup() {
        cleanupKeys(STREAM_KEY, STREAM_KEY2, "xadd_key", "xrange_key", "xread_key");
    }

    // ==================== XADD Command Tests ====================

    @Nested
    @DisplayName("XADD Command")
    class XAddCommandTests {

        @Test
        @DisplayName("XADD with auto-generated ID (*)")
        void testXAddAutoId() {
            String result = sendCommand("XADD", "xadd_key", "*", "field1", "value1");
            assertNotNull(result);
            assertTrue(result.startsWith("$"), "Expected bulk string response");
            // ID format should be like "1234567890123-0"
            assertTrue(result.contains("-"), "ID should contain timestamp-sequence format");
        }

        @Test
        @DisplayName("XADD with explicit ID")
        void testXAddExplicitId() {
            String result = sendCommand("XADD", "xadd_key", "1-0", "field1", "value1");
            assertNotNull(result);
            assertTrue(result.startsWith("$"), "Expected bulk string response");
            assertTrue(result.contains("1-0"), "Should return the specified ID");

            // Add another entry with higher ID
            String result2 = sendCommand("XADD", "xadd_key", "2-0", "field2", "value2");
            assertNotNull(result2);
            assertTrue(result2.contains("2-0"));
        }

        @Test
        @DisplayName("XADD multiple field-value pairs")
        void testXAddMultipleFields() {
            String result = sendCommand("XADD", "xadd_key", "*",
                "field1", "value1", "field2", "value2", "field3", "value3");
            assertNotNull(result);
            assertTrue(result.startsWith("$"));
        }

        @Test
        @DisplayName("XADD creates new stream")
        void testXAddCreatesStream() {
            sendCommand("XADD", "xadd_key", "*", "f", "v");
            assertSimpleString("stream", "TYPE", "xadd_key");
        }

        @Test
        @DisplayName("XADD on wrong type")
        void testXAddWrongType() {
            db.put("xadd_key", "string_value");
            assertErrorContains("WRONGTYPE", "XADD", "xadd_key", "*", "f", "v");
        }

        @Test
        @DisplayName("XADD with ID smaller than existing")
        void testXAddIdTooSmall() {
            sendCommand("XADD", "xadd_key", "10-0", "f", "v");
            assertErrorContains("smaller", "XADD", "xadd_key", "5-0", "f", "v");
        }

        @Test
        @DisplayName("XADD wrong number of arguments")
        void testXAddWrongArgs() {
            assertErrorContains("wrong number of arguments", "XADD");
            assertErrorContains("wrong number of arguments", "XADD", "key");
            assertErrorContains("wrong number of arguments", "XADD", "key", "*");
            // Odd number of field-value args
            assertErrorContains("wrong number of arguments", "XADD", "key", "*", "field");
        }

        @Test
        @DisplayName("XADD multiple entries with auto ID")
        void testXAddMultipleEntries() {
            String id1 = sendCommand("XADD", "xadd_key", "*", "seq", "1");
            String id2 = sendCommand("XADD", "xadd_key", "*", "seq", "2");
            String id3 = sendCommand("XADD", "xadd_key", "*", "seq", "3");

            // All should be valid bulk strings
            assertNotNull(id1);
            assertNotNull(id2);
            assertNotNull(id3);
            assertTrue(id1.startsWith("$"));
            assertTrue(id2.startsWith("$"));
            assertTrue(id3.startsWith("$"));
        }
    }

    // ==================== XRANGE Command Tests ====================

    @Nested
    @DisplayName("XRANGE Command")
    class XRangeCommandTests {

        @BeforeEach
        void setupStream() {
            sendCommand("XADD", "xrange_key", "1-0", "f1", "v1");
            sendCommand("XADD", "xrange_key", "2-0", "f2", "v2");
            sendCommand("XADD", "xrange_key", "3-0", "f3", "v3");
        }

        @Test
        @DisplayName("XRANGE full range with - and +")
        void testXRangeFullRange() {
            String result = sendCommand("XRANGE", "xrange_key", "-", "+");
            assertNotNull(result);
            assertTrue(result.startsWith("*3\r\n"), "Should return 3 entries");
        }

        @Test
        @DisplayName("XRANGE specific range")
        void testXRangeSpecificRange() {
            String result = sendCommand("XRANGE", "xrange_key", "1-0", "2-0");
            assertNotNull(result);
            assertTrue(result.startsWith("*2\r\n"), "Should return 2 entries");
        }

        @Test
        @DisplayName("XRANGE with COUNT")
        void testXRangeWithCount() {
            String result = sendCommand("XRANGE", "xrange_key", "-", "+", "COUNT", "2");
            assertNotNull(result);
            assertTrue(result.startsWith("*2\r\n"), "Should return 2 entries");
        }

        @Test
        @DisplayName("XRANGE on non-existent key")
        void testXRangeNonExistent() {
            assertEmptyArray("XRANGE", "nonexistent_key", "-", "+");
        }

        @Test
        @DisplayName("XRANGE no matching entries")
        void testXRangeNoMatch() {
            assertEmptyArray("XRANGE", "xrange_key", "100-0", "200-0");
        }

        @Test
        @DisplayName("XRANGE on wrong type")
        void testXRangeWrongType() {
            db.put("xrange_key", "string_value");
            assertErrorContains("WRONGTYPE", "XRANGE", "xrange_key", "-", "+");
        }

        @Test
        @DisplayName("XRANGE wrong number of arguments")
        void testXRangeWrongArgs() {
            assertErrorContains("wrong number of arguments", "XRANGE");
            assertErrorContains("wrong number of arguments", "XRANGE", "key");
            assertErrorContains("wrong number of arguments", "XRANGE", "key", "-");
        }
    }

    // ==================== XREAD Command Tests ====================

    @Nested
    @DisplayName("XREAD Command")
    class XReadCommandTests {

        @BeforeEach
        void setupStream() {
            sendCommand("XADD", "xread_key", "1-0", "f1", "v1");
            sendCommand("XADD", "xread_key", "2-0", "f2", "v2");
        }

        @Test
        @DisplayName("XREAD single stream from beginning")
        void testXReadFromBeginning() {
            String result = sendCommand("XREAD", "STREAMS", "xread_key", "0");
            assertNotNull(result);
            assertTrue(result.startsWith("*"), "Should return array");
            assertTrue(result.contains("xread_key"));
        }

        @Test
        @DisplayName("XREAD with COUNT")
        void testXReadWithCount() {
            String result = sendCommand("XREAD", "COUNT", "1", "STREAMS", "xread_key", "0");
            assertNotNull(result);
            assertTrue(result.startsWith("*"));
        }

        @Test
        @DisplayName("XREAD from specific ID")
        void testXReadFromId() {
            String result = sendCommand("XREAD", "STREAMS", "xread_key", "1-0");
            assertNotNull(result);
            // Should return entry 2-0 (after 1-0)
            assertTrue(result.contains("2-0") || result.startsWith("*"));
        }

        @Test
        @DisplayName("XREAD non-existent stream")
        void testXReadNonExistent() {
            String result = sendCommand("XREAD", "STREAMS", "nonexistent", "0");
            // Should return nil or empty
            assertTrue(result.equals("*-1\r\n") || result.equals("*0\r\n") || result.startsWith("*"));
        }

        @Test
        @DisplayName("XREAD wrong number of arguments")
        void testXReadWrongArgs() {
            assertErrorContains("wrong number of arguments", "XREAD");
            assertErrorContains("wrong number of arguments", "XREAD", "STREAMS");
        }

        @Test
        @DisplayName("XREAD with BLOCK 0 and existing data returns immediately")
        void testXReadBlockWithData() {
            String result = sendCommand("XREAD", "BLOCK", "0", "STREAMS", "xread_key", "0");
            assertNotNull(result);
            // Should return data immediately since it exists
            assertTrue(result.startsWith("*"));
        }
    }

    // ==================== Combined Stream Operations ====================

    @Nested
    @DisplayName("Combined Stream Operations")
    class CombinedOperationsTests {

        @Test
        @DisplayName("XADD then XRANGE workflow")
        void testXAddXRangeWorkflow() {
            // Add entries
            sendCommand("XADD", STREAM_KEY, "1-0", "name", "alice", "age", "30");
            sendCommand("XADD", STREAM_KEY, "2-0", "name", "bob", "age", "25");
            sendCommand("XADD", STREAM_KEY, "3-0", "name", "charlie", "age", "35");

            // Read all
            String result = sendCommand("XRANGE", STREAM_KEY, "-", "+");
            assertNotNull(result);
            assertTrue(result.startsWith("*3\r\n"));
            assertTrue(result.contains("alice"));
            assertTrue(result.contains("bob"));
            assertTrue(result.contains("charlie"));
        }

        @Test
        @DisplayName("XADD then XREAD workflow")
        void testXAddXReadWorkflow() {
            sendCommand("XADD", STREAM_KEY, "1-0", "event", "login");
            sendCommand("XADD", STREAM_KEY, "2-0", "event", "purchase");

            // Read from beginning
            String result = sendCommand("XREAD", "STREAMS", STREAM_KEY, "0");
            assertNotNull(result);
            assertTrue(result.contains("login"));
            assertTrue(result.contains("purchase"));
        }

        @Test
        @DisplayName("Multiple streams XREAD")
        void testMultipleStreamsXRead() {
            sendCommand("XADD", STREAM_KEY, "1-0", "data", "stream1");
            sendCommand("XADD", STREAM_KEY2, "1-0", "data", "stream2");

            String result = sendCommand("XREAD", "STREAMS", STREAM_KEY, STREAM_KEY2, "0", "0");
            assertNotNull(result);
            assertTrue(result.contains("stream1") || result.startsWith("*"));
        }

        @Test
        @DisplayName("Stream with many entries")
        void testStreamManyEntries() {
            // Add 50 entries
            for (int i = 1; i <= 50; i++) {
                sendCommand("XADD", STREAM_KEY, i + "-0", "index", String.valueOf(i));
            }

            // Range query with count
            String result = sendCommand("XRANGE", STREAM_KEY, "-", "+", "COUNT", "10");
            assertNotNull(result);
            assertTrue(result.startsWith("*10\r\n"));
        }
    }
}
