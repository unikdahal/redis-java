package com.redis.integration;

import com.redis.storage.RedisValue;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.*;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Transaction commands: MULTI, EXEC, DISCARD.
 * <p>
 * Tests the complete request-response cycle through the RESP protocol
 * with real EmbeddedChannel and RedisDatabase instances.
 * <p>
 * Test Coverage:
 * - Basic MULTI/EXEC flow
 * - MULTI/DISCARD flow
 * - Error handling
 * - Transaction isolation
 * - Large transactions
 */
@DisplayName("Transaction Commands Integration Tests")
public class TransactionCommandsIT extends BaseIntegrationTest {

    private static final String TX_KEY = "tx_it_key";
    private static final String TX_KEY2 = "tx_it_key2";
    private static final String TX_COUNTER = "tx_it_counter";

    @AfterEach
    void cleanup() {
        cleanupKeys(TX_KEY, TX_KEY2, TX_COUNTER);
        for (int i = 0; i < 100; i++) {
            db.remove("tx_large_" + i);
        }
    }

    // ==================== MULTI Command Tests ====================

    @Nested
    @DisplayName("MULTI Command")
    class MultiCommandTests {

        @Test
        @DisplayName("MULTI returns OK")
        void testMultiReturnsOk() {
            assertOk("MULTI");
        }

        @Test
        @DisplayName("Nested MULTI returns error")
        void testNestedMulti() {
            assertOk("MULTI");
            assertErrorContains("nested", "MULTI");
        }

        @Test
        @DisplayName("Commands after MULTI return QUEUED")
        void testCommandsQueued() {
            assertOk("MULTI");
            assertSimpleString("QUEUED", "SET", TX_KEY, "value");
            assertSimpleString("QUEUED", "GET", TX_KEY);
            assertSimpleString("QUEUED", "INCR", TX_COUNTER);
        }
    }

    // ==================== EXEC Command Tests ====================

    @Nested
    @DisplayName("EXEC Command")
    class ExecCommandTests {

        @Test
        @DisplayName("EXEC without MULTI returns error")
        void testExecWithoutMulti() {
            assertErrorContains("without MULTI", "EXEC");
        }

        @Test
        @DisplayName("EXEC executes queued commands")
        void testExecExecutesCommands() {
            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "hello");
            sendCommand("GET", TX_KEY);

            String result = sendCommand("EXEC");
            assertNotNull(result);
            assertTrue(result.startsWith("*2\r\n"));
            assertTrue(result.contains("+OK\r\n"));
            assertTrue(result.contains("$5\r\nhello\r\n"));

            assertEquals("hello", db.get(TX_KEY));
        }

        @Test
        @DisplayName("EXEC on empty transaction returns empty array")
        void testExecEmptyTransaction() {
            sendCommand("MULTI");
            assertEmptyArray("EXEC");
        }

        @Test
        @DisplayName("EXEC with INCR operations")
        void testExecWithIncr() {
            db.put(TX_COUNTER, "0");

            sendCommand("MULTI");
            sendCommand("INCR", TX_COUNTER);
            sendCommand("INCR", TX_COUNTER);
            sendCommand("INCR", TX_COUNTER);

            String result = sendCommand("EXEC");
            assertTrue(result.contains(":1\r\n"));
            assertTrue(result.contains(":2\r\n"));
            assertTrue(result.contains(":3\r\n"));

            assertEquals("3", db.get(TX_COUNTER));
        }

        @Test
        @DisplayName("EXEC preserves command order")
        void testExecPreservesOrder() {
            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "v1");
            sendCommand("SET", TX_KEY, "v2");
            sendCommand("SET", TX_KEY, "v3");
            sendCommand("GET", TX_KEY);

            String result = sendCommand("EXEC");
            assertTrue(result.startsWith("*4\r\n"));
            assertTrue(result.contains("$2\r\nv3\r\n"));
            assertEquals("v3", db.get(TX_KEY));
        }
    }

    // ==================== DISCARD Command Tests ====================

    @Nested
    @DisplayName("DISCARD Command")
    class DiscardCommandTests {

        @Test
        @DisplayName("DISCARD without MULTI returns error")
        void testDiscardWithoutMulti() {
            assertErrorContains("without MULTI", "DISCARD");
        }

        @Test
        @DisplayName("DISCARD cancels transaction")
        void testDiscardCancelsTransaction() {
            db.put(TX_KEY, "original");

            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "modified");
            assertOk("DISCARD");

            assertEquals("original", db.get(TX_KEY));
        }

        @Test
        @DisplayName("DISCARD clears queued commands")
        void testDiscardClearsQueue() {
            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "value");
            sendCommand("SET", TX_KEY2, "value");
            assertOk("DISCARD");

            // Should be able to start new transaction
            assertOk("MULTI");
            assertEmptyArray("EXEC");
        }

        @Test
        @DisplayName("Can start new transaction after DISCARD")
        void testNewTransactionAfterDiscard() {
            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "discarded");
            sendCommand("DISCARD");

            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "actual");
            sendCommand("EXEC");

            assertEquals("actual", db.get(TX_KEY));
        }
    }

    // ==================== Error Handling Tests ====================

    @Nested
    @DisplayName("Transaction Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Unknown command in transaction marks error")
        void testUnknownCommandMarksError() {
            sendCommand("MULTI");
            String queueResult = sendCommand("INVALIDCMD", "arg");
            assertTrue(queueResult.contains("ERR"));

            String execResult = sendCommand("EXEC");
            assertTrue(execResult.contains("EXECABORT") || execResult.contains("discarded"));
        }

        @Test
        @DisplayName("Command errors during EXEC are returned in array")
        void testCommandErrorsInExec() {
            db.put(TX_KEY, RedisValue.list(new ArrayList<>()));

            sendCommand("MULTI");
            sendCommand("SET", TX_KEY2, "value");  // Should succeed
            sendCommand("INCR", TX_KEY);  // Should fail - wrong type

            String result = sendCommand("EXEC");
            assertTrue(result.startsWith("*2\r\n"));
            assertTrue(result.contains("+OK\r\n"));
            assertTrue(result.contains("WRONGTYPE") || result.contains("-ERR"));

            // First command should have succeeded
            assertEquals("value", db.get(TX_KEY2));
        }
    }

    // ==================== Transaction Isolation Tests ====================

    @Nested
    @DisplayName("Transaction Isolation")
    class IsolationTests {

        @Test
        @DisplayName("Transaction state isolated per connection")
        void testIsolationPerConnection() {
            EmbeddedChannel channel2 = createNewChannel();

            try {
                // Start transaction on channel 1
                assertOk("MULTI");

                // Channel 2 should be able to start its own transaction
                String result = sendCommandOn(channel2, "MULTI");
                assertEquals("+OK\r\n", result);

                // Both can queue commands
                assertSimpleString("QUEUED", "SET", TX_KEY, "channel1");
                assertEquals("+QUEUED\r\n", sendCommandOn(channel2, "SET", TX_KEY2, "channel2"));

            } finally {
                channel2.close();
            }
        }

        @Test
        @DisplayName("EXEC on one connection doesn't affect another")
        void testExecIsolation() {
            EmbeddedChannel channel2 = createNewChannel();

            try {
                // Channel 1 starts and completes transaction
                sendCommand("MULTI");
                sendCommand("SET", TX_KEY, "from_channel1");
                sendCommand("EXEC");

                // Channel 2 starts transaction
                sendCommandOn(channel2, "MULTI");
                sendCommandOn(channel2, "SET", TX_KEY2, "from_channel2");

                // Channel 1's data should be visible
                assertEquals("from_channel1", db.get(TX_KEY));

                // Channel 2 hasn't committed yet
                assertNull(db.get(TX_KEY2));

                sendCommandOn(channel2, "EXEC");
                assertEquals("from_channel2", db.get(TX_KEY2));

            } finally {
                channel2.close();
            }
        }
    }

    // ==================== Large Transaction Tests ====================

    @Nested
    @DisplayName("Large Transactions")
    class LargeTransactionTests {

        @Test
        @DisplayName("Transaction with 100 commands")
        void testLargeTransaction() {
            sendCommand("MULTI");

            for (int i = 0; i < 100; i++) {
                String queueResult = sendCommand("SET", "tx_large_" + i, "value" + i);
                assertEquals("+QUEUED\r\n", queueResult);
            }

            String result = sendCommand("EXEC");
            assertTrue(result.startsWith("*100\r\n"));

            // Verify all were set
            for (int i = 0; i < 100; i++) {
                assertEquals("value" + i, db.get("tx_large_" + i));
            }
        }

        @Test
        @DisplayName("Large transaction with DISCARD")
        void testLargeTransactionDiscard() {
            sendCommand("MULTI");

            for (int i = 0; i < 50; i++) {
                sendCommand("SET", "tx_large_" + i, "value");
            }

            assertOk("DISCARD");

            // None should be set
            for (int i = 0; i < 50; i++) {
                assertNull(db.get("tx_large_" + i));
            }
        }
    }

    // ==================== Combined Operations Tests ====================

    @Nested
    @DisplayName("Combined Transaction Operations")
    class CombinedOperationsTests {

        @Test
        @DisplayName("Multiple transactions in sequence")
        void testMultipleTransactionsSequence() {
            for (int i = 0; i < 5; i++) {
                sendCommand("MULTI");
                sendCommand("SET", TX_KEY, "iteration" + i);
                sendCommand("EXEC");
                assertEquals("iteration" + i, db.get(TX_KEY));
            }
        }

        @Test
        @DisplayName("Transaction with multiple data types")
        void testTransactionMultipleTypes() {
            sendCommand("MULTI");
            sendCommand("SET", TX_KEY, "string_value");
            sendCommand("LPUSH", TX_KEY2, "list_item");
            sendCommand("TYPE", TX_KEY);
            sendCommand("TYPE", TX_KEY2);

            String result = sendCommand("EXEC");
            assertTrue(result.startsWith("*4\r\n"));
            assertTrue(result.contains("+string\r\n"));
            assertTrue(result.contains("+list\r\n"));
        }

        @Test
        @DisplayName("Read-only transaction")
        void testReadOnlyTransaction() {
            db.put(TX_KEY, "value1");
            db.put(TX_KEY2, "value2");

            sendCommand("MULTI");
            sendCommand("GET", TX_KEY);
            sendCommand("GET", TX_KEY2);
            sendCommand("TYPE", TX_KEY);

            String result = sendCommand("EXEC");
            assertTrue(result.startsWith("*3\r\n"));
            assertTrue(result.contains("$6\r\nvalue1\r\n"));
            assertTrue(result.contains("$6\r\nvalue2\r\n"));
            assertTrue(result.contains("+string\r\n"));
        }

        @Test
        @DisplayName("Transaction with DEL command")
        void testTransactionWithDel() {
            db.put(TX_KEY, "to_delete");

            sendCommand("MULTI");
            sendCommand("DEL", TX_KEY);
            sendCommand("GET", TX_KEY);

            String result = sendCommand("EXEC");
            assertTrue(result.contains(":1\r\n"));  // DEL returns 1
            assertTrue(result.contains("$-1\r\n")); // GET returns nil

            assertNull(db.get(TX_KEY));
        }

        @Test
        @DisplayName("Transaction with PING and ECHO")
        void testTransactionWithPingEcho() {
            sendCommand("MULTI");
            sendCommand("PING");
            sendCommand("ECHO", "hello");
            sendCommand("PING", "world");

            String result = sendCommand("EXEC");
            assertTrue(result.startsWith("*3\r\n"));
            assertTrue(result.contains("+PONG\r\n"));
            assertTrue(result.contains("$5\r\nhello\r\n"));
            assertTrue(result.contains("$5\r\nworld\r\n"));
        }
    }
}
